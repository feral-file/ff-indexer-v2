//go:build integration

package store

import (
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/registry"
)

const migration033Path = "../../db/migrations/033_shared_contract_releases.sql"

// migration033Fixture seeds one membership per case the cleanup must tell apart. Token
// and release ids are fixed so assertions can name them.
//
//	T1, T2  SuperRare shared contract (checksummed), OpenSea release R1 "superrare"  -> removed
//	T3      BAYC, OpenSea release R2                                                -> kept
//	T4      hic et nunc OBJKT, objkt release R3                                     -> removed
//	T5      Foundation shared contract, Art Blocks release R4 (a series vendor)     -> kept
//	T6      SuperRare shared contract stored lowercase, OpenSea release R5          -> removed
//	T7      a non-shared contract in the same OpenSea release R5                    -> kept
//
// Every releases row stays, including R1 and R3 with no member left: deleting them would
// race a worker's release-then-member write (see the file header).
const migration033Fixture = `
INSERT INTO tokens (id, token_cid, chain, standard, contract_address, token_number) VALUES
  (1, 'eip155:1:erc721:0xb932a70A57673d89f4acfFBE830E8ed7f75Fb9e0:1', 'eip155:1', 'erc721', '0xb932a70A57673d89f4acfFBE830E8ed7f75Fb9e0', '1'),
  (2, 'eip155:1:erc721:0xb932a70A57673d89f4acfFBE830E8ed7f75Fb9e0:2', 'eip155:1', 'erc721', '0xb932a70A57673d89f4acfFBE830E8ed7f75Fb9e0', '2'),
  (3, 'eip155:1:erc721:0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D:3', 'eip155:1', 'erc721', '0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D', '3'),
  (4, 'tezos:mainnet:fa2:KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton:4', 'tezos:mainnet', 'fa2', 'KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton', '4'),
  (5, 'eip155:1:erc721:0x3B3ee1931Dc30C1957379FAc9aba94D1C48a5405:5', 'eip155:1', 'erc721', '0x3B3ee1931Dc30C1957379FAc9aba94D1C48a5405', '5'),
  (6, 'eip155:1:erc721:0xb932a70a57673d89f4acffbe830e8ed7f75fb9e0:6', 'eip155:1', 'erc721', '0xb932a70a57673d89f4acffbe830e8ed7f75fb9e0', '6'),
  (7, 'eip155:1:erc721:0x0000000000000000000000000000000000000007:7', 'eip155:1', 'erc721', '0x0000000000000000000000000000000000000007', '7');

INSERT INTO releases (id, vendor, vendor_release_id, name) VALUES
  (1, 'opensea', 'superrare', 'SuperRare'),
  (2, 'opensea', 'bored-ape-yacht-club', 'Bored Ape Yacht Club'),
  (3, 'objkt', 'KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton', 'hic et nunc'),
  (4, 'artblocks', '1-0xabc-1', 'A project'),
  (5, 'opensea', 'mixed', 'Mixed');

INSERT INTO release_members (release_id, token_id, mint_number) VALUES
  (1, 1, 1), (1, 2, 2), (2, 3, 3), (3, 4, 4), (4, 5, 5), (5, 6, 6), (5, 7, 7);
`

// TestMigration033SharedContractReleases runs db/migrations/033_shared_contract_releases.sql
// through psql, as the steward does, against a throwaway database, then runs it again to
// show it is idempotent.
func TestMigration033SharedContractReleases(t *testing.T) {
	if _, err := exec.LookPath("psql"); err != nil {
		t.Skip("psql not available")
	}

	dbName := fmt.Sprintf("m033_%d", time.Now().UnixNano())
	require.NoError(t, testDB.Exec("CREATE DATABASE "+dbName).Error)
	t.Cleanup(func() { testDB.Exec("DROP DATABASE IF EXISTS " + dbName) })
	conn := dsnForDatabase(t, testDSN, dbName)

	runPSQLFile(t, conn, schemaPath)
	runPSQL(t, conn, migration033Fixture)

	members := `SELECT string_agg(token_id || ':' || release_id, ',' ORDER BY token_id) FROM release_members`
	releases := `SELECT string_agg(id::text, ',' ORDER BY id) FROM releases`

	output := runPSQLFile(t, conn, migration033Path)
	assert.Regexp(t, `(?m)^\s+4 \|\s+3$`, output, "reports 4 memberships removed from 3 releases")
	assert.Equal(t, "3:2,5:4,7:5", runPSQL(t, conn, members))
	assert.Equal(t, "1,2,3,4,5", runPSQL(t, conn, releases))

	runPSQLFile(t, conn, migration033Path)
	assert.Equal(t, "3:2,5:4,7:5", runPSQL(t, conn, members))
	assert.Equal(t, "1,2,3,4,5", runPSQL(t, conn, releases))
}

// Every contract the cleanup matches is a shared contract in the shipped registry, so the
// file removes nothing the enhancer would still write.
func TestMigration033ContractsAreRegistrySharedContracts(t *testing.T) {
	sql, err := os.ReadFile(migration033Path)
	require.NoError(t, err)
	reg, err := registry.NewPublisherRegistryLoader(adapter.NewFileSystem(), adapter.NewJSON()).
		Load("../../tools/registry/publisher.json")
	require.NoError(t, err)

	rows := regexp.MustCompile(`(?m)^\s+\('(eip155:1|tezos:mainnet)', '([^']+)'\)[,;]`).FindAllStringSubmatch(string(sql), -1)
	require.Len(t, rows, 8, "the file's VALUES list")
	for _, row := range rows {
		assert.True(t, reg.IsSharedContract(domain.Chain(row[1]), row[2]), row[2])
	}
}

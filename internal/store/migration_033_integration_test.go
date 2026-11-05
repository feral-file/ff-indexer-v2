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
//	T1, T2  SuperRare shared contract (checksummed), OpenSea release R1 "superrare"      -> removed
//	T3      BAYC, OpenSea release R2                                                    -> kept
//	T4      hic et nunc OBJKT, objkt release R3 (the contract's own KT1)                -> removed
//	T5      Foundation shared contract, Art Blocks release R4 (a series vendor)         -> kept
//	T6      SuperRare shared contract stored lowercase, OpenSea release R1              -> removed
//	T7      a non-shared contract in the same OpenSea release R1                        -> kept
//	T8      Rarible ERC-1155, OpenSea release R5 "xcopy-rarible" (a creator collection) -> kept
//	T9      Rarible ERC-721 of 2020 (0x6A5F...), OpenSea release R6 "rarible"           -> removed
//	T10     SuperRare shared contract, OpenSea release R7, OpenSea's misfiled bucket    -> removed
//
// Every releases row stays, including R1, R3, R6 and R7 with no member left: deleting
// them would race a worker's release-then-member write (see the file header).
const migration033Fixture = `
INSERT INTO tokens (id, token_cid, chain, standard, contract_address, token_number) VALUES
  (1, 'eip155:1:erc721:0xb932a70A57673d89f4acfFBE830E8ed7f75Fb9e0:1', 'eip155:1', 'erc721', '0xb932a70A57673d89f4acfFBE830E8ed7f75Fb9e0', '1'),
  (2, 'eip155:1:erc721:0xb932a70A57673d89f4acfFBE830E8ed7f75Fb9e0:2', 'eip155:1', 'erc721', '0xb932a70A57673d89f4acfFBE830E8ed7f75Fb9e0', '2'),
  (3, 'eip155:1:erc721:0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D:3', 'eip155:1', 'erc721', '0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D', '3'),
  (4, 'tezos:mainnet:fa2:KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton:4', 'tezos:mainnet', 'fa2', 'KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton', '4'),
  (5, 'eip155:1:erc721:0x3B3ee1931Dc30C1957379FAc9aba94D1C48a5405:5', 'eip155:1', 'erc721', '0x3B3ee1931Dc30C1957379FAc9aba94D1C48a5405', '5'),
  (6, 'eip155:1:erc721:0xb932a70a57673d89f4acffbe830e8ed7f75fb9e0:6', 'eip155:1', 'erc721', '0xb932a70a57673d89f4acffbe830e8ed7f75fb9e0', '6'),
  (7, 'eip155:1:erc721:0x0000000000000000000000000000000000000007:7', 'eip155:1', 'erc721', '0x0000000000000000000000000000000000000007', '7'),
  (8, 'eip155:1:erc1155:0xd07dc4262BCDbf85190C01c996b4C06a461d2430:8', 'eip155:1', 'erc1155', '0xd07dc4262BCDbf85190C01c996b4C06a461d2430', '8'),
  (9, 'eip155:1:erc721:0x6A5Ff3CEEcaE9CeB96E6ac6c76B82aF8B39f0Eb3:9', 'eip155:1', 'erc721', '0x6A5Ff3CEEcaE9CeB96E6ac6c76B82aF8B39f0Eb3', '9'),
  (10, 'eip155:1:erc721:0xb932a70A57673d89f4acfFBE830E8ed7f75Fb9e0:10', 'eip155:1', 'erc721', '0xb932a70A57673d89f4acfFBE830E8ed7f75Fb9e0', '10');

INSERT INTO releases (id, vendor, vendor_release_id, name) VALUES
  (1, 'opensea', 'superrare', 'SuperRare'),
  (2, 'opensea', 'bored-ape-yacht-club', 'Bored Ape Yacht Club'),
  (3, 'objkt', 'KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton', 'hic et nunc'),
  (4, 'artblocks', '1-0xabc-1', 'A project'),
  (5, 'opensea', 'xcopy-rarible', 'XCOPY - Rarible'),
  (6, 'opensea', 'rarible', 'Rarible'),
  (7, 'opensea', '0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85', '0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85');

INSERT INTO release_members (release_id, token_id, mint_number) VALUES
  (1, 1, 1), (1, 2, 2), (2, 3, 3), (3, 4, 4), (4, 5, 5), (1, 6, 6), (1, 7, 7), (5, 8, 8), (6, 9, 9), (7, 10, 10);
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
	assert.Regexp(t, `(?m)^\s+6 \|\s+4$`, output, "reports 6 memberships removed from 4 releases")
	assert.Equal(t, "3:2,5:4,7:1,8:5", runPSQL(t, conn, members))
	assert.Equal(t, "1,2,3,4,5,6,7", runPSQL(t, conn, releases))

	runPSQLFile(t, conn, migration033Path)
	assert.Equal(t, "3:2,5:4,7:1,8:5", runPSQL(t, conn, members))
	assert.Equal(t, "1,2,3,4,5,6,7", runPSQL(t, conn, releases))
}

// Every (contract, vendor, release) the cleanup matches is a platform-wide release in the
// shipped registry, so the file removes nothing the enhancer would still write.
func TestMigration033RowsArePlatformReleasesInRegistry(t *testing.T) {
	sql, err := os.ReadFile(migration033Path)
	require.NoError(t, err)
	reg, err := registry.NewPublisherRegistryLoader(adapter.NewFileSystem(), adapter.NewJSON()).
		Load("../../tools/registry/publisher.json")
	require.NoError(t, err)

	rows := regexp.MustCompile(`(?m)^\s+\('(eip155:1|tezos:mainnet)', '([^']+)', '([^']+)', '([^']+)'\)[,;]`).FindAllStringSubmatch(string(sql), -1)
	require.Len(t, rows, 14, "the file's VALUES list")
	for _, row := range rows {
		assert.True(t, reg.IsPlatformRelease(domain.Chain(row[1]), row[2], row[3], row[4]), "%s %s %s", row[2], row[3], row[4])
	}
}

// migration033MovedFixture models a worker re-enriching a token while the cleanup runs:
// T1 starts in the platform release R1 "superrare", and an event trigger that fires right
// after the file snapshots its removal list (CREATE TABLE AS) moves T1 to the creator
// collection R2. UpsertReleaseMember keeps the row id on such a move, so the snapshot's id
// now names a correct membership. The trigger runs inside the migration's transaction,
// which is enough to put that state in front of the DELETE deterministically.
const migration033MovedFixture = `
INSERT INTO tokens (id, token_cid, chain, standard, contract_address, token_number) VALUES
  (1, 'eip155:1:erc721:0xb932a70A57673d89f4acfFBE830E8ed7f75Fb9e0:1', 'eip155:1', 'erc721', '0xb932a70A57673d89f4acfFBE830E8ed7f75Fb9e0', '1');
INSERT INTO releases (id, vendor, vendor_release_id, name) VALUES
  (1, 'opensea', 'superrare', 'SuperRare'),
  (2, 'opensea', 'gazellio-spatial-exhibition', 'Gazell.io Spatial Exhibition Takeover');
INSERT INTO release_members (release_id, token_id, mint_number) VALUES (1, 1, 1);

CREATE FUNCTION migration_033_move_t1() RETURNS event_trigger LANGUAGE plpgsql AS $$
BEGIN
  UPDATE release_members SET release_id = 2 WHERE token_id = 1;
END $$;
CREATE EVENT TRIGGER migration_033_move_t1 ON ddl_command_end
  WHEN TAG IN ('CREATE TABLE AS') EXECUTE PROCEDURE migration_033_move_t1();
`

// TestMigration033KeepsMembershipMovedDuringCleanup: a membership that enrichment moves
// from the platform release to a creator collection after the file captured it must
// survive, because the DELETE re-checks the release it captured, not only the row id.
func TestMigration033KeepsMembershipMovedDuringCleanup(t *testing.T) {
	if _, err := exec.LookPath("psql"); err != nil {
		t.Skip("psql not available")
	}

	dbName := fmt.Sprintf("m033moved_%d", time.Now().UnixNano())
	require.NoError(t, testDB.Exec("CREATE DATABASE "+dbName).Error)
	t.Cleanup(func() { testDB.Exec("DROP DATABASE IF EXISTS " + dbName) })
	conn := dsnForDatabase(t, testDSN, dbName)

	runPSQLFile(t, conn, schemaPath)
	runPSQL(t, conn, migration033MovedFixture)

	output := runPSQLFile(t, conn, migration033Path)
	assert.Regexp(t, `(?m)^\s+0 \|\s+0$`, output, "reports nothing removed: the captured membership had moved")
	assert.Equal(t, "1:2", runPSQL(t, conn, `SELECT string_agg(token_id || ':' || release_id, ',') FROM release_members`))
}

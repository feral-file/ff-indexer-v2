//go:build integration

package store

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pgdriver "gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/plugin/dbresolver"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

func buildScanLog(block uint64, txHash string, logIndex uint) schema.AddressScanLog {
	return schema.AddressScanLog{
		BlockNumber: block,
		TxHash:      txHash,
		LogIndex:    logIndex,
		Address:     "0x1234567890123456789012345678901234567890",
		Topics:      []string{"0xaaaa", "0xbbbb"},
		Data:        []byte{0x01, 0x02},
		TxIndex:     1,
		BlockHash:   "0xbeef",
	}
}

// TestAddressScanSession_Lifecycle drives one session through the full design
// lifecycle (docs/address_scan_sessions.md): create → window appends with
// cursor advance → replay (tokens persisted, logs deleted, status flipped) →
// pending consumption → deletion with token cascade.
func TestAddressScanSession_Lifecycle(t *testing.T) {
	if testDB == nil {
		t.Skip("Test database not initialized")
	}
	st := initPGTestDB(t)
	ctx := context.Background()
	chain := domain.ChainEthereumMainnet
	addr := "0xScanOwner0000000000000000000000000000001"

	// No session yet.
	session, err := st.GetAddressScanSession(ctx, chain, addr)
	require.NoError(t, err)
	assert.Nil(t, session)

	// Create.
	session, err = st.CreateAddressScanSession(ctx, chain, addr, 100, 499)
	require.NoError(t, err)
	require.NotNil(t, session)
	assert.Equal(t, uint64(100), session.FromBlock)
	assert.Equal(t, uint64(499), session.ToBlock)
	assert.Equal(t, uint64(100), session.CursorBlock, "cursor starts at the range start")
	assert.Equal(t, schema.AddressScanStatusScanning, session.Status)

	// Racing create returns the existing session, not an error or a second row.
	dup, err := st.CreateAddressScanSession(ctx, chain, addr, 100, 499)
	require.NoError(t, err)
	require.NotNil(t, dup)
	assert.Equal(t, session.ID, dup.ID)

	// Window 1: two logs, cursor -> 300.
	err = st.AppendScanLogsAdvanceCursor(ctx, session.ID, []schema.AddressScanLog{
		buildScanLog(150, "0xtx1", 0),
		buildScanLog(250, "0xtx2", 1),
	}, 300)
	require.NoError(t, err)

	// Crash-replay of the same window is idempotent: same rows, same cursor.
	err = st.AppendScanLogsAdvanceCursor(ctx, session.ID, []schema.AddressScanLog{
		buildScanLog(150, "0xtx1", 0),
		buildScanLog(250, "0xtx2", 1),
	}, 300)
	require.NoError(t, err)

	// Window 2: empty window, cursor past the end.
	require.NoError(t, st.AppendScanLogsAdvanceCursor(ctx, session.ID, nil, 500))

	session, err = st.GetAddressScanSession(ctx, chain, addr)
	require.NoError(t, err)
	assert.Equal(t, uint64(500), session.CursorBlock)

	logs, err := st.GetAddressScanLogs(ctx, session.ID)
	require.NoError(t, err)
	require.Len(t, logs, 2, "idempotent re-append must not duplicate rows")
	assert.Equal(t, uint64(150), logs[0].BlockNumber, "logs come back in chain order")
	assert.Equal(t, []string{"0xaaaa", "0xbbbb"}, []string(logs[0].Topics))
	assert.Equal(t, []byte{0x01, 0x02}, logs[0].Data)

	// Replay: persist tokens, delete logs, flip status — one transaction.
	tokenA := domain.NewTokenCID(chain, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")
	tokenB := domain.NewTokenCID(chain, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "2")
	err = st.FinishAddressScanReplay(ctx, session.ID, []schema.AddressScanToken{
		{TokenCID: tokenA, BlockNumber: 150},
		{TokenCID: tokenB, BlockNumber: 250},
	})
	require.NoError(t, err)

	session, err = st.GetAddressScanSession(ctx, chain, addr)
	require.NoError(t, err)
	assert.Equal(t, schema.AddressScanStatusReplayed, session.Status)

	logs, err = st.GetAddressScanLogs(ctx, session.ID)
	require.NoError(t, err)
	assert.Empty(t, logs, "staged logs are deleted at replay")

	// Pending tokens come back newest-block first.
	pending, err := st.GetPendingAddressScanTokens(ctx, session.ID)
	require.NoError(t, err)
	require.Len(t, pending, 2)
	assert.Equal(t, tokenB, pending[0].TokenCID)
	assert.Equal(t, tokenA, pending[1].TokenCID)

	// Stamp one indexed; only the other stays pending.
	require.NoError(t, st.MarkAddressScanTokensIndexed(ctx, session.ID, []domain.TokenCID{tokenB}))
	pending, err = st.GetPendingAddressScanTokens(ctx, session.ID)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, tokenA, pending[0].TokenCID)

	// Delete; token rows cascade.
	sessionID := session.ID
	require.NoError(t, st.DeleteAddressScanSession(ctx, sessionID))
	session, err = st.GetAddressScanSession(ctx, chain, addr)
	require.NoError(t, err)
	assert.Nil(t, session)
	pending, err = st.GetPendingAddressScanTokens(ctx, sessionID)
	require.NoError(t, err)
	assert.Empty(t, pending, "token rows cascade with the session")
}

// TestAddressScanSession_CursorNeverMovesBackward pins the checkpoint guard: a
// duplicate delivery of an already-committed window (stale newCursor) must not
// rewind the cursor.
func TestAddressScanSession_CursorNeverMovesBackward(t *testing.T) {
	if testDB == nil {
		t.Skip("Test database not initialized")
	}
	st := initPGTestDB(t)
	ctx := context.Background()
	chain := domain.ChainEthereumMainnet
	addr := "0xScanOwner0000000000000000000000000000002"

	session, err := st.CreateAddressScanSession(ctx, chain, addr, 100, 499)
	require.NoError(t, err)

	require.NoError(t, st.AppendScanLogsAdvanceCursor(ctx, session.ID, nil, 400))
	require.NoError(t, st.AppendScanLogsAdvanceCursor(ctx, session.ID, nil, 300)) // stale duplicate

	session, err = st.GetAddressScanSession(ctx, chain, addr)
	require.NoError(t, err)
	assert.Equal(t, uint64(400), session.CursorBlock)
}

// TestAddressScanSession_InvalidRangeRejected pins input validation.
func TestAddressScanSession_InvalidRangeRejected(t *testing.T) {
	if testDB == nil {
		t.Skip("Test database not initialized")
	}
	st := initPGTestDB(t)
	ctx := context.Background()

	_, err := st.CreateAddressScanSession(ctx, domain.ChainEthereumMainnet, "0xbad", 500, 100)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid scan range")
}

// TestFinishAddressScanReplay_EmptyTokenListStillFlipsStatus covers the
// empty-wallet case: a scan that discovers nothing must still transition to
// replayed (and delete its logs) so the session can complete.
func TestFinishAddressScanReplay_EmptyTokenListStillFlipsStatus(t *testing.T) {
	if testDB == nil {
		t.Skip("Test database not initialized")
	}
	st := initPGTestDB(t)
	ctx := context.Background()
	chain := domain.ChainEthereumMainnet
	addr := "0xScanOwner0000000000000000000000000000003"

	session, err := st.CreateAddressScanSession(ctx, chain, addr, 100, 199)
	require.NoError(t, err)
	require.NoError(t, st.AppendScanLogsAdvanceCursor(ctx, session.ID, []schema.AddressScanLog{
		buildScanLog(150, "0xtx1", 0),
	}, 200))

	require.NoError(t, st.FinishAddressScanReplay(ctx, session.ID, nil))

	session, err = st.GetAddressScanSession(ctx, chain, addr)
	require.NoError(t, err)
	assert.Equal(t, schema.AddressScanStatusReplayed, session.Status)

	logs, err := st.GetAddressScanLogs(ctx, session.ID)
	require.NoError(t, err)
	assert.Empty(t, logs)

	pending, err := st.GetPendingAddressScanTokens(ctx, session.ID)
	require.NoError(t, err)
	assert.Empty(t, pending)
}

// divergentReplicaDSN derives a DSN identical to the test database but with
// search_path pointed at an EMPTY shadow schema. The resolver-enabled store's
// "replica" then genuinely diverges from the primary: any replica-routed read of
// a scan-session table returns nothing, while a primary-routed read returns the
// row. This is what lets the routing test fail deterministically without the
// dbresolver.Write pins — a same-schema "replica" (no lag) would let an
// unpinned read pass by accident, proving nothing.
func divergentReplicaDSN() string {
	if strings.Contains(testDSN, "://") { // URL form (testcontainers)
		sep := "?"
		if strings.Contains(testDSN, "?") {
			sep = "&"
		}
		return testDSN + sep + "options=-c%20search_path%3D" + scanReplicaShadowSchema
	}
	// Keyword form (external DB via TEST_DB_* env).
	return testDSN + " options='-c search_path=" + scanReplicaShadowSchema + "'"
}

const scanReplicaShadowSchema = "scan_replica_shadow"

// createScanReplicaShadow (re)creates the shadow schema with empty structural
// copies of every table the session lifecycle reads touch.
func createScanReplicaShadow(t *testing.T) {
	t.Helper()
	stmts := []string{
		`DROP SCHEMA IF EXISTS ` + scanReplicaShadowSchema + ` CASCADE`,
		`CREATE SCHEMA ` + scanReplicaShadowSchema,
		`CREATE TABLE ` + scanReplicaShadowSchema + `.address_scan_sessions (LIKE public.address_scan_sessions INCLUDING ALL)`,
		`CREATE TABLE ` + scanReplicaShadowSchema + `.address_scan_logs (LIKE public.address_scan_logs INCLUDING ALL)`,
		`CREATE TABLE ` + scanReplicaShadowSchema + `.address_scan_tokens (LIKE public.address_scan_tokens INCLUDING ALL)`,
		`CREATE TABLE ` + scanReplicaShadowSchema + `.watched_addresses (LIKE public.watched_addresses INCLUDING ALL)`,
	}
	for _, stmt := range stmts {
		require.NoError(t, testDB.Exec(stmt).Error, stmt)
	}
	t.Cleanup(func() {
		_ = testDB.Exec(`DROP SCHEMA IF EXISTS ` + scanReplicaShadowSchema + ` CASCADE`).Error
	})
}

// TestAddressScanLifecycleReads_RoutedToPrimary is the replica-routing
// regression guard for the session lifecycle reads (review finding on the
// scan-session PR). Each read gates a destructive or cost-bearing decision made
// moments after a primary write: a replica-routed GetPendingAddressScanTokens
// returning empty makes the workflow delete the session and cascade away
// unindexed tokens; a replica-routed GetAddressScanLogs feeds a partial event
// history to the replay, which then deletes the complete primary-side set.
//
// The "replica" is the same server with search_path on an EMPTY shadow schema
// (divergentReplicaDSN), so routing is observable: a replica-routed read returns
// nothing, a primary-routed read returns the row. Verified to fail without the
// dbresolver.Write pins. The negative control proves an unpinned read on this
// store really does land on the shadow, so the pins are what make the difference.
func TestAddressScanLifecycleReads_RoutedToPrimary(t *testing.T) {
	if testDB == nil {
		t.Fatal("Test database not initialized")
	}
	createScanReplicaShadow(t)

	db, err := gorm.Open(pgdriver.Open(testDSN), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	require.NoError(t, db.Use(dbresolver.Register(dbresolver.Config{
		Replicas: []gorm.Dialector{pgdriver.Open(divergentReplicaDSN())},
	})))
	require.True(t, hasDBResolver(db), "harness must actually register a resolver")

	ctx := context.Background()
	chain := domain.ChainEthereumMainnet
	addr := "0xScanReplicaRouting000000000000000000001"

	// Negative control: an unpinned SELECT on this store is replica-routed and
	// therefore lands on the empty shadow schema. If this ever stops holding,
	// the positive assertions below prove nothing.
	var shadowPath string
	require.NoError(t, db.WithContext(ctx).Raw(`select current_setting('search_path')`).Scan(&shadowPath).Error)
	require.Equal(t, scanReplicaShadowSchema, shadowPath, "unpinned reads must land on the divergent shadow replica")

	st := NewPGStore(db)
	t.Cleanup(func() {
		_ = testDB.Exec(`DELETE FROM address_scan_sessions WHERE address = ?`, addr).Error
		_ = testDB.Exec(`DELETE FROM watched_addresses WHERE address = ?`, addr).Error
	})

	// Create on the primary; a replica-routed session read would report "none".
	created, err := st.CreateAddressScanSession(ctx, chain, addr, 100, 299)
	require.NoError(t, err)
	got, err := st.GetAddressScanSession(ctx, chain, addr)
	require.NoError(t, err)
	require.NotNil(t, got, "GetAddressScanSession must read the primary: a replica miss means 'no session'")
	assert.Equal(t, created.ID, got.ID)

	// Stage logs + cursor on the primary; a replica-routed logs read would
	// return an empty/partial set and corrupt the ownership replay.
	require.NoError(t, st.AppendScanLogsAdvanceCursor(ctx, created.ID, []schema.AddressScanLog{
		buildScanLog(150, "0xrr-tx1", 0),
		buildScanLog(250, "0xrr-tx2", 1),
	}, 300))
	logs, err := st.GetAddressScanLogs(ctx, created.ID)
	require.NoError(t, err)
	require.Len(t, logs, 2, "GetAddressScanLogs must read the primary: a partial set corrupts the replay")
	got, err = st.GetAddressScanSession(ctx, chain, addr)
	require.NoError(t, err)
	assert.Equal(t, uint64(300), got.CursorBlock, "cursor read must observe the just-committed advance")

	// Replay on the primary; a replica-routed pending read returning empty is
	// the data-loss path (watermark advance + session delete + token cascade).
	tok := domain.NewTokenCID(chain, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "9")
	require.NoError(t, st.FinishAddressScanReplay(ctx, created.ID, []schema.AddressScanToken{
		{TokenCID: tok, BlockNumber: 250},
	}))
	pending, err := st.GetPendingAddressScanTokens(ctx, created.ID)
	require.NoError(t, err)
	require.Len(t, pending, 1, "GetPendingAddressScanTokens must read the primary: empty here cascades away unindexed tokens")
	got, err = st.GetAddressScanSession(ctx, chain, addr)
	require.NoError(t, err)
	assert.Equal(t, schema.AddressScanStatusReplayed, got.Status, "status read must observe replay")

	// Watermark: the workflow derives the NEXT scan range from this read right
	// after writing it; a replica-routed read would open a duplicate session.
	require.NoError(t, st.EnsureWatchedAddressExists(ctx, addr, chain, 10))
	require.NoError(t, st.UpdateIndexingBlockRangeForAddress(ctx, addr, chain, 100, 299))
	minB, maxB, err := st.GetIndexingBlockRangeForAddress(ctx, addr, chain)
	require.NoError(t, err)
	assert.Equal(t, uint64(100), minB)
	assert.Equal(t, uint64(299), maxB, "watermark read must observe the just-committed range")
}

//go:build integration

package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

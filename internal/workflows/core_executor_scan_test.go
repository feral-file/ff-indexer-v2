package workflows_test

import (
	"context"
	"errors"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

// ====================================================================================
// Ethereum owner-scan session executor tests
// ====================================================================================

// TestFetchAndPersistEthereumScanWindow_RowsAndCursor pins the checkpoint
// contract across the fetch/persist split: fetch converts merged owner logs to
// rows preserving every field the replay and the CryptoPunks receipt repair
// read (emitting contract, topics, data, block/tx/log position), and persist
// hands the store those rows together with the advanced cursor (windowEnd+1).
func TestFetchAndPersistEthereumScanWindow_RowsAndCursor(t *testing.T) {
	tm := setupTestExecutor(t)
	defer tearDownTestExecutor(tm)

	ctx := context.Background()
	address := "0xdadB0d80178819F2319190D340ce9A924f783711"
	const sessionID int64 = 7

	vLog := ethtypes.Log{
		Address:     common.HexToAddress("0x1234567890123456789012345678901234567890"),
		Topics:      []common.Hash{common.HexToHash("0xaaaa"), common.HexToHash("0xbbbb")},
		Data:        common.LeftPadBytes(big.NewInt(42).Bytes(), 32),
		BlockNumber: 150,
		TxHash:      common.HexToHash("0xdead"),
		TxIndex:     3,
		BlockHash:   common.HexToHash("0xbeef"),
		// Warehouse-served logs carry the block time; it must survive staging.
		BlockTimestamp: 1_700_000_000,
		Index:          9,
	}

	tm.ethClient.EXPECT().
		FetchOwnerLogsWindow(ctx, address, uint64(100), uint64(199)).
		Return([]ethtypes.Log{vLog}, nil)

	tm.store.EXPECT().
		AppendScanLogsAdvanceCursor(ctx, sessionID, gomock.Any(), uint64(200)).
		DoAndReturn(func(_ context.Context, _ int64, rows []schema.AddressScanLog, _ uint64) error {
			require.Len(t, rows, 1)
			row := rows[0]
			assert.Equal(t, uint64(150), row.BlockNumber)
			assert.Equal(t, vLog.TxHash.Hex(), row.TxHash)
			assert.Equal(t, uint(9), row.LogIndex)
			assert.Equal(t, vLog.Address.Hex(), row.Address)
			assert.Equal(t, []string{vLog.Topics[0].Hex(), vLog.Topics[1].Hex()}, []string(row.Topics))
			assert.Equal(t, vLog.Data, row.Data)
			assert.Equal(t, uint(3), row.TxIndex)
			assert.Equal(t, vLog.BlockHash.Hex(), row.BlockHash)
			assert.Equal(t, uint64(1_700_000_000), row.BlockTimestamp, "block timestamp must be staged (migration 029)")
			return nil
		})

	rows, err := tm.executor.FetchEthereumOwnerWindow(ctx, address, 100, 199)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.NoError(t, tm.executor.PersistEthereumScanWindow(ctx, sessionID, rows, 100, 199))
}

// TestFetchEthereumOwnerWindow_ErrorHasNoSideEffects verifies a failed fetch
// touches nothing: the strict store mock has no expectations at all, so any
// store call fails the test. This is what makes fetch safe to run ahead of
// its turn in the parallel pipeline and simply drop on a sibling failure.
func TestFetchEthereumOwnerWindow_ErrorHasNoSideEffects(t *testing.T) {
	tm := setupTestExecutor(t)
	defer tearDownTestExecutor(tm)

	ctx := context.Background()
	tm.ethClient.EXPECT().
		FetchOwnerLogsWindow(ctx, "0xowner", uint64(100), uint64(199)).
		Return(nil, errors.New("rpc down"))

	rows, err := tm.executor.FetchEthereumOwnerWindow(ctx, "0xowner", 100, 199)
	assert.ErrorContains(t, err, "rpc down")
	assert.Nil(t, rows)
}

// TestReplayEthereumScanSession_RoundTripsLogsAndPersistsTokens pins the replay
// handoff: staged rows are restored to go-ethereum log shape (field-for-field),
// the discovered tokens are persisted via FinishAddressScanReplay, and the
// token count is reported.
func TestReplayEthereumScanSession_RoundTripsLogsAndPersistsTokens(t *testing.T) {
	tm := setupTestExecutor(t)
	defer tearDownTestExecutor(tm)

	ctx := context.Background()
	address := "0xdadB0d80178819F2319190D340ce9A924f783711"
	const sessionID int64 = 7

	row := schema.AddressScanLog{
		SessionID:      sessionID,
		BlockNumber:    150,
		TxHash:         common.HexToHash("0xdead").Hex(),
		LogIndex:       9,
		Address:        common.HexToAddress("0x1234567890123456789012345678901234567890").Hex(),
		Topics:         []string{common.HexToHash("0xaaaa").Hex()},
		Data:           common.LeftPadBytes(big.NewInt(42).Bytes(), 32),
		TxIndex:        3,
		BlockHash:      common.HexToHash("0xbeef").Hex(),
		BlockTimestamp: 1_700_000_000,
	}
	tm.store.EXPECT().GetAddressScanLogs(ctx, sessionID).Return([]schema.AddressScanLog{row}, nil)

	discovered := []domain.TokenWithBlock{
		{TokenCID: domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1"), BlockNumber: 150},
	}
	tm.ethClient.EXPECT().
		DiscoverOwnedTokensFromLogs(ctx, address, gomock.Any(), tm.blacklist).
		DoAndReturn(func(_ context.Context, _ string, logs []ethtypes.Log, _ interface{}) ([]domain.TokenWithBlock, error) {
			require.Len(t, logs, 1)
			vLog := logs[0]
			assert.Equal(t, uint64(150), vLog.BlockNumber)
			assert.Equal(t, common.HexToHash("0xdead"), vLog.TxHash)
			assert.Equal(t, uint(9), vLog.Index)
			assert.Equal(t, common.HexToAddress("0x1234567890123456789012345678901234567890"), vLog.Address)
			assert.Equal(t, []common.Hash{common.HexToHash("0xaaaa")}, vLog.Topics)
			assert.Equal(t, row.Data, vLog.Data)
			assert.Equal(t, uint(3), vLog.TxIndex)
			assert.Equal(t, common.HexToHash("0xbeef"), vLog.BlockHash)
			assert.Equal(t, uint64(1_700_000_000), vLog.BlockTimestamp, "restored logs keep the staged block time so replay needs no block-provider lookup")
			return discovered, nil
		})

	tm.store.EXPECT().
		FinishAddressScanReplay(ctx, sessionID, gomock.Any()).
		DoAndReturn(func(_ context.Context, _ int64, tokens []schema.AddressScanToken) error {
			require.Len(t, tokens, 1)
			assert.Equal(t, discovered[0].TokenCID, tokens[0].TokenCID)
			assert.Equal(t, uint64(150), tokens[0].BlockNumber)
			return nil
		})

	count, err := tm.executor.ReplayEthereumScanSession(ctx, address, sessionID)
	assert.NoError(t, err)
	assert.Equal(t, 1, count)
}

// TestGetEthereumScanSession_MapsSchemaToInfo verifies the workflow-facing view:
// nil for no session, and the Replayed flag derived from the schema status.
func TestGetEthereumScanSession_MapsSchemaToInfo(t *testing.T) {
	tm := setupTestExecutor(t)
	defer tearDownTestExecutor(tm)

	ctx := context.Background()
	address := "0xdadB0d80178819F2319190D340ce9A924f783711"
	chain := domain.ChainEthereumMainnet

	tm.store.EXPECT().GetAddressScanSession(ctx, chain, address).Return(nil, nil)
	info, err := tm.executor.GetEthereumScanSession(ctx, address, chain)
	assert.NoError(t, err)
	assert.Nil(t, info)

	tm.store.EXPECT().GetAddressScanSession(ctx, chain, address).Return(&schema.AddressScanSession{
		ID: 7, FromBlock: 10, ToBlock: 99, CursorBlock: 50,
		Status: schema.AddressScanStatusReplayed,
	}, nil)
	info, err = tm.executor.GetEthereumScanSession(ctx, address, chain)
	assert.NoError(t, err)
	require.NotNil(t, info)
	assert.Equal(t, int64(7), info.ID)
	assert.Equal(t, uint64(10), info.FromBlock)
	assert.Equal(t, uint64(99), info.ToBlock)
	assert.Equal(t, uint64(50), info.CursorBlock)
	assert.True(t, info.Replayed)
}

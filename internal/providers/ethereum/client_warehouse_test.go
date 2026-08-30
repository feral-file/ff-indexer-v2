package ethereum_test

import (
	"context"
	"errors"
	"math/big"
	"testing"

	goeth "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	ethereum "github.com/feral-file/ff-indexer-v2/internal/providers/ethereum"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/helpers"
)

// TestCheckLogWarehouseChain pins the startup contract: a matching chain
// passes, another chain is a fatal mismatch, an unreachable warehouse is the
// distinct non-fatal sentinel, and a non-eip155 chain can never match.
func TestCheckLogWarehouseChain(t *testing.T) {
	t.Parallel()

	t.Run("matching chain passes", func(t *testing.T) {
		t.Parallel()
		wh := mocks.NewMockLogWarehouse(gomock.NewController(t))
		wh.EXPECT().ChainID(gomock.Any()).Return(big.NewInt(1), nil)
		require.NoError(t, ethereum.CheckLogWarehouseChain(context.Background(), wh, domain.ChainEthereumMainnet))
	})
	t.Run("other chain is a mismatch", func(t *testing.T) {
		t.Parallel()
		wh := mocks.NewMockLogWarehouse(gomock.NewController(t))
		wh.EXPECT().ChainID(gomock.Any()).Return(big.NewInt(11155111), nil)
		err := ethereum.CheckLogWarehouseChain(context.Background(), wh, domain.ChainEthereumMainnet)
		require.ErrorIs(t, err, ethereum.ErrLogWarehouseChainMismatch)
		require.ErrorContains(t, err, "11155111")
		require.ErrorContains(t, err, "eip155:1")
	})
	t.Run("unreachable is the non-fatal sentinel", func(t *testing.T) {
		t.Parallel()
		wh := mocks.NewMockLogWarehouse(gomock.NewController(t))
		wh.EXPECT().ChainID(gomock.Any()).Return(nil, errors.New("connection refused"))
		err := ethereum.CheckLogWarehouseChain(context.Background(), wh, domain.ChainEthereumMainnet)
		require.ErrorIs(t, err, ethereum.ErrLogWarehouseUnreachable)
		require.NotErrorIs(t, err, ethereum.ErrLogWarehouseChainMismatch)
	})
	t.Run("non-eip155 chain never matches", func(t *testing.T) {
		t.Parallel()
		wh := mocks.NewMockLogWarehouse(gomock.NewController(t))
		err := ethereum.CheckLogWarehouseChain(context.Background(), wh, domain.ChainTezosMainnet)
		require.ErrorIs(t, err, ethereum.ErrLogWarehouseChainMismatch)
	})
}

// TestLogWarehouseHead pins the planning hook: no warehouse → (0, false); a
// warehouse that answers → its head; one that fails → (0, false), never an
// error that would abort the caller.
func TestLogWarehouseHead(t *testing.T) {
	t.Parallel()

	t.Run("no warehouse configured", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		client, err := ethereum.NewGuardedClient(domain.ChainEthereumMainnet, mocks.NewMockEthClient(ctrl), mocks.NewMockClock(ctrl), nil, ethereum.ClientGuards{})
		require.NoError(t, err)
		head, ok := client.LogWarehouseHead(context.Background())
		require.False(t, ok)
		require.Zero(t, head)
	})
	t.Run("warehouse answers", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		wh := mocks.NewMockLogWarehouse(ctrl)
		wh.EXPECT().Head(gomock.Any()).Return(uint64(25_000_000), nil)
		client, err := ethereum.NewGuardedClient(domain.ChainEthereumMainnet, mocks.NewMockEthClient(ctrl), mocks.NewMockClock(ctrl), nil, ethereum.ClientGuards{LogWarehouse: wh})
		require.NoError(t, err)
		head, ok := client.LogWarehouseHead(context.Background())
		require.True(t, ok)
		require.Equal(t, uint64(25_000_000), head)
	})
	t.Run("warehouse fails", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		wh := mocks.NewMockLogWarehouse(ctrl)
		wh.EXPECT().Head(gomock.Any()).Return(uint64(0), errors.New("connection refused"))
		client, err := ethereum.NewGuardedClient(domain.ChainEthereumMainnet, mocks.NewMockEthClient(ctrl), mocks.NewMockClock(ctrl), nil, ethereum.ClientGuards{LogWarehouse: wh})
		require.NoError(t, err)
		_, ok := client.LogWarehouseHead(context.Background())
		require.False(t, ok)
	})
}

// TestFetchIngestionLogs_RoutesThroughWarehouse pins that chain ingestion's
// per-block fetch takes the warehouse leg like every other walk: a batch at
// or below the warehouse head is one warehouse query with the ingestion
// filter, and the vendor is not asked (strict mock, no FilterLogs).
func TestFetchIngestionLogs_RoutesThroughWarehouse(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	vendor := mocks.NewMockEthClient(ctrl)
	wh := mocks.NewMockLogWarehouse(ctrl)
	wh.EXPECT().Head(gomock.Any()).Return(uint64(1_010), nil)
	wh.EXPECT().FilterLogs(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, q goeth.FilterQuery) ([]types.Log, error) {
			require.Equal(t, uint64(1_000), q.FromBlock.Uint64())
			require.Equal(t, uint64(1_009), q.ToBlock.Uint64())
			require.Len(t, q.Topics, 1)
			require.Contains(t, q.Topics[0], helpers.TransferEventSignature)
			return []types.Log{
				{BlockNumber: 1_005, Index: 2, Topics: []common.Hash{helpers.TransferEventSignature}},
				{BlockNumber: 1_001, Index: 0, Topics: []common.Hash{helpers.TransferEventSignature}},
			}, nil
		})
	client, err := ethereum.NewGuardedClient(domain.ChainEthereumMainnet, vendor, mocks.NewMockClock(ctrl), nil, ethereum.ClientGuards{
		GetLogsSpanCap: 10_000,
		LogWarehouse:   wh,
	})
	require.NoError(t, err)

	logs, err := client.FetchIngestionLogs(context.Background(), 1_000, 1_009)
	require.NoError(t, err)
	require.Equal(t, []uint64{1_001, 1_005}, []uint64{logs[0].BlockNumber, logs[1].BlockNumber}, "ordering contract holds on warehouse logs too")
}

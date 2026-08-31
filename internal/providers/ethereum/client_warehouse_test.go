package ethereum_test

import (
	"context"
	"errors"
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

// TestLogWarehouseRequirements pins what a warehouse must prove per chain:
// mainnet needs the chain id AND the CryptoPunks internal-Transfer probe (the
// owner scan's corrupted-PunkBought repair depends on that 3-topic log, which
// an "ERC-20-shaped Transfers are dropped" warehouse build would omit
// silently); a testnet needs only its chain id; a non-eip155 chain is refused.
func TestLogWarehouseRequirements(t *testing.T) {
	t.Parallel()

	t.Run("mainnet probes the CryptoPunks internal Transfer", func(t *testing.T) {
		t.Parallel()
		reqs, err := ethereum.LogWarehouseRequirements(domain.ChainEthereumMainnet)
		require.NoError(t, err)
		require.Equal(t, uint64(1), reqs.ChainID)
		require.Len(t, reqs.Probes, 1)
		probe := reqs.Probes[0]
		punks := common.HexToAddress("0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb")
		require.Equal(t, []common.Address{punks}, probe.Query.Addresses)
		require.Equal(t, [][]common.Hash{{helpers.TransferEventSignature}}, probe.Query.Topics)
		require.Equal(t, probe.Query.FromBlock, probe.Query.ToBlock, "a single-block probe")
		require.Equal(t, int64(3_919_706), probe.Query.FromBlock.Int64())

		require.False(t, probe.Accept(nil), "no logs: the shape is missing")
		require.False(t, probe.Accept([]types.Log{{Address: punks, Topics: make([]common.Hash, 4)}}), "a 4-topic Transfer is not the internal one")
		require.False(t, probe.Accept([]types.Log{{Address: common.HexToAddress("0x1"), Topics: make([]common.Hash, 3)}}), "another emitter does not count")
		require.True(t, probe.Accept([]types.Log{{Address: punks, Topics: make([]common.Hash, 3)}}))
	})
	t.Run("testnet needs only the chain id", func(t *testing.T) {
		t.Parallel()
		reqs, err := ethereum.LogWarehouseRequirements(domain.ChainEthereumSepolia)
		require.NoError(t, err)
		require.Equal(t, uint64(11155111), reqs.ChainID)
		require.Empty(t, reqs.Probes)
	})
	t.Run("non-eip155 chain is refused", func(t *testing.T) {
		t.Parallel()
		_, err := ethereum.LogWarehouseRequirements(domain.ChainTezosMainnet)
		require.Error(t, err)
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
	wh.EXPECT().FilterLogs(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, q goeth.FilterQuery, _ *common.Hash) ([]types.Log, error) {
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

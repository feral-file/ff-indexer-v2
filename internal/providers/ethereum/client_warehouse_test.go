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
		require.Len(t, reqs.Probes, 3)
		probe := reqs.Probes[0]
		punks := common.HexToAddress("0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb")
		require.Equal(t, []common.Address{punks}, probe.Query.Addresses)
		require.Equal(t, [][]common.Hash{{helpers.TransferEventSignature}}, probe.Query.Topics)
		require.Equal(t, probe.Query.FromBlock, probe.Query.ToBlock, "a single-block probe")
		require.Equal(t, int64(3_919_706), probe.Query.FromBlock.Int64())
		require.Nil(t, probe.ERC1155ID, "the CryptoPunks probe sends a standard filter")

		require.False(t, probe.Accept(nil), "no logs: the shape is missing")
		require.False(t, probe.Accept([]types.Log{{Address: punks, Topics: make([]common.Hash, 4)}}), "a 4-topic Transfer is not the internal one")
		require.False(t, probe.Accept([]types.Log{{Address: common.HexToAddress("0x1"), Topics: make([]common.Hash, 3)}}), "another emitter does not count")
		require.True(t, probe.Accept([]types.Log{{Address: punks, Topics: make([]common.Hash, 3)}}))

		// The erc1155Id capability probe: single block, storefront, TransferSingle,
		// with the token id set, accepting only when every log carries that id.
		idProbe := reqs.Probes[1]
		require.Equal(t, "ERC-1155 erc1155Id filter", idProbe.Name)
		require.Equal(t, []common.Address{common.HexToAddress("0x495f947276749Ce646f68AC8c248420045cb7b5e")}, idProbe.Query.Addresses)
		require.Equal(t, [][]common.Hash{{helpers.ERC1155TransferSingleEventSignature}}, idProbe.Query.Topics)
		require.Equal(t, idProbe.Query.FromBlock, idProbe.Query.ToBlock, "a single-block probe")
		require.Equal(t, int64(14_048_809), idProbe.Query.FromBlock.Int64())
		require.NotNil(t, idProbe.ERC1155ID, "the id must be sent so the probe tests the filter")
		id := *idProbe.ERC1155ID
		match := types.Log{Data: append(append([]byte{}, id.Bytes()...), make([]byte, 32)...)}
		foreign := types.Log{Data: append(append([]byte{}, common.HexToHash("0x99").Bytes()...), make([]byte, 32)...)}
		require.False(t, idProbe.Accept(nil), "empty: the filter dropped everything or is unsupported")
		require.True(t, idProbe.Accept([]types.Log{match, match}), "the block's two matching logs")
		require.False(t, idProbe.Accept([]types.Log{match}), "only one of the two matching logs: a partial index is rejected")
		require.False(t, idProbe.Accept([]types.Log{match, foreign}), "a sibling token proves the filter was ignored")
		require.False(t, idProbe.Accept([]types.Log{foreign}), "a foreign token id alone is rejected")
		require.False(t, idProbe.Accept([]types.Log{{Data: []byte{0x01}}}), "a truncated data field is rejected")

		// The URI arm probe: single block, a URI-emitting contract, URI topic,
		// id set; URI carries the token id in topic1, not data.
		uriProbe := reqs.Probes[2]
		require.Equal(t, "ERC-1155 URI erc1155Id filter", uriProbe.Name)
		require.Equal(t, [][]common.Hash{{helpers.ERC1155URIEventSignature}}, uriProbe.Query.Topics)
		require.Equal(t, uriProbe.Query.FromBlock, uriProbe.Query.ToBlock, "a single-block probe")
		require.Equal(t, int64(6_938_761), uriProbe.Query.FromBlock.Int64())
		require.NotNil(t, uriProbe.ERC1155ID)
		uid := *uriProbe.ERC1155ID
		uriMatch := types.Log{Topics: []common.Hash{helpers.ERC1155URIEventSignature, uid}}
		uriForeign := types.Log{Topics: []common.Hash{helpers.ERC1155URIEventSignature, common.HexToHash("0x99")}}
		require.False(t, uriProbe.Accept(nil), "empty: dropped or unsupported")
		require.True(t, uriProbe.Accept([]types.Log{uriMatch}), "the block's one matching URI")
		require.False(t, uriProbe.Accept([]types.Log{uriMatch, uriMatch}), "more than the one expected URI is rejected")
		require.False(t, uriProbe.Accept([]types.Log{uriMatch, uriForeign}), "a sibling URI proves the filter was ignored")
		require.False(t, uriProbe.Accept([]types.Log{{Topics: []common.Hash{helpers.ERC1155URIEventSignature}}}), "a URI without topic1 is rejected")
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

// TestFetchIngestionLogs_StrictWarehouseDenseBlockRecoversViaReceipts is the
// regression for the strict default (LogWarehouseVendorFallthrough false):
// when a warehouse block exceeds the warehouse result cap, the bisection now
// raises a SingleBlockOverflowError (not a generic wrapped outage error), so
// FetchIngestionLogs's receipt recovery still fires and ingestion advances past
// the dense block. The neighbors are served by the warehouse, the dense block
// by its receipts, and no vendor eth_getLogs is ever issued.
func TestFetchIngestionLogs_StrictWarehouseDenseBlockRecoversViaReceipts(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	vendor := mocks.NewMockEthClient(ctrl)
	wh := mocks.NewMockLogWarehouse(ctrl)

	// Warehouse covers the whole range; block 101 is over its result cap.
	wh.EXPECT().Head(gomock.Any()).Return(uint64(1_000_000), nil).AnyTimes()
	capErr := errors.New("query returned more than 100000 results")
	wh.EXPECT().FilterLogs(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, q goeth.FilterQuery, _ *common.Hash) ([]types.Log, error) {
			from, to := q.FromBlock.Uint64(), q.ToBlock.Uint64()
			if from <= 101 && 101 <= to {
				return nil, capErr // any window touching block 101 is over the cap
			}
			var logs []types.Log
			for b := from; b <= to; b++ {
				logs = append(logs, types.Log{BlockNumber: b, Index: 0, Topics: []common.Hash{helpers.TransferEventSignature}})
			}
			return logs, nil
		}).AnyTimes()

	// The dense block is read from receipts (the vendor) — a single bounded
	// call, never an eth_getLogs walk; the strict vendor mock has no FilterLogs
	// expectation, so any warehouse-outage fall-through would fail the test.
	unrelated := common.HexToHash("0x1111")
	vendor.EXPECT().BlockReceipts(gomock.Any(), big.NewInt(101)).
		Return([]*types.Receipt{
			{Logs: []*types.Log{
				{BlockNumber: 101, Index: 0, Topics: []common.Hash{unrelated}},
				{BlockNumber: 101, Index: 1, Topics: []common.Hash{helpers.TransferEventSignature, {}, {}}},
			}},
			{Logs: []*types.Log{
				{BlockNumber: 101, Index: 2, Topics: []common.Hash{helpers.ERC1155TransferSingleEventSignature}},
			}},
		}, nil)

	client, err := ethereum.NewGuardedClient(domain.ChainEthereumMainnet, vendor, mocks.NewMockClock(ctrl), nil, ethereum.ClientGuards{
		GetLogsSpanCap: 10_000,
		LogWarehouse:   wh,
		// LogWarehouseVendorFallthrough left false: the strict default.
	})
	require.NoError(t, err)

	logs, err := client.FetchIngestionLogs(context.Background(), 100, 102)
	require.NoError(t, err)
	var got [][2]uint64
	for _, l := range logs {
		got = append(got, [2]uint64{l.BlockNumber, uint64(l.Index)})
	}
	require.Equal(t, [][2]uint64{{100, 0}, {101, 1}, {101, 2}, {102, 0}}, got,
		"neighbors via the warehouse, dense block via receipts, in chain order — even in strict mode")
}

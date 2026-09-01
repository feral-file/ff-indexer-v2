package adapters_test

import (
	"context"
	"errors"
	"math/big"
	"testing"
	"time"

	ethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	ethadapter "github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/adapters"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/helpers"
)

// erc1155Log builds a log with the given topics and data. BlockTimestamp is set
// so valid-shape cases resolve their timestamp without a provider.
func erc1155Log(topics []common.Hash, data []byte) types.Log {
	return types.Log{
		Address:        common.HexToAddress("0x0000000000000000000000000000000000000002"),
		BlockNumber:    25845883,
		BlockTimestamp: 1_700_000_000,
		Topics:         topics,
		Data:           data,
	}
}

func TestERC1155ParseEvent_TransferSingle(t *testing.T) {
	t.Parallel()

	operator := common.HexToAddress("0x3333333333333333333333333333333333333333")
	from := common.HexToAddress("0x1111111111111111111111111111111111111111")
	to := common.HexToAddress("0x2222222222222222222222222222222222222222")
	data := append(common.BigToHash(big.NewInt(7)).Bytes(), common.BigToHash(big.NewInt(3)).Bytes()...)

	vLog := erc1155Log([]common.Hash{
		helpers.ERC1155TransferSingleEventSignature,
		common.BytesToHash(operator.Bytes()),
		common.BytesToHash(from.Bytes()),
		common.BytesToHash(to.Bytes()),
	}, data)

	adapter := adapters.NewERC1155Adapter(nil, nil, domain.ChainEthereumMainnet, nil, false)
	parsed, err := adapter.ParseEvent(context.Background(), vLog)
	require.NoError(t, err)
	require.NotNil(t, parsed)
	require.Equal(t, domain.StandardERC1155, parsed.Standard)
	require.Equal(t, "7", parsed.TokenNumber)
	require.Equal(t, "3", parsed.Quantity)
}

// TestERC1155ParseEvent_MalformedSkippedBeforeTimestampLookup pins the same
// contract the ERC-721 adapter carries: a log whose shape does not match the
// standard event its topic0 claims is dropped before the block-timestamp
// lookup, never failed — a fatal parse error replays from the durable cursor
// and crash-loops ingestion, and the whole-chain topic0 filter lets any
// contract emit such a log.
func TestERC1155ParseEvent_MalformedSkippedBeforeTimestampLookup(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		topics []common.Hash
		data   []byte
	}{
		{"TransferSingle with one topic", []common.Hash{helpers.ERC1155TransferSingleEventSignature}, make([]byte, 128)},
		{"TransferSingle with three topics", []common.Hash{
			helpers.ERC1155TransferSingleEventSignature,
			common.BytesToHash(common.HexToAddress("0x3333333333333333333333333333333333333333").Bytes()),
			common.BytesToHash(common.HexToAddress("0x1111111111111111111111111111111111111111").Bytes()),
		}, make([]byte, 128)},
		{"TransferSingle with short data", []common.Hash{
			helpers.ERC1155TransferSingleEventSignature,
			common.BytesToHash(common.HexToAddress("0x3333333333333333333333333333333333333333").Bytes()),
			common.BytesToHash(common.HexToAddress("0x1111111111111111111111111111111111111111").Bytes()),
			common.BytesToHash(common.HexToAddress("0x2222222222222222222222222222222222222222").Bytes()),
		}, make([]byte, 32)},
		{"URI with one topic", []common.Hash{helpers.ERC1155URIEventSignature}, nil},
		{"URI with three topics", []common.Hash{
			helpers.ERC1155URIEventSignature,
			common.BigToHash(big.NewInt(1)),
			common.BigToHash(big.NewInt(2)),
		}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			blockProvider := mocks.NewMockBlockProvider(ctrl)
			blockProvider.EXPECT().
				GetBlockTimestamp(gomock.Any(), gomock.Any()).
				Return(time.Time{}, errors.New("block provider unavailable")).
				AnyTimes()

			vLog := erc1155Log(tc.topics, tc.data)
			vLog.BlockTimestamp = 0 // force the provider path if a lookup were attempted

			adapter := adapters.NewERC1155Adapter(nil, nil, domain.ChainEthereumMainnet, blockProvider, false)
			parsed, err := adapter.ParseEvent(context.Background(), vLog)
			require.NoError(t, err)
			require.Nil(t, parsed)
		})
	}
}

func TestERC1155ParseEvent_URI(t *testing.T) {
	t.Parallel()

	vLog := erc1155Log([]common.Hash{
		helpers.ERC1155URIEventSignature,
		common.BigToHash(big.NewInt(9)),
	}, nil)

	adapter := adapters.NewERC1155Adapter(nil, nil, domain.ChainEthereumMainnet, nil, false)
	parsed, err := adapter.ParseEvent(context.Background(), vLog)
	require.NoError(t, err)
	require.NotNil(t, parsed)
	require.Equal(t, domain.EventTypeMetadataUpdate, parsed.EventType)
	require.Equal(t, "9", parsed.TokenNumber)
}

func TestERC1155ParseEvent_UnknownSignature(t *testing.T) {
	t.Parallel()

	vLog := erc1155Log([]common.Hash{common.HexToHash("0xdeadbeef")}, nil)
	adapter := adapters.NewERC1155Adapter(nil, nil, domain.ChainEthereumMainnet, nil, false)
	parsed, err := adapter.ParseEvent(context.Background(), vLog)
	require.ErrorIs(t, err, adapters.ErrUnknownEvent)
	require.Nil(t, parsed)
}

// TestERC1155GetTokenEvents_RoutesTokenIDToWarehouse pins the indexer end of
// the ERC-1155 point-lookup: GetTokenEvents derives the 32-byte id from the
// decimal token number and forwards it as the warehouse hint, so a per-token
// history walk is served by the warehouse index rather than a whole-contract
// scan. The vendor mock has no expectation: the warehouse serves the range.
func TestERC1155GetTokenEvents_RoutesTokenIDToWarehouse(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	wh := mocks.NewMockLogWarehouse(ctrl)
	mockBlock := mocks.NewMockBlockProvider(ctrl)
	vendor := mocks.NewMockEthClient(ctrl) // never called

	const head = uint64(1_000)
	mockBlock.EXPECT().GetLatestBlock(gomock.Any()).Return(head, nil).AnyTimes()
	wh.EXPECT().Head(gomock.Any()).Return(head, nil)

	contract := common.HexToAddress("0x495f947276749ce646f68ac8c248420045cb7b5e")
	wantID := common.BigToHash(big.NewInt(42))
	value := common.BigToHash(big.NewInt(1))
	tsLog := types.Log{
		Address:        contract,
		Topics:         []common.Hash{helpers.ERC1155TransferSingleEventSignature, {}, {}, {}},
		Data:           append(append([]byte{}, wantID.Bytes()...), value.Bytes()...),
		BlockNumber:    5,
		BlockTimestamp: 1_700_000_000, // set so parsing needs no block lookup
		TxHash:         common.HexToHash("0xabc"),
		Index:          0,
	}
	wh.EXPECT().FilterLogs(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ ethereum.FilterQuery, id *common.Hash) ([]types.Log, error) {
			require.NotNil(t, id, "the token id must reach the warehouse")
			require.Equal(t, wantID, *id)
			return []types.Log{tsLog}, nil
		})

	pagination := helpers.NewGuardedPaginationHelper(vendor, ethadapter.NewClock(), mockBlock,
		helpers.PaginationGuards{LogWarehouse: wh})
	adapter := adapters.NewERC1155Adapter(vendor, pagination, domain.ChainEthereumMainnet, mockBlock, true)

	events, err := adapter.GetTokenEvents(context.Background(), contract.Hex(), "42")
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, "42", events[0].TokenNumber)
}

// TestERC1155GetTokenEvents_NoHintWhenUnverified pins the gate: when the chain's
// warehouse is not verified for the erc1155Id filter (warehouseTokenFilter
// false), GetTokenEvents sends no hint — the warehouse gets a nil id and returns
// the whole contract, which the client-side token-id filter then narrows.
func TestERC1155GetTokenEvents_NoHintWhenUnverified(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	wh := mocks.NewMockLogWarehouse(ctrl)
	mockBlock := mocks.NewMockBlockProvider(ctrl)
	vendor := mocks.NewMockEthClient(ctrl)

	const head = uint64(1_000)
	mockBlock.EXPECT().GetLatestBlock(gomock.Any()).Return(head, nil).AnyTimes()
	wh.EXPECT().Head(gomock.Any()).Return(head, nil)

	contract := common.HexToAddress("0x495f947276749ce646f68ac8c248420045cb7b5e")
	value := common.BigToHash(big.NewInt(1))
	tsLog := types.Log{
		Address:        contract,
		Topics:         []common.Hash{helpers.ERC1155TransferSingleEventSignature, {}, {}, {}},
		Data:           append(append([]byte{}, common.BigToHash(big.NewInt(42)).Bytes()...), value.Bytes()...),
		BlockNumber:    5,
		BlockTimestamp: 1_700_000_000,
		TxHash:         common.HexToHash("0xabc"),
	}
	wh.EXPECT().FilterLogs(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ ethereum.FilterQuery, id *common.Hash) ([]types.Log, error) {
			require.Nil(t, id, "no hint when the capability is unverified")
			return []types.Log{tsLog}, nil
		})

	pagination := helpers.NewGuardedPaginationHelper(vendor, ethadapter.NewClock(), mockBlock,
		helpers.PaginationGuards{LogWarehouse: wh})
	adapter := adapters.NewERC1155Adapter(vendor, pagination, domain.ChainEthereumMainnet, mockBlock, false)

	events, err := adapter.GetTokenEvents(context.Background(), contract.Hex(), "42")
	require.NoError(t, err)
	require.Len(t, events, 1)
}

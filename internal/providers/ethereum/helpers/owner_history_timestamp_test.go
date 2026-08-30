package helpers_test

import (
	"context"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/helpers"
)

const stampedBlockTime = uint64(1_700_000_000)

func word(v *big.Int) []byte { return common.LeftPadBytes(v.Bytes(), 32) }

// transferBatchLog builds a TransferBatch log for one (id, value) pair.
func transferBatchLog(owner common.Address, id, value *big.Int, stamped bool) types.Log {
	data := append([]byte{}, word(big.NewInt(64))...) // offset of ids
	data = append(data, word(big.NewInt(128))...)     // offset of values
	data = append(data, word(big.NewInt(1))...)       // len(ids)
	data = append(data, word(id)...)                  // ids[0]
	data = append(data, word(big.NewInt(1))...)       // len(values)
	data = append(data, word(value)...)               // values[0]
	l := types.Log{
		Address:     common.HexToAddress("0xc0"),
		Topics:      []common.Hash{helpers.ERC1155TransferBatchEventSignature, common.HexToHash("0x0"), common.BytesToHash(owner.Bytes()), common.HexToHash("0x2")},
		Data:        data,
		BlockNumber: 42,
		TxHash:      common.HexToHash("0xt1"),
		Index:       3,
	}
	if stamped {
		l.BlockTimestamp = stampedBlockTime
	}
	return l
}

// transferSingleLog builds a TransferSingle log to the owner.
func transferSingleLog(owner common.Address, id, value *big.Int, stamped bool) types.Log {
	l := types.Log{
		Address:     common.HexToAddress("0xc0"),
		Topics:      []common.Hash{helpers.ERC1155TransferSingleEventSignature, common.HexToHash("0x0"), common.HexToHash("0x1"), common.BytesToHash(owner.Bytes())},
		Data:        append(word(id), word(value)...),
		BlockNumber: 42,
		TxHash:      common.HexToHash("0xt2"),
		Index:       1,
	}
	if stamped {
		l.BlockTimestamp = stampedBlockTime
	}
	return l
}

// TestParseERC1155TransferBatch_PrefersOnLogTimestamp pins round-4 F2 for the
// batch shape: a warehouse-stamped log yields its own block time and never
// asks the block provider (strict mock, no GetBlockTimestamp expectation);
// an unstamped log keeps the provider lookup and its current-time fallback.
func TestParseERC1155TransferBatch_PrefersOnLogTimestamp(t *testing.T) {
	t.Parallel()
	owner := common.HexToAddress("0xaa")
	now := time.Unix(1_800_000_000, 0)
	fallbackNow := func() time.Time { return now }

	t.Run("stamped log needs no provider", func(t *testing.T) {
		t.Parallel()
		provider := mocks.NewMockBlockProvider(gomock.NewController(t))
		events, err := helpers.ParseERC1155TransferBatch(context.Background(), domain.ChainEthereumMainnet,
			transferBatchLog(owner, big.NewInt(7), big.NewInt(2), true), big.NewInt(7), provider, fallbackNow)
		require.NoError(t, err)
		require.Len(t, events, 1)
		require.Equal(t, time.Unix(int64(stampedBlockTime), 0), events[0].Timestamp)
	})
	t.Run("unstamped log falls back to now when the provider fails", func(t *testing.T) {
		t.Parallel()
		provider := mocks.NewMockBlockProvider(gomock.NewController(t))
		provider.EXPECT().GetBlockTimestamp(gomock.Any(), uint64(42)).Return(time.Time{}, errors.New("vendor down"))
		events, err := helpers.ParseERC1155TransferBatch(context.Background(), domain.ChainEthereumMainnet,
			transferBatchLog(owner, big.NewInt(7), big.NewInt(2), false), big.NewInt(7), provider, fallbackNow)
		require.NoError(t, err)
		require.Len(t, events, 1)
		require.Equal(t, now, events[0].Timestamp)
	})
}

// TestERC1155BalanceAndEventsForOwner_PrefersOnLogTimestamp pins round-4 F2
// for the single shape through the real owner-history path: with every log
// stamped, the block provider is asked for the latest block only — a failing
// GetBlockTimestamp would be an unexpected call on the strict mock — and the
// events carry the on-log time, not "now".
func TestERC1155BalanceAndEventsForOwner_PrefersOnLogTimestamp(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	owner := common.HexToAddress("0xaa")
	ethClient := mocks.NewMockEthClient(ctrl)
	provider := mocks.NewMockBlockProvider(ctrl)
	clock := mocks.NewMockClock(ctrl)

	ethClient.EXPECT().CallContract(gomock.Any(), gomock.Any(), gomock.Any()).Return(word(big.NewInt(2)), nil) // balanceOf
	provider.EXPECT().GetLatestBlock(gomock.Any()).Return(uint64(100), nil).AnyTimes()
	ethClient.EXPECT().FilterLogs(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
			// Only the "TransferSingle to owner" query (owner in topic 3) matches.
			if q.Topics[0][0] == helpers.ERC1155TransferSingleEventSignature && len(q.Topics) == 4 {
				return []types.Log{transferSingleLog(owner, big.NewInt(7), big.NewInt(2), true)}, nil
			}
			return nil, nil
		}).Times(4)

	pagination := helpers.NewPaginationHelper(ethClient, clock, provider)
	balance, events, err := helpers.ERC1155BalanceAndEventsForOwner(context.Background(), ethClient, pagination, provider,
		domain.ChainEthereumMainnet, func() time.Time { return time.Unix(1_800_000_000, 0) },
		"0xc0", "7", owner.Hex())
	require.NoError(t, err)
	require.Equal(t, "2", balance)
	require.Len(t, events, 1)
	require.Equal(t, time.Unix(int64(stampedBlockTime), 0), events[0].Timestamp, "the warehouse-stamped time, not the fallback")
}

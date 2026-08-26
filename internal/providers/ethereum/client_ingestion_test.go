package ethereum_test

import (
	"context"
	"errors"
	"math/big"
	"testing"

	goethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	ethereum "github.com/feral-file/ff-indexer-v2/internal/providers/ethereum"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/helpers"
)

func newIngestionClient(t *testing.T, ctrl *gomock.Controller) (ethereum.EthereumClient, *mocks.MockEthClient) {
	t.Helper()
	mockEth := mocks.NewMockEthClient(ctrl)
	mockClock := mocks.NewMockClock(ctrl)
	mockClock.EXPECT().Sleep(gomock.Any()).AnyTimes()
	client, err := ethereum.NewGuardedClient(domain.ChainEthereumMainnet, mockEth, mockClock, nil, ethereum.ClientGuards{})
	require.NoError(t, err)
	return client, mockEth
}

func blockRange(q goethereum.FilterQuery) (uint64, uint64) {
	return q.FromBlock.Uint64(), q.ToBlock.Uint64()
}

// TestFetchIngestionLogs_UsesFullTopicFilterAndSortsByBlockIndex pins the
// selection contract: one query, no address scope, topic0 = standard NFT
// signatures plus the registry's custom signatures (CryptoPunks on mainnet),
// and the result ordered by (block, log index) whatever the provider returned.
func TestFetchIngestionLogs_UsesFullTopicFilterAndSortsByBlockIndex(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	client, mockEth := newIngestionClient(t, ctrl)

	punkTransfer := common.HexToHash("0x05af636b70da6819000c49f85b21fa82081c632069bb626f30932034099107d8") // PunkTransfer(address,address,uint256)
	mockEth.EXPECT().
		FilterLogs(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, q goethereum.FilterQuery) ([]types.Log, error) {
			from, to := blockRange(q)
			require.Equal(t, uint64(100), from)
			require.Equal(t, uint64(105), to)
			require.Nil(t, q.Addresses, "ingestion is chain-wide, never address-scoped")
			require.Len(t, q.Topics, 1, "topic0-only filter")
			require.Contains(t, q.Topics[0], helpers.TransferEventSignature)
			require.Contains(t, q.Topics[0], helpers.ERC1155TransferSingleEventSignature)
			require.Contains(t, q.Topics[0], helpers.EIP4906MetadataUpdateEventSignature)
			require.Contains(t, q.Topics[0], punkTransfer, "registry custom signatures must be included")
			return []types.Log{
				{BlockNumber: 103, Index: 2},
				{BlockNumber: 101, Index: 9},
				{BlockNumber: 103, Index: 0},
			}, nil
		})

	logs, err := client.FetchIngestionLogs(context.Background(), 100, 105)
	require.NoError(t, err)
	require.Equal(t, []types.Log{{BlockNumber: 101, Index: 9}, {BlockNumber: 103, Index: 0}, {BlockNumber: 103, Index: 2}}, logs)
}

// TestFetchIngestionLogs_DenseBlockFallsBackToReceipts pins the result-cap
// path: when the provider refuses a single block ("query returned more than
// 10000 results" even for one block), that block is read from its receipts
// with the identical topic filter and spliced in order between the blocks on
// either side, instead of failing the subscription.
func TestFetchIngestionLogs_DenseBlockFallsBackToReceipts(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	client, mockEth := newIngestionClient(t, ctrl)

	capErr := errors.New("query returned more than 10000 results")
	mockEth.EXPECT().
		FilterLogs(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, q goethereum.FilterQuery) ([]types.Log, error) {
			from, to := blockRange(q)
			if from <= 101 && 101 <= to {
				return nil, capErr // any window touching block 101 is over the cap
			}
			var logs []types.Log
			for b := from; b <= to; b++ {
				logs = append(logs, types.Log{BlockNumber: b, Index: 0, Topics: []common.Hash{helpers.TransferEventSignature}})
			}
			return logs, nil
		}).
		AnyTimes()

	unrelated := common.HexToHash("0x1111")
	mockEth.EXPECT().
		BlockReceipts(gomock.Any(), big.NewInt(101)).
		Return([]*types.Receipt{
			{Logs: []*types.Log{
				{BlockNumber: 101, Index: 0, Topics: []common.Hash{unrelated}},
				{BlockNumber: 101, Index: 1, Topics: []common.Hash{helpers.TransferEventSignature, {}, {}}}, // ERC-20-shaped: still selected, as eth_getLogs would
			}},
			{Logs: []*types.Log{
				{BlockNumber: 101, Index: 2, Topics: []common.Hash{helpers.ERC1155TransferSingleEventSignature}},
				{BlockNumber: 101, Index: 3, Topics: nil},
			}},
		}, nil)

	logs, err := client.FetchIngestionLogs(context.Background(), 100, 102)
	require.NoError(t, err)

	var got [][2]uint64
	for _, l := range logs {
		got = append(got, [2]uint64{l.BlockNumber, uint64(l.Index)})
	}
	require.Equal(t, [][2]uint64{{100, 0}, {101, 1}, {101, 2}, {102, 0}}, got,
		"neighbors via eth_getLogs, dense block via receipts filtered to the ingestion topics, in chain order")
}

// TestFetchIngestionLogs_DenseBlockReceiptFailureIsReported pins that a
// failing receipts fetch surfaces as an error naming the block (the caller's
// retry/restart path), not as a silently empty block.
func TestFetchIngestionLogs_DenseBlockReceiptFailureIsReported(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	client, mockEth := newIngestionClient(t, ctrl)

	mockEth.EXPECT().FilterLogs(gomock.Any(), gomock.Any()).Return(nil, errors.New("query returned more than 10000 results")).AnyTimes()
	receiptErr := errors.New("rpc: receipts unavailable")
	mockEth.EXPECT().BlockReceipts(gomock.Any(), big.NewInt(50)).Return(nil, receiptErr)

	_, err := client.FetchIngestionLogs(context.Background(), 50, 50)
	require.ErrorIs(t, err, receiptErr)
	require.Contains(t, err.Error(), "dense block 50")
}

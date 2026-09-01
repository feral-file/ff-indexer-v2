package adapters_test

import (
	"context"
	"sort"
	"sync"
	"testing"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/adapters"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/helpers"
)

// TestERC1155OwnerScan_MergedTransferLegs pins the credit-guard query shape:
// a standalone ERC-1155 owner scan issues exactly two eth_getLogs queries
// (owner-as-sender and owner-as-recipient), each carrying BOTH TransferSingle
// and TransferBatch in topics[0]. The pre-guard shape was four queries — one
// per (event, position) pair — and each query walks the full block range on a
// span-capped provider, so a regression here silently doubles the RPC cost of
// every wallet scan.
func TestERC1155OwnerScan_MergedTransferLegs(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEthClient(ctrl)
	mockClock := mocks.NewMockClock(ctrl) // strict: any rejection sleep fails the test

	const (
		fromBlock uint64 = 5
		toBlock   uint64 = 100
	)
	owner := common.HexToAddress("0x00000000000000000000000000000000000000aa")
	ownerHash := common.BytesToHash(owner.Bytes())

	var (
		mu      sync.Mutex
		queries []ethereum.FilterQuery
	)
	mockClient.EXPECT().
		FilterLogs(gomock.Any(), gomock.Any()).
		Times(2).
		DoAndReturn(func(_ context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
			mu.Lock()
			defer mu.Unlock()
			queries = append(queries, query)
			return nil, nil
		})

	pagination := helpers.NewPaginationHelper(mockClient, mockClock, nil)
	adp := adapters.NewERC1155Adapter(mockClient, pagination, domain.ChainEthereumMainnet, nil, false)

	tokens, err := adp.GetTokensByOwner(context.Background(), owner.Hex(), fromBlock, toBlock, nil)
	require.NoError(t, err)
	require.Empty(t, tokens)
	require.Len(t, queries, 2)

	// Queries run concurrently; identify them by the topic position of the owner.
	sort.Slice(queries, func(i, j int) bool { return len(queries[i].Topics) < len(queries[j].Topics) })
	senderQuery, recipientQuery := queries[0], queries[1]

	bothTransferSigs := []common.Hash{
		helpers.ERC1155TransferSingleEventSignature,
		helpers.ERC1155TransferBatchEventSignature,
	}

	require.Len(t, senderQuery.Topics, 3)
	require.ElementsMatch(t, bothTransferSigs, senderQuery.Topics[0],
		"owner-as-sender query must cover TransferSingle and TransferBatch in one leg")
	require.Nil(t, senderQuery.Topics[1])
	require.Equal(t, []common.Hash{ownerHash}, senderQuery.Topics[2])

	require.Len(t, recipientQuery.Topics, 4)
	require.ElementsMatch(t, bothTransferSigs, recipientQuery.Topics[0],
		"owner-as-recipient query must cover TransferSingle and TransferBatch in one leg")
	require.Nil(t, recipientQuery.Topics[1])
	require.Nil(t, recipientQuery.Topics[2])
	require.Equal(t, []common.Hash{ownerHash}, recipientQuery.Topics[3])

	for _, q := range queries {
		require.Equal(t, fromBlock, q.FromBlock.Uint64())
		require.Equal(t, toBlock, q.ToBlock.Uint64())
	}
}

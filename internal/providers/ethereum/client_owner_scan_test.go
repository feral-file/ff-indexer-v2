package ethereum_test

import (
	"context"
	"math/big"
	"sync"
	"testing"
	"time"

	goethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	ethprovider "github.com/feral-file/ff-indexer-v2/internal/providers/ethereum"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/helpers"
)

func containsHash(haystack []common.Hash, needle common.Hash) bool {
	for _, h := range haystack {
		if h == needle {
			return true
		}
	}
	return false
}

// TestClient_OwnerScan_ThreeMergedWalks is the whole-scan credit guard: one
// owner scan issues EXACTLY three eth_getLogs walks — one per owner topic
// position — with the ERC-721, ERC-1155, and CryptoPunks signatures merged into
// shared topics[0] sets and no contract scoping. Each walk paginates the full
// block range on a span-capped provider (~2,500 calls for a mainnet history
// scan), so a fourth walk here means every wallet scan silently costs ~33%
// more Infura credits. Times(3) is strict: both a regression to the old
// 8-walk fan-out and a lost leg (false-negative discovery) fail this test.
func TestClient_OwnerScan_ThreeMergedWalks(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockEth := mocks.NewMockEthClient(ctrl)
	mockBlock := mocks.NewMockBlockProvider(ctrl)

	owner := common.HexToAddress("0x1111111111111111111111111111111111111111")
	ownerHash := common.BytesToHash(owner.Bytes())

	punkTransferSig := crypto.Keccak256Hash([]byte("PunkTransfer(address,address,uint256)"))
	punkAssignSig := crypto.Keccak256Hash([]byte("Assign(address,uint256)"))
	punkBoughtSig := crypto.Keccak256Hash([]byte("PunkBought(uint256,uint256,address,address)"))

	var (
		mu      sync.Mutex
		queries []goethereum.FilterQuery
	)
	mockEth.EXPECT().
		FilterLogs(gomock.Any(), gomock.Any()).
		Times(3).
		DoAndReturn(func(_ context.Context, q goethereum.FilterQuery) ([]types.Log, error) {
			mu.Lock()
			defer mu.Unlock()
			queries = append(queries, q)
			return nil, nil
		})

	client, err := ethprovider.NewClient(domain.ChainEthereumMainnet, mockEth, adapter.NewClock(), mockBlock)
	require.NoError(t, err)

	result, err := client.GetTokenCIDsByOwnerAndBlockRange(
		context.Background(), owner.Hex(), 0, 1000, 100, domain.BlockScanOrderAsc, nil)
	require.NoError(t, err)
	require.Empty(t, result.Tokens)
	require.Len(t, queries, 3)

	byPosition := make(map[int]goethereum.FilterQuery, 3)
	for _, q := range queries {
		require.Empty(t, q.Addresses, "merged owner-scan queries must not be contract-scoped")
		position := len(q.Topics) - 1
		require.Equal(t, []common.Hash{ownerHash}, q.Topics[position],
			"owner hash must sit alone at the query's last topic position")
		byPosition[position] = q
	}
	require.Len(t, byPosition, 3, "one query per owner topic position")

	require.ElementsMatch(t, []common.Hash{
		helpers.TransferEventSignature,
		punkTransferSig,
		punkAssignSig,
	}, byPosition[1].Topics[0])

	require.ElementsMatch(t, []common.Hash{
		helpers.TransferEventSignature, // ERC-721 to; subsumes the punks internal buyer leg
		helpers.ERC1155TransferSingleEventSignature,
		helpers.ERC1155TransferBatchEventSignature,
		punkTransferSig,
		punkBoughtSig,
	}, byPosition[2].Topics[0])

	require.ElementsMatch(t, []common.Hash{
		helpers.ERC1155TransferSingleEventSignature,
		helpers.ERC1155TransferBatchEventSignature,
		punkBoughtSig,
	}, byPosition[3].Topics[0],
		"Transfer must NOT appear at position 3: ERC-721 topic 3 is tokenId, and an owner hash there matches garbage")
}

// TestClient_OwnerScan_CorruptedPunkBoughtRepairViaMergedPool verifies the
// receipt repair survives the merged fetch: a corrupted acceptBidForPunk
// purchase (PunkBought with zero indexed buyer) is invisible to the owner
// queries, but the same-tx internal Transfer(seller, buyer, 1) arrives via the
// merged owner-at-topic-2 walk, and the post-process pass follows its receipt
// to recover the PunkBought log — so the scan still discovers the punk.
func TestClient_OwnerScan_CorruptedPunkBoughtRepairViaMergedPool(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockEth := mocks.NewMockEthClient(ctrl)
	mockBlock := mocks.NewMockBlockProvider(ctrl)
	mockBlock.EXPECT().
		GetBlockTimestamp(gomock.Any(), gomock.Any()).
		Return(time.Unix(1_700_000_003, 0), nil).
		AnyTimes()

	buyer := common.HexToAddress("0x1111111111111111111111111111111111111111")
	seller := common.HexToAddress("0x2222222222222222222222222222222222222222")
	buyerHash := common.BytesToHash(buyer.Bytes())
	punksContract := common.HexToAddress(cryptoPunksAddress)
	punkBoughtSig := crypto.Keccak256Hash([]byte("PunkBought(uint256,uint256,address,address)"))
	buyTx := common.HexToHash("0xbeef")

	// The punks internal Transfer(seller, buyer, 1): ERC-20 shaped, 3 topics,
	// value 1 in data. Matches the merged owner-at-topic-2 query via the shared
	// Transfer signature hash.
	internalTransfer := types.Log{
		Address:     punksContract,
		BlockNumber: 700,
		TxHash:      buyTx,
		Index:       0,
		Topics: []common.Hash{
			helpers.TransferEventSignature,
			common.BytesToHash(seller.Bytes()),
			buyerHash,
		},
		Data: common.LeftPadBytes(big.NewInt(1).Bytes(), 32),
	}
	// The corrupted PunkBought in the same tx: indexed toAddress is zero, so no
	// owner-scoped query can match it directly.
	corruptedPunkBought := types.Log{
		Address:     punksContract,
		BlockNumber: 700,
		TxHash:      buyTx,
		Index:       1,
		Topics: []common.Hash{
			punkBoughtSig,
			common.BigToHash(big.NewInt(7)),
			common.BytesToHash(seller.Bytes()),
			{}, // zero buyer — the corruption
		},
		Data: common.LeftPadBytes(big.NewInt(1).Bytes(), 32),
	}

	mockEth.EXPECT().
		FilterLogs(gomock.Any(), gomock.Any()).
		Times(3).
		DoAndReturn(func(_ context.Context, q goethereum.FilterQuery) ([]types.Log, error) {
			// Only the owner-at-topic-2 merged walk matches the internal Transfer.
			if len(q.Topics) == 3 && len(q.Topics[2]) == 1 && q.Topics[2][0] == buyerHash &&
				containsHash(q.Topics[0], helpers.TransferEventSignature) {
				return []types.Log{internalTransfer}, nil
			}
			return nil, nil
		})

	// Fetched twice: once by the post-process discovery pass and once by the
	// replay's inline repair, which recovers the buyer from the internal
	// Transfer log in the same receipt.
	mockEth.EXPECT().
		TransactionReceipt(gomock.Any(), buyTx).
		Return(&types.Receipt{Logs: []*types.Log{&internalTransfer, &corruptedPunkBought}}, nil).
		Times(2)

	client, err := ethprovider.NewClient(domain.ChainEthereumMainnet, mockEth, adapter.NewClock(), mockBlock)
	require.NoError(t, err)

	result, err := client.GetTokenCIDsByOwnerAndBlockRange(
		context.Background(), buyer.Hex(), 0, 1000, 100, domain.BlockScanOrderAsc, nil)
	require.NoError(t, err)
	require.Len(t, result.Tokens, 1)

	expectedCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, cryptoPunksAddress, "7")
	require.Equal(t, expectedCID, result.Tokens[0].TokenCID)
	require.Equal(t, uint64(700), result.Tokens[0].BlockNumber)
}

// TestClient_OwnerScan_SellerSideInternalTransferSkipsReceipt pins the repair
// pass's owner filter: the merged owner-at-topic-1 walk returns the punks
// internal Transfer(owner, buyer, 1) when the scanned owner is the SELLER, but
// only PunkBought's indexed buyer is ever corrupted — a sale by the owner is
// matched directly by the owner-at-topic-2 PunkBought query. Following
// seller-side transfers would pay one receipt RPC per historical sale and let a
// transient receipt failure abort an otherwise valid scan. The strict mock has
// no TransactionReceipt expectation, so any receipt fetch fails this test.
func TestClient_OwnerScan_SellerSideInternalTransferSkipsReceipt(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockEth := mocks.NewMockEthClient(ctrl)
	mockBlock := mocks.NewMockBlockProvider(ctrl)

	seller := common.HexToAddress("0x1111111111111111111111111111111111111111")
	buyer := common.HexToAddress("0x3333333333333333333333333333333333333333")
	sellerHash := common.BytesToHash(seller.Bytes())
	punksContract := common.HexToAddress(cryptoPunksAddress)

	// The owner's seller-side internal Transfer(seller, buyer, 1): arrives via
	// the merged owner-at-topic-1 walk (shared Transfer signature hash).
	sellerSideInternalTransfer := types.Log{
		Address:     punksContract,
		BlockNumber: 800,
		TxHash:      common.HexToHash("0xfeed"),
		Index:       0,
		Topics: []common.Hash{
			helpers.TransferEventSignature,
			sellerHash,
			common.BytesToHash(buyer.Bytes()),
		},
		Data: common.LeftPadBytes(big.NewInt(1).Bytes(), 32),
	}

	mockEth.EXPECT().
		FilterLogs(gomock.Any(), gomock.Any()).
		Times(3).
		DoAndReturn(func(_ context.Context, q goethereum.FilterQuery) ([]types.Log, error) {
			if len(q.Topics) == 2 && len(q.Topics[1]) == 1 && q.Topics[1][0] == sellerHash {
				return []types.Log{sellerSideInternalTransfer}, nil
			}
			return nil, nil
		})

	client, err := ethprovider.NewClient(domain.ChainEthereumMainnet, mockEth, adapter.NewClock(), mockBlock)
	require.NoError(t, err)

	result, err := client.GetTokenCIDsByOwnerAndBlockRange(
		context.Background(), seller.Hex(), 0, 1000, 100, domain.BlockScanOrderAsc, nil)
	require.NoError(t, err)
	require.Empty(t, result.Tokens, "an internal Transfer alone must not surface a token")
}

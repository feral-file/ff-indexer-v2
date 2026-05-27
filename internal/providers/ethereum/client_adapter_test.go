package ethereum_test

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"testing"
	"time"

	goethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	ethprovider "github.com/feral-file/ff-indexer-v2/internal/providers/ethereum"
)

const cryptoPunksAddress = "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb"

var (
	cryptoPunksContractABI = mustParseABI(`[
		{
			"constant": true,
			"inputs": [{"name": "index", "type": "uint256"}],
			"name": "punkIndexToAddress",
			"outputs": [{"name": "", "type": "address"}],
			"type": "function"
		}
	]`)
	erc721OwnerOfABI = mustParseABI(`[
		{
			"constant": true,
			"inputs": [{"name": "tokenId", "type": "uint256"}],
			"name": "ownerOf",
			"outputs": [{"name": "", "type": "address"}],
			"type": "function"
		}
	]`)
)

func mustParseABI(raw string) abi.ABI {
	parsed, err := abi.JSON(strings.NewReader(raw))
	if err != nil {
		panic(err)
	}
	return parsed
}

func TestClient_AdapterRouting_CryptoPunks(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockEth := mocks.NewMockEthClient(ctrl)
	mockBlock := mocks.NewMockBlockProvider(ctrl)

	ownerAddr := common.HexToAddress("0x1234567890123456789012345678901234567890")
	punkCallData, err := cryptoPunksContractABI.Pack("punkIndexToAddress", big.NewInt(1))
	require.NoError(t, err)

	mockEth.EXPECT().
		CallContract(gomock.Any(), gomock.Any(), gomock.Nil()).
		DoAndReturn(func(_ context.Context, msg goethereum.CallMsg, _ *big.Int) ([]byte, error) {
			require.NotNil(t, msg.To)
			require.Equal(t, common.HexToAddress(cryptoPunksAddress), *msg.To)
			require.Equal(t, punkCallData, msg.Data)
			return common.LeftPadBytes(ownerAddr.Bytes(), 32), nil
		}).
		Times(2)

	client := ethprovider.NewClient(domain.ChainEthereumMainnet, mockEth, adapter.NewClock(), mockBlock)

	exists, err := client.TokenExists(context.Background(), cryptoPunksAddress, "1", domain.StandardERC721)
	require.NoError(t, err)
	require.True(t, exists)

	owner, err := client.TokenOwner(context.Background(), cryptoPunksAddress, "1", domain.StandardERC721)
	require.NoError(t, err)
	require.Equal(t, ownerAddr.Hex(), owner)

	require.True(t, client.IsVendorOnlyMetadata(cryptoPunksAddress))
}

func TestClient_AdapterRouting_StandardERC721Regression(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockEth := mocks.NewMockEthClient(ctrl)
	mockBlock := mocks.NewMockBlockProvider(ctrl)

	contractAddress := "0x0000000000000000000000000000000000000123"
	ownerAddr := common.HexToAddress("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd")
	ownerCallData, err := erc721OwnerOfABI.Pack("ownerOf", big.NewInt(42))
	require.NoError(t, err)

	mockEth.EXPECT().
		CallContract(gomock.Any(), gomock.Any(), gomock.Nil()).
		DoAndReturn(func(_ context.Context, msg goethereum.CallMsg, _ *big.Int) ([]byte, error) {
			require.NotNil(t, msg.To)
			require.Equal(t, common.HexToAddress(contractAddress), *msg.To)
			require.Equal(t, ownerCallData, msg.Data)
			return common.LeftPadBytes(ownerAddr.Bytes(), 32), nil
		}).
		Times(2)

	client := ethprovider.NewClient(domain.ChainEthereumMainnet, mockEth, adapter.NewClock(), mockBlock)

	exists, err := client.TokenExists(context.Background(), contractAddress, "42", domain.StandardERC721)
	require.NoError(t, err)
	require.True(t, exists)

	owner, err := client.TokenOwner(context.Background(), contractAddress, "42", domain.StandardERC721)
	require.NoError(t, err)
	require.Equal(t, ownerAddr.Hex(), owner)

	require.False(t, client.IsVendorOnlyMetadata(contractAddress))
}

func expectBlockTimestamps(t *testing.T, mockBlock *mocks.MockBlockProvider, ts time.Time) {
	t.Helper()
	mockBlock.EXPECT().
		GetBlockTimestamp(gomock.Any(), gomock.Any()).
		Return(ts, nil).
		AnyTimes()
}

func TestClient_AdapterRouting_CryptoPunks_OutOfRangeToken(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockEth := mocks.NewMockEthClient(ctrl)
	mockBlock := mocks.NewMockBlockProvider(ctrl)

	mockEth.EXPECT().
		CallContract(gomock.Any(), gomock.Any(), gomock.Nil()).
		Return(nil, errors.New("execution reverted"))

	client := ethprovider.NewClient(domain.ChainEthereumMainnet, mockEth, adapter.NewClock(), mockBlock)

	exists, err := client.TokenExists(context.Background(), cryptoPunksAddress, "10000", domain.StandardERC721)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestClient_SupportsProvenance_CryptoPunksWithCustomEvents(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockEth := mocks.NewMockEthClient(ctrl)
	mockBlock := mocks.NewMockBlockProvider(ctrl)

	client := ethprovider.NewClient(domain.ChainEthereumMainnet, mockEth, adapter.NewClock(), mockBlock)
	require.True(t, client.SupportsProvenance(cryptoPunksAddress, domain.StandardERC721))
}

func TestClient_ParseEventLog_CryptoPunksPunkTransfer(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockEth := mocks.NewMockEthClient(ctrl)
	mockBlock := mocks.NewMockBlockProvider(ctrl)
	expectedTimestamp := time.Unix(1_700_000_000, 0)
	expectBlockTimestamps(t, mockBlock, expectedTimestamp)

	client := ethprovider.NewClient(domain.ChainEthereumMainnet, mockEth, adapter.NewClock(), mockBlock)

	fromAddr := common.HexToAddress("0x1111111111111111111111111111111111111111")
	toAddr := common.HexToAddress("0x2222222222222222222222222222222222222222")
	punkIndex := big.NewInt(42)

	vLog := types.Log{
		Address:     common.HexToAddress(cryptoPunksAddress),
		BlockNumber: 123,
		BlockHash:   common.HexToHash("0xabc"),
		TxHash:      common.HexToHash("0xdef"),
		Topics: []common.Hash{
			crypto.Keccak256Hash([]byte("PunkTransfer(address,address,uint256)")),
			common.BytesToHash(fromAddr.Bytes()),
			common.BytesToHash(toAddr.Bytes()),
		},
		Data: common.LeftPadBytes(punkIndex.Bytes(), 32),
	}

	event, err := client.ParseEventLog(context.Background(), vLog)
	require.NoError(t, err)
	require.NotNil(t, event)
	require.Equal(t, domain.StandardERC721, event.Standard)
	require.Equal(t, domain.EventTypeTransfer, event.EventType)
	require.Equal(t, "42", event.TokenNumber)
	require.Equal(t, "1", event.Quantity)
	require.Equal(t, expectedTimestamp, event.Timestamp)
	require.NotNil(t, event.FromAddress)
	require.NotNil(t, event.ToAddress)
	require.Equal(t, fromAddr.Hex(), *event.FromAddress)
	require.Equal(t, toAddr.Hex(), *event.ToAddress)
}

func TestClient_ParseEventLog_CryptoPunksAssign(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockEth := mocks.NewMockEthClient(ctrl)
	mockBlock := mocks.NewMockBlockProvider(ctrl)
	expectBlockTimestamps(t, mockBlock, time.Unix(1_700_000_001, 0))

	client := ethprovider.NewClient(domain.ChainEthereumMainnet, mockEth, adapter.NewClock(), mockBlock)

	toAddr := common.HexToAddress("0x3333333333333333333333333333333333333333")
	punkIndex := big.NewInt(7)

	vLog := types.Log{
		Address:     common.HexToAddress(cryptoPunksAddress),
		BlockNumber: 124,
		BlockHash:   common.HexToHash("0xabc"),
		TxHash:      common.HexToHash("0xdef"),
		Topics: []common.Hash{
			crypto.Keccak256Hash([]byte("Assign(address,uint256)")),
			common.BytesToHash(toAddr.Bytes()),
		},
		Data: common.LeftPadBytes(punkIndex.Bytes(), 32),
	}

	event, err := client.ParseEventLog(context.Background(), vLog)
	require.NoError(t, err)
	require.NotNil(t, event)
	require.Equal(t, domain.EventTypeMint, event.EventType)
	require.Equal(t, "7", event.TokenNumber)
	require.Nil(t, event.FromAddress)
	require.NotNil(t, event.ToAddress)
	require.Equal(t, toAddr.Hex(), *event.ToAddress)
}

func TestClient_ParseEventLog_CryptoPunksPunkBought(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockEth := mocks.NewMockEthClient(ctrl)
	mockBlock := mocks.NewMockBlockProvider(ctrl)
	expectedTimestamp := time.Unix(1_700_000_002, 0)
	expectBlockTimestamps(t, mockBlock, expectedTimestamp)

	client := ethprovider.NewClient(domain.ChainEthereumMainnet, mockEth, adapter.NewClock(), mockBlock)

	fromAddr := common.HexToAddress("0x4444444444444444444444444444444444444444")
	toAddr := common.HexToAddress("0x5555555555555555555555555555555555555555")
	punkIndex := big.NewInt(99)
	purchaseValue := big.NewInt(1_000_000_000_000_000_000)

	vLog := types.Log{
		Address:     common.HexToAddress(cryptoPunksAddress),
		BlockNumber: 500,
		BlockHash:   common.HexToHash("0xabc"),
		TxHash:      common.HexToHash("0xdef"),
		Topics: []common.Hash{
			crypto.Keccak256Hash([]byte("PunkBought(uint256,uint256,address,address)")),
			common.BigToHash(punkIndex),
			common.BytesToHash(fromAddr.Bytes()),
			common.BytesToHash(toAddr.Bytes()),
		},
		Data: common.LeftPadBytes(purchaseValue.Bytes(), 32),
	}

	event, err := client.ParseEventLog(context.Background(), vLog)
	require.NoError(t, err)
	require.NotNil(t, event)
	require.Equal(t, domain.EventTypeTransfer, event.EventType)
	require.Equal(t, "99", event.TokenNumber)
	require.Equal(t, expectedTimestamp, event.Timestamp)
	require.NotNil(t, event.FromAddress)
	require.NotNil(t, event.ToAddress)
	require.Equal(t, fromAddr.Hex(), *event.FromAddress)
	require.Equal(t, toAddr.Hex(), *event.ToAddress)
}

func TestClient_GetTokenCIDsByOwnerAndBlockRange_ConfiguredLegacyContract(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockEth := mocks.NewMockEthClient(ctrl)
	mockBlock := mocks.NewMockBlockProvider(ctrl)
	expectBlockTimestamps(t, mockBlock, time.Unix(1_700_000_003, 0))

	owner := common.HexToAddress("0x1111111111111111111111111111111111111111")
	seller := common.HexToAddress("0x2222222222222222222222222222222222222222")
	punkBoughtSig := crypto.Keccak256Hash([]byte("PunkBought(uint256,uint256,address,address)"))
	cryptoPunksContract := common.HexToAddress(cryptoPunksAddress)

	mockEth.EXPECT().
		FilterLogs(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, q goethereum.FilterQuery) ([]types.Log, error) {
			if len(q.Addresses) == 1 && q.Addresses[0] == cryptoPunksContract {
				ownerHash := common.BytesToHash(owner.Bytes())
				if len(q.Topics) >= 4 && q.Topics[3] != nil && len(q.Topics[3]) == 1 && q.Topics[3][0] == ownerHash {
					if len(q.Topics[0]) == 1 && q.Topics[0][0] == punkBoughtSig {
						return []types.Log{{
							Address:     cryptoPunksContract,
							BlockNumber: 500,
							BlockHash:   common.HexToHash("0xabc"),
							TxHash:      common.HexToHash("0xdef"),
							Index:       1,
							Topics: []common.Hash{
								punkBoughtSig,
								common.BigToHash(big.NewInt(42)),
								common.BytesToHash(seller.Bytes()),
								ownerHash,
							},
							Data: common.LeftPadBytes(big.NewInt(1).Bytes(), 32),
						}}, nil
					}
				}
			}
			return nil, nil
		}).
		AnyTimes()

	client := ethprovider.NewClient(domain.ChainEthereumMainnet, mockEth, adapter.NewClock(), mockBlock)

	result, err := client.GetTokenCIDsByOwnerAndBlockRange(
		context.Background(),
		owner.Hex(),
		0,
		1000,
		100,
		domain.BlockScanOrderAsc,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, result.Tokens, 1)

	expectedCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, cryptoPunksAddress, "42")
	require.Equal(t, expectedCID, result.Tokens[0].TokenCID)
	require.Equal(t, uint64(500), result.Tokens[0].BlockNumber)
}

func TestClient_GetTokenCIDsByOwnerAndBlockRange_TimestampLookupFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockEth := mocks.NewMockEthClient(ctrl)
	mockBlock := mocks.NewMockBlockProvider(ctrl)

	owner := common.HexToAddress("0x1111111111111111111111111111111111111111")
	punkBoughtSig := crypto.Keccak256Hash([]byte("PunkBought(uint256,uint256,address,address)"))
	cryptoPunksContract := common.HexToAddress(cryptoPunksAddress)

	mockBlock.EXPECT().
		GetBlockTimestamp(gomock.Any(), uint64(500)).
		Return(time.Time{}, context.DeadlineExceeded)

	mockEth.EXPECT().
		FilterLogs(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, q goethereum.FilterQuery) ([]types.Log, error) {
			if len(q.Addresses) == 1 && q.Addresses[0] == cryptoPunksContract {
				ownerHash := common.BytesToHash(owner.Bytes())
				if len(q.Topics) >= 4 && q.Topics[3] != nil && len(q.Topics[3]) == 1 && q.Topics[3][0] == ownerHash {
					return []types.Log{{
						Address:     cryptoPunksContract,
						BlockNumber: 500,
						BlockHash:   common.HexToHash("0xabc"),
						TxHash:      common.HexToHash("0xdef"),
						Index:       1,
						Topics: []common.Hash{
							punkBoughtSig,
							common.BigToHash(big.NewInt(42)),
							common.BytesToHash(common.HexToAddress("0x2222222222222222222222222222222222222222").Bytes()),
							ownerHash,
						},
						Data: common.LeftPadBytes(big.NewInt(1).Bytes(), 32),
					}}, nil
				}
			}
			return nil, nil
		}).
		AnyTimes()

	client := ethprovider.NewClient(domain.ChainEthereumMainnet, mockEth, adapter.NewClock(), mockBlock)

	_, err := client.GetTokenCIDsByOwnerAndBlockRange(
		context.Background(),
		owner.Hex(),
		0,
		1000,
		100,
		domain.BlockScanOrderAsc,
		nil,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "adapter ownership query failed")
	require.Contains(t, err.Error(), "resolve block timestamp")
}

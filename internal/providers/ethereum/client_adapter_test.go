package ethereum_test

import (
	"context"
	"math/big"
	"strings"
	"testing"

	goethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
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

	client := ethprovider.NewClient(domain.ChainEthereumMainnet, mockEth, adapter.NewClock(), nil)

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

	client := ethprovider.NewClient(domain.ChainEthereumMainnet, mockEth, adapter.NewClock(), nil)

	exists, err := client.TokenExists(context.Background(), contractAddress, "42", domain.StandardERC721)
	require.NoError(t, err)
	require.True(t, exists)

	owner, err := client.TokenOwner(context.Background(), contractAddress, "42", domain.StandardERC721)
	require.NoError(t, err)
	require.Equal(t, ownerAddr.Hex(), owner)

	require.False(t, client.IsVendorOnlyMetadata(contractAddress))
}

func TestClient_AdapterRouting_CryptoPunks_MissingToken(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockEth := mocks.NewMockEthClient(ctrl)

	mockEth.EXPECT().
		CallContract(gomock.Any(), gomock.Any(), gomock.Nil()).
		Return(common.LeftPadBytes(common.HexToAddress(domain.ETHEREUM_ZERO_ADDRESS).Bytes(), 32), nil)

	client := ethprovider.NewClient(domain.ChainEthereumMainnet, mockEth, adapter.NewClock(), nil)

	exists, err := client.TokenExists(context.Background(), cryptoPunksAddress, "9999", domain.StandardERC721)
	require.NoError(t, err)
	require.False(t, exists)
}

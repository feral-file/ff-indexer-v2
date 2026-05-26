package adapters_test

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"testing"
	"testing/fstest"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/adapters"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/helpers"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/registry"
)

const cryptopunksABI = `[
  {
    "constant": true,
    "inputs": [{"name": "index", "type": "uint256"}],
    "name": "punkIndexToAddress",
    "outputs": [{"name": "", "type": "address"}],
    "type": "function"
  }
]`

func TestGenericAdapter_TokenExists_AddressNonZero(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEthClient(ctrl)

	abiRegistry, err := helpers.NewABIRegistry(fstest.MapFS{
		"abis/cryptopunks.json": {Data: []byte(cryptopunksABI)},
	})
	require.NoError(t, err)

	ownerAddr := common.HexToAddress("0x1234567890123456789012345678901234567890")
	mockClient.EXPECT().
		CallContract(gomock.Any(), gomock.Any(), gomock.Nil()).
		DoAndReturn(func(_ context.Context, msg ethereum.CallMsg, _ *big.Int) ([]byte, error) {
			require.NotNil(t, msg.To)
			data, err := abiRegistry.MustGet("cryptopunks").Pack("punkIndexToAddress", big.NewInt(1))
			require.NoError(t, err)
			require.Equal(t, data, msg.Data)
			return common.LeftPadBytes(ownerAddr.Bytes(), 32), nil
		})

	existence, err := registry.BuildGenericAdapterFromConfig(registry.ContractConfig{
		Chain:    domain.ChainEthereumMainnet,
		Address:  "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
		Standard: domain.StandardERC721,
		Adapter: registry.AdapterConfig{
			Existence: registry.MethodConfig{
				Method:           "punkIndexToAddress",
				ABI:              "cryptopunks",
				Params:           []string{"${tokenId}"},
				SuccessCondition: "address_nonzero",
			},
			Owner: registry.MethodConfig{
				Method: "punkIndexToAddress",
				ABI:    "cryptopunks",
				Params: []string{"${tokenId}"},
			},
			Metadata: registry.MetadataConfig{Source: "vendor_only"},
		},
	}, abiRegistry, mockClient, nil, domain.ChainEthereumMainnet, nil, nil)
	require.NoError(t, err)

	exists, err := existence.TokenExists(context.Background(), "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb", "1")
	require.NoError(t, err)
	require.True(t, exists)
}

func TestGenericAdapter_TokenExists_ZeroAddress(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEthClient(ctrl)

	abiRegistry, err := helpers.NewABIRegistry(fstest.MapFS{
		"abis/cryptopunks.json": {Data: []byte(cryptopunksABI)},
	})
	require.NoError(t, err)

	mockClient.EXPECT().
		CallContract(gomock.Any(), gomock.Any(), gomock.Nil()).
		Return(common.LeftPadBytes(common.HexToAddress(domain.ETHEREUM_ZERO_ADDRESS).Bytes(), 32), nil)

	adp, err := registry.BuildGenericAdapterFromConfig(registry.ContractConfig{
		Chain:    domain.ChainEthereumMainnet,
		Address:  "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
		Standard: domain.StandardERC721,
		Adapter: registry.AdapterConfig{
			Existence: registry.MethodConfig{
				Method:           "punkIndexToAddress",
				ABI:              "cryptopunks",
				Params:           []string{"${tokenId}"},
				SuccessCondition: "address_nonzero",
			},
			Owner: registry.MethodConfig{
				Method: "punkIndexToAddress",
				ABI:    "cryptopunks",
				Params: []string{"${tokenId}"},
			},
		},
	}, abiRegistry, mockClient, nil, domain.ChainEthereumMainnet, nil, nil)
	require.NoError(t, err)

	exists, err := adp.TokenExists(context.Background(), "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb", "9999")
	require.NoError(t, err)
	require.False(t, exists)
}

func TestGenericAdapter_TokenExists_Revert(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEthClient(ctrl)

	abiRegistry, err := helpers.NewABIRegistry(fstest.MapFS{
		"abis/cryptopunks.json": {Data: []byte(cryptopunksABI)},
	})
	require.NoError(t, err)

	mockClient.EXPECT().
		CallContract(gomock.Any(), gomock.Any(), gomock.Nil()).
		Return(nil, errors.New("execution reverted"))

	adp, err := registry.BuildGenericAdapterFromConfig(registry.ContractConfig{
		Chain:    domain.ChainEthereumMainnet,
		Address:  "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
		Standard: domain.StandardERC721,
		Adapter: registry.AdapterConfig{
			Existence: registry.MethodConfig{
				Method:           "punkIndexToAddress",
				ABI:              "cryptopunks",
				Params:           []string{"${tokenId}"},
				SuccessCondition: "address_nonzero",
			},
			Owner: registry.MethodConfig{
				Method: "punkIndexToAddress",
				ABI:    "cryptopunks",
				Params: []string{"${tokenId}"},
			},
		},
	}, abiRegistry, mockClient, nil, domain.ChainEthereumMainnet, nil, nil)
	require.NoError(t, err)

	exists, err := adp.TokenExists(context.Background(), "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb", "1")
	require.NoError(t, err)
	require.False(t, exists)
}

func TestGenericAdapter_TokenOwner(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEthClient(ctrl)

	abiRegistry, err := helpers.NewABIRegistry(fstest.MapFS{
		"abis/cryptopunks.json": {Data: []byte(cryptopunksABI)},
	})
	require.NoError(t, err)

	ownerAddr := common.HexToAddress("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd")
	mockClient.EXPECT().
		CallContract(gomock.Any(), gomock.Any(), gomock.Nil()).
		Return(common.LeftPadBytes(ownerAddr.Bytes(), 32), nil)

	adp, err := registry.BuildGenericAdapterFromConfig(registry.ContractConfig{
		Chain:    domain.ChainEthereumMainnet,
		Address:  "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
		Standard: domain.StandardERC721,
		Adapter: registry.AdapterConfig{
			Existence: registry.MethodConfig{
				Method:           "punkIndexToAddress",
				ABI:              "cryptopunks",
				Params:           []string{"${tokenId}"},
				SuccessCondition: "address_nonzero",
			},
			Owner: registry.MethodConfig{
				Method: "punkIndexToAddress",
				ABI:    "cryptopunks",
				Params: []string{"${tokenId}"},
			},
		},
	}, abiRegistry, mockClient, nil, domain.ChainEthereumMainnet, nil, nil)
	require.NoError(t, err)

	owner, err := adp.TokenOwner(context.Background(), "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb", "1")
	require.NoError(t, err)
	require.Equal(t, ownerAddr.Hex(), owner)
}

func TestGenericAdapter_TokenURI_VendorOnly(t *testing.T) {
	adp := adapters.NewGenericAdapter(
		domain.StandardERC721,
		nil,
		nil,
		adapters.ContractMetadataConfig{
			Source: adapters.MetadataSourceVendorOnly,
		},
		nil,
		nil,
		false,
		nil,
		domain.ChainEthereumMainnet,
		nil,
		nil,
	)

	uri, err := adp.TokenURI(context.Background(), "0xabc", "1")
	require.NoError(t, err)
	require.Empty(t, uri)
}

func TestGenericAdapter_TokenOwner_ZeroAddressMeansNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEthClient(ctrl)

	abiRegistry, err := helpers.NewABIRegistry(fstest.MapFS{
		"abis/cryptopunks.json": {Data: []byte(cryptopunksABI)},
	})
	require.NoError(t, err)

	mockClient.EXPECT().
		CallContract(gomock.Any(), gomock.Any(), gomock.Nil()).
		Return(common.LeftPadBytes(common.HexToAddress(domain.ETHEREUM_ZERO_ADDRESS).Bytes(), 32), nil)

	adp, err := registry.BuildGenericAdapterFromConfig(registry.ContractConfig{
		Chain:    domain.ChainEthereumMainnet,
		Address:  "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
		Standard: domain.StandardERC721,
		Adapter: registry.AdapterConfig{
			Existence: registry.MethodConfig{
				Method:           "punkIndexToAddress",
				ABI:              "cryptopunks",
				Params:           []string{"${tokenId}"},
				SuccessCondition: "address_nonzero",
			},
			Owner: registry.MethodConfig{
				Method: "punkIndexToAddress",
				ABI:    "cryptopunks",
				Params: []string{"${tokenId}"},
			},
		},
	}, abiRegistry, mockClient, nil, domain.ChainEthereumMainnet, nil, nil)
	require.NoError(t, err)

	_, err = adp.TokenOwner(context.Background(), "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb", "10000")
	require.Error(t, err)
	require.True(t, errors.Is(err, domain.ErrTokenNotFoundOnChain))
}

func TestGenericAdapter_InvalidTokenNumber(t *testing.T) {
	abiRegistry, err := helpers.NewABIRegistry(fstest.MapFS{
		"abis/cryptopunks.json": {Data: []byte(cryptopunksABI)},
	})
	require.NoError(t, err)

	adp, err := registry.BuildGenericAdapterFromConfig(registry.ContractConfig{
		Chain:    domain.ChainEthereumMainnet,
		Address:  "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
		Standard: domain.StandardERC721,
		Adapter: registry.AdapterConfig{
			Existence: registry.MethodConfig{
				Method:           "punkIndexToAddress",
				ABI:              "cryptopunks",
				Params:           []string{"${tokenId}"},
				SuccessCondition: "address_nonzero",
			},
			Owner: registry.MethodConfig{
				Method: "punkIndexToAddress",
				ABI:    "cryptopunks",
				Params: []string{"${tokenId}"},
			},
		},
	}, abiRegistry, nil, nil, domain.ChainEthereumMainnet, nil, nil)
	require.NoError(t, err)

	_, err = adp.TokenExists(context.Background(), "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb", "not-a-number")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid token number")
}

func TestERC721Adapter_TokenExists(t *testing.T) {
	t.Run("exists", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockClient := mocks.NewMockEthClient(ctrl)
		ownerAddr := common.HexToAddress("0x1234567890123456789012345678901234567890")
		mockClient.EXPECT().
			CallContract(gomock.Any(), gomock.Any(), gomock.Nil()).
			Return(common.LeftPadBytes(ownerAddr.Bytes(), 32), nil)

		adp := adapters.NewERC721Adapter(mockClient, nil, domain.ChainEthereumMainnet, nil, nil)

		exists, err := adp.TokenExists(context.Background(), "0xabc", "1")
		require.NoError(t, err)
		require.True(t, exists)
	})

	t.Run("revert means missing", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockClient := mocks.NewMockEthClient(ctrl)
		mockClient.EXPECT().
			CallContract(gomock.Any(), gomock.Any(), gomock.Nil()).
			Return(nil, fmt.Errorf("execution reverted"))

		adp := adapters.NewERC721Adapter(mockClient, nil, domain.ChainEthereumMainnet, nil, nil)

		exists, err := adp.TokenExists(context.Background(), "0xabc", "1")
		require.NoError(t, err)
		require.False(t, exists)
	})
}

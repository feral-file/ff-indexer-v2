package adapter_test

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
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/adapter"
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

func testContractFS(t *testing.T, contractsJSON string) fstest.MapFS {
	t.Helper()

	return fstest.MapFS{
		"contracts.json":        {Data: []byte(contractsJSON)},
		"abis/cryptopunks.json": {Data: []byte(cryptopunksABI)},
	}
}

func TestGenericAdapter_TokenExists_AddressNonZero(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEthClient(ctrl)

	abiRegistry, err := adapter.NewABIRegistry(fstest.MapFS{
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

	existence, err := adapter.BuildGenericAdapterFromConfig(adapter.ContractConfig{
		Chain:    domain.ChainEthereumMainnet,
		Address:  "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
		Standard: domain.StandardERC721,
		Adapter: adapter.AdapterConfig{
			Existence: adapter.MethodConfig{
				Method:           "punkIndexToAddress",
				ABI:              "cryptopunks",
				Params:           []string{"${tokenId}"},
				SuccessCondition: "address_nonzero",
			},
			Owner: adapter.MethodConfig{
				Method: "punkIndexToAddress",
				ABI:    "cryptopunks",
				Params: []string{"${tokenId}"},
			},
			Metadata: adapter.MetadataConfig{Source: "vendor_only"},
		},
	}, abiRegistry, mockClient)
	require.NoError(t, err)

	exists, err := existence.TokenExists(context.Background(), "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb", "1")
	require.NoError(t, err)
	require.True(t, exists)
}

func TestGenericAdapter_TokenExists_ZeroAddress(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEthClient(ctrl)

	abiRegistry, err := adapter.NewABIRegistry(fstest.MapFS{
		"abis/cryptopunks.json": {Data: []byte(cryptopunksABI)},
	})
	require.NoError(t, err)

	mockClient.EXPECT().
		CallContract(gomock.Any(), gomock.Any(), gomock.Nil()).
		Return(common.LeftPadBytes(common.HexToAddress(domain.ETHEREUM_ZERO_ADDRESS).Bytes(), 32), nil)

	adp, err := adapter.BuildGenericAdapterFromConfig(adapter.ContractConfig{
		Chain:    domain.ChainEthereumMainnet,
		Address:  "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
		Standard: domain.StandardERC721,
		Adapter: adapter.AdapterConfig{
			Existence: adapter.MethodConfig{
				Method:           "punkIndexToAddress",
				ABI:              "cryptopunks",
				Params:           []string{"${tokenId}"},
				SuccessCondition: "address_nonzero",
			},
			Owner: adapter.MethodConfig{
				Method: "punkIndexToAddress",
				ABI:    "cryptopunks",
				Params: []string{"${tokenId}"},
			},
		},
	}, abiRegistry, mockClient)
	require.NoError(t, err)

	exists, err := adp.TokenExists(context.Background(), "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb", "9999")
	require.NoError(t, err)
	require.False(t, exists)
}

func TestGenericAdapter_TokenExists_Revert(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEthClient(ctrl)

	abiRegistry, err := adapter.NewABIRegistry(fstest.MapFS{
		"abis/cryptopunks.json": {Data: []byte(cryptopunksABI)},
	})
	require.NoError(t, err)

	mockClient.EXPECT().
		CallContract(gomock.Any(), gomock.Any(), gomock.Nil()).
		Return(nil, errors.New("execution reverted"))

	adp, err := adapter.BuildGenericAdapterFromConfig(adapter.ContractConfig{
		Chain:    domain.ChainEthereumMainnet,
		Address:  "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
		Standard: domain.StandardERC721,
		Adapter: adapter.AdapterConfig{
			Existence: adapter.MethodConfig{
				Method:           "punkIndexToAddress",
				ABI:              "cryptopunks",
				Params:           []string{"${tokenId}"},
				SuccessCondition: "address_nonzero",
			},
			Owner: adapter.MethodConfig{
				Method: "punkIndexToAddress",
				ABI:    "cryptopunks",
				Params: []string{"${tokenId}"},
			},
		},
	}, abiRegistry, mockClient)
	require.NoError(t, err)

	exists, err := adp.TokenExists(context.Background(), "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb", "1")
	require.NoError(t, err)
	require.False(t, exists)
}

func TestGenericAdapter_TokenOwner(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEthClient(ctrl)

	abiRegistry, err := adapter.NewABIRegistry(fstest.MapFS{
		"abis/cryptopunks.json": {Data: []byte(cryptopunksABI)},
	})
	require.NoError(t, err)

	ownerAddr := common.HexToAddress("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd")
	mockClient.EXPECT().
		CallContract(gomock.Any(), gomock.Any(), gomock.Nil()).
		Return(common.LeftPadBytes(ownerAddr.Bytes(), 32), nil)

	adp, err := adapter.BuildGenericAdapterFromConfig(adapter.ContractConfig{
		Chain:    domain.ChainEthereumMainnet,
		Address:  "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
		Standard: domain.StandardERC721,
		Adapter: adapter.AdapterConfig{
			Existence: adapter.MethodConfig{
				Method:           "punkIndexToAddress",
				ABI:              "cryptopunks",
				Params:           []string{"${tokenId}"},
				SuccessCondition: "address_nonzero",
			},
			Owner: adapter.MethodConfig{
				Method: "punkIndexToAddress",
				ABI:    "cryptopunks",
				Params: []string{"${tokenId}"},
			},
		},
	}, abiRegistry, mockClient)
	require.NoError(t, err)

	owner, err := adp.TokenOwner(context.Background(), "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb", "1")
	require.NoError(t, err)
	require.Equal(t, ownerAddr.Hex(), owner)
}

func TestGenericAdapter_TokenURI_VendorOnly(t *testing.T) {
	adp := adapter.NewGenericAdapter(nil, nil, adapter.ContractMetadataConfig{
		Source: adapter.MetadataSourceVendorOnly,
	}, adapter.ContractConstraints{}, nil, false)

	uri, err := adp.TokenURI(context.Background(), "0xabc", "1")
	require.NoError(t, err)
	require.Empty(t, uri)
}

func TestGenericAdapter_InvalidTokenNumber(t *testing.T) {
	abiRegistry, err := adapter.NewABIRegistry(fstest.MapFS{
		"abis/cryptopunks.json": {Data: []byte(cryptopunksABI)},
	})
	require.NoError(t, err)

	adp, err := adapter.BuildGenericAdapterFromConfig(adapter.ContractConfig{
		Chain:    domain.ChainEthereumMainnet,
		Address:  "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
		Standard: domain.StandardERC721,
		Adapter: adapter.AdapterConfig{
			Existence: adapter.MethodConfig{
				Method:           "punkIndexToAddress",
				ABI:              "cryptopunks",
				Params:           []string{"${tokenId}"},
				SuccessCondition: "address_nonzero",
			},
			Owner: adapter.MethodConfig{
				Method: "punkIndexToAddress",
				ABI:    "cryptopunks",
				Params: []string{"${tokenId}"},
			},
		},
	}, abiRegistry, nil)
	require.NoError(t, err)

	_, err = adp.TokenExists(context.Background(), "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb", "not-a-number")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid token number")
}

func TestGenericAdapter_TokenIDMaxConstraint(t *testing.T) {
	maxTokenID := int64(9999)

	// Create a simple adapter with just constraints to test validation
	// We don't need actual method calls since constraint validation happens first
	adp := adapter.NewGenericAdapter(
		nil, // existence - not needed for constraint test
		nil, // owner - not needed for constraint test
		adapter.ContractMetadataConfig{Source: adapter.MetadataSourceVendorOnly},
		adapter.ContractConstraints{TokenIDMax: &maxTokenID},
		nil, // ethClient - not needed since constraint check happens before calls
		false,
	)

	// Test token ID exceeding limit - should fail on constraint validation
	_, err := adp.TokenExists(context.Background(), "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb", "10000")
	require.Error(t, err)
	// Note: existence method is nil, so it fails on method check first
	require.Contains(t, err.Error(), "existence method not configured")

	// Test TokenOwner with exceeding ID - constraint is checked after nil check
	_, err = adp.TokenOwner(context.Background(), "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb", "10000")
	require.Error(t, err)
	require.Contains(t, err.Error(), "owner method not configured")

	// Test TokenURI with exceeding ID - validates constraints immediately
	_, err = adp.TokenURI(context.Background(), "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb", "10000")
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds maximum allowed 9999")

	// Test valid token ID (within limit) with TokenURI
	_, err = adp.TokenURI(context.Background(), "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb", "9999")
	require.NoError(t, err) // vendor_only returns empty string, no error
}

type stubStandardOps struct {
	ownerOfCalled bool
	owner         string
	ownerErr      error
}

func (s *stubStandardOps) ERC721OwnerOf(_ context.Context, _, _ string) (string, error) {
	s.ownerOfCalled = true
	return s.owner, s.ownerErr
}

func (s *stubStandardOps) ERC721TokenURI(_ context.Context, _, _ string) (string, error) {
	return "", nil
}

func (s *stubStandardOps) ERC1155URI(_ context.Context, _, _ string) (string, error) {
	return "", nil
}

func (s *stubStandardOps) ERC1155TokenExists(_ context.Context, _, _ string) (bool, error) {
	return false, nil
}

func TestERC721StandardAdapter_TokenExists(t *testing.T) {
	t.Run("exists", func(t *testing.T) {
		ops := &stubStandardOps{owner: "0x1234567890123456789012345678901234567890"}
		adp := adapter.NewERC721StandardAdapter(ops)

		exists, err := adp.TokenExists(context.Background(), "0xabc", "1")
		require.NoError(t, err)
		require.True(t, exists)
		require.True(t, ops.ownerOfCalled)
	})

	t.Run("revert means missing", func(t *testing.T) {
		ops := &stubStandardOps{ownerErr: fmt.Errorf("execution reverted")}
		adp := adapter.NewERC721StandardAdapter(ops)

		exists, err := adp.TokenExists(context.Background(), "0xabc", "1")
		require.NoError(t, err)
		require.False(t, exists)
	})
}

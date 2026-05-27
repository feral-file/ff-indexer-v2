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
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	ethadapter "github.com/feral-file/ff-indexer-v2/internal/adapter"
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
	}, abiRegistry, mockClient, nil, domain.ChainEthereumMainnet)
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
	}, abiRegistry, mockClient, nil, domain.ChainEthereumMainnet)
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
	}, abiRegistry, mockClient, nil, domain.ChainEthereumMainnet)
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
	}, abiRegistry, mockClient, nil, domain.ChainEthereumMainnet)
	require.NoError(t, err)

	owner, err := adp.TokenOwner(context.Background(), "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb", "1")
	require.NoError(t, err)
	require.Equal(t, ownerAddr.Hex(), owner)
}

func TestGenericAdapter_TokenURI_VendorOnly(t *testing.T) {
	adp := adapters.NewGenericAdapter(
		"0xabc",
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
	}, abiRegistry, mockClient, nil, domain.ChainEthereumMainnet)
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
	}, abiRegistry, nil, nil, domain.ChainEthereumMainnet)
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

		adp := adapters.NewERC721Adapter(mockClient, nil, domain.ChainEthereumMainnet)

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

		adp := adapters.NewERC721Adapter(mockClient, nil, domain.ChainEthereumMainnet)

		exists, err := adp.TokenExists(context.Background(), "0xabc", "1")
		require.NoError(t, err)
		require.False(t, exists)
	})
}

// TestGenericAdapter_GetTokensByOwner tests the ownership tracking for generic contracts.
func TestGenericAdapter_GetTokensByOwner(t *testing.T) {
	const cryptopunksTransferABI = `[
		{
			"anonymous": true,
			"inputs": [
				{"indexed": true, "name": "from", "type": "address"},
				{"indexed": true, "name": "to", "type": "address"},
				{"indexed": false, "name": "punkIndex", "type": "uint256"}
			],
			"name": "PunkTransfer",
			"type": "event"
		}
	]`

	t.Run("ERC721-style last-transfer-wins ownership", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockClient := mocks.NewMockEthClient(ctrl)

		abiRegistry, err := helpers.NewABIRegistry(fstest.MapFS{
			"abis/cryptopunks.json":          {Data: []byte(cryptopunksABI)},
			"abis/cryptopunks_transfer.json": {Data: []byte(cryptopunksTransferABI)},
		})
		require.NoError(t, err)

		pagination := helpers.NewPaginationHelper(mockClient, ethadapter.NewClock(), nil)

		contractAddr := "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb"
		owner := common.HexToAddress("0x1111111111111111111111111111111111111111")
		other := common.HexToAddress("0x2222222222222222222222222222222222222222")

		transferABI := abiRegistry.MustGet("cryptopunks_transfer")
		transferSig := transferABI.Events["PunkTransfer"].ID

		// Mock logs: punk #5 transferred to owner, punk #10 transferred to owner then to other
		mockClient.EXPECT().
			FilterLogs(gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
				// Check if this is a "from" or "to" query by inspecting topics
				if len(q.Topics) >= 2 && q.Topics[1] != nil && len(q.Topics[1]) > 0 {
					// "from" query
					if q.Topics[1][0] == common.BytesToHash(owner.Bytes()) {
						// punk #10: owner -> other (block 200)
						return []types.Log{
							{
								Address:     common.HexToAddress(contractAddr),
								Topics:      []common.Hash{transferSig, common.BytesToHash(owner.Bytes()), common.BytesToHash(other.Bytes())},
								Data:        common.LeftPadBytes(big.NewInt(10).Bytes(), 32),
								BlockNumber: 200,
								Index:       1,
							},
						}, nil
					}
				}
				if len(q.Topics) >= 3 && q.Topics[2] != nil && len(q.Topics[2]) > 0 {
					// "to" query
					if q.Topics[2][0] == common.BytesToHash(owner.Bytes()) {
						// punk #5: zero -> owner (mint, block 100)
						// punk #10: other -> owner (block 150)
						return []types.Log{
							{
								Address:     common.HexToAddress(contractAddr),
								Topics:      []common.Hash{transferSig, common.HexToHash("0x0"), common.BytesToHash(owner.Bytes())},
								Data:        common.LeftPadBytes(big.NewInt(5).Bytes(), 32),
								BlockNumber: 100,
								Index:       0,
							},
							{
								Address:     common.HexToAddress(contractAddr),
								Topics:      []common.Hash{transferSig, common.BytesToHash(other.Bytes()), common.BytesToHash(owner.Bytes())},
								Data:        common.LeftPadBytes(big.NewInt(10).Bytes(), 32),
								BlockNumber: 150,
								Index:       0,
							},
						}, nil
					}
				}
				return nil, nil
			}).AnyTimes()

		adp, err := registry.BuildGenericAdapterFromConfig(registry.ContractConfig{
			Chain:    domain.ChainEthereumMainnet,
			Address:  contractAddr,
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
				Events: []adapters.EventConfig{
					{
						Signature:          "PunkTransfer(address,address,uint256)",
						MapToStandardEvent: domain.EventTypeTransfer,
						IndexedParams:      []string{"from", "to"},
						DataParams:         []string{"punkIndex"},
						ParameterMappings: map[string]string{
							"from":      "FromAddress",
							"to":        "ToAddress",
							"punkIndex": "TokenNumber",
						},
					},
				},
			},
		}, abiRegistry, mockClient, pagination, domain.ChainEthereumMainnet)
		require.NoError(t, err)

		tokens, err := adp.GetTokensByOwner(context.Background(), owner.Hex(), 0, 300, nil)
		require.NoError(t, err)

		// Owner should only own punk #5 (punk #10 was transferred away)
		require.Len(t, tokens, 1)
		_, _, _, tokenNumber := tokens[0].TokenCID.Parse()
		require.Equal(t, "5", tokenNumber)
		require.Equal(t, uint64(100), tokens[0].BlockNumber)
	})

	t.Run("no provenance events returns empty", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockClient := mocks.NewMockEthClient(ctrl)

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
				Metadata: registry.MetadataConfig{Source: "vendor_only"},
				Events:   []adapters.EventConfig{}, // No events
			},
		}, abiRegistry, mockClient, nil, domain.ChainEthereumMainnet)
		require.NoError(t, err)

		tokens, err := adp.GetTokensByOwner(context.Background(), "0x1111111111111111111111111111111111111111", 0, 100, nil)
		require.NoError(t, err)
		require.Empty(t, tokens)
	})
}

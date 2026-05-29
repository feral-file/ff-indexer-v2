package adapters_test

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"testing"
	"testing/fstest"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
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
		Chain:          domain.ChainEthereumMainnet,
		Address:        "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
		OwnershipModel: adapters.OwnershipSingleOwner,
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
	}, abiRegistry, mockClient, nil, nil, domain.ChainEthereumMainnet)
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
		Chain:          domain.ChainEthereumMainnet,
		Address:        "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
		OwnershipModel: adapters.OwnershipSingleOwner,
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
	}, abiRegistry, mockClient, nil, nil, domain.ChainEthereumMainnet)
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
		Chain:          domain.ChainEthereumMainnet,
		Address:        "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
		OwnershipModel: adapters.OwnershipSingleOwner,
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
	}, abiRegistry, mockClient, nil, nil, domain.ChainEthereumMainnet)
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
		Chain:          domain.ChainEthereumMainnet,
		Address:        "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
		OwnershipModel: adapters.OwnershipSingleOwner,
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
	}, abiRegistry, mockClient, nil, nil, domain.ChainEthereumMainnet)
	require.NoError(t, err)

	owner, err := adp.TokenOwner(context.Background(), "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb", "1")
	require.NoError(t, err)
	require.Equal(t, ownerAddr.Hex(), owner)
}

func TestGenericAdapter_TokenURI_VendorOnly(t *testing.T) {
	adp := adapters.NewGenericAdapter(
		"0xabc",
		adapters.OwnershipSingleOwner,
		nil,
		nil,
		adapters.ContractMetadataConfig{
			Source: adapters.MetadataSourceVendorOnly,
		},
		nil,
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
		Chain:          domain.ChainEthereumMainnet,
		Address:        "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
		OwnershipModel: adapters.OwnershipSingleOwner,
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
	}, abiRegistry, mockClient, nil, nil, domain.ChainEthereumMainnet)
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
		Chain:          domain.ChainEthereumMainnet,
		Address:        "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
		OwnershipModel: adapters.OwnershipSingleOwner,
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
	}, abiRegistry, nil, nil, nil, domain.ChainEthereumMainnet)
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

		adp := adapters.NewERC721Adapter(mockClient, nil, nil, domain.ChainEthereumMainnet)

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

		adp := adapters.NewERC721Adapter(mockClient, nil, nil, domain.ChainEthereumMainnet)

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
			Chain:          domain.ChainEthereumMainnet,
			Address:        contractAddr,
			OwnershipModel: adapters.OwnershipSingleOwner,
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
		}, abiRegistry, mockClient, pagination, nil, domain.ChainEthereumMainnet)
		require.NoError(t, err)

		tokens, err := adp.GetTokensByOwner(context.Background(), owner.Hex(), 0, 300, nil)
		require.NoError(t, err)

		// Owner should only own punk #5 (punk #10 was transferred away)
		require.Len(t, tokens, 1)
		_, _, _, tokenNumber := tokens[0].TokenCID.Parse()
		require.Equal(t, "5", tokenNumber)
		require.Equal(t, uint64(100), tokens[0].BlockNumber)
	})

	t.Run("PunkBought marketplace purchase ownership", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockClient := mocks.NewMockEthClient(ctrl)

		const cryptopunksBoughtABI = `[
			{
				"anonymous": false,
				"inputs": [
					{"indexed": true, "name": "punkIndex", "type": "uint256"},
					{"indexed": false, "name": "value", "type": "uint256"},
					{"indexed": true, "name": "fromAddress", "type": "address"},
					{"indexed": true, "name": "toAddress", "type": "address"}
				],
				"name": "PunkBought",
				"type": "event"
			}
		]`

		abiRegistry, err := helpers.NewABIRegistry(fstest.MapFS{
			"abis/cryptopunks.json":        {Data: []byte(cryptopunksABI)},
			"abis/cryptopunks_bought.json": {Data: []byte(cryptopunksBoughtABI)},
		})
		require.NoError(t, err)

		pagination := helpers.NewPaginationHelper(mockClient, ethadapter.NewClock(), nil)

		contractAddr := "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb"
		owner := common.HexToAddress("0x1111111111111111111111111111111111111111")
		seller := common.HexToAddress("0x2222222222222222222222222222222222222222")
		boughtSig := crypto.Keccak256Hash([]byte("PunkBought(uint256,uint256,address,address)"))

		mockClient.EXPECT().
			FilterLogs(gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
				ownerHash := common.BytesToHash(owner.Bytes())
				if len(q.Topics) >= 4 && q.Topics[3] != nil && len(q.Topics[3]) > 0 && q.Topics[3][0] == ownerHash {
					return []types.Log{{
						Address:     common.HexToAddress(contractAddr),
						BlockNumber: 250,
						Topics: []common.Hash{
							boughtSig,
							common.BigToHash(big.NewInt(88)),
							common.BytesToHash(seller.Bytes()),
							ownerHash,
						},
						Data:  common.LeftPadBytes(big.NewInt(1).Bytes(), 32),
						Index: 0,
					}}, nil
				}
				return nil, nil
			}).AnyTimes()

		adp, err := registry.BuildGenericAdapterFromConfig(registry.ContractConfig{
			Chain:          domain.ChainEthereumMainnet,
			Address:        contractAddr,
			OwnershipModel: adapters.OwnershipSingleOwner,
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
						Signature:          "PunkBought(uint256,uint256,address,address)",
						MapToStandardEvent: domain.EventTypeTransfer,
						IndexedParams:      []string{"punkIndex", "fromAddress", "toAddress"},
						DataParams:         []string{},
						ParameterMappings: map[string]string{
							"punkIndex":   "TokenNumber",
							"fromAddress": "FromAddress",
							"toAddress":   "ToAddress",
						},
					},
				},
			},
		}, abiRegistry, mockClient, pagination, nil, domain.ChainEthereumMainnet)
		require.NoError(t, err)

		tokens, err := adp.GetTokensByOwner(context.Background(), owner.Hex(), 0, 300, nil)
		require.NoError(t, err)
		require.Len(t, tokens, 1)
		_, _, _, tokenNumber := tokens[0].TokenCID.Parse()
		require.Equal(t, "88", tokenNumber)
		require.Equal(t, uint64(250), tokens[0].BlockNumber)
	})

	t.Run("no provenance events returns empty", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockClient := mocks.NewMockEthClient(ctrl)

		abiRegistry, err := helpers.NewABIRegistry(fstest.MapFS{
			"abis/cryptopunks.json": {Data: []byte(cryptopunksABI)},
		})
		require.NoError(t, err)

		adp, err := registry.BuildGenericAdapterFromConfig(registry.ContractConfig{
			Chain:          domain.ChainEthereumMainnet,
			Address:        "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
			OwnershipModel: adapters.OwnershipSingleOwner,
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
		}, abiRegistry, mockClient, nil, nil, domain.ChainEthereumMainnet)
		require.NoError(t, err)

		tokens, err := adp.GetTokensByOwner(context.Background(), "0x1111111111111111111111111111111111111111", 0, 100, nil)
		require.NoError(t, err)
		require.Empty(t, tokens)
	})
}

func TestGenericAdapter_GetTokenEvents_DeduplicatesDuplicateTransferSources(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEthClient(ctrl)
	mockBlock := mocks.NewMockBlockProvider(ctrl)
	mockBlock.EXPECT().GetLatestBlock(gomock.Any()).Return(uint64(1_000_000), nil).AnyTimes()

	const cryptopunksEventsABI = `[
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true, "name": "from", "type": "address"},
				{"indexed": true, "name": "to", "type": "address"},
				{"indexed": false, "name": "punkIndex", "type": "uint256"}
			],
			"name": "PunkTransfer",
			"type": "event"
		},
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true, "name": "punkIndex", "type": "uint256"},
				{"indexed": false, "name": "value", "type": "uint256"},
				{"indexed": true, "name": "fromAddress", "type": "address"},
				{"indexed": true, "name": "toAddress", "type": "address"}
			],
			"name": "PunkBought",
			"type": "event"
		}
	]`

	abiRegistry, err := helpers.NewABIRegistry(fstest.MapFS{
		"abis/cryptopunks.json":        {Data: []byte(cryptopunksABI)},
		"abis/cryptopunks_events.json": {Data: []byte(cryptopunksEventsABI)},
	})
	require.NoError(t, err)

	pagination := helpers.NewPaginationHelper(mockClient, ethadapter.NewClock(), mockBlock)

	contractAddr := "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb"
	seller := common.HexToAddress("0x2222222222222222222222222222222222222222")
	buyer := common.HexToAddress("0x1111111111111111111111111111111111111111")
	txHash := common.HexToHash("0xabc123")
	punkIndex := big.NewInt(42)

	punkBoughtSig := crypto.Keccak256Hash([]byte("PunkBought(uint256,uint256,address,address)"))
	punkTransferSig := crypto.Keccak256Hash([]byte("PunkTransfer(address,address,uint256)"))

	mockClient.EXPECT().
		FilterLogs(gomock.Any(), gomock.Any()).
		Return([]types.Log{
			{
				Address:     common.HexToAddress(contractAddr),
				BlockNumber: 500,
				TxHash:      txHash,
				TxIndex:     1,
				Index:       0,
				Topics: []common.Hash{
					punkBoughtSig,
					common.BigToHash(punkIndex),
					common.BytesToHash(seller.Bytes()),
					common.BytesToHash(buyer.Bytes()),
				},
				Data: common.LeftPadBytes(big.NewInt(1_000_000_000_000_000_000).Bytes(), 32),
			},
			{
				Address:     common.HexToAddress(contractAddr),
				BlockNumber: 500,
				TxHash:      txHash,
				TxIndex:     1,
				Index:       1,
				Topics: []common.Hash{
					punkTransferSig,
					common.BytesToHash(seller.Bytes()),
					common.BytesToHash(buyer.Bytes()),
				},
				Data: common.LeftPadBytes(punkIndex.Bytes(), 32),
			},
		}, nil).AnyTimes()

	adp, err := registry.BuildGenericAdapterFromConfig(registry.ContractConfig{
		Chain:          domain.ChainEthereumMainnet,
		Address:        contractAddr,
		OwnershipModel: adapters.OwnershipSingleOwner,
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
				{
					Signature:          "PunkBought(uint256,uint256,address,address)",
					MapToStandardEvent: domain.EventTypeTransfer,
					IndexedParams:      []string{"punkIndex", "fromAddress", "toAddress"},
					DataParams:         []string{},
					ParameterMappings: map[string]string{
						"punkIndex":   "TokenNumber",
						"fromAddress": "FromAddress",
						"toAddress":   "ToAddress",
					},
				},
			},
		},
	}, abiRegistry, mockClient, pagination, nil, domain.ChainEthereumMainnet)
	require.NoError(t, err)

	events, err := adp.GetTokenEvents(context.Background(), contractAddr, "42")
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, domain.EventTypeTransfer, events[0].EventType)
	require.Equal(t, "42", events[0].TokenNumber)
	require.Equal(t, txHash.Hex(), events[0].TxHash)
	require.NotNil(t, events[0].FromAddress)
	require.NotNil(t, events[0].ToAddress)
	require.Equal(t, seller.Hex(), *events[0].FromAddress)
	require.Equal(t, buyer.Hex(), *events[0].ToAddress)
	require.Equal(t, uint64(0), events[0].LogIndex)
}

func TestGenericAdapter_GetTokenEvents_RepairsBrokenAcceptBidPunkBought(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEthClient(ctrl)
	mockBlock := mocks.NewMockBlockProvider(ctrl)
	mockBlock.EXPECT().GetLatestBlock(gomock.Any()).Return(uint64(20_000_000), nil).AnyTimes()
	mockBlock.EXPECT().GetBlockTimestamp(gomock.Any(), uint64(13633896)).Return(time.Unix(1_700_000_000, 0), nil)

	const cryptopunksEventsABI = `[
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true, "name": "punkIndex", "type": "uint256"},
				{"indexed": false, "name": "value", "type": "uint256"},
				{"indexed": true, "name": "fromAddress", "type": "address"},
				{"indexed": true, "name": "toAddress", "type": "address"}
			],
			"name": "PunkBought",
			"type": "event"
		}
	]`

	abiRegistry, err := helpers.NewABIRegistry(fstest.MapFS{
		"abis/cryptopunks.json":        {Data: []byte(cryptopunksABI)},
		"abis/cryptopunks_events.json": {Data: []byte(cryptopunksEventsABI)},
	})
	require.NoError(t, err)

	pagination := helpers.NewPaginationHelper(mockClient, ethadapter.NewClock(), mockBlock)

	contractAddr := "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb"
	seller := common.HexToAddress("0xc480fb0ebea2591470f571436926785be5ebcd22")
	buyer := common.HexToAddress("0x9df6a358688ccdc2a955568a05aacac7a998a319")
	txHash := common.HexToHash("0x6ea422c6920d55742ae58afb8310cf8663f3709ebc7ef4589120f2675f343972")
	punkBoughtSig := crypto.Keccak256Hash([]byte("PunkBought(uint256,uint256,address,address)"))

	var filterCalls int
	mockClient.EXPECT().
		FilterLogs(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ ethereum.FilterQuery) ([]types.Log, error) {
			if filterCalls == 0 {
				filterCalls++
				return []types.Log{{
					Address:     common.HexToAddress(contractAddr),
					BlockNumber: 13633896,
					TxHash:      txHash,
					Topics: []common.Hash{
						punkBoughtSig,
						common.BigToHash(big.NewInt(6690)),
						common.BytesToHash(seller.Bytes()),
						{},
					},
					Data: common.Hash{}.Bytes(),
				}}, nil
			}
			return nil, nil
		}).AnyTimes()

	mockClient.EXPECT().
		TransactionReceipt(gomock.Any(), txHash).
		Return(&types.Receipt{Logs: []*types.Log{{
			Address: common.HexToAddress(contractAddr),
			Topics: []common.Hash{
				helpers.TransferEventSignature,
				common.BytesToHash(seller.Bytes()),
				common.BytesToHash(buyer.Bytes()),
			},
			Data: common.LeftPadBytes(big.NewInt(1).Bytes(), 32),
		}}}, nil)

	adp, err := registry.BuildGenericAdapterFromConfig(registry.ContractConfig{
		Chain:          domain.ChainEthereumMainnet,
		Address:        contractAddr,
		OwnershipModel: adapters.OwnershipSingleOwner,
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
			Events: []adapters.EventConfig{{
				Signature:          "PunkBought(uint256,uint256,address,address)",
				MapToStandardEvent: domain.EventTypeTransfer,
				IndexedParams:      []string{"punkIndex", "fromAddress", "toAddress"},
				ParameterMappings: map[string]string{
					"punkIndex":   "TokenNumber",
					"fromAddress": "FromAddress",
					"toAddress":   "ToAddress",
				},
			}},
		},
	}, abiRegistry, mockClient, pagination, mockBlock, domain.ChainEthereumMainnet)
	require.NoError(t, err)

	events, err := adp.GetTokenEvents(context.Background(), contractAddr, "6690")
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, domain.EventTypeTransfer, events[0].EventType)
	require.Equal(t, buyer.Hex(), *events[0].ToAddress)
}

// TestGenericAdapter_GetTokensByOwner_CorruptedPunkBoughtBuyerDiscovery tests that owner sweeps
// can discover CryptoPunks buyers even when the PunkBought log has zero indexed toAddress.
// This uses the internal Transfer(seller, buyer, 1) event to find the real buyer.
func TestGenericAdapter_GetTokensByOwner_CorruptedPunkBoughtBuyerDiscovery(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEthClient(ctrl)
	mockBlock := mocks.NewMockBlockProvider(ctrl)
	mockBlock.EXPECT().GetLatestBlock(gomock.Any()).Return(uint64(20_000_000), nil).AnyTimes()
	mockBlock.EXPECT().GetBlockTimestamp(gomock.Any(), uint64(13633896)).Return(time.Unix(1_700_000_000, 0), nil)

	const cryptopunksEventsABI = `[
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true, "name": "punkIndex", "type": "uint256"},
				{"indexed": false, "name": "value", "type": "uint256"},
				{"indexed": true, "name": "fromAddress", "type": "address"},
				{"indexed": true, "name": "toAddress", "type": "address"}
			],
			"name": "PunkBought",
			"type": "event"
		}
	]`

	abiRegistry, err := helpers.NewABIRegistry(fstest.MapFS{
		"abis/cryptopunks.json":        {Data: []byte(cryptopunksABI)},
		"abis/cryptopunks_events.json": {Data: []byte(cryptopunksEventsABI)},
	})
	require.NoError(t, err)

	pagination := helpers.NewPaginationHelper(mockClient, ethadapter.NewClock(), mockBlock)

	contractAddr := "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb"
	seller := common.HexToAddress("0xc480fb0ebea2591470f571436926785be5ebcd22")
	buyer := common.HexToAddress("0x9df6a358688ccdc2a955568a05aacac7a998a319")
	txHash := common.HexToHash("0x6ea422c6920d55742ae58afb8310cf8663f3709ebc7ef4589120f2675f343972")
	punkBoughtSig := crypto.Keccak256Hash([]byte("PunkBought(uint256,uint256,address,address)"))

	// When querying for PunkBought with buyer in indexed toAddress (topic[3]), return nothing
	// because the corrupted log has zero in that position.
	// When querying for PunkBought with seller in indexed fromAddress (topic[2]), return the
	// corrupted PunkBought log (this is how we discover it for seller-side queries, which
	// we'll then filter/repair based on the buyer).
	// When querying for internal Transfer with buyer in topic[2], return the internal Transfer log.
	mockClient.EXPECT().
		FilterLogs(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
			// Check if this is a PunkBought query with seller in topic[2]
			if len(q.Topics) >= 3 && len(q.Topics[0]) > 0 && q.Topics[0][0] == punkBoughtSig {
				if len(q.Topics) >= 3 && q.Topics[2] != nil && len(q.Topics[2]) > 0 {
					// Query for seller in fromAddress (topic[2])
					if q.Topics[2][0] == common.BytesToHash(seller.Bytes()) {
						// Return the corrupted PunkBought log
						return []types.Log{{
							Address:     common.HexToAddress(contractAddr),
							BlockNumber: 13633896,
							TxHash:      txHash,
							TxIndex:     1,
							Index:       0,
							Topics: []common.Hash{
								punkBoughtSig,
								common.BigToHash(big.NewInt(6690)),
								common.BytesToHash(seller.Bytes()),
								{}, // Zero toAddress in indexed topic
							},
							Data: common.Hash{}.Bytes(),
						}}, nil
					}
				}
			}

			// Check if this is the internal Transfer query (topic[0] = Transfer, topic[2] = buyer)
			if len(q.Topics) == 3 && q.Topics[0][0] == helpers.TransferEventSignature {
				if q.Topics[2] != nil && len(q.Topics[2]) > 0 && q.Topics[2][0] == common.BytesToHash(buyer.Bytes()) {
					// Return the internal Transfer log
					return []types.Log{{
						Address:     common.HexToAddress(contractAddr),
						BlockNumber: 13633896,
						TxHash:      txHash,
						TxIndex:     1,
						Index:       1,
						Topics: []common.Hash{
							helpers.TransferEventSignature,
							common.BytesToHash(seller.Bytes()),
							common.BytesToHash(buyer.Bytes()),
						},
						Data: common.LeftPadBytes(big.NewInt(1).Bytes(), 32),
					}}, nil
				}
			}

			// All other queries return nothing
			return nil, nil
		}).AnyTimes()

	// When repair code fetches the receipt, provide both the corrupted PunkBought and internal Transfer
	mockClient.EXPECT().
		TransactionReceipt(gomock.Any(), txHash).
		Return(&types.Receipt{Logs: []*types.Log{
			{
				Address:     common.HexToAddress(contractAddr),
				BlockNumber: 13633896,
				TxHash:      txHash,
				TxIndex:     1,
				Index:       0,
				Topics: []common.Hash{
					punkBoughtSig,
					common.BigToHash(big.NewInt(6690)),
					common.BytesToHash(seller.Bytes()),
					{}, // Zero toAddress in indexed topic
				},
				Data: common.Hash{}.Bytes(),
			},
			{
				Address:     common.HexToAddress(contractAddr),
				BlockNumber: 13633896,
				TxHash:      txHash,
				TxIndex:     1,
				Index:       1,
				Topics: []common.Hash{
					helpers.TransferEventSignature,
					common.BytesToHash(seller.Bytes()),
					common.BytesToHash(buyer.Bytes()),
				},
				Data: common.LeftPadBytes(big.NewInt(1).Bytes(), 32),
			},
		}}, nil).Times(2) // Called twice: once in GetOwnerLogs, once in repair

	adp, err := registry.BuildGenericAdapterFromConfig(registry.ContractConfig{
		Chain:          domain.ChainEthereumMainnet,
		Address:        contractAddr,
		OwnershipModel: adapters.OwnershipSingleOwner,
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
			Events: []adapters.EventConfig{{
				Signature:          "PunkBought(uint256,uint256,address,address)",
				MapToStandardEvent: domain.EventTypeTransfer,
				IndexedParams:      []string{"punkIndex", "fromAddress", "toAddress"},
				ParameterMappings: map[string]string{
					"punkIndex":   "TokenNumber",
					"fromAddress": "FromAddress",
					"toAddress":   "ToAddress",
				},
			}},
		},
	}, abiRegistry, mockClient, pagination, mockBlock, domain.ChainEthereumMainnet)
	require.NoError(t, err)

	// Query for tokens owned by the buyer
	tokens, err := adp.GetTokensByOwner(context.Background(), buyer.Hex(), 0, 20_000_000, nil)
	require.NoError(t, err)

	// Should discover punk #6690 via the internal Transfer event, even though the PunkBought
	// log has zero in the indexed toAddress position
	require.Len(t, tokens, 1)
	_, _, _, tokenNumber := tokens[0].TokenCID.Parse()
	require.Equal(t, "6690", tokenNumber)
	require.Equal(t, uint64(13633896), tokens[0].BlockNumber)
}

// TestGenericAdapter_GetTokenEvents_UnrelatedCorruptedPunkBoughtDoesNotFailFiltering
// is a regression test for the repair-before-filter bug where GetTokenEvents would fail
// when querying token N if an unrelated token M had a corrupted PunkBought that couldn't be repaired.
// After fix, the single-event repair is skipped in ParseEvent, and only post-filter events are repaired.
func TestGenericAdapter_GetTokenEvents_UnrelatedCorruptedPunkBoughtDoesNotFailFiltering(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEthClient(ctrl)
	mockBlock := mocks.NewMockBlockProvider(ctrl)
	mockBlock.EXPECT().GetLatestBlock(gomock.Any()).Return(uint64(20_000_000), nil).AnyTimes()
	mockBlock.EXPECT().GetBlockTimestamp(gomock.Any(), uint64(500)).Return(time.Unix(1_700_000_000, 0), nil).AnyTimes()
	mockBlock.EXPECT().GetBlockTimestamp(gomock.Any(), uint64(600)).Return(time.Unix(1_700_001_000, 0), nil).AnyTimes()

	const cryptopunksEventsABI = `[
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true, "name": "punkIndex", "type": "uint256"},
				{"indexed": false, "name": "value", "type": "uint256"},
				{"indexed": true, "name": "fromAddress", "type": "address"},
				{"indexed": true, "name": "toAddress", "type": "address"}
			],
			"name": "PunkBought",
			"type": "event"
		}
	]`

	abiRegistry, err := helpers.NewABIRegistry(fstest.MapFS{
		"abis/cryptopunks.json":        {Data: []byte(cryptopunksABI)},
		"abis/cryptopunks_events.json": {Data: []byte(cryptopunksEventsABI)},
	})
	require.NoError(t, err)

	pagination := helpers.NewPaginationHelper(mockClient, ethadapter.NewClock(), mockBlock)

	contractAddr := "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb"
	seller1 := common.HexToAddress("0x1111111111111111111111111111111111111111")
	seller2 := common.HexToAddress("0x2222222222222222222222222222222222222222")
	buyer2 := common.HexToAddress("0x3333333333333333333333333333333333333333")
	txHash1 := common.HexToHash("0xabc123")
	txHash2 := common.HexToHash("0xdef456")
	punkBoughtSig := crypto.Keccak256Hash([]byte("PunkBought(uint256,uint256,address,address)"))

	// FilterLogs returns corrupted PunkBought for token 10 (unrepairable, no internal Transfer),
	// followed by valid PunkBought for target token 42
	mockClient.EXPECT().
		FilterLogs(gomock.Any(), gomock.Any()).
		Return([]types.Log{
			{
				Address:     common.HexToAddress(contractAddr),
				BlockNumber: 500,
				TxHash:      txHash1,
				Index:       0,
				Topics: []common.Hash{
					punkBoughtSig,
					common.BigToHash(big.NewInt(10)), // Token 10 (unrelated)
					common.BytesToHash(seller1.Bytes()),
					{}, // Zero toAddress - corrupted
				},
				Data: common.Hash{}.Bytes(),
			},
			{
				Address:     common.HexToAddress(contractAddr),
				BlockNumber: 600,
				TxHash:      txHash2,
				Index:       0,
				Topics: []common.Hash{
					punkBoughtSig,
					common.BigToHash(big.NewInt(42)), // Token 42 (target)
					common.BytesToHash(seller2.Bytes()),
					common.BytesToHash(buyer2.Bytes()),
				},
				Data: common.LeftPadBytes(big.NewInt(1_000_000_000_000_000_000).Bytes(), 32),
			},
		}, nil).AnyTimes()

	// TransactionReceipt for token 10's corrupted tx returns empty logs (no internal Transfer to repair from)
	mockClient.EXPECT().
		TransactionReceipt(gomock.Any(), txHash1).
		Return(&types.Receipt{Logs: []*types.Log{}}, nil).
		MaxTimes(1)

	adp, err := registry.BuildGenericAdapterFromConfig(registry.ContractConfig{
		Chain:          domain.ChainEthereumMainnet,
		Address:        contractAddr,
		OwnershipModel: adapters.OwnershipSingleOwner,
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
			Events: []adapters.EventConfig{{
				Signature:          "PunkBought(uint256,uint256,address,address)",
				MapToStandardEvent: domain.EventTypeTransfer,
				IndexedParams:      []string{"punkIndex", "fromAddress", "toAddress"},
				DataParams:         []string{"value"},
				ParameterMappings: map[string]string{
					"punkIndex":   "TokenNumber",
					"fromAddress": "FromAddress",
					"toAddress":   "ToAddress",
				},
			}},
		},
	}, abiRegistry, mockClient, pagination, mockBlock, domain.ChainEthereumMainnet)
	require.NoError(t, err)

	// Query for token 42 - should succeed even though token 10's corrupted event can't be repaired
	// because the single-event repair is now skipped in ParseEvent and only post-filter repair runs
	events, err := adp.GetTokenEvents(context.Background(), contractAddr, "42")
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, domain.EventTypeTransfer, events[0].EventType)
	require.Equal(t, "42", events[0].TokenNumber)
	require.NotNil(t, events[0].ToAddress)
	require.Equal(t, buyer2.Hex(), *events[0].ToAddress)
}

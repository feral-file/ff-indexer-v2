package registry_test

import (
	"context"
	"errors"
	"math/big"
	"testing"
	"testing/fstest"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	ethadapter "github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/block"
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

const cryptopunksContractConfigWithEvents = `{
  "contracts": [
    {
      "chain": "eip155:1",
      "address": "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
      "name": "CryptoPunks",
      "ownership_model": "single_owner",
      "adapter": {
        "existence": {
          "method": "punkIndexToAddress",
          "abi": "cryptopunks",
          "params": ["${tokenId}"],
          "success_condition": "address_nonzero"
        },
        "owner": {
          "method": "punkIndexToAddress",
          "abi": "cryptopunks",
          "params": ["${tokenId}"]
        },
        "metadata": {
          "source": "vendor_only"
        },
        "events": [
          {
            "signature": "PunkTransfer(address,address,uint256)",
            "mapToStandardEvent": "transfer",
            "indexedParams": ["from", "to"],
            "dataParams": ["punkIndex"],
            "parameterMappings": {
              "from": "FromAddress",
              "to": "ToAddress",
              "punkIndex": "TokenNumber"
            }
          },
          {
            "signature": "Assign(address,uint256)",
            "mapToStandardEvent": "mint",
            "indexedParams": ["to"],
            "dataParams": ["punkIndex"],
            "parameterMappings": {
              "to": "ToAddress",
              "punkIndex": "TokenNumber"
            }
          },
          {
            "signature": "PunkBought(uint256,uint256,address,address)",
            "mapToStandardEvent": "transfer",
            "indexedParams": ["punkIndex", "fromAddress", "toAddress"],
            "dataParams": [],
            "parameterMappings": {
              "punkIndex": "TokenNumber",
              "fromAddress": "FromAddress",
              "toAddress": "ToAddress"
            }
          }
        ]
      }
    }
  ]
}`

const cryptopunksContractConfig = `{
  "contracts": [
    {
      "chain": "eip155:1",
      "address": "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
      "name": "CryptoPunks",
      "ownership_model": "single_owner",
      "adapter": {
        "existence": {
          "method": "punkIndexToAddress",
          "abi": "cryptopunks",
          "params": ["${tokenId}"],
          "success_condition": "address_nonzero"
        },
        "owner": {
          "method": "punkIndexToAddress",
          "abi": "cryptopunks",
          "params": ["${tokenId}"]
        },
        "metadata": {
          "source": "vendor_only"
        }
      }
    }
  ]
}`

func testContractFS(t *testing.T, contractsJSON string) fstest.MapFS {
	t.Helper()

	return fstest.MapFS{
		"contracts.json":        {Data: []byte(contractsJSON)},
		"abis/cryptopunks.json": {Data: []byte(cryptopunksABI)},
	}
}

func newTestRegistry(t *testing.T, mockClient *mocks.MockEthClient, fs fstest.MapFS) *registry.AdapterRegistry {
	return newTestRegistryWithBlockProvider(t, mockClient, nil, fs)
}

func newTestRegistryWithBlockProvider(
	t *testing.T,
	mockClient *mocks.MockEthClient,
	blockProvider block.BlockProvider,
	fs fstest.MapFS,
) *registry.AdapterRegistry {
	t.Helper()

	pagination := helpers.NewPaginationHelper(mockClient, ethadapter.NewClock(), blockProvider)
	reg, err := registry.NewAdapterRegistry(
		fs,
		mockClient,
		blockProvider,
		pagination,
		domain.ChainEthereumMainnet,
	)
	require.NoError(t, err)
	return reg
}

func TestAdapterRegistry_GetAdapter(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEthClient(ctrl)

	reg := newTestRegistry(t, mockClient, testContractFS(t, cryptopunksContractConfig))
	require.Equal(t, 1, reg.ContractOverrideCount())

	t.Run("configured contract uses generic adapter", func(t *testing.T) {
		adp, err := reg.GetAdapter(
			domain.ChainEthereumMainnet,
			"0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
			domain.StandardERC721,
		)
		require.NoError(t, err)
		require.False(t, adp.SupportsProvenance())
	})

	t.Run("unknown contract uses standard adapter", func(t *testing.T) {
		ownerAddr := common.HexToAddress("0x1234567890123456789012345678901234567890")
		mockClient.EXPECT().
			CallContract(gomock.Any(), gomock.Any(), gomock.Nil()).
			Return(common.LeftPadBytes(ownerAddr.Bytes(), 32), nil)

		adp, err := reg.GetAdapter(
			domain.ChainEthereumMainnet,
			"0x0000000000000000000000000000000000000001",
			domain.StandardERC721,
		)
		require.NoError(t, err)
		require.True(t, adp.SupportsProvenance())

		exists, err := adp.TokenExists(context.Background(), "0x0000000000000000000000000000000000000001", "1")
		require.NoError(t, err)
		require.True(t, exists)
	})

	t.Run("unsupported standard returns error", func(t *testing.T) {
		_, err := reg.GetAdapter(
			domain.ChainEthereumMainnet,
			"0x0000000000000000000000000000000000000001",
			domain.StandardFA2,
		)
		require.ErrorIs(t, err, adapters.ErrUnsupportedContractStandard)
	})

	t.Run("configured contract rejects CID standard mismatch", func(t *testing.T) {
		_, err := reg.GetAdapter(
			domain.ChainEthereumMainnet,
			"0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
			domain.StandardERC1155,
		)
		require.ErrorIs(t, err, adapters.ErrConfiguredStandardMismatch)
	})

	t.Run("vendor only metadata flag", func(t *testing.T) {
		require.True(t, reg.IsVendorOnlyMetadata(
			domain.ChainEthereumMainnet,
			"0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
		))
		require.False(t, reg.IsVendorOnlyMetadata(
			domain.ChainEthereumMainnet,
			"0x0000000000000000000000000000000000000001",
		))
	})
}

func TestAdapterRegistry_EmptyConfig(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEthClient(ctrl)

	reg := newTestRegistry(t, mockClient, testContractFS(t, `{"contracts": []}`))
	require.Equal(t, 0, reg.ContractOverrideCount())

	adp, err := reg.GetAdapter(domain.ChainEthereumMainnet, "0xabc", domain.StandardERC1155)
	require.NoError(t, err)
	require.True(t, adp.SupportsProvenance())
}

func TestAdapterRegistry_SupportsProvenance(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEthClient(ctrl)

	reg := newTestRegistry(t, mockClient, testContractFS(t, cryptopunksContractConfig))

	supported, err := reg.SupportsProvenance(
		domain.ChainEthereumMainnet,
		"0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
		domain.StandardERC721,
	)
	require.NoError(t, err)
	require.False(t, supported)

	supported, err = reg.SupportsProvenance(
		domain.ChainEthereumMainnet,
		"0x0000000000000000000000000000000000000001",
		domain.StandardERC721,
	)
	require.NoError(t, err)
	require.True(t, supported)

	_, err = reg.SupportsProvenance(
		domain.ChainEthereumMainnet,
		"0x0000000000000000000000000000000000000001",
		domain.StandardFA2,
	)
	require.ErrorIs(t, err, adapters.ErrUnsupportedContractStandard)
}

func TestAdapterRegistry_SupportsProvenance_WithCustomEvents(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEthClient(ctrl)

	reg := newTestRegistry(t, mockClient, testContractFS(t, cryptopunksContractConfigWithEvents))

	supported, err := reg.SupportsProvenance(
		domain.ChainEthereumMainnet,
		"0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
		domain.StandardERC721,
	)
	require.NoError(t, err)
	require.True(t, supported)

	adp, err := reg.GetAdapter(
		domain.ChainEthereumMainnet,
		"0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
		domain.StandardERC721,
	)
	require.NoError(t, err)
	require.Len(t, adp.GetEventSignatures(), 3)
}

func TestAdapterRegistry_GetCustomEventSignaturesForChain(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEthClient(ctrl)

	reg := newTestRegistry(t, mockClient, testContractFS(t, cryptopunksContractConfigWithEvents))

	mainnetSigs := reg.GetCustomEventSignaturesForChain(domain.ChainEthereumMainnet)
	require.Len(t, mainnetSigs, 3)

	otherChainSigs := reg.GetCustomEventSignaturesForChain(domain.Chain("eip155:5"))
	require.Empty(t, otherChainSigs)
}

func TestAdapterRegistry_ParseEvent_SkipsUnconfiguredCustomSignatureAddress(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEthClient(ctrl)

	reg := newTestRegistry(t, mockClient, testContractFS(t, cryptopunksContractConfigWithEvents))

	punkTransferSig := reg.GetCustomEventSignaturesForChain(domain.ChainEthereumMainnet)[0]
	from := common.HexToAddress("0x1111111111111111111111111111111111111111")
	to := common.HexToAddress("0x2222222222222222222222222222222222222222")
	punkIndex := big.NewInt(42)

	vLog := types.Log{
		Address: common.HexToAddress("0x9999999999999999999999999999999999999999"),
		Topics: []common.Hash{
			punkTransferSig,
			common.BytesToHash(from.Bytes()),
			common.BytesToHash(to.Bytes()),
		},
		Data: common.LeftPadBytes(punkIndex.Bytes(), 32),
	}

	parsed, err := reg.ParseEvent(context.Background(), vLog, domain.ChainEthereumMainnet)
	require.ErrorIs(t, err, adapters.ErrUnconfiguredContract)
	require.Nil(t, parsed)
}

func TestAdapterRegistry_GetProvenanceContractsForChain(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEthClient(ctrl)

	reg := newTestRegistry(t, mockClient, testContractFS(t, cryptopunksContractConfigWithEvents))

	contracts := reg.GetProvenanceContractsForChain(domain.ChainEthereumMainnet)
	require.Len(t, contracts, 1)
	require.Contains(t, contracts, "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb")
	require.True(t, contracts["0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb"].SupportsProvenance())
}

func TestAdapterRegistry_ParseEvent_StandardERC721Transfer(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEthClient(ctrl)
	reg := newTestRegistry(t, mockClient, testContractFS(t, `{"contracts": []}`))

	from := common.HexToAddress("0x1111111111111111111111111111111111111111")
	to := common.HexToAddress("0x2222222222222222222222222222222222222222")
	tokenID := big.NewInt(42)

	vLog := typesLog(from, to, tokenID)
	parsed, err := reg.ParseEvent(context.Background(), vLog, domain.ChainEthereumMainnet)
	require.NoError(t, err)
	require.Equal(t, domain.StandardERC721, parsed.Standard)
	require.Equal(t, "42", parsed.TokenNumber)
}

func TestAdapterRegistry_ParseEvent_SkipsERC20Transfer(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEthClient(ctrl)
	mockBlock := mocks.NewMockBlockProvider(ctrl)
	reg := newTestRegistryWithBlockProvider(t, mockClient, mockBlock, testContractFS(t, `{"contracts": []}`))

	from := common.HexToAddress("0x1111111111111111111111111111111111111111")
	to := common.HexToAddress("0x2222222222222222222222222222222222222222")
	vLog := types.Log{
		Address:     common.HexToAddress("0x0000000000000000000000000000000000000001"),
		BlockNumber: 100,
		Topics: []common.Hash{
			helpers.TransferEventSignature,
			common.BytesToHash(from.Bytes()),
			common.BytesToHash(to.Bytes()),
		},
	}

	parsed, err := reg.ParseEvent(context.Background(), vLog, domain.ChainEthereumMainnet)
	require.NoError(t, err)
	require.Nil(t, parsed)
}

func TestAdapterRegistry_ParseEvent_TimestampLookupFails(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEthClient(ctrl)
	mockBlock := mocks.NewMockBlockProvider(ctrl)
	reg := newTestRegistryWithBlockProvider(t, mockClient, mockBlock, testContractFS(t, `{"contracts": []}`))

	from := common.HexToAddress("0x1111111111111111111111111111111111111111")
	to := common.HexToAddress("0x2222222222222222222222222222222222222222")
	tokenID := big.NewInt(42)
	vLog := typesLog(from, to, tokenID)
	vLog.BlockNumber = 100

	timestampErr := errors.New("boom")
	mockBlock.EXPECT().
		GetBlockTimestamp(gomock.Any(), uint64(100)).
		Return(time.Time{}, timestampErr)

	parsed, err := reg.ParseEvent(context.Background(), vLog, domain.ChainEthereumMainnet)
	require.Error(t, err)
	require.Nil(t, parsed)
	require.ErrorIs(t, err, timestampErr)
	require.Contains(t, err.Error(), "resolve block timestamp for block 100")
}

// typesLog builds a minimal ERC721 Transfer log for tests.
func typesLog(from, to common.Address, tokenID *big.Int) types.Log {
	return types.Log{
		Address: common.HexToAddress("0x0000000000000000000000000000000000000001"),
		Topics: []common.Hash{
			common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"),
			common.BytesToHash(from.Bytes()),
			common.BytesToHash(to.Bytes()),
			common.BigToHash(tokenID),
		},
	}
}

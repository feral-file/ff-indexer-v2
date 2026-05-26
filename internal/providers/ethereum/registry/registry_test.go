package registry_test

import (
	"context"
	"math/big"
	"testing"
	"testing/fstest"

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

const cryptopunksContractConfigWithEvents = `{
  "contracts": [
    {
      "chain": "eip155:1",
      "address": "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
      "name": "CryptoPunks",
      "standard": "erc721",
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
      "standard": "erc721",
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
	t.Helper()

	pagination := helpers.NewPaginationHelper(mockClient, ethadapter.NewClock(), nil)
	reg, err := registry.NewAdapterRegistry(
		fs,
		mockClient,
		ethadapter.NewClock(),
		nil,
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
	require.Len(t, adp.GetEventSignatures(), 2)
}

func TestAdapterRegistry_GetAllCustomEventSignatures(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEthClient(ctrl)

	reg := newTestRegistry(t, mockClient, testContractFS(t, cryptopunksContractConfigWithEvents))

	signatures := reg.GetAllCustomEventSignatures()
	require.Len(t, signatures, 2)
}

func TestAdapterRegistry_ParseEvent_StandardERC721Transfer(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEthClient(ctrl)
	reg := newTestRegistry(t, mockClient, testContractFS(t, `{"contracts": []}`))

	from := common.HexToAddress("0x1111111111111111111111111111111111111111")
	to := common.HexToAddress("0x2222222222222222222222222222222222222222")
	tokenID := big.NewInt(42)

	vLog := typesLog(from, to, tokenID)
	event := &domain.BlockchainEvent{Chain: domain.ChainEthereumMainnet}
	parsed, err := reg.ParseEvent(context.Background(), domain.ChainEthereumMainnet, vLog, event)
	require.NoError(t, err)
	require.Equal(t, domain.StandardERC721, parsed.Standard)
	require.Equal(t, "42", parsed.TokenNumber)
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

package adapter_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/adapter"
)

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

func TestAdapterRegistry_GetAdapter(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEthClient(ctrl)
	ops := &stubStandardOps{owner: "0x1234567890123456789012345678901234567890"}

	registry, err := adapter.NewAdapterRegistry(
		testContractFS(t, cryptopunksContractConfig),
		mockClient,
		ops,
	)
	require.NoError(t, err)
	require.Equal(t, 1, registry.ContractOverrideCount())

	t.Run("configured contract uses generic adapter", func(t *testing.T) {
		adp, err := registry.GetAdapter(
			domain.ChainEthereumMainnet,
			"0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
			domain.StandardERC721,
		)
		require.NoError(t, err)
		require.False(t, adp.SupportsProvenance())
	})

	t.Run("unknown contract uses standard adapter", func(t *testing.T) {
		adp, err := registry.GetAdapter(
			domain.ChainEthereumMainnet,
			"0x0000000000000000000000000000000000000001",
			domain.StandardERC721,
		)
		require.NoError(t, err)
		require.True(t, adp.SupportsProvenance())

		exists, err := adp.TokenExists(context.Background(), "0x0000000000000000000000000000000000000001", "1")
		require.NoError(t, err)
		require.True(t, exists)
		require.True(t, ops.ownerOfCalled)
	})

	t.Run("unsupported standard returns error", func(t *testing.T) {
		_, err := registry.GetAdapter(
			domain.ChainEthereumMainnet,
			"0x0000000000000000000000000000000000000001",
			domain.StandardFA2,
		)
		require.ErrorIs(t, err, adapter.ErrUnsupportedContractStandard)
	})

	t.Run("vendor only metadata flag", func(t *testing.T) {
		require.True(t, registry.IsVendorOnlyMetadata(
			domain.ChainEthereumMainnet,
			"0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
		))
		require.False(t, registry.IsVendorOnlyMetadata(
			domain.ChainEthereumMainnet,
			"0x0000000000000000000000000000000000000001",
		))
	})
}

func TestAdapterRegistry_EmptyConfig(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEthClient(ctrl)

	registry, err := adapter.NewAdapterRegistry(
		testContractFS(t, `{"contracts": []}`),
		mockClient,
		&stubStandardOps{},
	)
	require.NoError(t, err)
	require.Equal(t, 0, registry.ContractOverrideCount())

	adp, err := registry.GetAdapter(domain.ChainEthereumMainnet, "0xabc", domain.StandardERC1155)
	require.NoError(t, err)
	require.True(t, adp.SupportsProvenance())
}

func TestAdapterRegistry_SupportsProvenance(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEthClient(ctrl)

	registry, err := adapter.NewAdapterRegistry(
		testContractFS(t, cryptopunksContractConfig),
		mockClient,
		&stubStandardOps{},
	)
	require.NoError(t, err)

	supported, err := registry.SupportsProvenance(
		domain.ChainEthereumMainnet,
		"0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
		domain.StandardERC721,
	)
	require.NoError(t, err)
	require.False(t, supported)

	supported, err = registry.SupportsProvenance(
		domain.ChainEthereumMainnet,
		"0x0000000000000000000000000000000000000001",
		domain.StandardERC721,
	)
	require.NoError(t, err)
	require.True(t, supported)

	_, err = registry.SupportsProvenance(
		domain.ChainEthereumMainnet,
		"0x0000000000000000000000000000000000000001",
		domain.StandardFA2,
	)
	require.ErrorIs(t, err, adapter.ErrUnsupportedContractStandard)
}

package registry_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/adapters"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/registry"
)

func TestContractConfig_CIDStandard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		model    adapters.OwnershipModel
		expected domain.ChainStandard
	}{
		{name: "single owner maps to erc721", model: adapters.OwnershipSingleOwner, expected: domain.StandardERC721},
		{name: "multi holder maps to erc1155", model: adapters.OwnershipMultiHolder, expected: domain.StandardERC1155},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := registry.ContractConfig{OwnershipModel: tt.model}
			require.Equal(t, tt.expected, cfg.CIDStandard())
		})
	}
}

func TestLoadContractsConfig_OwnershipModelRequired(t *testing.T) {
	_, err := registry.LoadContractsConfig(testContractFS(t, `{
		"contracts": [{
			"chain": "eip155:1",
			"address": "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
			"adapter": {
				"existence": {"method": "punkIndexToAddress", "abi": "cryptopunks", "params": ["${tokenId}"]},
				"owner": {"method": "punkIndexToAddress", "abi": "cryptopunks", "params": ["${tokenId}"]}
			}
		}]
	}`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "ownership_model is required")
}

package adapter_test

import (
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/adapter"
)

func TestLoadContractsConfig_Valid(t *testing.T) {
	cfg, err := adapter.LoadContractsConfig(testContractFS(t, cryptopunksContractConfig))
	require.NoError(t, err)
	require.Len(t, cfg.Contracts, 1)
	require.Equal(t, "CryptoPunks", cfg.Contracts[0].Name)
	require.Equal(t, domain.ChainEthereumMainnet, cfg.Contracts[0].Chain)
}

func TestLoadContractsConfig_EmptyContracts(t *testing.T) {
	cfg, err := adapter.LoadContractsConfig(testContractFS(t, `{"contracts": []}`))
	require.NoError(t, err)
	require.Empty(t, cfg.Contracts)
}

func TestLoadContractsConfig_InvalidJSON(t *testing.T) {
	_, err := adapter.LoadContractsConfig(fstest.MapFS{
		"contracts.json": {Data: []byte("{invalid")},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "parse contracts config")
}

func TestLoadContractsConfig_MissingRequiredFields(t *testing.T) {
	_, err := adapter.LoadContractsConfig(testContractFS(t, `{
		"contracts": [{
			"chain": "eip155:1",
			"address": "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
			"standard": "erc721",
			"adapter": {
				"existence": {"method": "punkIndexToAddress", "abi": "cryptopunks", "params": ["${tokenId}"]},
				"owner": {"method": "", "abi": "cryptopunks", "params": ["${tokenId}"]}
			}
		}]
	}`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "adapter.owner.method is required")
}

func TestLoadContractsConfig_DuplicateEntries(t *testing.T) {
	duplicateConfig := `{
		"contracts": [
			{
				"chain": "eip155:1",
				"address": "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
				"standard": "erc721",
				"adapter": {
					"existence": {"method": "punkIndexToAddress", "abi": "cryptopunks", "params": ["${tokenId}"]},
					"owner": {"method": "punkIndexToAddress", "abi": "cryptopunks", "params": ["${tokenId}"]}
				}
			},
			{
				"chain": "eip155:1",
				"address": "0xB47e3Cd837dDF8e4c57F05d70Ab865de6e193BBB",
				"standard": "erc721",
				"adapter": {
					"existence": {"method": "punkIndexToAddress", "abi": "cryptopunks", "params": ["${tokenId}"]},
					"owner": {"method": "punkIndexToAddress", "abi": "cryptopunks", "params": ["${tokenId}"]}
				}
			}
		]
	}`

	_, err := adapter.LoadContractsConfig(testContractFS(t, duplicateConfig))
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate contract entry")
}

func TestNewABIRegistry_LoadsABIs(t *testing.T) {
	registry, err := adapter.NewABIRegistry(testContractFS(t, `{"contracts": []}`))
	require.NoError(t, err)

	cryptopunks, err := registry.Get("cryptopunks")
	require.NoError(t, err)
	require.NotEmpty(t, cryptopunks.Methods)

	_, err = registry.Get("missing")
	require.Error(t, err)
	require.Contains(t, err.Error(), "ABI not found")
}

func TestNewAdapterRegistry_MissingABI(t *testing.T) {
	_, err := adapter.NewAdapterRegistry(
		testContractFS(t, `{
			"contracts": [{
				"chain": "eip155:1",
				"address": "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
				"standard": "erc721",
				"adapter": {
					"existence": {"method": "punkIndexToAddress", "abi": "missing", "params": ["${tokenId}"]},
					"owner": {"method": "punkIndexToAddress", "abi": "cryptopunks", "params": ["${tokenId}"]}
				}
			}]
		}`),
		nil,
		&stubStandardOps{},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ABI not found")
}

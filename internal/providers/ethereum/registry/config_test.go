package registry_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	ethadapter "github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/contracts"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/helpers"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/registry"
)

const cryptoPunksAddress = "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb"

func TestLoadContractsConfig_Valid(t *testing.T) {
	cfg, err := registry.LoadContractsConfig(testContractFS(t, cryptopunksContractConfig))
	require.NoError(t, err)
	require.Len(t, cfg.Contracts, 1)
	require.Equal(t, "CryptoPunks", cfg.Contracts[0].Name)
	require.Equal(t, domain.ChainEthereumMainnet, cfg.Contracts[0].Chain)
}

func TestLoadContractsConfig_EmptyContracts(t *testing.T) {
	cfg, err := registry.LoadContractsConfig(testContractFS(t, `{"contracts": []}`))
	require.NoError(t, err)
	require.Empty(t, cfg.Contracts)
}

func TestLoadContractsConfig_InvalidJSON(t *testing.T) {
	_, err := registry.LoadContractsConfig(testContractFS(t, "{invalid}"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "parse contracts config")
}

func TestLoadContractsConfig_MissingRequiredFields(t *testing.T) {
	_, err := registry.LoadContractsConfig(testContractFS(t, `{
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

func TestLoadContractsConfig_WithCustomEvents(t *testing.T) {
	cfg, err := registry.LoadContractsConfig(testContractFS(t, cryptopunksContractConfigWithEvents))
	require.NoError(t, err)
	require.Len(t, cfg.Contracts, 1)
	require.Len(t, cfg.Contracts[0].Adapter.Events, 2)
	require.Equal(t, "PunkTransfer(address,address,uint256)", cfg.Contracts[0].Adapter.Events[0].Signature)
	require.Equal(t, domain.EventTypeTransfer, cfg.Contracts[0].Adapter.Events[0].MapToStandardEvent)
}

func TestLoadContractsConfig_InvalidEventSignature(t *testing.T) {
	_, err := registry.LoadContractsConfig(testContractFS(t, `{
		"contracts": [{
			"chain": "eip155:1",
			"address": "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
			"standard": "erc721",
			"adapter": {
				"existence": {"method": "punkIndexToAddress", "abi": "cryptopunks", "params": ["${tokenId}"]},
				"owner": {"method": "punkIndexToAddress", "abi": "cryptopunks", "params": ["${tokenId}"]},
				"events": [{
					"signature": "invalid signature",
					"mapToStandardEvent": "transfer",
					"indexedParams": ["from"],
					"dataParams": [],
					"parameterMappings": {"from": "FromAddress"}
				}]
			}
		}]
	}`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid signature format")
}

func TestLoadContractsConfig_MissingParameterMapping(t *testing.T) {
	_, err := registry.LoadContractsConfig(testContractFS(t, `{
		"contracts": [{
			"chain": "eip155:1",
			"address": "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
			"standard": "erc721",
			"adapter": {
				"existence": {"method": "punkIndexToAddress", "abi": "cryptopunks", "params": ["${tokenId}"]},
				"owner": {"method": "punkIndexToAddress", "abi": "cryptopunks", "params": ["${tokenId}"]},
				"events": [{
					"signature": "PunkTransfer(address,address,uint256)",
					"mapToStandardEvent": "transfer",
					"indexedParams": ["from", "to"],
					"dataParams": ["punkIndex"],
					"parameterMappings": {
						"from": "FromAddress",
						"to": "ToAddress"
					}
				}]
			}
		}]
	}`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "parameter \"punkIndex\" is not mapped")
}

func TestLoadContractsConfig_DuplicateTargetField(t *testing.T) {
	_, err := registry.LoadContractsConfig(testContractFS(t, `{
		"contracts": [{
			"chain": "eip155:1",
			"address": "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
			"standard": "erc721",
			"adapter": {
				"existence": {"method": "punkIndexToAddress", "abi": "cryptopunks", "params": ["${tokenId}"]},
				"owner": {"method": "punkIndexToAddress", "abi": "cryptopunks", "params": ["${tokenId}"]},
				"events": [{
					"signature": "BadEvent(address,uint256,uint256)",
					"mapToStandardEvent": "mint",
					"indexedParams": ["to"],
					"dataParams": ["tokenId1", "tokenId2"],
					"parameterMappings": {
						"to": "ToAddress",
						"tokenId1": "TokenNumber",
						"tokenId2": "TokenNumber"
					}
				}]
			}
		}]
	}`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate target field")
}

func TestLoadContractsConfig_DuplicateParameterName_AcrossIndexedAndData(t *testing.T) {
	_, err := registry.LoadContractsConfig(testContractFS(t, `{
		"contracts": [{
			"chain": "eip155:1",
			"address": "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
			"standard": "erc721",
			"adapter": {
				"existence": {"method": "punkIndexToAddress", "abi": "cryptopunks", "params": ["${tokenId}"]},
				"owner": {"method": "punkIndexToAddress", "abi": "cryptopunks", "params": ["${tokenId}"]},
				"events": [{
					"signature": "BadEvent(address,uint256)",
					"mapToStandardEvent": "mint",
					"indexedParams": ["tokenId"],
					"dataParams": ["tokenId"],
					"parameterMappings": {
						"tokenId": "TokenNumber",
						"to": "ToAddress"
					}
				}]
			}
		}]
	}`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "across indexedParams and dataParams")
}

func TestLoadContractsConfig_DuplicateParameterName_WithinData(t *testing.T) {
	_, err := registry.LoadContractsConfig(testContractFS(t, `{
		"contracts": [{
			"chain": "eip155:1",
			"address": "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
			"standard": "erc721",
			"adapter": {
				"existence": {"method": "punkIndexToAddress", "abi": "cryptopunks", "params": ["${tokenId}"]},
				"owner": {"method": "punkIndexToAddress", "abi": "cryptopunks", "params": ["${tokenId}"]},
				"events": [{
					"signature": "BadEvent(address,uint256,uint256)",
					"mapToStandardEvent": "mint",
					"indexedParams": ["to"],
					"dataParams": ["tokenId", "tokenId"],
					"parameterMappings": {
						"to": "ToAddress",
						"tokenId": "TokenNumber"
					}
				}]
			}
		}]
	}`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "in dataParams")
	require.NotContains(t, err.Error(), "across")
}

func TestLoadContractsConfig_EmptyParameterName(t *testing.T) {
	_, err := registry.LoadContractsConfig(testContractFS(t, `{
		"contracts": [{
			"chain": "eip155:1",
			"address": "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
			"standard": "erc721",
			"adapter": {
				"existence": {"method": "punkIndexToAddress", "abi": "cryptopunks", "params": ["${tokenId}"]},
				"owner": {"method": "punkIndexToAddress", "abi": "cryptopunks", "params": ["${tokenId}"]},
				"events": [{
					"signature": "BadEvent(address,uint256)",
					"mapToStandardEvent": "mint",
					"indexedParams": [""],
					"dataParams": ["tokenId"],
					"parameterMappings": {
						"tokenId": "TokenNumber",
						"to": "ToAddress"
					}
				}]
			}
		}]
	}`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty parameter name")
}

func TestLoadContractsConfig_MissingRequiredFieldForTransfer(t *testing.T) {
	_, err := registry.LoadContractsConfig(testContractFS(t, `{
		"contracts": [{
			"chain": "eip155:1",
			"address": "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
			"standard": "erc721",
			"adapter": {
				"existence": {"method": "punkIndexToAddress", "abi": "cryptopunks", "params": ["${tokenId}"]},
				"owner": {"method": "punkIndexToAddress", "abi": "cryptopunks", "params": ["${tokenId}"]},
				"events": [{
					"signature": "BadTransfer(address,uint256)",
					"mapToStandardEvent": "transfer",
					"indexedParams": ["from"],
					"dataParams": ["punkIndex"],
					"parameterMappings": {
						"from": "FromAddress",
						"punkIndex": "TokenNumber"
					}
				}]
			}
		}]
	}`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "transfer events require ToAddress mapping")
}

func TestLoadContractsConfig_MissingRequiredFieldForMint(t *testing.T) {
	_, err := registry.LoadContractsConfig(testContractFS(t, `{
		"contracts": [{
			"chain": "eip155:1",
			"address": "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
			"standard": "erc721",
			"adapter": {
				"existence": {"method": "punkIndexToAddress", "abi": "cryptopunks", "params": ["${tokenId}"]},
				"owner": {"method": "punkIndexToAddress", "abi": "cryptopunks", "params": ["${tokenId}"]},
				"events": [{
					"signature": "BadMint(address)",
					"mapToStandardEvent": "mint",
					"indexedParams": ["to"],
					"dataParams": [],
					"parameterMappings": {
						"to": "ToAddress"
					}
				}]
			}
		}]
	}`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "mint events require TokenNumber mapping")
}

func TestLoadContractsConfig_MissingRequiredFieldForBurn(t *testing.T) {
	_, err := registry.LoadContractsConfig(testContractFS(t, `{
		"contracts": [{
			"chain": "eip155:1",
			"address": "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
			"standard": "erc721",
			"adapter": {
				"existence": {"method": "punkIndexToAddress", "abi": "cryptopunks", "params": ["${tokenId}"]},
				"owner": {"method": "punkIndexToAddress", "abi": "cryptopunks", "params": ["${tokenId}"]},
				"events": [{
					"signature": "BadBurn(uint256)",
					"mapToStandardEvent": "burn",
					"indexedParams": [],
					"dataParams": ["tokenId"],
					"parameterMappings": {
						"tokenId": "TokenNumber"
					}
				}]
			}
		}]
	}`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "burn events require FromAddress mapping")
}

func TestLoadContractsConfig_MissingRequiredFieldForMetadataUpdate(t *testing.T) {
	_, err := registry.LoadContractsConfig(testContractFS(t, `{
		"contracts": [{
			"chain": "eip155:1",
			"address": "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
			"standard": "erc721",
			"adapter": {
				"existence": {"method": "punkIndexToAddress", "abi": "cryptopunks", "params": ["${tokenId}"]},
				"owner": {"method": "punkIndexToAddress", "abi": "cryptopunks", "params": ["${tokenId}"]},
				"events": [{
					"signature": "BadMetadataUpdate(string)",
					"mapToStandardEvent": "metadata_update",
					"indexedParams": [],
					"dataParams": ["uri"],
					"parameterMappings": {
						"uri": "Quantity"
					}
				}]
			}
		}]
	}`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "metadata_update events require TokenNumber mapping")
}

func TestLoadContractsConfig_DuplicateEventSignature(t *testing.T) {
	_, err := registry.LoadContractsConfig(testContractFS(t, `{
		"contracts": [{
			"chain": "eip155:1",
			"address": "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
			"standard": "erc721",
			"adapter": {
				"existence": {"method": "punkIndexToAddress", "abi": "cryptopunks", "params": ["${tokenId}"]},
				"owner": {"method": "punkIndexToAddress", "abi": "cryptopunks", "params": ["${tokenId}"]},
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
						"signature": "PunkTransfer(address,address,uint256)",
						"mapToStandardEvent": "mint",
						"indexedParams": ["to"],
						"dataParams": ["from", "punkIndex"],
						"parameterMappings": {
							"from": "FromAddress",
							"to": "ToAddress",
							"punkIndex": "TokenNumber"
						}
					}
				]
			}
		}]
	}`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate event signature")
}

func TestLoadContractsConfig_OnChainMetadataMissingMethod(t *testing.T) {
	_, err := registry.LoadContractsConfig(testContractFS(t, `{
		"contracts": [{
			"chain": "eip155:1",
			"address": "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
			"standard": "erc721",
			"adapter": {
				"existence": {"method": "punkIndexToAddress", "abi": "cryptopunks", "params": ["${tokenId}"]},
				"owner": {"method": "punkIndexToAddress", "abi": "cryptopunks", "params": ["${tokenId}"]},
				"metadata": {"source": "on_chain"}
			}
		}]
	}`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "adapter.metadata.method is required when source is on_chain")
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

	_, err := registry.LoadContractsConfig(testContractFS(t, duplicateConfig))
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate contract entry")
}

func TestLoadContractsConfig_TransferAddressNotIndexed_FromAddress(t *testing.T) {
	_, err := registry.LoadContractsConfig(testContractFS(t, `{
		"contracts": [{
			"chain": "eip155:1",
			"address": "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
			"standard": "erc721",
			"adapter": {
				"existence": {"method": "punkIndexToAddress", "abi": "cryptopunks", "params": ["${tokenId}"]},
				"owner": {"method": "punkIndexToAddress", "abi": "cryptopunks", "params": ["${tokenId}"]},
				"events": [{
					"signature": "Transfer(address,address,uint256)",
					"mapToStandardEvent": "transfer",
					"indexedParams": ["to"],
					"dataParams": ["from", "tokenId"],
					"parameterMappings": {
						"from": "FromAddress",
						"to": "ToAddress",
						"tokenId": "TokenNumber"
					}
				}]
			}
		}]
	}`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "transfer events require FromAddress to be an indexed parameter for ownership tracking")
}

func TestLoadContractsConfig_TransferAddressNotIndexed_ToAddress(t *testing.T) {
	_, err := registry.LoadContractsConfig(testContractFS(t, `{
		"contracts": [{
			"chain": "eip155:1",
			"address": "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
			"standard": "erc721",
			"adapter": {
				"existence": {"method": "punkIndexToAddress", "abi": "cryptopunks", "params": ["${tokenId}"]},
				"owner": {"method": "punkIndexToAddress", "abi": "cryptopunks", "params": ["${tokenId}"]},
				"events": [{
					"signature": "Transfer(address,address,uint256)",
					"mapToStandardEvent": "transfer",
					"indexedParams": ["from"],
					"dataParams": ["to", "tokenId"],
					"parameterMappings": {
						"from": "FromAddress",
						"to": "ToAddress",
						"tokenId": "TokenNumber"
					}
				}]
			}
		}]
	}`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "transfer events require ToAddress to be an indexed parameter for ownership tracking")
}

func TestLoadContractsConfig_MintAddressNotIndexed(t *testing.T) {
	_, err := registry.LoadContractsConfig(testContractFS(t, `{
		"contracts": [{
			"chain": "eip155:1",
			"address": "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
			"standard": "erc721",
			"adapter": {
				"existence": {"method": "punkIndexToAddress", "abi": "cryptopunks", "params": ["${tokenId}"]},
				"owner": {"method": "punkIndexToAddress", "abi": "cryptopunks", "params": ["${tokenId}"]},
				"events": [{
					"signature": "Mint(address,uint256)",
					"mapToStandardEvent": "mint",
					"indexedParams": [],
					"dataParams": ["to", "tokenId"],
					"parameterMappings": {
						"to": "ToAddress",
						"tokenId": "TokenNumber"
					}
				}]
			}
		}]
	}`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "mint events require ToAddress to be an indexed parameter for ownership tracking")
}

func TestLoadContractsConfig_BurnAddressNotIndexed(t *testing.T) {
	_, err := registry.LoadContractsConfig(testContractFS(t, `{
		"contracts": [{
			"chain": "eip155:1",
			"address": "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
			"standard": "erc721",
			"adapter": {
				"existence": {"method": "punkIndexToAddress", "abi": "cryptopunks", "params": ["${tokenId}"]},
				"owner": {"method": "punkIndexToAddress", "abi": "cryptopunks", "params": ["${tokenId}"]},
				"events": [{
					"signature": "Burn(address,uint256)",
					"mapToStandardEvent": "burn",
					"indexedParams": [],
					"dataParams": ["from", "tokenId"],
					"parameterMappings": {
						"from": "FromAddress",
						"tokenId": "TokenNumber"
					}
				}]
			}
		}]
	}`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "burn events require FromAddress to be an indexed parameter for ownership tracking")
}

func TestLoadContractsConfig_ERC1155TransferMissingQuantity(t *testing.T) {
	_, err := registry.LoadContractsConfig(testContractFS(t, `{
		"contracts": [{
			"chain": "eip155:1",
			"address": "0xd07dc4262bcdbf85190c01c996b4c06a461d2430",
			"standard": "erc1155",
			"adapter": {
				"existence": {"method": "punkIndexToAddress", "abi": "cryptopunks", "params": ["${tokenId}"]},
				"owner": {"method": "punkIndexToAddress", "abi": "cryptopunks", "params": ["${tokenId}"]},
				"events": [{
					"signature": "Transfer(address,address,uint256)",
					"mapToStandardEvent": "transfer",
					"indexedParams": ["from", "to"],
					"dataParams": ["tokenId"],
					"parameterMappings": {
						"from": "FromAddress",
						"to": "ToAddress",
						"tokenId": "TokenNumber"
					}
				}]
			}
		}]
	}`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "transfer events for ERC1155-style contracts require Quantity mapping")
}

func TestLoadContractsConfig_ERC1155MintMissingQuantity(t *testing.T) {
	_, err := registry.LoadContractsConfig(testContractFS(t, `{
		"contracts": [{
			"chain": "eip155:1",
			"address": "0xd07dc4262bcdbf85190c01c996b4c06a461d2430",
			"standard": "erc1155",
			"adapter": {
				"existence": {"method": "punkIndexToAddress", "abi": "cryptopunks", "params": ["${tokenId}"]},
				"owner": {"method": "punkIndexToAddress", "abi": "cryptopunks", "params": ["${tokenId}"]},
				"events": [{
					"signature": "Mint(address,uint256)",
					"mapToStandardEvent": "mint",
					"indexedParams": ["to"],
					"dataParams": ["tokenId"],
					"parameterMappings": {
						"to": "ToAddress",
						"tokenId": "TokenNumber"
					}
				}]
			}
		}]
	}`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "mint events for ERC1155-style contracts require Quantity mapping")
}

func TestLoadContractsConfig_ERC1155BurnMissingQuantity(t *testing.T) {
	_, err := registry.LoadContractsConfig(testContractFS(t, `{
		"contracts": [{
			"chain": "eip155:1",
			"address": "0xd07dc4262bcdbf85190c01c996b4c06a461d2430",
			"standard": "erc1155",
			"adapter": {
				"existence": {"method": "punkIndexToAddress", "abi": "cryptopunks", "params": ["${tokenId}"]},
				"owner": {"method": "punkIndexToAddress", "abi": "cryptopunks", "params": ["${tokenId}"]},
				"events": [{
					"signature": "Burn(address,uint256)",
					"mapToStandardEvent": "burn",
					"indexedParams": ["from"],
					"dataParams": ["tokenId"],
					"parameterMappings": {
						"from": "FromAddress",
						"tokenId": "TokenNumber"
					}
				}]
			}
		}]
	}`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "burn events for ERC1155-style contracts require Quantity mapping")
}

func TestLoadContractsConfig_ERC721DoesNotRequireQuantity(t *testing.T) {
	// This should pass - ERC721 doesn't require Quantity
	cfg, err := registry.LoadContractsConfig(testContractFS(t, `{
		"contracts": [{
			"chain": "eip155:1",
			"address": "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
			"standard": "erc721",
			"adapter": {
				"existence": {"method": "punkIndexToAddress", "abi": "cryptopunks", "params": ["${tokenId}"]},
				"owner": {"method": "punkIndexToAddress", "abi": "cryptopunks", "params": ["${tokenId}"]},
				"events": [{
					"signature": "Transfer(address,address,uint256)",
					"mapToStandardEvent": "transfer",
					"indexedParams": ["from", "to"],
					"dataParams": ["tokenId"],
					"parameterMappings": {
						"from": "FromAddress",
						"to": "ToAddress",
						"tokenId": "TokenNumber"
					}
				}]
			}
		}]
	}`))
	require.NoError(t, err)
	require.Len(t, cfg.Contracts, 1)
}

func TestNewABIRegistry_LoadsABIs(t *testing.T) {
	registry, err := helpers.NewABIRegistry(testContractFS(t, `{"contracts": []}`))
	require.NoError(t, err)

	cryptopunks, err := registry.Get("cryptopunks")
	require.NoError(t, err)
	require.NotEmpty(t, cryptopunks.Methods)

	_, err = registry.Get("missing")
	require.Error(t, err)
	require.Contains(t, err.Error(), "ABI not found")
}

func TestEmbeddedContractsConfig_LoadsCryptoPunks(t *testing.T) {
	cfg, err := registry.LoadContractsConfig(contracts.Files)
	require.NoError(t, err)
	require.Len(t, cfg.Contracts, 1)
	require.Equal(t, "CryptoPunks", cfg.Contracts[0].Name)
	require.Equal(t, domain.ChainEthereumMainnet, cfg.Contracts[0].Chain)
	require.Equal(t, cryptoPunksAddress, cfg.Contracts[0].Address)
	require.Len(t, cfg.Contracts[0].Adapter.Events, 2)
}

func TestNewAdapterRegistry_MissingABI(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEthClient(ctrl)
	_, err := registry.NewAdapterRegistry(
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
		mockClient,
		ethadapter.NewClock(),
		nil,
		helpers.NewPaginationHelper(mockClient, ethadapter.NewClock(), nil),
		domain.ChainEthereumMainnet,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ABI not found")
}

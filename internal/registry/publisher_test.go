package registry_test

import (
	"encoding/json"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/registry"
)

func TestPublisherRegistryLoader_Load(t *testing.T) {
	tests := []struct {
		name         string
		setupMocks   func(*mocks.MockFileSystem, *mocks.MockJSON)
		expectedErr  string // Error message to assert, empty means no error expected
		validateFunc func(t *testing.T, registry registry.PublisherRegistry)
	}{
		{
			name: "successful load with valid JSON",
			setupMocks: func(mockFS *mocks.MockFileSystem, mockJSON *mocks.MockJSON) {
				mockFS.
					EXPECT().
					ReadFile("test.json").
					Return([]byte(`{
					"version": 1,
					"min_blocks": {
						"eip155:1": 13492497
					},
					"publishers": [
						{
							"name": "Art Blocks",
							"url": "https://artblocks.io",
							"chains": {
								"eip155:1": {
									"collection_addresses": ["0x123"],
									"deployer_addresses": ["0xabc"]
								}
							}
						}
					]
				}`), nil)
				mockJSON.
					EXPECT().
					Unmarshal(gomock.Any(), gomock.Any()).
					DoAndReturn(func(data []byte, v interface{}) error {
						return json.Unmarshal(data, v)
					})
			},
			expectedErr: "",
			validateFunc: func(t *testing.T, reg registry.PublisherRegistry) {
				assert.NotNil(t, reg)
				publisher := reg.LookupPublisherByCollection(domain.ChainEthereumMainnet, "0x123")
				assert.NotNil(t, publisher)
				assert.Equal(t, registry.PublisherNameArtBlocks, publisher.Name)
				assert.Equal(t, "https://artblocks.io", publisher.URL)

				deployerPublisher := reg.LookupPublisherByDeployer(domain.ChainEthereumMainnet, "0xabc")
				assert.NotNil(t, deployerPublisher)
				assert.Equal(t, registry.PublisherNameArtBlocks, deployerPublisher.Name)

				minBlock, ok := reg.GetMinBlock(domain.ChainEthereumMainnet)
				assert.True(t, ok)
				assert.Equal(t, uint64(13492497), minBlock)
			},
		},
		{
			name: "successful load with multiple publishers",
			setupMocks: func(mockFS *mocks.MockFileSystem, mockJSON *mocks.MockJSON) {
				mockFS.
					EXPECT().
					ReadFile("test.json").
					Return([]byte(`{
					"version": 1,
					"publishers": [
						{
							"name": "Art Blocks",
							"url": "https://artblocks.io",
							"chains": {
								"eip155:1": {
									"collection_addresses": ["0x123"]
								}
							}
						},
						{
							"name": "fxhash",
							"url": "https://fxhash.xyz",
							"chains": {
								"tezos:mainnet": {
									"collection_addresses": ["KT1ABC"]
								}
							}
						}
					]
				}`), nil)
				mockJSON.
					EXPECT().
					Unmarshal(gomock.Any(), gomock.Any()).
					DoAndReturn(func(data []byte, v interface{}) error {
						return json.Unmarshal(data, v)
					})
			},
			expectedErr: "",
			validateFunc: func(t *testing.T, reg registry.PublisherRegistry) {
				assert.NotNil(t, reg)
				publisher1 := reg.LookupPublisherByCollection(domain.ChainEthereumMainnet, "0x123")
				assert.NotNil(t, publisher1)
				assert.Equal(t, registry.PublisherNameArtBlocks, publisher1.Name)

				publisher2 := reg.LookupPublisherByCollection(domain.ChainTezosMainnet, "KT1ABC")
				assert.NotNil(t, publisher2)
				assert.Equal(t, registry.PublisherNameFXHash, publisher2.Name)
			},
		},
		{
			name: "file read error",
			setupMocks: func(mockFS *mocks.MockFileSystem, mockJSON *mocks.MockJSON) {
				mockFS.
					EXPECT().
					ReadFile("test.json").
					Return(nil, assert.AnError)
			},
			expectedErr: "failed to read registry file",
		},
		{
			name: "JSON parse error",
			setupMocks: func(mockFS *mocks.MockFileSystem, mockJSON *mocks.MockJSON) {
				publisherJSON := []byte(`invalid json`)
				mockFS.
					EXPECT().
					ReadFile("test.json").
					Return(publisherJSON, nil)
				mockJSON.
					EXPECT().
					Unmarshal(publisherJSON, gomock.Any()).
					Return(assert.AnError)
			},
			expectedErr: "failed to parse registry JSON",
		},
		{
			name: "case insensitive lookup",
			setupMocks: func(mockFS *mocks.MockFileSystem, mockJSON *mocks.MockJSON) {
				mockFS.
					EXPECT().
					ReadFile("test.json").
					Return([]byte(`{
					"version": 1,
					"publishers": [
						{
							"name": "Art Blocks",
							"url": "https://artblocks.io",
							"chains": {
								"EIP155:1": {
									"collection_addresses": ["0x123ABC"]
								}
							}
						}
					]
				}`), nil)
				mockJSON.
					EXPECT().
					Unmarshal(gomock.Any(), gomock.Any()).
					DoAndReturn(func(data []byte, v interface{}) error {
						return json.Unmarshal(data, v)
					})
			},
			expectedErr: "",
			validateFunc: func(t *testing.T, reg registry.PublisherRegistry) {
				// Test case insensitive chain ID
				publisher := reg.LookupPublisherByCollection(domain.Chain(strings.ToUpper(string(domain.ChainEthereumMainnet))), "0x123ABC")
				assert.NotNil(t, publisher)

				publisher = reg.LookupPublisherByCollection(domain.Chain(strings.ToLower(string(domain.ChainEthereumMainnet))), "0x123abc")
				assert.NotNil(t, publisher)

				// Test case insensitive address
				publisher2 := reg.LookupPublisherByCollection(domain.ChainEthereumMainnet, "0X123ABC")
				assert.NotNil(t, publisher2)

				publisher2 = reg.LookupPublisherByCollection(domain.ChainEthereumMainnet, "0x123abc")
				assert.NotNil(t, publisher2)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockFS := mocks.NewMockFileSystem(ctrl)
			mockJSON := mocks.NewMockJSON(ctrl)

			if tt.setupMocks != nil {
				tt.setupMocks(mockFS, mockJSON)
			}

			loader := registry.NewPublisherRegistryLoader(mockFS, mockJSON)
			reg, err := loader.Load("test.json")

			if tt.expectedErr != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
				assert.Nil(t, reg)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, reg)
				if tt.validateFunc != nil {
					tt.validateFunc(t, reg)
				}
			}
		})
	}
}

func TestPublisherRegistry_LookupPublisherByCollection(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockFS := mocks.NewMockFileSystem(ctrl)
	mockJSON := mocks.NewMockJSON(ctrl)

	// Create registry through loader
	mockFS.
		EXPECT().
		ReadFile("test.json").
		Return([]byte(`{
		"version": 1,
		"publishers": [
			{
				"name": "Art Blocks",
				"url": "https://artblocks.io",
				"chains": {
					"eip155:1": {
						"collection_addresses": ["0x123"]
					}
				}
			}
		]
	}`), nil)
	mockJSON.
		EXPECT().
		Unmarshal(gomock.Any(), gomock.Any()).
		DoAndReturn(func(data []byte, v interface{}) error {
			return json.Unmarshal(data, v)
		})

	loader := registry.NewPublisherRegistryLoader(mockFS, mockJSON)
	reg, err := loader.Load("test.json")
	assert.NoError(t, err)
	assert.NotNil(t, reg)

	tests := []struct {
		name         string
		chainID      domain.Chain
		address      string
		expectedName registry.PublisherName
		expectedURL  string
	}{
		{
			name:         "found publisher",
			chainID:      domain.ChainEthereumMainnet,
			address:      "0x123",
			expectedName: registry.PublisherNameArtBlocks,
			expectedURL:  "https://artblocks.io",
		},
		{
			name:         "not found",
			chainID:      domain.ChainEthereumMainnet,
			address:      "0x999",
			expectedName: "",
			expectedURL:  "",
		},
		{
			name:         "case insensitive lookup",
			chainID:      domain.ChainEthereumMainnet,
			address:      "0X123",
			expectedName: registry.PublisherNameArtBlocks,
			expectedURL:  "https://artblocks.io",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := reg.LookupPublisherByCollection(tt.chainID, tt.address)
			if tt.expectedName == "" {
				assert.Nil(t, result)
			} else {
				assert.NotNil(t, result)
				assert.Equal(t, tt.expectedName, result.Name)
				assert.Equal(t, tt.expectedURL, result.URL)
			}
		})
	}
}

func TestPublisherRegistry_LookupPublisherByDeployer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockFS := mocks.NewMockFileSystem(ctrl)
	mockJSON := mocks.NewMockJSON(ctrl)

	// Create registry through loader
	mockFS.
		EXPECT().
		ReadFile("test.json").
		Return([]byte(`{
		"version": 1,
		"publishers": [
			{
				"name": "Art Blocks",
				"url": "https://artblocks.io",
				"chains": {
					"eip155:1": {
						"deployer_addresses": ["0xabc"]
					}
				}
			}
		]
	}`), nil)
	mockJSON.
		EXPECT().
		Unmarshal(gomock.Any(), gomock.Any()).
		DoAndReturn(func(data []byte, v interface{}) error {
			return json.Unmarshal(data, v)
		})

	loader := registry.NewPublisherRegistryLoader(mockFS, mockJSON)
	reg, err := loader.Load("test.json")
	assert.NoError(t, err)
	assert.NotNil(t, reg)

	tests := []struct {
		name         string
		chainID      domain.Chain
		address      string
		expectedName registry.PublisherName
		expectedURL  string
	}{
		{
			name:         "found publisher",
			chainID:      domain.ChainEthereumMainnet,
			address:      "0xabc",
			expectedName: registry.PublisherNameArtBlocks,
			expectedURL:  "https://artblocks.io",
		},
		{
			name:         "not found",
			chainID:      domain.ChainEthereumMainnet,
			address:      "0x999",
			expectedName: "",
			expectedURL:  "",
		},
		{
			name:         "case insensitive lookup",
			chainID:      domain.ChainEthereumMainnet,
			address:      "0XABC",
			expectedName: registry.PublisherNameArtBlocks,
			expectedURL:  "https://artblocks.io",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := reg.LookupPublisherByDeployer(tt.chainID, tt.address)
			if tt.expectedName == "" {
				assert.Nil(t, result)
			} else {
				assert.NotNil(t, result)
				assert.Equal(t, tt.expectedName, result.Name)
				assert.Equal(t, tt.expectedURL, result.URL)
			}
		})
	}
}

func TestPublisherRegistry_GetMinBlock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockFS := mocks.NewMockFileSystem(ctrl)
	mockJSON := mocks.NewMockJSON(ctrl)

	// Create registry through loader
	mockFS.
		EXPECT().
		ReadFile("test.json").
		Return([]byte(`{
		"version": 1,
		"min_blocks": {
			"eip155:1": 13492497,
			"tezos:mainnet": 1000000
		},
		"publishers": []
	}`), nil)
	mockJSON.
		EXPECT().
		Unmarshal(gomock.Any(), gomock.Any()).
		DoAndReturn(func(data []byte, v interface{}) error {
			return json.Unmarshal(data, v)
		})

	loader := registry.NewPublisherRegistryLoader(mockFS, mockJSON)
	reg, err := loader.Load("test.json")
	assert.NoError(t, err)
	assert.NotNil(t, reg)

	tests := []struct {
		name       string
		chainID    domain.Chain
		expected   uint64
		expectedOk bool
	}{
		{
			name:       "found min block",
			chainID:    domain.ChainEthereumMainnet,
			expected:   13492497,
			expectedOk: true,
		},
		{
			name:       "found min block tezos",
			chainID:    domain.ChainTezosMainnet,
			expected:   1000000,
			expectedOk: true,
		},
		{
			name:       "not found",
			chainID:    domain.ChainEthereumSepolia,
			expected:   0,
			expectedOk: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, ok := reg.GetMinBlock(tt.chainID)
			assert.Equal(t, tt.expectedOk, ok)
			if tt.expectedOk {
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestPublisherRegistry_IsSharedContract(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockFS := mocks.NewMockFileSystem(ctrl)
	mockJSON := mocks.NewMockJSON(ctrl)
	mockFS.
		EXPECT().
		ReadFile("test.json").
		Return([]byte(`{
		"version": 1,
		"shared_contracts": {
			"eip155:1": [{"name": "Shared", "address": "0xAbC"}],
			"tezos:mainnet": [{"name": "Shared FA2", "address": "KT1Shared"}]
		},
		"publishers": [
			{
				"name": "SuperRare",
				"url": "https://superrare.com",
				"chains": {"eip155:1": {"collection_addresses": ["0x123"]}}
			}
		]
	}`), nil)
	mockJSON.
		EXPECT().
		Unmarshal(gomock.Any(), gomock.Any()).
		DoAndReturn(func(data []byte, v interface{}) error {
			return json.Unmarshal(data, v)
		})

	reg, err := registry.NewPublisherRegistryLoader(mockFS, mockJSON).Load("test.json")
	assert.NoError(t, err)

	tests := []struct {
		name     string
		chainID  domain.Chain
		address  string
		expected bool
	}{
		{"listed", domain.ChainEthereumMainnet, "0xAbC", true},
		{"case insensitive", domain.ChainEthereumMainnet, "0XABC", true},
		{"listed on tezos", domain.ChainTezosMainnet, "KT1Shared", true},
		{"listed on another chain only", domain.ChainEthereumSepolia, "0xAbC", false},
		{"publisher collection is not shared by itself", domain.ChainEthereumMainnet, "0x123", false},
		{"unlisted", domain.ChainEthereumMainnet, "0x999", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, reg.IsSharedContract(tt.chainID, tt.address))
		})
	}
	// A shared contract is not a publisher collection: listing it attributes no publisher.
	assert.Nil(t, reg.LookupPublisherByCollection(domain.ChainEthereumMainnet, "0xAbC"))
}

// A registry without the optional shared_contracts section lists no shared contract.
func TestPublisherRegistry_IsSharedContract_SectionMissing(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockFS := mocks.NewMockFileSystem(ctrl)
	mockJSON := mocks.NewMockJSON(ctrl)
	mockFS.EXPECT().ReadFile("test.json").Return([]byte(`{"version": 1, "publishers": []}`), nil)
	mockJSON.
		EXPECT().
		Unmarshal(gomock.Any(), gomock.Any()).
		DoAndReturn(func(data []byte, v interface{}) error {
			return json.Unmarshal(data, v)
		})

	reg, err := registry.NewPublisherRegistryLoader(mockFS, mockJSON).Load("test.json")
	assert.NoError(t, err)
	assert.False(t, reg.IsSharedContract(domain.ChainEthereumMainnet, "0xb932a70a57673d89f4acffbe830e8ed7f75fb9e0"))
}

// The shipped registry lists the shared minting contracts named in ff-indexer-v2 #176,
// each address once and well-formed, and none of the series-level publishers' contracts.
func TestPublisherRegistryFile_SharedContracts(t *testing.T) {
	const path = "../../tools/registry/publisher.json"
	reg, err := registry.NewPublisherRegistryLoader(adapter.NewFileSystem(), adapter.NewJSON()).Load(path)
	require.NoError(t, err)

	for _, contract := range []struct {
		chain   domain.Chain
		address string
	}{
		{domain.ChainEthereumMainnet, "0xb932a70a57673d89f4acffbe830e8ed7f75fb9e0"}, // SuperRare
		{domain.ChainEthereumMainnet, "0x41A322b28D0fF354040e2CbC676F0320d8c8850d"}, // SupeRare v1
		{domain.ChainEthereumMainnet, "0x3B3ee1931Dc30C1957379FAc9aba94D1C48a5405"}, // Foundation
		{domain.ChainEthereumMainnet, "0x495f947276749Ce646f68AC8c248420045cb7b5e"}, // OpenSea Shared Storefront
		{domain.ChainEthereumMainnet, "0x60F80121C31A0d46B5279700f9DF786054aa5eE5"}, // Rarible ERC-721
		{domain.ChainEthereumMainnet, "0xd07dc4262BCDbf85190C01c996b4C06a461d2430"}, // Rarible ERC-1155
		{domain.ChainEthereumMainnet, "0xabEFBc9fD2F806065b4f3C237d4b59D9A97Bcac7"}, // Zora
		{domain.ChainTezosMainnet, "KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton"},          // hic et nunc OBJKT
	} {
		assert.True(t, reg.IsSharedContract(contract.chain, contract.address), contract.address)
	}

	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	var data registry.PublisherRegistryData
	require.NoError(t, json.Unmarshal(raw, &data))
	evmAddress := regexp.MustCompile(`^0x[0-9a-fA-F]{40}$`)
	tezosContract := regexp.MustCompile(`^KT1[1-9A-HJ-NP-Za-km-z]{33}$`)
	seen := map[string]bool{}
	for chainID, contracts := range data.SharedContracts {
		for _, contract := range contracts {
			key := strings.ToLower(chainID + ":" + contract.Address)
			assert.False(t, seen[key], "duplicate shared contract %s", key)
			seen[key] = true
			assert.NotEmpty(t, contract.Name, key)
			if strings.HasPrefix(chainID, "eip155:") {
				assert.Regexp(t, evmAddress, contract.Address)
			} else {
				assert.Regexp(t, tezosContract, contract.Address)
			}
		}
	}

	// Series-level publishers name a series inside their contracts; they are not listed.
	for _, publisher := range data.Publishers {
		switch publisher.Name {
		case registry.PublisherNameArtBlocks, registry.PublisherNameFeralFile, registry.PublisherNameFXHash:
			for chainID, chain := range publisher.Chains {
				for _, address := range chain.CollectionAddresses {
					assert.False(t, reg.IsSharedContract(domain.Chain(chainID), address), "%s %s", publisher.Name, address)
				}
			}
		}
	}
}

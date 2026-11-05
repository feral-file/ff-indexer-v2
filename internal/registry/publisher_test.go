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

func TestPublisherRegistry_IsPlatformRelease(t *testing.T) {
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
			"eip155:1": [{"name": "Shared", "address": "0xAbC", "platform_releases": {"opensea": ["platform", "Misfiled"]}}],
			"tezos:mainnet": [{"name": "Shared FA2", "address": "KT1Shared", "platform_releases": {"objkt": ["KT1Shared"]}}]
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
	require.NoError(t, err)

	tests := []struct {
		name     string
		chainID  domain.Chain
		address  string
		vendor   string
		release  string
		expected bool
	}{
		{"platform release", domain.ChainEthereumMainnet, "0xAbC", "opensea", "platform", true},
		{"second platform release", domain.ChainEthereumMainnet, "0xAbC", "opensea", "Misfiled", true},
		{"case insensitive", domain.ChainEthereumMainnet, "0XABC", "OpenSea", "MISFILED", true},
		{"creator collection on the shared contract", domain.ChainEthereumMainnet, "0xAbC", "opensea", "artist-series", false},
		{"platform release id from another vendor", domain.ChainEthereumMainnet, "0xAbC", "objkt", "platform", false},
		{"platform release id on another contract", domain.ChainEthereumMainnet, "0x999", "opensea", "platform", false},
		{"listed on another chain only", domain.ChainEthereumSepolia, "0xAbC", "opensea", "platform", false},
		{"objkt custom collection on tezos", domain.ChainTezosMainnet, "KT1Shared", "objkt", "KT1Shared", true},
		{"publisher collection is not shared by itself", domain.ChainEthereumMainnet, "0x123", "opensea", "platform", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, reg.IsPlatformRelease(tt.chainID, tt.address, tt.vendor, tt.release))
		})
	}
	// A shared contract is not a publisher collection: listing it attributes no publisher.
	assert.Nil(t, reg.LookupPublisherByCollection(domain.ChainEthereumMainnet, "0xAbC"))
}

// A registry without the optional shared_contracts section lists no platform release.
func TestPublisherRegistry_IsPlatformRelease_SectionMissing(t *testing.T) {
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
	require.NoError(t, err)
	assert.False(t, reg.IsPlatformRelease(domain.ChainEthereumMainnet, "0xb932a70a57673d89f4acffbe830e8ed7f75fb9e0", "opensea", "superrare"))
}

// A shared contract that names no platform release would drop nothing, so Load rejects it
// rather than letting a registry edit silently disable the rule.
func TestPublisherRegistry_Load_RejectsSharedContractWithoutPlatformRelease(t *testing.T) {
	tests := []struct {
		name    string
		shared  string
		wantErr string
	}{
		{"no platform_releases", `{"name": "Shared", "address": "0xAbC"}`, "lists no platform release"},
		{"empty vendor list", `{"name": "Shared", "address": "0xAbC", "platform_releases": {"opensea": []}}`, "lists no platform release"},
		{"empty release id", `{"name": "Shared", "address": "0xAbC", "platform_releases": {"opensea": [""]}}`, "empty platform release"},
		{"no address", `{"name": "Shared", "platform_releases": {"opensea": ["platform"]}}`, "has no address"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockFS := mocks.NewMockFileSystem(ctrl)
			mockJSON := mocks.NewMockJSON(ctrl)
			mockFS.EXPECT().ReadFile("test.json").Return([]byte(`{"version": 1, "shared_contracts": {"eip155:1": [`+tt.shared+`]}, "publishers": []}`), nil)
			mockJSON.
				EXPECT().
				Unmarshal(gomock.Any(), gomock.Any()).
				DoAndReturn(func(data []byte, v interface{}) error {
					return json.Unmarshal(data, v)
				})

			reg, err := registry.NewPublisherRegistryLoader(mockFS, mockJSON).Load("test.json")
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
			assert.Nil(t, reg)
		})
	}
}

// The shipped registry lists the shared minting contracts named in ff-indexer-v2 #176 with
// the platform-wide collection OpenSea or objkt files their tokens under, each address
// once and well-formed, and none of the series-level publishers' contracts.
func TestPublisherRegistryFile_SharedContracts(t *testing.T) {
	const path = "../../tools/registry/publisher.json"
	reg, err := registry.NewPublisherRegistryLoader(adapter.NewFileSystem(), adapter.NewJSON()).Load(path)
	require.NoError(t, err)

	// Platform-wide collections, measured 2026-11-05 on 1,200 tokens per contract: 88-100%
	// of each contract's tokens are filed under them. SuperRare's second slug is an OpenSea
	// misfiling (a 34k-item bucket named after an unrelated address) holding ~12% of its
	// tokens. OpenSea's "rarible" collection spans all seven Rarible contracts.
	const superRareMisfiled = "0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85"
	for _, release := range []struct {
		chain   domain.Chain
		address string
		vendor  string
		id      string
	}{
		{domain.ChainEthereumMainnet, "0xb932a70a57673d89f4acffbe830e8ed7f75fb9e0", "opensea", "superrare"}, // SuperRare
		{domain.ChainEthereumMainnet, "0xb932a70a57673d89f4acffbe830e8ed7f75fb9e0", "opensea", superRareMisfiled},
		{domain.ChainEthereumMainnet, "0x41A322b28D0fF354040e2CbC676F0320d8c8850d", "opensea", "superrare"}, // SupeRare v1
		{domain.ChainEthereumMainnet, "0x41A322b28D0fF354040e2CbC676F0320d8c8850d", "opensea", superRareMisfiled},
		{domain.ChainEthereumMainnet, "0x3B3ee1931Dc30C1957379FAc9aba94D1C48a5405", "opensea", "fnd"},                       // Foundation
		{domain.ChainEthereumMainnet, "0x60F80121C31A0d46B5279700f9DF786054aa5eE5", "opensea", "rarible"},                   // Rarible ERC-721
		{domain.ChainEthereumMainnet, "0x6A5Ff3CEEcaE9CeB96E6ac6c76B82aF8B39f0Eb3", "opensea", "rarible"},                   // Rarible ERC-721, 2020
		{domain.ChainEthereumMainnet, "0xF6793dA657495ffeFF9Ee6350824910Abc21356C", "opensea", "rarible"},                   // Rarible ERC-721, 2020
		{domain.ChainEthereumMainnet, "0xc9154424B823b10579895cCBE442d41b9Abd96Ed", "opensea", "rarible"},                   // Rarible ERC-721
		{domain.ChainEthereumMainnet, "0xe03e4721c75188de6DD9ccf4Fc0eb7C81399E305", "opensea", "rarible"},                   // Rarible ERC-721
		{domain.ChainEthereumMainnet, "0xd07dc4262BCDbf85190C01c996b4C06a461d2430", "opensea", "rarible"},                   // Rarible ERC-1155
		{domain.ChainEthereumMainnet, "0xB66a603f4cFe17e3D27B87a8BfCaD319856518B8", "opensea", "rarible"},                   // Rarible ERC-1155
		{domain.ChainEthereumMainnet, "0xabEFBc9fD2F806065b4f3C237d4b59D9A97Bcac7", "opensea", "zora"},                      // Zora
		{domain.ChainTezosMainnet, "KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton", "objkt", "KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton"}, // hic et nunc OBJKT
	} {
		assert.True(t, reg.IsPlatformRelease(release.chain, release.address, release.vendor, release.id), "%s %s %s", release.address, release.vendor, release.id)
	}

	// Creator-level collections on the same contracts are releases (held in production
	// when the rule was written).
	assert.False(t, reg.IsPlatformRelease(domain.ChainEthereumMainnet, "0xd07dc4262BCDbf85190C01c996b4C06a461d2430", "opensea", "xcopy-rarible"))
	assert.False(t, reg.IsPlatformRelease(domain.ChainEthereumMainnet, "0xb932a70a57673d89f4acffbe830e8ed7f75fb9e0", "opensea", "gazellio-spatial-exhibition"))

	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	var data registry.PublisherRegistryData
	require.NoError(t, json.Unmarshal(raw, &data))
	evmAddress := regexp.MustCompile(`^0x[0-9a-fA-F]{40}$`)
	tezosContract := regexp.MustCompile(`^KT1[1-9A-HJ-NP-Za-km-z]{33}$`)
	listed := map[string]bool{}
	for chainID, contracts := range data.SharedContracts {
		for _, contract := range contracts {
			key := strings.ToLower(chainID + ":" + contract.Address)
			assert.False(t, listed[key], "duplicate shared contract %s", key)
			listed[key] = true
			assert.NotEmpty(t, contract.Name, key)
			if strings.HasPrefix(chainID, "eip155:") {
				assert.Regexp(t, evmAddress, contract.Address)
			} else {
				assert.Regexp(t, tezosContract, contract.Address)
			}
			for vendor, releases := range contract.PlatformReleases {
				assert.Contains(t, []string{"opensea", "objkt"}, vendor, "%s: platform releases come from collection-level vendors", key)
				assert.NotEmpty(t, releases, "%s %s", key, vendor)
			}
		}
	}

	// The OpenSea Shared Storefront has no platform-wide collection (every token is in a
	// creator-made one), so listing it would only ever drop real series.
	assert.False(t, listed["eip155:1:0x495f947276749ce646f68ac8c248420045cb7b5e"], "OpenSea Shared Storefront must not be listed")

	// Series-level publishers name a series inside their contracts; they are not listed.
	for _, publisher := range data.Publishers {
		switch publisher.Name {
		case registry.PublisherNameArtBlocks, registry.PublisherNameFeralFile, registry.PublisherNameFXHash:
			for chainID, chain := range publisher.Chains {
				for _, address := range chain.CollectionAddresses {
					assert.False(t, listed[strings.ToLower(chainID+":"+address)], "%s %s", publisher.Name, address)
				}
			}
		}
	}
}

package registry_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

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

func TestPublisherNameToVendor(t *testing.T) {
	tests := []struct {
		name          string
		publisherName registry.PublisherName
		expected      string
	}{
		{
			name:          "Art Blocks",
			publisherName: registry.PublisherNameArtBlocks,
			expected:      "artblocks",
		},
		{
			name:          "FXHash",
			publisherName: registry.PublisherNameFXHash,
			expected:      "fxhash",
		},
		{
			name:          "Feral File",
			publisherName: registry.PublisherNameFeralFile,
			expected:      "feralfile",
		},
		{
			name:          "Foundation",
			publisherName: registry.PublisherNameFoundation,
			expected:      "foundation",
		},
		{
			name:          "SuperRare",
			publisherName: registry.PublisherNameSuperRare,
			expected:      "superrare",
		},
		{
			name:          "unknown",
			publisherName: registry.PublisherName("unknown"),
			expected:      "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := registry.PublisherNameToVendor(tt.publisherName)
			assert.Equal(t, tt.expected, string(result))
		})
	}
}

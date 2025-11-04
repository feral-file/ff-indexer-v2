package registry_test

import (
	"encoding/json"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/registry"
)

func TestBlacklistRegistryLoader_Load(t *testing.T) {
	tests := []struct {
		name         string
		setupMocks   func(*mocks.MockFileSystem, *mocks.MockJSON)
		expectedErr  string // Error message to assert, empty means no error expected
		validateFunc func(t *testing.T, reg registry.BlacklistRegistry)
	}{
		{
			name: "successful load with valid JSON",
			setupMocks: func(mockFS *mocks.MockFileSystem, mockJSON *mocks.MockJSON) {
				mockFS.
					EXPECT().
					ReadFile("blacklist.json").
					Return([]byte(`{
					"eip155:1": ["0x123", "0xabc"],
					"tezos:mainnet": ["KT1ABC", "KT1XYZ"]
				}`), nil)
				mockJSON.
					EXPECT().
					Unmarshal(gomock.Any(), gomock.Any()).
					DoAndReturn(func(data []byte, v interface{}) error {
						return json.Unmarshal(data, v)
					})
			},
			expectedErr: "",
			validateFunc: func(t *testing.T, reg registry.BlacklistRegistry) {
				assert.NotNil(t, reg)
				assert.True(t, reg.IsBlacklisted(domain.ChainEthereumMainnet, "0x123"))
				assert.True(t, reg.IsBlacklisted(domain.ChainEthereumMainnet, "0xabc"))
				assert.True(t, reg.IsBlacklisted(domain.ChainTezosMainnet, "KT1ABC"))
				assert.False(t, reg.IsBlacklisted(domain.ChainEthereumMainnet, "0x999"))
				assert.False(t, reg.IsBlacklisted(domain.ChainTezosMainnet, "KT1NONEXISTENT"))

				tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x123", "1")
				assert.True(t, reg.IsTokenCIDBlacklisted(tokenCID))
			},
		},
		{
			name: "successful load with empty blacklist",
			setupMocks: func(mockFS *mocks.MockFileSystem, mockJSON *mocks.MockJSON) {
				mockFS.
					EXPECT().
					ReadFile("blacklist.json").
					Return([]byte(`{}`), nil)
				mockJSON.
					EXPECT().
					Unmarshal(gomock.Any(), gomock.Any()).
					DoAndReturn(func(data []byte, v interface{}) error {
						return json.Unmarshal(data, v)
					})
			},
			expectedErr: "",
			validateFunc: func(t *testing.T, reg registry.BlacklistRegistry) {
				assert.NotNil(t, reg)
				assert.False(t, reg.IsBlacklisted(domain.ChainEthereumMainnet, "0x123"))
			},
		},
		{
			name: "file read error",
			setupMocks: func(mockFS *mocks.MockFileSystem, mockJSON *mocks.MockJSON) {
				mockFS.
					EXPECT().
					ReadFile("blacklist.json").
					Return(nil, assert.AnError)
			},
			expectedErr: "failed to read blacklist file",
		},
		{
			name: "JSON parse error",
			setupMocks: func(mockFS *mocks.MockFileSystem, mockJSON *mocks.MockJSON) {
				blacklistJSON := []byte(`invalid json`)
				mockFS.
					EXPECT().
					ReadFile("blacklist.json").
					Return(blacklistJSON, nil)
				mockJSON.
					EXPECT().
					Unmarshal(blacklistJSON, gomock.Any()).
					Return(assert.AnError)
			},
			expectedErr: "failed to parse blacklist JSON",
		},
		{
			name: "case insensitive lookup",
			setupMocks: func(mockFS *mocks.MockFileSystem, mockJSON *mocks.MockJSON) {
				mockFS.
					EXPECT().
					ReadFile("blacklist.json").
					Return([]byte(`{
					"EIP155:1": ["0x123ABC"]
				}`), nil)
				mockJSON.
					EXPECT().
					Unmarshal(gomock.Any(), gomock.Any()).
					DoAndReturn(func(data []byte, v interface{}) error {
						return json.Unmarshal(data, v)
					})
			},
			expectedErr: "",
			validateFunc: func(t *testing.T, reg registry.BlacklistRegistry) {
				// Test case insensitive chain ID
				assert.True(t, reg.IsBlacklisted(domain.ChainEthereumMainnet, "0x123ABC"))
				// Test case insensitive address
				assert.True(t, reg.IsBlacklisted(domain.ChainEthereumMainnet, "0X123ABC"))
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

			loader := registry.NewBlacklistRegistryLoader(mockFS, mockJSON)
			reg, err := loader.Load("blacklist.json")

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

func TestBlacklistRegistry_IsBlacklisted(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockFS := mocks.NewMockFileSystem(ctrl)
	mockJSON := mocks.NewMockJSON(ctrl)

	// Create registry through loader
	mockFS.
		EXPECT().
		ReadFile("blacklist.json").
		Return([]byte(`{
		"eip155:1": ["0x123", "0xabc"],
		"tezos:mainnet": ["KT1ABC"]
	}`), nil)
	mockJSON.
		EXPECT().
		Unmarshal(gomock.Any(), gomock.Any()).
		DoAndReturn(func(data []byte, v interface{}) error {
			return json.Unmarshal(data, v)
		})

	loader := registry.NewBlacklistRegistryLoader(mockFS, mockJSON)
	reg, err := loader.Load("blacklist.json")
	assert.NoError(t, err)
	assert.NotNil(t, reg)

	tests := []struct {
		name     string
		chainID  domain.Chain
		address  string
		expected bool
	}{
		{
			name:     "found in blacklist",
			chainID:  domain.ChainEthereumMainnet,
			address:  "0x123",
			expected: true,
		},
		{
			name:     "not found",
			chainID:  domain.ChainEthereumMainnet,
			address:  "0x999",
			expected: false,
		},
		{
			name:     "case insensitive lookup",
			chainID:  domain.ChainEthereumMainnet,
			address:  "0X123",
			expected: true,
		},
		{
			name:     "tezos address",
			chainID:  domain.ChainTezosMainnet,
			address:  "KT1ABC",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := reg.IsBlacklisted(tt.chainID, tt.address)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBlacklistRegistry_IsTokenCIDBlacklisted(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockFS := mocks.NewMockFileSystem(ctrl)
	mockJSON := mocks.NewMockJSON(ctrl)

	// Create registry through loader
	mockFS.
		EXPECT().
		ReadFile("blacklist.json").
		Return([]byte(`{
		"eip155:1": ["0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb"]
	}`), nil)
	mockJSON.
		EXPECT().
		Unmarshal(gomock.Any(), gomock.Any()).
		DoAndReturn(func(data []byte, v interface{}) error {
			return json.Unmarshal(data, v)
		})

	loader := registry.NewBlacklistRegistryLoader(mockFS, mockJSON)
	reg, err := loader.Load("blacklist.json")
	assert.NoError(t, err)
	assert.NotNil(t, reg)

	tests := []struct {
		name     string
		tokenCID domain.TokenCID
		expected bool
	}{
		{
			name:     "blacklisted token",
			tokenCID: domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb", "1"),
			expected: true,
		},
		{
			name:     "not blacklisted token",
			tokenCID: domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x396343362be2A4dA1cE0C1C210945346fb82Aa49", "1"),
			expected: false,
		},
		{
			name:     "invalid tokenCID",
			tokenCID: domain.TokenCID("invalid:tokenCID"),
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := reg.IsTokenCIDBlacklisted(tt.tokenCID)
			assert.Equal(t, tt.expected, result)
		})
	}
}

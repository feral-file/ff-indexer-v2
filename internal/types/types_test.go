package types

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

func TestStringPtr(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected *string
	}{
		{
			name:     "empty string",
			input:    "",
			expected: stringPtr(""),
		},
		{
			name:     "non-empty string",
			input:    "test",
			expected: stringPtr("test"),
		},
		{
			name:     "unicode string",
			input:    "测试",
			expected: stringPtr("测试"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := StringPtr(tt.input)
			assert.NotNil(t, result)
			assert.Equal(t, *tt.expected, *result)
			assert.Equal(t, tt.input, *result)
		})
	}
}

func TestStringNilOrEmpty(t *testing.T) {
	tests := []struct {
		name     string
		input    *string
		expected bool
	}{
		{
			name:     "nil pointer",
			input:    nil,
			expected: true,
		},
		{
			name:     "empty string",
			input:    stringPtr(""),
			expected: true,
		},
		{
			name:     "non-empty string",
			input:    stringPtr("test"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := StringNilOrEmpty(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSafeString(t *testing.T) {
	tests := []struct {
		name     string
		input    *string
		expected string
	}{
		{
			name:     "nil pointer",
			input:    nil,
			expected: "",
		},
		{
			name:     "empty string",
			input:    stringPtr(""),
			expected: "",
		},
		{
			name:     "non-empty string",
			input:    stringPtr("test"),
			expected: "test",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SafeString(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsNumeric(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{
			name:     "valid numeric",
			input:    "123",
			expected: true,
		},
		{
			name:     "valid numeric zero",
			input:    "0",
			expected: true,
		},
		{
			name:     "valid numeric large number",
			input:    "99999999999999999999999999999999999999999999999999999999999999999999999999999",
			expected: true,
		},
		{
			name:     "invalid with letter",
			input:    "123a",
			expected: false,
		},
		{
			name:     "invalid empty",
			input:    "",
			expected: false,
		},
		{
			name:     "invalid with negative sign",
			input:    "-123",
			expected: false,
		},
		{
			name:     "invalid with decimal",
			input:    "12.3",
			expected: false,
		},
		{
			name:     "invalid with symbol",
			input:    "123$",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsNumeric(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsPositiveNumeric(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{
			name:     "valid positive numeric",
			input:    "123",
			expected: true,
		},
		{
			name:     "valid positive numeric single digit",
			input:    "9",
			expected: true,
		},
		{
			name:     "invalid zero",
			input:    "0",
			expected: false,
		},
		{
			name:     "invalid with leading zero",
			input:    "0123",
			expected: false,
		},
		{
			name:     "invalid empty",
			input:    "",
			expected: false,
		},
		{
			name:     "invalid with letter",
			input:    "123a",
			expected: false,
		},
		{
			name:     "invalid with negative sign",
			input:    "-123",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsPositiveNumeric(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsTezosAddress(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{
			name:     "valid tz1 address",
			input:    "tz1VSUr8wwNhLAzempoch5d6hLRiTh8Cjcjb",
			expected: true,
		},
		{
			name:     "valid tz2 address",
			input:    "tz2N4RqF6bgxXzZvSfbmLFFwY2bH7MvLvXeV",
			expected: true,
		},
		{
			name:     "valid tz3 address",
			input:    "tz3RDC3Jdn4j15J7bBHZd29EUee9gVB1CxD9",
			expected: true,
		},
		{
			name:     "valid tz4 address",
			input:    "tz4eYJcgwYcZbYXJyVChd1nEG6sTs7DeqhXw",
			expected: true,
		},
		{
			name:     "valid KT1 contract address",
			input:    "KT1BvXTW1XqhE1GHTRKRvz8w3a7X5f5NqEZr",
			expected: true,
		},
		{
			name:     "invalid empty",
			input:    "",
			expected: false,
		},
		{
			name:     "invalid ethereum address",
			input:    "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb",
			expected: false,
		},
		{
			name:     "invalid format",
			input:    "tz5invalid",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsTezosAddress(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsTezosContractAddress(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{
			name:     "valid KT1 contract address",
			input:    "KT1BvXTW1XqhE1GHTRKRvz8w3a7X5f5NqEZr",
			expected: true,
		},
		{
			name:     "valid KT1 with base58 chars",
			input:    "KT1Hq1a2NnMWxBjtVkg1W3J3qnZqNvB8a1Zx",
			expected: true,
		},
		{
			name:     "invalid tz1 address",
			input:    "tz1VSUr8wwNhLAzempoch5d6hLRiTh8Cjcjb",
			expected: false,
		},
		{
			name:     "invalid too short",
			input:    "KT1short",
			expected: false,
		},
		{
			name:     "invalid wrong prefix",
			input:    "KT2BvXTW1XqhE1GHTRKRvz8w3a7X5f5NqEZr",
			expected: false,
		},
		{
			name:     "invalid empty",
			input:    "",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsTezosContractAddress(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsEthereumAddress(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{
			name:     "valid ethereum address",
			input:    "0x396343362be2A4dA1cE0C1C210945346fb82Aa49",
			expected: true,
		},
		{
			name:     "valid ethereum address lowercase",
			input:    "0x396343362be2A4dA1cE0C1C210945346fb82Aa49",
			expected: true,
		},
		{
			name:     "valid ethereum address uppercase",
			input:    "0x396343362be2A4dA1cE0C1C210945346fb82Aa49",
			expected: true,
		},
		{
			name:     "invalid too short",
			input:    "0x742d35Cc6634C0532925a3b844Bc9e7595f0b",
			expected: false,
		},
		{
			name:     "invalid no 0x prefix",
			input:    "742d35Cc6634C0532925a3b844Bc9e7595f0bEb",
			expected: false,
		},
		{
			name:     "invalid empty",
			input:    "",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsEthereumAddress(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsValidURL(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{
			name:     "valid http URL",
			input:    "http://example.com",
			expected: true,
		},
		{
			name:     "valid https URL",
			input:    "https://example.com",
			expected: true,
		},
		{
			name:     "valid URL with path",
			input:    "https://example.com/path/to/resource",
			expected: true,
		},
		{
			name:     "valid URL with query",
			input:    "https://example.com?query=value",
			expected: true,
		},
		{
			name:     "invalid no scheme",
			input:    "example.com",
			expected: false,
		},
		{
			name:     "invalid no host",
			input:    "https://",
			expected: false,
		},
		{
			name:     "invalid empty",
			input:    "",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsValidURL(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestTransferEventTypeToProvenanceEventType(t *testing.T) {
	tests := []struct {
		name     string
		input    domain.EventType
		expected schema.ProvenanceEventType
	}{
		{
			name:     "mint event",
			input:    domain.EventTypeMint,
			expected: schema.ProvenanceEventTypeMint,
		},
		{
			name:     "transfer event",
			input:    domain.EventTypeTransfer,
			expected: schema.ProvenanceEventTypeTransfer,
		},
		{
			name:     "burn event",
			input:    domain.EventTypeBurn,
			expected: schema.ProvenanceEventTypeBurn,
		},
		{
			name:     "unknown event type defaults to transfer",
			input:    domain.EventType("unknown"),
			expected: schema.ProvenanceEventTypeTransfer,
		},
		{
			name:     "metadata_update event defaults to transfer",
			input:    domain.EventTypeMetadataUpdate,
			expected: schema.ProvenanceEventTypeTransfer,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := TransferEventTypeToProvenanceEventType(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestProvenanceEventTypeToSubjectType(t *testing.T) {
	tests := []struct {
		name                string
		provenanceEventType schema.ProvenanceEventType
		standard            domain.ChainStandard
		expected            schema.SubjectType
	}{
		{
			name:                "mint event",
			provenanceEventType: schema.ProvenanceEventTypeMint,
			standard:            domain.StandardERC721,
			expected:            schema.SubjectTypeToken,
		},
		{
			name:                "burn event",
			provenanceEventType: schema.ProvenanceEventTypeBurn,
			standard:            domain.StandardERC721,
			expected:            schema.SubjectTypeToken,
		},
		{
			name:                "transfer event ERC721",
			provenanceEventType: schema.ProvenanceEventTypeTransfer,
			standard:            domain.StandardERC721,
			expected:            schema.SubjectTypeOwner,
		},
		{
			name:                "transfer event ERC1155",
			provenanceEventType: schema.ProvenanceEventTypeTransfer,
			standard:            domain.StandardERC1155,
			expected:            schema.SubjectTypeBalance,
		},
		{
			name:                "transfer event FA2",
			provenanceEventType: schema.ProvenanceEventTypeTransfer,
			standard:            domain.StandardFA2,
			expected:            schema.SubjectTypeBalance,
		},
		{
			name:                "metadata_update event",
			provenanceEventType: schema.ProvenanceEventTypeMetadataUpdate,
			standard:            domain.StandardERC721,
			expected:            schema.SubjectTypeMetadata,
		},
		{
			name:                "unknown event type defaults to token",
			provenanceEventType: schema.ProvenanceEventType("unknown"),
			standard:            domain.StandardERC721,
			expected:            schema.SubjectTypeToken,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ProvenanceEventTypeToSubjectType(tt.provenanceEventType, tt.standard)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Helper function for tests
func stringPtr(s string) *string {
	return &s
}

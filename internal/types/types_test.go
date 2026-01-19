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

func TestAddressToBlockchain(t *testing.T) {
	tests := []struct {
		name     string
		address  string
		expected domain.Blockchain
	}{
		{
			name:     "ethereum address",
			address:  "0x4838B106FCe9647Bdf1E7877BF73cE8B0BAD5f97",
			expected: domain.BlockchainEthereum,
		},
		{
			name:     "ethereum with all lowercase",
			address:  "0x4838b106fce9647bdf1e7877bf73ce8b0bad5f97",
			expected: domain.BlockchainEthereum,
		},
		{
			name:     "ethereum address uppercase 0X prefix defaults to unknown",
			address:  "0X742D35CC6634C0532925A3B844BC9E7595F0BEB",
			expected: domain.BlockchainUnknown,
		},
		{
			name:     "tezos address",
			address:  "tz1VSUr8wwNhLAzempoch5d6hLRiTh8Cjcjb",
			expected: domain.BlockchainTezos,
		},
		{
			name:     "tezos contract address",
			address:  "KT1BvXTW1XqhE1GHTRKRvz8w3a7X5f5NqEZr",
			expected: domain.BlockchainTezos,
		},
		{
			name:     "empty address defaults to unknown",
			address:  "",
			expected: domain.BlockchainUnknown,
		},
		{
			name:     "non-0x address defaults to unknown",
			address:  "someaddress",
			expected: domain.BlockchainUnknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := AddressToBlockchain(tt.address)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsHTTPSURL(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{
			name:     "valid https URL",
			input:    "https://example.com",
			expected: true,
		},
		{
			name:     "valid https URL with path",
			input:    "https://example.com/path/to/resource",
			expected: true,
		},
		{
			name:     "valid https URL with query",
			input:    "https://example.com?query=value",
			expected: true,
		},
		{
			name:     "valid https URL with port",
			input:    "https://example.com:8443",
			expected: true,
		},
		{
			name:     "valid https URL with subdomain",
			input:    "https://api.example.com",
			expected: true,
		},
		{
			name:     "invalid http URL",
			input:    "http://example.com",
			expected: false,
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
		{
			name:     "invalid ftp scheme",
			input:    "ftp://example.com",
			expected: false,
		},
		{
			name:     "invalid malformed URL",
			input:    "https:// invalid url",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsHTTPSURL(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGenerateUUID(t *testing.T) {
	t.Run("generates valid UUID", func(t *testing.T) {
		id, err := GenerateUUID()
		assert.NoError(t, err)
		assert.NotEmpty(t, id)
		// Check format: 8-4-4-4-12 hex characters
		assert.Regexp(t, `^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`, id)
	})

	t.Run("generates multiple unique UUIDs", func(t *testing.T) {
		ids := make(map[string]bool)
		count := 1000
		for range count {
			id, err := GenerateUUID()
			assert.NoError(t, err)
			assert.False(t, ids[id], "UUID should be unique")
			ids[id] = true
		}
		assert.Equal(t, count, len(ids))
	})
}

func TestGenerateSecureToken(t *testing.T) {
	t.Run("generates token with correct length", func(t *testing.T) {
		length := 32
		token, err := GenerateSecureToken(length)
		assert.NoError(t, err)
		assert.NotEmpty(t, token)
		// Output should be length*2 hex characters
		assert.Equal(t, length*2, len(token))
	})

	t.Run("generates valid hex string", func(t *testing.T) {
		token, err := GenerateSecureToken(16)
		assert.NoError(t, err)
		assert.Regexp(t, `^[0-9a-f]+$`, token)
	})

	t.Run("generates tokens of different lengths", func(t *testing.T) {
		tests := []int{8, 16, 32, 64, 128, 256, 512, 1024}
		for _, length := range tests {
			token, err := GenerateSecureToken(length)
			assert.NoError(t, err)
			assert.Equal(t, length*2, len(token), "Token length should be %d*2=%d", length, length*2)
		}
	})

	t.Run("generates multiple unique tokens", func(t *testing.T) {
		tokens := make(map[string]bool)
		count := 1000
		for range count {
			token, err := GenerateSecureToken(32)
			assert.NoError(t, err)
			assert.False(t, tokens[token], "Token should be unique")
			tokens[token] = true
		}
		assert.Equal(t, count, len(tokens))
	})

	t.Run("handles small lengths", func(t *testing.T) {
		token, err := GenerateSecureToken(1)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(token))
	})

	t.Run("handles zero length", func(t *testing.T) {
		token, err := GenerateSecureToken(0)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(token))
	})
}

func TestIsIPFSGatewayURL(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectedOK  bool
		expectedCID string
	}{
		{
			name:        "valid IPFS gateway URL with CIDv0",
			input:       "https://ipfs.io/ipfs/QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG",
			expectedOK:  true,
			expectedCID: "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG",
		},
		{
			name:        "valid IPFS gateway URL with CIDv0 and path",
			input:       "https://ipfs.io/ipfs/QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG/image.png",
			expectedOK:  true,
			expectedCID: "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG/image.png",
		},
		{
			name:        "valid IPFS gateway URL with CIDv1 bafybei",
			input:       "https://gateway.pinata.cloud/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi",
			expectedOK:  true,
			expectedCID: "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi",
		},
		{
			name:        "valid IPFS gateway URL with CIDv1 bafkrei",
			input:       "https://dweb.link/ipfs/bafkreih2grj7izfxk5wxgprr34ubv5bbmoq23ikqjsjvdvkfsldgddhgxe",
			expectedOK:  true,
			expectedCID: "bafkreih2grj7izfxk5wxgprr34ubv5bbmoq23ikqjsjvdvkfsldgddhgxe",
		},
		{
			name:        "valid HTTP IPFS gateway URL",
			input:       "http://localhost:8080/ipfs/QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG",
			expectedOK:  true,
			expectedCID: "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG",
		},
		{
			name:        "valid IPFS gateway URL with subdomain",
			input:       "https://api.example.com/ipfs/QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG",
			expectedOK:  true,
			expectedCID: "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG",
		},
		{
			name:        "valid IPFS gateway URL with port",
			input:       "https://localhost:8443/ipfs/QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG",
			expectedOK:  true,
			expectedCID: "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG",
		},
		{
			name:       "invalid IPFS URI scheme",
			input:      "ipfs://QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG",
			expectedOK: false,
		},
		{
			name:       "invalid URL with /ipfs/ but invalid CID",
			input:      "https://example.com/ipfs/invalid-cid",
			expectedOK: false,
		},
		{
			name:       "invalid URL with /ipfs/ but no CID",
			input:      "https://example.com/ipfs/",
			expectedOK: false,
		},
		{
			name:       "invalid URL with /ipfs/ in path but not CID path",
			input:      "https://example.com/my-ipfs-storage/file.txt",
			expectedOK: false,
		},
		{
			name:       "invalid URL without /ipfs/",
			input:      "https://example.com/QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG",
			expectedOK: false,
		},
		{
			name:       "invalid empty string",
			input:      "",
			expectedOK: false,
		},
		{
			name:       "invalid not a URL",
			input:      "not-a-url",
			expectedOK: false,
		},
		{
			name:       "invalid CID too short",
			input:      "https://ipfs.io/ipfs/Qm123",
			expectedOK: false,
		},
		{
			name:       "invalid CIDv0 with wrong characters",
			input:      "https://ipfs.io/ipfs/Qm0OIl123456789012345678901234567890123456",
			expectedOK: false,
		},
		{
			name:       "URL contains ipfs but in domain name",
			input:      "https://my-ipfs-gateway.com/files/document.pdf",
			expectedOK: false,
		},
		{
			name:       "URL with /ipfs/ in query parameter",
			input:      "https://example.com/file?path=/ipfs/QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG",
			expectedOK: false,
		},
		{
			name:        "valid CIDv1 longer format",
			input:       "https://cloudflare-ipfs.com/ipfs/bafybeie5gq4jxvzmsym6hjlwxej4rwdoxt7wadqvmmwbqi7r27fclha2va",
			expectedOK:  true,
			expectedCID: "bafybeie5gq4jxvzmsym6hjlwxej4rwdoxt7wadqvmmwbqi7r27fclha2va",
		},
		{
			name:        "valid CIDv1 with different prefix (bafkreif)",
			input:       "https://ipfs.io/ipfs/bafkreifzjut3te2nhyekklss27nh3k72ysco7y32koao5eei66wof36n5e",
			expectedOK:  true,
			expectedCID: "bafkreifzjut3te2nhyekklss27nh3k72ysco7y32koao5eei66wof36n5e",
		},
		{
			name:        "invalid host name",
			input:       "https://example@com/ipfs/QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG",
			expectedOK:  false,
			expectedCID: "",
		},
		{
			name:        "invalid URL with whitespace",
			input:       "https://example .com/ipfs/QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG",
			expectedOK:  false,
			expectedCID: "",
		},
		{
			name:        "IP address as host name",
			input:       "https://192.168.1.1/ipfs/QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG",
			expectedOK:  true,
			expectedCID: "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ok, cid := IsIPFSGatewayURL(tt.input)
			assert.Equal(t, tt.expectedOK, ok)
			if tt.expectedOK {
				assert.Equal(t, tt.expectedCID, cid)
			} else {
				assert.Empty(t, cid)
			}
		})
	}
}

func TestIsArweaveGatewayURL(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		expectedOK   bool
		expectedTxID string
	}{
		{
			name:         "valid Arweave gateway URL",
			input:        "https://arweave.net/sKqjvP7jFwM5HLZmyJQC_9l5hN7TVIYhT6MvSHDqwo0",
			expectedOK:   true,
			expectedTxID: "sKqjvP7jFwM5HLZmyJQC_9l5hN7TVIYhT6MvSHDqwo0",
		},
		{
			name:         "valid Arweave gateway URL with ar-io",
			input:        "https://ar-io.net/abc123def456ghi789jkl012mno345pqr678stuv012",
			expectedOK:   true,
			expectedTxID: "abc123def456ghi789jkl012mno345pqr678stuv012",
		},
		{
			name:         "valid Arweave gateway URL with path",
			input:        "https://arweave.net/sKqjvP7jFwM5HLZmyJQC_9l5hN7TVIYhT6MvSHDqwo0/metadata.json",
			expectedOK:   true,
			expectedTxID: "sKqjvP7jFwM5HLZmyJQC_9l5hN7TVIYhT6MvSHDqwo0",
		},
		{
			name:         "valid HTTP Arweave gateway URL",
			input:        "http://arweave.net/sKqjvP7jFwM5HLZmyJQC_9l5hN7TVIYhT6MvSHDqwo0",
			expectedOK:   true,
			expectedTxID: "sKqjvP7jFwM5HLZmyJQC_9l5hN7TVIYhT6MvSHDqwo0",
		},
		{
			name:         "valid Arweave gateway URL with port",
			input:        "https://arweave.net:8080/sKqjvP7jFwM5HLZmyJQC_9l5hN7TVIYhT6MvSHDqwo0",
			expectedOK:   true,
			expectedTxID: "sKqjvP7jFwM5HLZmyJQC_9l5hN7TVIYhT6MvSHDqwo0",
		},
		{
			name:         "invalid Arweave URI scheme",
			input:        "ar://sKqjvP7jFwM5HLZmyJQC_9l5hN7TVIYhT6MvSHDqwo0",
			expectedOK:   false,
			expectedTxID: "",
		},
		{
			name:         "invalid tx ID too short (42 chars)",
			input:        "https://arweave.net/abc123def456ghi789jkl012mno345pqr678stu",
			expectedOK:   false,
			expectedTxID: "",
		},
		{
			name:         "invalid tx ID too long (44 chars)",
			input:        "https://arweave.net/abc123def456ghi789jkl012mno345pqr678stu90",
			expectedOK:   false,
			expectedTxID: "",
		},
		{
			name:         "invalid URL with spaces",
			input:        "https://arweave.net/sKqjvP7jFwM5HLZmy JQC_9l5hN7TVIYhT6MvSHDqwo0",
			expectedOK:   false,
			expectedTxID: "",
		},
		{
			name:         "invalid empty string",
			input:        "",
			expectedOK:   false,
			expectedTxID: "",
		},
		{
			name:         "invalid URL without tx ID",
			input:        "https://arweave.net/",
			expectedOK:   false,
			expectedTxID: "",
		},
		{
			name:         "valid with underscore in tx ID",
			input:        "https://arweave.net/sKqjvP7jFwM5HLZmyJQC_9l5hN7TVIYhT6MvSHDqwo0",
			expectedOK:   true,
			expectedTxID: "sKqjvP7jFwM5HLZmyJQC_9l5hN7TVIYhT6MvSHDqwo0",
		},
		{
			name:         "valid with dash in tx ID",
			input:        "https://arweave.net/abc123def456ghi789-kl012mno345pqr678stuv012",
			expectedOK:   true,
			expectedTxID: "abc123def456ghi789-kl012mno345pqr678stuv012",
		},
		{
			name:         "invalid special character in tx ID",
			input:        "https://arweave.net/sKqjvP7jFwM5HLZmyJQC$9l5hN7TVIYhT6MvSHDqwo0",
			expectedOK:   false,
			expectedTxID: "",
		},
		{
			name:         "real Arweave tx ID example",
			input:        "https://arweave.net/B844nmKXjiBE0eMgATBHlVIRU0Wex9Ke3dnd0jC00lQ",
			expectedOK:   true,
			expectedTxID: "B844nmKXjiBE0eMgATBHlVIRU0Wex9Ke3dnd0jC00lQ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ok, txID := IsArweaveGatewayURL(tt.input)
			assert.Equal(t, tt.expectedOK, ok)
			if tt.expectedOK {
				assert.Equal(t, tt.expectedTxID, txID)
			} else {
				assert.Empty(t, txID)
			}
		})
	}
}

func TestIsOnChFSGatewayURL(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		expectedOK   bool
		expectedHash string
	}{
		{
			name:         "valid OnChFS URL with fxhash2.xyz",
			input:        "https://onchfs.fxhash2.xyz/a1b2c3d4e5f67890abcdef1234567890abcdef1234567890abcdef1234567890",
			expectedOK:   true,
			expectedHash: "a1b2c3d4e5f67890abcdef1234567890abcdef1234567890abcdef1234567890",
		},
		{
			name:         "valid OnChFS URL with fxhash.xyz",
			input:        "https://gateway.fxhash.xyz/a1b2c3d4e5f67890abcdef1234567890abcdef1234567890abcdef1234567890",
			expectedOK:   true,
			expectedHash: "a1b2c3d4e5f67890abcdef1234567890abcdef1234567890abcdef1234567890",
		},
		{
			name:         "valid OnChFS URL with onchfs in hostname",
			input:        "https://onchfs.example.com/a1b2c3d4e5f67890abcdef1234567890abcdef1234567890abcdef1234567890",
			expectedOK:   true,
			expectedHash: "a1b2c3d4e5f67890abcdef1234567890abcdef1234567890abcdef1234567890",
		},
		{
			name:         "valid OnChFS URL with uppercase hex",
			input:        "https://onchfs.fxhash2.xyz/A1B2C3D4E5F67890ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890",
			expectedOK:   true,
			expectedHash: "A1B2C3D4E5F67890ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890",
		},
		{
			name:         "valid OnChFS URL with mixed case hex",
			input:        "https://onchfs.fxhash2.xyz/a1B2c3D4e5F67890AbCdEf1234567890abcdef1234567890ABCDEF1234567890",
			expectedOK:   true,
			expectedHash: "a1B2c3D4e5F67890AbCdEf1234567890abcdef1234567890ABCDEF1234567890",
		},
		{
			name:         "valid OnChFS URL with query params",
			input:        "https://onchfs.fxhash2.xyz/a1b2c3d4e5f67890abcdef1234567890abcdef1234567890abcdef1234567890?param=value",
			expectedOK:   true,
			expectedHash: "a1b2c3d4e5f67890abcdef1234567890abcdef1234567890abcdef1234567890",
		},
		{
			name:         "valid OnChFS URL with path",
			input:        "https://onchfs.fxhash2.xyz/a1b2c3d4e5f67890abcdef1234567890abcdef1234567890abcdef1234567890/metadata.json",
			expectedOK:   true,
			expectedHash: "a1b2c3d4e5f67890abcdef1234567890abcdef1234567890abcdef1234567890",
		},
		{
			name:         "valid HTTP OnChFS URL",
			input:        "http://onchfs.fxhash2.xyz/a1b2c3d4e5f67890abcdef1234567890abcdef1234567890abcdef1234567890",
			expectedOK:   true,
			expectedHash: "a1b2c3d4e5f67890abcdef1234567890abcdef1234567890abcdef1234567890",
		},
		{
			name:         "valid OnChFS URL with port",
			input:        "https://onchfs.fxhash2.xyz:8080/a1b2c3d4e5f67890abcdef1234567890abcdef1234567890abcdef1234567890",
			expectedOK:   true,
			expectedHash: "a1b2c3d4e5f67890abcdef1234567890abcdef1234567890abcdef1234567890",
		},
		{
			name:         "invalid host without onchfs or fxhash",
			input:        "https://example.com/a1b2c3d4e5f67890abcdef1234567890abcdef1234567890abcdef1234567890",
			expectedOK:   false,
			expectedHash: "",
		},
		{
			name:         "invalid hash too short (63 chars)",
			input:        "https://onchfs.fxhash2.xyz/a1b2c3d4e5f67890abcdef1234567890abcdef1234567890abcdef123456789",
			expectedOK:   false,
			expectedHash: "",
		},
		{
			name:         "invalid hash too long (65 chars)",
			input:        "https://onchfs.fxhash2.xyz/a1b2c3d4e5f67890abcdef1234567890abcdef1234567890abcdef12345678901",
			expectedOK:   false,
			expectedHash: "",
		},
		{
			name:         "invalid non-hex character in hash",
			input:        "https://onchfs.fxhash2.xyz/g1b2c3d4e5f67890abcdef1234567890abcdef1234567890abcdef1234567890",
			expectedOK:   false,
			expectedHash: "",
		},
		{
			name:         "invalid IPFS CID instead of hash",
			input:        "https://onchfs.fxhash2.xyz/QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG",
			expectedOK:   false,
			expectedHash: "",
		},
		{
			name:         "invalid empty string",
			input:        "",
			expectedOK:   false,
			expectedHash: "",
		},
		{
			name:         "invalid URL without hash",
			input:        "https://onchfs.fxhash2.xyz/",
			expectedOK:   false,
			expectedHash: "",
		},
		{
			name:         "invalid onchfs URI scheme",
			input:        "onchfs://a1b2c3d4e5f67890abcdef1234567890abcdef1234567890abcdef1234567890",
			expectedOK:   false,
			expectedHash: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ok, hash := IsOnChFSGatewayURL(tt.input)
			assert.Equal(t, tt.expectedOK, ok)
			if tt.expectedOK {
				assert.Equal(t, tt.expectedHash, hash)
			} else {
				assert.Empty(t, hash)
			}
		})
	}
}

// Helper function for tests
func stringPtr(s string) *string {
	return &s
}

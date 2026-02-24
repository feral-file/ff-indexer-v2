package types

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/google/uuid"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
)

// StringPtr converts a string to a pointer to a string
func StringPtr(s string) *string {
	return &s
}

// StringNilOrEmpty checks if a pointer to a string is nil or empty
func StringNilOrEmpty(s *string) bool {
	return s == nil || *s == ""
}

// SafeString returns a safe string from a pointer to a string
func SafeString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// IsNumeric checks if a string is a valid numeric value
func IsNumeric(s string) bool {
	return regexp.MustCompile(`^[0-9]+$`).MatchString(s)
}

// IsPositiveNumeric checks if a string is a valid positive numeric value
func IsPositiveNumeric(s string) bool {
	regex := regexp.MustCompile(`^[1-9][0-9]*$`)
	return regex.MatchString(s)
}

// IsTezosAddress checks if a string is a valid Tezos address
func IsTezosAddress(s string) bool {
	return strings.HasPrefix(s, "tz1") ||
		strings.HasPrefix(s, "tz2") ||
		strings.HasPrefix(s, "tz3") ||
		strings.HasPrefix(s, "tz4") ||
		IsTezosContractAddress(s)
}

// IsTezosContractAddress checks if a string is a valid Tezos contract address
func IsTezosContractAddress(s string) bool {
	return regexp.MustCompile(`^KT1[1-9A-HJ-NP-Za-km-z]{33}$`).MatchString(s)
}

// IsEthereumAddress checks if a string is a valid Ethereum address
func IsEthereumAddress(s string) bool {
	return common.IsHexAddress(s)
}

// AddressToBlockchain converts an address to the blockchain it belongs to
func AddressToBlockchain(address string) domain.Blockchain {
	if IsEthereumAddress(address) {
		return domain.BlockchainEthereum
	}
	if IsTezosAddress(address) {
		return domain.BlockchainTezos
	}
	return domain.BlockchainUnknown
}

// IsValidURL checks if a string is a valid URL
func IsValidURL(s string) bool {
	url, err := url.Parse(s)
	if err != nil {
		return false
	}
	return url.Scheme != "" && url.Host != ""
}

// IsHTTPSURL checks if a string is a valid HTTPS URL
func IsHTTPSURL(s string) bool {
	u, err := url.Parse(s)
	if err != nil {
		return false
	}
	return u.Scheme == "https" && u.Host != ""
}

// GenerateUUID generates a new UUID v4
func GenerateUUID() (string, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}
	return id.String(), nil
}

// GenerateSecureToken generates a cryptographically secure random hex string
// The length parameter specifies the number of random bytes (output will be length*2 hex characters)
// For example, length=32 produces a 64-character hex string
func GenerateSecureToken(length int) (string, error) {
	bytes := make([]byte, length)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

// IsIPFSGatewayURL checks if a string is a valid IPFS gateway URL
// It matches URLs like https://ipfs.io/ipfs/Qm... or https://gateway.pinata.cloud/ipfs/bafybei...
// The CID must follow IPFS CID v0 (Qm...) or CIDv1 patterns
// Returns: (isValid, cidWithPath) where cidWithPath includes the CID and any path segments after it
func IsIPFSGatewayURL(s string) (bool, string) {
	// Regex pattern to match IPFS gateway URLs:
	// - Protocol: http:// or https://
	// - Host: valid hostname (alphanumeric, dots, hyphens, optional port)
	// - Path: /ipfs/{CID} followed by optional path (but no more /ipfs/ segments)
	// CIDv0: Qm followed by exactly 44 base58 characters (total 46 chars, fixed length)
	// CIDv1: b followed by at least 10 base32 characters (variable length, flexible)
	pattern := `^https?://[a-zA-Z0-9._-]+(?::[0-9]+)?/ipfs/((?:Qm[1-9A-HJ-NP-Za-km-z]{44}|b[a-z2-7]{10,}))(/.*)?$`
	re := regexp.MustCompile(pattern)

	matches := re.FindStringSubmatch(s)
	if len(matches) >= 2 {
		cid := matches[1]
		if len(matches) >= 3 && matches[2] != "" {
			// Include path if present
			return true, cid + matches[2]
		}
		return true, cid
	}

	return false, ""
}

// IsArweaveGatewayURL checks if a string is a valid Arweave gateway URL
// Returns: (isValid, txID) where txID is the transaction ID
// Example: https://arweave.net/abc123 -> (true, "abc123")
func IsArweaveGatewayURL(s string) (bool, string) {
	// Regex pattern to match Arweave gateway URLs
	// - Protocol: http:// or https://
	// - Host: valid hostname
	// - Path: /{txID} where txID is 43 characters of base64url
	pattern := `^https?://[a-zA-Z0-9._-]+(?::[0-9]+)?/([A-Za-z0-9_-]{43})(?:/.*)?$`
	re := regexp.MustCompile(pattern)

	matches := re.FindStringSubmatch(s)
	if len(matches) >= 2 {
		return true, matches[1]
	}

	return false, ""
}

// IsOnChFSGatewayURL checks if a string is a valid OnChFS gateway URL
// OnChFS uses Keccak-256 hashes which are 64 hex characters long
// Returns: (isValid, hash) where hash is the Keccak-256 hash with optional path/query
// Examples:
//   - https://onchfs.fxhash2.xyz/0123456789abcdef... (64 hex chars)
//   - https://gateway.fxhash.xyz/0123456789abcdef...
func IsOnChFSGatewayURL(s string) (bool, string) {
	// Match URLs with hosts containing onchfs, fxhash.xyz, or fxhash2.xyz
	// followed by a Keccak-256 hash (64 hex characters) with optional path/query
	// Pattern breakdown:
	// - Protocol: http:// or https://
	// - Host: must contain "onchfs" OR "fxhash.xyz" OR "fxhash2.xyz"
	// - Path: /[64 hex chars] followed by optional path or query params
	pattern := `^https?://[a-zA-Z0-9._-]*(?:onchfs|fxhash2?\.xyz)[a-zA-Z0-9._-]*(?::[0-9]+)?/([0-9a-fA-F]{64})(?:[/?].*)?$`
	re := regexp.MustCompile(pattern)

	matches := re.FindStringSubmatch(s)
	if len(matches) >= 2 {
		return true, matches[1]
	}

	return false, ""
}

// DataURI represents a parsed data URI according to RFC 2397
type DataURI struct {
	// MimeType is the media type (e.g., "image/png", "application/json")
	// Defaults to "text/plain" if not specified
	MimeType string
	// Parameters contains additional parameters from the metadata (e.g., charset=utf-8)
	Parameters map[string]string
	// IsBase64 indicates whether the data is base64-encoded
	IsBase64 bool
	// RawData is the raw data string before any decoding
	RawData string
	// DecodedData is the decoded data (base64 decoded if IsBase64 is true)
	DecodedData []byte
}

// ParseDataURI parses a data URI according to RFC 2397
// Format: data:[<mediatype>][;parameter=value][;base64],<data>
// Example: data:image/png;base64,iVBORw0KG...
// Example: data:application/json;charset=utf-8,{"key":"value"}
func ParseDataURI(uri string) (*DataURI, error) {
	// 1. Check if it starts with "data:"
	if !strings.HasPrefix(uri, "data:") {
		return nil, fmt.Errorf("invalid data URI: must start with 'data:'")
	}

	// 2. Remove "data:" prefix
	uriContent := uri[5:]

	// 3. Split by comma to separate metadata from data
	parts := strings.SplitN(uriContent, ",", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid data URI format: missing comma separator")
	}

	metadata := parts[0]
	rawData := parts[1]

	// 4. Parse metadata to extract mime type, parameters, and encoding
	mimeType, parameters, isBase64 := parseDataURIMetadata(metadata)

	// 5. Decode the data if base64-encoded
	var decodedData []byte
	var err error
	if isBase64 {
		decodedData, err = base64.StdEncoding.DecodeString(rawData)
		if err != nil {
			return nil, fmt.Errorf("failed to decode base64: %w", err)
		}
	} else {
		// For non-base64 data, URL-decode according to RFC 2397
		// The data part should be percent-encoded (e.g., %20 for space)
		decoded, err := url.QueryUnescape(rawData)
		if err != nil {
			return nil, fmt.Errorf("failed to URL-decode data: %w", err)
		}
		decodedData = []byte(decoded)
	}

	return &DataURI{
		MimeType:    mimeType,
		Parameters:  parameters,
		IsBase64:    isBase64,
		RawData:     rawData,
		DecodedData: decodedData,
	}, nil
}

// parseDataURIMetadata parses the metadata part of a data URI
// Returns: (mimeType, parameters, isBase64)
func parseDataURIMetadata(metadata string) (string, map[string]string, bool) {
	// Default mime type is text/plain according to RFC 2397
	mimeType := "text/plain"
	parameters := make(map[string]string)
	isBase64 := false

	if metadata == "" {
		return mimeType, parameters, isBase64
	}

	// Split by semicolon to get mime type and parameters
	parts := strings.Split(metadata, ";")

	// First part is the mime type (if not empty)
	if parts[0] != "" {
		mimeType = strings.TrimSpace(parts[0])
	}

	// Parse remaining parameters
	for i := 1; i < len(parts); i++ {
		param := strings.TrimSpace(parts[i])
		if param == "base64" {
			isBase64 = true
		} else if strings.Contains(param, "=") {
			// Parse key=value parameters
			kv := strings.SplitN(param, "=", 2)
			if len(kv) == 2 {
				key := strings.TrimSpace(kv[0])
				value := strings.TrimSpace(kv[1])
				parameters[key] = value
			}
		}
	}

	return mimeType, parameters, isBase64
}

// IsDataURI checks if a string is a valid data URI
func IsDataURI(s string) bool {
	return strings.HasPrefix(s, "data:") && strings.Contains(s, ",")
}

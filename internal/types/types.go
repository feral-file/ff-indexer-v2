package types

import (
	"crypto/rand"
	"encoding/hex"
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

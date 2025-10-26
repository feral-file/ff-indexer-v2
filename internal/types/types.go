package types

import (
	"regexp"
	"strings"

	"github.com/ethereum/go-ethereum/common"
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

// IsPositiveNumeric checks if a string is a valid positive numeric value
func IsPositiveNumeric(s string) bool {
	regex := regexp.MustCompile(`^[1-9][0-9]*$`)
	return regex.MatchString(s)
}

// IsTezosAddress checks if a string is a valid Tezos address
func IsTezosAddress(s string) bool {
	return strings.HasPrefix(s, "tz1") || strings.HasPrefix(s, "tz2") || strings.HasPrefix(s, "tz3") || strings.HasPrefix(s, "tz4") || strings.HasPrefix(s, "KT1")
}

// IsEthereumAddress checks if a string is a valid Ethereum address
func IsEthereumAddress(s string) bool {
	return common.IsHexAddress(s)
}

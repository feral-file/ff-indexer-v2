package types

import (
	"crypto/md5" //nolint:gosec,G501 // MD5 used for non-cryptographic database indexing only, not security
	"encoding/hex"
)

// MD5Hash computes the MD5 hash of a string and returns it as a hex string.
func MD5Hash(value string) string {
	hash := md5.Sum([]byte(value))
	return hex.EncodeToString(hash[:])
}

// MD5HashPtr computes the MD5 hash of a string pointer and returns it as a pointer to hex string.
func MD5HashPtr(value *string) *string {
	if value == nil || *value == "" {
		return nil
	}
	hash := MD5Hash(*value)
	return &hash
}

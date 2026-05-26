// Package helpers provides shared RPC utilities for the Ethereum provider and contract adapters.
package helpers

import (
	"errors"
	"strings"
)

// ErrContractNotFound is returned when a contract is not found or self-destructed.
var ErrContractNotFound = errors.New("contract not found")

// IsExecutionRevert reports whether an RPC error indicates a contract execution revert.
func IsExecutionRevert(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "execution reverted") ||
		strings.Contains(msg, "nonexistent token") ||
		strings.Contains(msg, "invalid opcode")
}

// IsOutOfGas reports whether an RPC error indicates an out-of-gas failure.
func IsOutOfGas(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "out of gas")
}

// IsTooManyResultsError reports whether a log query failed due to result limits.
func IsTooManyResultsError(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()
	return strings.Contains(errStr, "query returned more than 10000 results") ||
		strings.Contains(errStr, "too many results")
}

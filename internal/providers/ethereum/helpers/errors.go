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

// IsBlockRangeCapError reports whether a log query failed because the provider
// caps the queried block span rather than the result count, e.g.
// "range 9999999 exceeds limit of 10000". Unlike result-count limits, a span
// cap is a fixed provider property: once a span is rejected, every span that
// size or larger will be rejected too, so pagination must not probe above an
// accepted span again.
func IsBlockRangeCapError(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()
	return strings.Contains(errStr, "exceeds limit of") ||
		strings.Contains(errStr, "block range is too wide") ||
		strings.Contains(errStr, "range too large")
}

// IsTooManyResultsError reports whether a log query failed due to provider
// result or block-range limits. Both classes mean the same thing to callers:
// the queried window is too big and must be split, not treated as fatal.
func IsTooManyResultsError(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()
	return strings.Contains(errStr, "query returned more than 10000 results") ||
		strings.Contains(errStr, "too many results") ||
		IsBlockRangeCapError(err)
}

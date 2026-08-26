// Package helpers provides shared RPC utilities for the Ethereum provider and contract adapters.
package helpers

import (
	"errors"
	"strings"

	"github.com/ethereum/go-ethereum/rpc"
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

// containsAny reports whether the lowercased error text contains any needle.
// Provider messages are matched case-insensitively: vendors capitalize
// differently ("Block range limit exceeded" vs "block range is too wide") and
// a case slip must not turn a recoverable limit into a fatal walk.
func containsAny(err error, needles ...string) bool {
	msg := strings.ToLower(err.Error())
	for _, needle := range needles {
		if strings.Contains(msg, needle) {
			return true
		}
	}
	return false
}

// IsBlockRangeCapError reports whether a log query failed because the provider
// caps the queried block span rather than the result count. Unlike
// result-count limits, a span cap is a fixed provider property: once a span is
// rejected, every span that size or larger will be rejected too, so pagination
// must not probe above an accepted span again.
//
// Phrasings, each observed from a real provider:
//   - Infura:     "range 9999999 exceeds limit of 10000" (verified live)
//   - Chainstack: "Block range limit exceeded. See more details at
//     https://docs.chainstack.com/docs/limits#evm-range-limits" (-32602;
//     verified live through RealEthClient on 2026-08-26 — rejected above
//     toBlock-fromBlock = 10100. Any -32602 "invalid params" whose message
//     mentions a range or limit is treated as a span cap as well, so wording
//     drift degrades to a halving, not to an aborted walk)
//   - others:     "query exceeds max block range 100000",
//     "eth_getLogs is limited to 1024 block range", "block range is too wide",
//     "range too large"
func IsBlockRangeCapError(err error) bool {
	if err == nil {
		return false
	}
	if isInvalidParams(err) && containsAny(err, "range", "limit") {
		return true
	}
	return containsAny(err,
		"exceeds limit of",
		"block range limit", // Chainstack: "Block range limit exceeded. ..."
		"exceeds max block range",
		"block range is too wide",
		"range too large",
		"range is too large",
	) || (containsAny(err, "limited to") && containsAny(err, "block range"))
}

// isInvalidParams reports whether err carries JSON-RPC code -32602 (invalid
// params), the code Chainstack uses for its block-range rejection. The retry
// layer wraps provider errors with %w, so the code survives to callers.
func isInvalidParams(err error) bool {
	var rpcErr rpc.Error
	return errors.As(err, &rpcErr) && rpcErr.ErrorCode() == -32602
}

// IsTooManyResultsError reports whether a log query failed due to provider
// result or block-range limits. Both classes mean the same thing to callers:
// the queried window is too big and must be split, not treated as fatal.
//
// Result-cap phrasings observed: Infura "query returned more than 10000
// results", drpc "query returns too many logs, narrow your filter: 20000",
// "query exceeds max results 20000".
func IsTooManyResultsError(err error) bool {
	if err == nil {
		return false
	}
	return containsAny(err,
		"query returned more than",
		"too many results",
		"too many logs",
		"exceeds max results",
	) || IsBlockRangeCapError(err)
}

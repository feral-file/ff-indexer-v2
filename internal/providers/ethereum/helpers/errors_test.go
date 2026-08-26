package helpers_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/helpers"
)

func TestIsTooManyResultsError(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"alchemy result cap", errors.New("query returned more than 10000 results"), true},
		{"generic too many results", errors.New("too many results"), true},
		// Observed in production: provider caps the queried block span, and the
		// error arrives wrapped by the retry layer.
		{
			"range cap wrapped by retry layer",
			fmt.Errorf("ethereum operation FilterLogs failed after retries: permanent error: range 9999999 exceeds limit of 10000"),
			true,
		},
		{"block range too wide", errors.New("block range is too wide"), true},
		{"range too large", errors.New("range too large"), true},
		// Chainstack's span cap, as reported by users (its docs print no message).
		{
			"chainstack range cap wrapped by retry layer",
			fmt.Errorf("ethereum operation FilterLogs failed after retries: permanent error: Block range limit exceeded. See more details at https://docs.chainstack.com/docs/limits#evm-range-limits"),
			true,
		},
		// drpc result cap, observed live.
		{"drpc result cap", errors.New("query returns too many logs, narrow your filter: 20000"), true},
		{"max results phrasing", errors.New("query exceeds max results 20000, retry with the range 23879634-23879696"), true},
		{"unrelated error", errors.New("execution reverted"), false},
		{"connection error", errors.New("connection refused"), false},
		{"invalid params without a range hint", errors.New("invalid argument 0: json: cannot unmarshal"), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, helpers.IsTooManyResultsError(tc.err))
		})
	}
}

// rpcError mimics go-ethereum's jsonError: the provider's message plus the
// JSON-RPC code, which errors.As can reach through the retry layer's %w.
type rpcError struct {
	code int
	msg  string
}

func (e *rpcError) Error() string  { return e.msg }
func (e *rpcError) ErrorCode() int { return e.code }

func TestIsBlockRangeCapError(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{
			"range cap wrapped by retry layer",
			fmt.Errorf("ethereum operation FilterLogs failed after retries: permanent error: range 9999999 exceeds limit of 10000"),
			true,
		},
		{"block range too wide", errors.New("block range is too wide"), true},
		{"range too large", errors.New("range too large"), true},
		{"chainstack range cap", errors.New("Block range limit exceeded. See more details at https://docs.chainstack.com/docs/limits#evm-range-limits"), true},
		// -32602 with any range/limit wording is a span cap even if the exact text
		// drifts from what users reported; wrapped as the retry layer wraps it.
		{
			"invalid params mentioning a range, wrapped",
			fmt.Errorf("ethereum operation FilterLogs failed after retries: permanent error: %w", &rpcError{code: -32602, msg: "eth_getLogs block range too big"}),
			true,
		},
		{"invalid params without range wording", &rpcError{code: -32602, msg: "invalid argument 0: hex string without 0x prefix"}, false},
		{"other code mentioning a range", &rpcError{code: -32000, msg: "out of range"}, false},
		{"uppercase phrasing is matched", errors.New("RANGE TOO LARGE"), true},
		{"max block range phrasing", errors.New("query exceeds max block range 100000"), true},
		{"limited-to phrasing", errors.New("eth_getLogs is limited to 1024 block range. Please check the parameter requirements"), true},
		// Result-count limits are data-dependent, not a fixed span cap.
		{"drpc result cap", errors.New("query returns too many logs, narrow your filter: 20000"), false},
		{"alchemy result cap", errors.New("query returned more than 10000 results"), false},
		{"generic too many results", errors.New("too many results"), false},
		{"unrelated error", errors.New("execution reverted"), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, helpers.IsBlockRangeCapError(tc.err))
		})
	}
}

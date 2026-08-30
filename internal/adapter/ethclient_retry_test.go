package adapter

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestIsRetryableEthError_TransientProviderFailures pins the classifications a
// long pagination walk depends on. A misclassified transient error is not a
// slow retry — executeWithRetry wraps it in backoff.Permanent, so the walk
// aborts and every eth_getLogs call already paid for in that walk is discarded
// (a full owner scan is thousands of calls).
//
// The "unexpected EOF" case is a regression guard: the retryable list is
// matched against the LOWERCASED error message, so the entries themselves must
// be lowercase. Written as "EOF"/"ECONNRESET"/"ETIMEDOUT" they could never
// match, which silently made a dropped connection a permanent failure.
func TestIsRetryableEthError_TransientProviderFailures(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		err  error
	}{
		{"dropped connection mid-response", errors.New(`Post "https://mainnet.infura.io/v3/key": unexpected EOF`)},
		{"uppercase ECONNRESET from syscall text", errors.New("write tcp 1.2.3.4:443: ECONNRESET")},
		{"uppercase ETIMEDOUT from syscall text", errors.New("dial tcp 1.2.3.4:443: ETIMEDOUT")},
		{"provider 500 on a well-formed query", errors.New("Internal error")},
		{"connection reset by peer", errors.New("read tcp: connection reset by peer")},
		{"rate limited", errors.New("429 Too Many Requests")},
		// Both spellings are live provider phrasings; dropping either turns a
		// transient cancellation into an aborted walk (bot finding on #144).
		{"query cancelled (British spelling)", errors.New("query cancelled")}, //nolint:misspell
		{"query canceled (US spelling)", errors.New("query canceled")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.True(t, isRetryableEthError(tc.err), "must retry: %v", tc.err)
		})
	}
}

// TestIsRetryableEthError_PermanentFailures pins the errors that must NOT be
// retried. Provider window-limit errors are the load-bearing case: the
// pagination helper reacts to them by halving the block window
// (helpers.IsTooManyResultsError), so retrying them at the RPC layer would
// burn the whole retry budget on a query that can never succeed as issued.
func TestIsRetryableEthError_PermanentFailures(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		err  error
	}{
		{"too many results: pagination must halve instead", errors.New("query returned more than 10000 results")},
		{"block-span cap: pagination must halve instead", errors.New("range 10048 exceeds limit of 10000")},
		{"contract revert", errors.New("execution reverted")},
		{"context canceled", context.Canceled},
		{"context deadline exceeded", context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.False(t, isRetryableEthError(tc.err), "must not retry: %v", tc.err)
		})
	}
}

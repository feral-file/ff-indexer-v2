package executor_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/api/shared/executor"
	"github.com/feral-file/ff-indexer-v2/internal/store"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

func throttleFixture() executor.AddressIndexingThrottle {
	return executor.AddressIndexingThrottle{
		SuccessCooldown:    time.Hour,
		FailureBackoffBase: 30 * time.Minute,
		FailureBackoffCap:  24 * time.Hour,
	}
}

func terminalJob(status schema.IndexingJobStatus, at time.Time) *schema.AddressIndexingJob {
	job := &schema.AddressIndexingJob{Status: status, JobID: 42, UpdatedAt: at}
	switch status {
	case schema.IndexingJobStatusCompleted:
		job.CompletedAt = &at
	case schema.IndexingJobStatusFailed:
		job.FailedAt = &at
	case schema.IndexingJobStatusCanceled:
		job.CanceledAt = &at
	}
	return job
}

// TestAddressIndexingThrottle_RetryAt pins the throttle's two regimes and its
// deliberate gaps: fixed cooldown after success, exponential (capped) backoff
// after failures, and no restriction for empty history, canceled jobs, or
// disabled knobs.
func TestAddressIndexingThrottle_RetryAt(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)

	cases := []struct {
		name      string
		throttle  executor.AddressIndexingThrottle
		state     *store.AddressIndexingThrottleState
		wantAt    time.Time
		wantGated bool
	}{
		{
			name:      "nil state is unrestricted",
			throttle:  throttleFixture(),
			state:     nil,
			wantGated: false,
		},
		{
			name:      "no history is unrestricted",
			throttle:  throttleFixture(),
			state:     &store.AddressIndexingThrottleState{},
			wantGated: false,
		},
		{
			name:     "completed applies the fixed cooldown",
			throttle: throttleFixture(),
			state: &store.AddressIndexingThrottleState{
				LatestTerminal: terminalJob(schema.IndexingJobStatusCompleted, base),
			},
			wantAt:    base.Add(time.Hour),
			wantGated: true,
		},
		{
			name:     "first failure waits the base backoff",
			throttle: throttleFixture(),
			state: &store.AddressIndexingThrottleState{
				LatestTerminal:      terminalJob(schema.IndexingJobStatusFailed, base),
				ConsecutiveFailures: 1,
			},
			wantAt:    base.Add(30 * time.Minute),
			wantGated: true,
		},
		{
			name:     "fourth failure waits base times eight",
			throttle: throttleFixture(),
			state: &store.AddressIndexingThrottleState{
				LatestTerminal:      terminalJob(schema.IndexingJobStatusFailed, base),
				ConsecutiveFailures: 4,
			},
			wantAt:    base.Add(4 * time.Hour),
			wantGated: true,
		},
		{
			name:     "backoff is capped",
			throttle: throttleFixture(),
			state: &store.AddressIndexingThrottleState{
				LatestTerminal:      terminalJob(schema.IndexingJobStatusFailed, base),
				ConsecutiveFailures: 10, // 30m * 2^9 = 256h, above the 24h cap
			},
			wantAt:    base.Add(24 * time.Hour),
			wantGated: true,
		},
		{
			name:     "absurd streak does not overflow the shift",
			throttle: throttleFixture(),
			state: &store.AddressIndexingThrottleState{
				LatestTerminal:      terminalJob(schema.IndexingJobStatusFailed, base),
				ConsecutiveFailures: 100,
			},
			wantAt:    base.Add(24 * time.Hour),
			wantGated: true,
		},
		{
			name:     "canceled is unrestricted (operator intervened)",
			throttle: throttleFixture(),
			state: &store.AddressIndexingThrottleState{
				LatestTerminal: terminalJob(schema.IndexingJobStatusCanceled, base),
			},
			wantGated: false,
		},
		{
			name:     "cooldown disabled leaves success unrestricted",
			throttle: executor.AddressIndexingThrottle{FailureBackoffBase: 30 * time.Minute},
			state: &store.AddressIndexingThrottleState{
				LatestTerminal: terminalJob(schema.IndexingJobStatusCompleted, base),
			},
			wantGated: false,
		},
		{
			name:     "backoff disabled leaves failure unrestricted",
			throttle: executor.AddressIndexingThrottle{SuccessCooldown: time.Hour},
			state: &store.AddressIndexingThrottleState{
				LatestTerminal:      terminalJob(schema.IndexingJobStatusFailed, base),
				ConsecutiveFailures: 3,
			},
			wantGated: false,
		},
		{
			name:     "missing terminal timestamp falls back to UpdatedAt",
			throttle: throttleFixture(),
			state: &store.AddressIndexingThrottleState{
				LatestTerminal: &schema.AddressIndexingJob{
					Status:    schema.IndexingJobStatusCompleted,
					UpdatedAt: base,
					// CompletedAt deliberately nil
				},
			},
			wantAt:    base.Add(time.Hour),
			wantGated: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			at, gated := tc.throttle.RetryAt(tc.state)
			require.Equal(t, tc.wantGated, gated)
			if tc.wantGated {
				require.Equal(t, tc.wantAt, at)
			}
		})
	}
}

// TestAddressIndexingThrottle_Enabled pins the fast-path predicate the executor
// uses to skip the extra store query entirely when nothing is configured.
func TestAddressIndexingThrottle_Enabled(t *testing.T) {
	t.Parallel()
	require.False(t, executor.AddressIndexingThrottle{}.Enabled())
	require.False(t, executor.AddressIndexingThrottle{FailureBackoffCap: time.Hour}.Enabled(), "a cap alone throttles nothing")
	require.True(t, executor.AddressIndexingThrottle{SuccessCooldown: time.Hour}.Enabled())
	require.True(t, executor.AddressIndexingThrottle{FailureBackoffBase: time.Minute}.Enabled())
}

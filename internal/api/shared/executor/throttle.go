package executor

import (
	"time"

	"github.com/feral-file/ff-indexer-v2/internal/store"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

// AddressIndexingThrottle rate-limits how often one address can trigger a fresh
// wallet scan. A scan is one of the most expensive operations the API can start
// (an owner enumeration against a credit-metered RPC provider), and the trigger
// endpoint is public: without a throttle a client can re-run it the moment the
// previous job finishes — and on a failing address, each retry pays the full
// scan cost again. Zero values disable each part (no throttling).
//
// Two regimes, deliberately different:
//   - After success, a FIXED cooldown: what matters is data freshness, and a
//     fixed window gives the caller a predictable staleness bound.
//   - After failure, EXPONENTIAL backoff (base × 2^(streak-1), capped): repeated
//     failures are the credit-burn regime the incident came from, so the retry
//     frequency must halve with each attempt. The streak resets on success or
//     operator cancel.
type AddressIndexingThrottle struct {
	// SuccessCooldown is the minimum interval after a completed scan before the
	// same address may start another. Zero disables the cooldown.
	SuccessCooldown time.Duration
	// FailureBackoffBase is the wait after the first failure; each consecutive
	// failure doubles it. Zero disables failure backoff.
	FailureBackoffBase time.Duration
	// FailureBackoffCap bounds the doubled backoff. Zero means uncapped.
	FailureBackoffCap time.Duration
}

// Enabled reports whether any part of the throttle is configured.
func (t AddressIndexingThrottle) Enabled() bool {
	return t.SuccessCooldown > 0 || t.FailureBackoffBase > 0
}

// RetryAt returns the earliest time a new scan may start given the address's
// job history, and whether a throttle window applies at all. A false second
// return means "no restriction" (no history, a canceled latest job, or the
// relevant knob disabled) — callers must not compare against the zero time.
func (t AddressIndexingThrottle) RetryAt(state *store.AddressIndexingThrottleState) (time.Time, bool) {
	if state == nil || state.LatestTerminal == nil {
		return time.Time{}, false
	}

	job := state.LatestTerminal
	switch job.Status {
	case schema.IndexingJobStatusCompleted:
		if t.SuccessCooldown <= 0 {
			return time.Time{}, false
		}
		return terminalTime(job, job.CompletedAt).Add(t.SuccessCooldown), true

	case schema.IndexingJobStatusFailed:
		if t.FailureBackoffBase <= 0 {
			return time.Time{}, false
		}
		return terminalTime(job, job.FailedAt).Add(t.failureDelay(state.ConsecutiveFailures)), true

	default:
		// Canceled: an operator intervened; locking out their retry would make
		// the throttle fight the person operating it.
		return time.Time{}, false
	}
}

// failureDelay computes base × 2^(streak-1) with a shift-overflow guard and the
// configured cap.
func (t AddressIndexingThrottle) failureDelay(consecutiveFailures int) time.Duration {
	if consecutiveFailures < 1 {
		consecutiveFailures = 1
	}

	// 2^62 ns already exceeds a century; beyond 62 doublings the shift itself
	// overflows, so clamp to the cap (or a day, if uncapped) outright.
	const maxDoublings = 62
	if consecutiveFailures-1 > maxDoublings {
		return t.capDelay(time.Duration(1<<62 - 1))
	}

	delay := t.FailureBackoffBase << (consecutiveFailures - 1)
	if delay < t.FailureBackoffBase { // overflow wrapped negative
		delay = time.Duration(1<<62 - 1)
	}
	return t.capDelay(delay)
}

func (t AddressIndexingThrottle) capDelay(delay time.Duration) time.Duration {
	if t.FailureBackoffCap > 0 && delay > t.FailureBackoffCap {
		return t.FailureBackoffCap
	}
	return delay
}

// terminalTime picks the job's terminal timestamp, falling back to UpdatedAt
// (always set by the status writer) if the status-specific column is missing.
func terminalTime(job *schema.AddressIndexingJob, specific *time.Time) time.Time {
	if specific != nil {
		return *specific
	}
	return job.UpdatedAt
}

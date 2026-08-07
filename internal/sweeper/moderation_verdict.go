package sweeper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/alitto/pond/v2"
	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/objkt"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/opensea"
	"github.com/feral-file/ff-indexer-v2/internal/store"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

// ModerationVerdictSweeperConfig holds configuration for the moderation verdict sweeper
type ModerationVerdictSweeperConfig struct {
	BatchSize      int // Verdict rows to re-check per source per cycle
	WorkerPoolSize int // Concurrent workers
	// InitialRecheckInterval is the floor for a clean token's re-check delay
	// (also what the enricher schedules for a fresh verdict, see
	// store.DefaultModerationRecheckInterval). Successive clean checks double the
	// delay up to MaxRecheckInterval.
	InitialRecheckInterval time.Duration
	// MaxRecheckInterval caps the clean-token backoff and is the fixed re-check
	// delay for flagged tokens (appeals are rare, so flagged rows are polled
	// slowly — reversals still converge, just on this cadence).
	MaxRecheckInterval time.Duration
	// FailureBackoffInitial is the re-check delay after the first consecutive
	// failure; it doubles per additional failure up to MaxRecheckInterval.
	FailureBackoffInitial time.Duration
	// MaxConsecutiveFailures pins a row at MaxRecheckInterval once reached, so
	// permanently missing tokens (vendor dropped the item entirely) stop
	// burning API quota without ever leaving the queue for good.
	MaxConsecutiveFailures int
}

// moderationVerdictSweeper implements the Sweeper interface for re-checking vendor spam verdicts.
//
// Reason: the enricher only sees a vendor's moderation verdict at indexing time,
// which is exactly when moderation has not happened yet — takedowns land hours to
// days after a scam token is airdropped, and appealed takedowns get reversed.
// This sweeper re-asks the vendors on a per-row schedule (next_check_at) so both
// late flags and reversals converge.
//
// Constraints: rows only exist for tokens whose enrichment got at least one
// successful vendor signal; the sweeper widens no coverage, it only refreshes.
// Verdict writes go through the same store recompute as the enricher, so a
// feralfile pin always wins and events broadcast only on real flips.
type moderationVerdictSweeper struct {
	config        *ModerationVerdictSweeperConfig
	store         store.Store
	openseaClient opensea.Client
	objktClient   objkt.Client
	pool          pond.Pool
	clock         adapter.Clock
	running       atomic.Bool
	stopChan      chan struct{}
	stoppedCh     chan struct{}
}

// NewModerationVerdictSweeper creates a new moderation verdict sweeper
func NewModerationVerdictSweeper(
	config *ModerationVerdictSweeperConfig,
	st store.Store,
	openseaClient opensea.Client,
	objktClient objkt.Client,
	clock adapter.Clock,
) Sweeper {
	return &moderationVerdictSweeper{
		config:        config,
		store:         st,
		openseaClient: openseaClient,
		objktClient:   objktClient,
		clock:         clock,
		stopChan:      make(chan struct{}),
		stoppedCh:     make(chan struct{}),
	}
}

// Name returns the sweeper's name
func (s *moderationVerdictSweeper) Name() string {
	return "moderation-verdict-sweeper"
}

// Start begins the sweeper's main loop - continuously re-checks due verdicts
func (s *moderationVerdictSweeper) Start(ctx context.Context) error {
	if !s.running.CompareAndSwap(false, true) {
		return fmt.Errorf("sweeper already running")
	}
	defer func() {
		s.running.Store(false)
		close(s.stoppedCh) // Signal that we've stopped
	}()

	logger.InfoCtx(ctx, "Starting moderation verdict sweeper (continuous mode)",
		zap.Int("batch_size", s.config.BatchSize),
		zap.Int("worker_pool_size", s.config.WorkerPoolSize),
		zap.Duration("initial_recheck_interval", s.config.InitialRecheckInterval),
		zap.Duration("max_recheck_interval", s.config.MaxRecheckInterval),
	)

	// Continuous loop - stops when context is canceled or stop is requested
	for {
		select {
		case <-ctx.Done():
			logger.InfoCtx(ctx, "Spam verdict sweeper stopping due to context cancellation", zap.Error(ctx.Err()))
			s.cleanup()
			return nil
		case <-s.stopChan:
			logger.InfoCtx(ctx, "Spam verdict sweeper stop requested")
			s.cleanup()
			return nil
		default:
			if err := s.runSweepCycle(ctx); err != nil {
				if !errors.Is(err, context.Canceled) {
					logger.ErrorCtx(ctx, err)
				}
			}
		}
	}
}

// cleanup stops the worker pool and waits for tasks to complete
func (s *moderationVerdictSweeper) cleanup() {
	if s.pool != nil {
		s.pool.StopAndWait()
	}
}

// Stop gracefully stops the sweeper with timeout support
func (s *moderationVerdictSweeper) Stop(ctx context.Context) error {
	if !s.running.CompareAndSwap(true, false) {
		return nil // Already stopped
	}

	logger.InfoCtx(ctx, "Stopping moderation verdict sweeper")

	// Signal stop to the main loop
	close(s.stopChan)

	// Wait for main loop to exit, but respect context cancellation
	select {
	case <-s.stoppedCh:
		logger.InfoCtx(ctx, "Spam verdict sweeper stopped gracefully")
		return nil
	case <-ctx.Done():
		logger.WarnCtx(ctx, "Spam verdict sweeper stop interrupted by context timeout")
		return ctx.Err()
	}
}

// runSweepCycle re-checks one due batch per vendor source. Sources are separate
// queues (separate store queries) so OpenSea's API quota cannot starve objkt's
// and vice versa. Sleeps when every source is idle, and also before surfacing a
// store error (see below).
func (s *moderationVerdictSweeper) runSweepCycle(ctx context.Context) error {
	totalDue := 0
	for _, source := range []schema.ModerationSource{schema.ModerationSourceOpenSea, schema.ModerationSourceObjkt} {
		n, err := s.sweepSource(ctx, source)
		if err != nil {
			// Back off before returning: Start only logs the error and re-enters
			// this function immediately, and a failed due-query issues no HTTP
			// request, so neither the rate limiter nor the idle sleep below would
			// throttle the retry. Without this the loop spins at full speed —
			// saturating the database and Sentry — for the whole outage. Same
			// reasoning as the unconfigured-vendor path in checkItem.
			_ = s.sleep(ctx, SWEEP_CYCLE_INTERVAL)
			return err
		}
		totalDue += n
	}

	if totalDue == 0 {
		// Nothing due anywhere: wait before polling again. Context-aware sleep
		// so shutdown is not delayed.
		if !s.sleep(ctx, SWEEP_CYCLE_INTERVAL) {
			return ctx.Err()
		}
	}
	return nil
}

// sweepSource fetches and processes one source's due batch, returning how many
// rows were due — or 0 when the batch made no progress at all, so the caller
// treats the source as idle and sleeps.
//
// Reason for the no-progress signal: a row only leaves the due set when something
// moves its next_check_at. Every path that fails to do so (an unconfigured vendor,
// a failed verdict upsert, a failed failure-record) leaves the identical batch due,
// so reporting it as work would keep runSweepCycle from ever sleeping and respin
// the batch continuously — burning either CPU or paid vendor quota, depending on
// whether the failing path reached the rate-limited HTTP call.
func (s *moderationVerdictSweeper) sweepSource(ctx context.Context, source schema.ModerationSource) (int, error) {
	items, err := s.store.GetTokenModerationVerdictsDueForCheck(ctx, source, s.config.BatchSize)
	if err != nil {
		return 0, fmt.Errorf("failed to get due spam verdicts for %s: %w", source, err)
	}
	if len(items) == 0 {
		return 0, nil
	}

	logger.InfoCtx(ctx, "Re-checking spam verdicts",
		zap.String("source", source.String()),
		zap.Int("count", len(items)))

	// Fresh pool per batch, mirroring the media health sweeper's per-cycle pool.
	s.pool = pond.NewPool(
		s.config.WorkerPoolSize,
		pond.WithQueueSize(s.config.BatchSize),
		pond.WithContext(ctx),
	)
	var progressed atomic.Bool
	for _, item := range items {
		s.pool.Submit(func() {
			s.checkItem(ctx, source, item, &progressed)
		})
	}
	s.pool.StopAndWait()

	if !progressed.Load() {
		return 0, nil
	}

	return len(items), nil
}

// checkItem re-asks the vendor about one token and persists the outcome. It sets
// progressed when it manages to move the row's next_check_at, by either route —
// see the no-progress note on sweepSource for why that matters.
func (s *moderationVerdictSweeper) checkItem(ctx context.Context, source schema.ModerationSource, item store.TokenModerationCheckItem, progressed *atomic.Bool) {
	verdict, detail, err := s.fetchVendorVerdict(ctx, source, item)
	if err != nil {
		// ErrNoAPIKey means the whole source is unconfigured, not that this row
		// failed: writing failure state would walk every row's backoff to the
		// max for no reason. Leave the rows untouched — they stay due until a key
		// is configured — and make no progress, so the cycle sleeps instead of
		// respinning them. ErrNoAPIKey is returned before the request and the
		// rate limiter, so nothing else would throttle that respin.
		if errors.Is(err, opensea.ErrNoAPIKey) {
			logger.WarnCtx(ctx, "Skipping spam verdict re-check: vendor has no API key",
				zap.String("source", source.String()))
			return
		}
		s.recordFailure(ctx, source, item, err, progressed)
		return
	}

	next := s.clock.Now().Add(s.successInterval(item, verdict))
	// ExpectedLastCheckedAt guards against persisting a response that went stale
	// while it was in flight: the vendor call above is rate-limited and can take
	// seconds, and the enricher may write a fresher verdict for the same
	// (token, source) in that window. Without the guard this older response would
	// overwrite the newer one and its schedule, and the stale value would stand
	// until the next sweep.
	changed, err := s.store.UpsertTokenModerationVerdict(ctx, store.UpsertTokenModerationVerdictInput{
		TokenID:               item.TokenID,
		Source:                source,
		Verdict:               verdict,
		Detail:                detail,
		NextCheckAt:           &next,
		ExpectedLastCheckedAt: &item.LastCheckedAt,
	})
	if err != nil {
		logger.ErrorCtx(ctx, fmt.Errorf("failed to upsert re-checked moderation verdict: %w", err),
			zap.String("token_cid", item.TokenCID),
			zap.String("source", source.String()))
		return
	}
	progressed.Store(true)
	if changed {
		logger.InfoCtx(ctx, "Token moderation status changed by sweeper re-check",
			zap.String("token_cid", item.TokenCID),
			zap.String("source", source.String()),
			zap.String("moderation_status", verdict.String()))
	}
}

// fetchVendorVerdict queries the vendor moderation signal for one token and
// normalizes it into a verdict. Vendor clients expose their own API's shape
// (see objkt.Token.IsSpam), so the mapping onto schema.ModerationStatus happens
// here, matching what the enricher does on the indexing path.
func (s *moderationVerdictSweeper) fetchVendorVerdict(ctx context.Context, source schema.ModerationSource, item store.TokenModerationCheckItem) (schema.ModerationStatus, []byte, error) {
	switch source {
	case schema.ModerationSourceOpenSea:
		nft, err := s.openseaClient.GetNFT(ctx, item.ContractAddress, item.TokenNumber)
		if err != nil {
			// ErrNFTNotFound included: OpenSea dropping the item entirely is
			// "no opinion", not a verdict — the stored one stands (tri-state).
			return "", nil, err
		}
		detail, err := json.Marshal(map[string]any{"is_disabled": nft.IsDisabled})
		if err != nil {
			return "", nil, err
		}
		return moderationStatusFromVendorSpam(nft.IsDisabled), detail, nil
	case schema.ModerationSourceObjkt:
		token, err := s.objktClient.GetToken(ctx, item.ContractAddress, item.TokenNumber)
		if err != nil {
			// objkt has no not-found sentinel; every error takes the failure
			// path and backs off, settling at MaxRecheckInterval when permanent.
			return "", nil, err
		}
		detail, err := json.Marshal(map[string]any{"flag": token.Flag})
		if err != nil {
			return "", nil, err
		}
		return moderationStatusFromVendorSpam(token.IsSpam()), detail, nil
	default:
		return "", nil, fmt.Errorf("moderation source %s has no vendor client", source)
	}
}

// moderationStatusFromVendorSpam normalizes a vendor's own boolean moderation
// signal into the indexer's verdict enum.
func moderationStatusFromVendorSpam(spam bool) schema.ModerationStatus {
	if spam {
		return schema.ModerationStatusSpam
	}
	return schema.ModerationStatusNone
}

// recordFailure advances the row's failure backoff without touching its verdict,
// and reports that as progress: the row leaves the due set even though the check
// itself failed.
//
// The write is conditional on the row the sweeper read, mirroring the success
// path. A failed request is just as stale as a successful one, and landing it
// after a newer enrichment is worse: the backoff is derived here from the
// caller's consecutive_failures while the SQL increments the stored value, so a
// freshly re-checked token could be pushed out by the 720h maximum on a row that
// only records one failure. When the guard rejects, the winner already moved
// next_check_at, so the row is not due and there is nothing to retry.
func (s *moderationVerdictSweeper) recordFailure(ctx context.Context, source schema.ModerationSource, item store.TokenModerationCheckItem, checkErr error, progressed *atomic.Bool) {
	next := s.clock.Now().Add(s.failureInterval(item))
	applied, err := s.store.RecordTokenModerationCheckFailure(
		ctx, item.TokenID, source, checkErr.Error(), next, item.LastCheckedAt)
	if err != nil {
		logger.ErrorCtx(ctx, fmt.Errorf("failed to record moderation check failure: %w", err),
			zap.String("token_cid", item.TokenCID),
			zap.String("source", source.String()))
		return
	}
	if !applied {
		logger.InfoCtx(ctx, "Skipped stale moderation check failure",
			zap.String("token_cid", item.TokenCID),
			zap.String("source", source.String()))
		return
	}
	progressed.Store(true)
	logger.WarnCtx(ctx, "Spam verdict re-check failed",
		zap.String("token_cid", item.TokenCID),
		zap.String("source", source.String()),
		zap.Int("consecutive_failures", item.ConsecutiveFailures+1),
		zap.Error(checkErr))
}

// successInterval picks the next re-check delay after a successful check.
//
// Moderated tokens poll at the fixed maximum (appeals are rare). Clean tokens
// double their previous interval — derived as next_check_at − last_checked_at
// from the row itself, so no interval column is needed — clamped to
// [InitialRecheckInterval, MaxRecheckInterval].
//
// Constraints: the derivation is only valid when the row's next_check_at was
// last set by a successful check. RecordTokenModerationCheckFailure advances
// next_check_at while deliberately leaving last_checked_at frozen, so after any
// failure the difference measures the failure backoff instead of the previous
// success cadence — doubling that would push a clean token straight to the
// 30-day maximum because a vendor had a transient outage. Rows carrying
// failures therefore restart from the floor: checking too often costs quota,
// checking too rarely is a moderation gap.
func (s *moderationVerdictSweeper) successInterval(item store.TokenModerationCheckItem, verdict schema.ModerationStatus) time.Duration {
	if verdict != schema.ModerationStatusNone {
		return s.config.MaxRecheckInterval
	}
	if item.ConsecutiveFailures > 0 {
		return s.config.InitialRecheckInterval
	}
	prev := item.NextCheckAt.Sub(item.LastCheckedAt)
	next := 2 * prev
	if next < s.config.InitialRecheckInterval {
		next = s.config.InitialRecheckInterval
	}
	if next > s.config.MaxRecheckInterval {
		next = s.config.MaxRecheckInterval
	}
	return next
}

// maxFailureShift bounds the failure backoff shift so the doubling cannot overflow
// time.Duration. An int64 of nanoseconds tops out around 292 years; the smallest
// backoff worth configuring is a second, and 1s << 34 already exceeds that, so 34
// is past any real setting while still leaving the shift itself well-defined.
// Overflow would wrap to a negative or zero interval, scheduling the row in the
// past and respinning it every cycle — reachable only under a misconfigured
// max_consecutive_failures, but the failure mode is a hot loop, so it is clamped
// rather than trusted. The interval <= 0 guard below is the second line of defense
// for backoffs larger than a second.
const maxFailureShift = 34

// failureInterval picks the next re-check delay after a failed check: the
// initial failure backoff doubled per prior consecutive failure, pinned at
// MaxRecheckInterval once MaxConsecutiveFailures is reached (permanently
// missing tokens stop burning quota).
func (s *moderationVerdictSweeper) failureInterval(item store.TokenModerationCheckItem) time.Duration {
	failures := item.ConsecutiveFailures + 1 // counting the failure being recorded
	if failures >= s.config.MaxConsecutiveFailures {
		return s.config.MaxRecheckInterval
	}
	shift := failures - 1
	if shift > maxFailureShift {
		return s.config.MaxRecheckInterval
	}
	interval := s.config.FailureBackoffInitial << shift
	if interval <= 0 || interval > s.config.MaxRecheckInterval {
		return s.config.MaxRecheckInterval
	}
	return interval
}

// sleep waits for the duration, returning false if interrupted by context
// cancellation or a stop request.
func (s *moderationVerdictSweeper) sleep(ctx context.Context, duration time.Duration) bool {
	select {
	case <-s.clock.After(duration):
		return true // Sleep completed
	case <-ctx.Done():
		return false // Interrupted by context cancellation
	case <-s.stopChan:
		return false // Interrupted by stop signal
	}
}

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

// SpamVerdictSweeperConfig holds configuration for the spam verdict sweeper
type SpamVerdictSweeperConfig struct {
	BatchSize      int // Verdict rows to re-check per source per cycle
	WorkerPoolSize int // Concurrent workers
	// InitialRecheckInterval is the floor for a clean token's re-check delay
	// (also what the enricher schedules for a fresh verdict, see
	// store.DefaultSpamRecheckInterval). Successive clean checks double the
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

// spamVerdictSweeper implements the Sweeper interface for re-checking vendor spam verdicts.
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
type spamVerdictSweeper struct {
	config        *SpamVerdictSweeperConfig
	store         store.Store
	openseaClient opensea.Client
	objktClient   objkt.Client
	pool          pond.Pool
	clock         adapter.Clock
	running       atomic.Bool
	stopChan      chan struct{}
	stoppedCh     chan struct{}
}

// NewSpamVerdictSweeper creates a new spam verdict sweeper
func NewSpamVerdictSweeper(
	config *SpamVerdictSweeperConfig,
	st store.Store,
	openseaClient opensea.Client,
	objktClient objkt.Client,
	clock adapter.Clock,
) Sweeper {
	return &spamVerdictSweeper{
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
func (s *spamVerdictSweeper) Name() string {
	return "spam-verdict-sweeper"
}

// Start begins the sweeper's main loop - continuously re-checks due verdicts
func (s *spamVerdictSweeper) Start(ctx context.Context) error {
	if !s.running.CompareAndSwap(false, true) {
		return fmt.Errorf("sweeper already running")
	}
	defer func() {
		s.running.Store(false)
		close(s.stoppedCh) // Signal that we've stopped
	}()

	logger.InfoCtx(ctx, "Starting spam verdict sweeper (continuous mode)",
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
func (s *spamVerdictSweeper) cleanup() {
	if s.pool != nil {
		s.pool.StopAndWait()
	}
}

// Stop gracefully stops the sweeper with timeout support
func (s *spamVerdictSweeper) Stop(ctx context.Context) error {
	if !s.running.CompareAndSwap(true, false) {
		return nil // Already stopped
	}

	logger.InfoCtx(ctx, "Stopping spam verdict sweeper")

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
// and vice versa. Sleeps only when every source is idle.
func (s *spamVerdictSweeper) runSweepCycle(ctx context.Context) error {
	totalDue := 0
	for _, source := range []schema.SpamSource{schema.SpamSourceOpenSea, schema.SpamSourceObjkt} {
		n, err := s.sweepSource(ctx, source)
		if err != nil {
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
// rows were due.
func (s *spamVerdictSweeper) sweepSource(ctx context.Context, source schema.SpamSource) (int, error) {
	items, err := s.store.GetTokenSpamVerdictsDueForCheck(ctx, source, s.config.BatchSize)
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
	for _, item := range items {
		s.pool.Submit(func() {
			s.checkItem(ctx, source, item)
		})
	}
	s.pool.StopAndWait()

	return len(items), nil
}

// checkItem re-asks the vendor about one token and persists the outcome.
func (s *spamVerdictSweeper) checkItem(ctx context.Context, source schema.SpamSource, item store.TokenSpamCheckItem) {
	verdict, detail, err := s.fetchVendorVerdict(ctx, source, item)
	if err != nil {
		// ErrNoAPIKey means the whole source is unconfigured, not that this row
		// failed: writing failure state would walk every row's backoff to the
		// max for no reason. Leave rows untouched; they stay due until a key is
		// configured. The cycle sleep keeps this from tight-looping.
		if errors.Is(err, opensea.ErrNoAPIKey) {
			logger.WarnCtx(ctx, "Skipping spam verdict re-check: vendor has no API key",
				zap.String("source", source.String()))
			return
		}
		s.recordFailure(ctx, source, item, err)
		return
	}

	next := s.clock.Now().Add(s.successInterval(item, verdict))
	changed, err := s.store.UpsertTokenSpamVerdict(ctx, store.UpsertTokenSpamVerdictInput{
		TokenID:     item.TokenID,
		Source:      source,
		Verdict:     verdict,
		Detail:      detail,
		NextCheckAt: &next,
	})
	if err != nil {
		logger.ErrorCtx(ctx, fmt.Errorf("failed to upsert re-checked spam verdict: %w", err),
			zap.String("token_cid", item.TokenCID),
			zap.String("source", source.String()))
		return
	}
	if changed {
		logger.InfoCtx(ctx, "Token spam status changed by sweeper re-check",
			zap.String("token_cid", item.TokenCID),
			zap.String("source", source.String()),
			zap.Bool("is_spam", verdict))
	}
}

// fetchVendorVerdict queries the vendor moderation signal for one token.
func (s *spamVerdictSweeper) fetchVendorVerdict(ctx context.Context, source schema.SpamSource, item store.TokenSpamCheckItem) (bool, []byte, error) {
	switch source {
	case schema.SpamSourceOpenSea:
		nft, err := s.openseaClient.GetNFT(ctx, item.ContractAddress, item.TokenNumber)
		if err != nil {
			// ErrNFTNotFound included: OpenSea dropping the item entirely is
			// "no opinion", not a verdict — the stored one stands (tri-state).
			return false, nil, err
		}
		detail, err := json.Marshal(map[string]any{"is_disabled": nft.IsDisabled})
		if err != nil {
			return false, nil, err
		}
		return nft.IsDisabled, detail, nil
	case schema.SpamSourceObjkt:
		token, err := s.objktClient.GetToken(ctx, item.ContractAddress, item.TokenNumber)
		if err != nil {
			// objkt has no not-found sentinel; every error takes the failure
			// path and backs off, settling at MaxRecheckInterval when permanent.
			return false, nil, err
		}
		detail, err := json.Marshal(map[string]any{"flag": token.Flag})
		if err != nil {
			return false, nil, err
		}
		return token.IsBanned(), detail, nil
	default:
		return false, nil, fmt.Errorf("spam source %s has no vendor client", source)
	}
}

// recordFailure advances the row's failure backoff without touching its verdict.
func (s *spamVerdictSweeper) recordFailure(ctx context.Context, source schema.SpamSource, item store.TokenSpamCheckItem, checkErr error) {
	next := s.clock.Now().Add(s.failureInterval(item))
	if err := s.store.RecordTokenSpamCheckFailure(ctx, item.TokenID, source, checkErr.Error(), next); err != nil {
		logger.ErrorCtx(ctx, fmt.Errorf("failed to record spam check failure: %w", err),
			zap.String("token_cid", item.TokenCID),
			zap.String("source", source.String()))
		return
	}
	logger.WarnCtx(ctx, "Spam verdict re-check failed",
		zap.String("token_cid", item.TokenCID),
		zap.String("source", source.String()),
		zap.Int("consecutive_failures", item.ConsecutiveFailures+1),
		zap.Error(checkErr))
}

// successInterval picks the next re-check delay after a successful check.
//
// Flagged tokens poll at the fixed maximum (appeals are rare). Clean tokens
// double their previous interval — derived as next_check_at − last_checked_at
// from the row itself, so no interval column is needed — clamped to
// [InitialRecheckInterval, MaxRecheckInterval]. A degenerate derivation (first
// sweep after enrichment, clock skew) resets to the floor, which is the safe
// direction: checking a token too often is a quota cost, too rarely is a
// moderation gap.
func (s *spamVerdictSweeper) successInterval(item store.TokenSpamCheckItem, verdict bool) time.Duration {
	if verdict {
		return s.config.MaxRecheckInterval
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

// failureInterval picks the next re-check delay after a failed check: the
// initial failure backoff doubled per prior consecutive failure, pinned at
// MaxRecheckInterval once MaxConsecutiveFailures is reached (permanently
// missing tokens stop burning quota).
func (s *spamVerdictSweeper) failureInterval(item store.TokenSpamCheckItem) time.Duration {
	failures := item.ConsecutiveFailures + 1 // counting the failure being recorded
	if failures >= s.config.MaxConsecutiveFailures {
		return s.config.MaxRecheckInterval
	}
	interval := s.config.FailureBackoffInitial << (failures - 1)
	if interval > s.config.MaxRecheckInterval {
		return s.config.MaxRecheckInterval
	}
	return interval
}

// sleep waits for the duration, returning false if interrupted by context
// cancellation or a stop request.
func (s *spamVerdictSweeper) sleep(ctx context.Context, duration time.Duration) bool {
	select {
	case <-s.clock.After(duration):
		return true // Sleep completed
	case <-ctx.Done():
		return false // Interrupted by context cancellation
	case <-s.stopChan:
		return false // Interrupted by stop signal
	}
}

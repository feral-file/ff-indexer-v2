package sweeper

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alitto/pond/v2"
	"github.com/cenkalti/backoff/v4"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.uber.org/zap"

	"github.com/oklog/ulid/v2"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/providers/temporal"
	"github.com/feral-file/ff-indexer-v2/internal/store"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
	"github.com/feral-file/ff-indexer-v2/internal/types"
	"github.com/feral-file/ff-indexer-v2/internal/uri"
	"github.com/feral-file/ff-indexer-v2/internal/webhook"
	"github.com/feral-file/ff-indexer-v2/internal/workflows"
)

const (
	SWEEP_CYCLE_INTERVAL = 15 * time.Minute // Time to sleep between sweep cycles
)

// MediaHealthSweeperConfig holds configuration for the media health sweeper
type MediaHealthSweeperConfig struct {
	BatchSize      int           // URLs to check per batch
	WorkerPoolSize int           // Concurrent workers
	RecheckAfter   time.Duration // Only check URLs older than this
}

// mediaHealthSweeper implements the Sweeper interface for media health checking
type mediaHealthSweeper struct {
	config                *MediaHealthSweeperConfig
	store                 store.Store
	urlChecker            uri.URLChecker
	dataURIChecker        uri.DataURIChecker
	pool                  pond.Pool
	clock                 adapter.Clock
	orchestrator          temporal.TemporalOrchestrator
	orchestratorTaskQueue string
	running               atomic.Bool
	stopChan              chan struct{}
	stoppedCh             chan struct{}
}

// NewMediaHealthSweeper creates a new media health sweeper
func NewMediaHealthSweeper(
	config *MediaHealthSweeperConfig,
	st store.Store,
	checker uri.URLChecker,
	dataURIChecker uri.DataURIChecker,
	clock adapter.Clock,
	orchestrator temporal.TemporalOrchestrator,
	orchestratorTaskQueue string,
) Sweeper {
	return &mediaHealthSweeper{
		config:                config,
		store:                 st,
		urlChecker:            checker,
		dataURIChecker:        dataURIChecker,
		clock:                 clock,
		orchestrator:          orchestrator,
		orchestratorTaskQueue: orchestratorTaskQueue,
		stopChan:              make(chan struct{}),
		stoppedCh:             make(chan struct{}),
	}
}

// Name returns the sweeper's name
func (s *mediaHealthSweeper) Name() string {
	return "media-health-sweeper"
}

// Start begins the sweeper's main loop - continuously checks URLs without interval
func (s *mediaHealthSweeper) Start(ctx context.Context) error {
	if !s.running.CompareAndSwap(false, true) {
		return fmt.Errorf("sweeper already running")
	}
	defer func() {
		s.running.Store(false)
		close(s.stoppedCh) // Signal that we've stopped
	}()

	logger.InfoCtx(ctx, "Starting media health sweeper (continuous mode)",
		zap.Int("batch_size", s.config.BatchSize),
		zap.Int("worker_pool_size", s.config.WorkerPoolSize),
		zap.Duration("recheck_after", s.config.RecheckAfter),
	)

	// Create worker pool
	s.pool = pond.NewPool(
		s.config.WorkerPoolSize,
		pond.WithQueueSize(s.config.BatchSize),
		pond.WithContext(ctx),
	)

	// Continuous loop - stops when context is canceled or stop is requested
	for {
		select {
		case <-ctx.Done():
			logger.InfoCtx(ctx, "Media health sweeper stopping due to context cancellation", zap.Error(ctx.Err()))
			s.cleanup()
			return nil
		case <-s.stopChan:
			logger.InfoCtx(ctx, "Media health sweeper stop requested")
			s.cleanup()
			return nil
		default:
			// Run sweep cycle and immediately continue to next batch
			if err := s.runSweepCycle(ctx); err != nil {
				if !errors.Is(err, context.Canceled) {
					logger.ErrorCtx(ctx, err)
				}
			}
		}
	}
}

// cleanup stops the worker pool and waits for tasks to complete
func (s *mediaHealthSweeper) cleanup() {
	if s.pool != nil {
		s.pool.StopAndWait()
	}
}

// Stop gracefully stops the sweeper with timeout support
func (s *mediaHealthSweeper) Stop(ctx context.Context) error {
	if !s.running.CompareAndSwap(true, false) {
		return nil // Already stopped
	}

	logger.InfoCtx(ctx, "Stopping media health sweeper")

	// Signal stop to the main loop
	close(s.stopChan)

	// Wait for main loop to exit, but respect context cancellation
	select {
	case <-s.stoppedCh:
		logger.InfoCtx(ctx, "Media health sweeper stopped gracefully")
		return nil
	case <-ctx.Done():
		logger.WarnCtx(ctx, "Media health sweeper stop interrupted by context timeout")
		return ctx.Err()
	}
}

// runSweepCycle runs a single sweep cycle
func (s *mediaHealthSweeper) runSweepCycle(ctx context.Context) error {
	startTime := s.clock.Now()
	logger.InfoCtx(ctx, "Starting sweep cycle")

	// Get URLs that need checking (no locking, multiple workers may get the same URLs)
	urls, err := s.store.GetURLsForChecking(ctx, s.config.RecheckAfter, s.config.BatchSize)
	if err != nil {
		return fmt.Errorf("failed to get URLs for checking: %w", err)
	}

	if len(urls) == 0 {
		logger.InfoCtx(ctx, "No URLs need checking, waiting for new URLs...")
		// Sleep briefly to avoid tight loop when no URLs need checking
		// Use context-aware sleep so we can be interrupted
		if !s.sleep(ctx, SWEEP_CYCLE_INTERVAL) {
			return ctx.Err() // Context canceled during sleep
		}
		return nil
	}

	logger.InfoCtx(ctx, "Found URLs to check", zap.Int("count", len(urls)))

	// Track metrics
	var healthyCount, brokenCount, transientErrorCount atomic.Int32

	// Track token IDs that need viewability recomputation
	viewabilityUpdates := sync.Map{}

	// Submit all checks to worker pool
	for _, url := range urls {
		s.pool.Submit(func() {
			// Get tokens that use this URL
			tokenIDs, err := s.store.GetTokenIDsByMediaURL(ctx, url)
			if err != nil {
				logger.ErrorCtx(ctx, err, zap.String("url", url))
				return
			}

			// Collect token IDs that need viewability recomputation
			for _, tokenID := range tokenIDs {
				viewabilityUpdates.Store(tokenID, struct{}{})
			}

			// Check URL and update metrics
			s.checkURL(ctx, url, &healthyCount, &brokenCount, &transientErrorCount)
		})
	}

	// Wait for all checks to complete
	s.pool.StopAndWait()

	// Collect all token IDs that need viewability recomputation
	var tokenIDs []uint64
	viewabilityUpdates.Range(func(key, value interface{}) bool {
		tokenID := key.(uint64)
		tokenIDs = append(tokenIDs, tokenID)
		return true
	})

	// Batch update all viewability changes collected during this cycle with retry
	if err := s.flushViewabilityUpdatesWithRetry(ctx, tokenIDs); err != nil {
		// After all retries failed, log with high severity for monitoring/alerting
		logger.ErrorCtx(ctx, fmt.Errorf("CRITICAL: failed to flush viewability updates after retries: %w", err),
			zap.Int("token_count", len(tokenIDs)),
			zap.Uint64s("token_ids", tokenIDs),
		)
	}

	// Recreate pool for next cycle
	s.pool = pond.NewPool(
		s.config.WorkerPoolSize,
		pond.WithQueueSize(s.config.BatchSize),
		pond.WithContext(ctx),
	)

	duration := s.clock.Since(startTime)
	logger.InfoCtx(ctx, "Sweep cycle completed",
		zap.Duration("duration", duration),
		zap.Int("total_checked", len(urls)),
		zap.Int32("healthy", healthyCount.Load()),
		zap.Int32("broken", brokenCount.Load()),
		zap.Int32("transient_errors", transientErrorCount.Load()),
	)

	// Sleep for a while to avoid tight loop
	// Use context-aware sleep so we can be interrupted
	if !s.sleep(ctx, SWEEP_CYCLE_INTERVAL) {
		return ctx.Err() // Context canceled during sleep
	}

	return nil
}

// sleep sleeps for the given duration but can be interrupted by context cancellation
// Returns true if sleep completed normally, false if interrupted by context
func (s *mediaHealthSweeper) sleep(ctx context.Context, duration time.Duration) bool {
	select {
	case <-s.clock.After(duration):
		return true // Sleep completed
	case <-ctx.Done():
		return false // Interrupted by context cancellation
	case <-s.stopChan:
		return false // Interrupted by stop signal
	}
}

// checkURL checks a single URL and updates the database
func (s *mediaHealthSweeper) checkURL(ctx context.Context, url string, healthyCount, brokenCount, transientErrorCount *atomic.Int32) {
	logger.InfoCtx(ctx, "Checking URL", zap.String("url", url))

	// Perform health check based on the URL type
	if types.IsDataURI(url) {
		//Perform data URI health check
		result := s.dataURIChecker.Check(url)
		switch result.Valid {
		case true:
			logger.InfoCtx(ctx, "Data URI is valid", zap.String("url", url))
			healthyCount.Add(1)
			if err := s.store.UpdateTokenMediaHealthByURL(ctx, url, schema.MediaHealthStatusHealthy, nil); err != nil {
				logger.ErrorCtx(ctx, err, zap.String("url", url))
			}
		case false:
			logger.WarnCtx(ctx, "Data URI is invalid",
				zap.String("url", url),
				zap.Stringp("error", result.Error),
			)
			brokenCount.Add(1)
			if err := s.store.UpdateTokenMediaHealthByURL(ctx, url, schema.MediaHealthStatusBroken, result.Error); err != nil {
				logger.ErrorCtx(ctx, err, zap.String("url", url))
			}
		}
	} else {
		// Perform URL health check
		result := s.urlChecker.Check(ctx, url)
		switch result.Status {
		case uri.HealthStatusHealthy:
			healthyCount.Add(1)
			if result.WorkingURL != nil && *result.WorkingURL != url {
				// Found alternative working URL - update everything
				logger.InfoCtx(ctx, "Found working alternative URL",
					zap.String("original_url", url),
					zap.String("working_url", *result.WorkingURL),
				)

				if err := s.store.UpdateMediaURLAndPropagate(ctx, url, *result.WorkingURL); err != nil {
					logger.ErrorCtx(ctx, err,
						zap.String("url", url),
					)
				} else {
					logger.InfoCtx(ctx, "Successfully updated URL across all tables",
						zap.String("old_url", url),
						zap.String("new_url", *result.WorkingURL),
					)
				}
			} else {
				// Original URL works
				if err := s.store.UpdateTokenMediaHealthByURL(ctx, url, schema.MediaHealthStatusHealthy, nil); err != nil {
					logger.ErrorCtx(ctx, err,
						zap.String("url", url),
					)
				}
			}

		case uri.HealthStatusBroken:
			brokenCount.Add(1)
			logger.WarnCtx(ctx, "URL is broken",
				zap.String("url", url),
				zap.Stringp("error", result.Error),
			)

			if err := s.store.UpdateTokenMediaHealthByURL(ctx, url, schema.MediaHealthStatusBroken, result.Error); err != nil {
				logger.ErrorCtx(ctx, err,
					zap.String("url", url),
				)
			}

		case uri.HealthStatusTransientError:
			transientErrorCount.Add(1)
			logger.WarnCtx(ctx, "Transient error checking URL, will retry later",
				zap.String("url", url),
				zap.Stringp("error", result.Error),
			)
			// Reset status back to previous state so it can be retried
			// We don't know the previous status, so we just set it to unknown to allow retry
			if err := s.store.UpdateTokenMediaHealthByURL(ctx, url, schema.MediaHealthStatusUnknown, result.Error); err != nil {
				logger.ErrorCtx(ctx, err,
					zap.String("url", url),
				)
			}
			return
		}
	}
}

// flushViewabilityUpdatesWithRetry attempts to flush viewability updates with exponential backoff retry
func (s *mediaHealthSweeper) flushViewabilityUpdatesWithRetry(ctx context.Context, tokenIDs []uint64) error {
	if len(tokenIDs) == 0 {
		return nil
	}

	// Configure exponential backoff
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = 15 * time.Second
	b.MaxInterval = 2 * time.Minute
	b.MaxElapsedTime = 1 * time.Hour // Total retry time limit
	b.Multiplier = 2.0
	b.RandomizationFactor = 0.5 // Add jitter to prevent thundering herd

	// Wrap with context to respect cancellation
	backoffWithContext := backoff.WithContext(b, ctx)

	// Retry operation
	operation := func() error {
		return s.flushViewabilityUpdates(ctx, tokenIDs)
	}

	// Execute with retry and detailed logging
	var attemptCount int
	notifyOnError := func(err error, duration time.Duration) {
		attemptCount++
		logger.WarnCtx(ctx, "Viewability flush failed, retrying",
			zap.Error(err),
			zap.Int("attempt", attemptCount),
			zap.Duration("next_retry_in", duration),
		)
	}

	err := backoff.RetryNotify(operation, backoffWithContext, notifyOnError)
	if err != nil {
		return fmt.Errorf("failed after %d attempts: %w", attemptCount, err)
	}

	if attemptCount > 0 {
		logger.InfoCtx(ctx, "Viewability flush succeeded after retries",
			zap.Int("total_attempts", attemptCount+1),
		)
	}

	return nil
}

// flushViewabilityUpdates flushes all collected viewability updates in a single batch
func (s *mediaHealthSweeper) flushViewabilityUpdates(ctx context.Context, tokenIDs []uint64) error {
	if len(tokenIDs) == 0 {
		return nil
	}

	logger.InfoCtx(ctx, "Flushing viewability updates",
		zap.Int("num_tokens", len(tokenIDs)),
	)

	// Batch update all tokens in ONE database query
	// This computes viewability from latest token_media_health state and returns only changed tokens
	changes, err := s.store.BatchUpdateTokensViewability(ctx, tokenIDs)
	if err != nil {
		return fmt.Errorf("failed to batch update tokens viewability: %w", err)
	}

	logger.InfoCtx(ctx, "Viewability updates flushed successfully",
		zap.Int("num_checked", len(tokenIDs)),
		zap.Int("num_changed", len(changes)),
	)

	// Trigger webhooks and change journal only for tokens that actually changed
	for _, change := range changes {
		logger.InfoCtx(ctx, "Token viewability changed",
			zap.String("token_cid", change.TokenCID),
			zap.Uint64("token_id", change.TokenID),
			zap.Bool("was_viewable", change.OldViewable),
			zap.Bool("is_viewable", change.NewViewable),
		)

		// Trigger webhook
		s.triggerWebhook(ctx, change.TokenCID, change.NewViewable)
	}

	return nil
}

// triggerWebhook triggers a webhook notification for a token media health change
func (s *mediaHealthSweeper) triggerWebhook(ctx context.Context, tokenCID string, isViewable bool) {
	// Parse token CID to get components
	parsedTokenCID := domain.TokenCID(tokenCID)
	chain, standard, contract, tokenNumber := parsedTokenCID.Parse()

	// Create webhook event with ULID for unique, time-sortable event ID
	eventID := ulid.MustNewDefault(s.clock.Now()).String()

	webhookEvent := webhook.WebhookEvent{
		EventID:   eventID,
		EventType: webhook.EventTypeTokenViewabilityChanged,
		Timestamp: s.clock.Now(),
		Data: webhook.TokenViewabilityChanged{
			EventData: webhook.EventData{
				TokenCID:    tokenCID,
				Chain:       string(chain),
				Standard:    string(standard),
				Contract:    contract,
				TokenNumber: tokenNumber,
			},
			IsViewable: isViewable,
		},
	}

	// Start webhook notification workflow (fire-and-forget)
	workflowOptions := client.StartWorkflowOptions{
		ID:                    fmt.Sprintf("webhook-notify-%s-%s", webhookEvent.EventType, webhookEvent.EventID),
		TaskQueue:             s.orchestratorTaskQueue,
		WorkflowRunTimeout:    30 * time.Minute,
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	}

	w := workflows.NewWorkerCore(nil, workflows.WorkerCoreConfig{}, nil, nil)
	workflowRun, err := s.orchestrator.ExecuteWorkflow(ctx, workflowOptions, w.NotifyWebhookClients, webhookEvent)
	if err != nil {
		logger.ErrorCtx(ctx, err,
			zap.String("token_cid", tokenCID),
			zap.String("event_type", webhookEvent.EventType),
			zap.String("event_id", webhookEvent.EventID),
		)
		return
	}

	// Log workflow start (handle nil workflowRun from tests)
	if workflowRun != nil {
		logger.InfoCtx(ctx, "Webhook notification workflow started",
			zap.String("token_cid", tokenCID),
			zap.String("event_type", webhookEvent.EventType),
			zap.String("workflow_id", workflowRun.GetID()),
			zap.String("run_id", workflowRun.GetRunID()),
		)
	}
}

package sweeper

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/alitto/pond/v2"
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
	"github.com/feral-file/ff-indexer-v2/internal/uri"
	"github.com/feral-file/ff-indexer-v2/internal/webhook"
	"github.com/feral-file/ff-indexer-v2/internal/workflows"
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
	checker               uri.URLChecker
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
	clock adapter.Clock,
	orchestrator temporal.TemporalOrchestrator,
	orchestratorTaskQueue string,
) Sweeper {
	return &mediaHealthSweeper{
		config:                config,
		store:                 st,
		checker:               checker,
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
	defer close(s.stoppedCh)

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

	// Continuous loop - no interval between batches
	for {
		select {
		case <-ctx.Done():
			logger.InfoCtx(ctx, "Media health sweeper stopping", zap.Error(ctx.Err()))
			return s.Stop(ctx)
		case <-s.stopChan:
			logger.InfoCtx(ctx, "Media health sweeper stop requested")
			return nil
		default:
			// Run sweep cycle and immediately continue to next batch
			if err := s.runSweepCycle(ctx); err != nil {
				logger.ErrorCtx(ctx, err)
			}
		}
	}
}

// Stop gracefully stops the sweeper
func (s *mediaHealthSweeper) Stop(ctx context.Context) error {
	if !s.running.CompareAndSwap(true, false) {
		return nil // Already stopped
	}

	logger.InfoCtx(ctx, "Stopping media health sweeper")

	// Signal stop
	close(s.stopChan)

	// Stop accepting new tasks and wait for existing tasks to complete
	if s.pool != nil {
		s.pool.StopAndWait()
	}

	// Wait for main loop to exit
	<-s.stoppedCh

	logger.InfoCtx(ctx, "Media health sweeper stopped")
	return nil
}

// runSweepCycle runs a single sweep cycle
func (s *mediaHealthSweeper) runSweepCycle(ctx context.Context) error {
	startTime := s.clock.Now()
	logger.InfoCtx(ctx, "Starting sweep cycle")

	// Get URLs that need checking
	urls, err := s.store.GetMediaURLsNeedingCheck(ctx, s.config.RecheckAfter, s.config.BatchSize)
	if err != nil {
		return fmt.Errorf("failed to get URLs needing check: %w", err)
	}

	if len(urls) == 0 {
		logger.InfoCtx(ctx, "No URLs need checking, waiting for new URLs...")
		// Sleep briefly to avoid tight loop when no URLs need checking
		s.clock.Sleep(10 * time.Second)
		return nil
	}

	logger.InfoCtx(ctx, "Found URLs to check", zap.Int("count", len(urls)))

	// Track metrics
	var healthyCount, brokenCount, transientErrorCount atomic.Int32

	// Submit all checks to worker pool
	for _, url := range urls {
		s.pool.Submit(func() {
			s.checkURL(ctx, url, &healthyCount, &brokenCount, &transientErrorCount)
		})
	}

	// Wait for all checks to complete
	s.pool.StopAndWait()

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

	return nil
}

// checkURL checks a single URL and updates the database
func (s *mediaHealthSweeper) checkURL(ctx context.Context, url string, healthyCount, brokenCount, transientErrorCount *atomic.Int32) {
	logger.InfoCtx(ctx, "Checking URL", zap.String("url", url))

	// Get all tokens using this URL and their current viewability status BEFORE health check
	tokensBefore, err := s.store.GetTokensViewabilityByMediaURL(ctx, url)
	if err != nil {
		logger.ErrorCtx(ctx, err, zap.String("url", url))
		return
	}

	// Perform URL health check
	result := s.checker.Check(ctx, url)

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
		// Don't update status for transient errors - let it retry next cycle
		return
	}

	// Get all tokens using this URL and their viewability status AFTER health check update
	tokensAfter, err := s.store.GetTokensViewabilityByMediaURL(ctx, url)
	if err != nil {
		logger.ErrorCtx(ctx, err, zap.String("url", url))
		return
	}

	// Compare viewability and trigger webhooks for changed tokens
	s.triggerWebhooksForChangedTokens(ctx, tokensBefore, tokensAfter)
}

// triggerWebhooksForChangedTokens compares before/after viewability and triggers webhooks for tokens that changed
func (s *mediaHealthSweeper) triggerWebhooksForChangedTokens(ctx context.Context, before, after []store.TokenViewabilityInfo) {
	// Create maps for quick lookup
	beforeMap := make(map[uint64]bool)
	for _, token := range before {
		beforeMap[token.TokenID] = token.IsViewable
	}

	afterMap := make(map[uint64]store.TokenViewabilityInfo)
	for _, token := range after {
		afterMap[token.TokenID] = token
	}

	// Check each token for viewability changes
	for tokenID, wasViewable := range beforeMap {
		afterToken, exists := afterMap[tokenID]
		if !exists {
			continue // Token no longer has this URL (edge case)
		}

		isViewable := afterToken.IsViewable

		// Trigger webhook only if viewability changed
		if wasViewable != isViewable {
			healthStatus := schema.MediaHealthStatusBroken
			if isViewable {
				healthStatus = schema.MediaHealthStatusHealthy
			}

			logger.InfoCtx(ctx, "Token viewability changed, triggering webhook",
				zap.String("token_cid", afterToken.TokenCID),
				zap.Bool("was_viewable", wasViewable),
				zap.Bool("is_viewable", isViewable),
				zap.String("health_status", string(healthStatus)),
			)

			s.triggerWebhook(ctx, afterToken.TokenCID, healthStatus)
		} else {
			logger.DebugCtx(ctx, "Token viewability did not change, skipping webhook",
				zap.String("token_cid", afterToken.TokenCID),
				zap.Bool("was_viewable", wasViewable),
				zap.Bool("is_viewable", isViewable),
			)
		}
	}
}

// triggerWebhook triggers a webhook notification for a token media health change
func (s *mediaHealthSweeper) triggerWebhook(ctx context.Context, tokenCID string, healthStatus schema.MediaHealthStatus) {
	// Parse token CID to get components
	parsedTokenCID := domain.TokenCID(tokenCID)
	chain, standard, contract, tokenNumber := parsedTokenCID.Parse()

	// Create webhook event with ULID for unique, time-sortable event ID
	eventID := ulid.MustNewDefault(s.clock.Now()).String()

	webhookEvent := webhook.WebhookEvent{
		EventID:   eventID,
		EventType: webhook.EventTypeTokenMediaHealthChanged,
		Timestamp: s.clock.Now(),
		Data: webhook.TokenMediaHealthChanged{
			EventData: webhook.EventData{
				TokenCID:    tokenCID,
				Chain:       string(chain),
				Standard:    string(standard),
				Contract:    contract,
				TokenNumber: tokenNumber,
			},
			HealthStatus: healthStatus,
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

	logger.InfoCtx(ctx, "Webhook notification workflow started",
		zap.String("token_cid", tokenCID),
		zap.String("event_type", webhookEvent.EventType),
		zap.String("workflow_id", workflowRun.GetID()),
		zap.String("run_id", workflowRun.GetRunID()),
	)
}

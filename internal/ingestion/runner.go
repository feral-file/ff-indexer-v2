package ingestion

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/blockchain"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/providers/jobs"
	"github.com/feral-file/ff-indexer-v2/internal/registry"
	"github.com/feral-file/ff-indexer-v2/internal/store"
	"github.com/feral-file/ff-indexer-v2/internal/types"
)

const (
	defaultQueueCapacity = 2048
)

// Config defines how one chain ingestion runner subscribes, flushes, and
// persists durable progress.
type Config struct {
	ChainID           domain.Chain
	StartBlock        uint64
	TokenQueue        string
	QueueCapacity     int
	BlockFlushTimeout time.Duration // Max time to wait before flushing incomplete block
}

// Runner owns one chain ingestion pipeline from event source to workflow start.
//
//go:generate mockgen -source=runner.go -destination=../mocks/ingestion_runner.go -package=mocks -mock_names=Runner=MockIngestionRunner
type Runner interface {
	Run(ctx context.Context) error
	Close() error
}

// blockBuffer holds all events for one block number until the block is
// considered complete (next block arrives). The cursor is advanced once per
// flushed block, not once per event.
type blockBuffer struct {
	blockNumber uint64
	events      []*domain.BlockchainEvent
	firstSeen   time.Time
}

type runner struct {
	source    blockchain.EventSource
	store     store.Store
	jobQueue  jobs.JobQueue
	blacklist registry.BlacklistRegistry
	config    Config
	clock     adapter.Clock

	cancel    context.CancelFunc
	queue     chan *domain.BlockchainEvent
	closeOnce sync.Once
	doneCh    chan struct{}

	// flushErr is the fatal error from the flush loop (cursor or job enqueue
	// failure). It is written exactly once before flushFailed is closed, so any
	// reader that observes the close also observes the value (channel close
	// provides the happens-before edge — no mutex needed).
	flushErr    error
	flushFailed chan struct{}
}

// NewRunner creates one chain ingestion runner with its own in-memory flush
// queue and durable cursor stream.
//
// Reason: The ingestion boundary is the ordered flush from normalized chain
// event to workflow start plus cursor persistence.
// Trade-offs: The queue is process-local and intentionally not durable; replay
// comes from the persisted block cursor, which advances at block boundaries
// when the next block arrives or after BlockFlushTimeout elapses. Partial
// blocks are not flushed on shutdown to avoid race conditions where
// late-arriving events from the same block would be lost after cursor
// advancement.
//
// Constraints: A cursor write retry must not restart a workflow that already
// started successfully for the same queued item.
//
// Monotonic Cursor Guarantee: The runner enforces strictly forward cursor
// progression (block numbers must increase, never decrease). Blocks older than
// the current cursor are rejected and logged as warnings, preventing cursor
// regression from very old late-arriving events (e.g., after Tezos subscriber
// seal pruning). Same-height blocks (blockNumber == cursor) are allowed to
// accommodate legitimate late arrivals after timeout flushes. This maintains
// both liveness and data integrity.
//
// StartBlock Interaction: Config.StartBlock is an override that determines the
// subscription starting point regardless of any persisted cursor. If StartBlock
// is set to a value older than the persisted cursor, the monotonic guard will
// prevent cursor regression by dropping those old blocks with warnings. For
// intentional rewinds/backfills, operators must manually reset the persisted
// cursor to a value <= StartBlock before starting the runner.
//
// BlockFlushTimeout: Guard against sparse event streams. Events arriving after
// the timeout fires may be permanently lost if a crash occurs before processing.
// Operators must configure timeout >> expected maximum event arrival lag to
// minimize this risk.
func NewRunner(
	parentCtx context.Context,
	source blockchain.EventSource,
	st store.Store,
	jq jobs.JobQueue,
	blacklist registry.BlacklistRegistry,
	cfg Config,
	clock adapter.Clock,
) Runner {
	if cfg.QueueCapacity <= 0 {
		cfg.QueueCapacity = defaultQueueCapacity
	}

	ctx, cancel := context.WithCancel(parentCtx)

	r := &runner{
		source:      source,
		store:       st,
		jobQueue:    jq,
		blacklist:   blacklist,
		config:      cfg,
		clock:       clock,
		cancel:      cancel,
		queue:       make(chan *domain.BlockchainEvent, cfg.QueueCapacity),
		doneCh:      make(chan struct{}),
		flushFailed: make(chan struct{}),
	}

	go r.runFlushLoop(ctx)

	return r
}

// Run starts the chain source subscription and feeds normalized events into the
// ordered flush queue.
//
// Reason: The source adapter remains responsible for transport and parsing,
// while the ingestion runner owns ordering, retry, workflow start, and durable
// progress.
// Trade-offs: A saturated queue applies backpressure to the source instead of
// allowing unbounded memory growth.
// Constraints: The source must surface handler failures so replay remains
// explicit.
func (r *runner) Run(ctx context.Context) error {
	startBlock, err := r.resolveStartBlock(ctx)
	if err != nil {
		return err
	}

	logger.InfoCtx(ctx, "Starting chain ingestion",
		zap.String("chain", string(r.config.ChainID)),
		zap.Uint64("start_block", startBlock))

	// runCtx lets us cancel the subscribe stream if the flush loop signals a
	// fatal error, so the runner exits promptly instead of stalling on a hung
	// source or a saturated queue.
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	handler := func(event *domain.BlockchainEvent) error {
		return r.enqueue(runCtx, event)
	}

	subErrCh := make(chan error, 1)
	go func() {
		subErrCh <- r.source.SubscribeEvents(runCtx, startBlock, handler)
	}()

	select {
	case <-r.flushFailed:
		// The close on flushFailed pairs with the write to r.flushErr inside the
		// flush loop, so reading r.flushErr here is race-free.
		cancel()
		<-subErrCh
		logger.ErrorCtx(ctx, errors.New("chain ingestion failed"),
			zap.String("chain", string(r.config.ChainID)), zap.Error(r.flushErr))
		return r.flushErr
	case err = <-subErrCh:
	}

	if err != nil {
		if errors.Is(err, context.Canceled) {
			logger.WarnCtx(ctx, "Chain ingestion stopped", zap.String("chain", string(r.config.ChainID)), zap.Error(err))
		} else {
			logger.ErrorCtx(ctx, errors.New("chain ingestion failed"), zap.String("chain", string(r.config.ChainID)), zap.Error(err))
		}
	}

	return err
}

func (r *runner) enqueue(ctx context.Context, event *domain.BlockchainEvent) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.doneCh:
		return errors.New("ingestion runner is closed")
	case r.queue <- event:
		return nil
	}
}

func (r *runner) runFlushLoop(ctx context.Context) {
	defer close(r.doneCh)

	var current *blockBuffer
	var flushTimerC <-chan time.Time
	var err error

	// Track last flushed cursor for monotonic guard
	// Initialize lazily on first flush to avoid constructor-time DB read
	chainID := string(r.config.ChainID)
	var lastCursor uint64
	cursorInitialized := false

	for {
		select {
		case <-ctx.Done():
			// Don't flush partial block - cursor stays at last complete block.
			// On restart, incomplete block will replay (job unique keys prevent dupes).
			// This avoids the race where more events from the same block arrive
			// after we've already flushed and advanced the cursor.
			return

		case <-flushTimerC:
			// Timeout fired - flush current block even though next block hasn't arrived.
			// This handles sparse event streams but risks losing late-arriving events
			// from the same block if a crash occurs before they are processed.
			if current != nil {
				// Lazy-initialize cursor on first flush
				if !cursorInitialized {
					lastCursor, err = r.store.GetBlockCursor(ctx, chainID)
					if err != nil {
						logger.ErrorCtx(ctx, errors.New("failed to get initial cursor"),
							zap.String("chain", chainID), zap.Error(err))
						r.flushErr = fmt.Errorf("failed to get initial cursor: %w", err)
						close(r.flushFailed)
						return
					}
					cursorInitialized = true
				}

				newCursor, err := r.flushBlock(ctx, current, lastCursor)
				if err != nil {
					logger.ErrorCtx(ctx, errors.New("blockchain block flush failed"),
						zap.String("chain", string(r.config.ChainID)),
						zap.Uint64("block", current.blockNumber),
						zap.Error(err))
					r.flushErr = err
					close(r.flushFailed)
					return
				}
				lastCursor = newCursor
				current = nil
				flushTimerC = nil
			}

		case event := <-r.queue:
			// New block? Flush previous block first
			if current != nil && event.BlockNumber != current.blockNumber {
				// Lazy-initialize cursor on first flush
				if !cursorInitialized {
					lastCursor, err = r.store.GetBlockCursor(ctx, chainID)
					if err != nil {
						logger.ErrorCtx(ctx, errors.New("failed to get initial cursor"),
							zap.String("chain", chainID), zap.Error(err))
						r.flushErr = fmt.Errorf("failed to get initial cursor: %w", err)
						close(r.flushFailed)
						return
					}
					cursorInitialized = true
				}

				newCursor, err := r.flushBlock(ctx, current, lastCursor)
				if err != nil {
					// Cursor didn't advance, so the block will replay on restart.
					// Surface the error to Run via flushFailed and exit the loop.
					// All errors from flushBlock are fatal (filter reads, cursor
					// writes, job enqueues) - we rely on service-level retry for
					// transient failures.
					logger.ErrorCtx(ctx, errors.New("blockchain block flush failed"),
						zap.String("chain", string(r.config.ChainID)),
						zap.Uint64("block", current.blockNumber),
						zap.Error(err))
					r.flushErr = err
					close(r.flushFailed)
					return
				}
				lastCursor = newCursor
				current = nil
				flushTimerC = nil
			}

			// Start or append to current block
			if current == nil {
				current = &blockBuffer{
					blockNumber: event.BlockNumber,
					events:      []*domain.BlockchainEvent{event},
					firstSeen:   r.clock.Now(),
				}
				// Start flush timer for new block
				if r.config.BlockFlushTimeout > 0 {
					flushTimerC = r.clock.After(r.config.BlockFlushTimeout)
				}
			} else {
				current.events = append(current.events, event)
				// Reset timer - creates new timer, old one expires naturally.
				// Trade-off: temporary timer accumulation (bounded by BlockFlushTimeout
				// duration) for simpler lifecycle management without goroutine leaks.
				if r.config.BlockFlushTimeout > 0 {
					flushTimerC = r.clock.After(r.config.BlockFlushTimeout)
				}
			}
		}
	}
}

// flushBlock resolves and enqueues jobs for every event in the block, then
// advances the durable cursor once. Any error (filter reads, job enqueue, or
// cursor persistence) is fatal and returns immediately so Run can shut down
// and the block can replay unchanged on restart (job unique keys keep replays
// safe).
//
// Monotonic cursor guard: Rejects cursor regression to prevent data loss from
// very old late-arriving events (e.g., after Tezos subscriber seal pruning).
// Blocks with blockNumber < currentCursor are logged and skipped. Same-height
// blocks (blockNumber == currentCursor) are processed to accommodate legitimate
// late arrivals after timeout flushes. The runner continues processing to
// maintain liveness in both cases.
//
// Returns the new cursor value after successful flush (or unchanged cursor if skipped).
func (r *runner) flushBlock(ctx context.Context, block *blockBuffer, currentCursor uint64) (uint64, error) {
	chainID := string(r.config.ChainID)

	// Enforce monotonic cursor progression: reject backward movement only
	// Allow same-height (==) blocks to accommodate late arrivals after timeout flush
	if block.blockNumber < currentCursor {
		// Old late arrival - log and skip to prevent cursor regression
		logger.WarnCtx(ctx, "Dropping block older than cursor (backward late arrival)",
			zap.String("chain", chainID),
			zap.Uint64("block", block.blockNumber),
			zap.Uint64("current_cursor", currentCursor),
			zap.Int("events", len(block.events)),
			zap.String("reason", "monotonic cursor guard"))

		// Don't return error - this is expected for old late arrivals
		// Job deduplication would prevent duplicate work anyway
		// Return unchanged cursor
		return currentCursor, nil
	}

	// Process events for this block
	for _, event := range block.events {
		if err := r.resolveEvent(ctx, event); err != nil {
			return currentCursor, err
		}
	}

	// Advance cursor (guaranteed to move forward due to check above)
	if err := r.store.SetBlockCursor(ctx, chainID, block.blockNumber); err != nil {
		return currentCursor, fmt.Errorf("failed to persist block cursor: %w", err)
	}

	logger.InfoCtx(ctx, "Flushed blockchain block",
		zap.String("chain", chainID),
		zap.Uint64("block", block.blockNumber),
		zap.Int("events", len(block.events)))

	return block.blockNumber, nil
}

func (r *runner) resolveEvent(ctx context.Context, event *domain.BlockchainEvent) error {
	shouldProcess, err := r.shouldProcessEvent(ctx, event)
	if err != nil {
		return err
	}

	if !shouldProcess {
		logger.InfoCtx(ctx, "Dropping blockchain event",
			zap.String("chain", string(event.Chain)),
			zap.String("event_type", string(event.EventType)),
			zap.String("token_cid", event.TokenCID().String()),
			zap.String("from", types.SafeString(event.FromAddress)),
			zap.String("to", types.SafeString(event.ToAddress)))
		return nil
	}

	return r.enqueueChainJob(ctx, event)
}

// shouldProcessEvent decides whether an event reaches the job queue. It checks
// blacklist status, token existence, and address watch-list membership. Any
// database error is fatal and will halt the block flush, triggering ingestion
// restart. This fail-fast strategy relies on service-level retry and backoff
// to handle transient failures.
func (r *runner) shouldProcessEvent(ctx context.Context, event *domain.BlockchainEvent) (bool, error) {
	if r.blacklist != nil && r.blacklist.IsTokenCIDBlacklisted(event.TokenCID()) {
		logger.InfoCtx(ctx, "Dropping blacklisted token event",
			zap.String("chain", string(event.Chain)),
			zap.String("token_cid", event.TokenCID().String()))
		return false, nil
	}

	token, err := r.store.GetTokenByTokenCID(ctx, event.TokenCID().String())
	if err != nil {
		return false, fmt.Errorf("token lookup failed: %w", err)
	}
	if token != nil {
		return true, nil
	}

	var addresses []string
	if !types.StringNilOrEmpty(event.FromAddress) {
		addresses = append(addresses, *event.FromAddress)
	}
	if !types.StringNilOrEmpty(event.ToAddress) {
		addresses = append(addresses, *event.ToAddress)
	}
	if len(addresses) == 0 {
		return false, nil
	}

	watched, err := r.store.IsAnyAddressWatched(ctx, event.Chain, addresses)
	if err != nil {
		return false, fmt.Errorf("watch-list check failed: %w", err)
	}
	return watched, nil
}

func (r *runner) enqueueChainJob(ctx context.Context, event *domain.BlockchainEvent) error {
	var kind string
	switch event.EventType {
	case domain.EventTypeMint:
		kind = "IndexTokenMint"
	case domain.EventTypeTransfer:
		kind = "IndexTokenTransfer"
	case domain.EventTypeBurn:
		kind = "IndexTokenBurn"
	case domain.EventTypeMetadataUpdate:
		kind = "IndexMetadataUpdate"
	case domain.EventTypeMetadataUpdateRange:
		logger.WarnCtx(ctx, "Ignoring unsupported metadata range event",
			zap.String("chain", string(event.Chain)),
			zap.String("token_cid", event.TokenCID().String()),
			zap.String("tx_hash", event.TxHash))
		return nil
	default:
		logger.WarnCtx(ctx, "Ignoring unsupported blockchain event type",
			zap.String("chain", string(event.Chain)),
			zap.String("event_type", string(event.EventType)),
			zap.String("token_cid", event.TokenCID().String()))
		return nil
	}

	uk := jobs.ProcessEventUniqueKey(event.Chain, event.TxHash, event.LogIndex)
	// TODO(ingestion-workflow-start): Distinguish "job already present (dedup)"
	// from "enqueue did not start" so replay after a partial client-side
	// failure can advance the cursor instead of retrying the same item forever.
	if _, _, err := r.jobQueue.Enqueue(ctx, jobs.EnqueueOptions{
		Queue:     r.config.TokenQueue,
		Kind:      kind,
		Args:      []any{event},
		UniqueKey: &uk,
	}); err != nil {
		return fmt.Errorf("failed to enqueue chain job: %w", err)
	}

	logger.InfoCtx(ctx, "Enqueued job for blockchain event",
		zap.String("chain", string(event.Chain)),
		zap.String("event_type", string(event.EventType)),
		zap.String("token_cid", event.TokenCID().String()),
		zap.String("tx_hash", event.TxHash))

	return nil
}

func (r *runner) resolveStartBlock(ctx context.Context) (uint64, error) {
	if r.config.StartBlock != 0 {
		return r.config.StartBlock, nil
	}

	lastBlock, err := r.store.GetBlockCursor(ctx, string(r.config.ChainID))
	if err != nil {
		return 0, fmt.Errorf("failed to get block cursor: %w", err)
	}
	if lastBlock > 0 {
		// Block-boundary cursor: a stored value N means ingestion successfully
		// flushed block N when block N+1 first arrived. Subscriptions therefore
		// resume from the next height/number (N + 1).
		return lastBlock + 1, nil
	}

	latestBlock, err := r.source.GetLatestBlock(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get latest block number: %w", err)
	}

	return latestBlock, nil
}

// Close stops the flush loop and closes the underlying event source.
func (r *runner) Close() error {
	r.closeOnce.Do(func() {
		r.cancel()
		r.source.Close()
	})
	<-r.doneCh
	return nil
}

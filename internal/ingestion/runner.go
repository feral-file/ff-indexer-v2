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
	defaultRetryDelay    = time.Minute
)

// Config defines how one chain ingestion runner subscribes, flushes, and
// persists durable progress.
type Config struct {
	ChainID       domain.Chain
	StartBlock    uint64
	TokenQueue    string
	QueueCapacity int
	RetryDelay    time.Duration
}

// Runner owns one chain ingestion pipeline from event source to workflow start.
//
//go:generate mockgen -source=runner.go -destination=../mocks/ingestion_runner.go -package=mocks -mock_names=Runner=MockIngestionRunner
type Runner interface {
	Run(ctx context.Context) error
	Close() error
}

type flushOutcome string

const (
	flushOutcomeStarted            flushOutcome = "started"
	flushOutcomeDropped            flushOutcome = "dropped"
	flushOutcomeIgnoredUnsupported flushOutcome = "ignored_unsupported"
)

type queueItem struct {
	event    *domain.BlockchainEvent
	resolved bool
	outcome  flushOutcome
}

type runner struct {
	source    blockchain.EventSource
	store     store.Store
	jobQueue  jobs.JobQueue
	blacklist registry.BlacklistRegistry
	config    Config
	clock     adapter.Clock

	cancel    context.CancelFunc
	queue     chan *queueItem
	closeOnce sync.Once
	doneCh    chan struct{}
}

// NewRunner creates one chain ingestion runner with its own in-memory flush
// queue and durable cursor stream.
//
// Reason: The ingestion boundary is the ordered flush from normalized chain
// event to workflow start plus cursor persistence.
// Trade-offs: The queue is process-local and intentionally not durable. Replay
// comes from the persisted chain cursor.
// Constraints: A cursor write retry must not restart a workflow that already
// started successfully for the same queued item.
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
	if cfg.RetryDelay <= 0 {
		cfg.RetryDelay = defaultRetryDelay
	}

	ctx, cancel := context.WithCancel(parentCtx)

	r := &runner{
		source:    source,
		store:     st,
		jobQueue:  jq,
		blacklist: blacklist,
		config:    cfg,
		clock:     clock,
		cancel:    cancel,
		queue:     make(chan *queueItem, cfg.QueueCapacity),
		doneCh:    make(chan struct{}),
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

	handler := func(event *domain.BlockchainEvent) error {
		return r.enqueue(ctx, event)
	}

	err = r.source.SubscribeEvents(ctx, startBlock, handler)
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
	item := &queueItem{event: event}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.doneCh:
		return errors.New("ingestion runner is closed")
	case r.queue <- item:
		return nil
	}
}

func (r *runner) runFlushLoop(ctx context.Context) {
	defer close(r.doneCh)

	var current *queueItem
	var pending []*queueItem

	for {
		if current == nil {
			next, ok := r.nextItem(ctx, pending)
			if !ok {
				return
			}
			current = next.item
			pending = next.pending
		}

		if err := r.flushItem(ctx, current); err != nil {
			logger.WarnCtx(ctx, "Failed to flush blockchain event",
				zap.String("chain", string(current.event.Chain)),
				zap.String("event_type", string(current.event.EventType)),
				zap.String("tx_hash", current.event.TxHash),
				zap.Uint64("block", current.event.BlockNumber),
				zap.Duration("retry_delay", r.config.RetryDelay),
				zap.Error(err))

			select {
			case <-ctx.Done():
				return
			case <-r.clock.After(r.config.RetryDelay):
			}
			continue
		}

		current = nil
	}
}

type queueSnapshot struct {
	item    *queueItem
	pending []*queueItem
}

func (r *runner) nextItem(ctx context.Context, pending []*queueItem) (queueSnapshot, bool) {
	if len(pending) > 0 {
		return queueSnapshot{
			item:    pending[0],
			pending: pending[1:],
		}, true
	}

	select {
	case <-ctx.Done():
		return queueSnapshot{}, false
	case item := <-r.queue:
		pending = append(pending, r.drainQueue()...)
		return queueSnapshot{
			item:    item,
			pending: pending,
		}, true
	}
}

func (r *runner) drainQueue() []*queueItem {
	items := make([]*queueItem, 0)
	for {
		select {
		case item := <-r.queue:
			items = append(items, item)
		default:
			return items
		}
	}
}

func (r *runner) flushItem(ctx context.Context, item *queueItem) error {
	if !item.resolved {
		outcome, err := r.resolveEvent(ctx, item.event)
		if err != nil {
			return err
		}
		item.outcome = outcome
		item.resolved = true
	}

	// TODO(ingestion-cursor): Persist a cursor that distinguishes multiple
	// events within the same block so a restart cannot skip later events after
	// the first event in that block flushes successfully.
	if err := r.store.SetBlockCursor(ctx, string(item.event.Chain), item.event.BlockNumber); err != nil {
		return fmt.Errorf("failed to persist block cursor: %w", err)
	}

	logger.InfoCtx(ctx, "Flushed blockchain event",
		zap.String("chain", string(item.event.Chain)),
		zap.String("event_type", string(item.event.EventType)),
		zap.String("tx_hash", item.event.TxHash),
		zap.Uint64("block", item.event.BlockNumber),
		zap.String("outcome", string(item.outcome)))

	return nil
}

func (r *runner) resolveEvent(ctx context.Context, event *domain.BlockchainEvent) (flushOutcome, error) {
	shouldProcess, err := r.shouldProcessEvent(ctx, event)
	if err != nil {
		return "", err
	}

	if !shouldProcess {
		logger.InfoCtx(ctx, "Dropping blockchain event",
			zap.String("chain", string(event.Chain)),
			zap.String("event_type", string(event.EventType)),
			zap.String("token_cid", event.TokenCID().String()),
			zap.String("from", types.SafeString(event.FromAddress)),
			zap.String("to", types.SafeString(event.ToAddress)))
		return flushOutcomeDropped, nil
	}

	return r.enqueueChainJob(ctx, event)
}

func (r *runner) shouldProcessEvent(ctx context.Context, event *domain.BlockchainEvent) (bool, error) {
	if r.blacklist != nil && r.blacklist.IsTokenCIDBlacklisted(event.TokenCID()) {
		logger.InfoCtx(ctx, "Dropping blacklisted token event",
			zap.String("chain", string(event.Chain)),
			zap.String("token_cid", event.TokenCID().String()))
		return false, nil
	}

	token, err := r.store.GetTokenByTokenCID(ctx, event.TokenCID().String())
	if err != nil {
		return false, fmt.Errorf("failed to check token existence: %w", err)
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

	return r.store.IsAnyAddressWatched(ctx, event.Chain, addresses)
}

func (r *runner) enqueueChainJob(ctx context.Context, event *domain.BlockchainEvent) (flushOutcome, error) {
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
		return flushOutcomeIgnoredUnsupported, nil
	default:
		logger.WarnCtx(ctx, "Ignoring unsupported blockchain event type",
			zap.String("chain", string(event.Chain)),
			zap.String("event_type", string(event.EventType)),
			zap.String("token_cid", event.TokenCID().String()))
		return flushOutcomeIgnoredUnsupported, nil
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
		return "", fmt.Errorf("failed to enqueue chain job: %w", err)
	}

	logger.InfoCtx(ctx, "Enqueued job for blockchain event",
		zap.String("chain", string(event.Chain)),
		zap.String("event_type", string(event.EventType)),
		zap.String("token_cid", event.TokenCID().String()),
		zap.String("tx_hash", event.TxHash))

	return flushOutcomeStarted, nil
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

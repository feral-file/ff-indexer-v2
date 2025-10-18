package emitter

import (
	"context"
	"fmt"
	"time"

	logger "github.com/bitmark-inc/autonomy-logger"
	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/messaging"
	"github.com/feral-file/ff-indexer-v2/internal/store"
)

// Config holds the configuration for the event emitter
type Config struct {
	ChainID         domain.Chain
	StartBlock      uint64
	CursorSaveFreq  uint64        // Save cursor every N blocks
	CursorSaveDelay time.Duration // Or save cursor every N seconds
}

// Emitter defines the interface for the event emitter
//
//go:generate mockgen -source=emitter.go -destination=../mocks/emitter.go -package=mocks -mock_names=Emitter=MockEmitter
type Emitter interface {
	// Run starts the event emitter
	Run(ctx context.Context) error
	// Close closes the emitter and cleans up resources
	Close()
}

// Emitter handles blockchain event subscription and publishing to NATS
type emitter struct {
	subscriber messaging.Subscriber
	publisher  messaging.Publisher
	store      store.Store
	config     Config
	clock      adapter.Clock
}

// NewEmitter creates a new event emitter
func NewEmitter(
	sub messaging.Subscriber,
	pub messaging.Publisher,
	st store.Store,
	cfg Config,
	clock adapter.Clock,
) Emitter {
	return &emitter{
		subscriber: sub,
		publisher:  pub,
		store:      st,
		config:     cfg,
		clock:      clock,
	}
}

// Run starts the event emitter
func (e *emitter) Run(ctx context.Context) error {
	// Determine starting block
	startBlock := e.config.StartBlock
	if startBlock == 0 {
		// Get last processed block from store
		lastBlock, err := e.store.GetBlockCursor(ctx, string(e.config.ChainID))
		if err != nil {
			return fmt.Errorf("failed to get block cursor: %w", err)
		}

		if lastBlock > 0 {
			startBlock = lastBlock + 1
			logger.Info("Resuming from last processed block", zap.String("chain", string(e.config.ChainID)), zap.Uint64("block", startBlock))
		} else {
			// Start from latest block
			latestBlock, err := e.subscriber.GetLatestBlock(ctx)
			if err != nil {
				return fmt.Errorf("failed to get latest block number: %w", err)
			}
			startBlock = latestBlock
			logger.Info("Starting from latest block", zap.String("chain", string(e.config.ChainID)), zap.Uint64("block", startBlock))
		}
	} else {
		logger.Info("Starting from configured block", zap.String("chain", string(e.config.ChainID)), zap.Uint64("block", startBlock))
	}

	// Channel for events
	errCh := make(chan error, 1)

	// Start subscribing to events
	go func() {
		logger.Info("Starting event subscription", zap.String("chain", string(e.config.ChainID)))

		lastSavedBlock := uint64(0)
		lastSaveTime := e.clock.Now()

		handler := func(event *domain.BlockchainEvent) error {
			// Publish to NATS
			if err := e.publisher.PublishEvent(ctx, event); err != nil {
				return fmt.Errorf("failed to publish event %s: %w", event.TxHash, err)
			}

			// Save cursor periodically (every N blocks or N seconds)
			shouldSave := event.BlockNumber-lastSavedBlock >= e.config.CursorSaveFreq ||
				e.clock.Since(lastSaveTime) >= e.config.CursorSaveDelay

			if shouldSave {
				if err := e.store.SetBlockCursor(ctx, string(e.config.ChainID), event.BlockNumber); err != nil {
					fmt.Printf("[Emitter] Failed to save block cursor: %v\n", err)
				} else {
					lastSavedBlock = event.BlockNumber
					lastSaveTime = e.clock.Now()
				}
			}

			return nil
		}

		err := e.subscriber.SubscribeEvents(ctx, startBlock, handler)
		if err != nil {
			errCh <- err
		}
	}()

	// Wait for error or context cancellation
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close closes the emitter and cleans up resources
func (e *emitter) Close() {
	e.subscriber.Close()
}

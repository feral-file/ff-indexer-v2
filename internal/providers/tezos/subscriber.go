package tezos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/alitto/pond/v2"
	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/messaging"
)

const (
	SUBSCRIBE_TIMEOUT         = 15 * time.Second
	DEFAULT_WORKER_POOL_SIZE  = 20
	DEFAULT_WORKER_QUEUE_SIZE = 2048
)

// Config holds the configuration for Tezos/TzKT subscription
type Config struct {
	WebSocketURL    string       // WebSocket URL (e.g., https://api.tzkt.io/v1/ws)
	ChainID         domain.Chain // e.g., "tezos:mainnet"
	WorkerPoolSize  int          // Number of concurrent workers
	WorkerQueueSize int          // Size of the task queue
}

// MessageType - TzKT message type
type MessageType int

const (
	MessageTypeState MessageType = iota
	MessageTypeData
	MessageTypeReorg
	MessageTypeSubscribed
)

// TzKTMessage wraps TzKT SignalR messages
type TzKTMessage struct {
	Type  MessageType     `json:"type"`
	State uint64          `json:"state"`
	Data  json.RawMessage `json:"data,omitempty"`
}

type tzSubscriber struct {
	ctx        context.Context
	wsURL      string
	chainID    domain.Chain
	client     adapter.SignalRClient
	connected  bool
	handler    messaging.EventHandler
	signalR    adapter.SignalR
	clock      adapter.Clock
	tzktClient TzKTClient
	config     Config
	pool       pond.Pool
}

// NewSubscriber creates a new Tezos/TzKT event subscriber
func NewSubscriber(cfg Config, signalR adapter.SignalR, clock adapter.Clock, tzktClient TzKTClient) (messaging.Subscriber, error) {
	return &tzSubscriber{
		wsURL:      cfg.WebSocketURL,
		chainID:    cfg.ChainID,
		signalR:    signalR,
		clock:      clock,
		tzktClient: tzktClient,
		config:     cfg,
	}, nil
}

// SubscribeEvents subscribes to FA2 transfer events and metadata updates via TzKT SignalR
// Note: fromLevel parameter is ignored for Tezos as TzKT always subscribes from current
func (c *tzSubscriber) SubscribeEvents(ctx context.Context, fromLevel uint64, handler messaging.EventHandler) error {
	if c.connected {
		logger.WarnCtx(ctx, "Already connected to TzKT SignalR")
		return nil
	}

	// Store context for cancellation
	c.ctx = ctx

	// Get worker pool configuration
	workerPoolSize := c.config.WorkerPoolSize
	if workerPoolSize == 0 {
		workerPoolSize = DEFAULT_WORKER_POOL_SIZE
	}
	workerQueueSize := c.config.WorkerQueueSize
	if workerQueueSize == 0 {
		workerQueueSize = DEFAULT_WORKER_QUEUE_SIZE
	}

	// Create worker pool for concurrent event processing
	c.pool = pond.NewPool(
		workerPoolSize,
		pond.WithQueueSize(workerQueueSize),
		pond.WithContext(ctx),
	)

	logger.InfoCtx(ctx, "Tezos worker pool created",
		zap.Int("workers", workerPoolSize),
		zap.Int("queue_size", workerQueueSize),
		zap.String("chain", string(c.chainID)))

	// Ensure graceful shutdown of worker pool
	defer func() {
		if c.pool != nil {
			logger.InfoCtx(ctx, "Shutting down tezos worker pool",
				zap.Uint64("submitted", c.pool.SubmittedTasks()),
				zap.Uint64("waiting", c.pool.WaitingTasks()),
				zap.Uint64("successful", c.pool.SuccessfulTasks()),
				zap.Uint64("failed", c.pool.FailedTasks()))

			c.pool.StopAndWait()

			logger.InfoCtx(ctx, "Tezos worker pool shutdown complete",
				zap.Uint64("total_submitted", c.pool.SubmittedTasks()),
				zap.Uint64("total_completed", c.pool.CompletedTasks()),
				zap.Uint64("total_failed", c.pool.FailedTasks()))
		}
	}()

	// Start periodic metrics logging
	metricsTicker := c.clock.NewTicker(30 * time.Second)
	defer metricsTicker.Stop()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-metricsTicker.C:
				if c.pool != nil {
					logger.InfoCtx(ctx, "Tezos worker pool metrics",
						zap.Int64("running_workers", c.pool.RunningWorkers()),
						zap.Uint64("waiting_tasks", c.pool.WaitingTasks()),
						zap.Uint64("completed_tasks", c.pool.CompletedTasks()),
						zap.Uint64("failed_tasks", c.pool.FailedTasks()))
				}
			}
		}
	}()

	// Store handler for callback
	c.handler = handler

	// Create SignalR client with connection
	client, err := c.signalR.NewClient(ctx, c.wsURL, c)
	if err != nil {
		return fmt.Errorf("failed to create SignalR client: %w", err)
	}

	c.client = client

	// Connect to SignalR hub
	client.Start()

	// Wait for connection
	c.clock.Sleep(time.Second)

	// Subscribe to token transfers
	sendErrChan := client.Send("SubscribeToTokenTransfers", map[string]interface{}{})
	select {
	case err := <-sendErrChan:
		if err != nil {
			return fmt.Errorf("failed to subscribe to token transfers: %w", err)
		}
	case <-c.clock.After(SUBSCRIBE_TIMEOUT):
		// Timeout waiting for send
		return fmt.Errorf("timeout waiting for token transfers subscription: %w", domain.ErrSubscriptionFailed)
	}

	// Subscribe to big map updates for metadata changes
	// Filter for token_metadata path to get only metadata-related updates
	sendErrChan = client.Send("SubscribeToBigMaps", map[string]interface{}{
		"path": "token_metadata",
	})
	select {
	case err := <-sendErrChan:
		if err != nil {
			return fmt.Errorf("failed to subscribe to big maps: %w", err)
		}
	case <-c.clock.After(SUBSCRIBE_TIMEOUT):
		// Timeout waiting for send
		return fmt.Errorf("timeout waiting for big maps subscription: %w", domain.ErrSubscriptionFailed)
	}

	c.connected = true

	// Keep connection alive and handle events
	<-ctx.Done()

	logger.InfoCtx(ctx, "TzKT WebSocket connection closed due to context done")
	return ctx.Err()
}

// Transfers handles incoming transfer events from TzKT SignalR
// Method name must match the SignalR target name "transfers"
func (c *tzSubscriber) Transfers(data interface{}) {
	// Marshal data to JSON
	jsonData, err := json.Marshal(data)
	if err != nil {
		logger.ErrorCtx(c.ctx, errors.New("error marshaling transfers data"), zap.Error(err))
		return
	}

	// Unmarshal data to TzKTMessage
	var msg TzKTMessage
	if err := json.Unmarshal(jsonData, &msg); err != nil {
		logger.ErrorCtx(c.ctx, errors.New("error unmarshaling transfers message"), zap.Error(err))
		return
	}

	// Type 0 is state/confirmation message, ignore it
	if msg.Type == MessageTypeState {
		logger.DebugCtx(c.ctx, "Received transfers state message", zap.Any("state", msg.State))
		return
	}

	// Only handle data messages
	if msg.Type != MessageTypeData {
		// FIXME: Handle other message types
		logger.WarnCtx(c.ctx, "Transfers message type is not data", zap.Any("type", msg.Type))
		return
	}

	if msg.Data == nil {
		logger.WarnCtx(c.ctx, "Transfers message without data field")
		return
	}

	// Unmarshal data array into transfer events
	var transfers []TzKTTokenTransfer
	if err := json.Unmarshal(msg.Data, &transfers); err != nil {
		logger.ErrorCtx(c.ctx, errors.New("error unmarshaling transfers data"), zap.Error(err))
		return
	}

	for i := range transfers {
		transfer := transfers[i]
		if transfer.Token.Standard != domain.StandardFA2 {
			logger.DebugCtx(c.ctx, "Skipping token is not fa2",
				zap.String("standard", string(transfer.Token.Standard)),
				zap.String("contract", transfer.Token.Contract.Address),
				zap.String("tokenId", transfer.Token.TokenID))
			continue
		}

		// Call handler if set
		if c.handler != nil && c.pool != nil {
			// Submit event processing to worker pool instead of spawning unbounded goroutines
			c.pool.SubmitErr(func() error {
				// Parse the transfer to event
				event, err := c.tzktClient.ParseTransfer(c.ctx, &transfer)
				if err != nil {
					logger.ErrorCtx(c.ctx, errors.New("error parsing transfer"), zap.Error(err))
					return nil
				}

				if event == nil {
					return nil
				}

				if err := c.handler(event); err != nil {
					logger.ErrorCtx(c.ctx, errors.New("error handling transfer event"), zap.Error(err),
						zap.Error(err),
						zap.String("tx_hash", event.TxHash),
						zap.Uint64("block", event.BlockNumber))
					return err
				}
				return nil
			})
		}
	}
}

// Bigmaps handles incoming big map update events from TzKT SignalR
// Method name must match the SignalR target name "bigmaps"
func (c *tzSubscriber) Bigmaps(data interface{}) {
	// Marshal data to JSON
	jsonData, err := json.Marshal(data)
	if err != nil {
		logger.ErrorCtx(c.ctx, errors.New("error marshaling bigmaps data"), zap.Error(err))
		return
	}

	// Unmarshal data to TzKTMessage
	var msg TzKTMessage
	if err := json.Unmarshal(jsonData, &msg); err != nil {
		logger.ErrorCtx(c.ctx, errors.New("error unmarshaling bigmaps message"), zap.Error(err))
		return
	}

	// Type 0 is state/confirmation message, ignore it
	if msg.Type == MessageTypeState {
		logger.DebugCtx(c.ctx, "Received bigmaps state message", zap.Any("state", msg.State))
		return
	}

	// Only handle data messages
	if msg.Type != MessageTypeData {
		// FIXME: Handle other message types
		logger.WarnCtx(c.ctx, "Bigmaps message type is not data", zap.Any("type", msg.Type))
		return
	}

	if msg.Data == nil {
		logger.WarnCtx(c.ctx, "Bigmaps message without data field")
		return
	}

	// Unmarshal data array into bigmap updates
	var updates []TzKTBigMapUpdate
	if err := json.Unmarshal(msg.Data, &updates); err != nil {
		logger.ErrorCtx(c.ctx, errors.New("error unmarshaling bigmaps data"), zap.Error(err))
		return
	}

	for i := range updates {
		update := updates[i]

		// Only handle token_metadata updates
		if update.Path != "token_metadata" {
			continue
		}

		// Call handler if set
		if c.handler != nil && c.pool != nil {
			// Submit event processing to worker pool for consistency with Transfers
			c.pool.SubmitErr(func() error {
				// Parse the big map update to event
				event, err := c.tzktClient.ParseBigMapUpdate(c.ctx, &update)
				if err != nil {
					logger.ErrorCtx(c.ctx, errors.New("error parsing big map update"), zap.Error(err))
					return nil
				}

				if event == nil {
					return nil
				}

				if err := c.handler(event); err != nil {
					logger.ErrorCtx(c.ctx, errors.New("error handling metadata update event"), zap.Error(err),
						zap.Error(err),
						zap.String("tx_hash", event.TxHash),
						zap.Uint64("block", event.BlockNumber))
					return err
				}
				return nil
			})
		}
	}
}

// GetLatestBlock returns the latest block level from TzKT API
func (c *tzSubscriber) GetLatestBlock(ctx context.Context) (uint64, error) {
	// This would require HTTP client to call TzKT API
	// Simplified implementation - would need actual HTTP request
	// For now, return 0 to indicate we should start from current
	return 0, nil
}

// Close closes the SignalR connection
func (c *tzSubscriber) Close() {
	if c.client == nil || !c.connected {
		return
	}

	c.client.Stop()
	c.connected = false
	logger.InfoCtx(c.ctx, "TzKT WebSocket connection closed")
}

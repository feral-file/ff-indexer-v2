package tezos

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	logger "github.com/bitmark-inc/autonomy-logger"
	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/messaging"
)

const SUBSCRIBE_TIMEOUT = 15 * time.Second

// Config holds the configuration for Tezos/TzKT subscription
type Config struct {
	WebSocketURL string       // WebSocket URL (e.g., https://api.tzkt.io/v1/ws)
	ChainID      domain.Chain // e.g., "tezos:mainnet"
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
}

// NewSubscriber creates a new Tezos/TzKT event subscriber
func NewSubscriber(cfg Config, signalR adapter.SignalR, clock adapter.Clock, tzktClient TzKTClient) (messaging.Subscriber, error) {
	return &tzSubscriber{
		wsURL:      cfg.WebSocketURL,
		chainID:    cfg.ChainID,
		signalR:    signalR,
		clock:      clock,
		tzktClient: tzktClient,
	}, nil
}

// SubscribeEvents subscribes to FA2 transfer events and metadata updates via TzKT SignalR
// Note: fromLevel parameter is ignored for Tezos as TzKT always subscribes from current
func (c *tzSubscriber) SubscribeEvents(ctx context.Context, fromLevel uint64, handler messaging.EventHandler) error {
	if c.connected {
		logger.Warn("Already connected to TzKT SignalR")
		return nil
	}

	// Store context for cancellation
	c.ctx = ctx

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
	return ctx.Err()
}

// Transfers handles incoming transfer events from TzKT SignalR
// Method name must match the SignalR target name "transfers"
func (c *tzSubscriber) Transfers(data interface{}) {
	// Marshal data to JSON
	jsonData, err := json.Marshal(data)
	if err != nil {
		logger.Error(fmt.Errorf("error marshaling transfers data: %w", err), zap.Error(err))
		return
	}

	// Unmarshal data to TzKTMessage
	var msg TzKTMessage
	if err := json.Unmarshal(jsonData, &msg); err != nil {
		logger.Error(fmt.Errorf("error unmarshaling transfers message: %w", err), zap.Error(err))
		return
	}

	// Type 0 is state/confirmation message, ignore it
	if msg.Type == MessageTypeState {
		logger.Debug("Received transfers state message", zap.Any("state", msg.State))
		return
	}

	// Only handle data messages
	if msg.Type != MessageTypeData {
		// FIXME: Handle other message types
		logger.Warn("Transfers message type is not data", zap.Any("type", msg.Type))
		return
	}

	if msg.Data == nil {
		logger.Warn("Transfers message without data field")
		return
	}

	// Unmarshal data array into transfer events
	var transfers []TzKTTokenTransfer
	if err := json.Unmarshal(msg.Data, &transfers); err != nil {
		logger.Error(fmt.Errorf("error unmarshaling transfers data: %w", err), zap.Error(err))
		return
	}

	for i := range transfers {
		transfer := transfers[i]
		if transfer.Token.Standard != domain.StandardFA2 {
			logger.Debug("Skipping token is not fa2",
				zap.String("standard", string(transfer.Token.Standard)),
				zap.String("contract", transfer.Token.Contract.Address),
				zap.String("tokenId", transfer.Token.TokenID))
			continue
		}

		event, err := c.tzktClient.ParseTransfer(c.ctx, &transfer)
		if err != nil {
			logger.Error(fmt.Errorf("error parsing transfer: %w", err), zap.Error(err))
			continue
		}

		// Call handler if set
		if c.handler != nil {
			if err := c.handler(event); err != nil {
				logger.Error(fmt.Errorf("error handling event: %w", err), zap.Error(err))
			}
		}
	}
}

// Bigmaps handles incoming big map update events from TzKT SignalR
// Method name must match the SignalR target name "bigmaps"
func (c *tzSubscriber) Bigmaps(data interface{}) {
	// Marshal data to JSON
	jsonData, err := json.Marshal(data)
	if err != nil {
		logger.Error(fmt.Errorf("error marshaling bigmaps data: %w", err), zap.Error(err))
		return
	}

	// Unmarshal data to TzKTMessage
	var msg TzKTMessage
	if err := json.Unmarshal(jsonData, &msg); err != nil {
		logger.Error(fmt.Errorf("error unmarshaling bigmaps message: %w", err), zap.Error(err))
		return
	}

	// Type 0 is state/confirmation message, ignore it
	if msg.Type == MessageTypeState {
		logger.Debug("Received bigmaps state message", zap.Any("state", msg.State))
		return
	}

	// Only handle data messages
	if msg.Type != MessageTypeData {
		// FIXME: Handle other message types
		logger.Warn("Bigmaps message type is not data", zap.Any("type", msg.Type))
		return
	}

	if msg.Data == nil {
		logger.Warn("Bigmaps message without data field")
		return
	}

	// Unmarshal data array into bigmap updates
	var updates []TzKTBigMapUpdate
	if err := json.Unmarshal(msg.Data, &updates); err != nil {
		logger.Error(fmt.Errorf("error unmarshaling bigmaps data: %w", err), zap.Error(err))
		return
	}

	for i := range updates {
		update := updates[i]

		// Only handle token_metadata updates
		if update.Path != "token_metadata" {
			continue
		}

		event, err := c.tzktClient.ParseBigMapUpdate(c.ctx, &update)
		if err != nil {
			logger.Error(fmt.Errorf("error parsing big map update: %w", err), zap.Error(err))
			continue
		}

		// Call handler if set
		if c.handler != nil {
			if err := c.handler(event); err != nil {
				logger.Error(fmt.Errorf("error handling metadata update event: %w", err), zap.Error(err))
			}
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
	logger.Info("TzKT WebSocket connection closed")
}

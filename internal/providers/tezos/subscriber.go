package tezos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/blockchain"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
)

const (
	SUBSCRIBE_TIMEOUT      = 15 * time.Second
	defaultEventBufferSize = 2048
)

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
	cancel     context.CancelFunc
	wsURL      string
	chainID    domain.Chain
	client     adapter.SignalRClient
	connected  bool
	handler    blockchain.EventHandler
	signalR    adapter.SignalR
	clock      adapter.Clock
	tzktClient TzKTClient
	config     Config
	streamCh   chan streamMessage
	errCh      chan error
}

type streamMessage struct {
	transfers []TzKTTokenTransfer
	updates   []TzKTBigMapUpdate
}

// NewSubscriber creates a new Tezos/TzKT event subscriber
func NewSubscriber(cfg Config, signalR adapter.SignalR, clock adapter.Clock, tzktClient TzKTClient) (blockchain.EventSource, error) {
	return &tzSubscriber{
		wsURL:      cfg.WebSocketURL,
		chainID:    cfg.ChainID,
		signalR:    signalR,
		clock:      clock,
		tzktClient: tzktClient,
		config:     cfg,
	}, nil
}

// SubscribeEvents subscribes to FA2 transfer events and metadata updates via TzKT SignalR.
func (c *tzSubscriber) SubscribeEvents(ctx context.Context, fromLevel uint64, handler blockchain.EventHandler) error {
	if c.connected {
		logger.WarnCtx(ctx, "Already connected to TzKT SignalR")
		return nil
	}

	subscriptionCtx, cancel := context.WithCancel(ctx)
	c.ctx = subscriptionCtx
	c.cancel = cancel

	// Store handler for callback
	c.handler = handler
	c.streamCh = make(chan streamMessage, defaultEventBufferSize)
	c.errCh = make(chan error, 1)

	// Create SignalR client with connection
	client, err := c.signalR.NewClient(subscriptionCtx, c.wsURL, c)
	if err != nil {
		cancel()
		c.cancel = nil
		return fmt.Errorf("failed to create SignalR client: %w", err)
	}

	c.client = client

	defer func() {
		if !c.connected && c.client != nil {
			c.Close()
		}
	}()

	// Connect to SignalR hub
	client.Start()

	// Wait for connection
	c.clock.Sleep(time.Second)

	// Subscribe to token transfers
	// TODO(tezos-resume): Resume from fromLevel by backfilling missed events and
	// establishing a no-gap handoff into the live SignalR feeds.
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
	// TODO(tezos-feed-ordering): Preserve a stable cross-feed order between
	// transfers and bigmaps before advancing the durable cursor. Without a
	// merged ordering strategy, a later bigmap event can flush before an earlier
	// transfer event and move the cursor past unfinished work.
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

	go c.processStream(subscriptionCtx)
	c.connected = true

	select {
	case err := <-c.errCh:
		c.Close()
		return err
	case <-ctx.Done():
		c.Close()
		logger.InfoCtx(ctx, "TzKT WebSocket connection closed due to context done")
		return ctx.Err()
	}
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

	c.enqueueStream(streamMessage{transfers: transfers})
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

	c.enqueueStream(streamMessage{updates: updates})
}

// GetLatestBlock returns the latest block level from TzKT API
func (c *tzSubscriber) GetLatestBlock(ctx context.Context) (uint64, error) {
	return c.tzktClient.GetLatestBlock(ctx)
}

// Close closes the SignalR connection
func (c *tzSubscriber) Close() {
	if c.cancel != nil {
		c.cancel()
		c.cancel = nil
	}

	if c.client != nil {
		c.client.Stop()
		c.client = nil
	}

	c.connected = false
	logger.InfoCtx(c.ctx, "TzKT WebSocket connection closed")
}

func (c *tzSubscriber) enqueueStream(msg streamMessage) {
	if c.streamCh == nil {
		return
	}

	select {
	case <-c.ctx.Done():
	case c.streamCh <- msg:
	}
}

func (c *tzSubscriber) processStream(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-c.streamCh:
			if err := c.handleTransfers(ctx, msg.transfers); err != nil {
				c.reportError(err)
				return
			}
			if err := c.handleBigMapUpdates(ctx, msg.updates); err != nil {
				c.reportError(err)
				return
			}
		}
	}
}

func (c *tzSubscriber) handleTransfers(ctx context.Context, transfers []TzKTTokenTransfer) error {
	for i := range transfers {
		transfer := transfers[i]
		if transfer.Token.Standard != domain.StandardFA2 {
			logger.DebugCtx(ctx, "Skipping token is not fa2",
				zap.String("standard", string(transfer.Token.Standard)),
				zap.String("contract", transfer.Token.Contract.Address),
				zap.String("tokenId", transfer.Token.TokenID))
			continue
		}

		event, err := c.tzktClient.ParseTransfer(ctx, &transfer)
		if err != nil {
			logger.ErrorCtx(ctx, errors.New("error parsing transfer"), zap.Error(err))
			continue
		}
		if event == nil || c.handler == nil {
			continue
		}

		if err := c.handler(event); err != nil {
			return fmt.Errorf("failed to handle tezos transfer event %s at block %d: %w", event.TxHash, event.BlockNumber, err)
		}
	}

	return nil
}

func (c *tzSubscriber) handleBigMapUpdates(ctx context.Context, updates []TzKTBigMapUpdate) error {
	for i := range updates {
		update := updates[i]
		if update.Path != "token_metadata" {
			continue
		}

		event, err := c.tzktClient.ParseBigMapUpdate(ctx, &update)
		if err != nil {
			logger.ErrorCtx(ctx, errors.New("error parsing big map update"), zap.Error(err))
			continue
		}
		if event == nil || c.handler == nil {
			continue
		}

		if err := c.handler(event); err != nil {
			return fmt.Errorf("failed to handle tezos metadata event %s at block %d: %w", event.TxHash, event.BlockNumber, err)
		}
	}

	return nil
}

func (c *tzSubscriber) reportError(err error) {
	select {
	case c.errCh <- err:
	default:
	}
}

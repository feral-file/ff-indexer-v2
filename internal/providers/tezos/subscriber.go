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
	"github.com/feral-file/ff-indexer-v2/internal/subscriber"
)

const SUBSCRIBE_TIMEOUT = 15 * time.Second

// Config holds the configuration for Tezos/TzKT subscription
type Config struct {
	APIURL       string       // HTTP API URL (e.g., https://api.tzkt.io)
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
	apiURL    string
	wsURL     string
	chainID   domain.Chain
	client    adapter.SignalRClient
	connected bool
	handler   subscriber.EventHandler
	signalR   adapter.SignalR
	clock     adapter.Clock
}

// TzKTTransferEvent represents a transfer event from TzKT
type TzKTTransferEvent struct {
	Type      int    `json:"type"`      // Event type
	ID        uint64 `json:"id"`        // Transfer ID
	Level     uint64 `json:"level"`     // Block level
	Timestamp string `json:"timestamp"` // ISO timestamp

	Token struct {
		ID       uint64 `json:"id"`
		Contract struct {
			Address string `json:"address"`
		} `json:"contract"`
		TokenID  string               `json:"tokenId"`
		Standard domain.ChainStandard `json:"standard"` // "fa2", "fa1.2"
	} `json:"token"`

	From *struct {
		Address string `json:"address"`
	} `json:"from"`

	To *struct {
		Address string `json:"address"`
	} `json:"to"`

	Amount        string `json:"amount"`
	TransactionID uint64 `json:"transactionId"`

	// Additional metadata for metadata updates
	Metadata interface{} `json:"metadata,omitempty"`
}

// TzKTBigMapUpdate represents a big map update event from TzKT
type TzKTBigMapUpdate struct {
	ID        uint64 `json:"id"`        // Update ID
	Level     uint64 `json:"level"`     // Block level
	Timestamp string `json:"timestamp"` // ISO timestamp

	BigMap uint64 `json:"bigmap"` // BigMap ID

	Contract struct {
		Address string `json:"address"`
	} `json:"contract"`

	Path string `json:"path"` // Path in contract storage (e.g., "token_metadata")

	Action string `json:"action"` // "add_key", "update_key", "remove_key"

	Content struct {
		Hash  string      `json:"hash"`
		Key   interface{} `json:"key"`   // Token ID (can be int or string)
		Value interface{} `json:"value"` // Metadata value
	} `json:"content"`

	TransactionID *uint64 `json:"transactionId,omitempty"`
}

// NewSubscriber creates a new Tezos/TzKT event subscriber
func NewSubscriber(cfg Config, signalR adapter.SignalR, clock adapter.Clock) (subscriber.Subscriber, error) {
	return &tzSubscriber{
		apiURL:  cfg.APIURL,
		wsURL:   cfg.WebSocketURL,
		chainID: cfg.ChainID,
		signalR: signalR,
		clock:   clock,
	}, nil
}

// SubscribeEvents subscribes to FA2 transfer events and metadata updates via TzKT SignalR
// Note: fromLevel parameter is ignored for Tezos as TzKT always subscribes from current
func (s *tzSubscriber) SubscribeEvents(ctx context.Context, fromLevel uint64, handler subscriber.EventHandler) error {
	// Store handler for callback
	s.handler = handler

	// Create SignalR client with connection
	client, err := s.signalR.NewClient(ctx, s.wsURL, s)
	if err != nil {
		return fmt.Errorf("failed to create SignalR client: %w", err)
	}

	s.client = client

	// Connect to SignalR hub
	client.Start()

	// Wait for connection
	s.clock.Sleep(2 * time.Second)

	// Subscribe to token transfers
	sendErrChan := client.Send("SubscribeToTokenTransfers", map[string]interface{}{})
	select {
	case err := <-sendErrChan:
		if err != nil {
			return fmt.Errorf("failed to subscribe to token transfers: %w", err)
		}
	case <-s.clock.After(SUBSCRIBE_TIMEOUT):
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
	case <-s.clock.After(SUBSCRIBE_TIMEOUT):
		// Timeout waiting for send
		return fmt.Errorf("timeout waiting for big maps subscription: %w", domain.ErrSubscriptionFailed)
	}

	s.connected = true

	// Keep connection alive and handle events
	<-ctx.Done()
	return ctx.Err()
}

// Transfers handles incoming transfer events from TzKT SignalR
// Method name must match the SignalR target name "transfers"
func (s *tzSubscriber) Transfers(data interface{}) {
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
	var transfers []TzKTTransferEvent
	if err := json.Unmarshal(msg.Data, &transfers); err != nil {
		logger.Error(fmt.Errorf("error unmarshaling transfers data: %w", err), zap.Error(err))
		return
	}

	for _, transfer := range transfers {
		event, err := s.parseTransfer(transfer)
		if err != nil {
			logger.Error(fmt.Errorf("error parsing transfer: %w", err), zap.Error(err))
			continue
		}

		// Call handler if set
		if s.handler != nil {
			if err := s.handler(event); err != nil {
				logger.Error(fmt.Errorf("error handling event: %w", err), zap.Error(err))
			}
		}
	}
}

// Bigmaps handles incoming big map update events from TzKT SignalR
// Method name must match the SignalR target name "bigmaps"
func (s *tzSubscriber) Bigmaps(data interface{}) {
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

	for _, update := range updates {
		// Only handle token_metadata updates
		if update.Path != "token_metadata" {
			continue
		}

		event, err := s.parseBigMapUpdate(update)
		if err != nil {
			logger.Error(fmt.Errorf("error parsing big map update: %w", err), zap.Error(err))
			continue
		}

		// Call handler if set
		if s.handler != nil {
			if err := s.handler(event); err != nil {
				logger.Error(fmt.Errorf("error handling metadata update event: %w", err), zap.Error(err))
			}
		}
	}
}

// parseTransfer converts a TzKT transfer to a standardized blockchain event
func (s *tzSubscriber) parseTransfer(transfer TzKTTransferEvent) (*domain.BlockchainEvent, error) {
	// Parse timestamp
	timestamp, err := s.clock.Parse(time.RFC3339, transfer.Timestamp)
	if err != nil {
		timestamp = s.clock.Now()
	}

	// Determine addresses
	fromAddress := ""
	if transfer.From != nil {
		fromAddress = transfer.From.Address
	}

	toAddress := ""
	if transfer.To != nil {
		toAddress = transfer.To.Address
	}

	// Determine event type
	eventType := s.determineTransferEventType(fromAddress, toAddress)

	event := &domain.BlockchainEvent{
		Chain:           s.chainID,
		Standard:        transfer.Token.Standard,
		ContractAddress: transfer.Token.Contract.Address,
		TokenNumber:     transfer.Token.TokenID,
		EventType:       eventType,
		FromAddress:     fromAddress,
		ToAddress:       toAddress,
		Quantity:        transfer.Amount,
		TxHash:          fmt.Sprintf("%d", transfer.TransactionID), // TzKT uses transaction ID
		BlockNumber:     transfer.Level,
		BlockHash:       "", // TzKT doesn't provide block hash in transfer events
		Timestamp:       timestamp,
		LogIndex:        transfer.ID, // Use transfer ID as log index
	}

	return event, nil
}

// parseBigMapUpdate converts a TzKT big map update to a standardized blockchain event
func (s *tzSubscriber) parseBigMapUpdate(update TzKTBigMapUpdate) (*domain.BlockchainEvent, error) {
	// Parse timestamp
	timestamp, err := s.clock.Parse(time.RFC3339, update.Timestamp)
	if err != nil {
		timestamp = s.clock.Now()
	}

	// Extract token ID from the key
	tokenID := fmt.Sprintf("%v", update.Content.Key)

	// Build transaction hash
	txHash := ""
	if update.TransactionID != nil {
		txHash = fmt.Sprintf("%d", *update.TransactionID)
	} else {
		txHash = fmt.Sprintf("bigmap_%d", update.ID)
	}

	event := &domain.BlockchainEvent{
		Chain:           s.chainID,
		Standard:        domain.StandardFA2, // BigMap updates are for FA2 tokens on Tezos
		ContractAddress: update.Contract.Address,
		TokenNumber:     tokenID,
		EventType:       domain.EventTypeMetadataUpdate,
		FromAddress:     "",
		ToAddress:       "",
		Quantity:        "1",
		TxHash:          txHash,
		BlockNumber:     update.Level,
		BlockHash:       "", // TzKT doesn't provide block hash in big map events
		Timestamp:       timestamp,
		LogIndex:        update.ID,
	}

	return event, nil
}

// determineTransferEventType determines the event type based on from/to addresses
func (s *tzSubscriber) determineTransferEventType(from, to string) domain.EventType {
	if from == "" {
		return domain.EventTypeMint
	}
	if to == "" {
		return domain.EventTypeBurn
	}
	return domain.EventTypeTransfer
}

// GetLatestBlock returns the latest block level from TzKT API
func (s *tzSubscriber) GetLatestBlock(ctx context.Context) (uint64, error) {
	// This would require HTTP client to call TzKT API
	// Simplified implementation - would need actual HTTP request
	// For now, return 0 to indicate we should start from current
	return 0, nil
}

// Close closes the SignalR connection
func (s *tzSubscriber) Close() {
	if s.client == nil || !s.connected {
		return
	}

	s.client.Stop()
	s.connected = false
}

package tezos

import (
	"context"
	"fmt"
	"time"

	logger "github.com/bitmark-inc/autonomy-logger"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/philippseith/signalr"
	"go.uber.org/zap"
)

const SUBSCRIBE_TIMEOUT = 5 * time.Second

// EventHandler is called when a new event is received
type EventHandler func(event *domain.BlockchainEvent) error

// Subscriber defines the interface for subscribing to Tezos events via TzKT
type Subscriber interface {
	// SubscribeEvents subscribes to FA2 transfer events and metadata updates
	SubscribeEvents(ctx context.Context, handler EventHandler) error
	// GetLatestLevel returns the latest block level
	GetLatestLevel(ctx context.Context) (uint64, error)
	// Close closes the connection
	Close() error
}

// Config holds the configuration for Tezos/TzKT subscription
type Config struct {
	APIURL       string       // HTTP API URL (e.g., https://api.tzkt.io)
	WebSocketURL string       // WebSocket URL (e.g., https://api.tzkt.io/v1/ws)
	ChainID      domain.Chain // e.g., "tezos:mainnet"
}

type subscriber struct {
	apiURL    string
	wsURL     string
	chainID   domain.Chain
	client    signalr.Client
	connected bool
	handler   EventHandler
}

// TzKTTransferEvent represents a transfer event from TzKT
type TzKTTransferEvent struct {
	Type      int    `json:"type"`      // Event type
	ID        int64  `json:"id"`        // Transfer ID
	Level     int64  `json:"level"`     // Block level
	Timestamp string `json:"timestamp"` // ISO timestamp

	Token struct {
		ID       int64 `json:"id"`
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
	TransactionID int64  `json:"transactionId"`

	// Additional metadata for metadata updates
	Metadata interface{} `json:"metadata,omitempty"`
}

// TzKTBigMapUpdate represents a big map update event from TzKT
type TzKTBigMapUpdate struct {
	ID        int64  `json:"id"`        // Update ID
	Level     int64  `json:"level"`     // Block level
	Timestamp string `json:"timestamp"` // ISO timestamp

	BigMap int64 `json:"bigmap"` // BigMap ID

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

	TransactionID *int64 `json:"transactionId,omitempty"`
}

// NewSubscriber creates a new Tezos/TzKT event subscriber
func NewSubscriber(cfg Config) (Subscriber, error) {
	return &subscriber{
		apiURL:  cfg.APIURL,
		wsURL:   cfg.WebSocketURL,
		chainID: cfg.ChainID,
	}, nil
}

// SubscribeEvents subscribes to FA2 transfer events and metadata updates via TzKT SignalR
func (s *subscriber) SubscribeEvents(ctx context.Context, handler EventHandler) error {
	// Store handler for callback
	s.handler = handler

	// Create SignalR connection
	conn, err := signalr.NewHTTPConnection(ctx, s.wsURL)
	if err != nil {
		return fmt.Errorf("%w: failed to create connection: %v", domain.ErrConnectionFailed, err)
	}

	// Create SignalR client
	client, err := signalr.NewClient(ctx,
		signalr.WithConnection(conn),
		signalr.WithReceiver(s),
	)
	if err != nil {
		return fmt.Errorf("%w: failed to create SignalR client: %v", domain.ErrConnectionFailed, err)
	}

	s.client = client

	// Connect to SignalR hub
	client.Start()

	// Wait for connection
	time.Sleep(2 * time.Second)

	// Subscribe to token transfers
	sendErrChan := client.Send("SubscribeToTokenTransfers", map[string]interface{}{})
	select {
	case err := <-sendErrChan:
		if err != nil {
			return fmt.Errorf("%w: failed to subscribe to token transfers: %v", domain.ErrSubscriptionFailed, err)
		}
	case <-time.After(SUBSCRIBE_TIMEOUT):
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
			return fmt.Errorf("%w: failed to subscribe to big maps: %v", domain.ErrSubscriptionFailed, err)
		}
	case <-time.After(SUBSCRIBE_TIMEOUT):
		// Timeout waiting for send
		return fmt.Errorf("timeout waiting for big maps subscription: %w", domain.ErrSubscriptionFailed)
	}

	s.connected = true

	// Keep connection alive and handle events
	<-ctx.Done()
	return ctx.Err()
}

// ReceiveTokenTransfers handles incoming transfer events from TzKT
func (s *subscriber) ReceiveTokenTransfers(transfers []TzKTTransferEvent) {
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

// ReceiveBigMaps handles incoming big map update events from TzKT
func (s *subscriber) ReceiveBigMaps(updates []TzKTBigMapUpdate) {
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
func (s *subscriber) parseTransfer(transfer TzKTTransferEvent) (*domain.BlockchainEvent, error) {
	// Parse timestamp
	timestamp, err := time.Parse(time.RFC3339, transfer.Timestamp)
	if err != nil {
		timestamp = time.Now()
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
		BlockNumber:     uint64(transfer.Level),
		BlockHash:       "", // TzKT doesn't provide block hash in transfer events
		Timestamp:       timestamp,
		LogIndex:        uint(transfer.ID), // Use transfer ID as log index
	}

	return event, nil
}

// parseBigMapUpdate converts a TzKT big map update to a standardized blockchain event
func (s *subscriber) parseBigMapUpdate(update TzKTBigMapUpdate) (*domain.BlockchainEvent, error) {
	// Parse timestamp
	timestamp, err := time.Parse(time.RFC3339, update.Timestamp)
	if err != nil {
		timestamp = time.Now()
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
		BlockNumber:     uint64(update.Level),
		BlockHash:       "", // TzKT doesn't provide block hash in big map events
		Timestamp:       timestamp,
		LogIndex:        uint(update.ID),
	}

	return event, nil
}

// determineTransferEventType determines the event type based on from/to addresses
func (s *subscriber) determineTransferEventType(from, to string) domain.EventType {
	if from == "" {
		return domain.EventTypeMint
	}
	if to == "" {
		return domain.EventTypeBurn
	}
	return domain.EventTypeTransfer
}

// GetLatestLevel returns the latest block level from TzKT API
func (s *subscriber) GetLatestLevel(ctx context.Context) (uint64, error) {
	// This would require HTTP client to call TzKT API
	// Simplified implementation - would need actual HTTP request
	return 0, fmt.Errorf("not implemented")
}

// Close closes the SignalR connection
func (s *subscriber) Close() error {
	if s.client != nil && s.connected {
		s.client.Stop()
		s.connected = false
	}
	return nil
}

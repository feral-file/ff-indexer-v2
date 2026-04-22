package blockchain

import (
	"context"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
)

// EventHandler handles a normalized blockchain event emitted by a chain source.
type EventHandler func(event *domain.BlockchainEvent) error

// EventSource defines the common interface for Ethereum and Tezos event feeds.
//
//go:generate mockgen -source=source.go -destination=../mocks/blockchain_event_source.go -package=mocks -mock_names=EventSource=MockBlockchainEventSource
type EventSource interface {
	// SubscribeEvents starts the event stream from the provided block or level and
	// calls handler for each normalized blockchain event.
	SubscribeEvents(ctx context.Context, fromBlock uint64, handler EventHandler) error

	// GetLatestBlock returns the latest block number or level for the chain source.
	GetLatestBlock(ctx context.Context) (uint64, error)

	// Close shuts down the underlying chain connection.
	Close()
}

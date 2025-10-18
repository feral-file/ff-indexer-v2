package messaging

import (
	"context"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
)

// EventHandler is called when a new blockchain event is received
type EventHandler func(event *domain.BlockchainEvent) error

// Subscriber defines the common interface for subscribing to blockchain events
// Both Ethereum and Tezos subscribers implement this interface
//
//go:generate mockgen -source=subscriber.go -destination=../mocks/subscriber.go -package=mocks -mock_names=Subscriber=MockSubscriber
type Subscriber interface {
	// SubscribeEvents subscribes to blockchain events (transfers + metadata updates)
	// fromBlock/fromLevel: starting point for subscription (0 for latest)
	// handler: callback function to process each event
	SubscribeEvents(ctx context.Context, fromBlock uint64, handler EventHandler) error

	// GetLatestBlock returns the latest block number/level
	GetLatestBlock(ctx context.Context) (uint64, error)

	// Close closes the connection and cleans up resources
	Close()
}

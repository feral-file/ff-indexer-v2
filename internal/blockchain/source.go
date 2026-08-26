package blockchain

import (
	"context"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
)

// EventHandler handles a normalized blockchain event emitted by a chain source.
type EventHandler func(event *domain.BlockchainEvent) error

// RangeProgressHandler receives the highest block a source vouches for: every
// block up to and including `through` has been emitted to the event handler.
type RangeProgressHandler func(through uint64) error

// ProgressReporter is implemented by sources that fetch blocks in ranges (the
// Ethereum head-driven pull) and therefore know when a block is complete
// without waiting for the next one's events. The runner uses the reports to
// flush the open block promptly and to persist the cursor through ranges that
// carried no events, so a resume never re-scans them or trips the catch-up
// bound. Push-style sources (Tezos) do not implement it.
type ProgressReporter interface {
	// SetProgressHandler registers the handler; called by the runner before
	// SubscribeEvents. Sources must report only after the events of the range
	// have been handed to the event handler.
	SetProgressHandler(handler RangeProgressHandler)
}

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

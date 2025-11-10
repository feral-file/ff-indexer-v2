package messaging

import (
	"context"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
)

// Publisher defines the interface for publishing events to message queue
//
//go:generate mockgen -source=publisher.go -destination=../mocks/publisher.go -package=mocks -mock_names=Publisher=MockPublisher
type Publisher interface {
	// PublishEvent publishes a blockchain event to the message broker
	PublishEvent(ctx context.Context, event *domain.BlockchainEvent) error
	// Close closes the connection
	Close()
	// CloseChan returns a channel that is closed when the publisher is closed
	CloseChan() <-chan struct{}
}

package workflows

import (
	"go.temporal.io/sdk/workflow"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
)

// WorkerCore defines the interface for processing blockchain events
//
//go:generate mockgen -source=worker.go -destination=../mocks/worker_core.go -package=mocks -mock_names=WorkerCore=MockWorkerCore
type WorkerCore interface {
	// IndexTokenMint processes a token mint event
	IndexTokenMint(ctx workflow.Context, event *domain.BlockchainEvent) error

	// IndexTokenTransfer processes a token transfer event
	IndexTokenTransfer(ctx workflow.Context, event *domain.BlockchainEvent) error

	// IndexTokenBurn processes a token burn event
	IndexTokenBurn(ctx workflow.Context, event *domain.BlockchainEvent) error

	// IndexMetadataUpdate processes a metadata update event
	IndexMetadataUpdate(ctx workflow.Context, event *domain.BlockchainEvent) error
}

// WorkerMedia defines the interface for processing media events
// TODO: Define the interface for media events
type WorkerMedia interface{}

// workerCore is the concrete implementation of WorkerCore
type workerCore struct {
	executor Executor
}

// NewWorkerCore creates a new worker core instance
func NewWorkerCore(executor Executor) WorkerCore {
	return &workerCore{
		executor: executor,
	}
}

// workerMedia is the concrete implementation of WorkerMedia
type workerMedia struct {
	executor Executor
}

// NewWorkerMedia creates a new worker media instance
func NewWorkerMedia(executor Executor) WorkerMedia {
	return &workerMedia{
		executor: executor,
	}
}

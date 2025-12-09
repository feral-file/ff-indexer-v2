//go:build cgo

package workflowsmedia

import (
	"go.temporal.io/sdk/workflow"
)

// Worker defines the interface for processing media workflows
//
//go:generate mockgen -source=worker.go -destination=../../mocks/worker_media.go -package=mocks -mock_names=Worker=MockMediaWorker
type Worker interface {
	// IndexMediaWorkflow indexes a single media file
	IndexMediaWorkflow(ctx workflow.Context, url string) error

	// IndexMultipleMediaWorkflow indexes multiple media files
	IndexMultipleMediaWorkflow(ctx workflow.Context, urls []string) error
}

// worker is the concrete implementation of Worker
type worker struct {
	executor Executor
}

// NewWorker creates a new media worker instance
func NewWorker(executor Executor) Worker {
	return &worker{
		executor: executor,
	}
}

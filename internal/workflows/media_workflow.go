//go:build cgo

package workflows

import (
	"go.temporal.io/sdk/workflow"
)

// MediaWorkflows defines the interface for processing media workflows.
//
//go:generate mockgen -source=media_workflow.go -destination=../mocks/worker_media.go -package=mocks -mock_names=MediaWorkflows=MockMediaWorkflows
type MediaWorkflows interface {
	// IndexMediaWorkflow indexes a single media file
	IndexMediaWorkflow(ctx workflow.Context, url string) error

	// IndexMultipleMediaWorkflow indexes multiple media files
	IndexMultipleMediaWorkflow(ctx workflow.Context, urls []string) error
}

// mediaWorkflows is the concrete implementation of MediaWorkflows.
type mediaWorkflows struct {
	executor MediaExecutor
}

// NewMediaWorkflows creates a new media workflows instance.
func NewMediaWorkflows(executor MediaExecutor) MediaWorkflows {
	return &mediaWorkflows{
		executor: executor,
	}
}

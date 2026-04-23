//go:build cgo

package workflows

import (
	"go.temporal.io/sdk/workflow"

	"github.com/feral-file/ff-indexer-v2/internal/providers/jobs"
)

// MediaWorkflows defines the interface for processing media workflows.
//
//go:generate mockgen -source=media_workflow.go -destination=../mocks/media_workflows.go -package=mocks -mock_names=MediaWorkflows=MockMediaWorkflows
type MediaWorkflows interface {
	// IndexMediaWorkflow indexes a single media file
	IndexMediaWorkflow(ctx workflow.Context, url string) error

	// IndexMultipleMediaWorkflow indexes multiple media files
	IndexMultipleMediaWorkflow(ctx workflow.Context, urls []string) error
}

// mediaWorkflows is the concrete implementation of MediaWorkflows.
type mediaWorkflows struct {
	executor MediaExecutor
	jobQueue jobs.JobQueue
}

// NewMediaWorkflows creates a new media workflows instance.
// jobQueue may be nil when no enqueues are performed from this workflow.
func NewMediaWorkflows(executor MediaExecutor, jobQueue jobs.JobQueue) MediaWorkflows {
	return &mediaWorkflows{
		executor: executor,
		jobQueue: jobQueue,
	}
}

//go:build cgo

package workflows

import (
	"context"

	"github.com/feral-file/ff-indexer-v2/internal/providers/jobs"
)

// MediaWorkflows defines the interface for processing media workflows.
//
//go:generate mockgen -source=media_workflow.go -destination=../mocks/media_workflows.go -package=mocks -mock_names=MediaWorkflows=MockMediaWorkflows
type MediaWorkflows interface {
	// IndexMediaWorkflow indexes a single media file
	IndexMediaWorkflow(ctx context.Context, url string) error

	// IndexMultipleMediaWorkflow indexes multiple media files
	IndexMultipleMediaWorkflow(ctx context.Context, urls []string) error

	// RenderMediaProbe runs one L1 render probe for a URL (see RenderProbeExecutor)
	RenderMediaProbe(ctx context.Context, url string) error
}

// MediaWorkflowsConfig holds queue names and other settings for [MediaWorkflows].
type MediaWorkflowsConfig struct {
	// MediaTaskQueue is the jobs.queue for child IndexMediaWorkflow jobs enqueued by IndexMultipleMediaWorkflow.
	MediaTaskQueue string
}

// mediaWorkflows is the concrete implementation of MediaWorkflows.
type mediaWorkflows struct {
	executor            MediaExecutor
	renderProbeExecutor RenderProbeExecutor // nil when the render probe is disabled
	jobQueue            jobs.JobQueue
	config              MediaWorkflowsConfig
}

// NewMediaWorkflows creates a new media workflows instance.
// jobQueue is required. Non-test call sites that only need method values may use [jobs.NopQueue];
// renderProbeExecutor may be nil when the render probe is disabled — RenderMediaProbe
// jobs then no-op instead of erroring, so jobs already enqueued before a disable survive
// a config flip without failing.
func NewMediaWorkflows(executor MediaExecutor, renderProbeExecutor RenderProbeExecutor, jobQueue jobs.JobQueue, config MediaWorkflowsConfig) MediaWorkflows {
	if jobQueue == nil {
		panic("workflows: NewMediaWorkflows requires a non-nil jobQueue (see NewMediaWorkflows doc for NopQueue vs mocks)")
	}
	return &mediaWorkflows{
		executor:            executor,
		renderProbeExecutor: renderProbeExecutor,
		jobQueue:            jobQueue,
		config:              config,
	}
}

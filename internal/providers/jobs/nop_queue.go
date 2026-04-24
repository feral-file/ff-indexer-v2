package jobs

import (
	"context"
	"fmt"

	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

// NopQueue is a no-op [JobQueue] for **non-test** call sites: Enqueue is a no-op, GetStatus returns an error,
// Cancel is a no-op.
//
// Rationale: API handlers, ingestion, and similar production code construct CoreWorkflows only to obtain
// workflow method values (e.g. w.IndexTokenMint) for Temporal's client.ExecuteWorkflow. That stub is never
// used to run workflow logic or enqueue work—the worker runs the real process with a real queue. Unit tests
// should use a gomock-generated JobQueue mock instead; NopQueue is not a substitute for mocking in tests.
type NopQueue struct{}

// Enqueue implements JobQueue.
func (NopQueue) Enqueue(_ context.Context, _ EnqueueOptions) (*schema.Job, bool, error) {
	return nil, true, nil
}

// GetStatus implements JobQueue.
func (NopQueue) GetStatus(_ context.Context, _ int64) (*schema.Job, error) {
	return nil, fmt.Errorf("nop queue: no job status")
}

// Cancel implements JobQueue.
func (NopQueue) Cancel(_ context.Context, _ int64) error {
	return nil
}

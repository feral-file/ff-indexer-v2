// Package jobs coordinates postgres-backed work queues: producers enqueue work through JobQueue;
// a Worker process claims rows, dispatches to registered functions, and reports outcomes via store.Store.
// SQL remains in the store package.
package jobs

import (
	"context"
	"fmt"
	"time"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/store"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

//go:generate mockgen -source=queue.go -destination=../../mocks/job_queue.go -package=mocks -mock_names=JobQueue=MockJobQueue

// JobQueue is the producer side of the job system: create work and check or request cancellation.
// It wraps store.Store; implementations must not run SQL outside the store.
type JobQueue interface {
	// Enqueue inserts a job (JSON payload from Args) or returns an existing active row when
	// unique-key deduplication matches.
	Enqueue(ctx context.Context, opts EnqueueOptions) (job *schema.Job, created bool, err error)
	// GetStatus returns the current row for id (including status and timestamps), or a store error
	// if the id does not exist.
	GetStatus(ctx context.Context, id int64) (*schema.Job, error)
	// Cancel sets cancel_requested for a pending or running job. The worker applies terminal cancel.
	Cancel(ctx context.Context, id int64) error
}

// EnqueueOptions describes one logical unit of work.
type EnqueueOptions struct {
	// Queue is the queue name (e.g. token_index, media_index).
	Queue string
	// Kind is the registered handler name.
	Kind string
	// Args is marshaled to a JSON array and stored in jobs.payload. Nil becomes `[]`.
	Args []any
	// UniqueKey, when set, enables partial unique de-duplication for active jobs per (queue, kind, key).
	UniqueKey *string
	// RunAfter is the earliest time the job may be claimed; nil means "now" (UTC at enqueue).
	RunAfter *time.Time
}

type jobQueue struct {
	store store.Store
	codec adapter.JSON
}

// NewJobQueue returns a JobQueue backed by the given store. json must be the process-wide
// adapter.JSON (e.g. adapter.NewJSON() from main) so marshaling matches other services.
func NewJobQueue(s store.Store, json adapter.JSON) JobQueue {
	if s == nil {
		panic("jobs.NewJobQueue: store is nil")
	}
	if json == nil {
		panic("jobs.NewJobQueue: json is nil")
	}
	return &jobQueue{store: s, codec: json}
}

// Enqueue implements JobQueue.
func (q *jobQueue) Enqueue(ctx context.Context, opts EnqueueOptions) (*schema.Job, bool, error) {
	if opts.Queue == "" || opts.Kind == "" {
		return nil, false, fmt.Errorf("jobs.Enqueue: queue and kind are required")
	}
	args := opts.Args
	if args == nil {
		args = []any{}
	}
	payload, err := q.codec.Marshal(args)
	if err != nil {
		return nil, false, fmt.Errorf("jobs.Enqueue: marshal args: %w", err)
	}
	in := store.EnqueueJobInput{
		Queue:     opts.Queue,
		Kind:      opts.Kind,
		Payload:   payload,
		UniqueKey: opts.UniqueKey,
		RunAfter:  opts.RunAfter,
	}
	return q.store.EnqueueJob(ctx, in)
}

// GetStatus implements JobQueue.
func (q *jobQueue) GetStatus(ctx context.Context, id int64) (*schema.Job, error) {
	return q.store.GetJob(ctx, id)
}

// Cancel implements JobQueue.
func (q *jobQueue) Cancel(ctx context.Context, id int64) error {
	return q.store.RequestJobCancel(ctx, id)
}

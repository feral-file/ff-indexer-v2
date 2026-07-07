package jobs

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/alitto/pond/v2"
	"gorm.io/gorm"

	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/store"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

// WorkerConfig configures one poll loop and cancel watcher for a single queue name.
// Zero values are replaced by sensible defaults in NewWorker.
type WorkerConfig struct {
	// Queue is the jobs.queue value this worker services (e.g. token_index).
	Queue string
	// Concurrency is the maximum number of handler goroutines in flight at once.
	Concurrency int
	// PollInterval is how often to attempt ClaimJobs when idle.
	PollInterval time.Duration
	// BatchSize is the max rows claimed per tick (upper bound for skip-locked batch).
	BatchSize int
	// CancelInterval is how often to scan for in-flight jobs with cancel_requested and cancel ctx.
	CancelInterval time.Duration
	// MaxAttempts is the maximum number of times a job may be claimed before SweepOrphanedJobs
	// permanently fails it instead of resetting it to pending. This breaks the crash loop caused
	// by CGO/Rust SIGABRT panics (e.g. resvg feDisplacementMap): without a cap, a job that
	// crashes the process is orphaned, swept back to pending, and re-executed indefinitely.
	// Default: 3 (allows recovery from transient host pressure; terminates deterministic loops).
	MaxAttempts int
}

// Worker runs a single-queue consumer: advisory lock, startup sweep, poll loop, and cancel
// watcher, dispatching to a Registry. Persistence is entirely via store.Store (no SQL here).
//
// Reason: A dedicated worker per queue matches separate processes (core vs cgo) and relies on
// postgres FOR UPDATE SKIP LOCKED instead of in-app leases. There is no periodic heartbeat: a
// crash leaves rows as running until the next process startup sweep, which returns them to pending.
// Trade-offs: Latency is bounded by PollInterval; v1 does not add LISTEN/NOTIFY. Constraints:
// Only one process should hold the advisory lock per queue; a second worker exits without error
// so operators can colocate without fighting the same row set.
type Worker struct {
	store    store.Store
	registry *Registry
	config   WorkerConfig
	mu       sync.Mutex
	inflight map[int64]context.CancelFunc
	pool     pond.Pool
}

// NewWorker returns a worker. Store and registry must be non-nil. Empty Queue panics. Defaults:
// Concurrency=4, PollInterval=2s, BatchSize=16, CancelInterval=5s.
func NewWorker(st store.Store, reg *Registry, cfg WorkerConfig) *Worker {
	if st == nil {
		panic("jobs.NewWorker: store is nil")
	}
	if reg == nil {
		panic("jobs.NewWorker: registry is nil")
	}
	if cfg.Queue == "" {
		panic("jobs.NewWorker: Queue is empty")
	}
	if cfg.Concurrency < 1 {
		cfg.Concurrency = 4
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 2 * time.Second
	}
	if cfg.BatchSize < 1 {
		cfg.BatchSize = 16
	}
	if cfg.CancelInterval <= 0 {
		cfg.CancelInterval = 5 * time.Second
	}
	if cfg.MaxAttempts < 1 {
		cfg.MaxAttempts = 3
	}
	return &Worker{
		store:    st,
		registry: reg,
		config:   cfg,
		inflight: make(map[int64]context.CancelFunc),
	}
}

// Run acquires the per-queue session advisory lock, sweeps orphan running rows to pending, then
// runs until ctx is done: for each tick, claims work, respects Concurrency, dispatches, and
// records outcomes. If the lock is not acquired (another worker holds it), returns nil without error.
// On shutdown, in-flight handler contexts are canceled; jobs that return context.Canceled are
// rescheduled to pending if the run is stopping, or marked canceled when a user cancel was requested.
//
// If a terminal job state transition (MarkJobSucceeded, MarkJobFailed, etc.) fails to persist,
// the worker fails fast and returns the error. This allows the supervisor to restart the process,
// triggering the startup sweep to recover any orphaned running jobs.
//
// Reason: Internal worker context allows prompt cancellation of all in-flight handlers when a fatal
// error occurs or external shutdown is requested; stopPool (sync.Once + StopAndWait) drains the pool.
func (w *Worker) Run(ctx context.Context) error {
	if err := w.ctxErr(ctx); err != nil {
		return err
	}
	acquired, release, err := w.store.AcquireJobQueueLock(ctx, w.config.Queue)
	if err != nil {
		return err
	}
	if !acquired {
		return nil
	}
	defer release()

	reset, failed, err := w.store.SweepOrphanedJobs(ctx, w.config.Queue, w.config.MaxAttempts)
	if err != nil {
		return err
	}
	if reset > 0 || failed > 0 {
		logger.InfoCtx(ctx, "swept orphaned jobs on startup",
			zap.Int64("reset_to_pending", reset),
			zap.Int64("failed_crash_loop", failed),
			zap.Int("max_attempts", w.config.MaxAttempts),
		)
	}

	// Create worker pool with context
	w.pool = pond.NewPool(
		w.config.Concurrency,
		pond.WithQueueSize(w.config.BatchSize),
		pond.WithContext(ctx),
	)
	var poolStopOnce sync.Once
	stopPool := func() {
		poolStopOnce.Do(func() {
			w.pool.StopAndWait()
		})
	}
	defer stopPool()

	// runCtx is an internal worker context that we control for canceling all in-flight handlers.
	// When fatal error or external shutdown occurs, we cancel this to interrupt handlers promptly.
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()

	pollCh := time.NewTicker(w.config.PollInterval)
	defer pollCh.Stop()
	cancelCh := time.NewTicker(w.config.CancelInterval)
	defer cancelCh.Stop()

	// fatalErr captures the first terminal state persistence failure from any job execution.
	// Buffered to prevent goroutine leak if multiple jobs fail simultaneously.
	fatalErr := make(chan error, 1)

	if err := w.claimAndDispatch(runCtx, fatalErr); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			// Cancel all in-flight handlers on external shutdown.
			runCancel()
			// Drain the pool before reading fatalErr: handlers may still be persisting terminal
			// state (e.g. RescheduleJob on shutdown); otherwise we can return nil while the
			// buffered fatalErr is sent only after defers run.
			stopPool()
			select {
			case err := <-fatalErr:
				return err
			default:
				return nil
			}
		case err := <-fatalErr:
			// Terminal state persistence failure: cancel all handlers and fail-fast.
			runCancel()
			return err
		case <-pollCh.C:
			if err := w.claimAndDispatch(runCtx, fatalErr); err != nil && !errors.Is(err, context.Canceled) {
				return err
			}
		case <-cancelCh.C:
			w.flushCancelRequests(runCtx)
			w.sweepCanceledPending(runCtx)
		}
	}
}

func (w *Worker) ctxErr(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("jobs.Worker.Run: nil context")
	}
	return nil
}

// claimAndDispatch claims only as many jobs as the pool can immediately absorb and dispatches
// them to w.pool (created in Run).
//
// Reason: ClaimJobs marks rows running in the DB immediately upon claim. Without backpressure,
// every poll tick could claim up to BatchSize rows and mark them all running, while only
// Concurrency handlers actually execute — inflating the observable running count in the DB far
// beyond the configured limit. Gating claim size on pool availability (RunningWorkers +
// WaitingTasks) keeps DB running rows ≈ actual executing goroutines.
//
// Trade-offs: BatchSize still sets the pond queue buffer (WithQueueSize) as a safety valve for
// burst, but it no longer drives how many jobs are claimed per tick. Poll latency (PollInterval)
// bounds how quickly new work is picked up after capacity opens.
func (w *Worker) claimAndDispatch(ctx context.Context, fatalErr chan<- error) error {
	if err := w.ctxErr(ctx); err != nil {
		return err
	}

	// Only claim what the pool can absorb right now. RunningWorkers + WaitingTasks equals the
	// number of already-claimed jobs that have not yet completed. Claiming more would mark
	// additional rows running in the DB while they sit idle in the pond queue.
	// WaitingTasks returns uint64 but is bounded by BatchSize (a small configured int),
	// so the conversion to int cannot overflow in practice.
	inUse := int(w.pool.RunningWorkers()) + int(w.pool.WaitingTasks()) //nolint:gosec // bounded by BatchSize
	available := w.config.Concurrency - inUse
	if available <= 0 {
		return nil
	}
	// Also cap by BatchSize so the claim limit stays within the operator-configured batch bound
	// regardless of concurrency setting (e.g. concurrency=50, batch_size=16 → claim at most 16).
	if available > w.config.BatchSize {
		available = w.config.BatchSize
	}

	jobs, err := w.store.ClaimJobs(ctx, w.config.Queue, available)
	if err != nil {
		return err
	}

	for _, j := range jobs {
		if j == nil {
			continue
		}
		job := j

		w.pool.Submit(func() {
			if err := w.executeJob(ctx, job); err != nil {
				// Send the first fatal error; non-blocking if already sent.
				select {
				case fatalErr <- err:
				default:
				}
			}
		})
	}

	return nil
}

// flushCancelRequests matches DB cancel flags to in-flight handler contexts and cancels them.
// Reason: cancel_requested alone does not stop a running goroutine; the watcher bridges DB state
// to context cancellation. Trade-offs: O(inflight) ids per tick; v1 inflight is bounded by concurrency.
func (w *Worker) flushCancelRequests(ctx context.Context) {
	if err := w.ctxErr(ctx); err != nil {
		return
	}
	w.mu.Lock()
	ids := make([]int64, 0, len(w.inflight))
	byID := make(map[int64]context.CancelFunc, len(w.inflight))
	for id, c := range w.inflight {
		ids = append(ids, id)
		byID[id] = c
	}
	w.mu.Unlock()
	if len(ids) == 0 {
		return
	}
	rows, err := w.store.ListInFlightJobsWithCancelRequest(ctx, w.config.Queue, ids)
	if err != nil {
		return
	}
	for _, r := range rows {
		if r == nil {
			continue
		}
		if c, ok := byID[r.ID]; ok {
			c()
		}
	}
}

// sweepCanceledPending transitions pending jobs with cancel_requested to canceled status.
// Reason: ClaimJobs now filters out jobs with cancel_requested=true, but those rows remain
// in pending state indefinitely unless explicitly transitioned. This sweep ensures user
// cancellation intent is reflected in terminal job state.
func (w *Worker) sweepCanceledPending(ctx context.Context) {
	if err := w.ctxErr(ctx); err != nil {
		return
	}
	count, err := w.store.SweepCanceledPendingJobs(ctx, w.config.Queue)
	if err != nil {
		logger.WarnCtx(ctx, "failed to sweep canceled pending jobs", zap.Error(err))
		return
	}
	if count > 0 {
		logger.InfoCtx(ctx, "swept canceled pending jobs", zap.Int64("count", count))
	}
}

func (w *Worker) executeJob(parent context.Context, job *schema.Job) error {
	if parent == nil {
		parent = context.Background()
	}
	// Scope Sentry (and zapsentry via FromContext) to this job only, before cancel/job-id wiring.
	dispatchCtx := logger.ContextWithSentryJobHandler(parent, job.ID, job.Kind, w.config.Queue)
	workCtx, jobCancel := context.WithCancel(WithJobID(dispatchCtx, job.ID))
	defer jobCancel()
	w.addInflight(job.ID, jobCancel)
	defer w.removeInflight(job.ID)

	err := w.registry.Dispatch(workCtx, job)
	// stickyCtx carries the job Sentry hub so persistence errors after dispatch are traceable.
	return w.finishWithOutcome(context.WithoutCancel(dispatchCtx), job, err, parent)
}

func (w *Worker) addInflight(id int64, cancel context.CancelFunc) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.inflight == nil {
		w.inflight = make(map[int64]context.CancelFunc)
	}
	w.inflight[id] = cancel
}

func (w *Worker) removeInflight(id int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.inflight, id)
}

// finishWithOutcome maps handler errors to store transitions. The parent ctx is used to detect
// process shutdown vs. user- or watcher-initiated cancel.
//
// Returns an error if terminal state persistence fails (except ErrRecordNotFound), signaling that
// the worker should fail-fast to allow startup sweep to recover the orphaned running job.
func (w *Worker) finishWithOutcome(stickyCtx context.Context, job *schema.Job, err error, runCtx context.Context) error {
	id := job.ID
	if err == nil {
		if e := w.store.MarkJobSucceeded(stickyCtx, id); e != nil && !errors.Is(e, gorm.ErrRecordNotFound) {
			logger.ErrorCtx(stickyCtx, e, zap.String("operation", "MarkJobSucceeded"), zap.Int64("job_id", id))
			return fmt.Errorf("MarkJobSucceeded failed for job %d: %w", id, e)
		}
		return nil
	}
	var re *RescheduleError
	if errors.As(err, &re) {
		if e := w.store.RescheduleJob(stickyCtx, id, re.At); e != nil && !errors.Is(e, gorm.ErrRecordNotFound) {
			logger.ErrorCtx(stickyCtx, e, zap.String("operation", "RescheduleJob"), zap.Int64("job_id", id))
			return fmt.Errorf("RescheduleJob failed for job %d: %w", id, e)
		}
		return nil
	}
	if errors.Is(err, context.Canceled) {
		if runCtx.Err() != nil {
			if e := w.store.RescheduleJob(stickyCtx, id, time.Now().UTC()); e != nil && !errors.Is(e, gorm.ErrRecordNotFound) {
				logger.ErrorCtx(stickyCtx, e, zap.String("operation", "RescheduleJob"), zap.String("reason", "shutdown"), zap.Int64("job_id", id))
				return fmt.Errorf("RescheduleJob on shutdown failed for job %d: %w", id, e)
			}
			return nil
		}
		j2, ge := w.store.GetJob(stickyCtx, id)
		if ge == nil && j2 != nil && j2.CancelRequested {
			if e := w.store.MarkJobCanceled(stickyCtx, id); e != nil && !errors.Is(e, gorm.ErrRecordNotFound) {
				logger.ErrorCtx(stickyCtx, e, zap.String("operation", "MarkJobCanceled"), zap.Int64("job_id", id))
				return fmt.Errorf("MarkJobCanceled failed for job %d: %w", id, e)
			}
			return nil
		}
		if e := w.store.MarkJobFailed(stickyCtx, id, "canceled: "+err.Error()); e != nil && !errors.Is(e, gorm.ErrRecordNotFound) {
			logger.ErrorCtx(stickyCtx, e, zap.String("operation", "MarkJobFailed"), zap.String("reason", "canceled"), zap.Int64("job_id", id))
			return fmt.Errorf("MarkJobFailed on cancel failed for job %d: %w", id, e)
		}
		return nil
	}
	if e := w.store.MarkJobFailed(stickyCtx, id, err.Error()); e != nil && !errors.Is(e, gorm.ErrRecordNotFound) {
		logger.ErrorCtx(stickyCtx, e, zap.String("operation", "MarkJobFailed"), zap.Int64("job_id", id))
		return fmt.Errorf("MarkJobFailed failed for job %d: %w", id, e)
	}
	return nil
}

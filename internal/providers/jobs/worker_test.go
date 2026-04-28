package jobs_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/providers/jobs"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

func TestNewWorker_Panics(t *testing.T) {
	t.Parallel()
	st := mocks.NewMockStore(gomock.NewController(t))
	reg := jobs.NewRegistry(adapter.NewJSON())
	require.Panics(t, func() { jobs.NewWorker(nil, reg, jobs.WorkerConfig{Queue: "q"}) })
	require.Panics(t, func() { jobs.NewWorker(st, nil, jobs.WorkerConfig{Queue: "q"}) })
	require.Panics(t, func() { jobs.NewWorker(st, reg, jobs.WorkerConfig{}) })
}

func TestWorker_Run_NilContext(t *testing.T) {
	t.Parallel()
	st := mocks.NewMockStore(gomock.NewController(t))
	w := jobs.NewWorker(st, jobs.NewRegistry(adapter.NewJSON()), jobs.WorkerConfig{Queue: "q"})
	require.Error(t, w.Run(nil)) //nolint:staticcheck // intentional: exercise early ctx validation
}

func TestWorker_Run_LockNotAcquired(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	st := mocks.NewMockStore(ctrl)
	ctx := context.Background()
	st.EXPECT().AcquireJobQueueLock(ctx, "tok").Return(false, func() {}, nil)
	// Sweep must not run when the lock is not held.
	st.EXPECT().SweepOrphanedJobs(gomock.Any(), gomock.Any()).Times(0)

	w := jobs.NewWorker(st, jobs.NewRegistry(adapter.NewJSON()), jobs.WorkerConfig{Queue: "tok"})
	require.NoError(t, w.Run(ctx))
}

func TestWorker_Run_AcquireError(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	st := mocks.NewMockStore(ctrl)
	ctx := context.Background()
	want := errors.New("lock err")
	st.EXPECT().AcquireJobQueueLock(ctx, "tok").Return(false, nil, want)
	w := jobs.NewWorker(st, jobs.NewRegistry(adapter.NewJSON()), jobs.WorkerConfig{Queue: "tok"})
	require.ErrorIs(t, w.Run(ctx), want)
}

func TestWorker_Run_SweepError(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	st := mocks.NewMockStore(ctrl)
	ctx := context.Background()
	want := errors.New("sweep")
	st.EXPECT().AcquireJobQueueLock(ctx, "tok").Return(true, func() {}, nil)
	st.EXPECT().SweepOrphanedJobs(ctx, "tok").Return(int64(0), want)
	w := jobs.NewWorker(st, jobs.NewRegistry(adapter.NewJSON()), jobs.WorkerConfig{Queue: "tok"})
	require.ErrorIs(t, w.Run(ctx), want)
}

func TestWorker_Run_ClaimsJobAndMarksSucceeded(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	st := mocks.NewMockStore(ctrl)
	reg := jobs.NewRegistry(adapter.NewJSON())
	reg.Register("H", func(ctx context.Context) error { return nil })

	j := &schema.Job{ID: 1, Kind: "H", Queue: "tok", Payload: []byte("[]")}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var claimCalls int32
	st.EXPECT().AcquireJobQueueLock(gomock.Any(), "tok").Return(true, func() {}, nil)
	st.EXPECT().SweepOrphanedJobs(gomock.Any(), "tok").Return(int64(0), nil)
	st.EXPECT().ClaimJobs(gomock.Any(), "tok", gomock.Any()).AnyTimes().DoAndReturn(
		func(context.Context, string, int) ([]*schema.Job, error) {
			if atomic.AddInt32(&claimCalls, 1) == 1 {
				return []*schema.Job{j}, nil
			}
			return nil, nil
		},
	)
	done := make(chan struct{})
	st.EXPECT().MarkJobSucceeded(gomock.Any(), int64(1)).Return(nil).Do(func(context.Context, int64) { close(done) })
	st.EXPECT().ListInFlightJobsWithCancelRequest(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

	w := jobs.NewWorker(st, reg, jobs.WorkerConfig{Queue: "tok", PollInterval: 50 * time.Millisecond, BatchSize: 4, CancelInterval: time.Hour})
	errC := goRun(w, ctx)
	<-done
	cancel()
	require.NoError(t, <-errC)
}

// TestWorker_Run_HandlerReschedules covers ErrReschedule → RescheduleJob on the public Run path.
func TestWorker_Run_HandlerReschedules(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	st := mocks.NewMockStore(ctrl)
	reg := jobs.NewRegistry(adapter.NewJSON())
	when := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	reg.Register("R", func(context.Context) error { return jobs.ErrReschedule(when) })

	j := &schema.Job{ID: 2, Kind: "R", Queue: "tok", Payload: []byte("[]")}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var claimCalls int32
	st.EXPECT().AcquireJobQueueLock(gomock.Any(), "tok").Return(true, func() {}, nil)
	st.EXPECT().SweepOrphanedJobs(gomock.Any(), "tok").Return(int64(0), nil)
	st.EXPECT().ClaimJobs(gomock.Any(), "tok", gomock.Any()).AnyTimes().DoAndReturn(
		func(context.Context, string, int) ([]*schema.Job, error) {
			if atomic.AddInt32(&claimCalls, 1) == 1 {
				return []*schema.Job{j}, nil
			}
			return nil, nil
		},
	)
	done := make(chan struct{})
	st.EXPECT().RescheduleJob(gomock.Any(), int64(2), gomock.Any()).Return(nil).Do(func(context.Context, int64, time.Time) { close(done) })
	st.EXPECT().ListInFlightJobsWithCancelRequest(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

	w := jobs.NewWorker(st, reg, jobs.WorkerConfig{Queue: "tok", PollInterval: 50 * time.Millisecond, BatchSize: 4, CancelInterval: time.Hour})
	errC := goRun(w, ctx)
	<-done
	cancel()
	require.NoError(t, <-errC)
}

// TestWorker_Run_HandlerFails covers non-reschedule errors → MarkJobFailed.
func TestWorker_Run_HandlerFails(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	st := mocks.NewMockStore(ctrl)
	reg := jobs.NewRegistry(adapter.NewJSON())
	reg.Register("E", func(context.Context) error { return errors.New("handler boom") })

	j := &schema.Job{ID: 3, Kind: "E", Queue: "tok", Payload: []byte("[]")}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var claimCalls int32
	st.EXPECT().AcquireJobQueueLock(gomock.Any(), "tok").Return(true, func() {}, nil)
	st.EXPECT().SweepOrphanedJobs(gomock.Any(), "tok").Return(int64(0), nil)
	st.EXPECT().ClaimJobs(gomock.Any(), "tok", gomock.Any()).AnyTimes().DoAndReturn(
		func(context.Context, string, int) ([]*schema.Job, error) {
			if atomic.AddInt32(&claimCalls, 1) == 1 {
				return []*schema.Job{j}, nil
			}
			return nil, nil
		},
	)
	done := make(chan struct{})
	st.EXPECT().MarkJobFailed(gomock.Any(), int64(3), "handler boom").Return(nil).Do(func(context.Context, int64, string) { close(done) })
	st.EXPECT().ListInFlightJobsWithCancelRequest(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

	w := jobs.NewWorker(st, reg, jobs.WorkerConfig{Queue: "tok", PollInterval: 50 * time.Millisecond, BatchSize: 4, CancelInterval: time.Hour})
	errC := goRun(w, ctx)
	<-done
	cancel()
	require.NoError(t, <-errC)
}

// TestWorker_Run_CancelObserverMarksCanceled exercises cancel tick + ListInFlight with cancel_requested → context cancel → MarkJobCanceled.
func TestWorker_Run_CancelObserverMarksCanceled(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	st := mocks.NewMockStore(ctrl)
	reg := jobs.NewRegistry(adapter.NewJSON())
	started := make(chan struct{})
	reg.Register("B", func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})

	j := &schema.Job{ID: 4, Kind: "B", Queue: "tok", Payload: []byte("[]")}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var claimCalls int32
	st.EXPECT().AcquireJobQueueLock(gomock.Any(), "tok").Return(true, func() {}, nil)
	st.EXPECT().SweepOrphanedJobs(gomock.Any(), "tok").Return(int64(0), nil)
	st.EXPECT().ClaimJobs(gomock.Any(), "tok", gomock.Any()).AnyTimes().DoAndReturn(
		func(context.Context, string, int) ([]*schema.Job, error) {
			if atomic.AddInt32(&claimCalls, 1) == 1 {
				return []*schema.Job{j}, nil
			}
			return nil, nil
		},
	)
	st.EXPECT().ListInFlightJobsWithCancelRequest(gomock.Any(), "tok", gomock.Any()).AnyTimes().DoAndReturn(
		func(_ context.Context, _ string, ids []int64) ([]*schema.Job, error) {
			if len(ids) == 0 {
				return nil, nil
			}
			// In-flight: report cancel so the worker cancels handler ctx.
			return []*schema.Job{{ID: ids[0], CancelRequested: true}}, nil
		},
	)
	out := make(chan struct{})
	st.EXPECT().GetJob(gomock.Any(), int64(4)).Return(&schema.Job{ID: 4, CancelRequested: true}, nil).AnyTimes()
	st.EXPECT().MarkJobCanceled(gomock.Any(), int64(4)).Return(nil).Do(func(context.Context, int64) { close(out) })

	w := jobs.NewWorker(st, reg, jobs.WorkerConfig{Queue: "tok", PollInterval: time.Hour, BatchSize: 4, CancelInterval: 20 * time.Millisecond})
	errC := goRun(w, ctx)
	<-started
	<-out
	cancel()
	require.NoError(t, <-errC)
}

// TestWorker_Run_OnShutdownReschedules covers handler seeing context.Canceled from Run stopping → RescheduleJob.
func TestWorker_Run_OnShutdownReschedules(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	st := mocks.NewMockStore(ctrl)
	reg := jobs.NewRegistry(adapter.NewJSON())
	started := make(chan struct{})
	reg.Register("S", func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		return context.Canceled
	})

	j := &schema.Job{ID: 5, Kind: "S", Queue: "tok", Payload: []byte("[]")}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var claimCalls int32
	st.EXPECT().AcquireJobQueueLock(gomock.Any(), "tok").Return(true, func() {}, nil)
	st.EXPECT().SweepOrphanedJobs(gomock.Any(), "tok").Return(int64(0), nil)
	st.EXPECT().ClaimJobs(gomock.Any(), "tok", gomock.Any()).AnyTimes().DoAndReturn(
		func(context.Context, string, int) ([]*schema.Job, error) {
			if atomic.AddInt32(&claimCalls, 1) == 1 {
				return []*schema.Job{j}, nil
			}
			return nil, nil
		},
	)
	// ListInFlight: never return cancel for this test (user-initiated path not taken).
	st.EXPECT().ListInFlightJobsWithCancelRequest(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	sched := make(chan struct{})
	st.EXPECT().RescheduleJob(gomock.Any(), int64(5), gomock.Any()).Return(nil).Do(func(context.Context, int64, time.Time) { close(sched) })

	w := jobs.NewWorker(st, reg, jobs.WorkerConfig{Queue: "tok", PollInterval: time.Hour, BatchSize: 4, CancelInterval: time.Hour})
	errC := goRun(w, ctx)
	<-started
	cancel()
	<-sched
	require.NoError(t, <-errC)
}

// TestWorker_Run_MarkJobSucceededFails_WorkerExits covers handler success + MarkJobSucceeded error → worker exits.
func TestWorker_Run_MarkJobSucceededFails_WorkerExits(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	st := mocks.NewMockStore(ctrl)
	reg := jobs.NewRegistry(adapter.NewJSON())
	reg.Register("H", func(ctx context.Context) error { return nil })

	j := &schema.Job{ID: 6, Kind: "H", Queue: "tok", Payload: []byte("[]")}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var claimCalls int32
	st.EXPECT().AcquireJobQueueLock(gomock.Any(), "tok").Return(true, func() {}, nil)
	st.EXPECT().SweepOrphanedJobs(gomock.Any(), "tok").Return(int64(0), nil)
	st.EXPECT().ClaimJobs(gomock.Any(), "tok", gomock.Any()).AnyTimes().DoAndReturn(
		func(context.Context, string, int) ([]*schema.Job, error) {
			if atomic.AddInt32(&claimCalls, 1) == 1 {
				return []*schema.Job{j}, nil
			}
			return nil, nil
		},
	)
	persistErr := errors.New("db write failed")
	st.EXPECT().MarkJobSucceeded(gomock.Any(), int64(6)).Return(persistErr)
	st.EXPECT().ListInFlightJobsWithCancelRequest(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

	w := jobs.NewWorker(st, reg, jobs.WorkerConfig{Queue: "tok", PollInterval: 50 * time.Millisecond, BatchSize: 4, CancelInterval: time.Hour})
	err := w.Run(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "MarkJobSucceeded failed for job 6")
}

// TestWorker_Run_MarkJobFailedFails_WorkerExits covers handler error + MarkJobFailed error → worker exits.
func TestWorker_Run_MarkJobFailedFails_WorkerExits(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	st := mocks.NewMockStore(ctrl)
	reg := jobs.NewRegistry(adapter.NewJSON())
	reg.Register("E", func(context.Context) error { return errors.New("handler boom") })

	j := &schema.Job{ID: 7, Kind: "E", Queue: "tok", Payload: []byte("[]")}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var claimCalls int32
	st.EXPECT().AcquireJobQueueLock(gomock.Any(), "tok").Return(true, func() {}, nil)
	st.EXPECT().SweepOrphanedJobs(gomock.Any(), "tok").Return(int64(0), nil)
	st.EXPECT().ClaimJobs(gomock.Any(), "tok", gomock.Any()).AnyTimes().DoAndReturn(
		func(context.Context, string, int) ([]*schema.Job, error) {
			if atomic.AddInt32(&claimCalls, 1) == 1 {
				return []*schema.Job{j}, nil
			}
			return nil, nil
		},
	)
	persistErr := errors.New("db write failed")
	st.EXPECT().MarkJobFailed(gomock.Any(), int64(7), "handler boom").Return(persistErr)
	st.EXPECT().ListInFlightJobsWithCancelRequest(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

	w := jobs.NewWorker(st, reg, jobs.WorkerConfig{Queue: "tok", PollInterval: 50 * time.Millisecond, BatchSize: 4, CancelInterval: time.Hour})
	err := w.Run(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "MarkJobFailed failed for job 7")
}

// TestWorker_Run_RescheduleJobFails_WorkerExits covers ErrReschedule + RescheduleJob error → worker exits.
func TestWorker_Run_RescheduleJobFails_WorkerExits(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	st := mocks.NewMockStore(ctrl)
	reg := jobs.NewRegistry(adapter.NewJSON())
	when := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	reg.Register("R", func(context.Context) error { return jobs.ErrReschedule(when) })

	j := &schema.Job{ID: 8, Kind: "R", Queue: "tok", Payload: []byte("[]")}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var claimCalls int32
	st.EXPECT().AcquireJobQueueLock(gomock.Any(), "tok").Return(true, func() {}, nil)
	st.EXPECT().SweepOrphanedJobs(gomock.Any(), "tok").Return(int64(0), nil)
	st.EXPECT().ClaimJobs(gomock.Any(), "tok", gomock.Any()).AnyTimes().DoAndReturn(
		func(context.Context, string, int) ([]*schema.Job, error) {
			if atomic.AddInt32(&claimCalls, 1) == 1 {
				return []*schema.Job{j}, nil
			}
			return nil, nil
		},
	)
	persistErr := errors.New("db write failed")
	st.EXPECT().RescheduleJob(gomock.Any(), int64(8), gomock.Any()).Return(persistErr)
	st.EXPECT().ListInFlightJobsWithCancelRequest(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

	w := jobs.NewWorker(st, reg, jobs.WorkerConfig{Queue: "tok", PollInterval: 50 * time.Millisecond, BatchSize: 4, CancelInterval: time.Hour})
	err := w.Run(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "RescheduleJob failed for job 8")
}

// TestWorker_Run_ShutdownRescheduleFails_WorkerExits covers shutdown + RescheduleJob error → worker exits.
func TestWorker_Run_ShutdownRescheduleFails_WorkerExits(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	st := mocks.NewMockStore(ctrl)
	reg := jobs.NewRegistry(adapter.NewJSON())
	started := make(chan struct{})
	reg.Register("S", func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		return context.Canceled
	})

	j := &schema.Job{ID: 9, Kind: "S", Queue: "tok", Payload: []byte("[]")}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var claimCalls int32
	st.EXPECT().AcquireJobQueueLock(gomock.Any(), "tok").Return(true, func() {}, nil)
	st.EXPECT().SweepOrphanedJobs(gomock.Any(), "tok").Return(int64(0), nil)
	st.EXPECT().ClaimJobs(gomock.Any(), "tok", gomock.Any()).AnyTimes().DoAndReturn(
		func(context.Context, string, int) ([]*schema.Job, error) {
			if atomic.AddInt32(&claimCalls, 1) == 1 {
				return []*schema.Job{j}, nil
			}
			return nil, nil
		},
	)
	st.EXPECT().ListInFlightJobsWithCancelRequest(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	persistErr := errors.New("db write failed")
	st.EXPECT().RescheduleJob(gomock.Any(), int64(9), gomock.Any()).Return(persistErr)

	w := jobs.NewWorker(st, reg, jobs.WorkerConfig{Queue: "tok", PollInterval: time.Hour, BatchSize: 4, CancelInterval: time.Hour})
	errC := goRun(w, ctx)
	<-started
	cancel()
	err := <-errC
	require.Error(t, err)
	require.Contains(t, err.Error(), "RescheduleJob on shutdown failed for job 9")
}

// TestWorker_Run_FatalErrorCancelsBlockingHandlers covers the critical fail-fast scenario:
// when one job's terminal state transition fails, the worker cancels all in-flight handlers
// and exits promptly rather than waiting indefinitely for them to complete naturally.
func TestWorker_Run_FatalErrorCancelsBlockingHandlers(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	st := mocks.NewMockStore(ctrl)
	reg := jobs.NewRegistry(adapter.NewJSON())

	// Job 1: blocks on context until canceled.
	job1Started := make(chan struct{})
	job1Canceled := make(chan struct{})
	reg.Register("BlockingJob", func(ctx context.Context) error {
		close(job1Started)
		<-ctx.Done()
		close(job1Canceled)
		return ctx.Err()
	})

	// Job 2: succeeds immediately, but MarkJobSucceeded fails (fatal error).
	reg.Register("QuickJob", func(ctx context.Context) error {
		return nil
	})

	job1 := &schema.Job{ID: 100, Kind: "BlockingJob", Queue: "tok", Payload: []byte("[]")}
	job2 := &schema.Job{ID: 101, Kind: "QuickJob", Queue: "tok", Payload: []byte("[]")}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var claimCalls int32
	st.EXPECT().AcquireJobQueueLock(gomock.Any(), "tok").Return(true, func() {}, nil)
	st.EXPECT().SweepOrphanedJobs(gomock.Any(), "tok").Return(int64(0), nil)
	st.EXPECT().ClaimJobs(gomock.Any(), "tok", gomock.Any()).AnyTimes().DoAndReturn(
		func(context.Context, string, int) ([]*schema.Job, error) {
			if atomic.AddInt32(&claimCalls, 1) == 1 {
				// First claim: return both jobs.
				return []*schema.Job{job1, job2}, nil
			}
			// After jobs start, no more claims (ctx will be canceled).
			return nil, nil
		},
	)

	// Job 2 succeeds but MarkJobSucceeded fails (fatal error).
	persistErr := errors.New("db write failed")
	st.EXPECT().MarkJobSucceeded(gomock.Any(), int64(101)).Return(persistErr)

	// Job 1 gets rescheduled on shutdown (because its context was canceled by fail-fast).
	st.EXPECT().RescheduleJob(gomock.Any(), int64(100), gomock.Any()).Return(nil)
	st.EXPECT().ListInFlightJobsWithCancelRequest(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

	w := jobs.NewWorker(st, reg, jobs.WorkerConfig{Queue: "tok", PollInterval: 50 * time.Millisecond, BatchSize: 4, CancelInterval: time.Hour})

	// Run worker in background.
	errC := make(chan error, 1)
	go func() { errC <- w.Run(ctx) }()

	// Wait for blocking job to start.
	<-job1Started

	// Now wait for the blocking job to be canceled (by fail-fast).
	// This should happen promptly after job2's fatal error.
	select {
	case <-job1Canceled:
		// Good: blocking handler was canceled.
	case <-time.After(2 * time.Second):
		t.Fatal("Blocking handler was not canceled promptly on fatal error")
	}

	// Wait for worker to exit with fatal error.
	select {
	case err := <-errC:
		require.Error(t, err)
		require.Contains(t, err.Error(), "MarkJobSucceeded failed for job 101")
	case <-time.After(2 * time.Second):
		t.Fatal("Worker did not exit promptly after fatal error")
	}
}

// TestWorker_Run_MarkJobCanceledFails_WorkerExits covers user cancel + MarkJobCanceled error → worker exits.
func TestWorker_Run_MarkJobCanceledFails_WorkerExits(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	st := mocks.NewMockStore(ctrl)
	reg := jobs.NewRegistry(adapter.NewJSON())
	started := make(chan struct{})
	reg.Register("B", func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})

	j := &schema.Job{ID: 10, Kind: "B", Queue: "tok", Payload: []byte("[]")}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var claimCalls int32
	st.EXPECT().AcquireJobQueueLock(gomock.Any(), "tok").Return(true, func() {}, nil)
	st.EXPECT().SweepOrphanedJobs(gomock.Any(), "tok").Return(int64(0), nil)
	st.EXPECT().ClaimJobs(gomock.Any(), "tok", gomock.Any()).AnyTimes().DoAndReturn(
		func(context.Context, string, int) ([]*schema.Job, error) {
			if atomic.AddInt32(&claimCalls, 1) == 1 {
				return []*schema.Job{j}, nil
			}
			return nil, nil
		},
	)
	st.EXPECT().ListInFlightJobsWithCancelRequest(gomock.Any(), "tok", gomock.Any()).AnyTimes().DoAndReturn(
		func(_ context.Context, _ string, ids []int64) ([]*schema.Job, error) {
			if len(ids) == 0 {
				return nil, nil
			}
			return []*schema.Job{{ID: ids[0], CancelRequested: true}}, nil
		},
	)
	st.EXPECT().GetJob(gomock.Any(), int64(10)).Return(&schema.Job{ID: 10, CancelRequested: true}, nil).AnyTimes()
	persistErr := errors.New("db write failed")
	st.EXPECT().MarkJobCanceled(gomock.Any(), int64(10)).Return(persistErr)

	w := jobs.NewWorker(st, reg, jobs.WorkerConfig{Queue: "tok", PollInterval: time.Hour, BatchSize: 4, CancelInterval: 20 * time.Millisecond})
	errC := goRun(w, ctx)
	<-started
	err := <-errC
	require.Error(t, err)
	require.Contains(t, err.Error(), "MarkJobCanceled failed for job 10")
}

func goRun(w *jobs.Worker, ctx context.Context) <-chan error {
	ch := make(chan error, 1)
	go func() { ch <- w.Run(ctx) }()
	return ch
}

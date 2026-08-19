package executor_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/api/shared/dto"
	"github.com/feral-file/ff-indexer-v2/internal/api/shared/executor"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/store"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

// throttleTestAddress is pre-normalized (EIP-55) because the executor normalizes
// inputs before store lookups, and the mock expectations match on the argument.
var throttleTestAddress = domain.NormalizeAddress("0x00000000000000000000000000000000000000aa")

// addressThrottleFixture wires an executor with mock store, queue, and clock so
// the gate's time comparisons are deterministic.
type addressThrottleFixture struct {
	Exec  executor.Executor
	Store *mocks.MockStore
	JQ    *mocks.MockJobQueue
	Clock *mocks.MockClock

	lockHolds atomic.Int64
}

// initTestLoggerOnce guards the global logger swap: parallel tests racing
// logger.Initialize is a test-harness data race, not a production one.
var initTestLoggerOnce sync.Once

func newAddressThrottleFixture(t *testing.T, throttle executor.AddressIndexingThrottle) *addressThrottleFixture {
	t.Helper()
	initTestLoggerOnce.Do(func() { _ = logger.Initialize(logger.Config{Debug: true}) })
	ctrl := gomock.NewController(t)
	mockStore := mocks.NewMockStore(ctrl)
	mockJQ := mocks.NewMockJobQueue(ctrl)
	mockClock := mocks.NewMockClock(ctrl)
	exec := executor.NewExecutor(
		mockStore,
		mockJQ,
		"token_index",
		mocks.NewMockBlacklistRegistry(ctrl),
		adapter.NewJSON(),
		mockClock,
		domain.Chain("tezos:mainnet"),
		domain.Chain("eip155:1"),
		throttle,
	)
	// Every trigger path serializes inside the transactional per-address lock;
	// stub it to execute the gated body against the same mock store (the real
	// implementation passes a transaction-bound Store).
	f := &addressThrottleFixture{Exec: exec, Store: mockStore, JQ: mockJQ, Clock: mockClock}
	mockStore.EXPECT().
		WithAddressTriggerLock(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, _ domain.Chain, fn func(store.Store) error) error {
			f.lockHolds.Add(1)
			return fn(mockStore)
		}).
		AnyTimes()
	return f
}

// expectEnqueueSuccess registers the expectations of the un-throttled path:
// no active job, a queue enqueue, and the tracking-row insert.
func (f *addressThrottleFixture) expectEnqueueSuccess(jobID int64) {
	f.Store.EXPECT().
		GetActiveIndexingJobForAddress(gomock.Any(), throttleTestAddress, domain.Chain("eip155:1")).
		Return(nil, nil)
	f.Store.EXPECT().
		EnqueueJob(gomock.Any(), gomock.Any()).
		Return(&schema.Job{ID: jobID}, true, nil)
	f.Store.EXPECT().
		CreateAddressIndexingJob(gomock.Any(), gomock.Any()).
		Return(nil)
}

// TestTriggerAddressIndexing_ThrottledByCooldown pins the gate: an address whose
// scan completed inside the cooldown window gets no new job — the strict queue
// mock fails the test on any enqueue — and the response carries the last job
// plus the earliest retry time.
func TestTriggerAddressIndexing_ThrottledByCooldown(t *testing.T) {
	t.Parallel()
	f := newAddressThrottleFixture(t, executor.AddressIndexingThrottle{SuccessCooldown: time.Hour})

	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	completedAt := now.Add(-10 * time.Minute)

	f.Store.EXPECT().
		GetActiveIndexingJobForAddress(gomock.Any(), throttleTestAddress, domain.Chain("eip155:1")).
		Return(nil, nil)
	f.Store.EXPECT().
		GetAddressIndexingThrottleState(gomock.Any(), throttleTestAddress, domain.Chain("eip155:1")).
		Return(&store.AddressIndexingThrottleState{
			LatestTerminal: &schema.AddressIndexingJob{
				Status:      schema.IndexingJobStatusCompleted,
				JobID:       7,
				CompletedAt: &completedAt,
			},
		}, nil)
	f.Clock.EXPECT().Now().Return(now)

	resp, err := f.Exec.TriggerAddressIndexing(context.Background(), []string{throttleTestAddress})
	require.NoError(t, err)
	require.Len(t, resp.Jobs, 1)
	job := resp.Jobs[0]
	require.True(t, job.Throttled)
	require.Equal(t, int64(7), job.JobID, "throttled response must reference the last finished job")
	require.NotNil(t, job.RetryAt)
	require.Equal(t, completedAt.Add(time.Hour), *job.RetryAt)
}

// TestTriggerAddressIndexing_CooldownElapsedEnqueues pins the other side: once
// the window has passed, a new job is enqueued normally.
func TestTriggerAddressIndexing_CooldownElapsedEnqueues(t *testing.T) {
	t.Parallel()
	f := newAddressThrottleFixture(t, executor.AddressIndexingThrottle{SuccessCooldown: time.Hour})

	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	completedAt := now.Add(-2 * time.Hour)

	f.Store.EXPECT().
		GetAddressIndexingThrottleState(gomock.Any(), throttleTestAddress, domain.Chain("eip155:1")).
		Return(&store.AddressIndexingThrottleState{
			LatestTerminal: &schema.AddressIndexingJob{
				Status:      schema.IndexingJobStatusCompleted,
				JobID:       7,
				CompletedAt: &completedAt,
			},
		}, nil)
	f.Clock.EXPECT().Now().Return(now)
	f.expectEnqueueSuccess(8)

	resp, err := f.Exec.TriggerAddressIndexing(context.Background(), []string{throttleTestAddress})
	require.NoError(t, err)
	require.Len(t, resp.Jobs, 1)
	require.False(t, resp.Jobs[0].Throttled)
	require.Equal(t, int64(8), resp.Jobs[0].JobID)
}

// TestTriggerAddressIndexing_ThrottledByFailureBackoff pins the failure regime:
// a repeatedly failing address is inside its exponential window and gets no job.
func TestTriggerAddressIndexing_ThrottledByFailureBackoff(t *testing.T) {
	t.Parallel()
	f := newAddressThrottleFixture(t, executor.AddressIndexingThrottle{
		FailureBackoffBase: 30 * time.Minute,
		FailureBackoffCap:  24 * time.Hour,
	})

	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	failedAt := now.Add(-time.Hour) // streak 3 -> 30m * 4 = 2h window, 1h remains

	f.Store.EXPECT().
		GetActiveIndexingJobForAddress(gomock.Any(), throttleTestAddress, domain.Chain("eip155:1")).
		Return(nil, nil)
	f.Store.EXPECT().
		GetAddressIndexingThrottleState(gomock.Any(), throttleTestAddress, domain.Chain("eip155:1")).
		Return(&store.AddressIndexingThrottleState{
			LatestTerminal: &schema.AddressIndexingJob{
				Status:   schema.IndexingJobStatusFailed,
				JobID:    9,
				FailedAt: &failedAt,
			},
			ConsecutiveFailures: 3,
		}, nil)
	f.Clock.EXPECT().Now().Return(now)

	resp, err := f.Exec.TriggerAddressIndexing(context.Background(), []string{throttleTestAddress})
	require.NoError(t, err)
	require.Len(t, resp.Jobs, 1)
	require.True(t, resp.Jobs[0].Throttled)
	require.Equal(t, failedAt.Add(2*time.Hour), *resp.Jobs[0].RetryAt)
}

// TestTriggerAddressIndexing_ThrottleDisabledSkipsStateQuery pins the disabled
// fast path: with a zero-value throttle the gate must not even query throttle
// state (no GetAddressIndexingThrottleState expectation on the strict mock).
func TestTriggerAddressIndexing_ThrottleDisabledSkipsStateQuery(t *testing.T) {
	t.Parallel()
	f := newAddressThrottleFixture(t, executor.AddressIndexingThrottle{})

	f.expectEnqueueSuccess(11)

	resp, err := f.Exec.TriggerAddressIndexing(context.Background(), []string{throttleTestAddress})
	require.NoError(t, err)
	require.Len(t, resp.Jobs, 1)
	require.False(t, resp.Jobs[0].Throttled)
}

// TestTriggerAddressIndexing_ActiveJobWinsOverThrottle pins the gate ordering:
// an active (running) job answers the request before the throttle is consulted,
// so the response is the active job, not a throttle verdict.
func TestTriggerAddressIndexing_ActiveJobWinsOverThrottle(t *testing.T) {
	t.Parallel()
	f := newAddressThrottleFixture(t, executor.AddressIndexingThrottle{SuccessCooldown: time.Hour})

	f.Store.EXPECT().
		GetActiveIndexingJobForAddress(gomock.Any(), throttleTestAddress, domain.Chain("eip155:1")).
		Return(&schema.AddressIndexingJob{
			Status:     schema.IndexingJobStatusRunning,
			JobID:      13,
			WorkflowID: "13",
		}, nil)
	f.Store.EXPECT().
		GetJob(gomock.Any(), int64(13)).
		Return(&schema.Job{ID: 13, Status: schema.JobStatusRunning}, nil)

	resp, err := f.Exec.TriggerAddressIndexing(context.Background(), []string{throttleTestAddress})
	require.NoError(t, err)
	require.Len(t, resp.Jobs, 1)
	require.False(t, resp.Jobs[0].Throttled)
	require.Equal(t, int64(13), resp.Jobs[0].JobID)
}

// TestTriggerAddressIndexing_TrackingFailureRollsBackAndErrors pins the
// durability contract on the API path: if the address_indexing_jobs tracking
// row cannot be created for a job this request enqueued, the request fails —
// and because gates and enqueue share the lock transaction, the returned error
// rolls the enqueue back too (no cancel dance, no untracked scan). The strict
// mock proves no RequestJobCancel is attempted.
func TestTriggerAddressIndexing_TrackingFailureRollsBackAndErrors(t *testing.T) {
	t.Parallel()
	f := newAddressThrottleFixture(t, executor.AddressIndexingThrottle{})

	f.Store.EXPECT().
		GetActiveIndexingJobForAddress(gomock.Any(), throttleTestAddress, domain.Chain("eip155:1")).
		Return(nil, nil)
	f.Store.EXPECT().
		EnqueueJob(gomock.Any(), gomock.Any()).
		Return(&schema.Job{ID: 21}, true, nil)
	f.Store.EXPECT().
		CreateAddressIndexingJob(gomock.Any(), gomock.Any()).
		Return(errors.New("db unavailable"))

	resp, err := f.Exec.TriggerAddressIndexing(context.Background(), []string{throttleTestAddress})
	require.Error(t, err)
	require.Nil(t, resp)
}

// TestTriggerAddressIndexing_DoomedDedupJobIsSweptAndReplaced pins round-4 F1:
// when Enqueue deduplicates onto a pending job that is already
// cancel-requested (doomed — the sweeper will terminate it), the trigger must
// not report it as started. Instead the pending cancellation is applied
// immediately (freeing the unique key) and replacement work is enqueued.
func TestTriggerAddressIndexing_DoomedDedupJobIsSweptAndReplaced(t *testing.T) {
	t.Parallel()
	f := newAddressThrottleFixture(t, executor.AddressIndexingThrottle{})

	f.Store.EXPECT().
		GetActiveIndexingJobForAddress(gomock.Any(), throttleTestAddress, domain.Chain("eip155:1")).
		Return(nil, nil)
	first := f.Store.EXPECT().
		EnqueueJob(gomock.Any(), gomock.Any()).
		Return(&schema.Job{ID: 30, Status: schema.JobStatusPending, CancelRequested: true}, false, nil)
	sweep := f.Store.EXPECT().
		SweepCanceledPendingJobs(gomock.Any(), "token_index").
		Return(int64(1), nil).
		After(first)
	f.Store.EXPECT().
		EnqueueJob(gomock.Any(), gomock.Any()).
		Return(&schema.Job{ID: 31, Status: schema.JobStatusPending}, true, nil).
		After(sweep)
	f.Store.EXPECT().
		CreateAddressIndexingJob(gomock.Any(), gomock.Any()).
		Return(nil)

	resp, err := f.Exec.TriggerAddressIndexing(context.Background(), []string{throttleTestAddress})
	require.NoError(t, err)
	require.Len(t, resp.Jobs, 1)
	require.Equal(t, int64(31), resp.Jobs[0].JobID, "the replacement job must be reported, not the doomed one")
	require.False(t, resp.Jobs[0].Throttled)
}

// TestTriggerAddressIndexing_DedupTrackingFailureDoesNotCancel pins the other
// half of round-4 F1: a tracking-write failure against a DEDUPLICATED job must
// not cancel that job — it belongs to a concurrent trigger, and its tracking
// row becomes durable when the worker claims it (which fails the job until the
// row exists). The strict mock has no RequestJobCancel expectation, and the
// existing job is still reported.
func TestTriggerAddressIndexing_DedupTrackingFailureDoesNotCancel(t *testing.T) {
	t.Parallel()
	f := newAddressThrottleFixture(t, executor.AddressIndexingThrottle{})

	f.Store.EXPECT().
		GetActiveIndexingJobForAddress(gomock.Any(), throttleTestAddress, domain.Chain("eip155:1")).
		Return(nil, nil)
	f.Store.EXPECT().
		EnqueueJob(gomock.Any(), gomock.Any()).
		Return(&schema.Job{ID: 40, Status: schema.JobStatusPending}, false, nil)
	f.Store.EXPECT().
		CreateAddressIndexingJob(gomock.Any(), gomock.Any()).
		Return(errors.New("db unavailable"))

	resp, err := f.Exec.TriggerAddressIndexing(context.Background(), []string{throttleTestAddress})
	require.NoError(t, err, "a dedup tracking failure must not fail the request")
	require.Len(t, resp.Jobs, 1)
	require.Equal(t, int64(40), resp.Jobs[0].JobID)
}

// TestTriggerAddressIndexing_ConcurrentTriggersSerializedByLock covers round-6
// F1's exact scenario: two concurrent triggers for one address, where the first
// one's scan reaches terminal completion before the second's gate would have
// run unserialized. The advisory lock (a real mutex here) forces the second
// trigger's gate reads to happen after the first finishes, so it observes the
// fresh terminal state and is throttled — exactly one job is ever enqueued
// (pinned by Times(1) on the strict queue mock).
func TestTriggerAddressIndexing_ConcurrentTriggersSerializedByLock(t *testing.T) {
	t.Parallel()
	initTestLoggerOnce.Do(func() { _ = logger.Initialize(logger.Config{Debug: true}) })

	ctrl := gomock.NewController(t)
	mockStore := mocks.NewMockStore(ctrl)
	mockJQ := mocks.NewMockJobQueue(ctrl)
	mockClock := mocks.NewMockClock(ctrl)
	exec := executor.NewExecutor(
		mockStore, mockJQ, "token_index", mocks.NewMockBlacklistRegistry(ctrl),
		adapter.NewJSON(), mockClock,
		domain.Chain("tezos:mainnet"), domain.Chain("eip155:1"),
		executor.AddressIndexingThrottle{SuccessCooldown: time.Hour},
	)

	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	mockClock.EXPECT().Now().Return(now).AnyTimes()

	// The lock is a real mutex; shared state below simulates the DB: the first
	// trigger's scan completes (terminal row written) while it still holds the
	// lock, before the second trigger's gates can run.
	var mu sync.Mutex
	var terminal *schema.AddressIndexingJob
	mockStore.EXPECT().
		WithAddressTriggerLock(gomock.Any(), throttleTestAddress, domain.Chain("eip155:1"), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, _ domain.Chain, fn func(store.Store) error) error {
			mu.Lock()
			defer mu.Unlock()
			return fn(mockStore)
		}).
		Times(2)
	mockStore.EXPECT().
		GetActiveIndexingJobForAddress(gomock.Any(), throttleTestAddress, domain.Chain("eip155:1")).
		Return(nil, nil).
		Times(2)
	mockStore.EXPECT().
		GetAddressIndexingThrottleState(gomock.Any(), throttleTestAddress, domain.Chain("eip155:1")).
		DoAndReturn(func(context.Context, string, domain.Chain) (*store.AddressIndexingThrottleState, error) {
			return &store.AddressIndexingThrottleState{LatestTerminal: terminal}, nil
		}).
		Times(2)
	// Exactly ONE enqueue across both triggers: the second must be throttled.
	mockStore.EXPECT().
		EnqueueJob(gomock.Any(), gomock.Any()).
		Return(&schema.Job{ID: 50, Status: schema.JobStatusPending}, true, nil).
		Times(1)
	mockStore.EXPECT().
		CreateAddressIndexingJob(gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, store.CreateAddressIndexingJobInput) error {
			// Simulate the enqueued scan racing to completion inside the first
			// trigger's critical section — before the second trigger's gates run.
			completedAt := now.Add(-time.Minute)
			terminal = &schema.AddressIndexingJob{
				Status:      schema.IndexingJobStatusCompleted,
				JobID:       50,
				CompletedAt: &completedAt,
			}
			return nil
		}).
		Times(1)

	var wg sync.WaitGroup
	results := make([]*dto.TriggerAddressIndexingResponse, 2)
	errs := make([]error, 2)
	for i := range 2 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx], errs[idx] = exec.TriggerAddressIndexing(context.Background(), []string{throttleTestAddress})
		}(i)
	}
	wg.Wait()

	var started, throttled int
	for i := range 2 {
		require.NoError(t, errs[i])
		require.Len(t, results[i].Jobs, 1)
		if results[i].Jobs[0].Throttled {
			throttled++
			require.NotNil(t, results[i].Jobs[0].RetryAt)
		} else {
			started++
			require.Equal(t, int64(50), results[i].Jobs[0].JobID)
		}
	}
	require.Equal(t, 1, started, "exactly one trigger may start a scan")
	require.Equal(t, 1, throttled, "the serialized second trigger must observe the terminal state and throttle")
}

// TestTriggerAddressIndexing_DoomedActiveRowFinalizedAndReplaced pins round-9
// F2: an "active" tracking row whose pending queue job is already
// cancel-requested must not be returned as the existing job — the trigger
// finalizes the pending cancellation (sweep cancels job and tracking row
// atomically), re-reads, and proceeds to start replacement work. An operator's
// cancel-then-retry therefore works immediately instead of waiting for the
// sweeper's next tick.
func TestTriggerAddressIndexing_DoomedActiveRowFinalizedAndReplaced(t *testing.T) {
	t.Parallel()
	f := newAddressThrottleFixture(t, executor.AddressIndexingThrottle{})

	first := f.Store.EXPECT().
		GetActiveIndexingJobForAddress(gomock.Any(), throttleTestAddress, domain.Chain("eip155:1")).
		Return(&schema.AddressIndexingJob{
			Status: schema.IndexingJobStatusRunning,
			JobID:  60,
		}, nil)
	f.Store.EXPECT().
		GetJob(gomock.Any(), int64(60)).
		Return(&schema.Job{ID: 60, Status: schema.JobStatusPending, CancelRequested: true}, nil)
	sweep := f.Store.EXPECT().
		SweepCanceledPendingJobs(gomock.Any(), "token_index").
		Return(int64(1), nil).
		After(first)
	f.Store.EXPECT().
		GetActiveIndexingJobForAddress(gomock.Any(), throttleTestAddress, domain.Chain("eip155:1")).
		Return(nil, nil).
		After(sweep)
	f.Store.EXPECT().
		EnqueueJob(gomock.Any(), gomock.Any()).
		Return(&schema.Job{ID: 61, Status: schema.JobStatusPending}, true, nil)
	f.Store.EXPECT().
		CreateAddressIndexingJob(gomock.Any(), gomock.Any()).
		Return(nil)

	resp, err := f.Exec.TriggerAddressIndexing(context.Background(), []string{throttleTestAddress})
	require.NoError(t, err)
	require.Len(t, resp.Jobs, 1)
	require.Equal(t, int64(61), resp.Jobs[0].JobID, "the replacement job must be reported, not the doomed one")
}

// TestTriggerAddressIndexing_HealthyActiveRowStillReturned pins the guard's
// scope: an active row whose queue job is genuinely executable is returned
// as-is — no sweep, no replacement (strict mock).
func TestTriggerAddressIndexing_HealthyActiveRowStillReturned(t *testing.T) {
	t.Parallel()
	f := newAddressThrottleFixture(t, executor.AddressIndexingThrottle{})

	f.Store.EXPECT().
		GetActiveIndexingJobForAddress(gomock.Any(), throttleTestAddress, domain.Chain("eip155:1")).
		Return(&schema.AddressIndexingJob{
			Status:     schema.IndexingJobStatusRunning,
			JobID:      70,
			WorkflowID: "70",
		}, nil)
	f.Store.EXPECT().
		GetJob(gomock.Any(), int64(70)).
		Return(&schema.Job{ID: 70, Status: schema.JobStatusRunning}, nil)

	resp, err := f.Exec.TriggerAddressIndexing(context.Background(), []string{throttleTestAddress})
	require.NoError(t, err)
	require.Len(t, resp.Jobs, 1)
	require.Equal(t, int64(70), resp.Jobs[0].JobID)
	require.False(t, resp.Jobs[0].Throttled)
}

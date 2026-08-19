package executor_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
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
}

func newAddressThrottleFixture(t *testing.T, throttle executor.AddressIndexingThrottle) *addressThrottleFixture {
	t.Helper()
	_ = logger.Initialize(logger.Config{Debug: true})
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
	return &addressThrottleFixture{Exec: exec, Store: mockStore, JQ: mockJQ, Clock: mockClock}
}

// expectEnqueueSuccess registers the expectations of the un-throttled path:
// no active job, a queue enqueue, and the tracking-row insert.
func (f *addressThrottleFixture) expectEnqueueSuccess(jobID int64) {
	f.Store.EXPECT().
		GetActiveIndexingJobForAddress(gomock.Any(), throttleTestAddress, domain.Chain("eip155:1")).
		Return(nil, nil)
	f.JQ.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
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

	resp, err := f.Exec.TriggerAddressIndexing(context.Background(), []string{throttleTestAddress})
	require.NoError(t, err)
	require.Len(t, resp.Jobs, 1)
	require.False(t, resp.Jobs[0].Throttled)
	require.Equal(t, int64(13), resp.Jobs[0].JobID)
}

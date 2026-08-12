package sweeper_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/providers/jobs"
	"github.com/feral-file/ff-indexer-v2/internal/store"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
	"github.com/feral-file/ff-indexer-v2/internal/sweeper"
	"github.com/feral-file/ff-indexer-v2/internal/uri"
)

// testSweeperMocks contains all the mocks needed for testing the sweeper
type testSweeperMocks struct {
	ctrl           *gomock.Controller
	store          *mocks.MockStore
	urlChecker     *mocks.MockURLChecker
	dataURIChecker *mocks.MockDataURIChecker
	clock          *mocks.MockClock
	jobQueue       *mocks.MockJobQueue
	sweeper        sweeper.Sweeper
}

// setupTestSweeper creates all the mocks and sweeper for testing
func setupTestSweeper(t *testing.T) *testSweeperMocks {
	// Initialize logger for tests
	err := logger.Initialize(logger.Config{
		Debug: true,
	})
	if err != nil {
		t.Fatalf("Failed to initialize logger: %v", err)
	}

	ctrl := gomock.NewController(t)

	tm := &testSweeperMocks{
		ctrl:           ctrl,
		store:          mocks.NewMockStore(ctrl),
		urlChecker:     mocks.NewMockURLChecker(ctrl),
		dataURIChecker: mocks.NewMockDataURIChecker(ctrl),
		clock:          mocks.NewMockClock(ctrl),
		jobQueue:       mocks.NewMockJobQueue(ctrl),
	}

	config := &sweeper.MediaHealthSweeperConfig{
		BatchSize:      10,
		WorkerPoolSize: 2,
		RecheckAfter:   24 * time.Hour,
	}

	// These tests run with the render probe disabled, so every sweep cycle checks for
	// orphaned render gates. None of them exercises that path, so an empty answer keeps
	// the strict mocks focused on what each test is actually about
	// (TestMediaHealthSweeper_ReleasesOrphanedGatesWhenProbeDisabled covers it).
	tm.store.EXPECT().GetHealthGatedRenderProbes(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

	tm.sweeper = sweeper.NewMediaHealthSweeper(
		config,
		tm.store,
		tm.urlChecker,
		tm.dataURIChecker,
		tm.clock,
		tm.jobQueue,
		"test-task-queue",
		"test-media-queue",
	)

	return tm
}

// tearDownTestSweeper cleans up the test mocks
func tearDownTestSweeper(mocks *testSweeperMocks) {
	mocks.ctrl.Finish()
}

func TestMediaHealthSweeper_Name(t *testing.T) {
	mocks := setupTestSweeper(t)
	defer tearDownTestSweeper(mocks)

	assert.Equal(t, "media-health-sweeper", mocks.sweeper.Name())
}

func TestMediaHealthSweeper_CheckURL_Healthy(t *testing.T) {
	mocks := setupTestSweeper(t)
	defer tearDownTestSweeper(mocks)

	ctx := context.Background()
	testURL := "https://example.com/image.jpg"

	// Mock Get token IDs that use this URL
	mocks.store.EXPECT().
		GetTokenIDsByMediaURL(gomock.Any(), testURL).
		Return([]uint64{1}, nil)

	// Mock URL check returns healthy
	mocks.urlChecker.EXPECT().
		Check(gomock.Any(), testURL).
		Return(uri.HealthCheckResult{
			Status: uri.HealthStatusHealthy,
		})

	// Mock Update health status to healthy
	mocks.store.EXPECT().
		UpdateTokenMediaHealthByURL(gomock.Any(), testURL, store.MediaHealthUpdate{Status: schema.MediaHealthStatusHealthy}).
		Return(nil)

	// Mock Batch update viewability (returns changed tokens only)
	mocks.store.EXPECT().
		BatchUpdateTokensViewability(gomock.Any(), []uint64{1}).
		Return([]store.TokenViewabilityChange{
			{TokenID: 1, TokenCID: "token1", OldViewable: false, NewViewable: true},
		}, nil)

	mocks.jobQueue.EXPECT().Enqueue(gomock.Any(), gomock.Any()).
		Return(&schema.Job{ID: 1}, true, nil).Times(1)

	// Mock clock expectations
	now := time.Now()
	mocks.clock.EXPECT().Now().Return(now).AnyTimes()
	mocks.clock.EXPECT().Since(now).Return(time.Second).AnyTimes()
	// Make After return a channel that closes after a brief delay to allow Stop to execute
	mocks.clock.EXPECT().After(gomock.Any()).DoAndReturn(func(d time.Duration) <-chan time.Time {
		ch := make(chan time.Time, 1)
		go func() {
			time.Sleep(50 * time.Millisecond)
			ch <- time.Now()
		}()
		return ch
	}).AnyTimes()

	// Mock GetURLsForChecking - use InOrder to ensure first call returns URL, then empty
	gomock.InOrder(
		mocks.store.EXPECT().
			GetURLsForChecking(gomock.Any(), 24*time.Hour, 10).
			Return([]string{testURL}, nil).
			Times(1),
		mocks.store.EXPECT().
			GetURLsForChecking(gomock.Any(), 24*time.Hour, 10).
			Return([]string{}, nil).
			MinTimes(1),
	)

	// Start sweeper in goroutine and stop it after processing
	go func() {
		time.Sleep(200 * time.Millisecond)
		_ = mocks.sweeper.Stop(ctx)
	}()

	err := mocks.sweeper.Start(ctx)
	require.NoError(t, err)
}

func TestMediaHealthSweeper_CheckURL_SSrfBroken(t *testing.T) {
	mocks := setupTestSweeper(t)
	defer tearDownTestSweeper(mocks)

	ctx := context.Background()
	testURL := "http://127.0.0.1/image.jpg"

	mocks.store.EXPECT().
		GetTokenIDsByMediaURL(gomock.Any(), testURL).
		Return([]uint64{99}, nil)

	errMsg := "blocked by ssrf policy"
	mocks.urlChecker.EXPECT().
		Check(gomock.Any(), testURL).
		Return(uri.HealthCheckResult{
			Status:      uri.HealthStatusBroken,
			Error:       &errMsg,
			SSRFBlocked: true,
		})

	mocks.store.EXPECT().
		UpdateTokenMediaHealthByURL(gomock.Any(), testURL, store.MediaHealthUpdate{Status: schema.MediaHealthStatusBroken, LastError: &errMsg}).
		Return(nil)

	mocks.store.EXPECT().
		BatchUpdateTokensViewability(gomock.Any(), []uint64{99}).
		Return(nil, nil)

	now := time.Now()
	mocks.clock.EXPECT().Now().Return(now).AnyTimes()
	mocks.clock.EXPECT().Since(now).Return(time.Second).AnyTimes()
	mocks.clock.EXPECT().After(gomock.Any()).DoAndReturn(func(d time.Duration) <-chan time.Time {
		ch := make(chan time.Time, 1)
		go func() {
			time.Sleep(50 * time.Millisecond)
			ch <- time.Now()
		}()
		return ch
	}).AnyTimes()

	gomock.InOrder(
		mocks.store.EXPECT().
			GetURLsForChecking(gomock.Any(), 24*time.Hour, 10).
			Return([]string{testURL}, nil).
			Times(1),
		mocks.store.EXPECT().
			GetURLsForChecking(gomock.Any(), 24*time.Hour, 10).
			Return([]string{}, nil).
			MinTimes(1),
	)

	go func() {
		time.Sleep(200 * time.Millisecond)
		_ = mocks.sweeper.Stop(ctx)
	}()

	err := mocks.sweeper.Start(ctx)
	require.NoError(t, err)
}

// TestMediaHealthSweeper_CheckURL_AlternativeURLPropagationFailure pins the failure
// branch of fallback promotion: when a working alternative gateway is found but
// UpdateMediaURLAndPropagate fails, the original URL must be persisted broken before
// viewability recomputes. Leaving the row untouched would keep the token viewable while
// its stored public URL stays dead (#96 class); the indexing path handles this failure
// identically.
func TestMediaHealthSweeper_CheckURL_AlternativeURLPropagationFailure(t *testing.T) {
	mocks := setupTestSweeper(t)
	defer tearDownTestSweeper(mocks)

	ctx := context.Background()
	originalURL := "ipfs://QmTest123"
	workingURL := "https://ipfs.io/ipfs/QmTest123"

	mocks.store.EXPECT().
		GetTokenIDsByMediaURL(ctx, originalURL).
		Return([]uint64{1}, nil)

	mocks.urlChecker.EXPECT().
		Check(ctx, originalURL).
		Return(uri.HealthCheckResult{
			Status:     uri.HealthStatusHealthy,
			WorkingURL: &workingURL,
			// The checker preserves the direct probe's diagnostics on a fallback
			// success precisely for this path.
			FailureReason:       uri.FailureDirectoryListing,
			ObservedContentType: "text/html",
			SniffedContentType:  "text/html",
		})

	mocks.store.EXPECT().
		UpdateMediaURLAndPropagate(ctx, originalURL, workingURL, gomock.Nil(), gomock.Nil()).
		Return(assert.AnError)

	// The original URL is marked broken — not left as-is, not marked healthy — and it
	// keeps the direct probe's diagnosis so the per-reason breakdown stays truthful.
	mocks.store.EXPECT().
		UpdateTokenMediaHealthByURL(ctx, originalURL, gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, upd store.MediaHealthUpdate) error {
			require.Equal(t, schema.MediaHealthStatusBroken, upd.Status)
			require.NotNil(t, upd.LastError)
			require.Contains(t, *upd.LastError, "propagation of working alternative")
			require.NotNil(t, upd.FailureReason)
			require.Equal(t, uri.FailureDirectoryListing.String(), *upd.FailureReason)
			require.NotNil(t, upd.ObservedContentType)
			require.Equal(t, "text/html", *upd.ObservedContentType)
			require.NotNil(t, upd.SniffedContentType)
			require.Equal(t, "text/html", *upd.SniffedContentType)
			return nil
		})

	mocks.store.EXPECT().
		BatchUpdateTokensViewability(ctx, []uint64{1}).
		Return(nil, nil)

	now := time.Now()
	mocks.clock.EXPECT().Now().Return(now).AnyTimes()
	mocks.clock.EXPECT().Since(now).Return(time.Second).AnyTimes()
	mocks.clock.EXPECT().After(gomock.Any()).DoAndReturn(func(d time.Duration) <-chan time.Time {
		ch := make(chan time.Time, 1)
		go func() {
			time.Sleep(50 * time.Millisecond)
			ch <- time.Now()
		}()
		return ch
	}).AnyTimes()

	mocks.store.EXPECT().
		GetURLsForChecking(ctx, 24*time.Hour, 10).
		Return([]string{originalURL}, nil).
		Times(1)
	mocks.store.EXPECT().
		GetURLsForChecking(ctx, 24*time.Hour, 10).
		Return([]string{}, nil).
		AnyTimes()

	go func() {
		time.Sleep(150 * time.Millisecond)
		_ = mocks.sweeper.Stop(ctx)
	}()

	err := mocks.sweeper.Start(ctx)
	require.NoError(t, err)
}

func TestMediaHealthSweeper_CheckURL_AlternativeURL(t *testing.T) {
	mocks := setupTestSweeper(t)
	defer tearDownTestSweeper(mocks)

	ctx := context.Background()
	originalURL := "ipfs://QmTest123"
	workingURL := "https://ipfs.io/ipfs/QmTest123"

	// Mock Get token IDs that use this URL
	mocks.store.EXPECT().
		GetTokenIDsByMediaURL(ctx, originalURL).
		Return([]uint64{1}, nil)

	// Mock URL check returns healthy with alternative URL
	mocks.urlChecker.EXPECT().
		Check(ctx, originalURL).
		Return(uri.HealthCheckResult{
			Status:             uri.HealthStatusHealthy,
			WorkingURL:         &workingURL,
			WorkingURLObserved: "image/png",
			WorkingURLSniffed:  "image/png",
		})

	// Mock Update and propagate URL: the promoted row must carry the fallback
	// gateway's own validated observations, not NULLs.
	obs := "image/png"
	mocks.store.EXPECT().
		UpdateMediaURLAndPropagate(ctx, originalURL, workingURL, &obs, &obs).
		Return(nil)

	// Mock Batch update viewability (returns changed tokens only)
	mocks.store.EXPECT().
		BatchUpdateTokensViewability(ctx, []uint64{1}).
		Return([]store.TokenViewabilityChange{
			{TokenID: 1, TokenCID: "token1", OldViewable: false, NewViewable: true},
		}, nil)

	mocks.jobQueue.EXPECT().Enqueue(gomock.Any(), gomock.Any()).
		Return(&schema.Job{ID: 1}, true, nil)

	// Mock clock and sweep
	now := time.Now()
	mocks.clock.EXPECT().Now().Return(now).AnyTimes()
	mocks.clock.EXPECT().Since(now).Return(time.Second).AnyTimes()
	// Make After return a channel that closes after a brief delay
	mocks.clock.EXPECT().After(gomock.Any()).DoAndReturn(func(d time.Duration) <-chan time.Time {
		ch := make(chan time.Time, 1)
		go func() {
			time.Sleep(50 * time.Millisecond)
			ch <- time.Now()
		}()
		return ch
	}).AnyTimes()

	mocks.store.EXPECT().
		GetURLsForChecking(ctx, 24*time.Hour, 10).
		Return([]string{originalURL}, nil).
		Times(1)

	mocks.store.EXPECT().
		GetURLsForChecking(ctx, 24*time.Hour, 10).
		Return([]string{}, nil).
		AnyTimes()

	go func() {
		time.Sleep(150 * time.Millisecond)
		_ = mocks.sweeper.Stop(ctx)
	}()

	err := mocks.sweeper.Start(ctx)
	require.NoError(t, err)
}

func TestMediaHealthSweeper_CheckURL_Broken(t *testing.T) {
	mocks := setupTestSweeper(t)
	defer tearDownTestSweeper(mocks)

	ctx := context.Background()
	testURL := "https://example.com/broken.jpg"
	errorMsg := "404 Not Found"

	// Mock Get token IDs that use this URL
	mocks.store.EXPECT().
		GetTokenIDsByMediaURL(ctx, testURL).
		Return([]uint64{1}, nil)

	// Mock URL check returns broken
	mocks.urlChecker.EXPECT().
		Check(ctx, testURL).
		Return(uri.HealthCheckResult{
			Status: uri.HealthStatusBroken,
			Error:  &errorMsg,
		})

	// Mock Update health status to broken
	mocks.store.EXPECT().
		UpdateTokenMediaHealthByURL(ctx, testURL, store.MediaHealthUpdate{Status: schema.MediaHealthStatusBroken, LastError: &errorMsg}).
		Return(nil)

	// Mock Batch update viewability (returns changed tokens only)
	mocks.store.EXPECT().
		BatchUpdateTokensViewability(ctx, []uint64{1}).
		Return([]store.TokenViewabilityChange{
			{TokenID: 1, TokenCID: "token1", OldViewable: true, NewViewable: false},
		}, nil)

	mocks.jobQueue.EXPECT().Enqueue(gomock.Any(), gomock.Any()).
		Return(&schema.Job{ID: 1}, true, nil)

	// Mock clock and sweep
	now := time.Now()
	mocks.clock.EXPECT().Now().Return(now).AnyTimes()
	mocks.clock.EXPECT().Since(now).Return(time.Second).AnyTimes()
	// Make After return a channel that closes after a brief delay
	mocks.clock.EXPECT().After(gomock.Any()).DoAndReturn(func(d time.Duration) <-chan time.Time {
		ch := make(chan time.Time, 1)
		go func() {
			time.Sleep(50 * time.Millisecond)
			ch <- time.Now()
		}()
		return ch
	}).AnyTimes()

	mocks.store.EXPECT().
		GetURLsForChecking(ctx, 24*time.Hour, 10).
		Return([]string{testURL}, nil).
		Times(1)

	mocks.store.EXPECT().
		GetURLsForChecking(ctx, 24*time.Hour, 10).
		Return([]string{}, nil).
		AnyTimes()

	go func() {
		time.Sleep(150 * time.Millisecond)
		_ = mocks.sweeper.Stop(ctx)
	}()

	err := mocks.sweeper.Start(ctx)
	require.NoError(t, err)
}

// TestMediaHealthSweeper_EnqueuesRenderProbes asserts that with the render probe enabled
// the sweeper enqueues one RenderMediaProbe job per due URL onto the media queue with the
// dedup unique key, and that enqueue failures skip the URL without aborting the batch.
func TestMediaHealthSweeper_EnqueuesRenderProbes(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tm := &testSweeperMocks{
		ctrl:           ctrl,
		store:          mocks.NewMockStore(ctrl),
		urlChecker:     mocks.NewMockURLChecker(ctrl),
		dataURIChecker: mocks.NewMockDataURIChecker(ctrl),
		clock:          mocks.NewMockClock(ctrl),
		jobQueue:       mocks.NewMockJobQueue(ctrl),
	}
	tm.sweeper = sweeper.NewMediaHealthSweeper(
		&sweeper.MediaHealthSweeperConfig{
			BatchSize:            10,
			WorkerPoolSize:       2,
			RecheckAfter:         24 * time.Hour,
			RenderProbeEnabled:   true,
			RenderProbeEnforce:   true,
			RenderProbeBatchSize: 5,
		},
		tm.store, tm.urlChecker, tm.dataURIChecker, tm.clock, tm.jobQueue,
		"test-task-queue", "test-media-queue",
	)

	ctx := context.Background()
	dueURLs := []string{"https://example.com/a.html", "https://example.com/b.html"}

	// Health-check part of the cycle: nothing to check.
	tm.store.EXPECT().GetURLsForChecking(ctx, 24*time.Hour, 10).Return([]string{"https://example.com/x.png"}, nil).Times(1)
	tm.store.EXPECT().GetURLsForChecking(ctx, 24*time.Hour, 10).Return([]string{}, nil).AnyTimes()
	tm.store.EXPECT().GetTokenIDsByMediaURL(ctx, gomock.Any()).Return([]uint64{1}, nil)
	tm.urlChecker.EXPECT().Check(ctx, gomock.Any()).Return(uri.HealthCheckResult{Status: uri.HealthStatusHealthy})
	tm.store.EXPECT().UpdateTokenMediaHealthByURL(ctx, gomock.Any(), gomock.Any()).Return(nil)
	tm.store.EXPECT().BatchUpdateTokensViewability(ctx, gomock.Any()).Return(nil, nil)

	// Render-probe enqueue: first URL fails to enqueue, second succeeds. Scheduling now
	// also runs on subsequent empty-L0 cycles, which return no due URLs here.
	gomock.InOrder(
		tm.store.EXPECT().GetURLsDueForRenderProbe(ctx, 5).Return(dueURLs, nil).Times(1),
		tm.store.EXPECT().GetURLsDueForRenderProbe(ctx, 5).Return(nil, nil).AnyTimes(),
	)
	uk0 := jobs.RenderProbeUniqueKey(dueURLs[0])
	tm.jobQueue.EXPECT().
		Enqueue(ctx, gomock.Any()).
		DoAndReturn(func(_ context.Context, opts jobs.EnqueueOptions) (*schema.Job, bool, error) {
			require.Equal(t, "test-media-queue", opts.Queue)
			require.Equal(t, "RenderMediaProbe", opts.Kind)
			require.NotNil(t, opts.UniqueKey)
			require.Equal(t, uk0, *opts.UniqueKey)
			return nil, false, assert.AnError // enqueue failure must not abort the batch
		})
	tm.jobQueue.EXPECT().
		Enqueue(ctx, gomock.Any()).
		DoAndReturn(func(_ context.Context, opts jobs.EnqueueOptions) (*schema.Job, bool, error) {
			require.Equal(t, "RenderMediaProbe", opts.Kind)
			require.Equal(t, []any{dueURLs[1]}, opts.Args)
			return &schema.Job{ID: 2}, true, nil
		})

	now := time.Now()
	tm.clock.EXPECT().Now().Return(now).AnyTimes()
	tm.clock.EXPECT().Since(now).Return(time.Second).AnyTimes()
	tm.clock.EXPECT().After(gomock.Any()).DoAndReturn(func(d time.Duration) <-chan time.Time {
		ch := make(chan time.Time, 1)
		go func() {
			time.Sleep(50 * time.Millisecond)
			ch <- time.Now()
		}()
		return ch
	}).AnyTimes()

	go func() {
		time.Sleep(150 * time.Millisecond)
		_ = tm.sweeper.Stop(ctx)
	}()

	err := tm.sweeper.Start(ctx)
	require.NoError(t, err)
}

// TestMediaHealthSweeper_EnqueuesRenderProbesWhenNoL0Work pins the L1 cadence: render
// probes have their own schedule (retry_interval / broken_recheck_interval) and
// render-gated rows are deliberately excluded from the L0 query, so scheduling must run
// even when no L0 URL is due. Otherwise a debounce retry — or the healing pass for a
// gated URL — waits on unrelated L0 work.
func TestMediaHealthSweeper_EnqueuesRenderProbesWhenNoL0Work(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tm := &testSweeperMocks{
		ctrl:           ctrl,
		store:          mocks.NewMockStore(ctrl),
		urlChecker:     mocks.NewMockURLChecker(ctrl),
		dataURIChecker: mocks.NewMockDataURIChecker(ctrl),
		clock:          mocks.NewMockClock(ctrl),
		jobQueue:       mocks.NewMockJobQueue(ctrl),
	}
	tm.sweeper = sweeper.NewMediaHealthSweeper(
		&sweeper.MediaHealthSweeperConfig{
			BatchSize:            10,
			WorkerPoolSize:       2,
			RecheckAfter:         24 * time.Hour,
			RenderProbeEnabled:   true,
			RenderProbeEnforce:   true,
			RenderProbeBatchSize: 5,
		},
		tm.store, tm.urlChecker, tm.dataURIChecker, tm.clock, tm.jobQueue,
		"test-task-queue", "test-media-queue",
	)

	ctx := context.Background()
	gatedURL := "https://example.com/gated.html"

	// No L0 work at all — the path that previously returned before scheduling.
	tm.store.EXPECT().
		GetURLsForChecking(ctx, 24*time.Hour, 10).
		Return([]string{}, nil).
		MinTimes(1)

	enqueued := make(chan struct{}, 1)
	tm.store.EXPECT().
		GetURLsDueForRenderProbe(ctx, 5).
		Return([]string{gatedURL}, nil).
		MinTimes(1)
	tm.jobQueue.EXPECT().
		Enqueue(ctx, gomock.Any()).
		DoAndReturn(func(_ context.Context, opts jobs.EnqueueOptions) (*schema.Job, bool, error) {
			require.Equal(t, "test-media-queue", opts.Queue)
			require.Equal(t, "RenderMediaProbe", opts.Kind)
			select {
			case enqueued <- struct{}{}:
			default:
			}
			return &schema.Job{ID: 1}, true, nil
		}).
		MinTimes(1)

	now := time.Now()
	tm.clock.EXPECT().Now().Return(now).AnyTimes()
	tm.clock.EXPECT().Since(now).Return(time.Second).AnyTimes()
	tm.clock.EXPECT().After(gomock.Any()).DoAndReturn(func(d time.Duration) <-chan time.Time {
		ch := make(chan time.Time, 1)
		go func() {
			time.Sleep(20 * time.Millisecond)
			ch <- time.Now()
		}()
		return ch
	}).AnyTimes()

	go func() {
		<-enqueued
		time.Sleep(20 * time.Millisecond)
		_ = tm.sweeper.Stop(ctx)
	}()

	err := tm.sweeper.Start(ctx)
	require.NoError(t, err)
}

// TestMediaHealthSweeper_CheckURL_ContentValidationFailure asserts the sweeper persists
// the full L0 outcome — failure_reason and both content types — not just the error string.
func TestMediaHealthSweeper_CheckURL_ContentValidationFailure(t *testing.T) {
	mocks := setupTestSweeper(t)
	defer tearDownTestSweeper(mocks)

	ctx := context.Background()
	testURL := "https://gateway.example.com/ipfs/QmDirCID"
	errorMsg := `IPFS gateway directory listing (marker "index of /ipfs")`
	reason := uri.FailureDirectoryListing.String()
	htmlType := "text/html"

	mocks.store.EXPECT().
		GetTokenIDsByMediaURL(ctx, testURL).
		Return([]uint64{1}, nil)

	mocks.urlChecker.EXPECT().
		Check(ctx, testURL).
		Return(uri.HealthCheckResult{
			Status:              uri.HealthStatusBroken,
			Error:               &errorMsg,
			FailureReason:       uri.FailureDirectoryListing,
			ObservedContentType: htmlType,
			SniffedContentType:  htmlType,
		})

	mocks.store.EXPECT().
		UpdateTokenMediaHealthByURL(ctx, testURL, store.MediaHealthUpdate{
			Status:              schema.MediaHealthStatusBroken,
			LastError:           &errorMsg,
			FailureReason:       &reason,
			ObservedContentType: &htmlType,
			SniffedContentType:  &htmlType,
		}).
		Return(nil)

	mocks.store.EXPECT().
		BatchUpdateTokensViewability(ctx, []uint64{1}).
		Return(nil, nil)

	now := time.Now()
	mocks.clock.EXPECT().Now().Return(now).AnyTimes()
	mocks.clock.EXPECT().Since(now).Return(time.Second).AnyTimes()
	mocks.clock.EXPECT().After(gomock.Any()).DoAndReturn(func(d time.Duration) <-chan time.Time {
		ch := make(chan time.Time, 1)
		go func() {
			time.Sleep(50 * time.Millisecond)
			ch <- time.Now()
		}()
		return ch
	}).AnyTimes()

	mocks.store.EXPECT().
		GetURLsForChecking(ctx, 24*time.Hour, 10).
		Return([]string{testURL}, nil).
		Times(1)

	mocks.store.EXPECT().
		GetURLsForChecking(ctx, 24*time.Hour, 10).
		Return([]string{}, nil).
		AnyTimes()

	go func() {
		time.Sleep(150 * time.Millisecond)
		_ = mocks.sweeper.Stop(ctx)
	}()

	err := mocks.sweeper.Start(ctx)
	require.NoError(t, err)
}

func TestMediaHealthSweeper_CheckURL_NoViewabilityChange(t *testing.T) {
	mocks := setupTestSweeper(t)
	defer tearDownTestSweeper(mocks)

	ctx := context.Background()
	testURL := "https://example.com/image.jpg"

	// Mock Get token IDs that use this URL
	mocks.store.EXPECT().
		GetTokenIDsByMediaURL(ctx, testURL).
		Return([]uint64{1}, nil)

	// Mock URL check returns healthy
	mocks.urlChecker.EXPECT().
		Check(ctx, testURL).
		Return(uri.HealthCheckResult{
			Status: uri.HealthStatusHealthy,
		})

	// Mock Update health status
	mocks.store.EXPECT().
		UpdateTokenMediaHealthByURL(ctx, testURL, store.MediaHealthUpdate{Status: schema.MediaHealthStatusHealthy}).
		Return(nil)

	// Mock Batch update returns empty (no viewability change)
	mocks.store.EXPECT().
		BatchUpdateTokensViewability(ctx, []uint64{1}).
		Return([]store.TokenViewabilityChange{}, nil) // No changes

	// No webhook should be triggered (no viewability change)

	// Mock clock and sweep
	now := time.Now()
	mocks.clock.EXPECT().Now().Return(now).AnyTimes()
	mocks.clock.EXPECT().Since(now).Return(time.Second).AnyTimes()
	// Make After return a channel that closes after a brief delay
	mocks.clock.EXPECT().After(gomock.Any()).DoAndReturn(func(d time.Duration) <-chan time.Time {
		ch := make(chan time.Time, 1)
		go func() {
			time.Sleep(50 * time.Millisecond)
			ch <- time.Now()
		}()
		return ch
	}).AnyTimes()

	mocks.store.EXPECT().
		GetURLsForChecking(ctx, 24*time.Hour, 10).
		Return([]string{testURL}, nil).
		Times(1)

	mocks.store.EXPECT().
		GetURLsForChecking(ctx, 24*time.Hour, 10).
		Return([]string{}, nil).
		AnyTimes()

	go func() {
		time.Sleep(150 * time.Millisecond)
		_ = mocks.sweeper.Stop(ctx)
	}()

	err := mocks.sweeper.Start(ctx)
	require.NoError(t, err)
}

func TestMediaHealthSweeper_NoURLsToCheck(t *testing.T) {
	mocks := setupTestSweeper(t)
	defer tearDownTestSweeper(mocks)

	ctx := context.Background()

	// Mock No URLs need checking
	mocks.store.EXPECT().
		GetURLsForChecking(ctx, 24*time.Hour, 10).
		Return([]string{}, nil).
		AnyTimes()

	// Mock After to return a channel that closes after a brief delay
	mocks.clock.EXPECT().
		After(sweeper.SWEEP_CYCLE_INTERVAL).
		DoAndReturn(func(d time.Duration) <-chan time.Time {
			ch := make(chan time.Time, 1)
			go func() {
				time.Sleep(50 * time.Millisecond)
				ch <- time.Now()
			}()
			return ch
		}).
		MinTimes(1)

	// Mock clock
	now := time.Now()
	mocks.clock.EXPECT().Now().Return(now).AnyTimes()

	go func() {
		time.Sleep(150 * time.Millisecond)
		_ = mocks.sweeper.Stop(ctx)
	}()

	err := mocks.sweeper.Start(ctx)
	require.NoError(t, err)
}

func TestMediaHealthSweeper_StoreError_GetURLs(t *testing.T) {
	mocks := setupTestSweeper(t)
	defer tearDownTestSweeper(mocks)

	ctx := context.Background()

	// Mock Store error when getting URLs
	mocks.store.EXPECT().
		GetURLsForChecking(ctx, 24*time.Hour, 10).
		Return(nil, errors.New("database connection failed")).
		AnyTimes()

	// Mock clock
	now := time.Now()
	mocks.clock.EXPECT().Now().Return(now).AnyTimes()
	mocks.clock.EXPECT().Since(now).Return(time.Second).AnyTimes()

	go func() {
		time.Sleep(150 * time.Millisecond)
		_ = mocks.sweeper.Stop(ctx)
	}()

	err := mocks.sweeper.Start(ctx)
	require.NoError(t, err) // Sweeper continues despite errors
}

func TestMediaHealthSweeper_StoreError_UpdateHealth(t *testing.T) {
	mocks := setupTestSweeper(t)
	defer tearDownTestSweeper(mocks)

	ctx := context.Background()
	testURL := "https://example.com/image.jpg"

	// Mock Get token IDs
	mocks.store.EXPECT().
		GetTokenIDsByMediaURL(gomock.Any(), testURL).
		Return([]uint64{1}, nil)

	// Mock URL check returns healthy
	mocks.urlChecker.EXPECT().
		Check(gomock.Any(), testURL).
		Return(uri.HealthCheckResult{
			Status: uri.HealthStatusHealthy,
		})

	// Mock Store error when updating health
	mocks.store.EXPECT().
		UpdateTokenMediaHealthByURL(gomock.Any(), testURL, store.MediaHealthUpdate{Status: schema.MediaHealthStatusHealthy}).
		Return(errors.New("update failed"))

	// Even after update error, the sweeper still attempts batch update
	// Mock Batch update (with the token ID we collected earlier)
	mocks.store.EXPECT().
		BatchUpdateTokensViewability(gomock.Any(), []uint64{1}).
		Return([]store.TokenViewabilityChange{}, nil)

	// Mock clock and sweep
	now := time.Now()
	mocks.clock.EXPECT().Now().Return(now).AnyTimes()
	mocks.clock.EXPECT().Since(now).Return(time.Second).AnyTimes()
	// Make After return a channel that closes after a brief delay
	mocks.clock.EXPECT().After(gomock.Any()).DoAndReturn(func(d time.Duration) <-chan time.Time {
		ch := make(chan time.Time, 1)
		go func() {
			time.Sleep(50 * time.Millisecond)
			ch <- time.Now()
		}()
		return ch
	}).AnyTimes()

	gomock.InOrder(
		mocks.store.EXPECT().
			GetURLsForChecking(gomock.Any(), 24*time.Hour, 10).
			Return([]string{testURL}, nil).
			Times(1),
		mocks.store.EXPECT().
			GetURLsForChecking(gomock.Any(), 24*time.Hour, 10).
			Return([]string{}, nil).
			MinTimes(1),
	)

	go func() {
		time.Sleep(200 * time.Millisecond)
		_ = mocks.sweeper.Stop(ctx)
	}()

	err := mocks.sweeper.Start(ctx)
	require.NoError(t, err) // Sweeper continues despite errors
}

func TestMediaHealthSweeper_MultipleURLs(t *testing.T) {
	mocks := setupTestSweeper(t)
	defer tearDownTestSweeper(mocks)

	ctx := context.Background()
	url1 := "https://example.com/image1.jpg"
	url2 := "https://example.com/image2.jpg"

	// Mock Get token IDs for url1
	mocks.store.EXPECT().
		GetTokenIDsByMediaURL(ctx, url1).
		Return([]uint64{1}, nil)

	// Mock Check url1
	mocks.urlChecker.EXPECT().
		Check(ctx, url1).
		Return(uri.HealthCheckResult{Status: uri.HealthStatusHealthy})

	mocks.store.EXPECT().
		UpdateTokenMediaHealthByURL(ctx, url1, store.MediaHealthUpdate{Status: schema.MediaHealthStatusHealthy}).
		Return(nil)

	// Mock Get token IDs for url2
	mocks.store.EXPECT().
		GetTokenIDsByMediaURL(ctx, url2).
		Return([]uint64{2}, nil)

	// Mock Check url2
	mocks.urlChecker.EXPECT().
		Check(ctx, url2).
		Return(uri.HealthCheckResult{Status: uri.HealthStatusHealthy})

	mocks.store.EXPECT().
		UpdateTokenMediaHealthByURL(ctx, url2, store.MediaHealthUpdate{Status: schema.MediaHealthStatusHealthy}).
		Return(nil)

	// Mock Batch update for both tokens (returns both as changed)
	mocks.store.EXPECT().
		BatchUpdateTokensViewability(ctx, gomock.Any()).
		DoAndReturn(func(ctx context.Context, tokenIDs []uint64) ([]store.TokenViewabilityChange, error) {
			// Should receive both token IDs (order doesn't matter)
			require.ElementsMatch(t, []uint64{1, 2}, tokenIDs)
			return []store.TokenViewabilityChange{
				{TokenID: 1, TokenCID: "token1", OldViewable: false, NewViewable: true},
				{TokenID: 2, TokenCID: "token2", OldViewable: false, NewViewable: true},
			}, nil
		})

	mocks.jobQueue.EXPECT().Enqueue(gomock.Any(), gomock.Any()).
		Return(&schema.Job{ID: 1}, true, nil).
		Times(2)

	// Mock clock and sweep
	now := time.Now()
	mocks.clock.EXPECT().Now().Return(now).AnyTimes()
	mocks.clock.EXPECT().Since(now).Return(time.Second).AnyTimes()
	// Make After return a channel that closes after a brief delay
	mocks.clock.EXPECT().After(gomock.Any()).DoAndReturn(func(d time.Duration) <-chan time.Time {
		ch := make(chan time.Time, 1)
		go func() {
			time.Sleep(50 * time.Millisecond)
			ch <- time.Now()
		}()
		return ch
	}).AnyTimes()

	mocks.store.EXPECT().
		GetURLsForChecking(ctx, 24*time.Hour, 10).
		Return([]string{url1, url2}, nil).
		Times(1)

	mocks.store.EXPECT().
		GetURLsForChecking(ctx, 24*time.Hour, 10).
		Return([]string{}, nil).
		AnyTimes()

	go func() {
		time.Sleep(250 * time.Millisecond)
		_ = mocks.sweeper.Stop(ctx)
	}()

	err := mocks.sweeper.Start(ctx)
	require.NoError(t, err)
}

func TestMediaHealthSweeper_StopBeforeStart(t *testing.T) {
	mocks := setupTestSweeper(t)
	defer tearDownTestSweeper(mocks)

	ctx := context.Background()

	// Stop before starting should not error
	err := mocks.sweeper.Stop(ctx)
	require.NoError(t, err)
}

func TestMediaHealthSweeper_DoubleStart(t *testing.T) {
	mocks := setupTestSweeper(t)
	defer tearDownTestSweeper(mocks)

	ctx := context.Background()

	// Mock for first start
	mocks.store.EXPECT().
		GetURLsForChecking(ctx, 24*time.Hour, 10).
		Return([]string{}, nil).
		AnyTimes()

	mocks.clock.EXPECT().Now().Return(time.Now()).AnyTimes()
	// Make After return a channel that closes after a brief delay to allow Stop to execute
	mocks.clock.EXPECT().After(gomock.Any()).DoAndReturn(func(d time.Duration) <-chan time.Time {
		ch := make(chan time.Time, 1)
		go func() {
			time.Sleep(50 * time.Millisecond)
			ch <- time.Now()
		}()
		return ch
	}).AnyTimes()

	// Start in goroutine
	errChan := make(chan error, 1)
	go func() {
		errChan <- mocks.sweeper.Start(ctx)
	}()

	// Give first start time to begin
	time.Sleep(50 * time.Millisecond)

	// Try to start again - should fail
	err := mocks.sweeper.Start(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already running")

	// Stop first instance
	_ = mocks.sweeper.Stop(ctx)
	<-errChan
}

func TestMediaHealthSweeper_GetURLsError_HandledGracefully(t *testing.T) {
	mocks := setupTestSweeper(t)
	defer tearDownTestSweeper(mocks)

	ctx := context.Background()
	testURL := "https://example.com/image.jpg"

	// First call returns a URL successfully
	// Second call returns an error (simulating transient database issue)
	// Third call returns empty (sweeper continues running)
	gomock.InOrder(
		mocks.store.EXPECT().
			GetURLsForChecking(gomock.Any(), 24*time.Hour, 10).
			Return([]string{testURL}, nil).
			Times(1),
		mocks.store.EXPECT().
			GetURLsForChecking(gomock.Any(), 24*time.Hour, 10).
			Return(nil, errors.New("database connection timeout")).
			Times(1),
		mocks.store.EXPECT().
			GetURLsForChecking(gomock.Any(), 24*time.Hour, 10).
			Return([]string{}, nil).
			MinTimes(1),
	)

	// Mock URL check for the first successful call
	mocks.store.EXPECT().
		GetTokenIDsByMediaURL(gomock.Any(), testURL).
		Return([]uint64{1}, nil)

	mocks.urlChecker.EXPECT().
		Check(gomock.Any(), testURL).
		Return(uri.HealthCheckResult{Status: uri.HealthStatusHealthy})

	mocks.store.EXPECT().
		UpdateTokenMediaHealthByURL(gomock.Any(), testURL, store.MediaHealthUpdate{Status: schema.MediaHealthStatusHealthy}).
		Return(nil)

	// Mock Batch update returns changes
	mocks.store.EXPECT().
		BatchUpdateTokensViewability(gomock.Any(), []uint64{1}).
		Return([]store.TokenViewabilityChange{
			{TokenID: 1, TokenCID: "token1", OldViewable: false, NewViewable: true},
		}, nil)

	mocks.jobQueue.EXPECT().Enqueue(gomock.Any(), gomock.Any()).
		Return(&schema.Job{ID: 1}, true, nil)

	// Mock clock
	now := time.Now()
	mocks.clock.EXPECT().Now().Return(now).AnyTimes()
	mocks.clock.EXPECT().Since(now).Return(time.Second).AnyTimes()
	// Make After return a channel that closes after a brief delay
	mocks.clock.EXPECT().After(gomock.Any()).DoAndReturn(func(d time.Duration) <-chan time.Time {
		ch := make(chan time.Time, 1)
		go func() {
			time.Sleep(50 * time.Millisecond)
			ch <- time.Now()
		}()
		return ch
	}).AnyTimes()

	go func() {
		time.Sleep(300 * time.Millisecond)
		_ = mocks.sweeper.Stop(ctx)
	}()

	err := mocks.sweeper.Start(ctx)
	require.NoError(t, err) // Sweeper continues despite GetURLs errors
}

// Data URI Tests

func TestMediaHealthSweeper_CheckDataURI_Valid(t *testing.T) {
	mocks := setupTestSweeper(t)
	defer tearDownTestSweeper(mocks)

	ctx := context.Background()
	testDataURI := "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=="

	// Mock Get token IDs that use this data URI
	mocks.store.EXPECT().
		GetTokenIDsByMediaURL(ctx, testDataURI).
		Return([]uint64{1}, nil)

	// Mock Data URI check returns valid
	mocks.dataURIChecker.EXPECT().
		Check(testDataURI).
		Return(uri.DataURICheckResult{
			Valid: true,
		})

	// Mock Update health status to healthy
	mocks.store.EXPECT().
		UpdateTokenMediaHealthByURL(ctx, testDataURI, store.MediaHealthUpdate{Status: schema.MediaHealthStatusHealthy}).
		Return(nil)

	// Mock Batch update returns changes
	mocks.store.EXPECT().
		BatchUpdateTokensViewability(ctx, []uint64{1}).
		Return([]store.TokenViewabilityChange{
			{TokenID: 1, TokenCID: "token1", OldViewable: false, NewViewable: true},
		}, nil)

	// Mock Trigger webhook
	mocks.jobQueue.EXPECT().Enqueue(gomock.Any(), gomock.Any()).
		Return(&schema.Job{ID: 1}, true, nil)

	// Mock clock and sweep
	now := time.Now()
	mocks.clock.EXPECT().Now().Return(now).AnyTimes()
	mocks.clock.EXPECT().Since(now).Return(time.Second).AnyTimes()
	// Make After return a channel that closes after a brief delay
	mocks.clock.EXPECT().After(gomock.Any()).DoAndReturn(func(d time.Duration) <-chan time.Time {
		ch := make(chan time.Time, 1)
		go func() {
			time.Sleep(50 * time.Millisecond)
			ch <- time.Now()
		}()
		return ch
	}).AnyTimes()

	gomock.InOrder(
		mocks.store.EXPECT().
			GetURLsForChecking(gomock.Any(), 24*time.Hour, 10).
			Return([]string{testDataURI}, nil).
			Times(1),
		mocks.store.EXPECT().
			GetURLsForChecking(gomock.Any(), 24*time.Hour, 10).
			Return([]string{}, nil).
			MinTimes(1),
	)

	go func() {
		time.Sleep(200 * time.Millisecond)
		_ = mocks.sweeper.Stop(ctx)
	}()

	err := mocks.sweeper.Start(ctx)
	require.NoError(t, err)
}

func TestMediaHealthSweeper_CheckDataURI_Invalid(t *testing.T) {
	mocks := setupTestSweeper(t)
	defer tearDownTestSweeper(mocks)

	ctx := context.Background()
	testDataURI := "data:image/png;base64,invalid-base64-data"
	errorMsg := "failed to decode base64: illegal base64 data at input byte 7"

	// Mock Get token IDs that use this data URI
	mocks.store.EXPECT().
		GetTokenIDsByMediaURL(ctx, testDataURI).
		Return([]uint64{1}, nil)

	// Mock Data URI check returns invalid
	mocks.dataURIChecker.EXPECT().
		Check(testDataURI).
		Return(uri.DataURICheckResult{
			Valid: false,
			Error: &errorMsg,
		})

	// Mock Update health status to broken
	mocks.store.EXPECT().
		UpdateTokenMediaHealthByURL(ctx, testDataURI, store.MediaHealthUpdate{Status: schema.MediaHealthStatusBroken, LastError: &errorMsg}).
		Return(nil)

	// Mock Batch update returns changes
	mocks.store.EXPECT().
		BatchUpdateTokensViewability(ctx, []uint64{1}).
		Return([]store.TokenViewabilityChange{
			{TokenID: 1, TokenCID: "token1", OldViewable: true, NewViewable: false},
		}, nil)

	// Mock Trigger webhook
	mocks.jobQueue.EXPECT().Enqueue(gomock.Any(), gomock.Any()).
		Return(&schema.Job{ID: 1}, true, nil)

	// Mock clock and sweep
	now := time.Now()
	mocks.clock.EXPECT().Now().Return(now).AnyTimes()
	mocks.clock.EXPECT().Since(now).Return(time.Second).AnyTimes()
	// Make After return a channel that closes after a brief delay
	mocks.clock.EXPECT().After(gomock.Any()).DoAndReturn(func(d time.Duration) <-chan time.Time {
		ch := make(chan time.Time, 1)
		go func() {
			time.Sleep(50 * time.Millisecond)
			ch <- time.Now()
		}()
		return ch
	}).AnyTimes()

	gomock.InOrder(
		mocks.store.EXPECT().
			GetURLsForChecking(gomock.Any(), 24*time.Hour, 10).
			Return([]string{testDataURI}, nil).
			Times(1),
		mocks.store.EXPECT().
			GetURLsForChecking(gomock.Any(), 24*time.Hour, 10).
			Return([]string{}, nil).
			MinTimes(1),
	)

	go func() {
		time.Sleep(200 * time.Millisecond)
		_ = mocks.sweeper.Stop(ctx)
	}()

	err := mocks.sweeper.Start(ctx)
	require.NoError(t, err)
}

// TestMediaHealthSweeper_SameTokenMultipleURLs tests the scenario where
// multiple URLs belong to the same token and are checked in parallel.
// This verifies that the batch update correctly handles this without race conditions.
func TestMediaHealthSweeper_SameTokenMultipleURLs(t *testing.T) {
	mocks := setupTestSweeper(t)
	defer tearDownTestSweeper(mocks)

	ctx := context.Background()
	imageURL := "https://example.com/image.jpg"
	animationURL := "https://example.com/animation.mp4"

	// Both URLs belong to the same token
	mocks.store.EXPECT().
		GetTokenIDsByMediaURL(ctx, imageURL).
		Return([]uint64{1}, nil)

	mocks.store.EXPECT().
		GetTokenIDsByMediaURL(ctx, animationURL).
		Return([]uint64{1}, nil)

	// Check both URLs (parallel)
	mocks.urlChecker.EXPECT().
		Check(ctx, imageURL).
		Return(uri.HealthCheckResult{Status: uri.HealthStatusHealthy})

	mocks.urlChecker.EXPECT().
		Check(ctx, animationURL).
		Return(uri.HealthCheckResult{Status: uri.HealthStatusHealthy})

	// Update health for both URLs
	mocks.store.EXPECT().
		UpdateTokenMediaHealthByURL(ctx, imageURL, store.MediaHealthUpdate{Status: schema.MediaHealthStatusHealthy}).
		Return(nil)

	mocks.store.EXPECT().
		UpdateTokenMediaHealthByURL(ctx, animationURL, store.MediaHealthUpdate{Status: schema.MediaHealthStatusHealthy}).
		Return(nil)

	// Critical: Batch update should receive token ID only once (deduplicated)
	// and compute viewability from the latest DB state (both URLs healthy)
	mocks.store.EXPECT().
		BatchUpdateTokensViewability(ctx, []uint64{1}).
		Return([]store.TokenViewabilityChange{
			{TokenID: 1, TokenCID: "token1", OldViewable: false, NewViewable: true},
		}, nil).
		Times(1) // Should only be called once with deduplicated token ID

	mocks.jobQueue.EXPECT().Enqueue(gomock.Any(), gomock.Any()).
		Return(&schema.Job{ID: 1}, true, nil)

	// Mock clock and sweep
	now := time.Now()
	mocks.clock.EXPECT().Now().Return(now).AnyTimes()
	mocks.clock.EXPECT().Since(now).Return(time.Second).AnyTimes()
	// Make After return a channel that closes after a brief delay
	mocks.clock.EXPECT().After(gomock.Any()).DoAndReturn(func(d time.Duration) <-chan time.Time {
		ch := make(chan time.Time, 1)
		go func() {
			time.Sleep(50 * time.Millisecond)
			ch <- time.Now()
		}()
		return ch
	}).AnyTimes()

	mocks.store.EXPECT().
		GetURLsForChecking(ctx, 24*time.Hour, 10).
		Return([]string{imageURL, animationURL}, nil).
		Times(1)

	mocks.store.EXPECT().
		GetURLsForChecking(ctx, 24*time.Hour, 10).
		Return([]string{}, nil).
		AnyTimes()

	go func() {
		time.Sleep(250 * time.Millisecond)
		_ = mocks.sweeper.Stop(ctx)
	}()

	err := mocks.sweeper.Start(ctx)
	require.NoError(t, err)
}

// TestMediaHealthSweeper_ReleasesOrphanedGatesWhenProbeDisabled pins the rollback
// contract: a render gate's only healer is a successful render, so disabling the probe
// (rollback, misconfigured fingerprints, decommission) would otherwise leave every gated
// token permanently non-viewable — L0 is locked out of render_% rows by design. With the
// probe disabled the sweeper must release active gates instead of enqueueing probes;
// released rows return to unknown and the next L0 sweep re-verifies the bytes.
func TestMediaHealthSweeper_ReleasesOrphanedGatesWhenProbeDisabled(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tm := &testSweeperMocks{
		ctrl:           ctrl,
		store:          mocks.NewMockStore(ctrl),
		urlChecker:     mocks.NewMockURLChecker(ctrl),
		dataURIChecker: mocks.NewMockDataURIChecker(ctrl),
		clock:          mocks.NewMockClock(ctrl),
		jobQueue:       mocks.NewMockJobQueue(ctrl),
	}
	tm.sweeper = sweeper.NewMediaHealthSweeper(
		&sweeper.MediaHealthSweeperConfig{
			BatchSize:          10,
			WorkerPoolSize:     2,
			RecheckAfter:       24 * time.Hour,
			RenderProbeEnabled: false, // rollback: probe turned off with gates still held
		},
		tm.store, tm.urlChecker, tm.dataURIChecker, tm.clock, tm.jobQueue,
		"test-task-queue", "test-media-queue",
	)

	ctx := context.Background()
	gatedA := schema.MediaRenderProbe{MediaURL: "https://example.com/gated-a.html", HealthGated: true}
	gatedB := schema.MediaRenderProbe{MediaURL: "https://example.com/gated-b.html", HealthGated: true}

	tm.store.EXPECT().
		GetURLsForChecking(ctx, 24*time.Hour, 10).
		Return([]string{}, nil).
		MinTimes(1)
	// Strict mock: GetURLsDueForRenderProbe and Enqueue must never be called while the
	// probe is disabled.

	released := make(chan struct{}, 1)
	gomock.InOrder(
		tm.store.EXPECT().GetHealthGatedRenderProbes(ctx, gomock.Any()).Return([]schema.MediaRenderProbe{gatedA, gatedB}, nil).Times(1),
		tm.store.EXPECT().GetHealthGatedRenderProbes(ctx, gomock.Any()).Return(nil, nil).AnyTimes(),
	)
	// First release fails: it must not abort the batch, and the marker survives for the
	// next cycle's retry.
	tm.store.EXPECT().ReleaseRenderGate(ctx, gatedA).Return(nil, assert.AnError)
	tm.store.EXPECT().
		ReleaseRenderGate(ctx, gatedB).
		DoAndReturn(func(context.Context, schema.MediaRenderProbe) ([]uint64, error) {
			return []uint64{42}, nil
		})
	tm.store.EXPECT().
		BatchUpdateTokensViewability(ctx, []uint64{42}).
		DoAndReturn(func(context.Context, []uint64) ([]store.TokenViewabilityChange, error) {
			select {
			case released <- struct{}{}:
			default:
			}
			return nil, nil
		})

	now := time.Now()
	tm.clock.EXPECT().Now().Return(now).AnyTimes()
	tm.clock.EXPECT().Since(now).Return(time.Second).AnyTimes()
	tm.clock.EXPECT().After(gomock.Any()).DoAndReturn(func(d time.Duration) <-chan time.Time {
		ch := make(chan time.Time, 1)
		go func() {
			time.Sleep(20 * time.Millisecond)
			ch <- time.Now()
		}()
		return ch
	}).AnyTimes()

	go func() {
		<-released
		time.Sleep(20 * time.Millisecond)
		_ = tm.sweeper.Stop(ctx)
	}()

	require.NoError(t, tm.sweeper.Start(ctx))
}

// TestMediaHealthSweeper_ShadowModeReleasesGatesAndStillEnqueues pins the shadow
// contract at the scheduling layer: with the probe enabled but not enforcing, probes
// keep flowing (shadow observes) while any existing gates — leftovers from an enforcing
// deployment — are released each cycle, because shadow's promise is that L1 hides
// nothing.
func TestMediaHealthSweeper_ShadowModeReleasesGatesAndStillEnqueues(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tm := &testSweeperMocks{
		ctrl:           ctrl,
		store:          mocks.NewMockStore(ctrl),
		urlChecker:     mocks.NewMockURLChecker(ctrl),
		dataURIChecker: mocks.NewMockDataURIChecker(ctrl),
		clock:          mocks.NewMockClock(ctrl),
		jobQueue:       mocks.NewMockJobQueue(ctrl),
	}
	tm.sweeper = sweeper.NewMediaHealthSweeper(
		&sweeper.MediaHealthSweeperConfig{
			BatchSize:            10,
			WorkerPoolSize:       2,
			RecheckAfter:         24 * time.Hour,
			RenderProbeEnabled:   true,
			RenderProbeEnforce:   false, // shadow
			RenderProbeBatchSize: 5,
		},
		tm.store, tm.urlChecker, tm.dataURIChecker, tm.clock, tm.jobQueue,
		"test-task-queue", "test-media-queue",
	)

	ctx := context.Background()
	staleGate := schema.MediaRenderProbe{MediaURL: "https://example.com/stale.html", HealthGated: true}
	dueURL := "https://example.com/shadow-due.html"

	tm.store.EXPECT().GetURLsForChecking(ctx, 24*time.Hour, 10).Return([]string{}, nil).MinTimes(1)

	// Release of the stale gate AND probe enqueueing both happen in shadow.
	gomock.InOrder(
		tm.store.EXPECT().GetHealthGatedRenderProbes(ctx, gomock.Any()).Return([]schema.MediaRenderProbe{staleGate}, nil).Times(1),
		tm.store.EXPECT().GetHealthGatedRenderProbes(ctx, gomock.Any()).Return(nil, nil).AnyTimes(),
	)
	tm.store.EXPECT().ReleaseRenderGate(ctx, staleGate).Return([]uint64{7}, nil)
	tm.store.EXPECT().BatchUpdateTokensViewability(ctx, []uint64{7}).Return(nil, nil)

	enqueued := make(chan struct{}, 1)
	gomock.InOrder(
		tm.store.EXPECT().GetURLsDueForRenderProbe(ctx, 5).Return([]string{dueURL}, nil).Times(1),
		tm.store.EXPECT().GetURLsDueForRenderProbe(ctx, 5).Return(nil, nil).AnyTimes(),
	)
	tm.jobQueue.EXPECT().
		Enqueue(ctx, gomock.Any()).
		DoAndReturn(func(_ context.Context, opts jobs.EnqueueOptions) (*schema.Job, bool, error) {
			require.Equal(t, "RenderMediaProbe", opts.Kind)
			select {
			case enqueued <- struct{}{}:
			default:
			}
			return &schema.Job{ID: 1}, true, nil
		})

	now := time.Now()
	tm.clock.EXPECT().Now().Return(now).AnyTimes()
	tm.clock.EXPECT().Since(now).Return(time.Second).AnyTimes()
	tm.clock.EXPECT().After(gomock.Any()).DoAndReturn(func(d time.Duration) <-chan time.Time {
		ch := make(chan time.Time, 1)
		go func() {
			time.Sleep(20 * time.Millisecond)
			ch <- time.Now()
		}()
		return ch
	}).AnyTimes()

	go func() {
		<-enqueued
		time.Sleep(20 * time.Millisecond)
		_ = tm.sweeper.Stop(ctx)
	}()

	require.NoError(t, tm.sweeper.Start(ctx))
}

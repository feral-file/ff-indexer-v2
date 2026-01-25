package sweeper_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"

	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
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
	orchestrator   *mocks.MockTemporalOrchestrator
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
		orchestrator:   mocks.NewMockTemporalOrchestrator(ctrl),
	}

	config := &sweeper.MediaHealthSweeperConfig{
		BatchSize:      10,
		WorkerPoolSize: 2,
		RecheckAfter:   24 * time.Hour,
	}

	tm.sweeper = sweeper.NewMediaHealthSweeper(
		config,
		tm.store,
		tm.urlChecker,
		tm.dataURIChecker,
		tm.clock,
		tm.orchestrator,
		"test-task-queue",
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
		UpdateTokenMediaHealthByURL(gomock.Any(), testURL, schema.MediaHealthStatusHealthy, nil).
		Return(nil)

	// Mock Batch update viewability (returns changed tokens only)
	mocks.store.EXPECT().
		BatchUpdateTokensViewability(gomock.Any(), []uint64{1}).
		Return([]store.TokenViewabilityChange{
			{TokenID: 1, TokenCID: "token1", OldViewable: false, NewViewable: true},
		}, nil)

	// Mock Trigger webhook for token (viewability changed from false to true)
	mocks.orchestrator.EXPECT().
		ExecuteWorkflow(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).
		Return(client.WorkflowRun(nil), nil).
		Times(1)

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
			Status:     uri.HealthStatusHealthy,
			WorkingURL: &workingURL,
		})

	// Mock Update and propagate URL
	mocks.store.EXPECT().
		UpdateMediaURLAndPropagate(ctx, originalURL, workingURL).
		Return(nil)

	// Mock Batch update viewability (returns changed tokens only)
	mocks.store.EXPECT().
		BatchUpdateTokensViewability(ctx, []uint64{1}).
		Return([]store.TokenViewabilityChange{
			{TokenID: 1, TokenCID: "token1", OldViewable: false, NewViewable: true},
		}, nil)

	// Mock Trigger webhook for token (viewability changed)
	mocks.orchestrator.EXPECT().
		ExecuteWorkflow(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).
		Return(client.WorkflowRun(nil), nil)

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
		UpdateTokenMediaHealthByURL(ctx, testURL, schema.MediaHealthStatusBroken, &errorMsg).
		Return(nil)

	// Mock Batch update viewability (returns changed tokens only)
	mocks.store.EXPECT().
		BatchUpdateTokensViewability(ctx, []uint64{1}).
		Return([]store.TokenViewabilityChange{
			{TokenID: 1, TokenCID: "token1", OldViewable: true, NewViewable: false},
		}, nil)

	// Mock Trigger webhook (viewability changed from true to false)
	mocks.orchestrator.EXPECT().
		ExecuteWorkflow(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).
		Return(client.WorkflowRun(nil), nil)

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
		UpdateTokenMediaHealthByURL(ctx, testURL, schema.MediaHealthStatusHealthy, nil).
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
		UpdateTokenMediaHealthByURL(gomock.Any(), testURL, schema.MediaHealthStatusHealthy, nil).
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
		UpdateTokenMediaHealthByURL(ctx, url1, schema.MediaHealthStatusHealthy, nil).
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
		UpdateTokenMediaHealthByURL(ctx, url2, schema.MediaHealthStatusHealthy, nil).
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

	// Mock Trigger webhooks for both tokens
	mocks.orchestrator.EXPECT().
		ExecuteWorkflow(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(client.WorkflowRun(nil), nil).
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
		UpdateTokenMediaHealthByURL(gomock.Any(), testURL, schema.MediaHealthStatusHealthy, nil).
		Return(nil)

	// Mock Batch update returns changes
	mocks.store.EXPECT().
		BatchUpdateTokensViewability(gomock.Any(), []uint64{1}).
		Return([]store.TokenViewabilityChange{
			{TokenID: 1, TokenCID: "token1", OldViewable: false, NewViewable: true},
		}, nil)

	mocks.orchestrator.EXPECT().
		ExecuteWorkflow(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(client.WorkflowRun(nil), nil)

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
		UpdateTokenMediaHealthByURL(ctx, testDataURI, schema.MediaHealthStatusHealthy, nil).
		Return(nil)

	// Mock Batch update returns changes
	mocks.store.EXPECT().
		BatchUpdateTokensViewability(ctx, []uint64{1}).
		Return([]store.TokenViewabilityChange{
			{TokenID: 1, TokenCID: "token1", OldViewable: false, NewViewable: true},
		}, nil)

	// Mock Trigger webhook
	mocks.orchestrator.EXPECT().
		ExecuteWorkflow(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(client.WorkflowRun(nil), nil)

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
		UpdateTokenMediaHealthByURL(ctx, testDataURI, schema.MediaHealthStatusBroken, &errorMsg).
		Return(nil)

	// Mock Batch update returns changes
	mocks.store.EXPECT().
		BatchUpdateTokensViewability(ctx, []uint64{1}).
		Return([]store.TokenViewabilityChange{
			{TokenID: 1, TokenCID: "token1", OldViewable: true, NewViewable: false},
		}, nil)

	// Mock Trigger webhook
	mocks.orchestrator.EXPECT().
		ExecuteWorkflow(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(client.WorkflowRun(nil), nil)

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
		UpdateTokenMediaHealthByURL(ctx, imageURL, schema.MediaHealthStatusHealthy, nil).
		Return(nil)

	mocks.store.EXPECT().
		UpdateTokenMediaHealthByURL(ctx, animationURL, schema.MediaHealthStatusHealthy, nil).
		Return(nil)

	// Critical: Batch update should receive token ID only once (deduplicated)
	// and compute viewability from the latest DB state (both URLs healthy)
	mocks.store.EXPECT().
		BatchUpdateTokensViewability(ctx, []uint64{1}).
		Return([]store.TokenViewabilityChange{
			{TokenID: 1, TokenCID: "token1", OldViewable: false, NewViewable: true},
		}, nil).
		Times(1) // Should only be called once with deduplicated token ID

	mocks.orchestrator.EXPECT().
		ExecuteWorkflow(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(client.WorkflowRun(nil), nil)

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

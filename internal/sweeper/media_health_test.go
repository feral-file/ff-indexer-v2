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
	ctrl         *gomock.Controller
	store        *mocks.MockStore
	checker      *mocks.MockURLChecker
	clock        *mocks.MockClock
	orchestrator *mocks.MockTemporalOrchestrator
	sweeper      sweeper.Sweeper
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
		ctrl:         ctrl,
		store:        mocks.NewMockStore(ctrl),
		checker:      mocks.NewMockURLChecker(ctrl),
		clock:        mocks.NewMockClock(ctrl),
		orchestrator: mocks.NewMockTemporalOrchestrator(ctrl),
	}

	config := &sweeper.MediaHealthSweeperConfig{
		BatchSize:      10,
		WorkerPoolSize: 2,
		RecheckAfter:   24 * time.Hour,
	}

	tm.sweeper = sweeper.NewMediaHealthSweeper(
		config,
		tm.store,
		tm.checker,
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

	// Mock: Get tokens before check
	tokensBefore := []store.TokenViewabilityInfo{
		{TokenID: 1, TokenCID: "token1", IsViewable: false},
	}

	// Mock: Mark URL as checking (allow this instance to process it)
	mocks.store.EXPECT().
		MarkMediaURLAsChecking(gomock.Any(), testURL, 24*time.Hour).
		Return(true, nil)

	mocks.store.EXPECT().
		GetTokensViewabilityByMediaURL(gomock.Any(), testURL).
		Return(tokensBefore, nil)

	// Mock: URL check returns healthy
	mocks.checker.EXPECT().
		Check(gomock.Any(), testURL).
		Return(uri.HealthCheckResult{
			Status: uri.HealthStatusHealthy,
		})

	// Mock: Update health status to healthy
	mocks.store.EXPECT().
		UpdateTokenMediaHealthByURL(gomock.Any(), testURL, schema.MediaHealthStatusHealthy, nil).
		Return(nil)

	// Mock: Get tokens after check by IDs
	tokensAfter := []store.TokenViewabilityInfo{
		{TokenID: 1, TokenCID: "token1", IsViewable: true},
	}
	mocks.store.EXPECT().
		GetTokensViewabilityByIDs(gomock.Any(), []uint64{1}).
		Return(tokensAfter, nil)

	// Mock: Trigger webhook for token (viewability changed from false to true)
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
	mocks.clock.EXPECT().Sleep(gomock.Any()).AnyTimes()

	// Mock: GetMediaURLsNeedingCheck - use InOrder to ensure first call returns URL, then empty
	gomock.InOrder(
		mocks.store.EXPECT().
			GetMediaURLsNeedingCheck(gomock.Any(), 24*time.Hour, 10).
			Return([]string{testURL}, nil).
			Times(1),
		mocks.store.EXPECT().
			GetMediaURLsNeedingCheck(gomock.Any(), 24*time.Hour, 10).
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

	// Mock: Get tokens before check
	tokensBefore := []store.TokenViewabilityInfo{
		{TokenID: 1, TokenCID: "token1", IsViewable: false},
	}

	// Mock: Mark URL as checking
	mocks.store.EXPECT().
		MarkMediaURLAsChecking(ctx, originalURL, 24*time.Hour).
		Return(true, nil)

	mocks.store.EXPECT().
		GetTokensViewabilityByMediaURL(ctx, originalURL).
		Return(tokensBefore, nil)

	// Mock: URL check returns healthy with alternative URL
	mocks.checker.EXPECT().
		Check(ctx, originalURL).
		Return(uri.HealthCheckResult{
			Status:     uri.HealthStatusHealthy,
			WorkingURL: &workingURL,
		})

	// Mock: Update and propagate URL
	mocks.store.EXPECT().
		UpdateMediaURLAndPropagate(ctx, originalURL, workingURL).
		Return(nil)

	// Mock: Get tokens after check by IDs
	tokensAfter := []store.TokenViewabilityInfo{
		{TokenID: 1, TokenCID: "token1", IsViewable: true},
	}
	mocks.store.EXPECT().
		GetTokensViewabilityByIDs(ctx, []uint64{1}).
		Return(tokensAfter, nil)

	// Mock: Trigger webhook for token (viewability changed)
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
	mocks.clock.EXPECT().Sleep(gomock.Any()).AnyTimes()

	mocks.store.EXPECT().
		GetMediaURLsNeedingCheck(ctx, 24*time.Hour, 10).
		Return([]string{originalURL}, nil).
		Times(1)

	mocks.store.EXPECT().
		GetMediaURLsNeedingCheck(ctx, 24*time.Hour, 10).
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

	// Mock: Mark URL as checking
	mocks.store.EXPECT().
		MarkMediaURLAsChecking(ctx, testURL, 24*time.Hour).
		Return(true, nil)

	// Mock: Get tokens before check
	tokensBefore := []store.TokenViewabilityInfo{
		{TokenID: 1, TokenCID: "token1", IsViewable: true},
	}
	mocks.store.EXPECT().
		GetTokensViewabilityByMediaURL(ctx, testURL).
		Return(tokensBefore, nil)

	// Mock: URL check returns broken
	mocks.checker.EXPECT().
		Check(ctx, testURL).
		Return(uri.HealthCheckResult{
			Status: uri.HealthStatusBroken,
			Error:  &errorMsg,
		})

	// Mock: Update health status to broken
	mocks.store.EXPECT().
		UpdateTokenMediaHealthByURL(ctx, testURL, schema.MediaHealthStatusBroken, &errorMsg).
		Return(nil)

	// Mock: Get tokens after check by IDs
	tokensAfter := []store.TokenViewabilityInfo{
		{TokenID: 1, TokenCID: "token1", IsViewable: false},
	}
	mocks.store.EXPECT().
		GetTokensViewabilityByIDs(ctx, []uint64{1}).
		Return(tokensAfter, nil)

	// Mock: Trigger webhook (viewability changed from true to false)
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
	mocks.clock.EXPECT().Sleep(gomock.Any()).AnyTimes()

	mocks.store.EXPECT().
		GetMediaURLsNeedingCheck(ctx, 24*time.Hour, 10).
		Return([]string{testURL}, nil).
		Times(1)

	mocks.store.EXPECT().
		GetMediaURLsNeedingCheck(ctx, 24*time.Hour, 10).
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

	// Mock: Mark URL as checking
	mocks.store.EXPECT().
		MarkMediaURLAsChecking(ctx, testURL, 24*time.Hour).
		Return(true, nil)

	// Mock: Get tokens before check
	tokensBefore := []store.TokenViewabilityInfo{
		{TokenID: 1, TokenCID: "token1", IsViewable: true},
	}
	mocks.store.EXPECT().
		GetTokensViewabilityByMediaURL(ctx, testURL).
		Return(tokensBefore, nil)

	// Mock: URL check returns healthy
	mocks.checker.EXPECT().
		Check(ctx, testURL).
		Return(uri.HealthCheckResult{
			Status: uri.HealthStatusHealthy,
		})

	// Mock: Update health status
	mocks.store.EXPECT().
		UpdateTokenMediaHealthByURL(ctx, testURL, schema.MediaHealthStatusHealthy, nil).
		Return(nil)

	// Mock: Get tokens after check by IDs (still viewable)
	tokensAfter := []store.TokenViewabilityInfo{
		{TokenID: 1, TokenCID: "token1", IsViewable: true},
	}
	mocks.store.EXPECT().
		GetTokensViewabilityByIDs(ctx, []uint64{1}).
		Return(tokensAfter, nil)

	// No webhook should be triggered (no viewability change)

	// Mock clock and sweep
	now := time.Now()
	mocks.clock.EXPECT().Now().Return(now).AnyTimes()
	mocks.clock.EXPECT().Since(now).Return(time.Second).AnyTimes()
	mocks.clock.EXPECT().Sleep(gomock.Any()).AnyTimes()

	mocks.store.EXPECT().
		GetMediaURLsNeedingCheck(ctx, 24*time.Hour, 10).
		Return([]string{testURL}, nil).
		Times(1)

	mocks.store.EXPECT().
		GetMediaURLsNeedingCheck(ctx, 24*time.Hour, 10).
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

	// Mock: No URLs need checking
	mocks.store.EXPECT().
		GetMediaURLsNeedingCheck(ctx, 24*time.Hour, 10).
		Return([]string{}, nil).
		AnyTimes()

	// Mock: Sleep when no URLs found
	mocks.clock.EXPECT().
		Sleep(10 * time.Second).
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

	// Mock: Store error when getting URLs
	mocks.store.EXPECT().
		GetMediaURLsNeedingCheck(ctx, 24*time.Hour, 10).
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

	// Mock: Mark URL as checking
	mocks.store.EXPECT().
		MarkMediaURLAsChecking(gomock.Any(), testURL, 24*time.Hour).
		Return(true, nil)

	// Mock: Get tokens before check
	mocks.store.EXPECT().
		GetTokensViewabilityByMediaURL(gomock.Any(), testURL).
		Return([]store.TokenViewabilityInfo{}, nil)

	// Mock: URL check returns healthy
	mocks.checker.EXPECT().
		Check(gomock.Any(), testURL).
		Return(uri.HealthCheckResult{
			Status: uri.HealthStatusHealthy,
		})

	// Mock: Store error when updating health
	mocks.store.EXPECT().
		UpdateTokenMediaHealthByURL(gomock.Any(), testURL, schema.MediaHealthStatusHealthy, nil).
		Return(errors.New("update failed"))

	// IMPORTANT: Even after update error, the sweeper still checks viewability!
	// Mock: Get tokens after check by IDs (even though update failed)
	mocks.store.EXPECT().
		GetTokensViewabilityByIDs(gomock.Any(), []uint64{}).
		Return([]store.TokenViewabilityInfo{}, nil)

	// No webhook should be triggered (no tokens to compare)

	// Mock clock and sweep
	now := time.Now()
	mocks.clock.EXPECT().Now().Return(now).AnyTimes()
	mocks.clock.EXPECT().Since(now).Return(time.Second).AnyTimes()
	mocks.clock.EXPECT().Sleep(gomock.Any()).AnyTimes()

	gomock.InOrder(
		mocks.store.EXPECT().
			GetMediaURLsNeedingCheck(gomock.Any(), 24*time.Hour, 10).
			Return([]string{testURL}, nil).
			Times(1),
		mocks.store.EXPECT().
			GetMediaURLsNeedingCheck(gomock.Any(), 24*time.Hour, 10).
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

	// Mock for URL1
	mocks.store.EXPECT().
		MarkMediaURLAsChecking(ctx, url1, 24*time.Hour).
		Return(true, nil)

	mocks.store.EXPECT().
		GetTokensViewabilityByMediaURL(ctx, url1).
		Return([]store.TokenViewabilityInfo{
			{TokenID: 1, TokenCID: "token1", IsViewable: false},
		}, nil)

	mocks.checker.EXPECT().
		Check(ctx, url1).
		Return(uri.HealthCheckResult{Status: uri.HealthStatusHealthy})

	mocks.store.EXPECT().
		UpdateTokenMediaHealthByURL(ctx, url1, schema.MediaHealthStatusHealthy, nil).
		Return(nil)

	mocks.store.EXPECT().
		GetTokensViewabilityByIDs(ctx, []uint64{1}).
		Return([]store.TokenViewabilityInfo{
			{TokenID: 1, TokenCID: "token1", IsViewable: true},
		}, nil)

	mocks.orchestrator.EXPECT().
		ExecuteWorkflow(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(client.WorkflowRun(nil), nil)

	// Mock for URL2
	mocks.store.EXPECT().
		MarkMediaURLAsChecking(ctx, url2, 24*time.Hour).
		Return(true, nil)

	mocks.store.EXPECT().
		GetTokensViewabilityByMediaURL(ctx, url2).
		Return([]store.TokenViewabilityInfo{
			{TokenID: 2, TokenCID: "token2", IsViewable: false},
		}, nil)

	mocks.checker.EXPECT().
		Check(ctx, url2).
		Return(uri.HealthCheckResult{Status: uri.HealthStatusHealthy})

	mocks.store.EXPECT().
		UpdateTokenMediaHealthByURL(ctx, url2, schema.MediaHealthStatusHealthy, nil).
		Return(nil)

	mocks.store.EXPECT().
		GetTokensViewabilityByIDs(ctx, []uint64{2}).
		Return([]store.TokenViewabilityInfo{
			{TokenID: 2, TokenCID: "token2", IsViewable: true},
		}, nil)

	mocks.orchestrator.EXPECT().
		ExecuteWorkflow(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(client.WorkflowRun(nil), nil)

	// Mock clock and sweep
	now := time.Now()
	mocks.clock.EXPECT().Now().Return(now).AnyTimes()
	mocks.clock.EXPECT().Since(now).Return(time.Second).AnyTimes()
	mocks.clock.EXPECT().Sleep(gomock.Any()).AnyTimes()

	mocks.store.EXPECT().
		GetMediaURLsNeedingCheck(ctx, 24*time.Hour, 10).
		Return([]string{url1, url2}, nil).
		Times(1)

	mocks.store.EXPECT().
		GetMediaURLsNeedingCheck(ctx, 24*time.Hour, 10).
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
		GetMediaURLsNeedingCheck(ctx, 24*time.Hour, 10).
		Return([]string{}, nil).
		AnyTimes()

	mocks.clock.EXPECT().Now().Return(time.Now()).AnyTimes()
	mocks.clock.EXPECT().Sleep(gomock.Any()).AnyTimes()

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

func TestMediaHealthSweeper_URLAlreadyBeingChecked(t *testing.T) {
	mocks := setupTestSweeper(t)
	defer tearDownTestSweeper(mocks)

	ctx := context.Background()
	testURL := "https://example.com/image.jpg"

	// Mock: Mark URL as checking fails (another instance is checking it)
	mocks.store.EXPECT().
		MarkMediaURLAsChecking(gomock.Any(), testURL, 24*time.Hour).
		Return(false, nil)

	// No other store calls should happen (URL is skipped)

	// Mock clock and sweep
	now := time.Now()
	mocks.clock.EXPECT().Now().Return(now).AnyTimes()
	mocks.clock.EXPECT().Since(now).Return(time.Second).AnyTimes()
	mocks.clock.EXPECT().Sleep(gomock.Any()).AnyTimes()

	gomock.InOrder(
		mocks.store.EXPECT().
			GetMediaURLsNeedingCheck(gomock.Any(), 24*time.Hour, 10).
			Return([]string{testURL}, nil).
			Times(1),
		mocks.store.EXPECT().
			GetMediaURLsNeedingCheck(gomock.Any(), 24*time.Hour, 10).
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

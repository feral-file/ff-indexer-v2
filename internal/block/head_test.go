package block_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/feral-file/ff-indexer-v2/internal/block"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
)

func TestMain(m *testing.M) {
	// Initialize logger for tests
	err := logger.Initialize(logger.Config{
		Debug: false,
	})
	if err != nil {
		panic(err)
	}

	code := m.Run()
	os.Exit(code)
}

// testBlockHeadProviderMocks contains all the mocks needed for testing the block head provider
type testBlockHeadProviderMocks struct {
	ctrl       *gomock.Controller
	fetcher    *mocks.MockBlockFetcher
	clock      *mocks.MockClock
	provider   block.BlockHeadProvider
	testConfig block.Config
}

// setupTest creates all the mocks and block head provider for testing
func setupTest(t *testing.T) *testBlockHeadProviderMocks {
	ctrl := gomock.NewController(t)

	mockFetcher := mocks.NewMockBlockFetcher(ctrl)
	mockClock := mocks.NewMockClock(ctrl)

	testConfig := block.Config{
		TTL:         10 * time.Second,
		StaleWindow: 2 * time.Minute,
	}

	provider := block.NewBlockHeadProvider(mockFetcher, testConfig, mockClock)

	return &testBlockHeadProviderMocks{
		ctrl:       ctrl,
		fetcher:    mockFetcher,
		clock:      mockClock,
		provider:   provider,
		testConfig: testConfig,
	}
}

// tearDownTest cleans up the test mocks
func tearDownTest(tm *testBlockHeadProviderMocks) {
	tm.ctrl.Finish()
}

func TestBlockHeadProvider_GetLatestBlock_FirstFetch(t *testing.T) {
	tm := setupTest(t)
	defer tearDownTest(tm)

	ctx := context.Background()
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	tm.clock.EXPECT().Now().Return(now)
	tm.fetcher.EXPECT().FetchLatestBlock(ctx).Return(uint64(1000), nil)

	// Act
	blockNum, err := tm.provider.GetLatestBlock(ctx)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, uint64(1000), blockNum)
}

func TestBlockHeadProvider_GetLatestBlock_UsesCache_WithinTTL(t *testing.T) {
	tm := setupTest(t)
	defer tearDownTest(tm)

	ctx := context.Background()
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// First fetch - cache miss
	tm.clock.EXPECT().Now().Return(now)
	tm.fetcher.EXPECT().FetchLatestBlock(ctx).Return(uint64(1000), nil)

	blockNum1, err1 := tm.provider.GetLatestBlock(ctx)
	assert.NoError(t, err1)
	assert.Equal(t, uint64(1000), blockNum1)

	// Second fetch - should use cache (within TTL)
	tm.clock.EXPECT().Now().Return(now.Add(5 * time.Second))

	// Act
	blockNum2, err2 := tm.provider.GetLatestBlock(ctx)

	// Assert
	assert.NoError(t, err2)
	assert.Equal(t, uint64(1000), blockNum2) // Should return cached value - fetcher called only once
}

func TestBlockHeadProvider_GetLatestBlock_RefreshesCache_AfterTTL(t *testing.T) {
	tm := setupTest(t)
	defer tearDownTest(tm)

	ctx := context.Background()
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// First fetch - cache miss
	tm.clock.EXPECT().Now().Return(now)
	tm.fetcher.EXPECT().FetchLatestBlock(ctx).Return(uint64(1000), nil)

	blockNum1, err1 := tm.provider.GetLatestBlock(ctx)
	assert.NoError(t, err1)
	assert.Equal(t, uint64(1000), blockNum1)

	// Second fetch - after TTL expires
	laterTime := now.Add(15 * time.Second) // Beyond TTL
	tm.clock.EXPECT().Now().Return(laterTime)
	tm.fetcher.EXPECT().FetchLatestBlock(ctx).Return(uint64(1100), nil)

	// Act
	blockNum2, err2 := tm.provider.GetLatestBlock(ctx)

	// Assert
	assert.NoError(t, err2)
	assert.Equal(t, uint64(1100), blockNum2) // Should return new value
}

func TestBlockHeadProvider_GetLatestBlock_UsesStaleCacheOnError_WithinStaleWindow(t *testing.T) {
	tm := setupTest(t)
	defer tearDownTest(tm)

	ctx := context.Background()
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// First fetch - successful
	tm.clock.EXPECT().Now().Return(now)
	tm.fetcher.EXPECT().FetchLatestBlock(ctx).Return(uint64(1000), nil)

	blockNum1, err1 := tm.provider.GetLatestBlock(ctx)
	assert.NoError(t, err1)
	assert.Equal(t, uint64(1000), blockNum1)

	// Second fetch - after TTL expires but fetch fails
	laterTime := now.Add(30 * time.Second) // Beyond TTL but within StaleWindow
	tm.clock.EXPECT().Now().Return(laterTime)
	fetchError := errors.New("network error")
	tm.fetcher.EXPECT().FetchLatestBlock(ctx).Return(uint64(0), fetchError)

	// Act
	blockNum2, err2 := tm.provider.GetLatestBlock(ctx)

	// Assert - should use stale cache as fallback
	assert.NoError(t, err2)
	assert.Equal(t, uint64(1000), blockNum2) // Should return stale cached value
}

func TestBlockHeadProvider_GetLatestBlock_ReturnsError_WhenNoCache_AndFetchFails(t *testing.T) {
	tm := setupTest(t)
	defer tearDownTest(tm)

	ctx := context.Background()
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	tm.clock.EXPECT().Now().Return(now)
	fetchError := errors.New("network error")
	tm.fetcher.EXPECT().FetchLatestBlock(ctx).Return(uint64(0), fetchError)

	// Act
	blockNum, err := tm.provider.GetLatestBlock(ctx)

	// Assert
	assert.Error(t, err)
	assert.Equal(t, uint64(0), blockNum)
	assert.Contains(t, err.Error(), "failed to fetch latest block and no valid cache available")
}

func TestBlockHeadProvider_GetLatestBlock_ReturnsError_WhenStaleCache_BeyondStaleWindow(t *testing.T) {
	tm := setupTest(t)
	defer tearDownTest(tm)

	ctx := context.Background()
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// First fetch - successful
	tm.clock.EXPECT().Now().Return(now)
	tm.fetcher.EXPECT().FetchLatestBlock(ctx).Return(uint64(1000), nil)

	blockNum1, err1 := tm.provider.GetLatestBlock(ctx)
	assert.NoError(t, err1)
	assert.Equal(t, uint64(1000), blockNum1)

	// Second fetch - way beyond StaleWindow and fetch fails
	laterTime := now.Add(5 * time.Minute) // Beyond StaleWindow (2 minutes)
	tm.clock.EXPECT().Now().Return(laterTime)
	fetchError := errors.New("network error")
	tm.fetcher.EXPECT().FetchLatestBlock(ctx).Return(uint64(0), fetchError)

	// Act
	blockNum2, err2 := tm.provider.GetLatestBlock(ctx)

	// Assert - should return error as stale cache is too old
	assert.Error(t, err2)
	assert.Equal(t, uint64(0), blockNum2)
	assert.Contains(t, err2.Error(), "failed to fetch latest block and no valid cache available")
}

func TestBlockHeadProvider_ConcurrentAccess(t *testing.T) {
	tm := setupTest(t)
	defer tearDownTest(tm)

	ctx := context.Background()
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Mock the fetcher to return - AnyTimes() allows multiple concurrent calls
	tm.fetcher.EXPECT().FetchLatestBlock(ctx).Return(uint64(1000), nil).AnyTimes()
	tm.clock.EXPECT().Now().Return(now).AnyTimes()

	// Act - concurrent access
	done := make(chan bool, 10)
	for range 10 {
		go func() {
			blockNum, err := tm.provider.GetLatestBlock(ctx)
			assert.NoError(t, err)
			assert.Equal(t, uint64(1000), blockNum)
			done <- true
		}()
	}

	// Wait for all goroutines to complete
	for range 10 {
		<-done
	}
}

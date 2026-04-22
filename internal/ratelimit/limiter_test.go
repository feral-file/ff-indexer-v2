package ratelimit_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/config"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/ratelimit"
)

func TestMain(m *testing.M) {
	err := logger.Initialize(logger.Config{
		Debug: false,
	})
	if err != nil {
		panic(err)
	}

	code := m.Run()
	os.Exit(code)
}

func testConfig() config.RateLimiterConfig {
	return config.RateLimiterConfig{
		MaxWorkers:   2,
		MaxQueueSize: 10,
		Providers: map[string]config.RateLimitConfig{
			"test-provider": {
				RequestsPerSecond: 1000,
				Burst:             1000,
				MaxQueueTime:      100 * time.Millisecond,
			},
		},
	}
}

func TestNewLimiter_Success(t *testing.T) {
	limiter, err := ratelimit.NewLimiter(testConfig())
	require.NoError(t, err)
	require.NotNil(t, limiter)
	assert.NoError(t, limiter.Close())
}

func TestNewLimiter_InvalidConfig_NoProviders(t *testing.T) {
	limiter, err := ratelimit.NewLimiter(config.RateLimiterConfig{})
	require.Error(t, err)
	assert.Nil(t, limiter)
	assert.Contains(t, err.Error(), "at least one provider must be configured")
}

func TestNewLimiter_InvalidConfig_InvalidRPS(t *testing.T) {
	limiter, err := ratelimit.NewLimiter(config.RateLimiterConfig{
		Providers: map[string]config.RateLimitConfig{
			"test-provider": {RequestsPerSecond: 0},
		},
	})
	require.Error(t, err)
	assert.Nil(t, limiter)
	assert.Contains(t, err.Error(), "requests_per_second must be positive")
}

func TestDo_Success(t *testing.T) {
	limiter, err := ratelimit.NewLimiter(testConfig())
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, limiter.Close())
	})

	result, err := limiter.Do(context.Background(), "test-provider", func(ctx context.Context) (interface{}, error) {
		return "success", nil
	})

	require.NoError(t, err)
	assert.Equal(t, "success", result)
}

func TestDo_UnknownProvider(t *testing.T) {
	limiter, err := ratelimit.NewLimiter(testConfig())
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, limiter.Close())
	})

	result, err := limiter.Do(context.Background(), "missing-provider", func(ctx context.Context) (interface{}, error) {
		return "success", nil
	})

	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "provider 'missing-provider' not configured")
}

func TestDo_RequestFunctionError(t *testing.T) {
	limiter, err := ratelimit.NewLimiter(testConfig())
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, limiter.Close())
	})

	expectedErr := errors.New("request failed")
	result, err := limiter.Do(context.Background(), "test-provider", func(ctx context.Context) (interface{}, error) {
		return nil, expectedErr
	})

	require.ErrorIs(t, err, expectedErr)
	assert.Nil(t, result)
}

func TestDo_ContextCanceled(t *testing.T) {
	limiter, err := ratelimit.NewLimiter(testConfig())
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, limiter.Close())
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, err := limiter.Do(ctx, "test-provider", func(ctx context.Context) (interface{}, error) {
		return "success", nil
	})

	require.Error(t, err)
	assert.Nil(t, result)
}

func TestDo_QueueTimeout(t *testing.T) {
	cfg := config.RateLimiterConfig{
		MaxWorkers:   2,
		MaxQueueSize: 1,
		Providers: map[string]config.RateLimitConfig{
			"test-provider": {
				RequestsPerSecond: 1,
				Burst:             1,
				MaxQueueTime:      20 * time.Millisecond,
			},
		},
	}

	limiter, err := ratelimit.NewLimiter(cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, limiter.Close())
	})

	release := make(chan struct{})
	firstDone := make(chan struct{})

	go func() {
		defer close(firstDone)
		_, _ = limiter.Do(context.Background(), "test-provider", func(ctx context.Context) (interface{}, error) {
			<-release
			return "first", nil
		})
	}()

	time.Sleep(10 * time.Millisecond)

	result, err := limiter.Do(context.Background(), "test-provider", func(ctx context.Context) (interface{}, error) {
		return "second", nil
	})

	require.Error(t, err)
	assert.Nil(t, result)

	close(release)
	<-firstDone
}

func TestDo_LimiterClosed(t *testing.T) {
	limiter, err := ratelimit.NewLimiter(testConfig())
	require.NoError(t, err)
	require.NoError(t, limiter.Close())

	result, err := limiter.Do(context.Background(), "test-provider", func(ctx context.Context) (interface{}, error) {
		return "success", nil
	})

	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "limiter is closed")
}

func TestClose_Idempotent(t *testing.T) {
	limiter, err := ratelimit.NewLimiter(testConfig())
	require.NoError(t, err)

	assert.NoError(t, limiter.Close())
	assert.NoError(t, limiter.Close())
}

package ratelimit_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/go-redis/redis_rate/v10"
	"github.com/golang/mock/gomock"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"

	"github.com/feral-file/ff-indexer-v2/internal/config"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/ratelimit"
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

// testProxyMocks contains all the mocks needed for testing the proxy
type testProxyMocks struct {
	ctrl             *gomock.Controller
	redisClient      *mocks.MockRedisClient
	redisRateLimiter *mocks.MockRedisRateLimiter
	clock            *mocks.MockClock
}

// setupTestProxy creates all the mocks for testing
func setupTestProxy(t *testing.T) *testProxyMocks {
	ctrl := gomock.NewController(t)

	tm := &testProxyMocks{
		ctrl:             ctrl,
		redisClient:      mocks.NewMockRedisClient(ctrl),
		redisRateLimiter: mocks.NewMockRedisRateLimiter(ctrl),
		clock:            mocks.NewMockClock(ctrl),
	}

	return tm
}

// tearDownTestProxy cleans up the test mocks
func tearDownTestProxy(mocks *testProxyMocks) {
	mocks.ctrl.Finish()
}

// setupProxyWithMocks creates a proxy with common mock expectations
func setupProxyWithMocks(t *testing.T, mocks *testProxyMocks, cfg config.RateLimiterConfig, redisAvailable bool) (ratelimit.Proxy, *time.Ticker) {
	// Mock Redis ping
	statusCmd := redis.NewStatusCmd(context.Background())
	if redisAvailable {
		statusCmd.SetVal("PONG")
	} else {
		statusCmd.SetErr(errors.New("connection refused"))
	}
	mocks.redisClient.EXPECT().
		Ping(gomock.Any()).
		Return(statusCmd)

	// Mock rate limiter creation
	mocks.redisClient.EXPECT().
		NewRateLimiter().
		Return(mocks.redisRateLimiter)

	// Mock ticker for health monitoring goroutine
	ticker := time.NewTicker(10 * time.Second)
	mocks.clock.EXPECT().
		NewTicker(10 * time.Second).
		Return(ticker)

	proxy, err := ratelimit.NewProxy(cfg, mocks.redisClient, mocks.clock)
	assert.NoError(t, err)

	// Give the monitoring goroutine time to start
	time.Sleep(15 * time.Millisecond)

	return proxy, ticker
}

func TestNewProxy_Success(t *testing.T) {
	mocks := setupTestProxy(t)
	defer tearDownTestProxy(mocks)

	cfg := config.RateLimiterConfig{
		RedisAddr:               "localhost:6379",
		RedisKeyPrefix:          "test:limiter:",
		MaxWorkers:              10,
		MaxQueueSize:            100,
		EnableLocalFallback:     true,
		LocalFallbackMultiplier: 0.5,
		Providers: map[string]config.RateLimitConfig{
			"test-provider": {
				RequestsPerSecond: 10,
				Burst:             20,
				MaxQueueTime:      5 * time.Minute,
			},
		},
	}

	proxy, ticker := setupProxyWithMocks(t, mocks, cfg, true)
	assert.NotNil(t, proxy)

	// Clean up
	ticker.Stop()
	mocks.redisClient.EXPECT().Close().Return(nil).AnyTimes()
	_ = proxy.Close()
}

func TestNewProxy_RedisUnavailable_FallbackEnabled(t *testing.T) {
	mocks := setupTestProxy(t)
	defer tearDownTestProxy(mocks)

	cfg := config.RateLimiterConfig{
		RedisAddr:               "localhost:6379",
		RedisKeyPrefix:          "test:limiter:",
		MaxWorkers:              10,
		MaxQueueSize:            100,
		EnableLocalFallback:     true,
		LocalFallbackMultiplier: 0.5,
		Providers: map[string]config.RateLimitConfig{
			"test-provider": {
				RequestsPerSecond: 10,
				Burst:             20,
				MaxQueueTime:      5 * time.Minute,
			},
		},
	}

	proxy, ticker := setupProxyWithMocks(t, mocks, cfg, false)

	// Should succeed with fallback enabled
	assert.NotNil(t, proxy)

	// Clean up
	ticker.Stop()
	mocks.redisClient.EXPECT().Close().Return(nil).AnyTimes()
	_ = proxy.Close()
}

func TestNewProxy_RedisUnavailable_FallbackDisabled(t *testing.T) {
	mocks := setupTestProxy(t)
	defer tearDownTestProxy(mocks)

	cfg := config.RateLimiterConfig{
		RedisAddr:           "localhost:6379",
		RedisKeyPrefix:      "test:limiter:",
		MaxWorkers:          10,
		MaxQueueSize:        100,
		EnableLocalFallback: false,
		Providers: map[string]config.RateLimitConfig{
			"test-provider": {
				RequestsPerSecond: 10,
				Burst:             20,
				MaxQueueTime:      5 * time.Minute,
			},
		},
	}

	// Mock Redis ping failure
	statusCmd := redis.NewStatusCmd(context.Background())
	statusCmd.SetErr(errors.New("connection refused"))
	mocks.redisClient.EXPECT().
		Ping(gomock.Any()).
		Return(statusCmd)

	proxy, err := ratelimit.NewProxy(cfg, mocks.redisClient, mocks.clock)

	// Should fail without fallback
	assert.Error(t, err)
	assert.Nil(t, proxy)
	assert.Contains(t, err.Error(), "redis unavailable and fallback disabled")
}

func TestNewProxy_InvalidConfig_NoRedisAddr(t *testing.T) {
	mocks := setupTestProxy(t)
	defer tearDownTestProxy(mocks)

	cfg := config.RateLimiterConfig{
		RedisAddr: "",
		Providers: map[string]config.RateLimitConfig{
			"test-provider": {RequestsPerSecond: 10},
		},
	}

	proxy, err := ratelimit.NewProxy(cfg, mocks.redisClient, mocks.clock)

	assert.Error(t, err)
	assert.Nil(t, proxy)
	assert.Contains(t, err.Error(), "redis_addr is required")
}

func TestNewProxy_InvalidConfig_NoProviders(t *testing.T) {
	mocks := setupTestProxy(t)
	defer tearDownTestProxy(mocks)

	cfg := config.RateLimiterConfig{
		RedisAddr: "localhost:6379",
		Providers: map[string]config.RateLimitConfig{},
	}

	proxy, err := ratelimit.NewProxy(cfg, mocks.redisClient, mocks.clock)

	assert.Error(t, err)
	assert.Nil(t, proxy)
	assert.Contains(t, err.Error(), "at least one provider must be configured")
}

func TestNewProxy_InvalidConfig_InvalidRPS(t *testing.T) {
	mocks := setupTestProxy(t)
	defer tearDownTestProxy(mocks)

	cfg := config.RateLimiterConfig{
		RedisAddr: "localhost:6379",
		Providers: map[string]config.RateLimitConfig{
			"test-provider": {RequestsPerSecond: 0},
		},
	}

	proxy, err := ratelimit.NewProxy(cfg, mocks.redisClient, mocks.clock)

	assert.Error(t, err)
	assert.Nil(t, proxy)
	assert.Contains(t, err.Error(), "requests_per_second must be positive")
}

func TestProxy_Request_Success(t *testing.T) {
	mocks := setupTestProxy(t)
	defer tearDownTestProxy(mocks)

	cfg := config.RateLimiterConfig{
		RedisAddr:               "localhost:6379",
		RedisKeyPrefix:          "test:limiter:",
		MaxWorkers:              10,
		MaxQueueSize:            100,
		EnableLocalFallback:     true,
		LocalFallbackMultiplier: 0.5,
		Providers: map[string]config.RateLimitConfig{
			"test-provider": {
				RequestsPerSecond: 10,
				Burst:             20,
				MaxQueueTime:      5 * time.Minute,
			},
		},
	}

	proxy, ticker := setupProxyWithMocks(t, mocks, cfg, true)

	// Mock distributed limiter allowing request
	mocks.redisRateLimiter.EXPECT().
		Allow(gomock.Any(), "test:limiter:test-provider", gomock.Any()).
		Return(&redis_rate.Result{
			Allowed:   1,
			Remaining: 9,
		}, nil)

	// Mock fairness delay after successful token acquisition
	mocks.clock.EXPECT().
		After(gomock.Any()).
		DoAndReturn(func(d time.Duration) <-chan time.Time {
			ch := make(chan time.Time, 1)
			ch <- time.Now()
			return ch
		})

	// Execute request
	ctx := context.Background()
	expectedResult := "success"
	result, err := proxy.Request(ctx, "test-provider", func(ctx context.Context) (interface{}, error) {
		return expectedResult, nil
	})

	assert.NoError(t, err)
	assert.Equal(t, expectedResult, result)

	// Clean up
	ticker.Stop()
	mocks.redisClient.EXPECT().Close().Return(nil).AnyTimes()
	_ = proxy.Close()
}

func TestProxy_Request_UnknownProvider(t *testing.T) {
	mocks := setupTestProxy(t)
	defer tearDownTestProxy(mocks)

	cfg := config.RateLimiterConfig{
		RedisAddr:           "localhost:6379",
		EnableLocalFallback: true,
		Providers: map[string]config.RateLimitConfig{
			"test-provider": {
				RequestsPerSecond: 10,
				Burst:             20,
			},
		},
	}

	proxy, ticker := setupProxyWithMocks(t, mocks, cfg, true)

	ctx := context.Background()
	result, err := proxy.Request(ctx, "unknown-provider", func(ctx context.Context) (interface{}, error) {
		return "success", nil
	})

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "provider 'unknown-provider' not configured")

	// Clean up
	ticker.Stop()
	mocks.redisClient.EXPECT().Close().Return(nil).AnyTimes()
	_ = proxy.Close()
}

func TestProxy_Request_ContextCanceled(t *testing.T) {
	mocks := setupTestProxy(t)
	defer tearDownTestProxy(mocks)

	cfg := config.RateLimiterConfig{
		RedisAddr:           "localhost:6379",
		EnableLocalFallback: true,
		Providers: map[string]config.RateLimitConfig{
			"test-provider": {
				RequestsPerSecond: 10,
				Burst:             20,
				MaxQueueTime:      100 * time.Millisecond,
			},
		},
	}

	proxy, ticker := setupProxyWithMocks(t, mocks, cfg, true)

	// Create a context that's already canceled
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, err := proxy.Request(ctx, "test-provider", func(ctx context.Context) (interface{}, error) {
		return "success", nil
	})

	assert.Error(t, err)
	assert.Nil(t, result)

	// Clean up
	ticker.Stop()
	mocks.redisClient.EXPECT().Close().Return(nil).AnyTimes()
	_ = proxy.Close()
}

func TestProxy_Request_RateLimitExceeded_WithRetryAfter(t *testing.T) {
	mocks := setupTestProxy(t)
	defer tearDownTestProxy(mocks)

	cfg := config.RateLimiterConfig{
		RedisAddr:           "localhost:6379",
		RedisKeyPrefix:      "test:limiter:",
		MaxWorkers:          10,
		MaxQueueSize:        100,
		EnableLocalFallback: true,
		Providers: map[string]config.RateLimitConfig{
			"test-provider": {
				RequestsPerSecond: 10,
				Burst:             20,
				MaxQueueTime:      1 * time.Second,
			},
		},
	}

	proxy, ticker := setupProxyWithMocks(t, mocks, cfg, true)

	// First call: rate limit exceeded with retry after
	// Second call: allowed
	gomock.InOrder(
		mocks.redisRateLimiter.EXPECT().
			Allow(gomock.Any(), "test:limiter:test-provider", gomock.Any()).
			Return(&redis_rate.Result{
				Allowed:    0,
				Remaining:  0,
				RetryAfter: 50 * time.Millisecond,
			}, nil),
		mocks.clock.EXPECT().
			After(gomock.Any()). // Accept any duration due to jitter
			DoAndReturn(func(d time.Duration) <-chan time.Time {
				ch := make(chan time.Time, 1)
				ch <- time.Now()
				return ch
			}),
		mocks.redisRateLimiter.EXPECT().
			Allow(gomock.Any(), "test:limiter:test-provider", gomock.Any()).
			Return(&redis_rate.Result{
				Allowed:   1,
				Remaining: 9,
			}, nil),
		mocks.clock.EXPECT().
			After(gomock.Any()). // Fairness delay after successful token acquisition
			DoAndReturn(func(d time.Duration) <-chan time.Time {
				ch := make(chan time.Time, 1)
				ch <- time.Now()
				return ch
			}),
	)

	ctx := context.Background()
	result, err := proxy.Request(ctx, "test-provider", func(ctx context.Context) (interface{}, error) {
		return "success", nil
	})

	assert.NoError(t, err)
	assert.Equal(t, "success", result)

	// Clean up
	ticker.Stop()
	mocks.redisClient.EXPECT().Close().Return(nil).AnyTimes()
	_ = proxy.Close()
}

func TestProxy_Request_RedisFailure_FallbackToLocal(t *testing.T) {
	mocks := setupTestProxy(t)
	defer tearDownTestProxy(mocks)

	cfg := config.RateLimiterConfig{
		RedisAddr:               "localhost:6379",
		RedisKeyPrefix:          "test:limiter:",
		MaxWorkers:              10,
		MaxQueueSize:            100,
		EnableLocalFallback:     true,
		LocalFallbackMultiplier: 0.5,
		Providers: map[string]config.RateLimitConfig{
			"test-provider": {
				RequestsPerSecond: 10,
				Burst:             20,
				MaxQueueTime:      5 * time.Minute,
			},
		},
	}

	proxy, ticker := setupProxyWithMocks(t, mocks, cfg, true)

	// Mock distributed limiter returning error (Redis failure)
	mocks.redisRateLimiter.EXPECT().
		Allow(gomock.Any(), "test:limiter:test-provider", gomock.Any()).
		Return(nil, errors.New("redis connection error"))

	// Should fallback to local limiter
	ctx := context.Background()
	result, err := proxy.Request(ctx, "test-provider", func(ctx context.Context) (interface{}, error) {
		return "success with fallback", nil
	})

	assert.NoError(t, err)
	assert.Equal(t, "success with fallback", result)

	// Clean up
	ticker.Stop()
	mocks.redisClient.EXPECT().Close().Return(nil).AnyTimes()
	_ = proxy.Close()
}

func TestProxy_Request_RequestFunctionError(t *testing.T) {
	mocks := setupTestProxy(t)
	defer tearDownTestProxy(mocks)

	cfg := config.RateLimiterConfig{
		RedisAddr:           "localhost:6379",
		EnableLocalFallback: true,
		Providers: map[string]config.RateLimitConfig{
			"test-provider": {
				RequestsPerSecond: 10,
				Burst:             20,
			},
		},
	}

	proxy, ticker := setupProxyWithMocks(t, mocks, cfg, true)

	// Mock distributed limiter allowing request
	mocks.redisRateLimiter.EXPECT().
		Allow(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&redis_rate.Result{Allowed: 1}, nil)

	// Mock fairness delay after successful token acquisition
	mocks.clock.EXPECT().
		After(gomock.Any()).
		DoAndReturn(func(d time.Duration) <-chan time.Time {
			ch := make(chan time.Time, 1)
			ch <- time.Now()
			return ch
		})

	ctx := context.Background()
	expectedError := errors.New("request failed")
	result, err := proxy.Request(ctx, "test-provider", func(ctx context.Context) (interface{}, error) {
		return nil, expectedError
	})

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Equal(t, expectedError, err)

	// Clean up
	ticker.Stop()
	mocks.redisClient.EXPECT().Close().Return(nil).AnyTimes()
	_ = proxy.Close()
}

func TestProxy_Request_ProxyClosed(t *testing.T) {
	mocks := setupTestProxy(t)
	defer tearDownTestProxy(mocks)

	cfg := config.RateLimiterConfig{
		RedisAddr:           "localhost:6379",
		EnableLocalFallback: true,
		Providers: map[string]config.RateLimitConfig{
			"test-provider": {
				RequestsPerSecond: 10,
				Burst:             20,
			},
		},
	}

	proxy, ticker := setupProxyWithMocks(t, mocks, cfg, true)

	// Close the proxy
	mocks.redisClient.EXPECT().Close().Return(nil)

	// Stop ticker first
	ticker.Stop()

	_ = proxy.Close()

	// Try to make a request after closing
	ctx := context.Background()
	result, err := proxy.Request(ctx, "test-provider", func(ctx context.Context) (interface{}, error) {
		return "success", nil
	})

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "proxy is closed")
}

func TestProxy_Close_Multiple(t *testing.T) {
	mocks := setupTestProxy(t)
	defer tearDownTestProxy(mocks)

	cfg := config.RateLimiterConfig{
		RedisAddr:           "localhost:6379",
		EnableLocalFallback: true,
		Providers: map[string]config.RateLimitConfig{
			"test-provider": {
				RequestsPerSecond: 10,
				Burst:             20,
			},
		},
	}

	proxy, ticker := setupProxyWithMocks(t, mocks, cfg, true)

	// Give the monitoring goroutine time to start
	time.Sleep(10 * time.Millisecond)

	// Close should be called only once due to sync.Once
	mocks.redisClient.EXPECT().Close().Return(nil).Times(1)

	// Stop ticker first
	ticker.Stop()

	// Call Close multiple times
	err1 := proxy.Close()
	err2 := proxy.Close()
	err3 := proxy.Close()

	assert.NoError(t, err1)
	assert.NoError(t, err2)
	assert.NoError(t, err3)
}

func TestProxy_Request_Concurrent(t *testing.T) {
	mocks := setupTestProxy(t)
	defer tearDownTestProxy(mocks)

	cfg := config.RateLimiterConfig{
		RedisAddr:           "localhost:6379",
		RedisKeyPrefix:      "test:limiter:",
		MaxWorkers:          5,
		MaxQueueSize:        100,
		EnableLocalFallback: true,
		Providers: map[string]config.RateLimitConfig{
			"test-provider": {
				RequestsPerSecond: 10,
				Burst:             20,
				MaxQueueTime:      5 * time.Minute,
			},
		},
	}

	proxy, ticker := setupProxyWithMocks(t, mocks, cfg, true)

	// Mock distributed limiter allowing all requests
	mocks.redisRateLimiter.EXPECT().
		Allow(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&redis_rate.Result{Allowed: 1}, nil).
		MinTimes(3)

	// Mock fairness delay after successful token acquisition for each request
	mocks.clock.EXPECT().
		After(gomock.Any()).
		DoAndReturn(func(d time.Duration) <-chan time.Time {
			ch := make(chan time.Time, 1)
			ch <- time.Now()
			return ch
		}).
		MinTimes(3)

	ctx := context.Background()
	done := make(chan bool, 3)

	// Execute concurrent requests
	for i := range 3 {
		go func(id int) {
			result, err := proxy.Request(ctx, "test-provider", func(ctx context.Context) (interface{}, error) {
				time.Sleep(10 * time.Millisecond)
				return id, nil
			})
			assert.NoError(t, err)
			assert.NotNil(t, result)
			done <- true
		}(i)
	}

	// Wait for all requests to complete
	for range 3 {
		<-done
	}

	// Clean up
	ticker.Stop()
	mocks.redisClient.EXPECT().Close().Return(nil).AnyTimes()
	_ = proxy.Close()
}

func TestProxy_Request_MultipleProviders(t *testing.T) {
	mocks := setupTestProxy(t)
	defer tearDownTestProxy(mocks)

	cfg := config.RateLimiterConfig{
		RedisAddr:           "localhost:6379",
		RedisKeyPrefix:      "test:limiter:",
		MaxWorkers:          10,
		MaxQueueSize:        100,
		EnableLocalFallback: true,
		Providers: map[string]config.RateLimitConfig{
			"provider-1": {
				RequestsPerSecond: 10,
				Burst:             20,
			},
			"provider-2": {
				RequestsPerSecond: 5,
				Burst:             10,
			},
		},
	}

	proxy, ticker := setupProxyWithMocks(t, mocks, cfg, true)

	ctx := context.Background()

	// Test provider-1
	mocks.redisRateLimiter.EXPECT().
		Allow(gomock.Any(), "test:limiter:provider-1", gomock.Any()).
		Return(&redis_rate.Result{Allowed: 1}, nil)

	// Mock fairness delay for provider-1
	mocks.clock.EXPECT().
		After(gomock.Any()).
		DoAndReturn(func(d time.Duration) <-chan time.Time {
			ch := make(chan time.Time, 1)
			ch <- time.Now()
			return ch
		})

	result1, err := proxy.Request(ctx, "provider-1", func(ctx context.Context) (interface{}, error) {
		return "provider-1-result", nil
	})
	assert.NoError(t, err)
	assert.Equal(t, "provider-1-result", result1)

	// Test provider-2
	mocks.redisRateLimiter.EXPECT().
		Allow(gomock.Any(), "test:limiter:provider-2", gomock.Any()).
		Return(&redis_rate.Result{Allowed: 1}, nil)

	// Mock fairness delay for provider-2
	mocks.clock.EXPECT().
		After(gomock.Any()).
		DoAndReturn(func(d time.Duration) <-chan time.Time {
			ch := make(chan time.Time, 1)
			ch <- time.Now()
			return ch
		})

	result2, err := proxy.Request(ctx, "provider-2", func(ctx context.Context) (interface{}, error) {
		return "provider-2-result", nil
	})
	assert.NoError(t, err)
	assert.Equal(t, "provider-2-result", result2)

	// Clean up
	ticker.Stop()
	mocks.redisClient.EXPECT().Close().Return(nil).AnyTimes()
	_ = proxy.Close()
}

func TestProxy_RedisFailure_NoFallback(t *testing.T) {
	mocks := setupTestProxy(t)
	defer tearDownTestProxy(mocks)

	cfg := config.RateLimiterConfig{
		RedisAddr:           "localhost:6379",
		RedisKeyPrefix:      "test:limiter:",
		MaxWorkers:          10,
		MaxQueueSize:        100,
		EnableLocalFallback: false, // Fallback disabled
		Providers: map[string]config.RateLimitConfig{
			"test-provider": {
				RequestsPerSecond: 10,
				Burst:             20,
				MaxQueueTime:      5 * time.Minute,
			},
		},
	}

	proxy, ticker := setupProxyWithMocks(t, mocks, cfg, true)

	// Mock distributed limiter returning error (Redis failure)
	mocks.redisRateLimiter.EXPECT().
		Allow(gomock.Any(), "test:limiter:test-provider", gomock.Any()).
		Return(nil, errors.New("redis connection error"))

	ctx := context.Background()
	result, err := proxy.Request(ctx, "test-provider", func(ctx context.Context) (interface{}, error) {
		return "success", nil
	})

	// Should fail because fallback is disabled
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "redis rate limiter unavailable")

	// Clean up
	ticker.Stop()
	mocks.redisClient.EXPECT().Close().Return(nil).AnyTimes()
	_ = proxy.Close()
}

func TestProxy_Request_QueueTimeout(t *testing.T) {
	mocks := setupTestProxy(t)
	defer tearDownTestProxy(mocks)

	cfg := config.RateLimiterConfig{
		RedisAddr:           "localhost:6379",
		RedisKeyPrefix:      "test:limiter:",
		MaxWorkers:          1, // Only 1 worker to cause queueing
		MaxQueueSize:        10,
		EnableLocalFallback: true,
		Providers: map[string]config.RateLimitConfig{
			"test-provider": {
				RequestsPerSecond: 1,
				Burst:             1,
				MaxQueueTime:      50 * time.Millisecond, // Very short timeout
			},
		},
	}

	proxy, ticker := setupProxyWithMocks(t, mocks, cfg, true)

	// Mock rate limiter to always return "rate limited" to force waiting
	mocks.redisRateLimiter.EXPECT().
		Allow(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&redis_rate.Result{
			Allowed:    0,
			Remaining:  0,
			RetryAfter: 1 * time.Second, // Long retry after
		}, nil).
		AnyTimes()

	mocks.clock.EXPECT().
		After(gomock.Any()).
		DoAndReturn(func(d time.Duration) <-chan time.Time {
			// Never send on the channel to simulate waiting
			return make(chan time.Time)
		}).
		AnyTimes()

	ctx := context.Background()
	result, err := proxy.Request(ctx, "test-provider", func(ctx context.Context) (interface{}, error) {
		return "success", nil
	})

	// Should timeout due to MaxQueueTime
	assert.Error(t, err)
	assert.Nil(t, result)

	// Clean up
	ticker.Stop()
	mocks.redisClient.EXPECT().Close().Return(nil).AnyTimes()
	_ = proxy.Close()
}

func TestProxy_Close_WithRedisError(t *testing.T) {
	mocks := setupTestProxy(t)
	defer tearDownTestProxy(mocks)

	cfg := config.RateLimiterConfig{
		RedisAddr:           "localhost:6379",
		EnableLocalFallback: true,
		Providers: map[string]config.RateLimitConfig{
			"test-provider": {
				RequestsPerSecond: 10,
				Burst:             20,
			},
		},
	}

	proxy, ticker := setupProxyWithMocks(t, mocks, cfg, true)

	// Mock Redis close returning an error
	mocks.redisClient.EXPECT().Close().Return(errors.New("close error"))

	// Stop ticker first
	ticker.Stop()

	err := proxy.Close()

	// Error should be returned but operation should complete
	assert.Error(t, err)
}

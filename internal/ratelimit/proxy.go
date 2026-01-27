package ratelimit

import (
	"context"
	"fmt"
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alitto/pond/v2"
	"github.com/go-redis/redis_rate/v10"
	"go.uber.org/zap"
	"golang.org/x/time/rate"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/config"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
)

// RequestFunc is a function that performs the actual API request
// It receives a context and returns the result and any error
type RequestFunc func(ctx context.Context) (interface{}, error)

// requestResult wraps the result and error of a request
type requestResult struct {
	value interface{}
	err   error
}

// Proxy defines the interface for rate-limiting proxy
//
//go:generate mockgen -source=proxy.go -destination=../mocks/ratelimit_proxy.go -package=mocks -mock_names=Proxy=MockRateLimitProxy
type Proxy interface {
	// Request submits a rate-limited request for execution
	Request(ctx context.Context, providerName string, fn RequestFunc) (interface{}, error)

	// Close gracefully shuts down the proxy
	Close() error
}

// proxy is the concrete implementation of the rate-limiting proxy
type proxy struct {
	config         config.RateLimiterConfig
	pool           pond.ResultPool[*requestResult]
	limiters       map[string]*providerLimiter
	redis          adapter.RedisClient
	clock          adapter.Clock
	closed         atomic.Bool
	closeOnce      sync.Once
	redisAvailable atomic.Bool
}

// providerLimiter holds the rate limiting state for a single provider
type providerLimiter struct {
	name               string
	config             config.RateLimitConfig
	distributedLimiter adapter.RedisRateLimiter
	localLimiter       *rate.Limiter
	preFilterLimiter   *rate.Limiter
}

// NewProxy creates a new rate-limiting proxy
func NewProxy(cfg config.RateLimiterConfig, rc adapter.RedisClient, clock adapter.Clock) (Proxy, error) {
	// Validate and set defaults
	if err := validateConfig(&cfg); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	// Test Redis connectivity
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	redisAvailable := true
	if err := rc.Ping(ctx).Err(); err != nil {
		redisAvailable = false
		if !cfg.EnableLocalFallback {
			return nil, fmt.Errorf("redis unavailable and fallback disabled: %w", err)
		}
		logger.Warn("Redis unavailable, will use local fallback", zap.Error(err))
	}

	// Create distributed rate limiter
	distributedLimiter := rc.NewRateLimiter()

	// Create provider limiters
	limiters := make(map[string]*providerLimiter)
	for name, providerConfig := range cfg.Providers {
		// Create local fallback limiter with adjusted rate
		// Minimum rate of 1.0
		localRate := max(float64(providerConfig.RequestsPerSecond)*cfg.LocalFallbackMultiplier, 1.0)
		localLimiter := rate.NewLimiter(rate.Limit(localRate), providerConfig.Burst)

		// Create pre-filter limiter with the same rate as the provider
		// This reduces Redis pressure while maintaining full throughput
		preFilterLimiter := rate.NewLimiter(rate.Limit(providerConfig.RequestsPerSecond), providerConfig.Burst)

		limiters[name] = &providerLimiter{
			name:               name,
			config:             providerConfig,
			distributedLimiter: distributedLimiter,
			localLimiter:       localLimiter,
			preFilterLimiter:   preFilterLimiter,
		}
	}

	// Calculate pool size
	maxWorkers := cfg.MaxWorkers

	// Create worker pool with result support
	pool := pond.NewResultPool[*requestResult](
		maxWorkers,
		pond.WithQueueSize(cfg.MaxQueueSize),
	)

	p := &proxy{
		config:   cfg,
		pool:     pool,
		limiters: limiters,
		redis:    rc,
		clock:    clock,
	}
	p.redisAvailable.Store(redisAvailable)

	// Start Redis health check goroutine
	go p.monitorRedisHealth()

	logger.Info("Rate limit proxy initialized",
		zap.Int("max_workers", maxWorkers),
		zap.Int("max_queue_size", cfg.MaxQueueSize),
		zap.Int("providers", len(cfg.Providers)),
		zap.Bool("local_fallback", cfg.EnableLocalFallback),
	)

	return p, nil
}

// Request submits a rate-limited request for execution and returns the result with type safety
func Request[T any](ctx context.Context, p Proxy, providerName string, fn func(ctx context.Context) (T, error)) (T, error) {
	// If proxy is nil, execute the function directly
	if p == nil {
		return fn(ctx)
	}

	// Execute the request
	var zero T
	result, err := p.Request(ctx, providerName, func(ctx context.Context) (interface{}, error) {
		return fn(ctx)
	})
	if err != nil {
		return zero, err
	}
	return result.(T), nil
}

// Request submits a rate-limited request for execution and returns the result as interface{}
// The function blocks until:
// 1. A token is acquired and the request completes
// 2. The context is canceled
// 3. The maximum queue time is exceeded
func (p *proxy) Request(ctx context.Context, providerName string, fn RequestFunc) (interface{}, error) {
	// Check if proxy is closed
	if p.closed.Load() {
		return nil, fmt.Errorf("proxy is closed")
	}

	// Get provider limiter
	limiter, ok := p.limiters[providerName]
	if !ok {
		return nil, fmt.Errorf("provider '%s' not configured", providerName)
	}

	// Create context with timeout for queue waiting
	queueCtx, cancel := context.WithTimeout(ctx, limiter.config.MaxQueueTime)
	defer cancel()

	// Submit task to worker pool
	resultTask := p.pool.Submit(func() *requestResult {
		value, err := p.executeWithRateLimit(queueCtx, limiter, fn)
		return &requestResult{value: value, err: err}
	})

	// Wait for result
	result, err := resultTask.Wait()
	if err != nil {
		return nil, err
	}
	if result.err != nil {
		return nil, result.err
	}
	return result.value, nil
}

// executeWithRateLimit executes the request after acquiring a rate limit token
func (p *proxy) executeWithRateLimit(ctx context.Context, limiter *providerLimiter, fn RequestFunc) (interface{}, error) {
	// Acquire rate limit token (with retry loop)
	if err := p.acquireToken(ctx, limiter); err != nil {
		return nil, err
	}

	// Execute the request - no timeout wrapper here, let HTTP adapter handle it
	return fn(ctx)
}

// acquireToken acquires a rate limit token, blocking until one is available
func (p *proxy) acquireToken(ctx context.Context, limiter *providerLimiter) error {
	// Retry loop for token acquisition
	for {
		// Check context before attempting
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Try distributed limiter first if Redis is available
		if p.redisAvailable.Load() {
			allowed, retryAfter, err := p.tryDistributedLimit(ctx, limiter)
			if err != nil {
				// Check if this is a context error (from pre-filter or Redis call)
				if ctx.Err() != nil {
					return ctx.Err()
				}

				// Redis error - mark as unavailable and fall back to local if enabled
				p.redisAvailable.Store(false)

				if !p.config.EnableLocalFallback {
					return fmt.Errorf("redis rate limiter unavailable: %w", err)
				}

				logger.Warn("Redis rate limiter error, falling back to local",
					zap.String("provider", limiter.name),
					zap.Error(err),
				)
				// Continue to local limiter
			} else if allowed {
				// Token acquired successfully
				return nil
			} else if retryAfter > 0 {
				// Rate limited - sleep with jitter and retry
				// Add jitter to spread out retry attempts (50-150% of retryAfter)
				jitter := time.Duration(float64(retryAfter) * (0.5 + rand.Float64())) //nolint:gosec,G404
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-p.clock.After(jitter):
					// Retry
					continue
				}
			}
		}

		// Use local limiter as fallback or when Redis is unavailable
		if !p.redisAvailable.Load() && p.config.EnableLocalFallback {
			// Block until token is available
			if err := limiter.localLimiter.Wait(ctx); err != nil {
				return err
			}
			return nil
		}

		// If we get here without acquiring a token, sleep briefly and retry
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.clock.After(100 * time.Millisecond):
			// Retry
		}
	}
}

// tryDistributedLimit attempts to acquire a token from the distributed limiter
// Returns: (allowed bool, retryAfter duration, error)
func (p *proxy) tryDistributedLimit(ctx context.Context, limiter *providerLimiter) (bool, time.Duration, error) {
	if limiter.distributedLimiter == nil {
		return false, 0, fmt.Errorf("distributed limiter not available")
	}

	// Pre-filter requests to reduce Redis pressure
	if err := limiter.preFilterLimiter.Wait(ctx); err != nil {
		// Context error during pre-filter - not a Redis error
		return false, 0, err
	}

	redisKey := fmt.Sprintf("%s%s", p.config.RedisKeyPrefix, limiter.name)

	// Use redis_rate's Allow method with per-second limiting
	res, err := limiter.distributedLimiter.Allow(ctx, redisKey, redis_rate.PerSecond(limiter.config.RequestsPerSecond))
	if err != nil {
		return false, 0, err
	}

	if res.Allowed == 0 {
		// Rate limit exceeded
		logger.Debug("Rate limit token unavailable, waiting",
			zap.String("provider", limiter.name),
			zap.Duration("retry_after", res.RetryAfter),
			zap.Int("remaining", res.Remaining),
		)
		return false, res.RetryAfter, nil
	}

	// Token acquired successfully
	return true, 0, nil
}

// monitorRedisHealth periodically checks Redis health and updates availability status
func (p *proxy) monitorRedisHealth() {
	ticker := p.clock.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		if p.closed.Load() {
			return
		}

		<-ticker.C

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := p.redis.Ping(ctx).Err()
		cancel()

		redisAvailable := err == nil
		wasAvailable := p.redisAvailable.Load()
		p.redisAvailable.Store(redisAvailable)

		if !wasAvailable && redisAvailable {
			logger.Info("Redis connection restored")
		}
	}
}

// Close gracefully shuts down the proxy
// It waits for in-flight requests to complete with a timeout
func (p *proxy) Close() error {
	var err error
	p.closeOnce.Do(func() {
		p.closed.Store(true)

		logger.Info("Shutting down rate limit proxy")

		// Stop the pool and wait for tasks to complete
		tasks := p.pool.Stop()
		if errTasks := tasks.Wait(); errTasks != nil {
			logger.Warn("Error waiting for pool tasks to complete", zap.Error(err))
			err = errTasks
		}

		// Close Redis connection
		if closeErr := p.redis.Close(); closeErr != nil {
			logger.Warn("Error closing Redis connection", zap.Error(closeErr))
			err = closeErr
		}

		logger.Info("Rate limit proxy shutdown complete")
	})
	return err
}

// validateConfig validates and sets defaults for the configuration
func validateConfig(cfg *config.RateLimiterConfig) error {
	if cfg.RedisAddr == "" {
		return fmt.Errorf("redis_addr is required")
	}

	if len(cfg.Providers) == 0 {
		return fmt.Errorf("at least one provider must be configured")
	}

	for name, provider := range cfg.Providers {
		if provider.RequestsPerSecond <= 0 {
			return fmt.Errorf("provider %s: requests_per_second must be positive", name)
		}

		if provider.Burst <= 0 {
			provider.Burst = provider.RequestsPerSecond
		}

		if provider.MaxQueueTime <= 0 {
			provider.MaxQueueTime = 5 * time.Minute
		}

		cfg.Providers[name] = provider
	}

	if cfg.RedisKeyPrefix == "" {
		cfg.RedisKeyPrefix = "ff:indexer:limiter:"
	}

	if cfg.MaxWorkers <= 0 {
		cfg.MaxWorkers = runtime.NumCPU() * 10
	}

	if cfg.MaxQueueSize <= 0 {
		cfg.MaxQueueSize = 10000
	}

	if cfg.LocalFallbackMultiplier <= 0 {
		cfg.LocalFallbackMultiplier = 0.5
	}

	return nil
}

package ratelimit

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alitto/pond/v2"
	"go.uber.org/zap"
	"golang.org/x/time/rate"

	"github.com/feral-file/ff-indexer-v2/internal/config"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
)

// Func executes a provider request once the limiter has granted capacity.
type Func func(ctx context.Context) (interface{}, error)

// taskResult wraps the result and error of a request.
type taskResult struct {
	value interface{}
	err   error
}

// Limiter coordinates process-local rate limiting for outbound provider traffic.
//
//go:generate mockgen -source=limiter.go -destination=../mocks/ratelimit_limiter.go -package=mocks -mock_names=Limiter=MockLimiter
type Limiter interface {
	// Do submits a rate-limited request for execution.
	Do(ctx context.Context, providerName string, fn Func) (interface{}, error)

	// Close gracefully shuts down the limiter.
	Close() error
}

// localLimiter is the concrete implementation of the process-local limiter.
type localLimiter struct {
	pool      pond.ResultPool[*taskResult]
	providers map[string]*providerLimiter
	closed    atomic.Bool
	closeOnce sync.Once
}

// providerLimiter holds the rate limiting state for a single provider.
type providerLimiter struct {
	config  config.RateLimitConfig
	limiter *rate.Limiter
}

// NewLimiter creates a new process-local rate limiter.
func NewLimiter(cfg config.RateLimiterConfig) (Limiter, error) {
	if err := validateConfig(&cfg); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	providers := make(map[string]*providerLimiter, len(cfg.Providers))
	for name, providerConfig := range cfg.Providers {
		providers[name] = &providerLimiter{
			config:  providerConfig,
			limiter: rate.NewLimiter(rate.Limit(providerConfig.RequestsPerSecond), providerConfig.Burst),
		}
	}

	pool := pond.NewResultPool[*taskResult](
		cfg.MaxWorkers,
		pond.WithQueueSize(cfg.MaxQueueSize),
	)

	l := &localLimiter{
		pool:      pool,
		providers: providers,
	}

	logger.Info("Rate limiter initialized")

	return l, nil
}

// Do submits a rate-limited request for execution and returns the result with type safety.
func Do[T any](ctx context.Context, l Limiter, providerName string, fn func(ctx context.Context) (T, error)) (T, error) {
	if l == nil {
		return fn(ctx)
	}

	var zero T
	result, err := l.Do(ctx, providerName, func(ctx context.Context) (interface{}, error) {
		return fn(ctx)
	})
	if err != nil {
		return zero, err
	}
	return result.(T), nil
}

// Do submits a rate-limited request for execution and returns the result as interface{}.
func (l *localLimiter) Do(ctx context.Context, providerName string, fn Func) (interface{}, error) {
	if l.closed.Load() {
		return nil, fmt.Errorf("limiter is closed")
	}

	provider, ok := l.providers[providerName]
	if !ok {
		return nil, fmt.Errorf("provider '%s' not configured", providerName)
	}

	queueCtx, cancel := context.WithTimeout(ctx, provider.config.MaxQueueTime)
	defer cancel()

	resultTask := l.pool.Submit(func() *taskResult {
		if err := provider.limiter.Wait(queueCtx); err != nil {
			return &taskResult{err: err}
		}

		value, err := fn(queueCtx)
		return &taskResult{value: value, err: err}
	})

	result, err := resultTask.Wait()
	if err != nil {
		return nil, err
	}
	if result.err != nil {
		return nil, result.err
	}
	return result.value, nil
}

// Close gracefully shuts down the limiter and waits for in-flight requests.
func (l *localLimiter) Close() error {
	var err error
	l.closeOnce.Do(func() {
		l.closed.Store(true)

		logger.Info("Shutting down rate limiter")

		tasks := l.pool.Stop()
		if errTasks := tasks.Wait(); errTasks != nil {
			logger.Warn("Error waiting for pool tasks to complete", zap.Error(err))
			err = errTasks
		}

		logger.Info("Rate limiter shutdown complete")
	})
	return err
}

// validateConfig validates and sets defaults for the configuration.
func validateConfig(cfg *config.RateLimiterConfig) error {
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

	if cfg.MaxWorkers <= 0 {
		cfg.MaxWorkers = runtime.NumCPU() * 10
	}

	if cfg.MaxQueueSize <= 0 {
		cfg.MaxQueueSize = 10000
	}

	return nil
}

package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"net"
	"runtime"
	"strings"
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

// ErrQueueTimeout is returned when a request could not get capacity from its provider's
// bucket within the provider's max_queue_time. It means "we throttled ourselves", not that
// the remote failed, so callers should treat it as transient.
var ErrQueueTimeout = errors.New("rate limiter queue timeout")

// Limiter coordinates process-local rate limiting for outbound provider traffic.
//
// Two entry points share each provider's token bucket:
//   - Do: vendor clients wrap a whole API call; runs on the bounded worker pool.
//   - WaitHost: the HTTP transport paces every request (including retries and redirect
//     hops) to hosts mapped via RateLimitConfig.Hosts; it never touches the pool, so a
//     slow bucket cannot hold pool slots other providers need.
//
// A request admitted by Do is not charged again by WaitHost for the same provider.
//
//go:generate mockgen -source=limiter.go -destination=../mocks/ratelimit_limiter.go -package=mocks -mock_names=Limiter=MockLimiter
type Limiter interface {
	// Do submits a rate-limited request for execution.
	Do(ctx context.Context, providerName string, fn Func) (interface{}, error)

	// WaitHost blocks until the provider mapped to host has capacity and returns that
	// provider's name. Unmapped hosts return "" immediately. Fails with ErrQueueTimeout
	// after the provider's max_queue_time, or with ctx's error if ctx ends first.
	WaitHost(ctx context.Context, host string) (string, error)

	// Penalize pauses all of a provider's traffic (Do and WaitHost) for d, e.g. after a
	// 429 carrying Retry-After, so concurrent callers stop hitting a limit one of them
	// already found. Unknown providers are ignored; overlapping penalties keep the later end.
	Penalize(providerName string, d time.Duration)

	// Close gracefully shuts down the limiter.
	Close() error
}

// localLimiter is the concrete implementation of the process-local limiter.
type localLimiter struct {
	pool      pond.ResultPool[*taskResult]
	providers map[string]*providerLimiter
	hosts     map[string]string // normalized host suffix -> provider name
	closed    atomic.Bool
	closeOnce sync.Once
}

// providerLimiter holds the rate limiting state for a single provider.
type providerLimiter struct {
	name    string
	config  config.RateLimitConfig
	limiter *rate.Limiter
	// pausedUntil is a UnixNano deadline set by Penalize; zero when not paused.
	pausedUntil atomic.Int64
}

// admittedKey marks a context whose request was already admitted by Do for a provider.
type admittedKey struct{}

// NewLimiter creates a new process-local rate limiter.
func NewLimiter(cfg config.RateLimiterConfig) (Limiter, error) {
	if err := validateConfig(&cfg); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	providers := make(map[string]*providerLimiter, len(cfg.Providers))
	hosts := make(map[string]string)
	for name, providerConfig := range cfg.Providers {
		providers[name] = &providerLimiter{
			name:    name,
			config:  providerConfig,
			limiter: rate.NewLimiter(rate.Limit(providerConfig.RequestsPerSecond), providerConfig.Burst),
		}
		for _, h := range providerConfig.Hosts {
			hosts[h] = name
		}
	}

	pool := pond.NewResultPool[*taskResult](
		cfg.MaxWorkers,
		pond.WithQueueSize(cfg.MaxQueueSize),
	)

	l := &localLimiter{
		pool:      pool,
		providers: providers,
		hosts:     hosts,
	}

	logger.Info("Rate limiter initialized", zap.Int("providers", len(providers)), zap.Int("mapped_hosts", len(hosts)))

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
		if err := provider.wait(ctx, queueCtx); err != nil {
			return &taskResult{err: err}
		}

		value, err := fn(context.WithValue(queueCtx, admittedKey{}, providerName))
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

// WaitHost blocks until the provider mapped to host has capacity; see Limiter.WaitHost.
func (l *localLimiter) WaitHost(ctx context.Context, host string) (string, error) {
	name, ok := l.providerForHost(host)
	if !ok {
		return "", nil
	}
	// Already charged by Do for this provider (vendor client -> HTTP client); a redirect
	// to a different provider's host is still charged to that provider.
	if admitted, _ := ctx.Value(admittedKey{}).(string); admitted == name {
		return name, nil
	}
	if l.closed.Load() {
		return name, fmt.Errorf("limiter is closed")
	}

	provider := l.providers[name]
	queueCtx, cancel := context.WithTimeout(ctx, provider.config.MaxQueueTime)
	defer cancel()
	return name, provider.wait(ctx, queueCtx)
}

// Penalize pauses a provider's traffic for d; see Limiter.Penalize.
func (l *localLimiter) Penalize(providerName string, d time.Duration) {
	provider, ok := l.providers[providerName]
	if !ok || d <= 0 {
		return
	}
	until := time.Now().Add(d).UnixNano()
	for {
		cur := provider.pausedUntil.Load()
		if cur >= until {
			return
		}
		if provider.pausedUntil.CompareAndSwap(cur, until) {
			logger.Warn("Rate limiter provider paused", zap.String("provider", providerName), zap.Duration("pause", d))
			return
		}
	}
}

// providerForHost resolves host (optionally with port) to the provider whose suffix
// matches it most specifically.
func (l *localLimiter) providerForHost(host string) (string, bool) {
	if len(l.hosts) == 0 || host == "" {
		return "", false
	}
	h := normalizeHost(host)
	for {
		if name, ok := l.hosts[h]; ok {
			return name, true
		}
		i := strings.IndexByte(h, '.')
		if i < 0 {
			return "", false
		}
		h = h[i+1:]
	}
}

// wait takes a token and honors any Penalize pause, including one installed while this
// request was queued for its token: a request is only released once a token is held and
// no pause is active. ctx is the caller's context and queueCtx the same context bounded
// by max_queue_time: expiry of queueCtx alone is our own queue budget running out and is
// reported as ErrQueueTimeout.
func (p *providerLimiter) wait(ctx, queueCtx context.Context) error {
	if err := p.waitPause(ctx, queueCtx); err != nil {
		return err
	}
	if err := p.limiter.Wait(queueCtx); err != nil {
		return p.waitErr(ctx)
	}
	// Another caller may have hit a 429 while we were queued; the token we hold stays
	// spent, but the request must not go out during the pause.
	return p.waitPause(ctx, queueCtx)
}

// waitPause blocks until no Penalize pause is active, failing fast when the pause
// outlasts queueCtx. It loops because a pause can be extended while we sleep.
func (p *providerLimiter) waitPause(ctx, queueCtx context.Context) error {
	for {
		until := p.pausedUntil.Load()
		d := time.Until(time.Unix(0, until))
		if until == 0 || d <= 0 {
			return nil
		}
		if deadline, ok := queueCtx.Deadline(); ok && time.Now().Add(d).After(deadline) {
			return p.waitErr(ctx)
		}
		t := time.NewTimer(d)
		select {
		case <-queueCtx.Done():
			t.Stop()
			return p.waitErr(ctx)
		case <-t.C:
		}
	}
}

func (p *providerLimiter) waitErr(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return fmt.Errorf("%w: provider %s after %s", ErrQueueTimeout, p.name, p.config.MaxQueueTime)
}

// normalizeHost lowercases host and strips any port and trailing dot.
func normalizeHost(host string) string {
	h := strings.ToLower(strings.TrimSpace(host))
	if hp, _, err := net.SplitHostPort(h); err == nil {
		h = hp
	}
	return strings.TrimSuffix(h, ".")
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

	hostOwner := make(map[string]string)
	for name, provider := range cfg.Providers {
		if provider.RequestsPerSecond <= 0 {
			return fmt.Errorf("provider %s: requests_per_second must be positive", name)
		}

		hosts := make([]string, 0, len(provider.Hosts))
		for _, raw := range provider.Hosts {
			h, err := validateHost(raw)
			if err != nil {
				return fmt.Errorf("provider %s: hosts: %w", name, err)
			}
			if owner, dup := hostOwner[h]; dup {
				return fmt.Errorf("provider %s: host %q is already mapped to provider %s", name, h, owner)
			}
			hostOwner[h] = name
			hosts = append(hosts, h)
		}
		provider.Hosts = hosts

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

// validateHost normalizes a hosts entry and rejects values that are not a bare host
// suffix. Requiring a dot keeps a typo like "io" from throttling a whole TLD.
func validateHost(raw string) (string, error) {
	h := strings.ToLower(strings.TrimSpace(raw))
	h = strings.TrimSuffix(strings.TrimPrefix(h, "."), ".")
	switch {
	case h == "":
		return "", fmt.Errorf("empty host")
	case strings.ContainsAny(h, "/:@?#* "):
		return "", fmt.Errorf("%q must be a bare host suffix (no scheme, port, path or wildcard)", raw)
	case !strings.Contains(h, "."):
		return "", fmt.Errorf("%q must contain at least one dot", raw)
	}
	return h, nil
}

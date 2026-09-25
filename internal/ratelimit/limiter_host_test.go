package ratelimit_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/config"
	"github.com/feral-file/ff-indexer-v2/internal/ratelimit"
)

func newHostLimiter(t *testing.T, providers map[string]config.RateLimitConfig) ratelimit.Limiter {
	t.Helper()
	l, err := ratelimit.NewLimiter(config.RateLimiterConfig{
		MaxWorkers:   4,
		MaxQueueSize: 10,
		Providers:    providers,
	})
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, l.Close()) })
	return l
}

func fastProvider(hosts ...string) config.RateLimitConfig {
	return config.RateLimitConfig{RequestsPerSecond: 1000, Burst: 1000, MaxQueueTime: time.Second, Hosts: hosts}
}

func TestNewLimiter_RejectsInvalidHosts(t *testing.T) {
	tests := []struct {
		name  string
		hosts map[string][]string
		want  string
	}{
		{"bare TLD", map[string][]string{"a": {"io"}}, "at least one dot"},
		{"scheme", map[string][]string{"a": {"https://artblocks.io"}}, "bare host suffix"},
		{"port", map[string][]string{"a": {"artblocks.io:443"}}, "bare host suffix"},
		{"path", map[string][]string{"a": {"artblocks.io/token"}}, "bare host suffix"},
		{"wildcard", map[string][]string{"a": {"*.artblocks.io"}}, "bare host suffix"},
		{"empty", map[string][]string{"a": {" "}}, "empty host"},
		{"same host twice across providers", map[string][]string{"a": {"artblocks.io"}, "b": {"ArtBlocks.io."}}, "already mapped"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			providers := map[string]config.RateLimitConfig{}
			for name, hosts := range tt.hosts {
				providers[name] = fastProvider(hosts...)
			}
			_, err := ratelimit.NewLimiter(config.RateLimiterConfig{Providers: providers})
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.want)
		})
	}
}

func TestWaitHost_MatchesSuffixOnDotBoundary(t *testing.T) {
	l := newHostLimiter(t, map[string]config.RateLimitConfig{
		"artblocks": fastProvider("artblocks.io"),
		"example":   fastProvider("example.com"),
		"gen":       fastProvider("gen.example.com"),
		"plain":     fastProvider(), // Do-only provider
	})

	tests := []struct {
		host string
		want string
	}{
		{"artblocks.io", "artblocks"},
		{"api.artblocks.io", "artblocks"},
		{"media-proxy.artblocks.io", "artblocks"},
		{"API.ArtBlocks.IO:443", "artblocks"},
		{"generator.artblocks.io.", "artblocks"},
		{"notartblocks.io", ""},
		{"artblocks.io.evil.com", ""},
		{"example.com", "example"},
		{"a.gen.example.com", "gen"}, // most specific suffix wins
		{"ipfs.filebase.io", ""},
		{"", ""},
	}
	for _, tt := range tests {
		got, err := l.WaitHost(context.Background(), tt.host)
		require.NoError(t, err, tt.host)
		assert.Equal(t, tt.want, got, tt.host)
	}
}

func TestWaitHost_PacesToConfiguredRate(t *testing.T) {
	l := newHostLimiter(t, map[string]config.RateLimitConfig{
		"slow": {RequestsPerSecond: 20, Burst: 1, MaxQueueTime: time.Second, Hosts: []string{"slow.example"}},
	})

	start := time.Now()
	for range 5 {
		_, err := l.WaitHost(context.Background(), "api.slow.example")
		require.NoError(t, err)
	}
	// One token up front, then four more at 20/s.
	assert.GreaterOrEqual(t, time.Since(start), 180*time.Millisecond)
}

func TestWaitHost_QueueTimeoutIsSentinel(t *testing.T) {
	l := newHostLimiter(t, map[string]config.RateLimitConfig{
		"slow": {RequestsPerSecond: 1, Burst: 1, MaxQueueTime: 50 * time.Millisecond, Hosts: []string{"slow.example"}},
	})

	_, err := l.WaitHost(context.Background(), "slow.example")
	require.NoError(t, err)

	start := time.Now()
	provider, err := l.WaitHost(context.Background(), "slow.example")
	require.ErrorIs(t, err, ratelimit.ErrQueueTimeout)
	assert.Equal(t, "slow", provider)
	// rate.Limiter fails fast when the wait cannot fit the deadline.
	assert.Less(t, time.Since(start), 40*time.Millisecond)
}

// A caller deadline too short for the wait is an admission refusal, not a caller error:
// the request was never sent, so it must stay distinguishable from a real timeout (health
// checks classify context.DeadlineExceeded as broken, ErrQueueTimeout as transient).
func TestWaitHost_CallerDeadlineTooShortIsQueueTimeout(t *testing.T) {
	l := newHostLimiter(t, map[string]config.RateLimitConfig{
		"slow": {RequestsPerSecond: 1, Burst: 1, MaxQueueTime: time.Minute, Hosts: []string{"slow.example"}},
	})
	_, err := l.WaitHost(context.Background(), "slow.example")
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = l.WaitHost(ctx, "slow.example")
	require.ErrorIs(t, err, ratelimit.ErrQueueTimeout)
	assert.NotErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, time.Since(start), 50*time.Millisecond, "fails fast instead of sleeping to the deadline")
	require.NoError(t, ctx.Err(), "the caller's context is still live")
}

func TestWaitHost_CallerCancellationIsNotQueueTimeout(t *testing.T) {
	l := newHostLimiter(t, map[string]config.RateLimitConfig{
		"slow": {RequestsPerSecond: 1, Burst: 1, MaxQueueTime: time.Minute, Hosts: []string{"slow.example"}},
	})
	_, err := l.WaitHost(context.Background(), "slow.example")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = l.WaitHost(ctx, "slow.example")
	require.ErrorIs(t, err, context.Canceled)
	assert.NotErrorIs(t, err, ratelimit.ErrQueueTimeout)
}

func TestPenalize_PausesWaitHostAndDo(t *testing.T) {
	l := newHostLimiter(t, map[string]config.RateLimitConfig{
		"p": fastProvider("p.example"),
	})

	l.Penalize("p", 150*time.Millisecond)

	start := time.Now()
	_, err := l.WaitHost(context.Background(), "p.example")
	require.NoError(t, err)
	assert.GreaterOrEqual(t, time.Since(start), 120*time.Millisecond)

	l.Penalize("p", 150*time.Millisecond)
	start = time.Now()
	_, err = l.Do(context.Background(), "p", func(context.Context) (interface{}, error) { return nil, nil })
	require.NoError(t, err)
	assert.GreaterOrEqual(t, time.Since(start), 120*time.Millisecond)
}

func TestPenalize_LongerThanQueueBudgetFailsFast(t *testing.T) {
	l := newHostLimiter(t, map[string]config.RateLimitConfig{
		"p": {RequestsPerSecond: 1000, Burst: 1000, MaxQueueTime: 50 * time.Millisecond, Hosts: []string{"p.example"}},
	})
	l.Penalize("p", 5*time.Second)

	start := time.Now()
	_, err := l.WaitHost(context.Background(), "p.example")
	require.ErrorIs(t, err, ratelimit.ErrQueueTimeout)
	assert.Less(t, time.Since(start), 40*time.Millisecond)
}

func TestPenalize_KeepsLaterDeadlineAndIgnoresUnknown(t *testing.T) {
	l := newHostLimiter(t, map[string]config.RateLimitConfig{
		"p": fastProvider("p.example"),
	})
	l.Penalize("unknown", time.Hour) // must not panic
	l.Penalize("p", 150*time.Millisecond)
	l.Penalize("p", time.Millisecond) // shorter pause must not shorten the active one

	start := time.Now()
	_, err := l.WaitHost(context.Background(), "p.example")
	require.NoError(t, err)
	assert.GreaterOrEqual(t, time.Since(start), 120*time.Millisecond)
}

func TestWaitHost_NotChargedTwiceInsideDo(t *testing.T) {
	// Burst 1 at 1 rps: a second charge inside Do would block for ~1s.
	l := newHostLimiter(t, map[string]config.RateLimitConfig{
		"vendor": {RequestsPerSecond: 1, Burst: 1, MaxQueueTime: 5 * time.Second, Hosts: []string{"api.vendor.example"}},
		"other":  {RequestsPerSecond: 1, Burst: 1, MaxQueueTime: 5 * time.Second, Hosts: []string{"cdn.other.example"}},
	})

	start := time.Now()
	_, err := l.Do(context.Background(), "vendor", func(ctx context.Context) (interface{}, error) {
		_, err := l.WaitHost(ctx, "api.vendor.example")
		return nil, err
	})
	require.NoError(t, err)
	assert.Less(t, time.Since(start), 500*time.Millisecond)

	// A hop to another provider's host inside Do is still charged to that provider.
	_, err = l.WaitHost(context.Background(), "cdn.other.example")
	require.NoError(t, err)
	start = time.Now()
	_, err = l.Do(context.Background(), "vendor", func(ctx context.Context) (interface{}, error) {
		_, err := l.WaitHost(ctx, "cdn.other.example")
		return nil, err
	})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, time.Since(start), 800*time.Millisecond)
}

func TestWaitHost_ClosedLimiter(t *testing.T) {
	l, err := ratelimit.NewLimiter(config.RateLimiterConfig{
		Providers: map[string]config.RateLimitConfig{"p": fastProvider("p.example")},
	})
	require.NoError(t, err)
	require.NoError(t, l.Close())

	_, err = l.WaitHost(context.Background(), "p.example")
	require.Error(t, err)
	// Unmapped hosts are never gated, closed or not.
	_, err = l.WaitHost(context.Background(), "free.example")
	require.NoError(t, err)
}

// TestHTTPClient_PacesEveryRequestAndRedirectHop wires the real limiter into the real
// HTTP client: the redirect hop and every retry-free method must each take a token.
func TestHTTPClient_PacesEveryRequestAndRedirectHop(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if r.URL.Path == "/redirect" {
			http.Redirect(w, r, "/final", http.StatusFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	host := mustHostname(t, srv.URL)

	l := newHostLimiter(t, map[string]config.RateLimitConfig{
		"local": {RequestsPerSecond: 10, Burst: 1, MaxQueueTime: 5 * time.Second, Hosts: []string{host}},
	})
	client := adapter.NewHTTPClientWithSSRF(5*time.Second, nil, 0, adapter.WithHostRateLimiter(l))

	start := time.Now()
	resp, err := client.GetResponseNoRetry(context.Background(), srv.URL+"/redirect", nil) // 2 hops
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	resp, err = client.HeadNoRetry(context.Background(), srv.URL+"/final")
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	_, err = client.GetBytes(context.Background(), srv.URL+"/final", nil)
	require.NoError(t, err)

	assert.Equal(t, int32(4), hits.Load())
	// 4 requests, burst 1 at 10/s: at least 3 waits of 100ms.
	assert.GreaterOrEqual(t, time.Since(start), 280*time.Millisecond)
}

// TestHTTPClient_429PausesProviderForOtherCallers checks that one caller's 429 with
// Retry-After makes the next caller wait, even though that caller never saw a 429.
func TestHTTPClient_429PausesProviderForOtherCallers(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	// Queue budget must exceed the 1s pause, or the next caller fails fast instead.
	l := newHostLimiter(t, map[string]config.RateLimitConfig{
		"local": {RequestsPerSecond: 1000, Burst: 1000, MaxQueueTime: 5 * time.Second, Hosts: []string{mustHostname(t, srv.URL)}},
	})
	client := adapter.NewHTTPClientWithSSRF(5*time.Second, nil, 0, adapter.WithHostRateLimiter(l))

	resp, err := client.GetResponseNoRetry(context.Background(), srv.URL, nil)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusTooManyRequests, resp.StatusCode)

	start := time.Now()
	resp, err = client.GetResponseNoRetry(context.Background(), srv.URL, nil)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.GreaterOrEqual(t, time.Since(start), 900*time.Millisecond)
}

func TestHTTPClient_QueueTimeoutIsNotRetriedAndNeverSent(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	l := newHostLimiter(t, map[string]config.RateLimitConfig{
		"local": {RequestsPerSecond: 1, Burst: 1, MaxQueueTime: 20 * time.Millisecond, Hosts: []string{mustHostname(t, srv.URL)}},
	})
	client := adapter.NewHTTPClientWithSSRF(5*time.Second, nil, 0, adapter.WithHostRateLimiter(l))

	_, err := client.GetBytes(context.Background(), srv.URL, nil)
	require.NoError(t, err)

	start := time.Now()
	_, err = client.GetBytes(context.Background(), srv.URL, nil)
	require.Error(t, err)
	assert.True(t, errors.Is(err, adapter.ErrRateLimitWait), "got %v", err)
	assert.True(t, errors.Is(err, ratelimit.ErrQueueTimeout), "got %v", err)
	assert.Equal(t, int32(1), hits.Load(), "a request refused by the limiter must never reach the server")
	assert.Less(t, time.Since(start), 500*time.Millisecond, "queue timeout must not enter the 429 backoff loop")
}

func mustHostname(t *testing.T, raw string) string {
	t.Helper()
	u, err := url.Parse(raw)
	require.NoError(t, err)
	return u.Hostname()
}

// Review F1: a request already queued for a token when another caller installs a penalty
// must not be released during that penalty.
func TestPenalize_HonoredByRequestAlreadyQueued(t *testing.T) {
	l := newHostLimiter(t, map[string]config.RateLimitConfig{
		"p": {RequestsPerSecond: 5, Burst: 1, MaxQueueTime: 5 * time.Second, Hosts: []string{"p.example"}},
	})
	_, err := l.WaitHost(context.Background(), "p.example") // drain the burst
	require.NoError(t, err)

	start := time.Now()
	done := make(chan time.Duration, 1)
	go func() {
		_, err := l.WaitHost(context.Background(), "p.example") // queues ~200ms for a token
		assert.NoError(t, err)
		done <- time.Since(start)
	}()

	time.Sleep(50 * time.Millisecond)
	l.Penalize("p", 600*time.Millisecond) // lands while the goroutine is inside rate.Wait

	elapsed := <-done
	assert.GreaterOrEqual(t, elapsed, 600*time.Millisecond, "released during the penalty")
}

// Review round 2: callers already queued when a penalty lands must be re-paced by the
// bucket after it ends, not released together at the pause deadline.
func TestPenalize_QueuedCallersArePacedAfterPause(t *testing.T) {
	l := newHostLimiter(t, map[string]config.RateLimitConfig{
		"p": {RequestsPerSecond: 10, Burst: 1, MaxQueueTime: 5 * time.Second, Hosts: []string{"p.example"}},
	})
	_, err := l.WaitHost(context.Background(), "p.example") // drain the burst
	require.NoError(t, err)

	const n = 5
	start := time.Now()
	released := make(chan time.Duration, n)
	for range n {
		go func() {
			_, err := l.WaitHost(context.Background(), "p.example") // reservations at ~100..500ms
			assert.NoError(t, err)
			released <- time.Since(start)
		}()
	}

	time.Sleep(50 * time.Millisecond)
	l.Penalize("p", 600*time.Millisecond)

	times := make([]time.Duration, 0, n)
	for range n {
		times = append(times, <-released)
	}
	slices.Sort(times)
	assert.GreaterOrEqual(t, times[0], 600*time.Millisecond, "released during the penalty")
	// Re-paced at 10/s with burst 1: n releases span at least (n-1)*100ms.
	assert.GreaterOrEqual(t, times[n-1]-times[0], 350*time.Millisecond, "queued callers released as a burst: %v", times)
}

// Review F2: a redirect hop queued behind the host limiter must fail as a limiter wait
// (transient), never as a client timeout (broken). The dangerous case is a wait that fits
// inside Client.Timeout but leaves too little of it for the fetch itself.
func TestHTTPClient_RedirectHopWaitFailsAsLimiterWaitNotClientTimeout(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if r.URL.Path == "/redirect" {
			http.Redirect(w, r, "/final", http.StatusFound)
			return
		}
		time.Sleep(150 * time.Millisecond) // a normal, slightly slow origin
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	// The first hop takes the only token; the hop to /final waits ~333ms. That fits the
	// 450ms client timeout, but 333ms + 150ms does not: without a bounded wait, the fetch
	// dies as "Client.Timeout exceeded".
	l := newHostLimiter(t, map[string]config.RateLimitConfig{
		"local": {RequestsPerSecond: 3, Burst: 1, MaxQueueTime: time.Minute, Hosts: []string{mustHostname(t, srv.URL)}},
	})
	client := adapter.NewHTTPClientWithSSRF(450*time.Millisecond, nil, 0, adapter.WithHostRateLimiter(l))

	_, err := client.GetResponseNoRetry(context.Background(), srv.URL+"/redirect", nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, adapter.ErrRateLimitWait)
	assert.NotContains(t, err.Error(), "Client.Timeout exceeded")
	assert.Equal(t, int32(1), hits.Load(), "the queued hop is never sent")
}

package adapter

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeHostLimiter maps every host to provider (or none when provider is "") and records
// calls, so transport behavior can be tested without the ratelimit package.
type fakeHostLimiter struct {
	provider string
	waitErr  error

	mu        sync.Mutex
	waits     []string
	penalties map[string]time.Duration
}

func (f *fakeHostLimiter) WaitHost(ctx context.Context, host string) (string, error) {
	f.mu.Lock()
	f.waits = append(f.waits, host)
	f.mu.Unlock()
	if f.waitErr != nil {
		if err := ctx.Err(); err != nil {
			return f.provider, err
		}
		return f.provider, f.waitErr
	}
	return f.provider, nil
}

func (f *fakeHostLimiter) Penalize(providerName string, d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.penalties == nil {
		f.penalties = map[string]time.Duration{}
	}
	f.penalties[providerName] = d
}

func statusServer(t *testing.T, status int, retryAfter string) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		if retryAfter != "" {
			w.Header().Set("Retry-After", retryAfter)
		}
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)
	return srv, &hits
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 9, 25, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		in     string
		want   time.Duration
		wantOK bool
	}{
		{"6", 6 * time.Second, true}, // Cloudflare 1015
		{" 0 ", 0, true},
		{"3600", maxRetryAfter, true}, // capped
		{"30", maxRetryAfter, true},
		{"31", maxRetryAfter, true},
		{"9223372037", maxRetryAfter, true},              // overflows time.Duration if converted before capping
		{"99999999999999999999999", maxRetryAfter, true}, // beyond uint64
		{now.Add(10 * time.Second).Format(http.TimeFormat), 10 * time.Second, true},
		{now.Add(-time.Minute).Format(http.TimeFormat), 0, true},
		{"", 0, false},
		{"-1", 0, false},
		{"soon", 0, false},
		{"+5", 0, false},
		{"1.5", 0, false},
	}
	for _, tt := range tests {
		got, ok := parseRetryAfter(tt.in, now)
		assert.Equal(t, tt.wantOK, ok, tt.in)
		assert.Equal(t, tt.want, got, tt.in)
	}
}

func TestRetryAfterBackOff_StretchesOnlyTheNextInterval(t *testing.T) {
	b := &retryAfterBackOff{BackOff: backoff.NewConstantBackOff(time.Second)}

	b.next = 6 * time.Second
	assert.Equal(t, 6*time.Second, b.NextBackOff())
	assert.Equal(t, time.Second, b.NextBackOff(), "Retry-After applies once")

	b.next = 100 * time.Millisecond
	assert.Equal(t, time.Second, b.NextBackOff(), "never shorter than the policy")

	stop := &retryAfterBackOff{BackOff: &backoff.StopBackOff{}, next: time.Second}
	assert.Equal(t, backoff.Stop, stop.NextBackOff(), "Stop is never overridden")
}

func TestRateLimitRoundTripper_429PausesMappedProvider(t *testing.T) {
	tests := []struct {
		name       string
		retryAfter string
		want       time.Duration
	}{
		{"uses Retry-After", "6", 6 * time.Second},
		{"defaults without Retry-After", "", defaultRateLimitPause},
		// Seen live from Cloudflare 1015 at the end of its window: still a 429, must still pause.
		{"floors Retry-After 0", "0", minRateLimitPause},
		{"floors an HTTP-date already passed", "Thu, 01 Jan 2026 00:00:00 GMT", minRateLimitPause},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, _ := statusServer(t, http.StatusTooManyRequests, tt.retryAfter)
			l := &fakeHostLimiter{provider: "artblocks"}
			client := NewHTTPClientWithSSRF(5*time.Second, nil, 0, WithHostRateLimiter(l))

			resp, err := client.GetResponseNoRetry(context.Background(), srv.URL, nil)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())
			assert.Equal(t, map[string]time.Duration{"artblocks": tt.want}, l.penalties)
		})
	}
}

func TestRateLimitRoundTripper_UnmappedHostIsNeverPenalized(t *testing.T) {
	srv, _ := statusServer(t, http.StatusTooManyRequests, "6")
	l := &fakeHostLimiter{provider: ""}
	client := NewHTTPClientWithSSRF(5*time.Second, nil, 0, WithHostRateLimiter(l))

	resp, err := client.GetResponseNoRetry(context.Background(), srv.URL, nil)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.Empty(t, l.penalties)
	assert.Len(t, l.waits, 1, "every request still consults the limiter")
}

func TestRateLimitRoundTripper_WaitFailureWrappedAndNotSent(t *testing.T) {
	srv, hits := statusServer(t, http.StatusOK, "")
	l := &fakeHostLimiter{provider: "p", waitErr: errors.New("queue timeout")}
	client := NewHTTPClientWithSSRF(5*time.Second, nil, 0, WithHostRateLimiter(l))

	_, err := client.GetBytes(context.Background(), srv.URL, nil)
	require.ErrorIs(t, err, ErrRateLimitWait)
	assert.Equal(t, int32(0), hits.Load())
	assert.Len(t, l.waits, 1, "a limiter refusal is permanent for this request, not retried")
}

func TestRateLimitRoundTripper_CallerCancellationNotWrapped(t *testing.T) {
	srv, _ := statusServer(t, http.StatusOK, "")
	l := &fakeHostLimiter{provider: "p", waitErr: errors.New("queue timeout")}
	client := NewHTTPClientWithSSRF(5*time.Second, nil, 0, WithHostRateLimiter(l))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := client.GetResponseNoRetry(ctx, srv.URL, nil)
	require.ErrorIs(t, err, context.Canceled)
	assert.NotErrorIs(t, err, ErrRateLimitWait)
}

func TestRateLimitRoundTripper_BlockedRedirectHopConsumesNoCapacity(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/blocked", http.StatusFound)
	}))
	defer srv.Close()
	l := &fakeHostLimiter{provider: "p"}
	client := NewHTTPClientWithSSRF(5*time.Second, denyPathValidator{path: "/blocked"}, 3, WithHostRateLimiter(l))

	_, err := client.GetResponseNoRetry(context.Background(), srv.URL, nil)
	require.Error(t, err)
	assert.Len(t, l.waits, 1, "only the first hop is charged; the refused hop never reaches the limiter")
}

func TestRealHTTPClient_ChargesEachHopAndAttemptOnce(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := hits.Add(1)
		switch {
		case r.URL.Path == "/redirect":
			http.Redirect(w, r, "/final", http.StatusFound)
		case r.URL.Path == "/flaky" && n == 1:
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
		default:
			_, _ = w.Write([]byte("ok"))
		}
	}))
	defer srv.Close()

	l := &fakeHostLimiter{provider: "p"}
	client := NewHTTPClientWithSSRF(5*time.Second, nil, 0, WithHostRateLimiter(l))

	_, err := client.GetBytes(context.Background(), srv.URL+"/flaky", nil)
	require.NoError(t, err)
	assert.Len(t, l.waits, 2, "the 429'd attempt and its retry are each charged once")

	l.waits = nil
	resp, err := client.GetResponseNoRetry(context.Background(), srv.URL+"/redirect", nil)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.Len(t, l.waits, 2, "the initial request and the redirect hop are each charged once")
}

// slowHostLimiter admits every request after a fixed delay.
type slowHostLimiter struct{ delay time.Duration }

func (s slowHostLimiter) WaitHost(ctx context.Context, _ string) (string, error) {
	select {
	case <-time.After(s.delay):
		return "p", nil
	case <-ctx.Done():
		return "p", ctx.Err()
	}
}

func (slowHostLimiter) Penalize(string, time.Duration) {}

// Queueing for the limiter must not consume the fetch's Client.Timeout: a request that
// waited and then timed out would be recorded as broken by health checks.
func TestRealHTTPClient_LimiterWaitDoesNotConsumeClientTimeout(t *testing.T) {
	srv, _ := statusServer(t, http.StatusOK, "")
	client := NewHTTPClientWithSSRF(200*time.Millisecond, nil, 0, WithHostRateLimiter(slowHostLimiter{delay: 300 * time.Millisecond}))

	resp, err := client.GetResponseNoRetry(context.Background(), srv.URL, nil)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestDoRequestWithRetry_HonorsRetryAfter(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if hits.Add(1) == 1 {
			w.Header().Set("Retry-After", "2")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	start := time.Now()
	body, err := NewHTTPClient(5*time.Second).GetBytes(context.Background(), srv.URL, nil)
	require.NoError(t, err)
	assert.Equal(t, "ok", string(body))
	// Without Retry-After the first backoff is 1s ± 50%, so ≥2s proves the header won.
	assert.GreaterOrEqual(t, time.Since(start), 2*time.Second)
}

// denyPathValidator refuses URLs whose path is path and allows everything else.
type denyPathValidator struct{ path string }

func (d denyPathValidator) ValidateHTTPURL(_ context.Context, rawURL string) error {
	if strings.HasSuffix(rawURL, d.path) {
		return errors.New("denied")
	}
	return nil
}

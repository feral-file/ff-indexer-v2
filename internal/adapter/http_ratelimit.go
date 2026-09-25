package adapter

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/logger"
)

// ErrRateLimitWait wraps a failure to get capacity from the host rate limiter before
// sending a request. The remote was never contacted, so callers must treat it as
// transient (never as a broken URL) and must not retry it in a tight loop.
var ErrRateLimitWait = errors.New("outbound rate limit wait failed")

const (
	// maxRetryAfter caps a server-provided Retry-After so a hostile or misconfigured
	// origin cannot park workers for hours.
	maxRetryAfter = 30 * time.Second
	// defaultRateLimitPause is applied to a mapped provider on a 429 without Retry-After.
	defaultRateLimitPause = 2 * time.Second
	// minRateLimitPause floors the provider pause on any 429. Cloudflare's 1015 counts
	// Retry-After down to 0 at the end of its window while still answering 429; honoring
	// that literally would install no pause and let every worker keep hitting the limit.
	minRateLimitPause = time.Second
)

// HostRateLimiter paces outbound requests by destination host. ratelimit.Limiter
// satisfies it; it is declared here because ratelimit depends on adapter transitively.
type HostRateLimiter interface {
	// WaitHost blocks until host's provider has capacity and returns the provider
	// name, or "" when host is not rate limited.
	WaitHost(ctx context.Context, host string) (string, error)
	// Penalize pauses all traffic of a provider for d.
	Penalize(providerName string, d time.Duration)
}

// HTTPClientOption customizes clients built by NewHTTPClientWithSSRF.
type HTTPClientOption func(*httpClientOptions)

type httpClientOptions struct {
	limiter HostRateLimiter
}

// WithHostRateLimiter paces every request the client sends (all methods, retry attempts
// and redirect hops) through l. A nil l leaves the client unlimited.
func WithHostRateLimiter(l HostRateLimiter) HTTPClientOption {
	return func(o *httpClientOptions) { o.limiter = l }
}

// precharged records that RealHTTPClient.do already took a token for a request's first
// hop. One marker per send attempt: the transport consumes it on that attempt's first
// round trip and charges every later hop (redirects) itself.
type precharged struct {
	provider string
	used     atomic.Bool
}

type prechargedKey struct{}

// do sends req, first waiting for host capacity when a limiter is configured.
//
// The wait happens here, before Client.Do, because http.Client.Timeout covers the whole
// exchange including time spent inside the transport: waiting there would eat into the
// fetch's own budget and turn a queued request into a client timeout, which health checks
// record as broken. Redirect hops can only be seen by the transport and are charged there.
func (c *RealHTTPClient) do(req *http.Request) (*http.Response, error) {
	if c.limiter == nil {
		return c.client.Do(req)
	}
	provider, err := waitHost(req.Context(), c.limiter, req)
	if err != nil {
		return nil, err
	}
	// Marked even for unlimited hosts ("") so the transport does not look the host up twice.
	req = req.WithContext(context.WithValue(req.Context(), prechargedKey{}, &precharged{provider: provider}))
	return c.client.Do(req)
}

// waitHost waits (bounded by ctx) for req's host and wraps a limiter refusal in
// ErrRateLimitWait; the request's own cancellation or deadline is returned unwrapped.
func waitHost(ctx context.Context, l HostRateLimiter, req *http.Request) (string, error) {
	provider, err := l.WaitHost(ctx, req.URL.Host)
	if err != nil {
		if ctxErr := req.Context().Err(); ctxErr != nil {
			return "", ctxErr
		}
		return "", fmt.Errorf("%w: %s: %w", ErrRateLimitWait, req.URL.Host, err)
	}
	return provider, nil
}

// rateLimitRoundTripper waits for host capacity before each round trip and turns a 429
// from a limited host into a provider-wide pause, so the other workers back off too
// instead of each discovering the limit on its own.
type rateLimitRoundTripper struct {
	next    http.RoundTripper
	limiter HostRateLimiter
}

// RoundTrip implements http.RoundTripper.
func (t *rateLimitRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	var provider string
	if pc, ok := req.Context().Value(prechargedKey{}).(*precharged); ok && pc.used.CompareAndSwap(false, true) {
		provider = pc.provider // first hop, already charged by RealHTTPClient.do
	} else {
		// A redirect hop: it can only be seen here, inside the exchange that
		// http.Client.Timeout bounds. Give the wait at most half the time left so a
		// queued hop surfaces as ErrRateLimitWait (transient) with time still left for
		// the fetch, instead of burning the budget into a client timeout that health
		// checks record as broken.
		waitCtx := req.Context()
		if deadline, ok := waitCtx.Deadline(); ok {
			var cancel context.CancelFunc
			waitCtx, cancel = context.WithDeadline(waitCtx, deadline.Add(-time.Until(deadline)/2))
			defer cancel()
		}
		var err error
		if provider, err = waitHost(waitCtx, t.limiter, req); err != nil {
			return nil, err
		}
	}

	resp, err := t.next.RoundTrip(req)
	if err == nil && provider != "" && resp.StatusCode == http.StatusTooManyRequests {
		pause, ok := parseRetryAfter(resp.Header.Get("Retry-After"), time.Now())
		if !ok {
			pause = defaultRateLimitPause
		}
		pause = max(pause, minRateLimitPause)
		logger.WarnCtx(req.Context(), "rate limited by remote, pausing provider",
			zap.String("provider", provider),
			zap.String("host", req.URL.Host),
			zap.Duration("pause", pause),
		)
		t.limiter.Penalize(provider, pause)
	}
	return resp, err
}

// parseRetryAfter reads a Retry-After header in either delta-seconds or HTTP-date form
// (RFC 9110 §10.2.3), capped at maxRetryAfter. ok is false when the header is absent or
// unparsable.
func parseRetryAfter(v string, now time.Time) (time.Duration, bool) {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0, false
	}
	var d time.Duration
	if secs, err := strconv.ParseUint(v, 10, 64); err == nil || errors.Is(err, strconv.ErrRange) {
		// Cap before converting: time.Duration(secs)*time.Second overflows for huge
		// values, and an all-digit value too large for uint64 (ErrRange) is huge too.
		if err != nil || secs > uint64(maxRetryAfter/time.Second) {
			return maxRetryAfter, true
		}
		d = time.Duration(secs) * time.Second
	} else if at, err := http.ParseTime(v); err == nil {
		d = at.Sub(now)
		if d < 0 {
			d = 0
		}
	} else {
		return 0, false
	}
	if d > maxRetryAfter {
		d = maxRetryAfter
	}
	return d, true
}

// retryAfterBackOff stretches the next backoff interval to a server-requested
// Retry-After while keeping the inner policy's growth and its overall retry budget.
type retryAfterBackOff struct {
	*backoff.ExponentialBackOff
	next time.Duration
}

// NextBackOff implements backoff.BackOff.
//
// The inner policy checks MaxElapsedTime against its own interval before the stretch is
// applied, so the budget is checked again here: a Retry-After that would land the next
// attempt past MaxElapsedTime stops retrying instead of silently extending the budget.
func (b *retryAfterBackOff) NextBackOff() time.Duration {
	d := b.ExponentialBackOff.NextBackOff()
	if d == backoff.Stop {
		return d
	}
	if b.next > d {
		d = b.next
		if b.MaxElapsedTime != 0 && b.GetElapsedTime()+d > b.MaxElapsedTime {
			d = backoff.Stop
		}
	}
	b.next = 0
	return d
}

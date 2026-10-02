package adapter

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/security/ssrf"
)

// pacedClient returns a client whose redirects are followed by doPaced.
func pacedClient(timeout time.Duration, v SSRFValidator, maxRedirects int, l HostRateLimiter) HTTPClient {
	return NewHTTPClientWithSSRF(timeout, v, maxRedirects, WithHostRateLimiter(l))
}

// artBlocksChain mimics the production tokenURI chain: /token/N → 302 → /N → 301
// (relative Location) → /1/contract/N.
func artBlocksChain(t *testing.T, delay time.Duration) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(delay)
		switch r.URL.Path {
		case "/token/129":
			http.Redirect(w, r, "/129", http.StatusFound)
		case "/129":
			w.Header().Set("Location", "/1/0xa7d8/129")
			w.WriteHeader(http.StatusMovedPermanently)
		case "/1/0xa7d8/129":
			_, _ = w.Write([]byte(`{"name":"Ringers #129"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// Regression for the 2026-10-02 Ringers fetch failures: a redirect hop's slot wait must
// not consume Client.Timeout. Before doPaced, hops 2 and 3 waited inside the 200ms
// timeout (capped at half the remainder) and the fetch failed.
func TestDoPaced_RedirectHopWaitDoesNotConsumeClientTimeout(t *testing.T) {
	srv := artBlocksChain(t, 0)
	client := pacedClient(200*time.Millisecond, nil, 0, slowHostLimiter{delay: 300 * time.Millisecond})

	var got map[string]any
	require.NoError(t, client.GetAndUnmarshal(context.Background(), srv.URL+"/token/129", &got))
	assert.Equal(t, "Ringers #129", got["name"])
}

// Each hop gets its own Client.Timeout instead of sharing one across the chain.
func TestDoPaced_EachHopGetsItsOwnTimeout(t *testing.T) {
	srv := artBlocksChain(t, 120*time.Millisecond) // 3 hops × 120ms > 200ms, each hop < 200ms
	client := pacedClient(200*time.Millisecond, nil, 0, &fakeHostLimiter{provider: "p"})

	resp, err := client.GetResponseNoRetry(context.Background(), srv.URL+"/token/129", nil)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "/1/0xa7d8/129", resp.Request.URL.Path, "resp.Request is the final hop, as with net/http")
}

func TestDoPaced_ChargesEveryHopOnceIncludingRelativeLocation(t *testing.T) {
	srv := artBlocksChain(t, 0)
	l := &fakeHostLimiter{provider: "artblocks"}
	client := pacedClient(5*time.Second, nil, 0, l)

	resp, err := client.GetResponseNoRetry(context.Background(), srv.URL+"/token/129", nil)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.Len(t, l.waits, 3, "three hops, each charged exactly once")
}

// A limiter refusal on a later hop surfaces as ErrRateLimitWait, so callers can treat
// it as self-throttling (the metadata workflow defers on it).
func TestDoPaced_LaterHopRefusalIsRateLimitWait(t *testing.T) {
	srv := artBlocksChain(t, 0)
	l := &refuseAfterLimiter{allow: 1}
	client := pacedClient(5*time.Second, nil, 0, l)

	_, err := client.GetBytes(context.Background(), srv.URL+"/token/129", nil)
	require.ErrorIs(t, err, ErrRateLimitWait)
}

// The SSRF redirect cap still applies with a limiter, and over-budget targets are not
// validated (mirrors TestNewHTTPClientWithSSRF_overBudgetRedirectSkipsRedirectTargetValidation).
func TestDoPaced_SSRFRedirectCapBeforeValidation(t *testing.T) {
	srv := artBlocksChain(t, 0)
	v := &countAllValidations{}
	client := pacedClient(5*time.Second, v, 1, &fakeHostLimiter{provider: "p"})

	_, err := client.GetResponseNoRetry(context.Background(), srv.URL+"/token/129", nil)
	require.ErrorIs(t, err, ssrf.ErrBlocked)
	assert.Contains(t, err.Error(), "stopped after 1 redirects")
	// Initial URL (RoundTrip), hop 2 target (doPaced) + hop 2 (RoundTrip); hop 3 never validated.
	assert.Equal(t, 3, v.n)
}

// A redirect target the validator refuses is reported as a blocked redirect and never
// reaches the limiter or the network.
func TestDoPaced_SSRFRefusedTargetNotChargedOrSent(t *testing.T) {
	var hits sync.Map
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Store(r.URL.Path, true)
		http.Redirect(w, r, "/internal", http.StatusFound)
	}))
	t.Cleanup(srv.Close)
	l := &fakeHostLimiter{provider: "p"}
	client := pacedClient(5*time.Second, denyPathValidator{path: "/internal"}, 3, l)

	_, err := client.GetResponseNoRetry(context.Background(), srv.URL+"/start", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "redirect blocked")
	assert.Len(t, l.waits, 1)
	_, sent := hits.Load("/internal")
	assert.False(t, sent)
}

// Without an SSRF validator the cap is net/http's default of 10 and is not
// classified as SSRF policy.
func TestDoPaced_DefaultRedirectCapWithoutValidator(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/loop", http.StatusFound)
	}))
	t.Cleanup(srv.Close)
	client := pacedClient(5*time.Second, nil, 0, &fakeHostLimiter{provider: "p"})

	_, err := client.GetResponseNoRetry(context.Background(), srv.URL, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "stopped after 10 redirects")
	assert.False(t, errors.Is(err, ssrf.ErrBlocked))
}

// Method and body rules follow net/http: 303 turns POST into a bodyless GET, 307 keeps
// both.
func TestDoPaced_RedirectMethodAndBodyRules(t *testing.T) {
	type seen struct{ method, body, contentType string }
	var mu sync.Mutex
	got := map[string]seen{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		got[r.URL.Path] = seen{r.Method, string(b), r.Header.Get("Content-Type")}
		mu.Unlock()
		switch r.URL.Path {
		case "/see-other":
			http.Redirect(w, r, "/after-303", http.StatusSeeOther)
		case "/temporary":
			http.Redirect(w, r, "/after-307", http.StatusTemporaryRedirect)
		}
	}))
	t.Cleanup(srv.Close)
	client := pacedClient(5*time.Second, nil, 0, &fakeHostLimiter{provider: "p"})
	headers := map[string]string{"Content-Type": "application/json"}

	resp, err := client.PostNoRetry(context.Background(), srv.URL+"/see-other", headers, bytes.NewBufferString(`{"a":1}`))
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, seen{http.MethodGet, "", ""}, got["/after-303"])

	resp, err = client.PostNoRetry(context.Background(), srv.URL+"/temporary", headers, bytes.NewBufferString(`{"a":1}`))
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, seen{http.MethodPost, `{"a":1}`, "application/json"}, got["/after-307"])
}

// Caller headers (Range, API keys) follow every hop; sensitive headers are dropped once
// the chain leaves the original host; Referer is set like net/http.
func TestDoPaced_RedirectHeaders(t *testing.T) {
	var mu sync.Mutex
	got := map[string]http.Header{}
	record := func(name string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			got[name] = r.Header.Clone()
			mu.Unlock()
		}
	}
	other := httptest.NewServer(record("other"))
	t.Cleanup(other.Close)
	otherURL := strings.Replace(other.URL, "127.0.0.1", "localhost", 1) // different hostname

	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/start":
			http.Redirect(w, r, "/same", http.StatusFound)
		case "/same":
			record("same")(w, r)
			http.Redirect(w, r, otherURL+"/away", http.StatusFound)
		}
	}))
	t.Cleanup(origin.Close)
	client := pacedClient(5*time.Second, nil, 0, &fakeHostLimiter{provider: "p"})

	headers := map[string]string{"Range": "bytes=0-9", "X-Api-Key": "k", "Authorization": "Bearer t"}
	resp, err := client.GetResponseNoRetry(context.Background(), origin.URL+"/start", headers)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	assert.Equal(t, "bytes=0-9", got["same"].Get("Range"))
	assert.Equal(t, "Bearer t", got["same"].Get("Authorization"), "kept on the same host")
	assert.Equal(t, origin.URL+"/start", got["same"].Get("Referer"))

	assert.Equal(t, "bytes=0-9", got["other"].Get("Range"))
	assert.Equal(t, "k", got["other"].Get("X-Api-Key"))
	assert.Empty(t, got["other"].Get("Authorization"), "stripped once the chain leaves the host")
	assert.Equal(t, origin.URL+"/same", got["other"].Get("Referer"))
}

// A redirect status without Location is returned to the caller unfollowed.
func TestDoPaced_RedirectWithoutLocationReturned(t *testing.T) {
	srv, _ := statusServer(t, http.StatusFound, "")
	client := pacedClient(5*time.Second, nil, 0, &fakeHostLimiter{provider: "p"})

	resp, err := client.GetResponseNoRetry(context.Background(), srv.URL, nil)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, http.StatusFound, resp.StatusCode)
}

// refuseAfterLimiter admits the first allow waits and refuses the rest.
type refuseAfterLimiter struct {
	mu    sync.Mutex
	allow int
}

func (r *refuseAfterLimiter) WaitHost(context.Context, string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.allow > 0 {
		r.allow--
		return "p", nil
	}
	return "p", errors.New("rate limiter queue timeout")
}

func (*refuseAfterLimiter) Penalize(string, time.Duration) {}

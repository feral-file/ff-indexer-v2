package adapter

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/feral-file/ff-indexer-v2/internal/security/ssrf"
)

// defaultMaxRedirects matches net/http's own cap, used when no SSRF policy sets one.
const defaultMaxRedirects = 10

// redirectDrainBytes is how much of a redirect body is read before closing it so the
// connection can be reused (net/http drains the same amount).
const redirectDrainBytes = 2 << 10

// sensitiveRedirectHeaders are dropped once a redirect leaves the original host (or its
// subdomains), mirroring net/http's redirect policy.
var sensitiveRedirectHeaders = []string{"Authorization", "Www-Authenticate", "Cookie", "Cookie2", "Proxy-Authorization"}

// doPaced sends req and follows its redirects itself, so every hop waits for host
// capacity before its own Client.Timeout starts.
//
// Reason: when http.Client follows redirects, every later hop is only visible to the
// transport, inside the exchange that Client.Timeout bounds; such a hop could wait at
// most half the remaining timeout. An Art Blocks tokenURI is three hops on one 5 rps
// bucket, so once ~37 requests were queued every fetch died on a redirect hop after
// spending capacity on the earlier ones (2026-10-02, ~950 Ringers fetches). Following
// redirects here gives each hop the same treatment as the first: a full
// max_queue_time slot wait (bounded by ctx) and then its own Client.Timeout.
//
// Trade-offs: a slow chain can now take up to (maxRedirects+1) × Client.Timeout plus
// slot waits, instead of one Client.Timeout for the whole chain. A queued hop no longer
// fails mid-chain; callers needing a tighter bound pass a ctx deadline, which the
// limiter honors (it refuses a wait that cannot fit).
//
// Constraints: preserves net/http's redirect semantics that matter here (method and
// body rules per status, relative Location, Referer, sensitive-header stripping across
// hosts, the redirect cap) and this package's SSRF policy: the cap is enforced before
// the next URL is validated, and a URL the validator refuses never takes a slot. The
// transport's ssrfRoundTripper still validates every hop before connecting.
func (c *RealHTTPClient) doPaced(req *http.Request) (*http.Response, error) {
	orig := req
	stripSensitive := false
	for redirects := 0; ; redirects++ {
		resp, err := c.sendPaced(req)
		if err != nil {
			return nil, err
		}
		next, err := c.nextHop(orig, req, resp, redirects, &stripSensitive)
		if next == nil && err == nil {
			return resp, nil
		}
		drainAndClose(resp)
		if err != nil {
			return nil, err
		}
		req = next
	}
}

// sendPaced waits for req's host capacity, marks the request as charged so the
// transport does not charge it again, and sends exactly one hop.
func (c *RealHTTPClient) sendPaced(req *http.Request) (*http.Response, error) {
	provider, err := waitHost(req.Context(), c.limiter, req)
	if err != nil {
		return nil, err
	}
	// Marked even for unlimited hosts ("") so the transport does not look the host up twice.
	charged := req.WithContext(withPrecharged(req.Context(), provider))
	return c.client.Do(charged)
}

// nextHop returns the request for the redirect resp asks for, or (nil, nil) when resp
// is the final response. orig is the caller's request (source of headers, matching
// net/http, which copies from the initial request on every hop).
func (c *RealHTTPClient) nextHop(orig, prev *http.Request, resp *http.Response, redirects int, stripSensitive *bool) (*http.Request, error) {
	method, keepBody, ok := redirectMethod(prev, resp)
	if !ok {
		return nil, nil
	}
	loc := resp.Header.Get("Location")

	maxRedirects := c.maxRedirects
	if redirects >= maxRedirects {
		return nil, redirectError(prev, loc, c.redirectCapError(maxRedirects))
	}
	target, err := prev.URL.Parse(loc)
	if err != nil {
		return nil, redirectError(prev, loc, fmt.Errorf("failed to parse Location header %q: %w", loc, err))
	}
	if c.validator != nil {
		if err := c.validator.ValidateHTTPURL(prev.Context(), target.String()); err != nil {
			return nil, redirectError(prev, target.String(), fmt.Errorf("redirect blocked: %w", err))
		}
	}
	return buildRedirectRequest(orig, prev, target, method, keepBody, stripSensitive)
}

// redirectMethod reports whether resp is a redirect to follow and, if so, the method
// for the next hop and whether the body is resent. 301/302/303 switch non-GET/HEAD to
// GET without a body; 307/308 keep both, and are not followed when the body cannot be
// replayed. A redirect without Location is returned to the caller as-is.
func redirectMethod(prev *http.Request, resp *http.Response) (method string, keepBody bool, ok bool) {
	if resp.Header.Get("Location") == "" {
		return "", false, false
	}
	switch resp.StatusCode {
	case http.StatusMovedPermanently, http.StatusFound, http.StatusSeeOther:
		if prev.Method != http.MethodGet && prev.Method != http.MethodHead {
			return http.MethodGet, false, true
		}
		return prev.Method, false, true
	case http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
		hasBody := prev.Body != nil && prev.Body != http.NoBody
		if hasBody && prev.GetBody == nil {
			return "", false, false
		}
		return prev.Method, true, true
	default:
		return "", false, false
	}
}

// buildRedirectRequest creates the next hop with the caller's context and headers.
func buildRedirectRequest(orig, prev *http.Request, target *url.URL, method string, keepBody bool, stripSensitive *bool) (*http.Request, error) {
	var body io.ReadCloser
	if keepBody && prev.GetBody != nil {
		b, err := prev.GetBody()
		if err != nil {
			return nil, redirectError(prev, target.String(), err)
		}
		body = b
	}
	// target passed the redirect cap and SSRF validator in nextHop, and ssrfRoundTripper
	// validates it again before connecting.
	next, err := http.NewRequestWithContext(orig.Context(), method, target.String(), body) //nolint:gosec // G704: validated above
	if err != nil {
		return nil, redirectError(prev, target.String(), err)
	}
	if keepBody {
		next.GetBody = prev.GetBody
		next.ContentLength = prev.ContentLength
	}

	next.Header = orig.Header.Clone()
	if !keepBody {
		next.Header.Del("Content-Type")
		next.Header.Del("Content-Length")
	}
	if !*stripSensitive && !sameOrSubdomain(orig.URL, target) {
		*stripSensitive = true
	}
	if *stripSensitive {
		for _, h := range sensitiveRedirectHeaders {
			next.Header.Del(h)
		}
	}
	if orig.Header.Get("Referer") == "" {
		if ref := refererFor(prev.URL, target); ref != "" {
			next.Header.Set("Referer", ref)
		}
	}
	return next, nil
}

// redirectCapError is the cap error net/http would return, classified as SSRF policy
// (ssrf.ErrBlocked) when the cap comes from an SSRF validator configuration.
func (c *RealHTTPClient) redirectCapError(maxRedirects int) error {
	if c.validator != nil {
		return fmt.Errorf("%w: stopped after %d redirects", ssrf.ErrBlocked, maxRedirects)
	}
	return fmt.Errorf("stopped after %d redirects", maxRedirects)
}

// redirectError wraps err the way net/http reports redirect failures.
func redirectError(prev *http.Request, target string, err error) error {
	op := prev.Method
	if op != "" {
		op = op[:1] + strings.ToLower(op[1:])
	}
	return &url.Error{Op: op, URL: target, Err: err}
}

// sameOrSubdomain reports whether dst is src's host or one of its subdomains, the
// condition under which net/http keeps sensitive headers across a redirect.
func sameOrSubdomain(src, dst *url.URL) bool {
	s, d := strings.ToLower(src.Hostname()), strings.ToLower(dst.Hostname())
	return d == s || strings.HasSuffix(d, "."+s)
}

// refererFor returns the Referer net/http would send for a redirect from prev to
// target: prev without user info, and nothing on an https → http downgrade.
func refererFor(prev, target *url.URL) string {
	if prev.Scheme == "https" && target.Scheme == "http" {
		return ""
	}
	ref := *prev
	ref.User = nil
	return ref.String()
}

// drainAndClose reads a little of a discarded response so its connection can be
// reused, then closes it.
func drainAndClose(resp *http.Response) {
	_, _ = io.CopyN(io.Discard, resp.Body, redirectDrainBytes)
	_ = resp.Body.Close()
}

// errNoRedirectFollow makes http.Client return every redirect response unfollowed, so
// doPaced can pace each hop.
func errNoRedirectFollow(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

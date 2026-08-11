//go:build chromium

// Package probe's live-browser smoke test.
//
// Reason: every other test in this package mocks adapter.ChromedpClient, so CI verifies
// the probe's logic but never that chromium is present, navigates, captures a viewport,
// or honors request interception. This file closes that gap for pre-enablement
// verification without making ordinary `make check` runs depend on a browser.
//
// Run it against the real browser in the CGO image (see docs/media_viewability.md):
//
//	go test -tags="cgo chromium" ./internal/media/probe/ -run TestChromiumSmoke -v
//
// Constraints: requires a chromium binary on PATH. Serves fixtures from a local
// httptest server so the test needs no network egress; the SSRF case points at a
// link-local address that must be refused rather than fetched.
package probe_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/media/phash"
	"github.com/feral-file/ff-indexer-v2/internal/media/probe"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

// loopbackAllowingValidator permits the test server's loopback origin (which a real SSRF
// policy would refuse) while still refusing link-local metadata addresses, so the smoke
// can exercise both interception outcomes locally.
type loopbackAllowingValidator struct{ allowHost string }

func (v loopbackAllowingValidator) ValidateHTTPURL(_ context.Context, rawURL string) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return err
	}
	if u.Host == v.allowHost {
		return nil
	}
	host := u.Hostname()
	if ip := net.ParseIP(host); ip != nil && (ip.IsLinkLocalUnicast() || ip.IsPrivate() || ip.IsLoopback()) {
		return fmt.Errorf("blocked: %s", host)
	}
	return errors.New("blocked: not allowlisted for smoke test")
}

var _ adapter.SSRFValidator = loopbackAllowingValidator{}

const (
	// A deterministic, clearly non-blank page.
	contentHTML = `<html><body style="margin:0">
		<div style="width:100vw;height:100vh;background:linear-gradient(135deg,#0af,#f0a)"></div>
		</body></html>`
	// Visible frame is blank; content sits far below the fold. A full-page capture would
	// classify this as rendered_ok — a viewport capture must call it blank.
	belowFoldHTML = `<html><body style="margin:0;background:#000">
		<div style="height:20000px"></div>
		<div style="width:100vw;height:100vh;background:linear-gradient(135deg,#0af,#f0a)"></div>
		</body></html>`
	// Attempts to reach the cloud-metadata endpoint from inside the page.
	metadataFetchHTML = `<html><body style="margin:0;background:#fff">
		<img src="http://169.254.169.254/latest/meta-data/" onerror="document.title='blocked'">
		<div style="width:100vw;height:100vh;background:linear-gradient(45deg,#093,#fc0)"></div>
		</body></html>`

	// Child-target escape attempts: a dedicated worker, an iframe, and a popup, each
	// reaching for the metadata endpoint. Interception is installed on the page target,
	// so this pins which child contexts it actually covers.
	childTargetTmpl = `<html><body style="margin:0;background:#fff">
		<div style="width:100vw;height:100vh;background:linear-gradient(45deg,#093,#fc0)"></div>
		<script>%s</script></body></html>`
	workerEscapeJS = `try{const b=new Blob(["fetch('http://169.254.169.254/worker').catch(()=>{})"],` +
		`{type:'application/javascript'});new Worker(URL.createObjectURL(b));}catch(e){}`
	iframeEscapeJS = `try{const f=document.createElement('iframe');` +
		`f.src='http://169.254.169.254/iframe';document.body.appendChild(f);}catch(e){}`
	// The popup targets the (allowed) test server so a server-side hit proves a new web
	// contents opened and issued a request the interceptor never saw.
	popupEscapeJS = `try{window.open('/popup-escaped','_blank');}catch(e){}`
)

// popupHits counts requests made by a popup that managed to open — it must stay at zero.
var popupHits int32

func TestChromiumSmoke(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/content", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(contentHTML))
	})
	mux.HandleFunc("/below-fold", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(belowFoldHTML))
	})
	mux.HandleFunc("/metadata-fetch", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(metadataFetchHTML))
	})
	for path, js := range map[string]string{
		"/child-worker": workerEscapeJS,
		"/child-iframe": iframeEscapeJS,
		"/child-popup":  popupEscapeJS,
	} {
		body := fmt.Sprintf(childTargetTmpl, js)
		mux.HandleFunc(path, func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/html")
			_, _ = w.Write([]byte(body))
		})
	}
	mux.HandleFunc("/popup-escaped", func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&popupHits, 1)
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	srvURL, err := url.Parse(srv.URL)
	require.NoError(t, err)
	validator := loopbackAllowingValidator{allowHost: srvURL.Host}

	renderer := probe.NewRenderer(adapter.NewChromedpClient(), &probe.RendererConfig{
		ViewportWidth:    512,
		ViewportHeight:   512,
		TimeoutMs:        30000,
		SettleMs:         1000,
		AllocatorOptions: probe.AllocatorOptions(),
		SSRFValidator:    validator,
	})
	defer func() { _ = renderer.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	t.Run("renders a real page and records engine and viewport", func(t *testing.T) {
		capture, err := renderer.RenderProbe(ctx, srv.URL+"/content")
		require.NoError(t, err, "chromium must be present and able to navigate")
		require.NotNil(t, capture.Image)

		assert.Equal(t, "512x512", capture.Viewport)
		assert.Contains(t, capture.EngineVersion, "Chrome", "engine version is recorded with every capture")
		bounds := capture.Image.Bounds()
		assert.LessOrEqual(t, bounds.Dx(), 512*4, "capture must be viewport-bounded, not full-document")

		cls, err := probe.Classify(capture.Image, nil, 0.001)
		require.NoError(t, err)
		assert.Equal(t, schema.RenderProbeVerdictRenderedOK, cls.Verdict)

		// The same page must hash identically across runs on one engine — the property
		// the whole fingerprint/drift design rests on.
		second, err := renderer.RenderProbe(ctx, srv.URL+"/content")
		require.NoError(t, err)
		secondCls, err := probe.Classify(second.Image, nil, 0.001)
		require.NoError(t, err)
		assert.LessOrEqual(t, phash.Distance(cls.Phash, secondCls.Phash), 2,
			"repeat captures of a deterministic page must be stable")
	})

	t.Run("viewport capture classifies a below-the-fold page as blank", func(t *testing.T) {
		capture, err := renderer.RenderProbe(ctx, srv.URL+"/below-fold")
		require.NoError(t, err)
		cls, err := probe.Classify(capture.Image, nil, 0.001)
		require.NoError(t, err)
		assert.Equal(t, schema.RenderProbeVerdictBlank, cls.Verdict,
			"content below the fold must not rescue a blank viewport")
	})

	t.Run("blocks a page-initiated request to the metadata endpoint", func(t *testing.T) {
		capture, err := renderer.RenderProbe(ctx, srv.URL+"/metadata-fetch")
		require.NoError(t, err)
		assert.Positive(t, capture.BlockedRequests,
			"the page's request to 169.254.169.254 must be refused by the interceptor")
	})

	// Child targets: interception is installed on the page target, so workers and
	// iframes must still be covered by it, and a popup — which would get its own,
	// uncovered target — must be prevented from opening at all.
	t.Run("blocks a worker request to a private address", func(t *testing.T) {
		capture, err := renderer.RenderProbe(ctx, srv.URL+"/child-worker")
		require.NoError(t, err)
		assert.Positive(t, capture.BlockedRequests,
			"a dedicated worker's request must pass through the page target's interceptor")
	})

	t.Run("blocks an iframe request to a private address", func(t *testing.T) {
		capture, err := renderer.RenderProbe(ctx, srv.URL+"/child-iframe")
		require.NoError(t, err)
		assert.Positive(t, capture.BlockedRequests,
			"an iframe's request must pass through the page target's interceptor")
	})

	t.Run("prevents a popup from opening a new uncovered target", func(t *testing.T) {
		before := atomic.LoadInt32(&popupHits)
		_, err := renderer.RenderProbe(ctx, srv.URL+"/child-popup")
		require.NoError(t, err)
		assert.Equal(t, before, atomic.LoadInt32(&popupHits),
			"block-new-web-contents must stop the popup opening; otherwise its requests bypass interception")
	})
}

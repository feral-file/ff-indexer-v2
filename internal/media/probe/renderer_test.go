package probe_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/fetch"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/chromedp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/media/probe"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
)

// encodePNG encodes the shared gradient frame so the renderer's decode path runs on
// real bytes.
func encodePNG(t *testing.T) []byte {
	var buf bytes.Buffer
	require.NoError(t, png.Encode(&buf, gradientFrame()))
	return buf.Bytes()
}

type rendererHarness struct {
	chromedp *mocks.MockChromedpClient
	ssrf     *mocks.MockSSRFValidator
	renderer probe.Renderer
	// listeners captures every event handler the renderer installs (the main-document
	// status tracker and, with a validator, the Fetch interception handler); emit
	// broadcasts to all of them, mirroring chromedp's target event delivery.
	listeners []func(ev any)
}

// emit delivers an event to every installed listener, like chromedp broadcasts target
// events.
func (h *rendererHarness) emit(ev any) {
	for _, fn := range h.listeners {
		fn(ev)
	}
}

func newMockedRenderer(t *testing.T, validator *mocks.MockSSRFValidator) *rendererHarness {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	h := &rendererHarness{
		chromedp: mocks.NewMockChromedpClient(ctrl),
		ssrf:     validator,
	}

	h.chromedp.EXPECT().
		NewExecAllocator(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ []chromedp.ExecAllocatorOption) (context.Context, context.CancelFunc) {
			return context.WithCancel(ctx)
		})

	cfg := &probe.RendererConfig{
		ViewportWidth:  640,
		ViewportHeight: 480,
		TimeoutMs:      5000,
		SettleMs:       10,
	}
	// The status tracker is installed on every render, validator or not.
	h.chromedp.EXPECT().NetworkEnable().Return(nil).AnyTimes()
	h.chromedp.EXPECT().
		ListenTarget(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, fn func(ev any)) { h.listeners = append(h.listeners, fn) }).
		AnyTimes()
	if validator != nil {
		cfg.SSRFValidator = validator
		h.chromedp.EXPECT().FetchEnable().Return(nil).AnyTimes()
		h.chromedp.EXPECT().AddScriptToEvaluateOnNewDocument(gomock.Any()).Return(nil).AnyTimes()
	}

	h.renderer = probe.NewRenderer(h.chromedp, cfg)
	t.Cleanup(func() { _ = h.renderer.Close() })
	return h
}

// expectRenderActions sets up the standard action-builder expectations, filling the
// screenshot buffer with pngBytes and the user agent.
func (h *rendererHarness) expectRenderActions(t *testing.T, url string, pngBytes []byte) {
	t.Helper()
	h.chromedp.EXPECT().
		NewContext(gomock.Any()).
		DoAndReturn(func(ctx context.Context) (context.Context, context.CancelFunc) {
			return context.WithCancel(ctx)
		})
	h.chromedp.EXPECT().EmulateViewport(int64(640), int64(480)).Return(nil)
	h.chromedp.EXPECT().Navigate(url).Return(nil)
	h.chromedp.EXPECT().WaitReady(":root").Return(nil)
	h.chromedp.EXPECT().
		Evaluate("navigator.userAgent", gomock.Any()).
		DoAndReturn(func(_ string, res any, _ ...chromedp.EvaluateOption) chromedp.EvaluateAction {
			*(res.(*string)) = "HeadlessChrome/123.0"
			return nil
		})
	h.chromedp.EXPECT().Sleep(10 * time.Millisecond).Return(nil)
	// Viewport-bounded capture; FullScreenshot must never be used by the probe.
	h.chromedp.EXPECT().
		CaptureScreenshot(gomock.Any()).
		DoAndReturn(func(res *[]byte) chromedp.Action {
			*res = pngBytes
			return nil
		})
}

func TestRenderProbe_capturesViewportFrameAndEngine(t *testing.T) {
	h := newMockedRenderer(t, nil)
	url := "https://example.com/work.html"

	h.expectRenderActions(t, url, encodePNG(t))
	h.chromedp.EXPECT().
		Run(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	capture, err := h.renderer.RenderProbe(context.Background(), url, 0)
	require.NoError(t, err)
	require.NotNil(t, capture)
	assert.Equal(t, "HeadlessChrome/123.0", capture.EngineVersion)
	assert.Equal(t, "640x480", capture.Viewport)
	require.NotNil(t, capture.Image)
	assert.Equal(t, 0, capture.BlockedRequests)
	assert.Equal(t, 0, capture.MainStatus, "no network events observed means status unknown")
}

// TestRenderProbe_usesViewportCaptureNotFullPage pins the bound: an untrusted page can
// make the document arbitrarily tall, so the probe must never ask for a full-page
// screenshot. The strict mock has no FullScreenshot expectation.
func TestRenderProbe_usesViewportCaptureNotFullPage(t *testing.T) {
	h := newMockedRenderer(t, nil)
	url := "https://example.com/tall-page.html"

	h.expectRenderActions(t, url, encodePNG(t))
	h.chromedp.EXPECT().
		Run(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	_, err := h.renderer.RenderProbe(context.Background(), url, 0)
	require.NoError(t, err)
}

func TestRenderProbe_runErrorIsStalledSignal(t *testing.T) {
	h := newMockedRenderer(t, nil)
	url := "https://example.com/hangs.html"

	h.chromedp.EXPECT().
		NewContext(gomock.Any()).
		DoAndReturn(func(ctx context.Context) (context.Context, context.CancelFunc) {
			return context.WithCancel(ctx)
		})
	h.chromedp.EXPECT().EmulateViewport(gomock.Any(), gomock.Any()).Return(nil)
	h.chromedp.EXPECT().Navigate(url).Return(nil)
	h.chromedp.EXPECT().WaitReady(":root").Return(nil)
	h.chromedp.EXPECT().Evaluate(gomock.Any(), gomock.Any()).Return(nil)
	h.chromedp.EXPECT().Sleep(gomock.Any()).Return(nil)
	h.chromedp.EXPECT().CaptureScreenshot(gomock.Any()).Return(nil)
	h.chromedp.EXPECT().
		Run(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(context.DeadlineExceeded)

	capture, err := h.renderer.RenderProbe(context.Background(), url, 0)
	require.Error(t, err)
	assert.Nil(t, capture)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

// TestRenderProbe_callerCancellationSurfaces verifies job cancellation reaches the
// browser context (via the context.AfterFunc bridge, since the browser context hangs off
// the long-lived allocator) and is reported as cancellation rather than as an artwork
// verdict.
func TestRenderProbe_callerCancellationSurfaces(t *testing.T) {
	h := newMockedRenderer(t, nil)
	url := "https://example.com/slow.html"

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // job already canceled when the probe starts

	var browserCtx context.Context
	h.chromedp.EXPECT().
		NewContext(gomock.Any()).
		DoAndReturn(func(parent context.Context) (context.Context, context.CancelFunc) {
			c, cancelBrowser := context.WithCancel(parent)
			browserCtx = c
			return c, cancelBrowser
		})
	h.chromedp.EXPECT().EmulateViewport(gomock.Any(), gomock.Any()).Return(nil)
	h.chromedp.EXPECT().Navigate(url).Return(nil)
	h.chromedp.EXPECT().WaitReady(":root").Return(nil)
	h.chromedp.EXPECT().Evaluate(gomock.Any(), gomock.Any()).Return(nil)
	h.chromedp.EXPECT().Sleep(gomock.Any()).Return(nil)
	h.chromedp.EXPECT().CaptureScreenshot(gomock.Any()).Return(nil)
	// Real chromedp returns the context error once its context is done.
	h.chromedp.EXPECT().
		Run(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(errors.New("chromedp: context canceled"))

	capture, err := h.renderer.RenderProbe(ctx, url, 0)
	require.Error(t, err)
	assert.Nil(t, capture)
	assert.ErrorIs(t, err, context.Canceled, "cancellation must surface as cancellation, not a render verdict")

	// The bridge fired: the browser context derived from the allocator is canceled even
	// though the allocator itself (rooted at Background) is still alive.
	require.NotNil(t, browserCtx)
	assert.Error(t, browserCtx.Err(), "caller cancellation must cancel the browser context")
}

func TestRenderProbe_undecodableScreenshotErrors(t *testing.T) {
	h := newMockedRenderer(t, nil)
	url := "https://example.com/garbage.html"

	h.expectRenderActions(t, url, []byte("not a png"))
	h.chromedp.EXPECT().
		Run(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	capture, err := h.renderer.RenderProbe(context.Background(), url, 0)
	require.Error(t, err)
	assert.Nil(t, capture)
	assert.Contains(t, err.Error(), "screenshot")
}

// TestRenderProbe_rejectsOversizedScreenshot pins the decompression-bomb guard: the
// declared dimensions are checked from the PNG header before the pixel buffer is
// allocated.
func TestRenderProbe_rejectsOversizedScreenshot(t *testing.T) {
	h := newMockedRenderer(t, nil)
	url := "https://example.com/bomb.html"

	// 8000x8000 = 64M pixels, above the 16M cap. Encoding a uniform image of that size
	// is cheap; decoding it is what the guard prevents.
	huge := image.NewGray(image.Rect(0, 0, 8000, 8000))
	var buf bytes.Buffer
	require.NoError(t, png.Encode(&buf, huge))

	h.expectRenderActions(t, url, buf.Bytes())
	h.chromedp.EXPECT().
		Run(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	capture, err := h.renderer.RenderProbe(context.Background(), url, 0)
	require.Error(t, err)
	assert.Nil(t, capture)
	assert.Contains(t, err.Error(), "pixel cap")
}

// TestRenderProbe_interceptsBrowserRequests is the SSRF trust-path test: chromium does
// its own network I/O, so every paused request — navigation, redirect hop, subresource —
// must be validated, and refused destinations failed with AccessDenied.
//
// Paused requests are driven from the Sleep action builder, which the renderer
// constructs after interception is installed and before Run executes.
func TestRenderProbe_interceptsBrowserRequests(t *testing.T) {
	ctrl := gomock.NewController(t)
	validator := mocks.NewMockSSRFValidator(ctrl)
	h := newMockedRenderer(t, validator)
	url := "https://example.com/redirector.html"

	allowed := make(chan struct{})
	refused := make(chan struct{})

	// A permitted subresource proceeds.
	validator.EXPECT().
		ValidateHTTPURL(gomock.Any(), "https://cdn.example.com/lib.js").
		Return(nil)
	h.chromedp.EXPECT().
		ContinueRequest(gomock.Any(), fetch.RequestID("allowed")).
		DoAndReturn(func(context.Context, fetch.RequestID) error { close(allowed); return nil })

	// A redirect to the cloud-metadata endpoint is refused, never continued.
	validator.EXPECT().
		ValidateHTTPURL(gomock.Any(), "http://169.254.169.254/latest/meta-data/").
		Return(errors.New("blocked: link-local"))
	h.chromedp.EXPECT().
		FailRequest(gomock.Any(), fetch.RequestID("blocked"), network.ErrorReasonAccessDenied).
		DoAndReturn(func(context.Context, fetch.RequestID, network.ErrorReason) error { close(refused); return nil })

	h.chromedp.EXPECT().
		NewContext(gomock.Any()).
		DoAndReturn(func(ctx context.Context) (context.Context, context.CancelFunc) {
			return context.WithCancel(ctx)
		})
	h.chromedp.EXPECT().EmulateViewport(gomock.Any(), gomock.Any()).Return(nil)
	h.chromedp.EXPECT().Navigate(url).Return(nil)
	h.chromedp.EXPECT().WaitReady(":root").Return(nil)
	h.chromedp.EXPECT().
		Evaluate("navigator.userAgent", gomock.Any()).
		DoAndReturn(func(_ string, res any, _ ...chromedp.EvaluateOption) chromedp.EvaluateAction {
			*(res.(*string)) = "HeadlessChrome/123.0"
			return nil
		})
	h.chromedp.EXPECT().
		Sleep(gomock.Any()).
		DoAndReturn(func(time.Duration) chromedp.Action {
			require.NotEmpty(t, h.listeners, "Fetch interception must be installed before navigation")
			h.emit(&fetch.EventRequestPaused{
				RequestID: "allowed",
				Request:   &network.Request{URL: "https://cdn.example.com/lib.js"},
			})
			h.emit(&fetch.EventRequestPaused{
				RequestID: "blocked",
				Request:   &network.Request{URL: "http://169.254.169.254/latest/meta-data/"},
			})
			<-allowed
			<-refused
			return nil
		})
	pngBytes := encodePNG(t)
	h.chromedp.EXPECT().
		CaptureScreenshot(gomock.Any()).
		DoAndReturn(func(res *[]byte) chromedp.Action { *res = pngBytes; return nil })
	h.chromedp.EXPECT().
		Run(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	capture, err := h.renderer.RenderProbe(context.Background(), url, 0)
	require.NoError(t, err)
	require.NotNil(t, capture)
	assert.Equal(t, 1, capture.BlockedRequests, "the refused request is counted on the capture")
}

// solidGray keeps the oversized-image helper honest about memory in CI.
var _ = color.Gray{}

// TestRenderProbe_boundsConcurrentRequestDecisions is the stress case for the request
// decision pool: the page controls how many requests fire, and validation can hit DNS,
// so per-event goroutines (the previous shape) let one hostile page fan out thousands of
// resolver-backed goroutines. The pool must cap concurrent validations at the worker
// count, accept at most queue+workers requests, and silently drop the flood's excess —
// dropped requests stay paused, which is the attacker's own stall, not our spend. With
// per-event spawning this test fails immediately: all 2000 validations run at once.
func TestRenderProbe_boundsConcurrentRequestDecisions(t *testing.T) {
	const flood = 2000
	const maxAccepted = 256 + 8 // requestDecisionQueue + requestDecisionWorkers

	ctrl := gomock.NewController(t)
	validator := mocks.NewMockSSRFValidator(ctrl)
	h := newMockedRenderer(t, validator)
	url := "https://example.com/flood.html"

	gate := make(chan struct{})
	var inflight, maxInflight, validated, decided atomic.Int64
	validator.EXPECT().
		ValidateHTTPURL(gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, string) error {
			cur := inflight.Add(1)
			for {
				prev := maxInflight.Load()
				if cur <= prev || maxInflight.CompareAndSwap(prev, cur) {
					break
				}
			}
			<-gate
			inflight.Add(-1)
			validated.Add(1)
			return nil
		}).
		AnyTimes()
	h.chromedp.EXPECT().
		ContinueRequest(gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, fetch.RequestID) error { decided.Add(1); return nil }).
		AnyTimes()

	h.chromedp.EXPECT().
		NewContext(gomock.Any()).
		DoAndReturn(func(ctx context.Context) (context.Context, context.CancelFunc) {
			return context.WithCancel(ctx)
		})
	h.chromedp.EXPECT().EmulateViewport(gomock.Any(), gomock.Any()).Return(nil)
	h.chromedp.EXPECT().Navigate(url).Return(nil)
	h.chromedp.EXPECT().WaitReady(":root").Return(nil)
	h.chromedp.EXPECT().
		Evaluate("navigator.userAgent", gomock.Any()).
		DoAndReturn(func(_ string, res any, _ ...chromedp.EvaluateOption) chromedp.EvaluateAction {
			*(res.(*string)) = "HeadlessChrome/123.0"
			return nil
		})
	h.chromedp.EXPECT().
		Sleep(gomock.Any()).
		DoAndReturn(func(time.Duration) chromedp.Action {
			require.NotEmpty(t, h.listeners)
			for i := range flood {
				h.emit(&fetch.EventRequestPaused{
					RequestID: fetch.RequestID(fmt.Sprintf("req-%d", i)),
					Request:   &network.Request{URL: fmt.Sprintf("https://cdn.example.com/a%d.js", i)},
				})
			}
			// All workers should be wedged in validation now; nothing beyond the worker
			// count may validate concurrently no matter how many events fired.
			require.Eventually(t, func() bool { return inflight.Load() == 8 },
				2*time.Second, time.Millisecond, "all workers blocked in validation")
			close(gate)
			// Drain: every accepted request gets a decision; the flood's excess got none.
			require.Eventually(t, func() bool { return decided.Load() >= 256 },
				5*time.Second, time.Millisecond)
			time.Sleep(150 * time.Millisecond) // settle: no further decisions may trickle in
			return nil
		})
	pngBytes := encodePNG(t)
	h.chromedp.EXPECT().
		CaptureScreenshot(gomock.Any()).
		DoAndReturn(func(res *[]byte) chromedp.Action { *res = pngBytes; return nil })
	h.chromedp.EXPECT().
		Run(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	capture, err := h.renderer.RenderProbe(context.Background(), url, 0)
	require.NoError(t, err)
	require.NotNil(t, capture)

	assert.LessOrEqual(t, maxInflight.Load(), int64(8),
		"concurrent validations must never exceed the worker pool")
	assert.GreaterOrEqual(t, decided.Load(), int64(256), "everything accepted is decided")
	assert.LessOrEqual(t, decided.Load(), int64(maxAccepted),
		"the flood's excess must be dropped, not queued or decided")
	assert.Equal(t, validated.Load(), decided.Load(), "every decision came through validation")
}

// expectFailingRun sets up the builder expectations for a render whose Run fails with
// runErr before any action executes.
func (h *rendererHarness) expectFailingRun(runErr error) {
	h.chromedp.EXPECT().
		NewContext(gomock.Any()).
		DoAndReturn(func(ctx context.Context) (context.Context, context.CancelFunc) {
			return context.WithCancel(ctx)
		})
	h.chromedp.EXPECT().EmulateViewport(gomock.Any(), gomock.Any()).Return(nil)
	h.chromedp.EXPECT().Navigate(gomock.Any()).Return(nil)
	h.chromedp.EXPECT().WaitReady(":root").Return(nil)
	h.chromedp.EXPECT().Evaluate(gomock.Any(), gomock.Any()).Return(nil)
	h.chromedp.EXPECT().Sleep(gomock.Any()).Return(nil)
	h.chromedp.EXPECT().CaptureScreenshot(gomock.Any()).Return(nil)
	h.chromedp.EXPECT().
		Run(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(runErr)
}

// TestRenderProbe_launchFailureIsBrowserUnavailable pins the infrastructure/evidence
// split: a browser that never started must surface as ErrBrowserUnavailable so the
// executor retries the job instead of recording a stalled verdict. The fixture errors
// are the literal shapes observed in the 2026-08-17 fork-exhaustion incident, which
// recorded ~2,100 launch failures as artwork evidence.
func TestRenderProbe_launchFailureIsBrowserUnavailable(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"fork failure", &os.PathError{Op: "fork/exec", Path: "/usr/bin/chromium", Err: errors.New("resource temporarily unavailable")}},
		{"startup failure", errors.New("chrome failed to start:\n/usr/bin/chromium: line 12: can't fork: Resource temporarily unavailable")},
		{"devtools socket timeout", errors.New("websocket url timeout reached")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newMockedRenderer(t, nil)
			h.expectFailingRun(tc.err)

			capture, err := h.renderer.RenderProbe(context.Background(), "https://example.com/work.html", 0)
			require.Error(t, err)
			assert.Nil(t, capture)
			assert.ErrorIs(t, err, probe.ErrBrowserUnavailable)
		})
	}
}

// TestRenderProbe_pageLoadFailureIsNotBrowserUnavailable keeps the boundary honest in
// the other direction: an in-page load failure stays a plain error (the executor's
// stalled signal) and must never be mistaken for browser unavailability, or genuinely
// broken URLs would be retried forever without ever recording a verdict.
func TestRenderProbe_pageLoadFailureIsNotBrowserUnavailable(t *testing.T) {
	h := newMockedRenderer(t, nil)
	h.expectFailingRun(errors.New("page load error net::ERR_ABORTED"))

	capture, err := h.renderer.RenderProbe(context.Background(), "https://example.com/download.bin", 0)
	require.Error(t, err)
	assert.Nil(t, capture)
	assert.NotErrorIs(t, err, probe.ErrBrowserUnavailable)
}

// TestRenderProbe_recordsMainDocumentStatus pins main-document status capture: the final
// response of the navigated frame is recorded, while iframe documents and subresources —
// whose statuses say nothing about what the server sent for the artwork itself — must
// not overwrite it. Chromium paints HTTP error bodies like normal pages (production
// measurement: ipfs.io's 410 bot-block page classified 1,692 healthy artworks as
// blank), so this status is the executor's only signal that the frame is not the
// artwork.
func TestRenderProbe_recordsMainDocumentStatus(t *testing.T) {
	h := newMockedRenderer(t, nil)
	url := "https://example.com/blocked.html"

	h.chromedp.EXPECT().
		NewContext(gomock.Any()).
		DoAndReturn(func(ctx context.Context) (context.Context, context.CancelFunc) {
			return context.WithCancel(ctx)
		})
	h.chromedp.EXPECT().EmulateViewport(gomock.Any(), gomock.Any()).Return(nil)
	h.chromedp.EXPECT().Navigate(url).Return(nil)
	h.chromedp.EXPECT().WaitReady(":root").Return(nil)
	h.chromedp.EXPECT().
		Evaluate("navigator.userAgent", gomock.Any()).
		DoAndReturn(func(_ string, res any, _ ...chromedp.EvaluateOption) chromedp.EvaluateAction {
			*(res.(*string)) = "HeadlessChrome/123.0"
			return nil
		})
	// Network events are emitted from the Sleep builder, after the tracker's listener is
	// installed and before Run executes — same trick as the interception tests.
	h.chromedp.EXPECT().
		Sleep(gomock.Any()).
		DoAndReturn(func(time.Duration) chromedp.Action {
			require.NotEmpty(t, h.listeners, "status tracker must be installed before navigation")
			mainFrame := cdp.FrameID("main-frame")
			iframe := cdp.FrameID("child-frame")
			// The navigation's document request pins the main frame.
			h.emit(&network.EventRequestWillBeSent{Type: network.ResourceTypeDocument, FrameID: mainFrame})
			h.emit(&network.EventResponseReceived{Type: network.ResourceTypeDocument, FrameID: mainFrame,
				Response: &network.Response{Status: 410}})
			// An iframe's document and a failing subresource must not overwrite it.
			h.emit(&network.EventRequestWillBeSent{Type: network.ResourceTypeDocument, FrameID: iframe})
			h.emit(&network.EventResponseReceived{Type: network.ResourceTypeDocument, FrameID: iframe,
				Response: &network.Response{Status: 200}})
			h.emit(&network.EventResponseReceived{Type: network.ResourceTypeStylesheet, FrameID: mainFrame,
				Response: &network.Response{Status: 500}})
			return nil
		})
	pngBytes := encodePNG(t)
	h.chromedp.EXPECT().
		CaptureScreenshot(gomock.Any()).
		DoAndReturn(func(res *[]byte) chromedp.Action { *res = pngBytes; return nil })
	h.chromedp.EXPECT().
		Run(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	capture, err := h.renderer.RenderProbe(context.Background(), url, 0)
	require.NoError(t, err)
	require.NotNil(t, capture)
	assert.Equal(t, 410, capture.MainStatus, "main frame's final document status wins")
}

package probe_test

import (
	"bytes"
	"context"
	"errors"
	"image"
	"image/color"
	"image/png"
	"testing"
	"time"

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
	// listener captures the Fetch event handler the renderer installs, so tests can
	// drive paused-request decisions without a browser.
	listener func(ev any)
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
	if validator != nil {
		cfg.SSRFValidator = validator
		h.chromedp.EXPECT().FetchEnable().Return(nil).AnyTimes()
		h.chromedp.EXPECT().
			ListenTarget(gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, fn func(ev any)) { h.listener = fn }).
			AnyTimes()
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
	h.chromedp.EXPECT().WaitReady("body").Return(nil)
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
		Run(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	capture, err := h.renderer.RenderProbe(context.Background(), url)
	require.NoError(t, err)
	require.NotNil(t, capture)
	assert.Equal(t, "HeadlessChrome/123.0", capture.EngineVersion)
	assert.Equal(t, "640x480", capture.Viewport)
	require.NotNil(t, capture.Image)
	assert.Equal(t, 0, capture.BlockedRequests)
}

// TestRenderProbe_usesViewportCaptureNotFullPage pins the bound: an untrusted page can
// make the document arbitrarily tall, so the probe must never ask for a full-page
// screenshot. The strict mock has no FullScreenshot expectation.
func TestRenderProbe_usesViewportCaptureNotFullPage(t *testing.T) {
	h := newMockedRenderer(t, nil)
	url := "https://example.com/tall-page.html"

	h.expectRenderActions(t, url, encodePNG(t))
	h.chromedp.EXPECT().
		Run(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	_, err := h.renderer.RenderProbe(context.Background(), url)
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
	h.chromedp.EXPECT().WaitReady("body").Return(nil)
	h.chromedp.EXPECT().Evaluate(gomock.Any(), gomock.Any()).Return(nil)
	h.chromedp.EXPECT().Sleep(gomock.Any()).Return(nil)
	h.chromedp.EXPECT().CaptureScreenshot(gomock.Any()).Return(nil)
	h.chromedp.EXPECT().
		Run(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(context.DeadlineExceeded)

	capture, err := h.renderer.RenderProbe(context.Background(), url)
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
	h.chromedp.EXPECT().WaitReady("body").Return(nil)
	h.chromedp.EXPECT().Evaluate(gomock.Any(), gomock.Any()).Return(nil)
	h.chromedp.EXPECT().Sleep(gomock.Any()).Return(nil)
	h.chromedp.EXPECT().CaptureScreenshot(gomock.Any()).Return(nil)
	// Real chromedp returns the context error once its context is done.
	h.chromedp.EXPECT().
		Run(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(errors.New("chromedp: context canceled"))

	capture, err := h.renderer.RenderProbe(ctx, url)
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
		Run(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	capture, err := h.renderer.RenderProbe(context.Background(), url)
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
		Run(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	capture, err := h.renderer.RenderProbe(context.Background(), url)
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
	h.chromedp.EXPECT().WaitReady("body").Return(nil)
	h.chromedp.EXPECT().
		Evaluate("navigator.userAgent", gomock.Any()).
		DoAndReturn(func(_ string, res any, _ ...chromedp.EvaluateOption) chromedp.EvaluateAction {
			*(res.(*string)) = "HeadlessChrome/123.0"
			return nil
		})
	h.chromedp.EXPECT().
		Sleep(gomock.Any()).
		DoAndReturn(func(time.Duration) chromedp.Action {
			require.NotNil(t, h.listener, "Fetch interception must be installed before navigation")
			h.listener(&fetch.EventRequestPaused{
				RequestID: "allowed",
				Request:   &network.Request{URL: "https://cdn.example.com/lib.js"},
			})
			h.listener(&fetch.EventRequestPaused{
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
		Run(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	capture, err := h.renderer.RenderProbe(context.Background(), url)
	require.NoError(t, err)
	require.NotNil(t, capture)
	assert.Equal(t, 1, capture.BlockedRequests, "the refused request is counted on the capture")
}

// solidGray keeps the oversized-image helper honest about memory in CI.
var _ = color.Gray{}

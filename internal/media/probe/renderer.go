// Package probe implements the L1 render probe: loading a media URL in headless
// chromium, capturing a frame, and classifying what was painted.
//
// The package splits along testability lines: renderer.go drives the browser (mockable
// via adapter.ChromedpClient), classifier.go is pure functions over the captured image.
// No cgo — chromedp speaks the DevTools protocol over a socket, so the lightweight build
// compiles this package even though only the media worker wires it.
package probe

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"image"
	"image/png"
	"time"

	"github.com/chromedp/cdproto/fetch"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/chromedp"
	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
)

const (
	// DefaultViewportWidth/Height are square by default: artwork aspect ratios vary, and
	// a square viewport biases the capture toward neither orientation.
	DefaultViewportWidth  = 1024
	DefaultViewportHeight = 1024
	// DefaultTimeoutMs bounds the whole probe (navigate + settle + screenshot).
	DefaultTimeoutMs = 30000
	// DefaultSettleMs is how long the page runs after load before the capture — long
	// enough for generative works to paint their first frame, matching the rasterizer's
	// settle behavior.
	DefaultSettleMs = 5000

	// maxScreenshotBytes caps the encoded PNG accepted from the browser. A viewport
	// capture of the default 1024x1024 is far below this; the cap exists so a hostile
	// page cannot drive unbounded allocation through an oversized capture.
	maxScreenshotBytes = 16 << 20
	// maxDecodedPixels caps the decoded frame. Sized well above any sane viewport
	// (4096x4096) so legitimate high-DPI captures pass while a decompression bomb does
	// not reach the hashing stage.
	maxDecodedPixels = 16 << 20
)

// ErrRequestBlocked reports that the SSRF policy refused a browser-initiated request.
var ErrRequestBlocked = errors.New("browser request blocked by SSRF policy")

// egressGuardScript neutralizes browser egress APIs the CDP Fetch domain cannot
// intercept, before any page script runs.
//
// Reason: measured against real chromium (TestEgressVectors), Fetch interception covers
// navigations, redirects, subresources, iframes, dedicated workers — including nested
// ones — and sendBeacon. It does NOT cover the WebSocket handshake, which CDP exposes for
// observation only; WebRTC and EventSource can likewise open connections the interceptor
// never sees. Rendering one artwork frame needs none of them, so they are removed rather
// than policed. Defense in depth, not a substitute for network-level egress restriction
// (see docs/media_viewability.md).
const egressGuardScript = `
(() => {
  const block = (name) => {
    try {
      Object.defineProperty(window, name, {
        configurable: false,
        get() { throw new Error(name + " is disabled in the render probe"); },
      });
    } catch (e) { /* already locked down */ }
  };
  ["WebSocket", "RTCPeerConnection", "webkitRTCPeerConnection", "RTCDataChannel", "EventSource"].forEach(block);
})();`

// Capture is one render observation of a URL.
type Capture struct {
	// Image is the decoded screenshot frame.
	Image image.Image
	// EngineVersion is the browser's User-Agent — recorded with every capture because a
	// pHash without its engine is not comparable to anything (chromium upgrades change
	// rasterization).
	EngineVersion string
	// Viewport is the capture viewport as "WxH".
	Viewport string
	// BlockedRequests counts requests the SSRF policy refused during this render. A
	// non-zero count means the page tried to reach a disallowed destination; the frame
	// is still classified (a blocked subresource usually just fails to paint).
	BlockedRequests int
}

// Renderer drives headless chromium to capture what a URL paints.
//
//go:generate mockgen -source=renderer.go -destination=../../mocks/render_probe_renderer.go -package=mocks -mock_names=Renderer=MockRenderProbeRenderer
type Renderer interface {
	// RenderProbe loads the URL, waits for it to settle, and screenshots the viewport.
	// Errors (navigation failure, timeout, cancellation) are the caller's "stalled"
	// signal.
	//
	// SECURITY: chromium fetches URLs itself, outside the Go HTTP client and its SSRF
	// RoundTripper. Every browser-initiated request — the navigation, each redirect hop,
	// and every subresource — is paused via the CDP Fetch domain and validated against
	// the SSRF policy before it is allowed to proceed; refused requests are failed with
	// AccessDenied. Callers should still validate the URL up front so an obviously
	// blocked target never launches a browser context at all.
	RenderProbe(ctx context.Context, url string) (*Capture, error)

	// Close releases the browser allocator. Call during shutdown.
	Close() error
}

// RendererConfig holds render-probe browser settings.
type RendererConfig struct {
	ViewportWidth  int
	ViewportHeight int
	TimeoutMs      int
	SettleMs       int
	// AllocatorOptions are the chromium launch flags; callers should pass
	// AllocatorOptions() unless they have a reason to diverge.
	AllocatorOptions []chromedp.ExecAllocatorOption
	// SSRFValidator vets every browser-initiated request. When nil, interception is not
	// installed (SSRF protection disabled by configuration).
	SSRFValidator adapter.SSRFValidator
}

// AllocatorOptions returns the chromium launch flags for the render probe.
//
// Reason: deliberately NOT the SVG rasterizer's flag set. The rasterizer renders bytes
// we fetched and validated ourselves from a data URI or temp file; the probe navigates
// untrusted remote pages, so it must not run them with web security disabled — that flag
// would let a hostile page read cross-origin (including private) responses and exfiltrate
// them. Everything else matches the rasterizer so container behavior stays predictable.
func AllocatorOptions() []chromedp.ExecAllocatorOption {
	return allocatorOptions(false)
}

// AllocatorOptionsNoSandbox is AllocatorOptions with chromium's sandbox disabled.
//
// Only for runtimes whose kernel/container policy cannot support the sandbox (no
// unprivileged user namespaces, restrictive seccomp). Running untrusted artwork in an
// unsandboxed renderer means a renderer exploit gains the media worker's process access —
// its configuration and database credentials — so prefer fixing the runtime.
func AllocatorOptionsNoSandbox() []chromedp.ExecAllocatorOption {
	return allocatorOptions(true)
}

func allocatorOptions(noSandbox bool) []chromedp.ExecAllocatorOption {
	opts := []chromedp.ExecAllocatorOption{
		chromedp.NoFirstRun,
		chromedp.NoDefaultBrowserCheck,
		chromedp.DisableGPU,
		chromedp.Headless,
		// NOTE: no disable-web-security here, by design (see doc comment).
		//
		// Child-target containment. Fetch interception is installed on the page target;
		// measured against real chromium, that covers the main frame, iframes, and
		// dedicated workers, but NOT a popup opened via window.open — a new web contents
		// gets its own target and would issue unintercepted requests. Rather than
		// auto-attaching to arbitrary child targets (a larger surface to get right), the
		// probe forbids them: rendering one artwork frame never legitimately requires a
		// popup or a service worker.
		chromedp.Flag("block-new-web-contents", true),
		chromedp.Flag("disable-shared-workers", true),
		chromedp.Flag("disable-features", "ServiceWorker"),
		chromedp.Flag("disable-popup-blocking", false),
		chromedp.Flag("disable-dev-shm-usage", true),
		chromedp.Flag("disable-software-rasterizer", true),
		chromedp.Flag("disable-extensions", true),
		chromedp.Flag("disable-background-networking", true),
		chromedp.Flag("disable-sync", true),
		chromedp.Flag("disable-translate", true),
		chromedp.Flag("hide-scrollbars", true),
		chromedp.Flag("mute-audio", true),
		chromedp.Flag("no-first-run", true),
		chromedp.Flag("no-default-browser-check", true),
		chromedp.Flag("disable-logging", true),
		chromedp.Flag("disable-permissions-api", true),
		// NOTE: no single-process here, unlike the SVG rasterizer. --single-process runs
		// the renderer inside the browser process, disabling the renderer sandbox
		// outright — unacceptable for untrusted remote pages.
	}
	if noSandbox {
		opts = append(opts, chromedp.NoSandbox)
	}
	return opts
}

type chromedpRenderer struct {
	chromedpClient adapter.ChromedpClient
	ssrfValidator  adapter.SSRFValidator
	allocCtx       context.Context
	allocCancel    context.CancelFunc
	viewportWidth  int
	viewportHeight int
	timeoutMs      int
	settleMs       int
}

// NewRenderer creates a render-probe renderer with its own browser allocator.
//
// Reason: a separate allocator (rather than sharing the SVG rasterizer's) keeps the two
// consumers' lifecycles, timeouts, and — importantly — their launch flags independent;
// the probe runs untrusted pages and the rasterizer does not.
func NewRenderer(chromedpClient adapter.ChromedpClient, cfg *RendererConfig) Renderer {
	if cfg == nil {
		cfg = &RendererConfig{}
	}
	if cfg.ViewportWidth <= 0 {
		cfg.ViewportWidth = DefaultViewportWidth
	}
	if cfg.ViewportHeight <= 0 {
		cfg.ViewportHeight = DefaultViewportHeight
	}
	if cfg.TimeoutMs <= 0 {
		cfg.TimeoutMs = DefaultTimeoutMs
	}
	if cfg.SettleMs <= 0 {
		cfg.SettleMs = DefaultSettleMs
	}
	if cfg.AllocatorOptions == nil {
		cfg.AllocatorOptions = AllocatorOptions()
	}

	allocCtx, allocCancel := chromedpClient.NewExecAllocator(context.Background(), cfg.AllocatorOptions)

	return &chromedpRenderer{
		chromedpClient: chromedpClient,
		ssrfValidator:  cfg.SSRFValidator,
		allocCtx:       allocCtx,
		allocCancel:    allocCancel,
		viewportWidth:  cfg.ViewportWidth,
		viewportHeight: cfg.ViewportHeight,
		timeoutMs:      cfg.TimeoutMs,
		settleMs:       cfg.SettleMs,
	}
}

// RenderProbe implements Renderer.
func (r *chromedpRenderer) RenderProbe(ctx context.Context, url string) (*Capture, error) {
	start := time.Now()

	// The browser context hangs off the allocator (which owns the chromium process
	// lifetime), but caller cancellation must still interrupt an in-flight render:
	// AfterFunc bridges job cancellation and worker shutdown into the browser context
	// without making the allocator a child of a per-job context.
	timeoutCtx, cancel := context.WithTimeout(r.allocCtx, time.Duration(r.timeoutMs)*time.Millisecond)
	defer cancel()
	stopBridge := context.AfterFunc(ctx, cancel)
	defer stopBridge()

	browserCtx, browserCancel := r.chromedpClient.NewContext(timeoutCtx)
	defer browserCancel()

	blocked := r.interceptRequests(browserCtx)

	var screenshot []byte
	var userAgent string
	actions := []chromedp.Action{
		r.chromedpClient.EmulateViewport(int64(r.viewportWidth), int64(r.viewportHeight)),
		r.chromedpClient.Navigate(url),
		r.chromedpClient.WaitReady("body"),
		r.chromedpClient.Evaluate("navigator.userAgent", &userAgent),
		r.chromedpClient.Sleep(time.Duration(r.settleMs) * time.Millisecond),
		// Viewport-bounded capture: FullScreenshot would capture the whole scrollable
		// document, which an untrusted page can make arbitrarily tall — unbounded work,
		// and a pHash that no longer corresponds to the recorded viewport.
		r.chromedpClient.CaptureScreenshot(&screenshot),
	}
	if r.ssrfValidator != nil {
		// Install both before anything navigates: the egress guard neutralizes the APIs
		// interception cannot see, then Fetch pauses everything it can.
		actions = append([]chromedp.Action{
			r.chromedpClient.AddScriptToEvaluateOnNewDocument(egressGuardScript),
			r.chromedpClient.FetchEnable(),
		}, actions...)
	}

	if err := r.chromedpClient.Run(browserCtx, actions...); err != nil {
		// Surface caller cancellation as itself so the executor does not record a
		// shutdown as evidence about the artwork.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, fmt.Errorf("render probe canceled for %s: %w", url, ctxErr)
		}
		return nil, fmt.Errorf("render probe failed for %s: %w", url, err)
	}

	img, err := decodeScreenshot(screenshot)
	if err != nil {
		return nil, fmt.Errorf("decoding screenshot for %s: %w", url, err)
	}

	blockedCount := blocked.count()
	logger.InfoCtx(ctx, "Render probe captured frame",
		zap.String("url", url),
		zap.String("engine", userAgent),
		zap.Int("blocked_requests", blockedCount),
		zap.Duration("duration", time.Since(start)),
	)

	return &Capture{
		Image:           img,
		EngineVersion:   userAgent,
		Viewport:        fmt.Sprintf("%dx%d", r.viewportWidth, r.viewportHeight),
		BlockedRequests: blockedCount,
	}, nil
}

// decodeScreenshot enforces encoded and decoded size bounds before returning the frame.
func decodeScreenshot(screenshot []byte) (image.Image, error) {
	if len(screenshot) == 0 {
		return nil, errors.New("empty screenshot")
	}
	if len(screenshot) > maxScreenshotBytes {
		return nil, fmt.Errorf("screenshot of %d bytes exceeds the %d-byte cap", len(screenshot), maxScreenshotBytes)
	}
	// Read the header first so a decompression bomb is rejected on its declared
	// dimensions rather than after allocating the full pixel buffer.
	cfg, err := png.DecodeConfig(bytes.NewReader(screenshot))
	if err != nil {
		return nil, fmt.Errorf("reading screenshot header: %w", err)
	}
	if int64(cfg.Width)*int64(cfg.Height) > maxDecodedPixels {
		return nil, fmt.Errorf("screenshot of %dx%d exceeds the %d-pixel cap", cfg.Width, cfg.Height, maxDecodedPixels)
	}
	return png.Decode(bytes.NewReader(screenshot))
}

// blockedCounter tallies refused requests without a mutex-heavy API surface.
type blockedCounter struct {
	ch chan int
	n  int
}

func (b *blockedCounter) count() int {
	if b == nil {
		return 0
	}
	// Drain everything reported so far. The handler goroutines are done issuing
	// decisions by the time Run returns.
	for {
		select {
		case <-b.ch:
			b.n++
		default:
			return b.n
		}
	}
}

// interceptRequests installs a CDP Fetch handler validating every paused request against
// the SSRF policy. Returns a counter of refusals, or nil when no validator is configured.
//
// Reason: chromium performs its own network I/O, so the Go HTTP client's SSRF
// RoundTripper never sees navigations, redirect hops, or subresource loads. Without this,
// an L0-healthy public page could redirect or script-fetch its way to loopback, private,
// link-local, or cloud-metadata addresses from inside the media worker.
func (r *chromedpRenderer) interceptRequests(browserCtx context.Context) *blockedCounter {
	if r.ssrfValidator == nil {
		return nil
	}

	counter := &blockedCounter{ch: make(chan int, 256)}
	r.chromedpClient.ListenTarget(browserCtx, func(ev any) {
		paused, ok := ev.(*fetch.EventRequestPaused)
		if !ok {
			return
		}
		// CDP decisions must not be issued from the event goroutine.
		go func() {
			if err := r.ssrfValidator.ValidateHTTPURL(browserCtx, paused.Request.URL); err != nil {
				logger.WarnCtx(browserCtx, "Blocked browser request by SSRF policy",
					zap.String("request_url", paused.Request.URL),
					zap.Error(err),
				)
				select {
				case counter.ch <- 1:
				default: // counter saturated; the log above is the durable signal
				}
				_ = r.chromedpClient.FailRequest(browserCtx, paused.RequestID, network.ErrorReasonAccessDenied)
				return
			}
			_ = r.chromedpClient.ContinueRequest(browserCtx, paused.RequestID)
		}()
	})
	return counter
}

// Close implements Renderer.
func (r *chromedpRenderer) Close() error {
	r.allocCancel()
	return nil
}

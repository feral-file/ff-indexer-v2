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
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chromedp/cdproto/cdp"
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
	// DefaultTimeoutMs bounds the whole probe (navigate + settle + screenshot). Sized to
	// leave ~30s of navigation headroom on top of the settle window: shrinking that
	// headroom converts slow-loading (IPFS-hosted) works into stalled verdicts, the
	// opposite of what a generous settle is for.
	DefaultTimeoutMs = 45000
	// DefaultSettleMs is how long the page runs after load before the capture. Generous
	// on purpose: the blank debounce only protects against *transient* blanks, so a work
	// that deterministically needs longer than the settle window (heavy WebGL compiling
	// shaders under software GL, large assets fetched at runtime) would be gated on its
	// second probe no matter how many chances it gets. Err-healthy says spend render
	// capacity, not artworks.
	DefaultSettleMs = 15000
	// MinRenderHeadroomMs is the minimum budget the probe timeout must reserve beyond
	// the settle window for navigation, readiness, and the screenshot. A timeout that
	// leaves less is not a tuning choice but a structural guarantee of failure: every
	// probe burns its whole budget in the settle sleep and records stalled, and after
	// the debounce that gates healthy media at corpus scale. Startup validation
	// enforces timeout >= settle + this, on EFFECTIVE values (defaults resolved) — the
	// startup self-check cannot catch it because self-check renders use a short fixture
	// settle. 5s is the floor for the fixed costs, not a recommendation; the default
	// pairing reserves 30s because slow IPFS navigations are normal.
	MinRenderHeadroomMs = 5000

	// MinViewportDim/MaxViewportDim bound each configured viewport edge, and
	// MaxViewportPixels bounds their product. Startup validation enforces these so a
	// configured viewport can never collide with the capture caps below: a viewport
	// whose captures are rejected post-hoc records stalled on every probe and would
	// render-gate perfectly healthy media after the debounce — a config typo silently
	// hiding artworks. MaxViewportPixels (~3.1M pixels, e.g. 2048x1536) keeps even a
	// worst-case incompressible RGBA PNG (4 bytes/pixel plus filter/deflate overhead)
	// inside maxScreenshotBytes with margin, and sits far under maxDecodedPixels.
	MinViewportDim    = 64
	MaxViewportDim    = 4096
	MaxViewportPixels = 3 << 20

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

// ErrBrowserUnavailable reports that chromium never came up for this render: the process
// could not be forked, failed during startup, or never exposed its DevTools socket.
//
// Reason: a browser that never started says nothing about the artwork, and the
// distinction is load-bearing for the caller — the 2026-08-17 incident recorded ~2,100
// launch failures (host fork exhaustion) as "stalled" verdicts, driving healthy URLs to
// the gate threshold. Callers must treat this as an infrastructure failure (retry the
// job) and never as render evidence.
var ErrBrowserUnavailable = errors.New("browser unavailable")

// isBrowserLaunchFailure reports whether a chromedp Run error means the browser never
// started, as opposed to a page-level load failure.
//
// Reason: chromedp does not type launch failures, so detection is structural where
// possible (fork/exec surfaces as *os.PathError from os.StartProcess) and textual where
// not ("chrome failed to start" and "websocket url timeout" are chromedp's literal
// allocator messages). Trade-offs: string matching is fragile against chromedp upgrades,
// but a missed match only degrades to the old behavior (a stalled verdict debounced over
// FailureGateThreshold probes), never to a false gate on its own.
func isBrowserLaunchFailure(err error) bool {
	var pathErr *os.PathError
	if errors.As(err, &pathErr) && pathErr.Op == "fork/exec" {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "chrome failed to start") ||
		strings.Contains(msg, "websocket url timeout")
}

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
(function applyEgressGuard() {
  const NAMES = ["WebSocket", "RTCPeerConnection", "webkitRTCPeerConnection", "RTCDataChannel", "EventSource"];

  // ` + "`self`" + ` is the window in a document and the WorkerGlobalScope in a worker, so
  // one guard body serves both.
  for (const name of NAMES) {
    try {
      Object.defineProperty(self, name, {
        configurable: false,
        get() { throw new Error(name + " is disabled in the render probe"); },
      });
    } catch (e) {
      try { self[name] = undefined; } catch (e2) { /* already locked down */ }
    }
  }

  // Workers get a fresh global scope that the block above never reaches — measured: a
  // worker could open a WebSocket with the page guarded, and a worker-created worker
  // could do the same with only the page's Worker wrapped. So the guard re-installs
  // itself in every worker it creates, recursively: the named function expression
  // serializes its own source as each worker's prologue, which on execution wraps that
  // scope's Worker in turn. The original script is then pulled in with importScripts,
  // an ordinary fetch that request interception still vets. Workers that cannot be
  // wrapped are refused rather than run unguarded.
  const RealWorker = self.Worker;
  if (typeof RealWorker === "function") {
    const prologue = "(" + applyEgressGuard.toString() + ")();";
    const GuardedWorker = function (script, options) {
      let wrapped;
      try {
        // Resolve against the creating scope so the loader below gets an absolute URL;
        // fall back to the raw value if it will not parse.
        let target = String(script);
        try { target = new URL(target, self.location.href).href; } catch (e) { /* keep raw */ }

        // Module workers have no importScripts — forwarding {type:"module"} while
        // injecting one would break every legitimate module worker, and art that paints
        // from a worker would then be captured blank and gated. Load modules with a
        // dynamic import instead, which module scope does provide.
        const isModule = !!(options && options.type === "module");
        const load = isModule
          ? "import(" + JSON.stringify(target) + ");"
          : "importScripts(" + JSON.stringify(target) + ");";
        wrapped = URL.createObjectURL(new Blob([prologue + load], { type: "application/javascript" }));
      } catch (e) {
        throw new Error("Worker creation blocked in the render probe");
      }
      return new RealWorker(wrapped, options);
    };
    GuardedWorker.prototype = RealWorker.prototype;
    try {
      Object.defineProperty(self, "Worker", { configurable: false, value: GuardedWorker });
    } catch (e) { /* already locked down */ }
  }
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
	// MainStatus is the HTTP status of the main document's final response (after
	// redirects), or 0 when it could not be observed. Chromium renders any HTTP error
	// body it receives — a 4xx/5xx page paints and screenshots like a normal page — so
	// classification alone cannot tell an artwork frame from a served error page. The
	// caller uses a non-2xx status as "this frame is not evidence about the artwork":
	// measured in production, ipfs.io's HTTP 410 bot-block page classified 1,692
	// distinct artworks as blank.
	MainStatus int
}

// Renderer drives headless chromium to capture what a URL paints.
//
//go:generate mockgen -source=renderer.go -destination=../../mocks/render_probe_renderer.go -package=mocks -mock_names=Renderer=MockRenderProbeRenderer
type Renderer interface {
	// RenderProbe loads the URL, waits for it to settle, and screenshots the viewport.
	// Errors (navigation failure, timeout, cancellation) are the caller's "stalled"
	// signal — except errors matching ErrBrowserUnavailable (the browser never
	// started), which are infrastructure failures the caller must retry rather than
	// record as render evidence. A successful capture carries the main document's HTTP
	// status; callers must not classify the frame when it is not 2xx (chromium paints
	// error bodies like normal pages).
	//
	// settleMs overrides the configured settle window for this render; <= 0 keeps the
	// configured default. The caller owns class knowledge (static images paint on decode
	// and need only a fraction of the window generative works do), the renderer owns the
	// safe default — so an override can only be deliberate, never accidental.
	//
	// SECURITY: chromium fetches URLs itself, outside the Go HTTP client and its SSRF
	// RoundTripper. Every browser-initiated request — the navigation, each redirect hop,
	// and every subresource — is paused via the CDP Fetch domain and validated against
	// the SSRF policy before it is allowed to proceed; refused requests are failed with
	// AccessDenied. Callers should still validate the URL up front so an obviously
	// blocked target never launches a browser context at all.
	RenderProbe(ctx context.Context, url string, settleMs int) (*Capture, error)

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
		// Chromium cold start on a loaded shared host (CI runners, prod boxes running
		// postgres alongside) can exceed chromedp's default 20s websocket-URL wait,
		// failing the first render with "websocket url timeout reached" while every
		// later one succeeds. The boot happens once per allocator; a generous wait
		// costs nothing steady-state and removes the cold-start flake.
		chromedp.WSURLReadTimeout(60 * time.Second),
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
		// WebGL must keep a software backend. DisableGPU above turns off GPU
		// compositing (correct for headless capture), but combining it with
		// disable-software-rasterizer — as an earlier revision did — removes the
		// SwiftShader fallback too, leaving WebGL context creation with no backend at
		// all. A WebGL-only artwork then paints nothing, classifies blank, and is gated
		// after the debounce: healthy generative art hidden by the probe's own launch
		// flags. Pinning ANGLE to SwiftShader makes the software path explicit instead
		// of an implicit fallback; the chromium smoke suite proves a WebGL canvas
		// classifies rendered_ok under exactly these flags.
		chromedp.Flag("use-angle", "swiftshader"),
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
func (r *chromedpRenderer) RenderProbe(ctx context.Context, url string, settleMs int) (*Capture, error) {
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

	// Install the status tracker before interception so both listeners observe the
	// navigation from its first event; chromedp broadcasts every target event to every
	// registered listener.
	tracker := r.trackMainDocument(browserCtx)
	blocked := r.interceptRequests(browserCtx)

	settle := r.settleMs
	if settleMs > 0 {
		settle = settleMs
	}

	var screenshot []byte
	var userAgent string
	actions := []chromedp.Action{
		// Network events (main-document status) are needed on every render, with or
		// without SSRF interception.
		r.chromedpClient.NetworkEnable(),
		r.chromedpClient.EmulateViewport(int64(r.viewportWidth), int64(r.viewportHeight)),
		r.chromedpClient.Navigate(url),
		// ":root" matches the document element in both HTML and SVG documents. "body"
		// never matches a directly-navigated SVG — measured, that burns the whole probe
		// timeout and records a stalled verdict, gating healthy SVG artworks.
		r.chromedpClient.WaitReady(":root"),
		r.chromedpClient.Evaluate("navigator.userAgent", &userAgent),
		r.chromedpClient.Sleep(time.Duration(settle) * time.Millisecond),
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
		// A browser that never started is an infrastructure failure, not render
		// evidence; the sentinel lets the executor retry the job instead of recording a
		// stalled verdict.
		if isBrowserLaunchFailure(err) {
			return nil, fmt.Errorf("render probe browser launch failed for %s: %w: %w",
				url, ErrBrowserUnavailable, err)
		}
		return nil, fmt.Errorf("render probe failed for %s: %w", url, err)
	}

	img, err := decodeScreenshot(screenshot)
	if err != nil {
		return nil, fmt.Errorf("decoding screenshot for %s: %w", url, err)
	}

	blockedCount := blocked.count()
	mainStatus := tracker.mainStatus()
	logger.InfoCtx(ctx, "Render probe captured frame",
		zap.String("url", url),
		zap.String("engine", userAgent),
		zap.Int("blocked_requests", blockedCount),
		zap.Int("main_status", mainStatus),
		zap.Duration("duration", time.Since(start)),
	)

	return &Capture{
		Image:           img,
		EngineVersion:   userAgent,
		Viewport:        fmt.Sprintf("%dx%d", r.viewportWidth, r.viewportHeight),
		BlockedRequests: blockedCount,
		MainStatus:      mainStatus,
	}, nil
}

// mainDocTracker observes Network events to record the HTTP status of the main
// document's final response.
type mainDocTracker struct {
	mu sync.Mutex
	// mainFrame is the frame of the first document request — the navigation itself.
	// Iframe documents arrive on other frame IDs and must not overwrite the main
	// status (a 404 ad-frame inside a healthy artwork page is not the artwork's status).
	mainFrame cdp.FrameID
	status    int
}

// handle processes one target event. Redirect hops re-fire EventRequestWillBeSent on the
// same frame and report their 3xx inside the redirectResponse field, so the single
// EventResponseReceived seen for the main frame is the final response of the chain.
func (t *mainDocTracker) handle(ev any) {
	switch e := ev.(type) {
	case *network.EventRequestWillBeSent:
		if e.Type != network.ResourceTypeDocument {
			return
		}
		t.mu.Lock()
		if t.mainFrame == "" {
			t.mainFrame = e.FrameID
		}
		t.mu.Unlock()
	case *network.EventResponseReceived:
		if e.Type != network.ResourceTypeDocument || e.Response == nil {
			return
		}
		t.mu.Lock()
		if e.FrameID == t.mainFrame {
			t.status = int(e.Response.Status)
		}
		t.mu.Unlock()
	}
}

// mainStatus returns the recorded status, or 0 when no main-document response was seen
// (non-HTTP navigations, or event delivery raced probe teardown — callers treat 0 as
// unknown, never as failure).
func (t *mainDocTracker) mainStatus() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.status
}

// trackMainDocument installs the Network-event listener recording the main document's
// response status for this render.
//
// Reason: chromium reports HTTP error responses as successful navigations — the error
// body paints and screenshots normally — so without the status the classifier grades
// whatever page the server chose to send. Production measurement: ipfs.io's 410
// bot-block page (one line of text on white, variance ~0.00096) classified 1,692
// distinct healthy artworks as blank. The status lets the executor refuse to treat such
// frames as evidence.
func (r *chromedpRenderer) trackMainDocument(browserCtx context.Context) *mainDocTracker {
	tracker := &mainDocTracker{}
	r.chromedpClient.ListenTarget(browserCtx, tracker.handle)
	return tracker
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

const (
	// requestDecisionWorkers bounds concurrent SSRF validations per render. Validation
	// can hit DNS, so an unbounded per-event goroutine (the previous shape) let one
	// hostile page fan out thousands of resolver-backed goroutines during the settle
	// window. Eight workers keep a legitimate asset-heavy page fast — its validations
	// are host-cached after the first few — while capping what a hostile one can spend.
	requestDecisionWorkers = 8
	// requestDecisionQueue bounds pending paused requests awaiting a decision. Sized
	// well above what legitimate artwork pages issue in a settle window.
	requestDecisionQueue = 256
)

// pausedRequest is one browser request awaiting an SSRF decision.
type pausedRequest struct {
	id  fetch.RequestID
	url string
}

// interceptRequests installs a CDP Fetch handler validating every paused request against
// the SSRF policy. Returns a counter of refusals, or nil when no validator is configured.
//
// Reason: chromium performs its own network I/O, so the Go HTTP client's SSRF
// RoundTripper never sees navigations, redirect hops, or subresource loads. Without this,
// an L0-healthy public page could redirect or script-fetch its way to loopback, private,
// link-local, or cloud-metadata addresses from inside the media worker.
//
// Decisions run on a fixed worker pool over a bounded queue, never per-event goroutines:
// validation can perform DNS lookups, and the page controls how many requests fire, so
// per-event spawning handed a hostile page an amplifier for media-worker memory, CPU,
// and resolver load. When the queue is full the event is DROPPED — the request stays
// paused and the page waits on it. Deliberate, not an oversight: promptly failing excess
// requests would cost a CDP command per event, a number the attacker chooses, while
// dropping bounds our spend at zero and turns the flood into the attacker's own stall
// (an unfinished page classifies stalled; legitimate pages never reach the cap).
// Undecided requests die with the browser context at probe end.
func (r *chromedpRenderer) interceptRequests(browserCtx context.Context) *blockedCounter {
	if r.ssrfValidator == nil {
		return nil
	}

	counter := &blockedCounter{ch: make(chan int, 256)}
	queue := make(chan pausedRequest, requestDecisionQueue)
	var dropped atomic.Int64

	for range requestDecisionWorkers {
		go func() {
			for {
				select {
				case <-browserCtx.Done():
					return
				case req := <-queue:
					r.decidePausedRequest(browserCtx, req, counter)
				}
			}
		}()
	}

	r.chromedpClient.ListenTarget(browserCtx, func(ev any) {
		paused, ok := ev.(*fetch.EventRequestPaused)
		if !ok {
			return
		}
		// The event goroutine must neither issue CDP decisions nor block.
		select {
		case queue <- pausedRequest{id: paused.RequestID, url: paused.Request.URL}:
		default:
			if dropped.Add(1) == 1 {
				logger.WarnCtx(browserCtx, "Request decision queue saturated; excess browser requests left paused",
					zap.String("first_dropped_url", paused.Request.URL),
					zap.Int("queue_capacity", requestDecisionQueue),
				)
			}
		}
	})
	return counter
}

// decidePausedRequest validates one paused request and issues the CDP decision.
func (r *chromedpRenderer) decidePausedRequest(browserCtx context.Context, req pausedRequest, counter *blockedCounter) {
	if err := r.ssrfValidator.ValidateHTTPURL(browserCtx, req.url); err != nil {
		logger.WarnCtx(browserCtx, "Blocked browser request by SSRF policy",
			zap.String("request_url", req.url),
			zap.Error(err),
		)
		select {
		case counter.ch <- 1:
		default: // counter saturated; the log above is the durable signal
		}
		_ = r.chromedpClient.FailRequest(browserCtx, req.id, network.ErrorReasonAccessDenied)
		return
	}
	_ = r.chromedpClient.ContinueRequest(browserCtx, req.id)
}

// Close implements Renderer.
func (r *chromedpRenderer) Close() error {
	r.allocCancel()
	return nil
}

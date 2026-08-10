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
	"fmt"
	"image"
	"image/png"
	"time"

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
)

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
}

// Renderer drives headless chromium to capture what a URL paints.
//
//go:generate mockgen -source=renderer.go -destination=../../mocks/render_probe_renderer.go -package=mocks -mock_names=Renderer=MockRenderProbeRenderer
type Renderer interface {
	// RenderProbe loads the URL, waits for it to settle, and screenshots a frame.
	// Errors (navigation failure, timeout) are the caller's "stalled" signal.
	//
	// SECURITY: chromium fetches the URL itself, bypassing the Go HTTP client and its
	// SSRF RoundTripper entirely — the caller MUST validate the URL against the SSRF
	// policy before invoking this. In-page redirects and subresource fetches are not
	// re-validated (documented residual risk; mitigants: the URL already passed the
	// L0 probe's SSRF-enforced fetch, and background networking is disabled).
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
	// AllocatorOptions are the chromium launch flags; nil callers must pass
	// rasterizer.DefaultAllocatorOptions() (kept as a parameter so this package does not
	// import the rasterizer, and tests can pass none).
	AllocatorOptions []chromedp.ExecAllocatorOption
}

type chromedpRenderer struct {
	chromedpClient adapter.ChromedpClient
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
// consumers' lifecycles and timeouts independent — a hung render probe cannot starve SVG
// rasterization jobs and vice versa. Both launch with identical flags via
// rasterizer.DefaultAllocatorOptions().
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

	allocCtx, allocCancel := chromedpClient.NewExecAllocator(context.Background(), cfg.AllocatorOptions)

	return &chromedpRenderer{
		chromedpClient: chromedpClient,
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

	// Per-probe timeout derives from the allocator context (browser lifecycle), but is
	// also bounded by the caller's ctx via the select below chromedp does internally.
	timeoutCtx, cancel := context.WithTimeout(r.allocCtx, time.Duration(r.timeoutMs)*time.Millisecond)
	defer cancel()
	browserCtx, browserCancel := r.chromedpClient.NewContext(timeoutCtx)
	defer browserCancel()

	var screenshot []byte
	var userAgent string
	err := r.chromedpClient.Run(browserCtx,
		r.chromedpClient.EmulateViewport(int64(r.viewportWidth), int64(r.viewportHeight)),
		r.chromedpClient.Navigate(url),
		r.chromedpClient.WaitReady("body"),
		r.chromedpClient.Evaluate("navigator.userAgent", &userAgent),
		r.chromedpClient.Sleep(time.Duration(r.settleMs)*time.Millisecond),
		r.chromedpClient.FullScreenshot(&screenshot, 100),
	)
	if err != nil {
		return nil, fmt.Errorf("render probe failed for %s: %w", url, err)
	}

	img, err := png.Decode(bytes.NewReader(screenshot))
	if err != nil {
		return nil, fmt.Errorf("decoding screenshot for %s: %w", url, err)
	}

	logger.InfoCtx(ctx, "Render probe captured frame",
		zap.String("url", url),
		zap.String("engine", userAgent),
		zap.Duration("duration", time.Since(start)),
	)

	return &Capture{
		Image:         img,
		EngineVersion: userAgent,
		Viewport:      fmt.Sprintf("%dx%d", r.viewportWidth, r.viewportHeight),
	}, nil
}

// Close implements Renderer.
func (r *chromedpRenderer) Close() error {
	r.allocCancel()
	return nil
}

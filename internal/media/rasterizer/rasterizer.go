//go:build cgo

package rasterizer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"

	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
)

// ErrUnsupportedSVGFilter is returned when an SVG contains filter primitives that would crash
// resvg (SIGABRT via CGO) and no browser renderer is available to handle it safely. Callers
// should treat this as a skippable condition — the job is marked failed rather than retried
// indefinitely, which would otherwise loop via SweepOrphanedJobs after each process crash.
var ErrUnsupportedSVGFilter = errors.New("SVG contains filter primitives unsupported by resvg (no browser renderer available)")

// Rasterizer handles SVG to PNG conversion using resvg or browser fallback
//
//go:generate mockgen -source=rasterizer.go -destination=../../mocks/media_rasterizer.go -package=mocks -mock_names=Rasterizer=MockRasterizer
type Rasterizer interface {
	// Rasterize converts an SVG to a PNG image
	Rasterize(ctx context.Context, svgData []byte) ([]byte, error)
}

type rasterizer struct {
	resvgClient       adapter.ResvgClient
	browserRasterizer BrowserRasterizer
	imageEncoder      adapter.ImageEncoder
	width             int
	enableBrowser     bool
}

// Config holds configuration for the rasterizer
type Config struct {
	// Width is the target width for rasterization (0 = use SVG natural size)
	// Height is automatically calculated to maintain aspect ratio using ScaleBestFit
	Width int

	// EnableBrowserFallback enables headless browser rendering for complex SVGs
	EnableBrowserFallback bool
}

// NewRasterizer creates a new SVG rasterizer instance
func NewRasterizer(
	resvgClient adapter.ResvgClient,
	imageEncoder adapter.ImageEncoder,
	browserRasterizer BrowserRasterizer,
	cfg *Config,
) Rasterizer {
	if cfg == nil {
		cfg = &Config{}
	}

	return &rasterizer{
		resvgClient:       resvgClient,
		browserRasterizer: browserRasterizer,
		imageEncoder:      imageEncoder,
		width:             cfg.Width,
		enableBrowser:     cfg.EnableBrowserFallback,
	}
}

// Rasterize converts SVG data to PNG format.
//
// Routing priority:
//  1. willCrashResvg — SVG features that cause SIGABRT in resvg (e.g. feDisplacementMap).
//     These MUST use the browser renderer. If the browser is unavailable, return
//     ErrUnsupportedSVGFilter so the caller can skip the job safely instead of crashing the process.
//  2. requiresBrowserRendering — SVG features resvg cannot render faithfully but that will not
//     crash (e.g. <foreignObject>, SMIL animations). Prefer browser; fall back to resvg if needed.
//  3. Standard SVGs — use the fast resvg path.
func (r *rasterizer) Rasterize(ctx context.Context, svgData []byte) ([]byte, error) {
	logger.InfoCtx(ctx, "Starting SVG rasterization to PNG",
		zap.Int("svgSize", len(svgData)),
		zap.Int("targetWidth", r.width),
	)

	// Check for SVG features that will crash resvg at the OS level (SIGABRT via CGO).
	// These cannot be handled by resvg under any circumstance.
	if crash, reasons := willCrashResvg(svgData); crash {
		logger.WarnCtx(ctx, "SVG contains filter primitives that crash resvg; must use browser renderer",
			zap.Strings("reasons", reasons),
		)
		if r.enableBrowser && r.browserRasterizer != nil {
			return r.rasterizeWithBrowser(ctx, svgData)
		}
		// Browser not available: return a skippable error so the job is marked failed
		// rather than crashing the process and looping forever via SweepOrphanedJobs.
		logger.WarnCtx(ctx, "Browser renderer unavailable for crash-inducing SVG; skipping",
			zap.Strings("reasons", reasons),
		)
		return nil, ErrUnsupportedSVGFilter
	}

	// Check for SVG features that resvg cannot render faithfully (degraded but not crashing).
	if needsBrowser, reasons := r.requiresBrowserRendering(svgData); needsBrowser {
		logger.InfoCtx(ctx, "SVG prefers browser rendering",
			zap.Strings("reasons", reasons),
		)
		if r.enableBrowser && r.browserRasterizer != nil {
			return r.rasterizeWithBrowser(ctx, svgData)
		}
		logger.WarnCtx(ctx, "Browser rendering preferred but unavailable, falling back to resvg",
			zap.Strings("reasons", reasons),
		)
	}

	// Standard SVG: use the fast resvg path.
	logger.InfoCtx(ctx, "Using resvg for standard SVG")
	return r.rasterizeWithResvg(ctx, svgData)
}

// willCrashResvg reports whether the SVG contains filter primitives known to trigger a fatal
// Rust assertion in resvg (SIGABRT). These SVGs must never reach resvg.Render because an OS
// abort cannot be caught by Go's recover() — it terminates the entire process.
//
// Reason: resvg's feDisplacementMap implementation asserts that the source, map, and destination
// buffers share the same width. Certain SVGs violate this assumption and trigger the assertion:
//
//	assertion failed: src.width == map.width && src.width == dest.width
//	note: run with RUST_BACKTRACE=1 ...
//	fatal runtime error: failed to initiate panic, error 5
//	SIGABRT: abort
//
// Trade-offs: String matching is conservative (false-positive safe — over-routing to the browser
// is better than crashing). Constraints: If the browser renderer is also unavailable, callers
// must return domain.ErrUnsupportedMediaFile to skip the job without crashing.
func willCrashResvg(svgData []byte) (bool, []string) {
	content := string(svgData)
	var reasons []string

	// feDisplacementMap triggers a Rust assertion failure in resvg when buffer dimensions
	// don't match after compositing — a known resvg bug on certain SVG inputs.
	if strings.Contains(content, "feDisplacementMap") {
		reasons = append(reasons, "feDisplacementMap")
	}

	// feComposite can produce mismatched buffer dimensions when composed with certain filter chains.
	if strings.Contains(content, "feComposite") {
		reasons = append(reasons, "feComposite")
	}

	// feTurbulence → feDisplacementMap is the most common crash-inducing pattern on production.
	// Detect feTurbulence independently so that combination is caught even if feDisplacementMap
	// check above ever becomes more targeted.
	if strings.Contains(content, "feTurbulence") {
		reasons = append(reasons, "feTurbulence")
	}

	return len(reasons) > 0, reasons
}

// requiresBrowserRendering analyzes SVG content to determine if browser rendering is preferred.
// These are features resvg cannot render faithfully but where falling back to resvg still
// produces a non-crashing (if degraded) result. For features that cause SIGABRT, see willCrashResvg.
func (r *rasterizer) requiresBrowserRendering(svgData []byte) (bool, []string) {
	content := string(svgData)
	var reasons []string

	// Check for <foreignObject> - contains HTML that resvg can't render
	if strings.Contains(content, "<foreignObject") || strings.Contains(content, "<foreignobject>") {
		reasons = append(reasons, "foreignObject")
	}

	// Check for <script> tags - JavaScript that resvg can't execute
	if strings.Contains(content, "<script") {
		reasons = append(reasons, "script")
	}

	// Check for CSS animations - @keyframes, animation properties
	if strings.Contains(content, "@keyframes") ||
		strings.Contains(content, "animation:") ||
		strings.Contains(content, "transition:") {
		reasons = append(reasons, "css-animation")
	}

	// Check for SMIL animations - <animate> tags
	if strings.Contains(content, "<animate") ||
		strings.Contains(content, "<animateTransform") ||
		strings.Contains(content, "<animateMotion") ||
		strings.Contains(content, "<set") {
		reasons = append(reasons, "smil-animation")
	}

	return len(reasons) > 0, reasons
}

// rasterizeWithResvg renders SVG using resvg (static renderer)
func (r *rasterizer) rasterizeWithResvg(ctx context.Context, svgData []byte) ([]byte, error) {
	logger.InfoCtx(ctx, "Rasterizing with resvg")

	img, err := r.resvgClient.Render(svgData, r.width)
	if err != nil {
		logger.ErrorCtx(ctx, err)
		return nil, fmt.Errorf("failed to render SVG with resvg: %w", err)
	}

	bounds := img.Bounds()
	renderedWidth := bounds.Dx()
	renderedHeight := bounds.Dy()

	logger.InfoCtx(ctx, "SVG rendered with resvg",
		zap.Int("renderedWidth", renderedWidth),
		zap.Int("renderedHeight", renderedHeight),
	)

	var buf bytes.Buffer
	if err := r.imageEncoder.EncodePNG(&buf, img); err != nil {
		return nil, fmt.Errorf("failed to encode PNG: %w", err)
	}

	result := buf.Bytes()
	logger.InfoCtx(ctx, "resvg rasterization completed",
		zap.Int("outputSize", len(result)),
	)

	return result, nil
}

// rasterizeWithBrowser renders SVG using headless browser (for complex/animated SVGs)
func (r *rasterizer) rasterizeWithBrowser(ctx context.Context, svgData []byte) ([]byte, error) {
	logger.InfoCtx(ctx, "Rasterizing with browser")

	pngData, err := r.browserRasterizer.RasterizeSVG(ctx, svgData, r.width)
	if err != nil {
		return nil, fmt.Errorf("failed to render SVG with browser: %w", err)
	}

	logger.InfoCtx(ctx, "Browser rasterization completed",
		zap.Int("outputSize", len(pngData)),
	)

	return pngData, nil
}

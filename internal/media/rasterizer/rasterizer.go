//go:build cgo

package rasterizer

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
)

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

// Rasterize converts SVG data to PNG format
func (r *rasterizer) Rasterize(ctx context.Context, svgData []byte) ([]byte, error) {
	logger.InfoCtx(ctx, "Starting SVG rasterization to PNG",
		zap.Int("svgSize", len(svgData)),
		zap.Int("targetWidth", r.width),
	)

	// Detect if SVG requires browser rendering
	needsBrowser, reasons := r.requiresBrowserRendering(svgData)

	if needsBrowser {
		logger.InfoCtx(ctx, "SVG requires browser rendering",
			zap.Strings("reasons", reasons),
		)

		if !r.enableBrowser {
			logger.WarnCtx(ctx, "Browser rendering required but disabled, falling back to resvg")
			return r.rasterizeWithResvg(ctx, svgData)
		}

		if r.browserRasterizer == nil {
			logger.WarnCtx(ctx, "Browser rasterizer not configured, falling back to resvg")
			return r.rasterizeWithResvg(ctx, svgData)
		}

		return r.rasterizeWithBrowser(ctx, svgData)
	}

	// Use fast path (resvg) for standard SVGs
	logger.InfoCtx(ctx, "Using resvg for standard SVG")
	return r.rasterizeWithResvg(ctx, svgData)
}

// requiresBrowserRendering analyzes SVG content to determine if browser rendering is needed
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

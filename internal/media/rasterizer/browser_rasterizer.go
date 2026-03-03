package rasterizer

import (
	"context"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/chromedp/chromedp"
	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
)

const (
	DEFAULT_WIDTH   = 2048
	DEFAULT_TIMEOUT = 10000 // 10 seconds
)

// BrowserRasterizer defines an interface for browser-based SVG rasterization
//
//go:generate mockgen -source=browser_rasterizer.go -destination=../../mocks/browser_rasterizer.go -package=mocks -mock_names=BrowserRasterizer=MockBrowserRasterizer
type BrowserRasterizer interface {
	// RasterizeSVG renders SVG data to PNG using a headless browser
	// Parameters:
	//   - ctx: context for cancellation and timeout
	//   - svgData: raw SVG content as bytes
	//   - width: target width in pixels (0 = use natural SVG size)
	// Returns:
	//   - PNG image data as bytes
	//   - error if rendering fails
	RasterizeSVG(ctx context.Context, svgData []byte, width int) ([]byte, error)

	// Close releases browser resources (connections, processes)
	// Should be called during application shutdown
	Close() error
}

// BrowserRasterizerConfig holds configuration for the browser rasterizer
type BrowserRasterizerConfig struct {
	// Width is the target width for rasterization (0 = use SVG natural size)
	Width int

	// TimeoutMs is the maximum time to wait for page operations (default: 10000ms)
	TimeoutMs int
}

// chromedpBrowserRasterizer implements browser-based SVG rendering using chromedp
type chromedpBrowserRasterizer struct {
	chromedpClient adapter.ChromedpClient
	xml            adapter.XML
	fs             adapter.FileSystem
	allocCtx       context.Context
	allocCancel    context.CancelFunc
	width          int
	timeoutMs      int
}

// NewBrowserRasterizer creates a new chromedp-based SVG rasterizer
func NewBrowserRasterizer(chromedpClient adapter.ChromedpClient, xml adapter.XML, fs adapter.FileSystem, cfg *BrowserRasterizerConfig) BrowserRasterizer {
	if cfg == nil {
		cfg = &BrowserRasterizerConfig{
			Width:     DEFAULT_WIDTH,
			TimeoutMs: DEFAULT_TIMEOUT,
		}
	}

	// Set defaults
	if cfg.TimeoutMs <= 0 {
		cfg.TimeoutMs = DEFAULT_TIMEOUT
	}
	if cfg.Width <= 0 {
		cfg.Width = DEFAULT_WIDTH
	}

	// Create chrome options
	opts := []chromedp.ExecAllocatorOption{
		chromedp.NoFirstRun,
		chromedp.NoDefaultBrowserCheck,
		chromedp.DisableGPU,
		chromedp.NoSandbox,
		chromedp.Headless,
		chromedp.Flag("disable-web-security", true),          // Disables same-origin policy and CSP
		chromedp.Flag("disable-dev-shm-usage", true),         // Critical: Avoids /dev/shm issues in Docker
		chromedp.Flag("disable-software-rasterizer", true),   // Prevents software rendering hangs
		chromedp.Flag("disable-extensions", true),            // Disable extensions
		chromedp.Flag("disable-background-networking", true), // Disable background networking
		chromedp.Flag("disable-sync", true),                  // Disable sync
		chromedp.Flag("disable-translate", true),             // Disable translate
		chromedp.Flag("hide-scrollbars", true),               // Hide scrollbars
		chromedp.Flag("mute-audio", true),                    // Mute audio
		chromedp.Flag("no-first-run", true),                  // No first run
		chromedp.Flag("no-default-browser-check", true),      // No default browser check
		chromedp.Flag("disable-logging", true),               // Disable logging
		chromedp.Flag("disable-permissions-api", true),       // Disable permissions API
		chromedp.Flag("single-process", true),                // Run in single process (important for containers)
	}

	// Create allocator context (manages Chrome instance lifecycle)
	allocCtx, allocCancel := chromedpClient.NewExecAllocator(context.Background(), opts)

	return &chromedpBrowserRasterizer{
		chromedpClient: chromedpClient,
		xml:            xml,
		fs:             fs,
		allocCtx:       allocCtx,
		allocCancel:    allocCancel,
		width:          cfg.Width,
		timeoutMs:      cfg.TimeoutMs,
	}
}

// RasterizeSVG renders SVG data to PNG using headless Chromium via chromedp
func (r *chromedpBrowserRasterizer) RasterizeSVG(ctx context.Context, svgData []byte, width int) ([]byte, error) {
	startTime := time.Now()
	logger.InfoCtx(ctx, "Starting browser-based SVG rasterization with chromedp",
		zap.Int("svgSize", len(svgData)),
		zap.Int("targetWidth", width),
	)

	// Use configured width if not specified
	if width <= 0 {
		width = r.width
	}
	if width <= 0 {
		width = DEFAULT_WIDTH // fallback default
	}

	// Try data URI first (for small SVGs)
	svgDataURI := r.createDataURI(svgData)
	var cleanup func()

	// If data URI is too large, use temp file
	if svgDataURI == "" {
		logger.InfoCtx(ctx, "SVG too large for data URI, using temp file")
		var err error
		svgDataURI, cleanup, err = r.createTempFileURL(ctx, svgData)
		if err != nil {
			return nil, fmt.Errorf("failed to create temp file for large SVG: %w", err)
		}
		defer cleanup()
	}

	// Create context with timeout from the allocator context
	timeoutCtx, cancel := context.WithTimeout(r.allocCtx, time.Duration(r.timeoutMs)*time.Millisecond)
	defer cancel()

	// Create browser context with timeout
	browserCtx, browserCancel := r.chromedpClient.NewContext(timeoutCtx)
	defer browserCancel()

	// Render SVG to PNG
	buf, err := r.renderSVG(browserCtx, svgDataURI, width, svgData)
	if err != nil {
		return nil, fmt.Errorf("failed to render SVG with chromedp: %w", err)
	}

	elapsed := time.Since(startTime)
	logger.InfoCtx(ctx, "Browser-based SVG rasterization completed",
		zap.Int("outputSize", len(buf)),
		zap.Duration("duration", elapsed),
	)

	return buf, nil
}

// createDataURI creates a data URI from SVG bytes for small SVGs
// or returns empty string if SVG is too large (caller should use temp file)
func (r *chromedpBrowserRasterizer) createDataURI(svgData []byte) string {
	// Data URIs have length limits (~2MB in Chrome)
	// Use data URI for small SVGs (< 1MB when base64 encoded)
	const maxDataURISize = 1024 * 1024 // 1MB

	htmlWrapper := r.createHTMLWrapper(svgData)
	encoded := base64.StdEncoding.EncodeToString([]byte(htmlWrapper))

	// Check if the resulting data URI would be too large
	if len(encoded) > maxDataURISize {
		return "" // Signal that temp file should be used
	}

	return fmt.Sprintf("data:text/html;base64,%s", encoded)
}

// createHTMLWrapper wraps SVG content in a minimal HTML document
func (r *chromedpBrowserRasterizer) createHTMLWrapper(svgData []byte) string {
	return fmt.Sprintf(`<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
body { margin: 0; padding: 0; overflow: hidden; }
svg { display: block; }
</style>
</head>
<body>
%s
</body>
</html>`, string(svgData))
}

// createTempFileURL creates a temporary HTML file with SVG content and returns file:// URL
func (r *chromedpBrowserRasterizer) createTempFileURL(ctx context.Context, svgData []byte) (string, func(), error) {
	htmlContent := r.createHTMLWrapper(svgData)

	// Create temporary file
	tmpFile, err := r.fs.CreateTemp("", "svg-*.html")
	if err != nil {
		return "", nil, fmt.Errorf("failed to create temp file: %w", err)
	}

	tmpPath := tmpFile.Name()

	// Cleanup function to remove temp file
	cleanup := func() {
		if err := r.fs.Remove(tmpPath); err != nil {
			logger.WarnCtx(ctx, "Failed to remove temp file", zap.String("path", tmpPath), zap.Error(err))
		}
	}

	// Write HTML content
	_, err = r.fs.WriteFile(tmpFile, []byte(htmlContent))
	if err != nil {
		cleanup()
		return "", nil, fmt.Errorf("failed to write temp file: %w", err)
	}

	// Close file
	err = r.fs.Close(tmpFile)
	if err != nil {
		cleanup()
		return "", nil, fmt.Errorf("failed to close temp file: %w", err)
	}

	// Return file:// URL
	fileURL := "file://" + tmpPath
	return fileURL, cleanup, nil
}

// renderSVG performs the actual SVG rendering using chromedp
func (r *chromedpBrowserRasterizer) renderSVG(ctx context.Context, svgDataURI string, targetWidth int, svgData []byte) ([]byte, error) {
	var buf []byte

	// Try to get SVG dimensions from the XML first (fast path)
	dims, err := r.parseSVGDimensions(svgData)
	if err != nil {
		// If parsing fails, get dimensions from browser (fallback)
		logger.InfoCtx(ctx, "Failed to parse SVG dimensions, using browser detection", zap.Error(err))
		dims, err = r.getSVGDimensionsFromBrowser(ctx, svgDataURI)
		if err != nil {
			return nil, fmt.Errorf("failed to get SVG dimensions: %w", err)
		}
	}

	// Calculate viewport dimensions
	viewportWidth, viewportHeight := r.calculateViewport(dims, targetWidth)

	logger.InfoCtx(ctx, "SVG loaded in browser",
		zap.Int("naturalWidth", dims.Width),
		zap.Int("naturalHeight", dims.Height),
		zap.Int("viewportWidth", viewportWidth),
		zap.Int("viewportHeight", viewportHeight),
	)

	// Capture screenshot with calculated viewport
	err = r.chromedpClient.Run(ctx,
		r.chromedpClient.Navigate(svgDataURI),
		r.chromedpClient.WaitReady("svg, body", chromedp.ByQuery), // Wait for either SVG or body
		r.chromedpClient.EmulateViewport(int64(viewportWidth), int64(viewportHeight)),
		// Fix SVG dimensions to match viewport (needed when SVG has percentage-based viewBox)
		r.chromedpClient.Evaluate(fmt.Sprintf(`
			(() => {
				const svg = document.querySelector('svg');
				if (svg) {
					svg.setAttribute('width', %d);
					svg.setAttribute('height', %d);
					svg.style.width = '%dpx';
					svg.style.height = '%dpx';
				}
			})()
		`, viewportWidth, viewportHeight, viewportWidth, viewportHeight), nil),
		// Trigger resize event for JavaScript-driven SVGs that initialize on resize
		r.chromedpClient.Evaluate(`window.dispatchEvent(new Event('resize'))`, nil),
		// Simulate click on SVG for interactive artwork that requires user interaction
		r.chromedpClient.Evaluate(`
			(() => {
				const svg = document.querySelector('svg');
				if (svg) {
					// Dispatch click event on SVG element
					svg.dispatchEvent(new MouseEvent('click', {
						bubbles: true,
						cancelable: true,
						view: window
					}));
				}
			})()
		`, nil),
		r.chromedpClient.Sleep(5*time.Second), // Wait longer for WebGL/animation frames to render
		r.chromedpClient.FullScreenshot(&buf, 100),
	)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

// svgDimensions holds the dimensions of an SVG
type svgDimensions struct {
	Width  int
	Height int
}

// parseSVGDimensions extracts dimensions from SVG XML without loading in browser
func (r *chromedpBrowserRasterizer) parseSVGDimensions(svgData []byte) (*svgDimensions, error) {
	// Parse SVG XML to get root element attributes
	var svg struct {
		Width   string `xml:"width,attr"`
		Height  string `xml:"height,attr"`
		ViewBox string `xml:"viewBox,attr"`
	}

	err := r.xml.Unmarshal(svgData, &svg)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SVG XML: %w", err)
	}

	// Try to parse width and height attributes (assuming px only)
	if svg.Width != "" && svg.Height != "" {
		width, err := r.parseDimension(svg.Width)
		if err == nil {
			height, err := r.parseDimension(svg.Height)
			if err == nil && width > 0 && height > 0 {
				return &svgDimensions{
					Width:  width,
					Height: height,
				}, nil
			}
		}
	}

	// Fallback to viewBox if width/height not available or has units
	if svg.ViewBox != "" {
		dims, err := r.parseViewBox(svg.ViewBox)
		if err == nil {
			return dims, nil
		}
	}

	return nil, fmt.Errorf("could not parse dimensions from SVG (width=%q, height=%q, viewBox=%q)",
		svg.Width, svg.Height, svg.ViewBox)
}

// parseDimension parses a dimension string, accepting only pixel values (e.g., "100", "100px")
// Returns error for other units (em, rem, %, etc.) to trigger browser fallback
func (r *chromedpBrowserRasterizer) parseDimension(value string) (int, error) {
	// Remove "px" suffix if present
	value = strings.TrimSpace(value)
	value = strings.TrimSuffix(value, "px")

	// Check for other units - if present, return error to fallback to browser
	if strings.ContainsAny(value, "emrt%") {
		return 0, fmt.Errorf("non-px unit detected: %s", value)
	}

	// Parse as float and convert to int
	f, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid dimension value: %w", err)
	}

	return int(f), nil
}

// parseViewBox parses viewBox attribute (format: "minX minY width height")
func (r *chromedpBrowserRasterizer) parseViewBox(viewBox string) (*svgDimensions, error) {
	parts := strings.Fields(viewBox)
	if len(parts) != 4 {
		return nil, fmt.Errorf("invalid viewBox format: %s", viewBox)
	}

	// viewBox format: "minX minY width height"
	// We only need width and height (indices 2 and 3)
	width, err := strconv.ParseFloat(parts[2], 64)
	if err != nil {
		return nil, fmt.Errorf("invalid viewBox width: %w", err)
	}

	height, err := strconv.ParseFloat(parts[3], 64)
	if err != nil {
		return nil, fmt.Errorf("invalid viewBox height: %w", err)
	}

	if width <= 0 || height <= 0 {
		return nil, fmt.Errorf("invalid viewBox dimensions: width=%v, height=%v", width, height)
	}

	return &svgDimensions{
		Width:  int(width),
		Height: int(height),
	}, nil
}

// getSVGDimensionsFromBrowser retrieves the natural dimensions of an SVG by loading it in the browser
func (r *chromedpBrowserRasterizer) getSVGDimensionsFromBrowser(ctx context.Context, svgDataURI string) (*svgDimensions, error) {
	var dims map[string]interface{}

	err := r.chromedpClient.Run(ctx,
		r.chromedpClient.Navigate(svgDataURI),
		r.chromedpClient.WaitReady("svg, body", chromedp.ByQuery), // Wait for either SVG or body
		r.chromedpClient.Evaluate(`(() => {
			const svg = document.querySelector('svg');
			if (!svg) return { width: 2048, height: 1536 };
			
			// Check if there's a canvas inside (common for generative art)
			const canvas = svg.querySelector('canvas');
			if (canvas && canvas.width > 0 && canvas.height > 0) {
				// Use canvas dimensions for WebGL/canvas-based artwork
				return {
					width: canvas.width,
					height: canvas.height
				};
			}
			
			const rect = svg.getBoundingClientRect();
			return {
				width: rect.width || svg.width.baseVal.value || 2048,
				height: rect.height || svg.height.baseVal.value || 1536
			};
		})()`, &dims),
	)
	if err != nil {
		return nil, err
	}

	width := int(dims["width"].(float64))
	height := int(dims["height"].(float64))

	return &svgDimensions{
		Width:  width,
		Height: height,
	}, nil
}

// calculateViewport calculates the viewport dimensions based on SVG dimensions and target width
func (r *chromedpBrowserRasterizer) calculateViewport(dims *svgDimensions, targetWidth int) (int, int) {
	var viewportWidth, viewportHeight int

	// Only scale if explicitly requested and target is different from natural dimensions
	if targetWidth > 0 && targetWidth != dims.Width {
		scale := float64(targetWidth) / float64(dims.Width)
		viewportWidth = targetWidth
		viewportHeight = int(float64(dims.Height) * scale)
	} else {
		// Use natural dimensions
		viewportWidth = dims.Width
		viewportHeight = dims.Height
	}

	// Ensure minimum dimensions
	if viewportWidth < 1 {
		viewportWidth = 1
	}
	if viewportHeight < 1 {
		viewportHeight = 1
	}

	return viewportWidth, viewportHeight
}

// Close releases browser resources
func (r *chromedpBrowserRasterizer) Close() error {
	if r.allocCancel != nil {
		r.allocCancel()
	}
	return nil
}

//go:build cgo

package transformer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/alitto/pond/v2"
	"github.com/cshum/vipsgen/vips"
	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/config"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
)

var (
	// ErrTooManyPixels is returned when the decoded image exceeds the maximum pixel count
	ErrTooManyPixels = errors.New("image exceeds maximum pixel count")

	// ErrCannotMeetTargetSize is returned when the image cannot be resized/compressed to meet the target size
	ErrCannotMeetTargetSize = errors.New("cannot meet target size even after transformation")

	// ErrAnimationCannotFit is returned when an animated image cannot fit within the target size
	ErrAnimationCannotFit = errors.New("animated image cannot fit within size limits")

	// ErrInputTooLarge is returned when the input file exceeds the maximum input size
	ErrInputTooLarge = errors.New("input file exceeds maximum input size")

	// ErrUnsupportedFormat is returned when the image format is not supported
	ErrUnsupportedFormat = errors.New("unsupported image format")
)

// MAX_WEBP_DIMENSION is the maximum dimension of a WebP image in pixels
const MAX_WEBP_DIMENSION = 16383

// TransformInput contains the input parameters for image transformation
type TransformInput struct {
	// SourceURL is the URL of the image to transform
	SourceURL string

	// IsAnimated indicates if this is an animated image (GIF or animated WebP)
	IsAnimated bool
}

// TransformResult contains the result of image transformation
type TransformResult struct {
	// Data is the transformed image data
	Data []byte

	// ContentType is the MIME type of the transformed image
	ContentType string

	// Filename is the suggested filename for the transformed image
	Filename string

	// OriginalSize is the size of the original image in bytes
	OriginalSize int64

	// TransformedSize is the size of the transformed image in bytes
	TransformedSize int64

	// Width is the width of the transformed image
	Width int

	// Height is the height of the transformed image
	Height int

	// Applied transformations
	Resized    bool
	Compressed bool
	Quality    int
}

// Reader returns an io.Reader for the transformed image data
func (r *TransformResult) Reader() io.Reader {
	return bytes.NewReader(r.Data)
}

// Config is an alias to config.TransformConfig for convenience
type Config = config.TransformConfig

// Transformer defines the interface for image transformation
//
//go:generate mockgen -source=transformer.go -destination=../../mocks/transformer.go -package=mocks -mock_names=Transformer=MockTransformer
//go:generate sh -c "printf '//go:build cgo\n\n' | cat - ../../mocks/transformer.go > ../../mocks/transformer.go.tmp && mv ../../mocks/transformer.go.tmp ../../mocks/transformer.go"
type Transformer interface {
	// Transform transforms an image to meet size and dimension constraints
	// This method is safe for concurrent use and enforces the configured worker concurrency
	Transform(ctx context.Context, input *TransformInput) (*TransformResult, error)

	// Close gracefully shuts down the transformer and its worker pool
	Close() error
}

// transformer is the implementation of the Transformer interface using vipsgen/vips
type transformer struct {
	config     Config
	pool       pond.ResultPool[*TransformResult]
	httpClient adapter.HTTPClient
	vipsClient adapter.VipsClient
}

// NewTransformer creates a new image transformer with bounded worker pool
func NewTransformer(
	cfg Config,
	httpClient adapter.HTTPClient,
	io adapter.IO,
	vipsClient adapter.VipsClient,
) Transformer {
	// Initialize vips once
	vipsClient.Startup(&vips.Config{
		ConcurrencyLevel: cfg.WorkerConcurrency,
		MaxCacheMem:      100 * 1024 * 1024, // 100MB cache
		MaxCacheSize:     500,
	})

	// Create simple bounded worker pool that returns results
	pool := pond.NewResultPool[*TransformResult](cfg.WorkerConcurrency)

	return &transformer{
		config:     cfg,
		pool:       pool,
		httpClient: httpClient,
		vipsClient: vipsClient,
	}
}

// Transform transforms an image to meet size and dimension constraints
func (t *transformer) Transform(ctx context.Context, input *TransformInput) (*TransformResult, error) {
	if input == nil {
		return nil, fmt.Errorf("input cannot be nil")
	}

	if input.SourceURL == "" {
		return nil, fmt.Errorf("source URL cannot be empty")
	}

	logger.InfoCtx(ctx, "Starting image transformation",
		zap.String("sourceURL", input.SourceURL),
		zap.Bool("isAnimated", input.IsAnimated),
	)

	// Add timeout to context
	ctx, cancel := context.WithTimeout(ctx, t.config.TransformTimeout)
	defer cancel()

	// Submit to pool and wait for result
	task := t.pool.SubmitErr(func() (*TransformResult, error) {
		return t.transformInternal(ctx, input)
	})

	result, err := task.Wait()
	if err != nil {
		return nil, err
	}

	logger.InfoCtx(ctx, "Image transformation completed",
		zap.String("sourceURL", input.SourceURL),
		zap.Int("originalSize", int(result.OriginalSize)),
		zap.Int("transformedSize", int(result.TransformedSize)),
		zap.Int("width", result.Width),
		zap.Int("height", result.Height),
		zap.Bool("resized", result.Resized),
		zap.Bool("compressed", result.Compressed),
		zap.Int("quality", result.Quality),
	)

	return result, nil
}

// Close gracefully shuts down the transformer
func (t *transformer) Close() error {
	_ = t.pool.Stop()
	t.vipsClient.Shutdown()
	return nil
}

// transformInternal performs the actual transformation
func (t *transformer) transformInternal(ctx context.Context, input *TransformInput) (*TransformResult, error) {
	// Step 1: Stream download the image
	resp, err := t.httpClient.GetResponseNoRetry(ctx, input.SourceURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	// Get original size if available
	originalSize := resp.ContentLength

	// Check input size limit
	if originalSize > 0 && originalSize > t.config.MaxInputBytes {
		_ = resp.Body.Close()
		logger.WarnCtx(ctx, "Input image exceeds maximum input size",
			zap.Int64("size", originalSize),
			zap.Int64("maxSize", t.config.MaxInputBytes),
		)
		return nil, ErrInputTooLarge
	}

	// Step 2: Create vips Source from the streaming reader
	source := t.vipsClient.NewSource(resp.Body)
	defer source.Close()

	// Step 3: Load image from source (streaming, not buffering)
	loadOpts := vips.DefaultLoadOptions()
	if input.IsAnimated {
		// For animated images, load all frames
		loadOpts.N = -1
	}
	loadOpts.FailOnError = true
	// Use random access to allow resize operations without "out of order read" errors
	loadOpts.Access = vips.AccessRandom

	img, err := t.vipsClient.NewImageFromSource(source, loadOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to load image from source: %w", err)
	}
	defer img.Close()

	// Step 4: Transform based on whether it's animated
	if input.IsAnimated {
		return t.transformAnimated(ctx, input, img, originalSize)
	}

	return t.transformStill(ctx, input, img, originalSize)
}

// transformStill transforms a still image (JPEG, PNG, WebP, etc.)
func (t *transformer) transformStill(ctx context.Context, input *TransformInput, img adapter.VipsImage, originalSize int64) (*TransformResult, error) {
	logger.InfoCtx(ctx, "Transforming still image",
		zap.String("sourceURL", input.SourceURL),
		zap.Int64("originalSize", originalSize),
	)

	// Determine output format based on alpha channel
	outputFormat := "jpeg"
	if img.HasAlpha() {
		outputFormat = "webp"
	}

	return t.transformImage(ctx, input, img, originalSize, outputFormat, false, nil, 0)
}

// transformSingleFrame transforms an animated image by extracting the first frame
// and encoding it as a high-quality single-frame WebP
func (t *transformer) transformSingleFrame(ctx context.Context, input *TransformInput, img adapter.VipsImage, originalSize int64) (*TransformResult, error) {
	logger.InfoCtx(ctx, "Transforming to single frame image",
		zap.String("sourceURL", input.SourceURL),
		zap.Int64("originalSize", originalSize),
		zap.Int("pages", img.Pages()),
		zap.Int("pageHeight", img.PageHeight()),
	)

	// Extract the first frame by cropping to the page height
	pageHeight := img.PageHeight()
	width := img.Width()

	// Extract just the first frame (top-left corner at 0,0)
	if err := img.ExtractArea(0, 0, width, pageHeight); err != nil {
		return nil, fmt.Errorf("failed to extract first frame: %w", err)
	}

	logger.InfoCtx(ctx, "Extracted first frame",
		zap.Int("width", width),
		zap.Int("height", pageHeight),
	)

	// Transform as a still image using WebP for best quality
	return t.transformImage(ctx, input, img, originalSize, "webp", false, nil, 0)
}

// transformAnimated transforms an animated image (GIF or animated WebP)
func (t *transformer) transformAnimated(ctx context.Context, input *TransformInput, img adapter.VipsImage, originalSize int64) (*TransformResult, error) {
	logger.InfoCtx(ctx, "Transforming animated image",
		zap.String("sourceURL", input.SourceURL),
		zap.Int64("originalSize", originalSize),
		zap.Int("pages", img.Pages()),
		zap.Int("pageHeight", img.PageHeight()),
		zap.Int("width", img.Width()),
		zap.Int("height", img.Height()),
	)

	// Extract animation metadata before any transformations
	// libvips loses this metadata during resize operations, so we need to preserve and restore it
	delay, err := img.GetArrayInt("delay")
	if err != nil {
		logger.WarnCtx(ctx, "Failed to get delay metadata, using default",
			zap.Error(err),
		)
		// Use default delay of 100ms per frame if not available
		delay = make([]int, img.Pages())
		for i := range delay {
			delay[i] = 100
		}
	}

	loop, err := img.GetInt("loop")
	if err != nil {
		logger.WarnCtx(ctx, "Failed to get loop metadata, using default (infinite loop)",
			zap.Error(err),
		)
		loop = 0 // Default to infinite loop
	}

	// Fast-fail check: WebP has a maximum dimension of 16,383 pixels
	// For animated images stored as vertical strips, this limits the total strip height
	// Check if the animation can physically fit within WebP's limits even at minimum dimension
	minDimension := t.config.MinAnimatedImageDimension
	pages := img.Pages()
	minPossibleStripHeight := pages * minDimension

	if minPossibleStripHeight > MAX_WEBP_DIMENSION {
		logger.WarnCtx(ctx, "Animation has too many frames to fit in WebP format even at minimum dimension, falling back to single frame",
			zap.Int("pages", pages),
			zap.Int("minDimension", minDimension),
			zap.Int("minPossibleStripHeight", minPossibleStripHeight),
			zap.Int("maxWebPDimension", MAX_WEBP_DIMENSION),
		)
		// Fall back to encoding as single frame WebP to keep original quality
		return t.transformSingleFrame(ctx, input, img, originalSize)
	}

	// Always use WebP for animated images - better compression than GIF
	// WebP supports both animation and alpha channel with superior compression
	return t.transformImage(ctx, input, img, originalSize, "webp", true, delay, loop)
}

// transformImage is the common transformation logic for both still and animated images
func (t *transformer) transformImage(
	ctx context.Context,
	input *TransformInput,
	img adapter.VipsImage,
	originalSize int64,
	outputFormat string,
	isAnimated bool,
	animationDelay []int,
	animationLoop int,
) (*TransformResult, error) {
	// Get image properties
	width := img.Width()
	height := img.Height()

	// For animated images, use frame dimensions, not the total strip dimensions
	frameWidth := width
	frameHeight := height
	if isAnimated {
		frameHeight = img.PageHeight()
	}

	// Check total pixel count (safety limit to avoid large computational effort)
	// For still images: width * height
	// For animated images: width * height * frames
	totalPixels := t.calculateTotalPixels(img, width, height, isAnimated)
	if totalPixels > t.config.MaxDecodedPixels {
		imageType := "image"
		if isAnimated {
			imageType = "animated image (total across all frames)"
		}
		logger.WarnCtx(ctx, fmt.Sprintf("%s exceeds maximum pixel count", imageType),
			zap.Int64("totalPixels", totalPixels),
			zap.Int64("maxPixels", t.config.MaxDecodedPixels),
		)
		return nil, ErrTooManyPixels
	}

	// Try with original dimensions and high quality first
	quality := t.config.InitialQuality
	processed, err := t.encodeImage(img, outputFormat, quality, isAnimated)
	if err != nil {
		// For animated images, initial encode can fail due to WebP dimension limits
		// If so, skip directly to resize loop to try to fix it
		if !isWebPSizeError(err, isAnimated) {
			return nil, fmt.Errorf("failed to encode image: %w", err)
		}
		// WebP size error - will be handled in resize loop below
		logger.DebugCtx(ctx, "Initial WebP encode failed due to size limits, will resize",
			zap.Error(err),
			zap.Int("width", frameWidth),
			zap.Int("height", frameHeight),
		)
	} else {
		// Check if already meets targets
		if t.meetsTarget(img, processed, frameWidth, frameHeight, isAnimated) {
			return t.buildResult(input.SourceURL, processed, outputFormat, originalSize,
				frameWidth, frameHeight, false, false, quality), nil
		}
	}

	// RESIZE FIRST (preferred approach)
	currentWidth := frameWidth
	currentHeight := frameHeight
	resized := false
	longEdge := max(currentWidth, currentHeight)

	// Determine dimension limits based on image type
	minDimension := t.config.MinImageDimension
	maxDimension := t.config.MaxImageDimension
	if isAnimated {
		minDimension = t.config.MinAnimatedImageDimension
		maxDimension = t.config.MaxAnimatedImageDimension
	}

	// Keep resizing until we meet target or hit minimum dimension
	for longEdge > minDimension {
		// Calculate new scale
		var scale float64
		if longEdge > maxDimension {
			// Resize to max dimension
			scale = float64(maxDimension) / float64(longEdge)
		} else {
			// Reduce by step percentage, but don't go below minDimension
			scale = float64(100-t.config.ResizeStepPercentage) / 100.0
			newLongEdge := float64(longEdge) * scale
			if int(newLongEdge) < minDimension {
				// Would drop below minimum, scale to exactly minDimension instead
				scale = float64(minDimension) / float64(longEdge)
			}
		}

		// For animated images, adjust scale to ensure exact frame alignment
		// This prevents drift by ensuring totalHeight = pages × frameHeight exactly
		if isAnimated {
			pages := img.Pages()
			originalScale := scale
			// Calculate target frame height
			targetFrameHeight := int(float64(currentHeight) * scale)
			// Calculate exact total height needed
			exactTotalHeight := pages * targetFrameHeight
			// Recalculate scale to hit exact total height
			scale = float64(exactTotalHeight) / float64(img.Height())

			logger.DebugCtx(ctx, "Adjusted scale for frame alignment",
				zap.Float64("originalScale", originalScale),
				zap.Float64("adjustedScale", scale),
				zap.Int("targetFrameHeight", targetFrameHeight),
				zap.Int("exactTotalHeight", exactTotalHeight),
			)
		}

		// Resize image (works for both still and animated)
		err = img.Resize(scale, vips.DefaultResizeOptions())
		if err != nil {
			return nil, fmt.Errorf("failed to resize image: %w", err)
		}

		// Get new dimensions
		currentWidth = img.Width()
		totalHeight := img.Height()

		// For animated images, fix metadata and ensure proper frame alignment
		if isAnimated {
			pages := img.Pages()
			newPageHeight := totalHeight / pages

			// CRITICAL: Ensure total height is exactly pages × pageHeight
			// The resize scale adjustment above should prevent misalignment, but check anyway
			expectedHeight := pages * newPageHeight
			if totalHeight != expectedHeight {
				// This should rarely happen now, but handle it as a safety check
				// Log as warning since it indicates the scale adjustment didn't work perfectly
				logger.WarnCtx(ctx, "Height mismatch after resize despite scale adjustment - cropping",
					zap.Int("totalHeight", totalHeight),
					zap.Int("expectedHeight", expectedHeight),
					zap.Int("difference", totalHeight-expectedHeight),
				)
				if err := img.ExtractArea(0, 0, currentWidth, expectedHeight); err != nil {
					return nil, fmt.Errorf("failed to crop image to exact height: %w", err)
				}
			}

			// Update page height metadata (libvips doesn't do this correctly)
			if err := img.SetPageHeight(newPageHeight); err != nil {
				return nil, fmt.Errorf("failed to set page height after resize: %w", err)
			}

			// Restore animation metadata (libvips loses delay/loop during resize)
			if err := t.restoreAnimationMetadata(img, animationDelay, animationLoop); err != nil {
				return nil, fmt.Errorf("failed to restore animation metadata: %w", err)
			}

			currentHeight = newPageHeight
		} else {
			currentHeight = totalHeight
		}

		resized = true
		longEdge = max(currentWidth, currentHeight)

		// Encode with high quality
		processed, err = t.encodeImage(img, outputFormat, quality, isAnimated)
		if err != nil {
			// For animated images, encoding can fail due to WebP dimension limits (16,383px max)
			if isWebPSizeError(err, isAnimated) {
				if longEdge > minDimension {
					// Continue resizing to get below WebP dimension limit
					logger.DebugCtx(ctx, "WebP encoding failed due to size limits, continuing to resize",
						zap.Error(err),
						zap.Int("longEdge", longEdge),
						zap.Int("currentWidth", currentWidth),
						zap.Int("currentHeight", currentHeight),
						zap.Int("height", img.Height()),
						zap.Int("width", img.Width()),
						zap.Int("minDimension", minDimension),
					)
					continue
				}
				// Hit minimum dimension with WebP size error
				// This means the animation has too many frames to fit within WebP's dimension limits
				// Compression won't help since this is a dimension issue, not a quality issue
				totalStripHeight := img.Height()
				logger.WarnCtx(ctx, "Animated image exceeds WebP dimension limits even at minimum size",
					zap.Error(err),
					zap.Int("pages", img.Pages()),
					zap.Int("frameHeight", currentHeight),
					zap.Int("totalStripHeight", totalStripHeight),
					zap.Int("maxWebPDimension", MAX_WEBP_DIMENSION),
				)
				return nil, fmt.Errorf("animated image cannot be encoded: %d frames × %dpx = %d pixels total height exceeds WebP limit of %d pixels",
					img.Pages(), currentHeight, totalStripHeight, MAX_WEBP_DIMENSION)
			}

			// For other errors (not WebP size issues), return immediately
			return nil, fmt.Errorf("failed to encode resized image: %w", err)
		}

		logFields := []zap.Field{
			zap.Int("width", currentWidth),
			zap.Int("height", currentHeight),
			zap.Int("size", len(processed)),
		}
		if isAnimated {
			pages := img.Pages()
			pageHeight := img.PageHeight()
			logFields = append(logFields,
				zap.Int("pages", pages),
				zap.Int("pageHeight", pageHeight),
				zap.Int64("totalPixels", t.calculateTotalPixels(img, currentWidth, currentHeight, isAnimated)),
			)
		}
		logger.DebugCtx(ctx, "Resized image", logFields...)

		// Check if we've met the targets
		if t.meetsTarget(img, processed, currentWidth, currentHeight, isAnimated) {
			return t.buildResult(input.SourceURL, processed, outputFormat, originalSize,
				currentWidth, currentHeight, resized, false, t.config.InitialQuality), nil
		}

		// Ensure we don't go below minimum
		minDim := min(currentWidth, currentHeight)
		if minDim <= minDimension {
			break
		}
	}

	// COMPRESSION AS ESCAPE HATCH (only if resize didn't work)
	// Start from InitialQuality - QualityStep to avoid re-encoding at the quality we just tried
	compressed := false
	for quality = t.config.InitialQuality - t.config.QualityStep; quality >= t.config.MinQuality; quality -= t.config.QualityStep {
		processed, err = t.encodeImage(img, outputFormat, quality, isAnimated)
		if err != nil {
			return nil, fmt.Errorf("failed to compress image: %w", err)
		}

		compressed = true

		logger.DebugCtx(ctx, "Compressed image",
			zap.Int("quality", quality),
			zap.Int("size", len(processed)),
		)

		// Check if we've met the targets
		if t.meetsTarget(img, processed, currentWidth, currentHeight, isAnimated) {
			return t.buildResult(input.SourceURL, processed, outputFormat, originalSize,
				currentWidth, currentHeight, resized, compressed, quality), nil
		}
	}

	// Still too large - give up
	finalErr := ErrCannotMeetTargetSize
	if isAnimated {
		finalErr = ErrAnimationCannotFit
	}

	// Build final log fields
	finalLogFields := []zap.Field{
		zap.Int64("targetSize", t.config.TargetImageSize),
		zap.Int("finalSize", len(processed)),
		zap.Bool("isAnimated", isAnimated),
	}
	if isAnimated {
		pages := img.Pages()
		finalLogFields = append(finalLogFields,
			zap.Int64("targetPixels", t.config.TargetImagePixels),
			zap.Int64("finalTotalPixels", t.calculateTotalPixels(img, currentWidth, currentHeight, isAnimated)),
			zap.Int("pages", pages),
		)
	}

	logger.WarnCtx(ctx, "Cannot meet target constraints even after transformation", finalLogFields...)
	return nil, finalErr
}

// meetsTarget checks if the image meets the target constraints
// For still images: only checks file size
// For animated images: checks both file size AND total pixels
func (t *transformer) meetsTarget(img adapter.VipsImage, processedData []byte, width, height int, isAnimated bool) bool {
	fileSize := int64(len(processedData))

	// Check file size for all images
	if fileSize > t.config.TargetImageSize {
		return false
	}

	// For animated images, also check total pixels
	if isAnimated {
		totalPixels := t.calculateTotalPixels(img, width, height, isAnimated)
		return totalPixels <= t.config.TargetImagePixels
	}

	return true
}

// calculateTotalPixels calculates total pixels (for logging purposes)
// For still images: width * height
// For animated images: width * height * frames
func (t *transformer) calculateTotalPixels(img adapter.VipsImage, width, height int, isAnimated bool) int64 {
	if isAnimated {
		pages := img.Pages()
		return int64(width) * int64(height) * int64(pages)
	}
	return int64(width) * int64(height)
}

// restoreAnimationMetadata restores animation metadata (delay and loop) on an image
// This is needed because libvips loses this metadata during operations like resize
func (t *transformer) restoreAnimationMetadata(img adapter.VipsImage, delay []int, loop int) error {
	if len(delay) > 0 {
		if err := img.SetArrayInt("delay", delay); err != nil {
			return fmt.Errorf("failed to set delay metadata: %w", err)
		}
	}
	img.SetInt("loop", loop)
	return nil
}

// encodeImage encodes an image to the specified format with given quality
func (t *transformer) encodeImage(img adapter.VipsImage, format string, quality int, isAnimated bool) ([]byte, error) {
	switch format {
	case "jpeg":
		opts := vips.DefaultJpegsaveBufferOptions()
		opts.Q = quality
		opts.OptimizeCoding = true
		opts.Keep = vips.KeepNone // Strip metadata
		return img.JpegsaveBuffer(opts)

	case "webp":
		opts := vips.DefaultWebpsaveBufferOptions()
		opts.Q = quality
		opts.Effort = 4 // Balance between speed and compression

		// For animated images, preserve metadata and set page height
		if isAnimated {
			// CRITICAL: Keep metadata (delay, loop) - stripping it breaks animation
			opts.Keep = vips.KeepAll
			// CRITICAL: Set PageHeight to trigger animation encoding in libvips
			opts.PageHeight = img.PageHeight()
			// Allow mixed lossy/lossless encoding for better compression
			opts.Mixed = true
		} else {
			// Strip metadata for still images
			opts.Keep = vips.KeepNone
		}

		return img.WebpsaveBuffer(opts)

	default:
		return nil, fmt.Errorf("unsupported output format: %s", format)
	}
}

// buildResult constructs a TransformResult
func (t *transformer) buildResult(
	sourceURL string,
	data []byte,
	format string,
	originalSize int64,
	width, height int,
	resized, compressed bool,
	quality int,
) *TransformResult {
	// Extract filename from URL
	filename := filepath.Base(sourceURL)

	// Determine content type and extension
	var contentType, ext string
	switch format {
	case "jpeg":
		contentType = "image/jpeg"
		ext = ".jpg"
	case "webp":
		contentType = "image/webp"
		ext = ".webp"
	default:
		contentType = "image/jpeg"
		ext = ".jpg"
	}

	// Ensure filename has correct extension
	currentExt := filepath.Ext(filename)
	if currentExt != ext {
		filename = strings.TrimSuffix(filename, currentExt) + ext
	}

	return &TransformResult{
		Data:            data,
		ContentType:     contentType,
		Filename:        filename,
		OriginalSize:    originalSize,
		TransformedSize: int64(len(data)),
		Width:           width,
		Height:          height,
		Resized:         resized,
		Compressed:      compressed,
		Quality:         quality,
	}
}

// isWebPSizeError checks if an error is related to WebP dimension/size limits
// WebP has a maximum dimension of 16,383 pixels. For animated images stored as
// vertical strips, this limit is easily exceeded with many frames.
func isWebPSizeError(err error, isAnimated bool) bool {
	if err == nil || !isAnimated {
		return false
	}

	return strings.Contains(err.Error(), "image too large")
}

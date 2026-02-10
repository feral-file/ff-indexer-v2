//go:build cgo

package processor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/gabriel-vasile/mimetype"
	"go.uber.org/zap"

	"gorm.io/datatypes"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/downloader"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	mediaprovider "github.com/feral-file/ff-indexer-v2/internal/media/provider"
	"github.com/feral-file/ff-indexer-v2/internal/media/rasterizer"
	"github.com/feral-file/ff-indexer-v2/internal/media/transformer"
	"github.com/feral-file/ff-indexer-v2/internal/providers/cloudflare"
	"github.com/feral-file/ff-indexer-v2/internal/store"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
	"github.com/feral-file/ff-indexer-v2/internal/uri"
)

// Processor defines the interface for processing media files
//
//go:generate mockgen -source=processor.go -destination=../../mocks/media_processor.go -package=mocks -mock_names=Processor=MockMediaProcessor
type Processor interface {
	// Process handles the entire pipeline for importing a media asset from an external source:
	//   1. Probes the media at the given source URL to determine its type and relevant metadata (like size, mime type, etc.).
	//   2. Depending on the media type (image, SVG, video), applies any necessary processing or transformations,
	//      such as resizing, format conversion, or rasterization.
	//   3. Uploads the processed or original media to the configured external storage provider, capturing provider-specific details (such as asset IDs and variant URLs).
	//   4. Persists a reference and all related metadata about the uploaded asset to the database for later retrieval and use.
	//   5. Handles errors at each step.
	//
	// Parameters:
	//   - ctx: context for cancellation and timeout
	//   - sourceURL: the original URL of the media
	// Returns:
	//   - error if any step fails
	Process(ctx context.Context, sourceURL string) error

	// Provider returns the provider name
	Provider() string
}

// processor is the implementation of Processor
type processor struct {
	// Dependencies
	httpClient  adapter.HTTPClient
	uriResolver uri.Resolver
	provider    mediaprovider.Provider
	store       store.Store
	rasterizer  rasterizer.Rasterizer
	downloader  downloader.Downloader
	transformer transformer.Transformer

	// Adapters
	io         adapter.IO
	json       adapter.JSON
	filesystem adapter.FileSystem

	// Configuration
	maxImageSize int64
	maxVideoSize int64
}

// NewProcessor creates a new Processor instance
func NewProcessor(
	httpClient adapter.HTTPClient,
	uriResolver uri.Resolver,
	provider mediaprovider.Provider,
	st store.Store,
	svgRasterizer rasterizer.Rasterizer,
	filesystem adapter.FileSystem,
	io adapter.IO,
	json adapter.JSON,
	dl downloader.Downloader,
	trans transformer.Transformer,
	maxImageSize int64,
	maxVideoSize int64) Processor {
	return &processor{
		httpClient:   httpClient,
		uriResolver:  uriResolver,
		provider:     provider,
		store:        st,
		rasterizer:   svgRasterizer,
		filesystem:   filesystem,
		io:           io,
		json:         json,
		downloader:   dl,
		transformer:  trans,
		maxImageSize: maxImageSize,
		maxVideoSize: maxVideoSize,
	}
}

// probeResult contains the result of probing a media file
type probeResult struct {
	contentType     string
	contentLength   int64
	isVideo         bool
	isImage         bool
	isSVG           bool
	isAnimatedImage bool
}

// probe detects the content type and size of a media file
func (p *processor) probe(ctx context.Context, sourceURL string) (*probeResult, error) {
	var contentType string
	var contentLength int64

	// Try HEAD request first to get content-type and size
	resp, err := p.httpClient.Head(ctx, sourceURL)
	if err == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
		// HEAD request succeeded
		defer func() {
			if resp.Body != nil {
				if err := resp.Body.Close(); err != nil {
					logger.WarnCtx(ctx, "failed to close response body", zap.Error(err), zap.String("url", sourceURL))
				}
			}
		}()

		contentType = resp.Header.Get("Content-Type")
		contentLength = resp.ContentLength

		logger.InfoCtx(ctx, "Media content-type detected via HEAD",
			zap.String("sourceURL", sourceURL),
			zap.String("contentType", contentType),
			zap.Int64("contentLength", contentLength),
		)
	} else {
		// HEAD failed, fallback to partial GET to detect content-type
		if resp != nil && resp.Body != nil {
			if err := resp.Body.Close(); err != nil {
				logger.WarnCtx(ctx, "failed to close response body", zap.Error(err), zap.String("url", sourceURL))
			}
		}

		logger.InfoCtx(ctx, "HEAD request failed, falling back to partial GET",
			zap.String("sourceURL", sourceURL),
			zap.Error(err),
			zap.Int("statusCode", func() int {
				if resp != nil {
					return resp.StatusCode
				}
				return 0
			}()),
		)

		// Fetch first 512 bytes to detect content-type
		partialContent, err := p.httpClient.GetPartialBytes(ctx, sourceURL, 512)
		if err != nil {
			return nil, fmt.Errorf("failed to get content-type via partial GET: %w", err)
		}

		// Detect content-type from the actual bytes using mimetype library
		mtype := mimetype.Detect(partialContent)
		if mtype != nil {
			contentType = mtype.String()
		}

		logger.InfoCtx(ctx, "Media content-type detected via partial GET",
			zap.String("sourceURL", sourceURL),
			zap.String("contentType", contentType),
		)
	}

	if contentType == "" {
		logger.WarnCtx(ctx, "Missing content type", zap.String("sourceURL", sourceURL))
		return nil, domain.ErrMissingContentLength
	}

	// Determine media type characteristics
	isVideo := strings.HasPrefix(contentType, "video/")
	isImage := strings.HasPrefix(contentType, "image/")
	isSVG := contentType == "image/svg+xml" || contentType == "image/svg"
	isAnimatedImage := strings.HasPrefix(contentType, "image/gif") || strings.HasPrefix(contentType, "image/webp")

	if !isVideo && !isImage {
		logger.WarnCtx(ctx, "Unsupported media file", zap.String("contentType", contentType))
		return nil, domain.ErrUnsupportedMediaFile
	}

	return &probeResult{
		contentType:     contentType,
		contentLength:   contentLength,
		isVideo:         isVideo,
		isImage:         isImage,
		isSVG:           isSVG,
		isAnimatedImage: isAnimatedImage,
	}, nil
}

// processSVG handles SVG files by rasterizing them to PNG
func (p *processor) processSVG(ctx context.Context, sourceURL string, probe *probeResult, metadata map[string]interface{}) (*mediaprovider.UploadResult, string, int64, error) {
	logger.InfoCtx(ctx, "SVG detected, rasterizing to PNG", zap.String("url", sourceURL))

	// Download the SVG content using the downloader
	downloadResult, err := p.downloader.Download(ctx, sourceURL)
	if err != nil {
		return nil, "", 0, fmt.Errorf("failed to download SVG: %w", err)
	}
	defer func() {
		if err := downloadResult.Close(); err != nil {
			logger.WarnCtx(ctx, "Failed to close download result", zap.Error(err))
		}
	}()

	// Read all SVG data
	svgData, err := p.io.ReadAll(downloadResult.Reader())
	if err != nil {
		return nil, "", 0, fmt.Errorf("failed to read SVG data: %w", err)
	}

	// Rasterize SVG to PNG
	pngData, err := p.rasterizer.Rasterize(ctx, svgData)
	if err != nil {
		logger.ErrorCtx(ctx, err)
		return nil, "", 0, fmt.Errorf("failed to rasterize SVG: %w", err)
	}

	// Update content type and length for the rasterized image
	newContentType := "image/png"
	newContentLength := int64(len(pngData))

	// Update upload metadata
	metadata["original_mime_type"] = probe.contentType
	metadata["mime_type"] = newContentType
	metadata["file_size"] = newContentLength
	metadata["rasterized"] = true

	logger.InfoCtx(ctx, "SVG rasterized successfully",
		zap.Int("pngSize", len(pngData)),
		zap.String("originalContentType", probe.contentType),
	)

	// Upload the rasterized PNG from reader
	logger.InfoCtx(ctx, "Uploading rasterized image from reader")
	uploadResult, err := p.uploadImageFromBytes(ctx, pngData, "rasterized.png", newContentType, metadata)
	if err != nil {
		return nil, "", 0, fmt.Errorf("failed to upload rasterized image: %w", err)
	}

	return uploadResult, newContentType, newContentLength, nil
}

// processVideo handles video files by uploading them directly
func (p *processor) processVideo(ctx context.Context, sourceURL string, probe *probeResult, metadata map[string]interface{}) (*mediaprovider.UploadResult, error) {
	// Check if the file size is within the allowed limits
	if probe.contentLength > 0 && probe.contentLength > p.maxVideoSize {
		logger.WarnCtx(ctx, "Video file size exceeds the allowed limit",
			zap.Int64("contentLength", probe.contentLength),
			zap.Int64("maxSize", p.maxVideoSize))
		return nil, domain.ErrExceededMaxFileSize
	}

	logger.InfoCtx(ctx, "Uploading video", zap.String("url", sourceURL))
	uploadResult, err := p.provider.UploadVideo(ctx, sourceURL, metadata)
	if err != nil {
		// Handle known unsupported errors
		if errors.Is(err, domain.ErrUnsupportedSelfHostedMediaFile) || errors.Is(err, domain.ErrUnsupportedURL) {
			logger.WarnCtx(ctx, "Unsupported video URL", zap.String("url", sourceURL))
			return nil, nil
		}
		return nil, fmt.Errorf("failed to upload video to provider: %w", err)
	}

	return uploadResult, nil
}

// processImage handles image files with optional transformation
func (p *processor) processImage(ctx context.Context, sourceURL string, probe *probeResult, metadata map[string]interface{}) (*mediaprovider.UploadResult, string, int64, error) {
	// Decide if we need to transform upfront based on known size
	needsTransform := probe.contentLength > p.maxImageSize
	var uploadResult *mediaprovider.UploadResult
	var err error
	finalContentType := probe.contentType
	finalContentLength := probe.contentLength

	if needsTransform {
		// Transform image if we know it's too large
		logger.InfoCtx(ctx, "Transforming image before upload", zap.String("url", sourceURL))
		uploadResult, finalContentType, finalContentLength, err = p.transformAndUpload(ctx, sourceURL, probe.isAnimatedImage, metadata)
		if err != nil {
			return nil, "", 0, fmt.Errorf("failed to transform and upload image: %w", err)
		}
	} else {
		// Try direct upload first (size unknown or within limits)
		logger.InfoCtx(ctx, "Uploading image", zap.String("url", sourceURL))
		uploadResult, err = p.provider.UploadImageFromURL(ctx, sourceURL, metadata)

		if err != nil {
			// Check if upload failed due to size error
			if isSizeError(err) {
				logger.InfoCtx(ctx, "Direct upload failed due to size, will transform and retry",
					zap.String("url", sourceURL),
					zap.Error(err))

				// Retry with transformation
				uploadResult, finalContentType, finalContentLength, err = p.transformAndUpload(ctx, sourceURL, probe.isAnimatedImage, metadata)
				if err != nil {
					return nil, "", 0, fmt.Errorf("failed to transform and upload image after size error: %w", err)
				}
			} else if errors.Is(err, domain.ErrUnsupportedSelfHostedMediaFile) || errors.Is(err, domain.ErrUnsupportedURL) {
				// Known unsupported errors, skip processing
				logger.WarnCtx(ctx, "Unsupported media file", zap.String("url", sourceURL))
				return nil, "", 0, nil
			} else {
				return nil, "", 0, fmt.Errorf("failed to upload media to provider: %w", err)
			}
		}
	}

	return uploadResult, finalContentType, finalContentLength, nil
}

// storeMediaAsset stores the media asset in the database
func (p *processor) storeMediaAsset(ctx context.Context, sourceURL string, contentType string, contentLength int64, uploadResult *mediaprovider.UploadResult) error {
	// Convert variant URLs to JSON
	variantURLsJSON, err := p.json.Marshal(uploadResult.VariantURLs)
	if err != nil {
		return fmt.Errorf("failed to marshal variant URLs: %w", err)
	}

	// Convert provider metadata to JSON
	var providerMetadataJSON datatypes.JSON
	if uploadResult.ProviderMetadata != nil {
		metadataBytes, err := p.json.Marshal(uploadResult.ProviderMetadata)
		if err != nil {
			return fmt.Errorf("failed to marshal provider metadata: %w", err)
		}
		providerMetadataJSON = datatypes.JSON(metadataBytes)
	}

	// Prepare media asset input
	mediaAssetInput := store.CreateMediaAssetInput{
		SourceURL:        sourceURL,
		MimeType:         &contentType,
		Provider:         p.toSchemaStorageProvider(),
		ProviderAssetID:  &uploadResult.ProviderAssetID,
		ProviderMetadata: providerMetadataJSON,
		VariantURLs:      datatypes.JSON(variantURLsJSON),
	}

	// Set file size if available
	if contentLength > 0 {
		mediaAssetInput.FileSizeBytes = &contentLength
	}

	// Create the media asset record
	_, err = p.store.CreateMediaAsset(ctx, mediaAssetInput)
	if err != nil {
		return fmt.Errorf("failed to store media asset in database: %w", err)
	}

	return nil
}

// Process uploads a media file from URL to a provider and stores the reference in the database
func (p *processor) Process(ctx context.Context, sourceURL string) error {
	logger.InfoCtx(ctx, "Starting media processing",
		zap.String("sourceURL", sourceURL),
		zap.String("provider", p.provider.Name()),
	)

	// Step 1: Probe the media file to get metadata
	probe, err := p.probe(ctx, sourceURL)
	if err != nil {
		return err
	}

	// Step 2: Prepare metadata for upload
	uploadMetadata := map[string]interface{}{
		"source_url": sourceURL,
		"mime_type":  probe.contentType,
	}

	if probe.contentLength > 0 {
		uploadMetadata["file_size"] = probe.contentLength
	}

	// Step 3: Process based on media type
	var uploadResult *mediaprovider.UploadResult
	finalContentType := probe.contentType
	finalContentLength := probe.contentLength

	if probe.isSVG {
		uploadResult, finalContentType, finalContentLength, err = p.processSVG(ctx, sourceURL, probe, uploadMetadata)
		if err != nil {
			return err
		}
	} else if probe.isVideo {
		uploadResult, err = p.processVideo(ctx, sourceURL, probe, uploadMetadata)
		if err != nil {
			return err
		}
	} else if probe.isImage || probe.isAnimatedImage {
		uploadResult, finalContentType, finalContentLength, err = p.processImage(ctx, sourceURL, probe, uploadMetadata)
		if err != nil {
			return err
		}
	}

	// Known unsupported errors return nil result
	if uploadResult == nil {
		return nil
	}

	logger.InfoCtx(ctx, "Media uploaded to provider",
		zap.String("provider", p.provider.Name()),
		zap.String("providerAssetID", uploadResult.ProviderAssetID),
		zap.Int("variantCount", len(uploadResult.VariantURLs)),
	)

	// Step 4: Store in database
	err = p.storeMediaAsset(ctx, sourceURL, finalContentType, finalContentLength, uploadResult)
	if err != nil {
		return err
	}

	logger.InfoCtx(ctx, "Media processing completed",
		zap.String("sourceURL", sourceURL),
		zap.String("providerAssetID", uploadResult.ProviderAssetID),
	)

	return nil
}

func (p *processor) Provider() string {
	return p.provider.Name()
}

// toSchemaStorageProvider converts the provider name to a schema storage provider
func (p *processor) toSchemaStorageProvider() schema.StorageProvider {
	switch p.provider.Name() {
	case cloudflare.CLOUDFLARE_PROVIDER_NAME:
		return schema.StorageProviderCloudflare
	default:
		return schema.StorageProviderSelfHosted
	}
}

// uploadImageFromBytes uploads image data from a byte slice using the provider's reader-based upload
func (p *processor) uploadImageFromBytes(ctx context.Context, data []byte, filename, contentType string, metadata map[string]interface{}) (*mediaprovider.UploadResult, error) {
	// Create a reader from the byte slice
	reader := bytes.NewReader(data)

	// Upload using the provider's reader-based method
	return p.provider.UploadImageFromReader(ctx, reader, filename, contentType, metadata)
}

// transformAndUpload transforms an image and uploads it to the provider
func (p *processor) transformAndUpload(
	ctx context.Context,
	sourceURL string,
	isAnimated bool,
	metadata map[string]interface{},
) (*mediaprovider.UploadResult, string, int64, error) {
	// Transform the image
	transformInput := &transformer.TransformInput{
		SourceURL:  sourceURL,
		IsAnimated: isAnimated,
	}

	result, err := p.transformer.Transform(ctx, transformInput)
	if err != nil {
		// Log specific error types
		switch {
		case errors.Is(err, transformer.ErrAnimationCannotFit):
			logger.WarnCtx(ctx, "Animation cannot fit within size limits even after transformation",
				zap.String("sourceURL", sourceURL))
			return nil, "", 0, err
		case errors.Is(err, transformer.ErrCannotMeetTargetSize):
			logger.WarnCtx(ctx, "Cannot meet target size even after transformation",
				zap.String("sourceURL", sourceURL))
			return nil, "", 0, err
		default:
			return nil, "", 0, fmt.Errorf("transformation failed: %w", err)
		}
	}

	logger.InfoCtx(ctx, "Image transformed successfully",
		zap.String("sourceURL", sourceURL),
		zap.Int("originalSize", int(result.OriginalSize)),
		zap.Int("transformedSize", int(result.TransformedSize)),
		zap.Int("width", result.Width),
		zap.Int("height", result.Height),
		zap.Bool("resized", result.Resized),
		zap.Bool("compressed", result.Compressed),
		zap.Int("quality", result.Quality),
		zap.String("contentType", result.ContentType),
	)

	// Update metadata with transformation info
	metadata["transformed"] = true
	metadata["original_size"] = result.OriginalSize
	metadata["transformed_size"] = result.TransformedSize
	metadata["mime_type"] = result.ContentType

	// Upload the transformed image
	reader := bytes.NewReader(result.Data)
	uploadResult, err := p.provider.UploadImageFromReader(ctx, reader, result.Filename, result.ContentType, metadata)
	if err != nil {
		return nil, "", 0, err
	}

	// Return the new content type and length from the transform result
	return uploadResult, result.ContentType, result.TransformedSize, nil
}

// isSizeError checks if an error is a Cloudflare size-related error
func isSizeError(err error) bool {
	return errors.Is(err, cloudflare.ErrImageExceededMaxFileSize) ||
		errors.Is(err, cloudflare.ErrAnimationTooLarge) ||
		errors.Is(err, cloudflare.ErrRequestEntityTooLarge)
}

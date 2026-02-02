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
	"github.com/feral-file/ff-indexer-v2/internal/providers/cloudflare"
	"github.com/feral-file/ff-indexer-v2/internal/store"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
	"github.com/feral-file/ff-indexer-v2/internal/uri"
)

// Processor defines the interface for processing media files
//
//go:generate mockgen -source=processor.go -destination=../../mocks/media_processor.go -package=mocks -mock_names=Processor=MockMediaProcessor
type Processor interface {
	// Process uploads a media file from URL to a provider and stores the reference in the database
	// This is an atomic operation - all steps succeed or all fail
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

	// Adapters
	io         adapter.IO
	json       adapter.JSON
	filesystem adapter.FileSystem

	// Configuration
	maxStaticImageSize   int64
	maxAnimatedImageSize int64
	maxVideoSize         int64
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
	maxStaticImageSize int64,
	maxAnimatedImageSize int64,
	maxVideoSize int64) Processor {
	return &processor{
		httpClient:           httpClient,
		uriResolver:          uriResolver,
		provider:             provider,
		store:                st,
		rasterizer:           svgRasterizer,
		filesystem:           filesystem,
		io:                   io,
		json:                 json,
		downloader:           dl,
		maxStaticImageSize:   maxStaticImageSize,
		maxAnimatedImageSize: maxAnimatedImageSize,
		maxVideoSize:         maxVideoSize,
	}
}

// Process uploads a media file from URL to a provider and stores the reference in the database
func (p *processor) Process(ctx context.Context, sourceURL string) error {
	logger.InfoCtx(ctx, "Starting media processing",
		zap.String("sourceURL", sourceURL),
		zap.String("provider", p.provider.Name()),
	)

	// Step 1: Try HEAD request first to get content-type and size
	// If HEAD fails, fallback to partial GET request
	var contentType string
	var contentLength int64

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
			return fmt.Errorf("failed to get content-type via partial GET: %w", err)
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
		return domain.ErrMissingContentLength
	}

	// Determine if this is a video, image, or SVG
	isVideo := strings.HasPrefix(contentType, "video/")
	isImage := strings.HasPrefix(contentType, "image/")
	isSVG := contentType == "image/svg+xml" || contentType == "image/svg"
	isAnimatedImage := strings.HasPrefix(contentType, "image/gif") || strings.HasPrefix(contentType, "image/webp")

	if !isVideo && !isImage {
		logger.WarnCtx(ctx, "Unsupported media file", zap.String("contentType", contentType))
		return domain.ErrUnsupportedMediaFile
	}

	// Prepare metadata for upload
	uploadMetadata := map[string]interface{}{
		"source_url": sourceURL,
		"mime_type":  contentType,
	}

	// Get content-length if available
	if contentLength > 0 {
		uploadMetadata["file_size"] = contentLength
	}

	// Check if the file size is within the allowed limits (only if we have content length from HEAD)
	if contentLength > 0 {
		if isAnimatedImage && contentLength > p.maxAnimatedImageSize {
			logger.WarnCtx(ctx, "Animated image file size exceeds the allowed limit", zap.Int64("contentLength", contentLength), zap.Int64("maxSize", p.maxAnimatedImageSize))
			return domain.ErrExceededMaxFileSize
		}
		if isVideo && contentLength > p.maxVideoSize {
			logger.WarnCtx(ctx, "Video file size exceeds the allowed limit", zap.Int64("contentLength", contentLength), zap.Int64("maxSize", p.maxVideoSize))
			return domain.ErrExceededMaxFileSize
		}
		if isImage && contentLength > p.maxStaticImageSize {
			logger.WarnCtx(ctx, "Image file size exceeds the allowed limit", zap.Int64("contentLength", contentLength), zap.Int64("maxSize", p.maxStaticImageSize))
			return domain.ErrExceededMaxFileSize
		}
	}

	// Step 2: Handle SVG rasterization if needed
	originalContentType := contentType
	var uploadResult *mediaprovider.UploadResult

	if isSVG {
		logger.InfoCtx(ctx, "SVG detected, rasterizing to PNG", zap.String("url", sourceURL))

		// Download the SVG content using the downloader
		downloadResult, err := p.downloader.Download(ctx, sourceURL)
		if err != nil {
			return fmt.Errorf("failed to download SVG: %w", err)
		}
		defer func() {
			if err := downloadResult.Close(); err != nil {
				logger.WarnCtx(ctx, "Failed to close download result", zap.Error(err))
			}
		}()

		// Read all SVG data
		svgData, err := p.io.ReadAll(downloadResult.Reader())
		if err != nil {
			return fmt.Errorf("failed to read SVG data: %w", err)
		}

		// Rasterize SVG to PNG
		pngData, err := p.rasterizer.Rasterize(ctx, svgData)
		if err != nil {
			logger.ErrorCtx(ctx, err)
			return fmt.Errorf("failed to rasterize SVG: %w", err)
		}

		// Update content type and length for the rasterized image
		contentType = "image/png"
		contentLength = int64(len(pngData))

		// Update upload metadata
		uploadMetadata["original_mime_type"] = originalContentType
		uploadMetadata["mime_type"] = contentType
		uploadMetadata["file_size"] = contentLength
		uploadMetadata["rasterized"] = true

		logger.InfoCtx(ctx, "SVG rasterized successfully",
			zap.Int("pngSize", len(pngData)),
			zap.String("originalContentType", originalContentType),
		)

		// Upload the rasterized PNG from reader
		logger.InfoCtx(ctx, "Uploading rasterized image from reader")
		uploadResult, err = p.uploadImageFromBytes(ctx, pngData, "rasterized.png", contentType, uploadMetadata)
		if err != nil {
			return fmt.Errorf("failed to upload rasterized image: %w", err)
		}
	} else {
		// Step 3: Upload to provider based on media type
		if isVideo {
			logger.InfoCtx(ctx, "Uploading video", zap.String("url", sourceURL))
			uploadResult, err = p.provider.UploadVideo(ctx, sourceURL, uploadMetadata)
		} else {
			logger.InfoCtx(ctx, "Uploading image", zap.String("url", sourceURL))
			uploadResult, err = p.provider.UploadImageFromURL(ctx, sourceURL, uploadMetadata)
		}

		if err != nil {
			switch {
			case errors.Is(err, domain.ErrUnsupportedSelfHostedMediaFile),
				errors.Is(err, domain.ErrUnsupportedURL),

				// There are no way to effectively handle these errors, so we skip processing
				errors.Is(err, cloudflare.ErrAnimationTooLarge),
				errors.Is(err, cloudflare.ErrImageExceededMaxFileSize):

				// Known error, skip processing
				return nil
			default:
				return fmt.Errorf("failed to upload media to provider: %w", err)
			}
		}
	}

	logger.InfoCtx(ctx, "Media uploaded to provider",
		zap.String("provider", p.provider.Name()),
		zap.String("providerAssetID", uploadResult.ProviderAssetID),
		zap.Int("variantCount", len(uploadResult.VariantURLs)),
	)

	// Step 4: Convert variant URLs to JSON
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

	// Step 5: Store in database
	mediaAssetInput := store.CreateMediaAssetInput{
		SourceURL:        sourceURL,
		MimeType:         &contentType,
		Provider:         p.toSchemaStorageProvider(),
		ProviderAssetID:  &uploadResult.ProviderAssetID,
		ProviderMetadata: providerMetadataJSON,
		VariantURLs:      datatypes.JSON(variantURLsJSON),
	}

	// Set file size if available
	if resp != nil && resp.ContentLength > 0 {
		mediaAssetInput.FileSizeBytes = &resp.ContentLength
	}

	// Create the media asset record
	_, err = p.store.CreateMediaAsset(ctx, mediaAssetInput)
	if err != nil {
		return fmt.Errorf("failed to store media asset in database: %w", err)
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

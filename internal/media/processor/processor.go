package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	logger "github.com/bitmark-inc/autonomy-logger"
	"go.uber.org/zap"

	"gorm.io/datatypes"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	mediaprovider "github.com/feral-file/ff-indexer-v2/internal/media/provider"
	"github.com/feral-file/ff-indexer-v2/internal/providers/cloudflare"
	"github.com/feral-file/ff-indexer-v2/internal/store"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
	"github.com/feral-file/ff-indexer-v2/internal/uri"
)

// Processor defines the interface for processing media files
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
	httpClient   adapter.HTTPClient
	uriResolver  uri.Resolver
	provider     mediaprovider.Provider
	store        store.Store
	maxImageSize int64
	maxVideoSize int64
}

// NewProcessor creates a new Processor instance
func NewProcessor(httpClient adapter.HTTPClient, uriResolver uri.Resolver, provider mediaprovider.Provider, st store.Store, maxImageSize int64, maxVideoSize int64) Processor {
	return &processor{
		httpClient:   httpClient,
		uriResolver:  uriResolver,
		provider:     provider,
		store:        st,
		maxImageSize: maxImageSize,
		maxVideoSize: maxVideoSize,
	}
}

// Process uploads a media file from URL to a provider and stores the reference in the database
func (p *processor) Process(ctx context.Context, sourceURL string) error {
	logger.Info("Starting media processing",
		zap.String("sourceURL", sourceURL),
		zap.String("provider", p.provider.Name()),
	)

	// Step 1: Resolve the URL (handle IPFS, Arweave, etc.)
	resolvedURL, err := p.uriResolver.Resolve(ctx, sourceURL)
	if err != nil {
		return fmt.Errorf("failed to resolve URL: %w", err)
	}

	logger.Info("URL resolved", zap.String("sourceURL", sourceURL), zap.String("resolvedURL", resolvedURL))

	// Step 2: Use HEAD request to get content-type and determine if it's image or video
	resp, err := p.httpClient.Head(ctx, resolvedURL)
	if err != nil {
		return fmt.Errorf("failed to get content-type via HEAD request: %w", err)
	}
	defer func() {
		if resp.Body != nil {
			_ = resp.Body.Close()
		}
	}()

	contentType := resp.Header.Get("Content-Type")
	if contentType == "" {
		return fmt.Errorf("no content-type header in response")
	}

	logger.Info("Media content-type detected",
		zap.String("sourceURL", sourceURL),
		zap.String("contentType", contentType),
	)

	// Determine if this is a video or image
	isVideo := strings.HasPrefix(contentType, "video/")
	isImage := strings.HasPrefix(contentType, "image/")

	if !isVideo && !isImage {
		return fmt.Errorf("unsupported media type: %s", contentType)
	}

	// Prepare metadata for upload
	uploadMetadata := map[string]interface{}{
		"source_url": sourceURL,
		"mime_type":  contentType,
	}

	// Get content-length if available
	if resp.ContentLength > 0 {
		uploadMetadata["file_size"] = resp.ContentLength
	}

	// Check if the file size is within the allowed limits
	if isVideo && resp.ContentLength > p.maxVideoSize {
		return fmt.Errorf("video file size exceeds the allowed limit: %d > %d", resp.ContentLength, p.maxVideoSize)
	}
	if isImage && resp.ContentLength > p.maxImageSize {
		return fmt.Errorf("image file size exceeds the allowed limit: %d > %d", resp.ContentLength, p.maxImageSize)
	}

	// Step 3: Upload to provider based on media type
	var uploadResult *mediaprovider.UploadResult
	if isVideo {
		logger.Info("Uploading video", zap.String("url", resolvedURL))
		uploadResult, err = p.provider.UploadVideo(ctx, resolvedURL, uploadMetadata)
		if err != nil {
			return fmt.Errorf("failed to upload video to provider: %w", err)
		}
	} else {
		logger.Info("Uploading image", zap.String("url", resolvedURL))
		uploadResult, err = p.provider.UploadImage(ctx, resolvedURL, uploadMetadata)
		if err != nil {
			return fmt.Errorf("failed to upload image to provider: %w", err)
		}
	}

	logger.Info("Media uploaded to provider",
		zap.String("provider", p.provider.Name()),
		zap.String("providerAssetID", uploadResult.ProviderAssetID),
		zap.Int("variantCount", len(uploadResult.VariantURLs)),
	)

	// Step 4: Convert variant URLs to JSON
	variantURLsJSON, err := json.Marshal(uploadResult.VariantURLs)
	if err != nil {
		return fmt.Errorf("failed to marshal variant URLs: %w", err)
	}

	// Convert provider metadata to JSON
	var providerMetadataJSON datatypes.JSON
	if uploadResult.ProviderMetadata != nil {
		metadataBytes, err := json.Marshal(uploadResult.ProviderMetadata)
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
	if resp.ContentLength > 0 {
		mediaAssetInput.FileSizeBytes = &resp.ContentLength
	}

	// Create the media asset record
	_, err = p.store.CreateMediaAsset(ctx, mediaAssetInput)
	if err != nil {
		return fmt.Errorf("failed to store media asset in database: %w", err)
	}

	logger.Info("Media processing completed",
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

package cloudflare

import (
	"context"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/cloudflare/cloudflare-go"

	logger "github.com/bitmark-inc/autonomy-logger"
	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	mediaprovider "github.com/feral-file/ff-indexer-v2/internal/media/provider"
)

const (
	CLOUDFLARE_PROVIDER_NAME  = "cloudflare"
	CLOUDFLARE_IMAGE_ENDPOINT = "https://imagedelivery.net"
)

// Config holds configuration for Cloudflare Images and Stream
type Config struct {
	// AccountID is the Cloudflare account ID for Images
	AccountID string
	// APIToken is the API token for authentication
	APIToken string
}

// mediaProvider implements the media.Provider interface for Cloudflare Images and Stream
type mediaProvider struct {
	cfClient adapter.CloudflareClient
	config   *Config
	rc       *cloudflare.ResourceContainer
}

// NewMediaProvider creates a new Cloudflare Images and Stream provider
func NewMediaProvider(cfClient adapter.CloudflareClient, config *Config) mediaprovider.Provider {
	return &mediaProvider{
		cfClient: cfClient,
		config:   config,
		rc: &cloudflare.ResourceContainer{
			Level:      cloudflare.AccountRouteLevel,
			Identifier: config.AccountID,
		},
	}
}

// UploadImage uploads an image to Cloudflare Images from a URL
func (p *mediaProvider) UploadImage(ctx context.Context, sourceURL string, metadata map[string]interface{}) (*mediaprovider.UploadResult, error) {
	return p.uploadImage(ctx, sourceURL, metadata)
}

// UploadVideo uploads a video to Cloudflare Stream from a URL
func (p *mediaProvider) UploadVideo(ctx context.Context, sourceURL string, metadata map[string]interface{}) (*mediaprovider.UploadResult, error) {
	return p.uploadVideo(ctx, sourceURL, metadata)
}

// uploadImage uploads an image to Cloudflare Images from a URL
func (p *mediaProvider) uploadImage(ctx context.Context, sourceURL string, metadata map[string]interface{}) (*mediaprovider.UploadResult, error) {
	if !strings.HasPrefix(sourceURL, CLOUDFLARE_IMAGE_ENDPOINT) {
		return nil, fmt.Errorf("unsupported media URL: %s", sourceURL)
	}

	logger.Info("Uploading to Cloudflare Images", zap.String("url", sourceURL), zap.Any("metadata", metadata))

	// Upload image via URL
	params := cloudflare.UploadImageParams{
		URL:      sourceURL,
		Metadata: metadata,
	}

	// Upload using the SDK
	image, err := p.cfClient.UploadImage(ctx, p.rc, params)
	if err != nil {
		return nil, fmt.Errorf("failed to upload image: %w", err)
	}

	// Convert variants from []string to map[string]string
	// Variant URLs are in format: https://imagedelivery.net/{account_hash}/{image_id}/{variant_name}
	variantURLs := make(map[string]string)
	for _, variantURL := range image.Variants {
		variantName := extractVariantName(variantURL)
		if variantName != "" {
			variantURLs[variantName] = variantURL
		}
	}

	// Provider metadata
	providerMetadata := map[string]interface{}{
		"account_id":  p.config.AccountID,
		"uploaded_at": image.Uploaded.Format(time.RFC3339),
		"filename":    image.Filename,
		"media_type":  "image",
	}

	logger.Info("Successfully uploaded to Cloudflare Images",
		zap.String("imageID", image.ID),
		zap.Int("variantCount", len(variantURLs)),
	)

	return &mediaprovider.UploadResult{
		ProviderAssetID:  image.ID,
		VariantURLs:      variantURLs,
		ProviderMetadata: providerMetadata,
	}, nil
}

// uploadVideo uploads a video to Cloudflare Stream via URL
func (p *mediaProvider) uploadVideo(ctx context.Context, sourceURL string, metadata map[string]interface{}) (*mediaprovider.UploadResult, error) {
	logger.Info("Uploading to Cloudflare Stream", zap.String("url", sourceURL), zap.Any("metadata", metadata))

	// Upload using URL-based method (no file download needed)
	video, err := p.cfClient.UploadVideoFromURL(ctx, cloudflare.StreamUploadFromURLParameters{
		AccountID: p.config.AccountID,
		URL:       sourceURL,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to upload video: %w", err)
	}

	// Poll for video details until processing is complete or timeout (5 minutes)
	videoDetails, err := p.waitForVideoReady(ctx, video.UID)
	if err != nil {
		// If we can't get details or it times out, continue with basic info
		logger.Warn("Failed to get complete video details, using basic info",
			zap.Error(err),
			zap.String("videoID", video.UID),
		)
		videoDetails = video
	}

	// Build variant URLs for different playback options
	variantURLs := make(map[string]string)

	// Add HLS manifest URL (for adaptive bitrate streaming)
	if videoDetails.Playback.HLS != "" {
		variantURLs["hls"] = videoDetails.Playback.HLS
	}

	// Add DASH manifest URL
	if videoDetails.Playback.Dash != "" {
		variantURLs["dash"] = videoDetails.Playback.Dash
	}

	// Add thumbnail URL
	if videoDetails.Thumbnail != "" {
		variantURLs["thumbnail"] = videoDetails.Thumbnail
	}

	// Add preview URL if available
	if videoDetails.Preview != "" {
		variantURLs["preview"] = videoDetails.Preview
	}

	// Provider metadata
	providerMetadata := map[string]interface{}{
		"account_id": p.rc.Identifier,
		"media_type": "video",
		"duration":   videoDetails.Duration,
		"status":     videoDetails.Status.State,
	}

	if videoDetails.Uploaded != nil {
		providerMetadata["uploaded_at"] = videoDetails.Uploaded.Format(time.RFC3339)
	}
	if videoDetails.Input.Width > 0 && videoDetails.Input.Height > 0 {
		providerMetadata["width"] = videoDetails.Input.Width
		providerMetadata["height"] = videoDetails.Input.Height
	}

	logger.Info("Successfully uploaded to Cloudflare Stream",
		zap.String("videoID", video.UID),
		zap.String("status", string(videoDetails.Status.State)),
		zap.Int("variantCount", len(variantURLs)),
	)

	return &mediaprovider.UploadResult{
		ProviderAssetID:  video.UID,
		VariantURLs:      variantURLs,
		ProviderMetadata: providerMetadata,
	}, nil
}

// waitForVideoReady polls Cloudflare Stream until the video is ready or timeout using backoff retry
func (p *mediaProvider) waitForVideoReady(ctx context.Context, videoID string) (cloudflare.StreamVideo, error) {
	var videoDetails cloudflare.StreamVideo

	// Configure exponential backoff with 5-minute timeout
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = 2 * time.Second
	b.MaxInterval = 30 * time.Second
	b.MaxElapsedTime = 5 * time.Minute // 5-minute timeout as requested
	b.Multiplier = 1.5
	b.RandomizationFactor = 0.5 // Add jitter

	operation := func() error {
		// Fetch video details
		video, err := p.cfClient.GetVideo(ctx, cloudflare.StreamParameters{
			AccountID: p.config.AccountID,
			VideoID:   videoID,
		})
		if err != nil {
			// Network errors are retryable
			logger.Warn("Failed to fetch video details, retrying", zap.Error(err))
			return fmt.Errorf("failed to get video: %w", err)
		}

		videoDetails = video

		// Check video status
		switch video.Status.State {
		case "ready":
			logger.Info("Video processing complete",
				zap.String("videoID", videoID),
			)
			return nil // Success - stop retrying

		case "error", "failed":
			// Permanent error - don't retry
			return backoff.Permanent(fmt.Errorf("video processing failed: %s", video.Status.ErrorReasonText))

		case "inprogress", "queued", "downloading":
			// Still processing - retry
			logger.Debug("Video still processing",
				zap.String("videoID", videoID),
				zap.String("status", string(video.Status.State)),
			)
			return fmt.Errorf("video not ready yet: %s", video.Status.State)

		default:
			// Unknown status - retry
			logger.Warn("Unknown video status",
				zap.String("videoID", videoID),
				zap.String("status", string(video.Status.State)),
			)
			return fmt.Errorf("unknown video status: %s", video.Status.State)
		}
	}

	// Execute with retry and context support
	if err := backoff.Retry(operation, backoff.WithContext(b, ctx)); err != nil {
		return videoDetails, fmt.Errorf("timeout or error waiting for video to be ready: %w", err)
	}

	return videoDetails, nil
}

// Name returns the provider name
func (p *mediaProvider) Name() string {
	return CLOUDFLARE_PROVIDER_NAME
}

// extractVariantName extracts the variant name from a Cloudflare Image variant URL
// Format: https://imagedelivery.net/{account_hash}/{image_id}/{variant_name}
func extractVariantName(variantURL string) string {
	// Split by "/" and get the last segment
	parts := strings.Split(variantURL, "/")
	if len(parts) > 0 {
		return path.Base(variantURL)
	}
	return ""
}

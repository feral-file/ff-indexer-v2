package cloudflare

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/cloudflare/cloudflare-go"
	"github.com/gabriel-vasile/mimetype"

	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/downloader"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	mediaprovider "github.com/feral-file/ff-indexer-v2/internal/media/provider"
	"github.com/feral-file/ff-indexer-v2/internal/types"
)

const (
	CLOUDFLARE_PROVIDER_NAME         = "cloudflare"
	CLOUDFLARE_IMAGE_ENDPOINT_REGEX  = `^(https://imagedelivery\.net/[A-Za-z0-9_-]+/[A-Za-z0-9_-]+)(?:/([A-Za-z0-9_-]+))?$`
	CLOUDFLARE_STREAM_ENDPOINT_REGEX = `^(https://customer-[A-Za-z0-9]+\.cloudflarestream\.com/[A-Za-z0-9]+/manifest/video)\.(m3u8|mpd)$`
)

var (
	// Error Code: 5413
	ErrImageExceededMaxFileSize = errors.New("image exceeded max file size")

	// Error Code: 5443
	ErrAnimationTooLarge = errors.New("animation too large")
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
	cfClient   adapter.CloudflareClient
	config     *Config
	rc         *cloudflare.ResourceContainer
	downloader downloader.Downloader
	fs         adapter.FileSystem
}

// NewMediaProvider creates a new Cloudflare Images and Stream provider
func NewMediaProvider(cfClient adapter.CloudflareClient, config *Config, dl downloader.Downloader, fs adapter.FileSystem) mediaprovider.Provider {
	return &mediaProvider{
		cfClient:   cfClient,
		config:     config,
		downloader: dl,
		fs:         fs,
		rc: &cloudflare.ResourceContainer{
			Level:      cloudflare.AccountRouteLevel,
			Identifier: config.AccountID,
		},
	}
}

// UploadImageFromURL uploads an image to Cloudflare Images from a URL
func (p *mediaProvider) UploadImageFromURL(ctx context.Context, sourceURL string, metadata map[string]interface{}) (*mediaprovider.UploadResult, error) {
	result, err := p.uploadImageFromURLInternal(ctx, sourceURL, metadata)
	if err != nil {
		return nil, categorizeError(err)
	}
	return result, nil
}

// UploadImageFromReader uploads an image to Cloudflare Images from an io.Reader
func (p *mediaProvider) UploadImageFromReader(ctx context.Context, reader io.Reader, filename, contentType string, metadata map[string]interface{}) (*mediaprovider.UploadResult, error) {
	logger.InfoCtx(ctx, "Uploading image from reader to Cloudflare Images",
		zap.String("filename", filename),
		zap.String("contentType", contentType),
		zap.Any("metadata", metadata),
	)

	// Upload directly from the reader
	image, err := p.uploadImageUsingReader(ctx, reader, filename, metadata)
	if err != nil {
		return nil, categorizeError(err)
	}

	// Convert variants to result format
	return p.buildImageUploadResult(ctx, image), nil
}

// UploadVideo uploads a video to Cloudflare Stream from a URL
func (p *mediaProvider) UploadVideo(ctx context.Context, sourceURL string, metadata map[string]interface{}) (*mediaprovider.UploadResult, error) {
	result, err := p.uploadVideo(ctx, sourceURL, metadata)
	if err != nil {
		return nil, categorizeError(err)
	}
	return result, nil
}

// uploadImageFromURLInternal uploads an image to Cloudflare Images from a URL
// with automatic fallback to download-and-upload if URL-based upload fails
func (p *mediaProvider) uploadImageFromURLInternal(ctx context.Context, sourceURL string, metadata map[string]interface{}) (*mediaprovider.UploadResult, error) {
	// Validate source URL is a valid image URL
	if !types.IsHTTPSURL(sourceURL) {
		logger.WarnCtx(ctx, "only HTTPS URLs are supported", zap.String("url", sourceURL))
		return nil, domain.ErrUnsupportedURL
	}

	// Validate the source URL is a Cloudflare Images URL
	if ok, _ := IsCloudflareImageURL(sourceURL); ok {
		logger.WarnCtx(ctx, "Unsupported self-hosted image URL", zap.String("url", sourceURL))
		return nil, domain.ErrUnsupportedSelfHostedMediaFile
	}

	logger.InfoCtx(ctx, "Uploading to Cloudflare Images", zap.String("url", sourceURL), zap.Any("metadata", metadata))

	// Try URL-based upload first
	image, err := p.uploadImageDirectlyFromURL(ctx, sourceURL, metadata)
	if err != nil {
		logger.WarnCtx(ctx, "URL-based image upload failed, trying download fallback",
			zap.String("url", sourceURL),
			zap.Error(err),
		)

		// Fallback to download and upload from reader
		image, err = p.uploadImageFromURLWithDownloadFallback(ctx, sourceURL, metadata)
		if err != nil {
			return nil, fmt.Errorf("failed to upload image: %w", err)
		}
	}

	// Convert variants to result format
	return p.buildImageUploadResult(ctx, image), nil
}

// uploadImageDirectlyFromURL uploads an image to Cloudflare using URL-based upload
// (without downloading - lets Cloudflare fetch the URL)
func (p *mediaProvider) uploadImageDirectlyFromURL(ctx context.Context, sourceURL string, metadata map[string]interface{}) (*cloudflare.Image, error) {
	params := cloudflare.UploadImageParams{
		URL:      sourceURL,
		Metadata: metadata,
	}

	image, err := p.cfClient.UploadImage(ctx, p.rc, params)
	if err != nil {
		return &cloudflare.Image{}, err
	}

	logger.InfoCtx(ctx, "Successfully uploaded image via URL",
		zap.String("url", sourceURL),
		zap.String("imageID", image.ID),
	)

	return &image, nil
}

// uploadImageFromURLWithDownloadFallback downloads an image from URL and uploads using io.Reader
// This is used as a fallback when direct URL-based upload fails
func (p *mediaProvider) uploadImageFromURLWithDownloadFallback(ctx context.Context, sourceURL string, metadata map[string]interface{}) (*cloudflare.Image, error) {
	// Download the file
	downloadResult, err := p.downloader.Download(ctx, sourceURL)
	if err != nil {
		return nil, fmt.Errorf("failed to download image: %w", err)
	}
	defer func() {
		if err := downloadResult.Close(); err != nil {
			logger.WarnCtx(ctx, "Failed to close download result", zap.Error(err))
		}
	}()

	// Extract filename from URL or use a default with correct extension
	filename := filepath.Base(sourceURL)
	if filepath.Ext(filename) == "" {
		// Filename exists but has no extension, add one based on mime type
		ext := getFileExtFromMimeType(downloadResult.ContentType())
		filename = fmt.Sprintf("%s%s", filename, ext)
	}

	// Upload using the reader
	return p.uploadImageUsingReader(ctx, downloadResult.Reader(), filename, metadata)
}

// uploadImageUsingReader uploads an image to Cloudflare using an io.Reader
// This is a low-level helper used by both the public UploadImageFromReader method
// and the download fallback mechanism
func (p *mediaProvider) uploadImageUsingReader(ctx context.Context, reader io.Reader, filename string, metadata map[string]interface{}) (*cloudflare.Image, error) {
	// Wrap reader with NopCloser if it doesn't implement io.ReadCloser
	var readCloser io.ReadCloser
	if rc, ok := reader.(io.ReadCloser); ok {
		readCloser = rc
	} else {
		readCloser = io.NopCloser(reader)
	}

	params := cloudflare.UploadImageParams{
		File:     readCloser,
		Name:     filename,
		Metadata: metadata,
	}

	image, err := p.cfClient.UploadImage(ctx, p.rc, params)
	if err != nil {
		return nil, fmt.Errorf("failed to upload image from reader: %w", err)
	}

	logger.InfoCtx(ctx, "Successfully uploaded image from reader",
		zap.String("filename", filename),
		zap.String("imageID", image.ID),
	)

	return &image, nil
}

// buildImageUploadResult converts a Cloudflare Image to an UploadResult
func (p *mediaProvider) buildImageUploadResult(ctx context.Context, image *cloudflare.Image) *mediaprovider.UploadResult {
	// Convert variants from []string to map[string]string
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

	logger.InfoCtx(ctx, "Successfully uploaded to Cloudflare Images",
		zap.String("imageID", image.ID),
		zap.Int("variantCount", len(variantURLs)),
	)

	return &mediaprovider.UploadResult{
		ProviderAssetID:  image.ID,
		VariantURLs:      variantURLs,
		ProviderMetadata: providerMetadata,
	}
}

// uploadVideo uploads a video to Cloudflare Stream via URL
func (p *mediaProvider) uploadVideo(ctx context.Context, sourceURL string, metadata map[string]interface{}) (*mediaprovider.UploadResult, error) {
	// Validate the source URL is a valid video URL
	if !types.IsHTTPSURL(sourceURL) {
		logger.WarnCtx(ctx, "only HTTPS URLs are supported", zap.String("url", sourceURL))
		return nil, domain.ErrUnsupportedURL
	}

	// Validate the source URL is a Cloudflare Stream URL
	if isCloudflareStreamURL(sourceURL) {
		logger.WarnCtx(ctx, "Unsupported self-hosted video URL", zap.String("url", sourceURL))
		return nil, domain.ErrUnsupportedSelfHostedMediaFile
	}

	logger.InfoCtx(ctx, "Uploading to Cloudflare Stream", zap.String("url", sourceURL), zap.Any("metadata", metadata))

	// Try URL-based upload first
	video, err := p.uploadVideoFromURL(ctx, sourceURL)
	if err != nil {
		logger.WarnCtx(ctx, "URL-based video upload failed, trying download fallback",
			zap.String("url", sourceURL),
			zap.Error(err),
		)

		// Fallback to download and upload from file
		video, err = p.uploadVideoFromFile(ctx, sourceURL)
		if err != nil {
			return nil, fmt.Errorf("failed to upload video: %w", err)
		}
	}

	// Poll for video details until processing is complete or timeout (5 minutes)
	videoDetails, err := p.waitForVideoReady(ctx, video.UID)
	if err != nil {
		// If we can't get details or it times out, continue with basic info
		logger.WarnCtx(ctx, "Failed to get complete video details, using basic info",
			zap.Error(err),
			zap.String("videoID", video.UID),
		)
		videoDetails = video
	}

	// Convert to result format
	return p.buildVideoUploadResult(ctx, videoDetails), nil
}

// uploadVideoFromURL uploads a video to Cloudflare using URL-based upload
func (p *mediaProvider) uploadVideoFromURL(ctx context.Context, sourceURL string) (cloudflare.StreamVideo, error) {
	video, err := p.cfClient.UploadVideoFromURL(ctx, cloudflare.StreamUploadFromURLParameters{
		AccountID: p.config.AccountID,
		URL:       sourceURL,
	})
	if err != nil {
		return cloudflare.StreamVideo{}, err
	}

	logger.InfoCtx(ctx, "Successfully uploaded video via URL",
		zap.String("url", sourceURL),
		zap.String("videoID", video.UID),
	)

	return video, nil
}

// uploadVideoFromFile downloads and uploads a video from a temporary file
func (p *mediaProvider) uploadVideoFromFile(ctx context.Context, sourceURL string) (cloudflare.StreamVideo, error) {
	// Download the file
	downloadResult, err := p.downloader.Download(ctx, sourceURL)
	if err != nil {
		return cloudflare.StreamVideo{}, fmt.Errorf("failed to download video: %w", err)
	}
	defer func() {
		if err := downloadResult.Close(); err != nil {
			logger.WarnCtx(ctx, "Failed to close download result", zap.Error(err))
		}
	}()

	// Create temp file for video
	tempDir := p.fs.TempDir()

	// Get filename with appropriate extension based on content type
	filename := filepath.Base(sourceURL)
	if filepath.Ext(filename) == "" {
		// Filename exists but has no extension, add one based on mime type
		ext := getFileExtFromMimeType(downloadResult.ContentType())
		filename = fmt.Sprintf("%s%s", filename, ext)
	}

	tempFile := filepath.Join(tempDir, fmt.Sprintf("ff-indexer-video-%d-%s", time.Now().UnixNano(), filename))

	logger.InfoCtx(ctx, "Saving video to temp file",
		zap.String("tempFile", tempFile),
	)

	// Save to temp file
	err = downloadResult.AsFile(tempFile)
	if err != nil {
		return cloudflare.StreamVideo{}, fmt.Errorf("failed to save video to temp file: %w", err)
	}

	// Ensure temp file is cleaned up
	defer func() {
		if err := p.fs.Remove(tempFile); err != nil {
			logger.WarnCtx(ctx, "Failed to remove temp file", zap.String("file", tempFile), zap.Error(err))
		} else {
			logger.DebugCtx(ctx, "Cleaned up temp file", zap.String("file", tempFile))
		}
	}()

	// Upload from file
	video, err := p.cfClient.UploadVideoFromFile(ctx, cloudflare.StreamUploadFileParameters{
		AccountID: p.config.AccountID,
		FilePath:  tempFile,
	})
	if err != nil {
		return cloudflare.StreamVideo{}, fmt.Errorf("failed to upload video from file: %w", err)
	}

	logger.InfoCtx(ctx, "Successfully uploaded video using download fallback",
		zap.String("url", sourceURL),
		zap.String("videoID", video.UID),
	)

	return video, nil
}

// buildVideoUploadResult converts a Cloudflare StreamVideo to an UploadResult
func (p *mediaProvider) buildVideoUploadResult(ctx context.Context, videoDetails cloudflare.StreamVideo) *mediaprovider.UploadResult {
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

	logger.InfoCtx(ctx, "Successfully uploaded to Cloudflare Stream",
		zap.String("videoID", videoDetails.UID),
		zap.String("status", string(videoDetails.Status.State)),
		zap.Int("variantCount", len(variantURLs)),
	)

	return &mediaprovider.UploadResult{
		ProviderAssetID:  videoDetails.UID,
		VariantURLs:      variantURLs,
		ProviderMetadata: providerMetadata,
	}
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
			logger.WarnCtx(ctx, "Failed to fetch video details, retrying", zap.Error(err))
			return fmt.Errorf("failed to get video: %w", err)
		}

		videoDetails = video

		// Check video status
		switch video.Status.State {
		case "ready":
			logger.InfoCtx(ctx, "Video processing complete",
				zap.String("videoID", videoID),
			)
			return nil // Success - stop retrying

		case "error", "failed":
			// Permanent error - don't retry
			return backoff.Permanent(fmt.Errorf("video processing failed: %s", video.Status.ErrorReasonText))

		case "inprogress", "queued", "downloading":
			// Still processing - retry
			logger.DebugCtx(ctx, "Video still processing",
				zap.String("videoID", videoID),
				zap.String("status", string(video.Status.State)),
			)
			return fmt.Errorf("video not ready yet: %s", video.Status.State)

		default:
			// Unknown status - retry
			logger.WarnCtx(ctx, "Unknown video status",
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

// IsCloudflareImageURL checks if a URL is a Cloudflare Images URL
// It returns a boolean indicating if the URL is a Cloudflare Images URL and a boolean indicating if the URL has a variant
func IsCloudflareImageURL(url string) (bool, bool) {
	reg := regexp.MustCompile(CLOUDFLARE_IMAGE_ENDPOINT_REGEX)
	matches := reg.FindStringSubmatch(url)
	if len(matches) == 0 {
		return false, false
	}
	return true, len(matches) > 2 && matches[2] != ""
}

// isCloudflareStreamURL checks if a URL is a Cloudflare Stream URL
func isCloudflareStreamURL(url string) bool {
	return regexp.MustCompile(CLOUDFLARE_STREAM_ENDPOINT_REGEX).MatchString(url)
}

// getFileExtFromMimeType returns a file extension for a given mime type
func getFileExtFromMimeType(mimeType string) string {
	// Use mimetype library to detect extension
	mtype := mimetype.Lookup(mimeType)
	if mtype != nil {
		ext := mtype.Extension()
		if ext != "" {
			return ext
		}
	}

	// Fallback for common cases if mimetype library doesn't have it
	mainType := strings.Split(mimeType, ";")[0]
	mainType = strings.TrimSpace(mainType)

	if strings.HasPrefix(mainType, "video/") {
		return ".mp4"
	}
	if strings.HasPrefix(mainType, "image/") {
		return ".jpg"
	}

	return ""
}

// categorizeError categorizes a Cloudflare error into a specific error type
// It returns the specific error type or the original error if it doesn't match any of the known error codes
func categorizeError(err error) error {
	if err == nil {
		return nil
	}

	switch {
	case strings.Contains(err.Error(), "5413"):
		return ErrImageExceededMaxFileSize
	case strings.Contains(err.Error(), "5443"):
		return ErrAnimationTooLarge
	}
	return err
}

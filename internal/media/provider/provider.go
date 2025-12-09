package mediaprovider

import (
	"context"
	"io"
)

// UploadResult represents the result of uploading media to a provider
type UploadResult struct {
	// ProviderAssetID is the provider-specific identifier (e.g., Cloudflare image ID, S3 key)
	ProviderAssetID string
	// VariantURLs maps variant names to their URLs (e.g., {"small": "https://...", "original": "https://..."})
	VariantURLs map[string]string
	// ProviderMetadata contains provider-specific metadata
	ProviderMetadata map[string]interface{}
}

// Provider defines the interface for media storage providers
type Provider interface {
	// UploadImageFromURL uploads an image from a URL to the provider
	UploadImageFromURL(ctx context.Context, sourceURL string, metadata map[string]interface{}) (*UploadResult, error)

	// UploadImageFromReader uploads an image from an io.Reader to the provider
	// Parameters:
	//   - reader: the io.Reader containing the image data
	//   - filename: the filename for the image (with extension)
	//   - contentType: the MIME type of the image (e.g., "image/png")
	//   - metadata: additional metadata to attach to the upload
	UploadImageFromReader(ctx context.Context, reader io.Reader, filename, contentType string, metadata map[string]interface{}) (*UploadResult, error)

	// UploadVideo uploads a video from a URL to the provider
	UploadVideo(ctx context.Context, sourceURL string, metadata map[string]interface{}) (*UploadResult, error)

	// Name returns the provider name
	Name() string
}

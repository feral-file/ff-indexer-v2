package mediaprovider

import (
	"context"
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

// Provider defines the interface for media storage providers (URL-based uploads only)
type Provider interface {
	// UploadImage uploads an image from a URL to the provider
	UploadImage(ctx context.Context, sourceURL string, metadata map[string]interface{}) (*UploadResult, error)

	// UploadVideo uploads a video from a URL to the provider
	UploadVideo(ctx context.Context, sourceURL string, metadata map[string]interface{}) (*UploadResult, error)

	// Name returns the provider name
	Name() string
}

package adapter

import (
	"context"

	"github.com/cloudflare/cloudflare-go"
)

// CloudflareClient defines an interface for Cloudflare Images and Stream API operations to enable mocking
//
//go:generate mockgen -source=cloudflare.go -destination=../mocks/cloudflare.go -package=mocks -mock_names=CloudflareClient=MockCloudflareClient
type CloudflareClient interface {
	// UploadImage uploads a single image to Cloudflare Images
	UploadImage(ctx context.Context, rc *cloudflare.ResourceContainer, params cloudflare.UploadImageParams) (cloudflare.Image, error)

	// GetImage gets the details of an uploaded image, including variant URLs
	GetImage(ctx context.Context, rc *cloudflare.ResourceContainer, id string) (cloudflare.Image, error)

	// UploadVideoFromURL uploads a video to Cloudflare Stream via URL
	UploadVideoFromURL(ctx context.Context, params cloudflare.StreamUploadFromURLParameters) (cloudflare.StreamVideo, error)

	// GetVideo retrieves video details from Cloudflare Stream
	GetVideo(ctx context.Context, params cloudflare.StreamParameters) (cloudflare.StreamVideo, error)
}

// RealCloudflareClient implements CloudflareClient using the official Cloudflare SDK
type RealCloudflareClient struct {
	api *cloudflare.API
}

// NewCloudflareClient creates a new real Cloudflare client
func NewCloudflareClient(apiToken string) (CloudflareClient, error) {
	api, err := cloudflare.NewWithAPIToken(apiToken)
	if err != nil {
		return nil, err
	}
	return &RealCloudflareClient{
		api: api,
	}, nil
}

// UploadImage uploads a single image to Cloudflare Images
func (c *RealCloudflareClient) UploadImage(ctx context.Context, rc *cloudflare.ResourceContainer, params cloudflare.UploadImageParams) (cloudflare.Image, error) {
	return c.api.UploadImage(ctx, rc, params)
}

// GetImage gets the details of an uploaded image
func (c *RealCloudflareClient) GetImage(ctx context.Context, rc *cloudflare.ResourceContainer, id string) (cloudflare.Image, error) {
	return c.api.GetImage(ctx, rc, id)
}

// UploadVideoFromURL uploads a video to Cloudflare Stream via URL
func (c *RealCloudflareClient) UploadVideoFromURL(ctx context.Context, params cloudflare.StreamUploadFromURLParameters) (cloudflare.StreamVideo, error) {
	return c.api.StreamUploadFromURL(ctx, params)
}

// GetVideo retrieves video details from Cloudflare Stream
func (c *RealCloudflareClient) GetVideo(ctx context.Context, params cloudflare.StreamParameters) (cloudflare.StreamVideo, error) {
	return c.api.StreamGetVideo(ctx, params)
}

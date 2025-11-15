package feralfile

import (
	"context"
	"fmt"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
)

const (
	// CDN is the base URL for Feral File CDN
	CDN = "https://cdn.feralfileassets.com"
)

// ArtworkResponse represents the API response from Feral File
type ArtworkResponse struct {
	Result Artwork `json:"result"`
}

// Artwork represents an artwork from Feral File API
type Artwork struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	ThumbnailURI string `json:"thumbnailURI"`
	PreviewURI   string `json:"previewURI"`
	Series       Series `json:"series"`
}

// Series represents a series (collection) in Feral File
type Series struct {
	Description string `json:"description"`
	Medium      string `json:"medium"`
	Artist      Artist `json:"artist"`
}

// Artist represents an artist in Feral File
type Artist struct {
	ID            string        `json:"ID"`
	AlumniAccount AlumniAccount `json:"alumniAccount"`
}

// AlumniAccount represents an alumni account with addresses
type AlumniAccount struct {
	ID        string            `json:"ID"`
	Alias     string            `json:"alias"`
	Addresses map[string]string `json:"addresses"`
}

// URL returns the full URL for a given path
func URL(path string) string {
	return fmt.Sprintf("%s/%s", CDN, path)
}

// Client defines the interface for Feral File client operations to enable mocking
//
//go:generate mockgen -source=client.go -destination=../../../mocks/feralfile_client.go -package=mocks -mock_names=Client=MockFeralFileClient
type Client interface {
	// GetArtwork fetches artwork data from Feral File API
	GetArtwork(ctx context.Context, tokenID string) (*Artwork, error)
}

// FeralFileClient implements Feral File client
type FeralFileClient struct {
	httpClient adapter.HTTPClient
	apiBaseURL string
}

// NewClient creates a new Feral File client
func NewClient(httpClient adapter.HTTPClient, apiBaseURL string) Client {
	return &FeralFileClient{
		httpClient: httpClient,
		apiBaseURL: apiBaseURL,
	}
}

// GetArtwork fetches artwork data from Feral File API
func (c *FeralFileClient) GetArtwork(ctx context.Context, tokenID string) (*Artwork, error) {
	url := fmt.Sprintf("%s/artworks/%s?includeArtist=true", c.apiBaseURL, tokenID)

	var response ArtworkResponse
	if err := c.httpClient.Get(ctx, url, &response); err != nil {
		return nil, fmt.Errorf("failed to call Feral File API: %w", err)
	}

	return &response.Result, nil
}

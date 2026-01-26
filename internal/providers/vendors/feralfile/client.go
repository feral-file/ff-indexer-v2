package feralfile

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/providers/cloudflare"
)

const (
	// CDN is the base URL for Feral File CDN
	CDN = "https://cdn.feralfileassets.com"

	// API_ENDPOINT is the base URL for Feral File API
	API_ENDPOINT = "https://feralfile.com/api"

	// Maya Man StarQuest contract
	MAYA_MAN_STARQUEST_CONTRACT = "0x67E3ad1902A55074AAdD84d9b335105B2D52b813"
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

// CanonicalName returns the canonical name for an artwork
func (a *Artwork) CanonicalName() string {
	if strings.HasPrefix(a.Name, "#") ||
		a.Name == "AE" ||
		a.Name == "AP" ||
		a.Name == "PP" {
		return fmt.Sprintf("%s %s", a.Series.Title, a.Name)
	}
	return a.Name
}

// Series represents a series (collection) in Feral File
type Series struct {
	Title       string `json:"title"`
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

// URL returns the full URL for a given URI
func URL(uri string) string {
	if uri == "" {
		return ""
	}

	// If the URI is a Cloudflare Images URL and does not have a variant, return the XL variant as default
	cloudFlareImage, hasVariant := cloudflare.IsCloudflareImageURL(uri)
	if cloudFlareImage && !hasVariant {
		return fmt.Sprintf("%s/%s", uri, "xl")
	}

	// If the URI is a relative URI, return the full URL with the CDN
	url, _ := url.Parse(uri)
	if url.Scheme == "" && url.Host == "" {
		return fmt.Sprintf("%s/%s", CDN, strings.TrimPrefix(uri, "/"))
	}

	return uri
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
	if err := c.httpClient.GetAndUnmarshal(ctx, url, &response); err != nil {
		return nil, fmt.Errorf("failed to call Feral File API: %w", err)
	}

	return &response.Result, nil
}

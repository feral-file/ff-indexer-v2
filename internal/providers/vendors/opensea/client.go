package opensea

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/ratelimit"
)

const PROVIDER_NAME = "opensea"

var ErrNoAPIKey = errors.New("no API key provided")

// NFTMetadata represents the NFT metadata from OpenSea API
type NFTMetadata struct {
	Identifier   string  `json:"identifier"`
	Collection   string  `json:"collection"`
	Contract     string  `json:"contract"`
	Name         *string `json:"name"`
	Description  *string `json:"description"`
	ImageURL     *string `json:"image_url"`
	AnimationURL *string `json:"animation_url"`
	MetadataURL  *string `json:"metadata_url"`
	Traits       []Trait `json:"traits"`
}

// Trait represents a trait/attribute of an NFT
type Trait struct {
	TraitType   string      `json:"trait_type"`
	DisplayType *string     `json:"display_type"`
	MaxValue    interface{} `json:"max_value"`
	Value       interface{} `json:"value"`
}

// NFTResponse represents the response from OpenSea Get NFT endpoint
type NFTResponse struct {
	NFT    NFTMetadata `json:"nft"`
	Errors []string    `json:"errors,omitempty"`
}

// Client defines the interface for OpenSea client operations to enable mocking
//
//go:generate mockgen -source=client.go -destination=../../../mocks/opensea_client.go -package=mocks -mock_names=Client=MockOpenSeaClient
type Client interface {
	// GetNFT fetches NFT metadata from OpenSea API v2
	GetNFT(ctx context.Context, contractAddress, tokenID string) (*NFTMetadata, error)
}

// OpenSeaClient implements OpenSea client
type OpenSeaClient struct {
	httpClient     adapter.HTTPClient
	rateLimitProxy ratelimit.Proxy
	apiURL         string
	apiKey         string
	json           adapter.JSON
}

// NewClient creates a new OpenSea client
func NewClient(httpClient adapter.HTTPClient, rateLimitProxy ratelimit.Proxy, apiURL string, apiKey string, json adapter.JSON) Client {
	return &OpenSeaClient{
		httpClient:     httpClient,
		rateLimitProxy: rateLimitProxy,
		apiURL:         apiURL,
		apiKey:         apiKey,
		json:           json,
	}
}

// GetNFT fetches NFT metadata from OpenSea API v2
func (c *OpenSeaClient) GetNFT(ctx context.Context, contractAddress, tokenID string) (*NFTMetadata, error) {
	if c.apiKey == "" {
		return nil, ErrNoAPIKey
	}

	// Build the API URL
	url := fmt.Sprintf("%s/chain/%s/contract/%s/nfts/%s",
		c.apiURL,
		"ethereum",
		strings.ToLower(contractAddress),
		tokenID,
	)

	// Make the request with API key header
	headers := map[string]string{
		"X-API-KEY": c.apiKey,
	}

	respBody, err := ratelimit.Request(ctx, c.rateLimitProxy, PROVIDER_NAME, func(ctx context.Context) ([]byte, error) {
		return c.httpClient.GetBytes(ctx, url, headers)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to call OpenSea API: %w", err)
	}

	var response NFTResponse
	if err := c.json.Unmarshal(respBody, &response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal OpenSea response: %w", err)
	}

	if len(response.Errors) > 0 {
		return nil, fmt.Errorf("OpenSea API errors: %v", response.Errors)
	}

	return &response.NFT, nil
}

// ExtractArtistFromTraits attempts to extract artist name from traits
// This follows a similar pattern to metadata resolution where we look for
// traits like "artist", "creator", "Artist Name", etc.
func ExtractArtistFromTraits(traits []Trait) string {
	// Common trait names that might contain artist information
	artistTraitNames := []string{
		"artist",
		"artists",
		"creator",
		"artist name",
		"creator name",
		"made by",
		"created by",
	}

	var artistNames []string
	var seenArtistNames = make(map[string]bool)
	for _, trait := range traits {
		traitTypeLower := strings.ToLower(trait.TraitType)
		for _, artistTraitName := range artistTraitNames {
			if traitTypeLower == artistTraitName {
				// Convert value to string
				if strValue, ok := trait.Value.(string); ok && strValue != "" {
					if !seenArtistNames[strValue] {
						artistNames = append(artistNames, strValue)
						seenArtistNames[strValue] = true
					}
				}
			}
		}
	}

	if len(artistNames) == 0 {
		return ""
	}

	return strings.TrimSuffix(strings.Join(artistNames, ", "), ", ")
}

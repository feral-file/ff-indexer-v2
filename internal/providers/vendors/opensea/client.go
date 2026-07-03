package opensea

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/ratelimit"
)

const PROVIDER_NAME = "opensea"

var (
	// ErrNoAPIKey is returned when no API key is configured.
	ErrNoAPIKey = errors.New("no API key provided")
	// ErrCollectionNotFound is returned when a collection slug does not resolve to a known collection.
	ErrCollectionNotFound = errors.New("opensea collection not found")
)

// editionNumberRe matches the "#<digits>" pattern commonly used in generative art NFT names
// (e.g. "Fidenza #78", "Chromie Squiggle #100").
var editionNumberRe = regexp.MustCompile(`#(\d+)`)

// NFTMetadata represents the NFT metadata from OpenSea API
type NFTMetadata struct {
	Identifier          string  `json:"identifier"`
	Collection          string  `json:"collection"`
	Contract            string  `json:"contract"`
	Name                *string `json:"name"`
	Description         *string `json:"description"`
	ImageURL            *string `json:"image_url"`
	DisplayAnimationURL *string `json:"display_animation_url"`
	MetadataURL         *string `json:"metadata_url"`
	Traits              []Trait `json:"traits"`
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

// CollectionContract represents a single contract associated with an OpenSea collection.
type CollectionContract struct {
	Address string `json:"address"`
	Chain   string `json:"chain"`
}

// CollectionMetadata represents the collection-level metadata from the OpenSea v2
// GET /api/v2/collections/{slug} endpoint.
//
// Design note: OpenSea does not have a separate numeric or UUID collection ID.
// The Collection field (the slug) is both the stable identifier and the URL slug.
// For this indexer, vendor_release_id = vendor_release_slug = collection slug.
type CollectionMetadata struct {
	// Collection is the slug AND the stable identifier (e.g. "boredapeyachtclub").
	Collection  string               `json:"collection"`
	Name        string               `json:"name"`
	Description string               `json:"description"`
	TotalSupply int64                `json:"total_supply"`
	Contracts   []CollectionContract `json:"contracts"`
}

// CollectionResponse is the top-level OpenSea collection API envelope.
// The collection fields are returned at the top level (not nested), so CollectionMetadata
// is embedded directly. Errors appear when the request fails (e.g. slug not found).
type CollectionResponse struct {
	CollectionMetadata
	Errors []string `json:"errors,omitempty"`
}

// Client defines the interface for OpenSea client operations to enable mocking
//
//go:generate mockgen -source=client.go -destination=../../../mocks/opensea_client.go -package=mocks -mock_names=Client=MockOpenSeaClient
type Client interface {
	// GetNFT fetches NFT metadata from OpenSea API v2
	GetNFT(ctx context.Context, contractAddress, tokenID string) (*NFTMetadata, error)

	// GetCollection fetches collection metadata from OpenSea API v2.
	// Returns ErrCollectionNotFound when the slug does not match any known collection.
	GetCollection(ctx context.Context, slug string) (*CollectionMetadata, error)

	// ResolveSlug validates that an OpenSea collection slug exists and returns it unchanged.
	//
	// For OpenSea, the slug IS the vendor_release_id — there is no separate stable numeric ID.
	// This method calls GetCollection to confirm the slug resolves to a real collection with
	// at least one associated contract, then returns the slug as-is.
	//
	// Returns ErrCollectionNotFound when the slug does not match any known collection.
	ResolveSlug(ctx context.Context, slug string) (string, error)
}

// OpenSeaClient implements OpenSea client
type OpenSeaClient struct {
	httpClient  adapter.HTTPClient
	rateLimiter ratelimit.Limiter
	apiURL      string
	apiKey      string
	json        adapter.JSON
}

// NewClient creates a new OpenSea client
func NewClient(httpClient adapter.HTTPClient, rateLimiter ratelimit.Limiter, apiURL string, apiKey string, json adapter.JSON) Client {
	return &OpenSeaClient{
		httpClient:  httpClient,
		rateLimiter: rateLimiter,
		apiURL:      apiURL,
		apiKey:      apiKey,
		json:        json,
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

	respBody, err := ratelimit.Do(ctx, c.rateLimiter, PROVIDER_NAME, func(ctx context.Context) ([]byte, error) {
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

// GetCollection fetches collection-level metadata from the OpenSea v2 API.
//
// Calls GET /api/v2/collections/{slug}. The slug is both the URL path parameter and
// the stable collection identifier — OpenSea has no separate numeric collection ID.
//
// Returns ErrCollectionNotFound when the slug does not resolve to a collection.
// Wraps ErrNoAPIKey when no key is configured.
func (c *OpenSeaClient) GetCollection(ctx context.Context, slug string) (*CollectionMetadata, error) {
	if c.apiKey == "" {
		return nil, ErrNoAPIKey
	}

	url := fmt.Sprintf("%s/collections/%s", c.apiURL, slug)
	headers := map[string]string{
		"X-API-KEY": c.apiKey,
	}

	respBody, err := ratelimit.Do(ctx, c.rateLimiter, PROVIDER_NAME, func(ctx context.Context) ([]byte, error) {
		return c.httpClient.GetBytes(ctx, url, headers)
	})
	if err != nil {
		// OpenSea returns HTTP 404 for unknown slugs; the HTTP client surfaces this as a
		// status-code error rather than a JSON body we can unmarshal.
		if isOpenSeaNotFoundError(err) {
			return nil, fmt.Errorf("opensea collection %q: %w", slug, ErrCollectionNotFound)
		}
		return nil, fmt.Errorf("failed to call OpenSea collections API: %w", err)
	}

	var response CollectionResponse
	if err := c.json.Unmarshal(respBody, &response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal OpenSea collection response: %w", err)
	}

	if len(response.Errors) > 0 {
		// OpenSea returns {"errors": ["Resource not found"]} for unknown slugs.
		for _, e := range response.Errors {
			if strings.Contains(strings.ToLower(e), "not found") {
				return nil, fmt.Errorf("opensea collection %q: %w", slug, ErrCollectionNotFound)
			}
		}
		return nil, fmt.Errorf("OpenSea collections API errors: %v", response.Errors)
	}

	// Guard against a successful HTTP response with an empty collection slug, which
	// would indicate an unexpected API schema change.
	if response.Collection == "" {
		return nil, fmt.Errorf("opensea collection %q: %w", slug, ErrCollectionNotFound)
	}

	meta := response.CollectionMetadata
	return &meta, nil
}

// ResolveSlug validates that an OpenSea collection slug exists and returns it unchanged.
//
// For OpenSea, the slug IS the vendor_release_id (no separate numeric ID exists).
// This method confirms the slug resolves to a real collection with at least one contract,
// then returns it as-is so the caller can use it as vendor_release_id.
func (c *OpenSeaClient) ResolveSlug(ctx context.Context, slug string) (string, error) {
	collection, err := c.GetCollection(ctx, slug)
	if err != nil {
		if errors.Is(err, ErrCollectionNotFound) {
			return "", fmt.Errorf("opensea slug not found: %q: %w", slug, ErrCollectionNotFound)
		}
		return "", fmt.Errorf("failed to resolve opensea slug %q: %w", slug, err)
	}

	if len(collection.Contracts) == 0 {
		return "", fmt.Errorf("opensea collection %q has no associated contracts", slug)
	}

	// The slug is stable and is the canonical vendor_release_id for OpenSea.
	return slug, nil
}

// isOpenSeaNotFoundError reports whether err represents an OpenSea "collection not found"
// response, either as HTTP 404 from the HTTP client or a JSON errors payload mentioning not found.
func isOpenSeaNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "unexpected status code 404") ||
		strings.Contains(msg, "not found")
}

// ReleaseMetadataFromCollection maps OpenSea collection metadata to release name and
// total_mints pointers suitable for store.UpsertRelease.
//
// total_mints is omitted when TotalSupply is zero or negative (DB CHECK requires
// total_mints IS NULL OR total_mints > 0).
func ReleaseMetadataFromCollection(collection *CollectionMetadata) (name *string, totalMints *int64) {
	if collection == nil {
		return nil, nil
	}
	if title := strings.TrimSpace(collection.Name); title != "" {
		name = &title
	}
	if collection.TotalSupply > 0 {
		total := collection.TotalSupply
		totalMints = &total
	}
	return name, totalMints
}

// ExtractMintNumber derives a mint number from an NFT's name or identifier.
//
// Strategy:
//  1. Try parsing "#<integer>" from name (e.g. "Fidenza #78" → 78). This is the most
//     reliable indicator for generative art collections that embed the edition in the name.
//  2. Fall back to parsing identifier (the on-chain token ID) as an integer.
//
// Returns (mintNumber, true) on success, (0, false) when both strategies fail.
// A return of (0, false) is unusual and indicates an NFT with a non-numeric identifier
// and no "#N" pattern in its name.
func ExtractMintNumber(name, identifier string) (int64, bool) {
	if name != "" {
		if m := editionNumberRe.FindStringSubmatch(name); len(m) == 2 {
			if n, err := strconv.ParseInt(m[1], 10, 64); err == nil {
				return n, true
			}
		}
	}
	if identifier != "" {
		if n, err := strconv.ParseInt(identifier, 10, 64); err == nil {
			return n, true
		}
	}
	return 0, false
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

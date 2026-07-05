package feralfile

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/providers/cloudflare"
)

// feralfileArtworksPageSize is the maximum artworks fetched per request in GetSeriesArtworks.
// The FF API backend accepts up to ~300; 100 is used conservatively.
const feralfileArtworksPageSize = 100

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

// ArtworkRef holds the resolved on-chain identity of a single Feral File artwork,
// used for CID derivation during release indexing.
//
// The top-level chain/contractAddress/tokenID fields in the FF API response are
// already resolved: for artworks that have completed a Bitmark→EVM/Tezos swap, they
// reflect the target chain identity. Artworks still on Bitmark have Chain=="bitmark"
// and no usable on-chain token ID for this indexer.
type ArtworkRef struct {
	// Index is the 0-based edition number within the series (artwork.index in FF API).
	// MintNumber = *Index + 1. Nil means the API omitted the index field, which indicates
	// unknown release membership; callers must skip artworks with a nil Index rather than
	// treating them as mint number 1 (which would be the result of Index==0).
	Index *int64
	// Chain is the resolved chain string: "ethereum", "tezos", or "bitmark".
	Chain string
	// ContractAddress is the on-chain contract (swapped contract if swap is complete,
	// otherwise the original mint contract). Empty for unswapped Bitmark artworks.
	ContractAddress string
	// TokenID is the on-chain token identifier (swapped token ID if swap is complete,
	// otherwise the Bitmark artwork ID string).
	TokenID string
}

// artworkListResponse is the paginated artwork list envelope from the FF API.
type artworkListResponse struct {
	Result []artworkRefRaw `json:"result"`
}

// artworkRefRaw captures only the fields needed for CID derivation from each artwork
// in the FF artworks list response.
type artworkRefRaw struct {
	Index           *int64 `json:"index"`
	Chain           string `json:"chain"`
	ContractAddress string `json:"contractAddress"`
	TokenID         string `json:"tokenID"`
}

// Artwork represents an artwork from Feral File API.
// Index is a pointer so that a missing "index" key in JSON can be distinguished
// from an explicit zero value. Callers must check for nil before using Index to
// compute mint_number; an absent index means release membership cannot be determined.
type Artwork struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	ThumbnailURI string `json:"thumbnailURI"`
	PreviewURI   string `json:"previewURI"`
	Series       Series `json:"series"`
	SeriesID     string `json:"seriesID"`
	Index        *int64 `json:"index"`
}

// SeriesIDOrFallback returns the stable FF series identifier.
// Prefer the artwork-level seriesID; fall back to the nested series ID when present.
func (a *Artwork) SeriesIDOrFallback() string {
	if a.SeriesID != "" {
		return a.SeriesID
	}
	return a.Series.ID
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
	ID    string `json:"ID"`
	Title string `json:"title"`
	// Slug is the URL slug for this series on the Feral File website
	// (e.g. "data-pilgrims-01-769"). Populated when the artwork response
	// includes the nested series object.
	Slug        string         `json:"slug"`
	Description string         `json:"description"`
	Medium      string         `json:"medium"`
	Settings    SeriesSettings `json:"settings"`
	Artist      Artist         `json:"artist"`
}

// SeriesSettings holds edition-size settings for a FF series.
type SeriesSettings struct {
	MaxArtwork int64 `json:"maxArtwork"`
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

// seriesListResponse is the paginated series list response from the FF API.
type seriesListResponse struct {
	Result []struct {
		ID   string `json:"id"`
		Slug string `json:"slug"`
	} `json:"result"`
}

// Client defines the interface for Feral File client operations to enable mocking
//
//go:generate mockgen -source=client.go -destination=../../../mocks/feralfile_client.go -package=mocks -mock_names=Client=MockFeralFileClient
type Client interface {
	// GetArtwork fetches artwork data from Feral File API
	GetArtwork(ctx context.Context, tokenID string) (*Artwork, error)

	// GetSeriesArtworks fetches artworks for a FF series in edition-index (mint) order.
	// mintFrom and mintTo are 1-based mint numbers (inclusive); the method translates
	// these to the 0-based artwork.index offsets used by the FF API.
	//
	// The FF API does not have a dedicated series artworks endpoint; artworks are
	// fetched via GET /artworks?seriesID={uuid}&sortBy=index&sortOrder=ASC.
	// This method paginates internally so callers always receive the full range.
	//
	// Artworks originally minted on Bitmark that have not yet been swapped to
	// EVM/Tezos are included in the result with Chain=="bitmark". Callers must
	// check Chain before building a CID and skip Bitmark-origin artworks.
	GetSeriesArtworks(ctx context.Context, seriesID string, mintFrom, mintTo int64) ([]ArtworkRef, error)

	// ResolveSlug resolves a Feral File series URL slug (e.g. "data-pilgrims-01-769")
	// to the series UUID used as vendor_release_id.
	// Returns an error when no matching series is found.
	ResolveSlug(ctx context.Context, slug string) (string, error)
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

// GetSeriesArtworks fetches artworks for a FF series in ascending edition-index order,
// paginating internally with feralfileArtworksPageSize.
//
// Reason: the FF API returns artworks sorted by index (0-based edition number) via
// GET /artworks?seriesID=...&sortBy=index&sortOrder=ASC. mintFrom/mintTo (1-based)
// translate to offset=(mintFrom-1) and limit=(mintTo-mintFrom+1) in the query. When
// the range spans multiple pages, subsequent requests advance the offset until all
// artworks in the range are collected.
//
// Trade-off: synchronous API calls during job execution add latency but are bounded
// by the mint range cap enforced at the API layer (max 10,000). For typical releases
// (≤1000 editions), this is at most 10 requests of 100 artworks each.
func (c *FeralFileClient) GetSeriesArtworks(ctx context.Context, seriesID string, mintFrom, mintTo int64) ([]ArtworkRef, error) {
	// The FF API uses 0-based artwork.index; mintFrom-1 gives the first index to fetch.
	totalNeeded := mintTo - mintFrom + 1
	apiOffset := mintFrom - 1

	var all []ArtworkRef

	for int64(len(all)) < totalNeeded {
		remaining := totalNeeded - int64(len(all))
		pageLimit := int64(feralfileArtworksPageSize)
		if remaining < pageLimit {
			pageLimit = remaining
		}

		// url.QueryEscape ensures reserved characters in the caller-supplied seriesID
		// (e.g. &, =, %) are percent-encoded and cannot alter the outbound query string.
		apiURL := fmt.Sprintf("%s/artworks?seriesID=%s&sortBy=index&sortOrder=ASC&offset=%d&limit=%d",
			c.apiBaseURL, url.QueryEscape(seriesID), apiOffset+int64(len(all)), pageLimit)

		var resp artworkListResponse
		if err := c.httpClient.GetAndUnmarshal(ctx, apiURL, &resp); err != nil {
			return nil, fmt.Errorf("failed to call Feral File artworks API: %w", err)
		}

		if len(resp.Result) == 0 {
			// No more artworks available (range exceeds total supply).
			break
		}

		for _, raw := range resp.Result {
			// Preserve nil: a missing index means release membership is unknown.
			// Callers must skip artworks with Index==nil rather than treating them
			// as mint 1 (which would happen if nil were coerced to 0).
			all = append(all, ArtworkRef(raw))
		}

		if int64(len(resp.Result)) < pageLimit {
			// Partial page — we have reached the end of available artworks.
			break
		}
	}

	return all, nil
}

// ResolveSlug resolves a Feral File series URL slug to the series UUID used as vendor_release_id.
//
// Queries GET /api/series?slug={slug}&limit=1 to map the human-readable URL slug to the
// stable series UUID. The UUID is used internally as vendor_release_id because it is stable
// across slug renames.
func (c *FeralFileClient) ResolveSlug(ctx context.Context, slug string) (string, error) {
	// Use url.QueryEscape so reserved characters in caller-supplied slugs (e.g. &, =, %)
	// cannot alter the query string structure of the outbound vendor request.
	apiURL := fmt.Sprintf("%s/series?slug=%s&limit=1", c.apiBaseURL, url.QueryEscape(slug))

	var resp seriesListResponse
	if err := c.httpClient.GetAndUnmarshal(ctx, apiURL, &resp); err != nil {
		return "", fmt.Errorf("failed to call Feral File series API: %w", err)
	}

	if len(resp.Result) == 0 {
		return "", fmt.Errorf("feral file series slug not found: %q", slug)
	}

	return resp.Result[0].ID, nil
}

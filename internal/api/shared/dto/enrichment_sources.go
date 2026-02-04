package dto

import (
	"encoding/json"
	"time"

	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

// EnrichmentSourceResponse represents enrichment source data from vendor APIs
type EnrichmentSourceResponse struct {
	TokenID      uint64           `json:"token_id"`
	Vendor       string           `json:"vendor"`
	VendorJSON   json.RawMessage  `json:"vendor_json,omitempty"`
	VendorHash   *string          `json:"vendor_hash,omitempty"`
	ImageURL     *string          `json:"image_url,omitempty"`
	AnimationURL *string          `json:"animation_url,omitempty"`
	Name         *string          `json:"name,omitempty"`
	Description  *string          `json:"description,omitempty"`
	Artists      []ArtistResponse `json:"artists,omitempty"`
	MimeType     *string          `json:"mime_type,omitempty"`
	CreatedAt    time.Time        `json:"created_at"`
	UpdatedAt    time.Time        `json:"updated_at"`
}

// MediaURLs returns the media URLs from the enrichment source
func (e *EnrichmentSourceResponse) MediaURLs() []string {
	var urls []string
	if e.ImageURL != nil && *e.ImageURL != "" {
		urls = append(urls, *e.ImageURL)
	}
	if e.AnimationURL != nil && *e.AnimationURL != "" {
		urls = append(urls, *e.AnimationURL)
	}
	return urls
}

// MapEnrichmentSourceToDTO maps a schema.EnrichmentSource to EnrichmentSourceResponse
func MapEnrichmentSourceToDTO(enrichment *schema.EnrichmentSource) *EnrichmentSourceResponse {
	if enrichment == nil {
		return nil
	}

	var artists []ArtistResponse
	for _, artist := range enrichment.Artists {
		artists = append(artists, ArtistResponse{
			DID:  artist.DID.String(),
			Name: artist.Name,
		})
	}

	return &EnrichmentSourceResponse{
		TokenID:      enrichment.TokenID,
		Vendor:       string(enrichment.Vendor),
		VendorJSON:   json.RawMessage(enrichment.VendorJSON),
		VendorHash:   enrichment.VendorHash,
		ImageURL:     enrichment.ImageURL,
		AnimationURL: enrichment.AnimationURL,
		Name:         enrichment.Name,
		Description:  enrichment.Description,
		Artists:      artists,
		MimeType:     enrichment.MimeType,
		CreatedAt:    enrichment.CreatedAt,
		UpdatedAt:    enrichment.UpdatedAt,
	}
}

package dto

import (
	"encoding/json"
	"time"

	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

// ArtistResponse represents an artist/creator in the API response
type ArtistResponse struct {
	DID  string `json:"did,omitempty"`
	Name string `json:"name,omitempty"`
}

// PublisherResponse represents the publisher of the token
type PublisherResponse struct {
	Name *string `json:"name,omitempty"`
	URL  *string `json:"url,omitempty"`
}

// TokenMetadataResponse represents token metadata
type TokenMetadataResponse struct {
	TokenID         uint64             `json:"token_id"`
	OriginJSON      json.RawMessage    `json:"origin_json,omitempty"`
	LatestJSON      json.RawMessage    `json:"latest_json,omitempty"`
	LatestHash      *string            `json:"latest_hash,omitempty"`
	EnrichmentLevel string             `json:"enrichment_level"`
	LastRefreshedAt *time.Time         `json:"last_refreshed_at,omitempty"`
	ImageURL        *string            `json:"image_url,omitempty"`
	AnimationURL    *string            `json:"animation_url,omitempty"`
	Name            *string            `json:"name,omitempty"`
	Description     *string            `json:"description,omitempty"`
	Artists         []ArtistResponse   `json:"artists,omitempty"`
	Publisher       *PublisherResponse `json:"publisher,omitempty"`
}

// MapTokenMetadataToDTO maps a schema.TokenMetadata to TokenMetadataResponse
func MapTokenMetadataToDTO(metadata *schema.TokenMetadata) *TokenMetadataResponse {
	// Convert schema.Artists to ArtistResponse
	var artists []ArtistResponse
	for _, artist := range metadata.Artists {
		artists = append(artists, ArtistResponse{
			DID:  artist.DID.String(),
			Name: artist.Name,
		})
	}

	var publisher *PublisherResponse
	if metadata.Publisher != nil {
		publisher = &PublisherResponse{
			Name: metadata.Publisher.Name,
			URL:  metadata.Publisher.URL,
		}
	}

	return &TokenMetadataResponse{
		TokenID:         metadata.TokenID,
		OriginJSON:      json.RawMessage(metadata.OriginJSON),
		LatestJSON:      json.RawMessage(metadata.LatestJSON),
		LatestHash:      metadata.LatestHash,
		EnrichmentLevel: string(metadata.EnrichmentLevel),
		LastRefreshedAt: metadata.LastRefreshedAt,
		ImageURL:        metadata.ImageURL,
		AnimationURL:    metadata.AnimationURL,
		Name:            metadata.Name,
		Description:     metadata.Description,
		Artists:         artists,
		Publisher:       publisher,
	}
}

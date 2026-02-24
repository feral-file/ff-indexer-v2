package dto

import (
	"encoding/json"
	"time"

	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

// MediaAssetResponse represents a media asset reference
type MediaAssetResponse struct {
	ID               int64           `json:"id"`
	SourceURL        string          `json:"source_url"`
	MimeType         *string         `json:"mime_type,omitempty"`
	FileSizeBytes    *int64          `json:"file_size_bytes,omitempty"`
	Provider         string          `json:"provider"`
	ProviderAssetID  *string         `json:"provider_asset_id,omitempty"`
	ProviderMetadata json.RawMessage `json:"provider_metadata,omitempty"`
	VariantURLs      json.RawMessage `json:"variant_urls"`
	CreatedAt        time.Time       `json:"created_at"`
	UpdatedAt        time.Time       `json:"updated_at"`
}

// MapMediaAssetToDTO maps a schema.MediaAsset to MediaAssetResponse
func MapMediaAssetToDTO(media *schema.MediaAsset) *MediaAssetResponse {
	dto := &MediaAssetResponse{
		ID:              media.ID,
		SourceURL:       media.SourceURL,
		MimeType:        media.MimeType,
		FileSizeBytes:   media.FileSizeBytes,
		Provider:        string(media.Provider),
		ProviderAssetID: media.ProviderAssetID,
		CreatedAt:       media.CreatedAt,
		UpdatedAt:       media.UpdatedAt,
	}

	if media.ProviderMetadata != nil {
		dto.ProviderMetadata = json.RawMessage(media.ProviderMetadata)
	}

	if media.VariantURLs != nil {
		dto.VariantURLs = json.RawMessage(media.VariantURLs)
	}

	return dto
}

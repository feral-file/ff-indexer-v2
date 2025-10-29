package dto

import (
	"encoding/json"
	"time"

	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

// MediaAssetResponse represents a media asset
type MediaAssetResponse struct {
	ID            int64           `json:"id"`
	TokenID       uint64          `json:"token_id"`
	Role          string          `json:"role"`
	SourceURL     *string         `json:"source_url,omitempty"`
	ContentHash   *string         `json:"content_hash,omitempty"`
	CFImageID     *string         `json:"cf_image_id,omitempty"`
	CFVariantMap  json.RawMessage `json:"cf_variant_map,omitempty"`
	Status        string          `json:"status"`
	LastCheckedAt *time.Time      `json:"last_checked_at,omitempty"`
	CreatedAt     time.Time       `json:"created_at"`
	UpdatedAt     time.Time       `json:"updated_at"`
}

// MapMediaAssetToDTO maps a schema.MediaAsset to MediaAssetResponse
func MapMediaAssetToDTO(media *schema.MediaAsset) *MediaAssetResponse {
	dto := &MediaAssetResponse{
		ID:            media.ID,
		TokenID:       media.TokenID,
		Role:          string(media.Role),
		SourceURL:     media.SourceURL,
		ContentHash:   media.ContentHash,
		CFImageID:     media.CFImageID,
		Status:        string(media.Status),
		LastCheckedAt: media.LastCheckedAt,
		CreatedAt:     media.CreatedAt,
		UpdatedAt:     media.UpdatedAt,
	}

	if media.CFVariantMap != nil {
		dto.CFVariantMap = json.RawMessage(media.CFVariantMap)
	}

	return dto
}

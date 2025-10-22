package dto

import (
	"encoding/json"
	"time"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

// TokenResponse represents a token with optional expansions
type TokenResponse struct {
	TokenCID         string               `json:"token_cid"`
	Chain            domain.Chain         `json:"chain"`
	Standard         domain.ChainStandard `json:"standard"`
	ContractAddress  string               `json:"contract_address"`
	TokenNumber      string               `json:"token_number"`
	CurrentOwner     *string              `json:"current_owner"`
	Burned           bool                 `json:"burned"`
	LastActivityTime time.Time            `json:"last_activity_time"`
	CreatedAt        time.Time            `json:"created_at"`
	UpdatedAt        time.Time            `json:"updated_at"`

	// Metadata (always included when available)
	Metadata *TokenMetadataResponse `json:"metadata,omitempty"`

	// Expansions
	Owners           *PaginatedOwners           `json:"owners,omitempty"`
	ProvenanceEvents *PaginatedProvenanceEvents `json:"provenance_events,omitempty"`
}

// TokenMetadataResponse represents token metadata
type TokenMetadataResponse struct {
	OriginJSON      json.RawMessage `json:"origin_json,omitempty"`
	LatestJSON      json.RawMessage `json:"latest_json,omitempty"`
	LatestHash      *string         `json:"latest_hash,omitempty"`
	EnrichmentLevel string          `json:"enrichment_level"`
	LastRefreshedAt *time.Time      `json:"last_refreshed_at,omitempty"`
	ImageURL        *string         `json:"image_url,omitempty"`
	AnimationURL    *string         `json:"animation_url,omitempty"`
	Name            *string         `json:"name,omitempty"`
	Artists         []string        `json:"artists,omitempty"`
}

// OwnerResponse represents a token owner (balance record)
type OwnerResponse struct {
	OwnerAddress string    `json:"owner_address"`
	Quantity     string    `json:"quantity"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// ProvenanceEventResponse represents a provenance event
type ProvenanceEventResponse struct {
	ID          uint64                     `json:"id"`
	Chain       domain.Chain               `json:"chain"`
	EventType   schema.ProvenanceEventType `json:"event_type"`
	FromAddress *string                    `json:"from_address,omitempty"`
	ToAddress   *string                    `json:"to_address,omitempty"`
	Quantity    *string                    `json:"quantity,omitempty"`
	TxHash      *string                    `json:"tx_hash,omitempty"`
	BlockNumber *uint64                    `json:"block_number,omitempty"`
	BlockHash   *string                    `json:"block_hash,omitempty"`
	Timestamp   time.Time                  `json:"timestamp"`
	Raw         json.RawMessage            `json:"raw,omitempty"`
}

// PaginatedOwners represents paginated owners
type PaginatedOwners struct {
	Owners []OwnerResponse `json:"items"`
	Offset *int            `json:"offset,omitempty"`
	Total  uint64          `json:"total"`
}

// PaginatedProvenanceEvents represents paginated provenance events
type PaginatedProvenanceEvents struct {
	Events []ProvenanceEventResponse `json:"items"`
	Offset *int                      `json:"offset,omitempty"`
	Total  uint64                    `json:"total"`
}

// TokenListResponse represents a paginated list of tokens
type TokenListResponse struct {
	Tokens []TokenResponse `json:"items"`
	Offset *int            `json:"offset,omitempty"`
	Total  uint64          `json:"total"`
}

// MapTokenToDTO maps a schema.Token to TokenResponse
func MapTokenToDTO(token *schema.Token, metadata *schema.TokenMetadata) *TokenResponse {
	dto := &TokenResponse{
		TokenCID:         token.TokenCID,
		Chain:            token.Chain,
		Standard:         token.Standard,
		ContractAddress:  token.ContractAddress,
		TokenNumber:      token.TokenNumber,
		CurrentOwner:     token.CurrentOwner,
		Burned:           token.Burned,
		LastActivityTime: token.LastActivityTime,
		CreatedAt:        token.CreatedAt,
		UpdatedAt:        token.UpdatedAt,
	}

	if metadata != nil {
		dto.Metadata = MapTokenMetadataToDTO(metadata)
	}

	return dto
}

// MapTokenMetadataToDTO maps a schema.TokenMetadata to TokenMetadataResponse
func MapTokenMetadataToDTO(metadata *schema.TokenMetadata) *TokenMetadataResponse {
	return &TokenMetadataResponse{
		OriginJSON:      json.RawMessage(metadata.OriginJSON),
		LatestJSON:      json.RawMessage(metadata.LatestJSON),
		LatestHash:      metadata.LatestHash,
		EnrichmentLevel: string(metadata.EnrichmentLevel),
		LastRefreshedAt: metadata.LastRefreshedAt,
		ImageURL:        metadata.ImageURL,
		AnimationURL:    metadata.AnimationURL,
		Name:            metadata.Name,
		Artists:         metadata.Artists,
	}
}

// MapOwnerToDTO maps a schema.Balance to OwnerResponse
func MapOwnerToDTO(balance *schema.Balance) *OwnerResponse {
	return &OwnerResponse{
		OwnerAddress: balance.OwnerAddress,
		Quantity:     balance.Quantity,
		UpdatedAt:    balance.UpdatedAt,
	}
}

// MapProvenanceEventToDTO maps a schema.ProvenanceEvent to ProvenanceEventResponse
func MapProvenanceEventToDTO(event *schema.ProvenanceEvent) *ProvenanceEventResponse {
	return &ProvenanceEventResponse{
		ID:          event.ID,
		Chain:       event.Chain,
		EventType:   event.EventType,
		FromAddress: event.FromAddress,
		ToAddress:   event.ToAddress,
		Quantity:    event.Quantity,
		TxHash:      event.TxHash,
		BlockNumber: event.BlockNumber,
		BlockHash:   event.BlockHash,
		Timestamp:   event.Timestamp,
		Raw:         json.RawMessage(event.Raw),
	}
}

// ChangeResponse represents a change journal entry
type ChangeResponse struct {
	ID          uint64             `json:"id"`
	TokenCID    string             `json:"token_cid"`
	SubjectType schema.SubjectType `json:"subject_type"`
	SubjectID   string             `json:"subject_id"`
	ChangedAt   time.Time          `json:"changed_at"`
	Meta        json.RawMessage    `json:"meta,omitempty"`

	// Expansion
	Subject interface{} `json:"subject,omitempty"` // Expanded subject based on subject_type
}

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

// ChangeListResponse represents a paginated list of changes
type ChangeListResponse struct {
	Changes []ChangeResponse `json:"items"`
	Offset  *int             `json:"offset,omitempty"` // Offset for the next page
	Total   uint64           `json:"total"`
}

// MapChangeToDTO maps a schema.ChangesJournal to ChangeResponse
func MapChangeToDTO(change *schema.ChangesJournal, token *schema.Token) *ChangeResponse {
	dto := &ChangeResponse{
		ID:          change.ID,
		TokenCID:    token.TokenCID,
		SubjectType: change.SubjectType,
		SubjectID:   change.SubjectID,
		ChangedAt:   change.ChangedAt,
	}

	if change.Meta != nil {
		dto.Meta = json.RawMessage(change.Meta)
	}

	return dto
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

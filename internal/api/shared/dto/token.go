package dto

import (
	"time"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

// TokenResponse represents a token with optional expansions
type TokenResponse struct {
	ID              uint64               `json:"id"`
	TokenCID        string               `json:"token_cid"`
	Chain           domain.Chain         `json:"chain"`
	Standard        domain.ChainStandard `json:"standard"`
	ContractAddress string               `json:"contract_address"`
	TokenNumber     string               `json:"token_number"`
	CurrentOwner    *string              `json:"current_owner"`
	Burned          bool                 `json:"burned"`
	CreatedAt       time.Time            `json:"created_at"`
	UpdatedAt       time.Time            `json:"updated_at"`

	// Metadata (always included when available)
	Metadata *TokenMetadataResponse `json:"metadata,omitempty"`

	// Expansions
	Owners                      *PaginatedOwners           `json:"owners,omitempty"`
	ProvenanceEvents            *PaginatedProvenanceEvents `json:"provenance_events,omitempty"`
	EnrichmentSource            *EnrichmentSourceResponse  `json:"enrichment_source,omitempty"`
	MetadataMediaAssets         []MediaAssetResponse       `json:"metadata_media_assets,omitempty"`
	EnrichmentSourceMediaAssets []MediaAssetResponse       `json:"enrichment_source_media_assets,omitempty"`
}

// OwnerResponse represents a token owner (balance record)
type OwnerResponse struct {
	OwnerAddress string    `json:"owner_address"`
	Quantity     string    `json:"quantity"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// PaginatedOwners represents paginated owners
type PaginatedOwners struct {
	Owners []OwnerResponse `json:"items"`
	Offset *uint64         `json:"offset,omitempty"`
	Total  uint64          `json:"total"`
}

// TokenListResponse represents a paginated list of tokens
type TokenListResponse struct {
	Tokens []TokenResponse `json:"items"`
	Offset *uint64         `json:"offset,omitempty"`
	Total  uint64          `json:"total"`
}

// MapTokenToDTO maps a schema.Token to TokenResponse
func MapTokenToDTO(token *schema.Token, metadata *schema.TokenMetadata) *TokenResponse {
	dto := &TokenResponse{
		ID:              token.ID,
		TokenCID:        token.TokenCID,
		Chain:           token.Chain,
		Standard:        token.Standard,
		ContractAddress: token.ContractAddress,
		TokenNumber:     token.TokenNumber,
		CurrentOwner:    token.CurrentOwner,
		Burned:          token.Burned,
		CreatedAt:       token.CreatedAt,
		UpdatedAt:       token.UpdatedAt,
	}

	if metadata != nil {
		dto.Metadata = MapTokenMetadataToDTO(metadata)
	}

	return dto
}

// MapOwnerToDTO maps a schema.Balance to OwnerResponse
func MapOwnerToDTO(balance *schema.Balance) *OwnerResponse {
	return &OwnerResponse{
		OwnerAddress: balance.OwnerAddress,
		Quantity:     balance.Quantity,
		CreatedAt:    balance.CreatedAt,
		UpdatedAt:    balance.UpdatedAt,
	}
}

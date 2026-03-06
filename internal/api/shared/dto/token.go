package dto

import (
	"time"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

// TokenResponse represents a token with optional expansions
type TokenResponse struct {
	ID                      uint64               `json:"id"`
	TokenCID                string               `json:"token_cid"`
	Chain                   domain.Chain         `json:"chain"`
	Standard                domain.ChainStandard `json:"standard"`
	ContractAddress         string               `json:"contract_address"`
	TokenNumber             string               `json:"token_number"`
	CurrentOwner            *string              `json:"current_owner"`
	Burned                  bool                 `json:"burned"`
	Viewable                bool                 `json:"viewable"`
	LastProvenanceTimestamp *time.Time           `json:"last_provenance_timestamp"`
	CreatedAt               time.Time            `json:"created_at"`
	UpdatedAt               time.Time            `json:"updated_at"`

	// Expansions
	Display          *TokenDisplayResponse      `json:"display,omitempty"`
	Metadata         *TokenMetadataResponse     `json:"metadata,omitempty"`
	Owners           *PaginatedOwners           `json:"owners,omitempty"`
	ProvenanceEvents *PaginatedProvenanceEvents `json:"provenance_events,omitempty"`
	OwnerProvenances *PaginatedOwnerProvenances `json:"owner_provenances,omitempty"`
	EnrichmentSource *EnrichmentSourceResponse  `json:"enrichment_source,omitempty"`
	// Deprecated: Use MediaAssets instead
	MetadataMediaAssets []MediaAssetResponse `json:"metadata_media_assets,omitempty"`
	// Deprecated: Use MediaAssets instead
	EnrichmentSourceMediaAssets []MediaAssetResponse `json:"enrichment_source_media_assets,omitempty"`
	// MediaAssets contains all media assets from both metadata and enrichment sources
	MediaAssets []MediaAssetResponse `json:"media_assets,omitempty"`
}

// OwnerResponse represents a token owner (balance record)
type OwnerResponse struct {
	OwnerAddress string    `json:"owner_address"`
	Quantity     string    `json:"quantity"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// OwnerProvenanceResponse represents the latest provenance event for a specific owner
type OwnerProvenanceResponse struct {
	OwnerAddress  string    `json:"owner_address"`
	LastTimestamp time.Time `json:"last_timestamp"`
	LastTxIndex   int64     `json:"last_tx_index"`
	LastEventType string    `json:"last_event_type"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// PaginatedOwners represents paginated owners
type PaginatedOwners struct {
	Owners []OwnerResponse `json:"items"`
	Offset *uint64         `json:"offset,omitempty"`
	Total  uint64          `json:"total"`
}

// PaginatedOwnerProvenances represents paginated owner provenances
type PaginatedOwnerProvenances struct {
	OwnerProvenances []OwnerProvenanceResponse `json:"items"`
	Offset           *uint64                   `json:"offset,omitempty"`
	Total            uint64                    `json:"total"`
}

// TokenListResponse represents a paginated list of tokens
type TokenListResponse struct {
	Tokens []TokenResponse `json:"items"`
	Offset *uint64         `json:"offset,omitempty"`
	Total  uint64          `json:"total"`
}

// MapTokenToDTO maps a schema.Token to TokenResponse
func MapTokenToDTO(token *schema.Token) *TokenResponse {
	if token == nil {
		return nil
	}

	return &TokenResponse{
		ID:                      token.ID,
		TokenCID:                token.TokenCID,
		Chain:                   token.Chain,
		Standard:                token.Standard,
		ContractAddress:         token.ContractAddress,
		TokenNumber:             token.TokenNumber,
		CurrentOwner:            token.CurrentOwner,
		Burned:                  token.Burned,
		Viewable:                token.IsViewable,
		LastProvenanceTimestamp: token.LastProvenanceTimestamp,
		CreatedAt:               token.CreatedAt,
		UpdatedAt:               token.UpdatedAt,
	}
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

// MapOwnerProvenanceToDTO maps a schema.TokenOwnershipProvenance to OwnerProvenanceResponse
func MapOwnerProvenanceToDTO(provenance *schema.TokenOwnershipProvenance) *OwnerProvenanceResponse {
	return &OwnerProvenanceResponse{
		OwnerAddress:  provenance.OwnerAddress,
		LastTimestamp: provenance.LastTimestamp,
		LastTxIndex:   provenance.LastTxIndex,
		LastEventType: string(provenance.LastEventType),
		CreatedAt:     provenance.CreatedAt,
		UpdatedAt:     provenance.UpdatedAt,
	}
}

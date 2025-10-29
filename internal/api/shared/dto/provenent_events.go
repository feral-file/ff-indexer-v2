package dto

import (
	"encoding/json"
	"time"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

// ProvenanceEventResponse represents a provenance event
type ProvenanceEventResponse struct {
	ID          uint64                     `json:"id"`
	TokenID     uint64                     `json:"token_id"`
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
	CreatedAt   time.Time                  `json:"created_at"`
	UpdatedAt   time.Time                  `json:"updated_at"`
}

// PaginatedProvenanceEvents represents paginated provenance events
type PaginatedProvenanceEvents struct {
	Events []ProvenanceEventResponse `json:"items"`
	Offset *uint64                   `json:"offset,omitempty"`
	Total  uint64                    `json:"total"`
}

// MapProvenanceEventToDTO maps a schema.ProvenanceEvent to ProvenanceEventResponse
func MapProvenanceEventToDTO(event *schema.ProvenanceEvent) *ProvenanceEventResponse {
	return &ProvenanceEventResponse{
		ID:          event.ID,
		TokenID:     event.TokenID,
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
		CreatedAt:   event.CreatedAt,
		UpdatedAt:   event.UpdatedAt,
	}
}

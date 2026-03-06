package dto

import (
	"encoding/json"
	"time"

	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

// TokenEvent represents a single token event
type TokenEvent struct {
	// ID: event ID
	ID uint64 `json:"id"`

	// TokenID: token ID
	TokenID uint64 `json:"token_id"`

	// EventType: acquired, released, metadata_updated, enrichment_updated, viewability_changed
	EventType string `json:"event_type"`

	// OwnerAddress: owner address (NULL for attribute events that broadcast to all current owners)
	OwnerAddress *string `json:"owner_address"`

	// OccurredAt: when the event occurred
	OccurredAt time.Time `json:"occurred_at"`

	// Metadata: lightweight metadata (JSONB)
	Metadata json.RawMessage `json:"metadata,omitempty"`
}

// MapTokenEventToDTO maps a schema.TokenEvent to TokenEvent
func MapTokenEventToDTO(event *schema.TokenEvent) *TokenEvent {
	if event == nil {
		return nil
	}

	return &TokenEvent{
		ID:           event.ID,
		TokenID:      event.TokenID,
		EventType:    string(event.EventType),
		OwnerAddress: event.OwnerAddress,
		OccurredAt:   event.OccurredAt,
		Metadata:     event.Metadata,
	}
}

// MapTokenEventsToDTOs maps a slice of schema.TokenEvent to a slice of TokenEvent
func MapTokenEventsToDTOs(events []schema.TokenEvent) []TokenEvent {
	dtos := make([]TokenEvent, len(events))
	for i := range events {
		tokenEventDTO := MapTokenEventToDTO(&events[i])
		dtos[i] = *tokenEventDTO
	}
	return dtos
}

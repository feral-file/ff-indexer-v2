package schema

import "time"

// TokenEventType represents the type of token event
type TokenEventType string

const (
	EventTypeAcquired           TokenEventType = "acquired"
	EventTypeReleased           TokenEventType = "released"
	EventTypeMetadataUpdated    TokenEventType = "metadata_updated"
	EventTypeEnrichmentUpdated  TokenEventType = "enrichment_updated"
	EventTypeViewabilityChanged TokenEventType = "viewability_changed"
)

// TokenEvent represents the token_events table - unified event log for ownership and attribute changes
// This table enables efficient collection sync with simple pagination
type TokenEvent struct {
	// ID is the internal database primary key
	ID uint64 `gorm:"column:id;primaryKey;autoIncrement"`
	// TokenID references the token this event relates to
	TokenID uint64 `gorm:"column:token_id;not null"`
	// EventType indicates the type of event (acquired, released, metadata_updated, enrichment_updated, viewability_changed)
	EventType TokenEventType `gorm:"column:event_type;not null;type:text"`
	// OwnerAddress is the blockchain address for ownership events (NULL for attribute events that broadcast to all owners)
	OwnerAddress *string `gorm:"column:owner_address;type:text"`
	// OccurredAt is the timestamp when the event actually occurred (NOT when it was recorded)
	OccurredAt time.Time `gorm:"column:occurred_at;not null;type:timestamptz"`
	// Metadata stores lightweight reference data (JSONB)
	// Examples:
	//   acquired: {"tx_hash": "0x...", "quantity": "1"}
	//   released: {"tx_hash": "0x...", "quantity": "1", "to_address": "0x..."}
	//   metadata_updated: {"changed_fields": ["name", "image_url"]}
	//   enrichment_updated: {"vendor": "artblocks", "changed_fields": ["animation_url"]}
	//   viewability_changed: {"is_viewable": true}
	Metadata []byte `gorm:"column:metadata;type:jsonb"`
	// CreatedAt is the timestamp when this event was recorded in the database
	CreatedAt time.Time `gorm:"column:created_at;not null;default:now();type:timestamptz"`

	// Associations
	Token Token `gorm:"foreignKey:TokenID;constraint:OnDelete:CASCADE"`
}

// TableName specifies the table name for the TokenEvent model
func (TokenEvent) TableName() string {
	return "token_events"
}

// AcquisitionMetadata contains reference data for token acquisition events
type AcquisitionMetadata struct {
	TxHash   string `json:"tx_hash,omitempty"`  // Transaction hash for tracing
	Quantity string `json:"quantity,omitempty"` // For ERC1155/FA2 tokens
}

// ReleaseMetadata contains reference data for token release events
type ReleaseMetadata struct {
	TxHash    string `json:"tx_hash,omitempty"`    // Transaction hash for tracing
	Quantity  string `json:"quantity,omitempty"`   // For ERC1155/FA2 tokens
	ToAddress string `json:"to_address,omitempty"` // Where the token was transferred to
}

// MetadataUpdateMetadata contains reference data for metadata update events
type MetadataUpdateMetadata struct {
	ChangedFields []string `json:"changed_fields"` // List of field names that changed (e.g., "name", "image_url")
}

// EnrichmentUpdateMetadata contains reference data for enrichment update events
type EnrichmentUpdateMetadata struct {
	Vendor        string   `json:"vendor"`                   // Enrichment vendor (e.g., "artblocks", "opensea")
	ChangedFields []string `json:"changed_fields,omitempty"` // List of field names that changed
}

// ViewabilityChangeMetadata contains reference data for viewability change events
type ViewabilityChangeMetadata struct {
	IsViewable bool `json:"is_viewable"` // New viewability state
}

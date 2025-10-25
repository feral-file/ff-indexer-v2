package schema

import (
	"time"

	"gorm.io/datatypes"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
)

// ProvenanceEventType represents the type of blockchain event
type ProvenanceEventType string

const (
	// ProvenanceEventTypeMint indicates token creation/minting
	ProvenanceEventTypeMint ProvenanceEventType = "mint"
	// ProvenanceEventTypeTransfer indicates token ownership transfer
	ProvenanceEventTypeTransfer ProvenanceEventType = "transfer"
	// ProvenanceEventTypeBurn indicates token destruction
	ProvenanceEventTypeBurn ProvenanceEventType = "burn"
	// ProvenanceEventTypeMetadataUpdate indicates metadata was updated on-chain
	ProvenanceEventTypeMetadataUpdate ProvenanceEventType = "metadata_update"
)

// ProvenanceEvent represents the provenance_events table - optional audit trail of blockchain events
type ProvenanceEvent struct {
	// ID is the internal database primary key
	ID uint64 `gorm:"column:id;primaryKey;autoIncrement"`
	// TokenID references the token this event relates to
	TokenID uint64 `gorm:"column:token_id;not null"`
	// Chain identifies the blockchain network where this event occurred
	Chain domain.Chain `gorm:"column:chain;not null;type:text;uniqueIndex:idx_provenance_chain_tx_hash"`
	// EventType identifies the type of blockchain event (mint, transfer, burn, metadata_update)
	EventType ProvenanceEventType `gorm:"column:event_type;not null;type:text"`
	// FromAddress is the sender's blockchain address (nil for mint events)
	FromAddress *string `gorm:"column:from_address;type:text"`
	// ToAddress is the recipient's blockchain address (nil for burn events)
	ToAddress *string `gorm:"column:to_address;type:text"`
	// Quantity is the number of tokens involved (stored as string to support up to 78 digits)
	Quantity *string `gorm:"column:quantity;type:numeric(78,0)"`
	// TxHash is the transaction hash that triggered this event
	TxHash *string `gorm:"column:tx_hash;type:text;uniqueIndex:idx_provenance_chain_tx_hash"`
	// BlockNumber is the block number where this event was recorded
	BlockNumber *uint64 `gorm:"column:block_number;type:bigint"`
	// BlockHash is the hash of the block containing this event
	BlockHash *string `gorm:"column:block_hash;type:text"`
	// Timestamp is the blockchain timestamp when this event occurred
	Timestamp time.Time `gorm:"column:timestamp;not null;type:timestamptz"`
	// Raw contains the complete raw event data as JSON for debugging and analysis
	Raw datatypes.JSON `gorm:"column:raw;type:jsonb;index:idx_provenance_events_raw"`
	// CreatedAt is the timestamp when this record was indexed
	CreatedAt time.Time `gorm:"column:created_at;not null;default:now();type:timestamptz"`
	// UpdatedAt is the timestamp when this record was last updated
	UpdatedAt time.Time `gorm:"column:updated_at;not null;default:now();type:timestamptz"`

	// Associations
	Token Token `gorm:"foreignKey:TokenID;constraint:OnDelete:CASCADE"`
}

// TableName specifies the table name for the ProvenanceEvent model
func (ProvenanceEvent) TableName() string {
	return "provenance_events"
}

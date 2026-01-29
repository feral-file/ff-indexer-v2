package schema

import "time"

// TokenOwnershipProvenance tracks the most recent provenance event per token-owner pair
// This table enables fast owner-filtered queries by denormalizing the latest provenance
// timestamp for each address that received tokens (to_address only, not from_address).
// Maintained by application code in sync with provenance_events inserts.
type TokenOwnershipProvenance struct {
	// ID is the internal database primary key
	ID uint64 `gorm:"column:id;primaryKey;autoIncrement"`
	// TokenID references the token this record tracks
	TokenID uint64 `gorm:"column:token_id;not null;uniqueIndex:uq_token_ownership_provenance_token_owner,priority:1"`
	// OwnerAddress is the blockchain address that received the token (to_address from provenance event)
	OwnerAddress string `gorm:"column:owner_address;not null;type:text;uniqueIndex:uq_token_ownership_provenance_token_owner,priority:2"`
	// LastTimestamp is the timestamp of the most recent provenance event where this address was the recipient
	LastTimestamp time.Time `gorm:"column:last_timestamp;not null;type:timestamptz"`
	// LastTxIndex is the transaction index of the most recent provenance event (tiebreaker when timestamps are equal)
	LastTxIndex int64 `gorm:"column:last_tx_index;not null"`
	// LastEventType is the type of the most recent provenance event (mint, transfer, burn, metadata_update)
	LastEventType ProvenanceEventType `gorm:"column:last_event_type;not null;type:text"`
	// CreatedAt is the timestamp when this record was first created
	CreatedAt time.Time `gorm:"column:created_at;not null;default:now();type:timestamptz"`
	// UpdatedAt is the timestamp when this record was last updated
	UpdatedAt time.Time `gorm:"column:updated_at;not null;default:now();type:timestamptz"`

	// Associations
	Token Token `gorm:"foreignKey:TokenID;constraint:OnDelete:CASCADE"`
}

// TableName specifies the table name for the TokenOwnershipProvenance model
func (TokenOwnershipProvenance) TableName() string {
	return "token_ownership_provenance"
}

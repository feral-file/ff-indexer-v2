package schema

import (
	"time"

	"gorm.io/datatypes"
)

// SubjectType represents the type of entity that was changed
type SubjectType string

const (
	// SubjectTypeToken indicates a change to token data (mint, burn, etc.)
	SubjectTypeToken SubjectType = "token"
	// SubjectTypeOwner indicates a change in token ownership (transfer)
	SubjectTypeOwner SubjectType = "owner"
	// SubjectTypeBalance indicates a change in token balance (for ERC1155/FA2)
	SubjectTypeBalance SubjectType = "balance"
	// SubjectTypeMetadata indicates a change in token metadata
	SubjectTypeMetadata SubjectType = "metadata"
	// SubjectTypeMedia indicates a change in media asset status or URLs
	SubjectTypeMedia SubjectType = "media"
)

// ChangesJournal represents the changes_journal table - audit log for tracking all changes to indexed data
type ChangesJournal struct {
	// Cursor is an auto-incrementing sequence number for efficient pagination and ordering
	Cursor int64 `gorm:"column:\"cursor\";primaryKey;autoIncrement"`
	// TokenID is the foreign key to the token that was affected by this change
	TokenID int64 `gorm:"column:token_id;not null"`
	// SubjectType identifies what kind of entity changed (token, owner, balance, metadata, media)
	SubjectType SubjectType `gorm:"column:subject_type;not null;type:text"`
	// SubjectID is a polymorphic reference to the specific record that changed
	// - For 'token'/'owner': provenance_event_id
	// - For 'balance': balance_id
	// - For 'metadata': token_id
	// - For 'media': media_asset_id
	SubjectID string `gorm:"column:subject_id;not null;type:text"`
	// ChangedAt is the timestamp when the change occurred
	ChangedAt time.Time `gorm:"column:changed_at;not null;default:now();type:timestamptz"`
	// Meta contains additional context about the change as JSON (e.g., what fields changed, event details)
	Meta datatypes.JSON `gorm:"column:meta;type:jsonb"`
	// CreatedAt is the timestamp when this change was created
	CreatedAt time.Time `gorm:"column:created_at;not null;default:now();type:timestamptz"`
	// UpdatedAt is the timestamp when this change was last updated
	UpdatedAt time.Time `gorm:"column:updated_at;not null;default:now();type:timestamptz"`

	// Associations
	Token Token `gorm:"foreignKey:TokenID;constraint:OnDelete:CASCADE"`
}

// TableName specifies the table name for the ChangesJournal model
func (ChangesJournal) TableName() string {
	return "changes_journal"
}

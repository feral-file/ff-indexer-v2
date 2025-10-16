package schema

import (
	"time"

	"gorm.io/datatypes"
)

// SubjectType represents the type of entity that was changed
type SubjectType string

const (
	// SubjectTypeToken indicates a change to token data (ownership, burned status, etc.)
	SubjectTypeToken SubjectType = "token"
	// SubjectTypeOwner indicates a change in token ownership
	SubjectTypeOwner SubjectType = "owner"
	// SubjectTypeBalance indicates a change in token balance (for ERC1155/FA2)
	SubjectTypeBalance SubjectType = "balance"
	// SubjectTypeMetadata indicates a change in token metadata
	SubjectTypeMetadata SubjectType = "metadata"
	// SubjectTypeMedia indicates a change in media asset status or URLs
	SubjectTypeMedia SubjectType = "media"
	// SubjectTypeProvenance indicates a new provenance event was recorded
	SubjectTypeProvenance SubjectType = "provenance"
)

// ChangesJournal represents the changes_journal table - audit log for tracking all changes to indexed data
type ChangesJournal struct {
	// Cursor is an auto-incrementing sequence number for efficient pagination and ordering
	Cursor int64 `gorm:"column:\"cursor\";primaryKey;autoIncrement"`
	// SubjectType identifies what kind of entity changed (token, owner, balance, metadata, media, provenance)
	SubjectType SubjectType `gorm:"column:subject_type;not null;type:text"`
	// SubjectID is the identifier of the changed entity (typically a token_cid or owner address)
	SubjectID string `gorm:"column:subject_id;not null;type:text"`
	// ChangedAt is the timestamp when the change occurred
	ChangedAt time.Time `gorm:"column:changed_at;not null;default:now();type:timestamptz"`
	// Meta contains additional context about the change as JSON (e.g., what fields changed, event details)
	Meta datatypes.JSON `gorm:"column:meta;type:jsonb"`

	// Associations
	Token Token `gorm:"foreignKey:SubjectID;constraint:OnDelete:CASCADE"`
}

// TableName specifies the table name for the ChangesJournal model
func (ChangesJournal) TableName() string {
	return "changes_journal"
}

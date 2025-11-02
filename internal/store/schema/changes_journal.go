package schema

import (
	"time"

	"gorm.io/datatypes"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
)

// SubjectType represents the type of entity that was changed
type SubjectType string

const (
	// SubjectTypeToken indicates a change to token data (mint, burn)
	SubjectTypeToken SubjectType = "token"
	// SubjectTypeOwner indicates a change in token ownership (transfer for ERC721)
	SubjectTypeOwner SubjectType = "owner"
	// SubjectTypeBalance indicates a change in token balance (transfer for ERC1155/FA2)
	SubjectTypeBalance SubjectType = "balance"
	// SubjectTypeMetadata indicates a change in token metadata
	SubjectTypeMetadata SubjectType = "metadata"
)

// ProvenanceChangeMeta represents the metadata for token, owner, and balance changes
// It stores essential provenance information to quickly identify what changed without joining tables
type ProvenanceChangeMeta struct {
	Chain    domain.Chain         `json:"chain"`              // e.g., "eip155:1", "tezos:mainnet"
	Standard domain.ChainStandard `json:"standard"`           // e.g., "erc721", "erc1155", "fa2"
	Contract string               `json:"contract"`           // Contract address
	Token    string               `json:"token"`              // Token number
	From     *string              `json:"from,omitempty"`     // Sender address (nil for mints)
	To       *string              `json:"to,omitempty"`       // Receiver address (nil for burns)
	Quantity string               `json:"quantity,omitempty"` // Quantity transferred/minted/burned
}

// MetadataChangeMeta represents the metadata for metadata update changes
// It stores the old and new values of normalized metadata fields to track what changed
type MetadataChangeMeta struct {
	Old MetadataFields `json:"old"` // Previous metadata values
	New MetadataFields `json:"new"` // New metadata values
}

// MetadataFields represents the normalized metadata fields we track for changes
type MetadataFields struct {
	AnimationURL *string    `json:"animation_url,omitempty"` // URL to animated content
	ImageURL     *string    `json:"image_url,omitempty"`     // URL to image
	Artists      []Artist   `json:"artists,omitempty"`       // List of artist names
	Publisher    *Publisher `json:"publisher,omitempty"`     // Publisher name
	MimeType     *string    `json:"mime_type,omitempty"`     // MIME type of the artwork
}

// ChangesJournal represents the changes_journal table - audit log for tracking all changes to indexed data
type ChangesJournal struct {
	// ID is an auto-incrementing sequence number for record identification
	ID uint64 `gorm:"column:id;primaryKey;autoIncrement"`
	// TokenID is the foreign key to the token that was affected by this change
	TokenID uint64 `gorm:"column:token_id;not null"`
	// SubjectType identifies what kind of entity changed (token, owner, balance, metadata)
	SubjectType SubjectType `gorm:"column:subject_type;not null;type:text"`
	// SubjectID is a polymorphic reference to the specific record that changed
	// - For 'token'/'owner'/'balance': provenance_event_id
	// - For 'metadata': token_metadata_id (which equals token_id since it's 1-to-1)
	SubjectID string `gorm:"column:subject_id;not null;type:text"`
	// ChangedAt is the timestamp when the change occurred
	ChangedAt time.Time `gorm:"column:changed_at;not null;default:now();type:timestamptz"`
	// Meta contains additional context about the change as JSON
	// - For token/owner/balance: ProvenanceChangeMeta with chain, standard, contract, token, from, to, quantity
	// - For metadata: MetadataChangeMeta with old and new normalized metadata fields
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

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
	// SubjectTypeEnrichSource indicates a change in enrichment source data
	SubjectTypeEnrichSource SubjectType = "enrich_source"
	// SubjectTypeMediaAsset indicates a change in media asset data
	SubjectTypeMediaAsset SubjectType = "media_asset"
)

// ProvenanceChangeMeta represents the metadata for token, owner, and balance changes
// It stores essential provenance information to quickly identify what changed without joining tables
type ProvenanceChangeMeta struct {
	TokenID     uint64               `json:"token_id"`           // Token ID
	Chain       domain.Chain         `json:"chain"`              // e.g., "eip155:1", "tezos:mainnet"
	Standard    domain.ChainStandard `json:"standard"`           // e.g., "erc721", "erc1155", "fa2"
	Contract    string               `json:"contract"`           // Contract address
	TokenNumber string               `json:"token_number"`       // Token number
	From        *string              `json:"from,omitempty"`     // Sender address (nil for mints)
	To          *string              `json:"to,omitempty"`       // Receiver address (nil for burns)
	Quantity    string               `json:"quantity,omitempty"` // Quantity transferred/minted/burned
}

// MetadataChangeMeta represents the metadata for metadata update changes
// It stores the old and new values of normalized metadata fields to track what changed
type MetadataChangeMeta struct {
	TokenID uint64         `json:"token_id"` // Token ID
	Old     MetadataFields `json:"old"`      // Previous metadata values
	New     MetadataFields `json:"new"`      // New metadata values
}

// MetadataFields represents the normalized metadata fields we track for changes
type MetadataFields struct {
	AnimationURL *string    `json:"animation_url,omitempty"` // URL to animated content
	ImageURL     *string    `json:"image_url,omitempty"`     // URL to image
	Artists      []Artist   `json:"artists,omitempty"`       // List of artist names
	Publisher    *Publisher `json:"publisher,omitempty"`     // Publisher name
	Name         *string    `json:"name,omitempty"`          // Name from vendor
	Description  *string    `json:"description,omitempty"`   // Description from vendor
	MimeType     *string    `json:"mime_type,omitempty"`     // MIME type of the artwork
}

// EnrichmentSourceChangeMeta represents the metadata for enrichment source changes
// It stores the old and new values of enrichment source fields to track what changed
type EnrichmentSourceChangeMeta struct {
	TokenID uint64                 `json:"token_id"` // Token ID
	Old     EnrichmentSourceFields `json:"old"`      // Previous enrichment source values
	New     EnrichmentSourceFields `json:"new"`      // New enrichment source values
}

// EnrichmentSourceFields represents the enrichment source fields we track for changes
type EnrichmentSourceFields struct {
	Vendor       string   `json:"vendor"`                  // Vendor type (artblocks, fxhash, etc.)
	VendorHash   *string  `json:"vendor_hash,omitempty"`   // Hash of vendor JSON
	AnimationURL *string  `json:"animation_url,omitempty"` // URL to animated content from vendor
	ImageURL     *string  `json:"image_url,omitempty"`     // URL to image from vendor
	Name         *string  `json:"name,omitempty"`          // Name from vendor
	Description  *string  `json:"description,omitempty"`   // Description from vendor
	Artists      []Artist `json:"artists,omitempty"`       // List of artist names from vendor
	MimeType     *string  `json:"mime_type,omitempty"`     // MIME type from vendor
}

// MediaAssetChangeMeta represents the metadata for media asset changes
// It stores the old and new values of media asset fields to track what changed
type MediaAssetChangeMeta struct {
	Old MediaAssetFields `json:"old"` // Previous media asset values
	New MediaAssetFields `json:"new"` // New media asset values
}

// MediaAssetFields represents the media asset fields we track for changes
type MediaAssetFields struct {
	SourceURL        string  `json:"source_url"`                  // Original source URL
	Provider         string  `json:"provider"`                    // Storage provider
	ProviderAssetID  *string `json:"provider_asset_id,omitempty"` // Provider-specific asset ID
	MimeType         *string `json:"mime_type,omitempty"`         // MIME type
	FileSizeBytes    *int64  `json:"file_size_bytes,omitempty"`   // File size in bytes
	VariantURLs      string  `json:"variant_urls"`                // JSON string of variant URLs
	ProviderMetadata *string `json:"provider_metadata,omitempty"` // Provider-specific metadata as JSON string
}

// ChangesJournal represents the changes_journal table - audit log for tracking all changes to indexed data
type ChangesJournal struct {
	// ID is an auto-incrementing sequence number for record identification
	ID uint64 `gorm:"column:id;primaryKey;autoIncrement"`
	// SubjectType identifies what kind of entity changed (token, owner, balance, metadata, enrich_source, media_asset)
	SubjectType SubjectType `gorm:"column:subject_type;not null;type:text"`
	// SubjectID is a polymorphic reference to the specific record that changed
	// - For 'token'/'owner'/'balance': provenance_event_id
	// - For 'metadata': token_metadata_id (which equals token_id since it's 1-to-1)
	// - For 'enrich_source': token_id (enrichment_sources.token_id is the primary key)
	// - For 'media_asset': media_asset_id
	SubjectID string `gorm:"column:subject_id;not null;type:text"`
	// ChangedAt is the timestamp when the change occurred
	ChangedAt time.Time `gorm:"column:changed_at;not null;default:now();type:timestamptz"`
	// Meta contains additional context about the change as JSON
	// - For token/owner/balance: ProvenanceChangeMeta with chain, standard, contract, token, from, to, quantity
	// - For metadata: MetadataChangeMeta with old and new normalized metadata fields
	// - For enrich_source: EnrichmentSourceChangeMeta with old and new enrichment source fields
	// - For media_asset: MediaAssetChangeMeta with old and new media asset fields
	Meta datatypes.JSON `gorm:"column:meta;type:jsonb"`
	// CreatedAt is the timestamp when this change was created
	CreatedAt time.Time `gorm:"column:created_at;not null;default:now();type:timestamptz"`
	// UpdatedAt is the timestamp when this change was last updated
	UpdatedAt time.Time `gorm:"column:updated_at;not null;default:now();type:timestamptz"`
}

// TableName specifies the table name for the ChangesJournal model
func (ChangesJournal) TableName() string {
	return "changes_journal"
}

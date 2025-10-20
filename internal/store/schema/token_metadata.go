package schema

import (
	"time"

	"gorm.io/datatypes"
)

// EnrichmentLevel indicates the completeness of metadata enrichment
type EnrichmentLevel string

const (
	// EnrichmentLevelNone indicates no metadata has been fetched yet
	EnrichmentLevelNone EnrichmentLevel = "none"
	// EnrichmentLevelVendor indicates metadata has been enriched from vendor APIs (OpenSea, ArtBlocks, etc.)
	EnrichmentLevelVendor EnrichmentLevel = "vendor"
	// EnrichmentLevelVerified indicates metadata has been fully verified and enriched
	EnrichmentLevelVerified EnrichmentLevel = "full"
)

// TokenMetadata represents the token_metadata table - stores original and enriched metadata for tokens
type TokenMetadata struct {
	// TokenID references the associated token (primary key, one-to-one relationship)
	TokenID int64 `gorm:"column:token_id;primaryKey"`
	// OriginJSON is the original metadata fetched directly from the blockchain (tokenURI)
	OriginJSON datatypes.JSON `gorm:"column:origin_json;type:jsonb"`
	// LatestJSON is the enriched metadata combining origin and vendor data
	LatestJSON datatypes.JSON `gorm:"column:latest_json;type:jsonb"`
	// LatestHash is the hash of LatestJSON to detect changes and prevent redundant updates
	LatestHash *string `gorm:"column:latest_hash;type:text"`
	// EnrichmentLevel indicates how much enrichment has been performed (none, vendor, full)
	EnrichmentLevel EnrichmentLevel `gorm:"column:enrichment_level;not null;default:'none';type:text"`
	// LastRefreshedAt is the timestamp when metadata was last fetched from sources
	LastRefreshedAt *time.Time `gorm:"column:last_refreshed_at;type:timestamptz"`
	// ImageURL is the direct URL to the token's image (extracted for quick access)
	ImageURL *string `gorm:"column:image_url;type:text"`
	// AnimationURL is the URL to animated content like videos or interactive content
	AnimationURL *string `gorm:"column:animation_url;type:text"`
	// Name is the token's name (extracted for quick access and indexing)
	Name *string `gorm:"column:name;type:text"`
	// Artists are the creators' names (extracted for quick access and indexing)
	Artists []string `gorm:"column:artists;type:text[]"`
	// CreatedAt is the timestamp when this record was created
	CreatedAt time.Time `gorm:"column:created_at;not null;default:now();type:timestamptz"`
	// UpdatedAt is the timestamp when this record was last updated
	UpdatedAt time.Time `gorm:"column:updated_at;not null;default:now();type:timestamptz"`

	// Associations
	Token Token `gorm:"foreignKey:TokenID;constraint:OnDelete:CASCADE"`
}

// TableName specifies the table name for the TokenMetadata model
func (TokenMetadata) TableName() string {
	return "token_metadata"
}

package schema

import (
	"database/sql/driver"
	"encoding/json"
	"time"

	"gorm.io/datatypes"
)

// Artist represents an artist/creator with their decentralized identifier and name
type Artist struct {
	DID  string `json:"did"`
	Name string `json:"name"`
}

// Artists is a slice of Artist that can be stored as JSONB in PostgreSQL
type Artists []Artist

// Scan implements the sql.Scanner interface for reading from database
func (a *Artists) Scan(value interface{}) error {
	if value == nil {
		*a = nil
		return nil
	}

	var bytes []byte
	switch v := value.(type) {
	case []byte:
		bytes = v
	case string:
		bytes = []byte(v)
	default:
		return nil
	}

	return json.Unmarshal(bytes, a)
}

// Value implements the driver.Valuer interface for writing to database
func (a Artists) Value() (driver.Value, error) {
	if a == nil {
		return nil, nil
	}
	return json.Marshal(a)
}

// EnrichmentLevel indicates the completeness of metadata enrichment
type EnrichmentLevel string

const (
	// EnrichmentLevelNone indicates no metadata has been fetched yet
	EnrichmentLevelNone EnrichmentLevel = "none"
	// EnrichmentLevelVendor indicates metadata has been enriched from vendor APIs (FXHash, ArtBlocks, etc.)
	EnrichmentLevelVendor EnrichmentLevel = "vendor"
)

// TokenMetadata represents the token_metadata table - stores original and enriched metadata for tokens
type TokenMetadata struct {
	// TokenID references the associated token (primary key, one-to-one relationship)
	TokenID uint64 `gorm:"column:token_id;primaryKey"`
	// OriginJSON is the original metadata fetched directly from the blockchain (tokenURI)
	OriginJSON datatypes.JSON `gorm:"column:origin_json;type:jsonb"`
	// LatestJSON is the enriched metadata combining origin and vendor data
	LatestJSON datatypes.JSON `gorm:"column:latest_json;type:jsonb"`
	// LatestHash is the hash of LatestJSON to detect changes and prevent redundant updates
	LatestHash *string `gorm:"column:latest_hash;type:text"`
	// EnrichmentLevel indicates how much enrichment has been performed (none, vendor, full)
	EnrichmentLevel EnrichmentLevel `gorm:"column:enrichment_level;not null;default:'none';type:text;index:idx_token_metadata_enrichment_level"`
	// LastRefreshedAt is the timestamp when metadata was last fetched from sources
	LastRefreshedAt *time.Time `gorm:"column:last_refreshed_at;type:timestamptz;index:idx_token_metadata_last_refreshed_at"`
	// ImageURL is the direct URL to the token's image (extracted for quick access)
	ImageURL *string `gorm:"column:image_url;type:text"`
	// AnimationURL is the URL to animated content like videos or interactive content
	AnimationURL *string `gorm:"column:animation_url;type:text"`
	// Name is the token's name (extracted for quick access and indexing)
	Name *string `gorm:"column:name;type:text"`
	// Artists are the creators (extracted for quick access and indexing)
	Artists Artists `gorm:"column:artists;type:jsonb;serializer:json;index:idx_token_metadata_artists"`
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

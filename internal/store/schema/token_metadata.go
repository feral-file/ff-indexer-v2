package schema

import (
	"database/sql/driver"
	"encoding/json"
	"time"

	"gorm.io/datatypes"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
)

// Artist represents an artist/creator with their decentralized identifier and name
type Artist struct {
	DID  domain.DID `json:"did"`
	Name string     `json:"name"`
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

// Publisher represents the publisher of the token
type Publisher struct {
	Name *string `json:"name,omitempty"`
	URL  *string `json:"url,omitempty"`
}

func (p *Publisher) Value() (driver.Value, error) {
	if p == nil {
		return nil, nil
	}
	return json.Marshal(p)
}

func (p *Publisher) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	return json.Unmarshal(value.([]byte), p)
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
	// LatestJSON is the latest metadata fetched from the blockchain
	LatestJSON datatypes.JSON `gorm:"column:latest_json;type:jsonb"`
	// LatestHash is the hash of LatestJSON to detect changes and prevent redundant updates
	LatestHash *string `gorm:"column:latest_hash;type:text"`
	// EnrichmentLevel indicates how much enrichment has been performed (none, vendor)
	EnrichmentLevel EnrichmentLevel `gorm:"column:enrichment_level;not null;default:'none';type:text;index:idx_token_metadata_enrichment_level"`
	// LastRefreshedAt is the timestamp when metadata was last fetched from sources
	LastRefreshedAt *time.Time `gorm:"column:last_refreshed_at;type:timestamptz;index:idx_token_metadata_last_refreshed_at"`
	// ImageURL is the direct URL to the token's image
	ImageURL *string `gorm:"column:image_url;type:text"`
	// ImageURLHash is the MD5 hash of ImageURL for efficient indexing
	ImageURLHash *string `gorm:"column:image_url_hash;type:text"`
	// AnimationURL is the URL to animated content like videos or interactive content
	AnimationURL *string `gorm:"column:animation_url;type:text"`
	// AnimationURLHash is the MD5 hash of AnimationURL for efficient indexing
	AnimationURLHash *string `gorm:"column:animation_url_hash;type:text"`
	// Name is the token's name
	Name *string `gorm:"column:name;type:text"`
	// Description is the token's description
	Description *string `gorm:"column:description;type:text"`
	// Artists are the creators
	Artists Artists `gorm:"column:artists;type:jsonb;serializer:json;index:idx_token_metadata_artists"`
	// Publisher is the publisher of the token
	Publisher *Publisher `gorm:"column:publisher;type:jsonb;serializer:json;index:idx_token_metadata_publisher"`
	// MimeType is the MIME type of the artwork (detected from animation_url or image_url)
	MimeType *string `gorm:"column:mime_type;type:text"`
	// CreatedAt is the timestamp when this record was created
	CreatedAt time.Time `gorm:"column:created_at;not null;default:now();type:timestamptz"`
	// UpdatedAt is the timestamp when this record was last updated
	UpdatedAt time.Time `gorm:"column:updated_at;not null;default:now();type:timestamptz"`

	// Associations
	Token Token `gorm:"foreignKey:TokenID;constraint:OnDelete:CASCADE"`
}

// MediaURLs returns the media URLs from the token metadata
func (t *TokenMetadata) MediaURLs() []string {
	var urls []string
	if t.ImageURL != nil && *t.ImageURL != "" {
		urls = append(urls, *t.ImageURL)
	}
	if t.AnimationURL != nil && *t.AnimationURL != "" {
		urls = append(urls, *t.AnimationURL)
	}
	return urls
}

// TableName specifies the table name for the TokenMetadata model
func (TokenMetadata) TableName() string {
	return "token_metadata"
}

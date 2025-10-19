package schema

import (
	"time"
)

// Vendor represents the source of metadata enrichment
type Vendor string

const (
	// VendorOpenSea represents metadata from OpenSea API
	VendorOpenSea Vendor = "opensea"
	// VendorArtBlocks represents metadata from Art Blocks API
	VendorArtBlocks Vendor = "artblocks"
	// VendorOnchain represents metadata fetched directly from blockchain
	VendorOnchain Vendor = "onchain"
	// VendorObjkt represents metadata from Objkt (Tezos marketplace)
	VendorObjkt Vendor = "objkt"
)

// EnrichmentSource represents the enrichment_sources table - tracks metadata fetch attempts from various vendors
type EnrichmentSource struct {
	// ID is the internal database primary key
	ID int64 `gorm:"column:id;primaryKey;autoIncrement"`
	// TokenID references the token being enriched
	TokenID int64 `gorm:"column:token_id;not null;uniqueIndex:idx_enrichment_sources_token_vendor,priority:1"`
	// Vendor identifies the metadata source (opensea, artblocks, onchain, objkt)
	Vendor Vendor `gorm:"column:vendor;not null;type:text;uniqueIndex:idx_enrichment_sources_token_vendor,priority:2"`
	// SourceURL is the API endpoint or URI used to fetch metadata
	SourceURL *string `gorm:"column:source_url;type:text"`
	// ETag is the HTTP ETag header value for cache validation
	ETag *string `gorm:"column:etag;type:text"`
	// LastStatus is the HTTP status code from the last fetch attempt
	LastStatus *int `gorm:"column:last_status;type:integer"`
	// LastError contains the error message if the last fetch failed
	LastError *string `gorm:"column:last_error;type:text"`
	// LastFetchedAt is the timestamp of the last fetch attempt
	LastFetchedAt *time.Time `gorm:"column:last_fetched_at;type:timestamptz"`
	// LastHash is the content hash from the last successful fetch to detect changes
	LastHash *string `gorm:"column:last_hash;type:text"`
	// CreatedAt is the timestamp when this enrichment source was created
	CreatedAt time.Time `gorm:"column:created_at;not null;default:now();type:timestamptz"`
	// UpdatedAt is the timestamp when this enrichment source was last updated
	UpdatedAt time.Time `gorm:"column:updated_at;not null;default:now();type:timestamptz"`

	// Associations
	Token Token `gorm:"foreignKey:TokenID;constraint:OnDelete:CASCADE"`
}

// TableName specifies the table name for the EnrichmentSource model
func (EnrichmentSource) TableName() string {
	return "enrichment_sources"
}

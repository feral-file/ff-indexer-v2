package schema

import (
	"time"

	"gorm.io/datatypes"
)

// Vendor represents the source of metadata enrichment
type Vendor string

const (
	// VendorArtBlocks represents metadata from Art Blocks API
	VendorArtBlocks Vendor = "artblocks"
	// VendorFXHash represents metadata from fxhash API
	VendorFXHash Vendor = "fxhash"
	// VendorFoundation represents metadata from Foundation API
	VendorFoundation Vendor = "foundation"
	// VendorSuperRare represents metadata from SuperRare API
	VendorSuperRare Vendor = "superrare"
	// VendorFeralFile represents metadata from Feral File API
	VendorFeralFile Vendor = "feralfile"
	// VendorObjkt represents metadata from objkt v3 API
	VendorObjkt Vendor = "objkt"
	// VendorOpenSea represents metadata from OpenSea API
	VendorOpenSea Vendor = "opensea"
)

// EnrichmentSource represents the enrichment_sources table - stores enriched metadata from vendor APIs
type EnrichmentSource struct {
	// TokenID references the token being enriched (primary key, one-to-one relationship)
	TokenID uint64 `gorm:"column:token_id;primaryKey"`
	// Vendor identifies the metadata source (artblocks, fxhash, foundation, superrare, feralfile)
	Vendor Vendor `gorm:"column:vendor;not null;type:text"`
	// VendorJSON is the raw response from vendor API for auditing and reprocessing
	VendorJSON datatypes.JSON `gorm:"column:vendor_json;type:jsonb"`
	// VendorHash is the hash of VendorJSON to detect changes in vendor data
	VendorHash *string `gorm:"column:vendor_hash;type:text"`
	// ImageURL is the normalized image URL from vendor
	ImageURL *string `gorm:"column:image_url;type:text"`
	// AnimationURL is the normalized animation URL from vendor
	AnimationURL *string `gorm:"column:animation_url;type:text"`
	// Name is the normalized name from vendor
	Name *string `gorm:"column:name;type:text"`
	// Description is the normalized description from vendor
	Description *string `gorm:"column:description;type:text"`
	// Artists are the normalized artists array from vendor
	Artists Artists `gorm:"column:artists;type:jsonb"`
	// MimeType is the MIME type of the artwork (detected from animation_url or image_url)
	MimeType *string `gorm:"column:mime_type;type:text"`
	// CreatedAt is the timestamp when this enrichment source was created
	CreatedAt time.Time `gorm:"column:created_at;not null;default:now();type:timestamptz"`
	// UpdatedAt is the timestamp when this enrichment source was last updated
	UpdatedAt time.Time `gorm:"column:updated_at;not null;default:now();type:timestamptz"`

	// Associations
	Token Token `gorm:"foreignKey:TokenID;constraint:OnDelete:CASCADE"`
}

// MediaURLs returns the media URLs from the enrichment source
func (e *EnrichmentSource) MediaURLs() []string {
	var urls []string
	if e.ImageURL != nil && *e.ImageURL != "" {
		urls = append(urls, *e.ImageURL)
	}
	if e.AnimationURL != nil && *e.AnimationURL != "" {
		urls = append(urls, *e.AnimationURL)
	}
	return urls
}

// TableName specifies the table name for the EnrichmentSource model
func (EnrichmentSource) TableName() string {
	return "enrichment_sources"
}

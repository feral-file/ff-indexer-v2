package schema

import (
	"time"

	"gorm.io/datatypes"
)

// StorageProvider represents the storage provider type
type StorageProvider string

// String returns the storage provider name
func (sp StorageProvider) String() string {
	return string(sp)
}

const (
	// StorageProviderSelfHosted represents self-hosted storage
	StorageProviderSelfHosted StorageProvider = "self_hosted"
	// StorageProviderCloudflare represents Cloudflare storage
	StorageProviderCloudflare StorageProvider = "cloudflare"
	// StorageProviderS3 represents S3-compatible storage
	StorageProviderS3 StorageProvider = "s3"
)

// MediaAsset represents the media_assets table - reference mapping between original URLs and provider URLs
type MediaAsset struct {
	// ID is the internal database primary key
	ID int64 `gorm:"column:id;primaryKey;autoIncrement"`

	// Original source
	// SourceURL is the original URL where the media was found
	SourceURL string `gorm:"column:source_url;not null;type:text;index:idx_media_assets_source_url"`
	// MimeType is the MIME type of the media (e.g., image/jpeg, video/mp4)
	MimeType *string `gorm:"column:mime_type;type:text"`
	// FileSizeBytes is the file size in bytes
	FileSizeBytes *int64 `gorm:"column:file_size_bytes"`

	// Storage provider info
	// Provider identifies the storage provider type
	Provider StorageProvider `gorm:"column:provider;not null;type:text;index:idx_media_assets_provider;uniqueIndex:idx_media_assets_provider_asset_id,priority:1"`
	// ProviderAssetID is the provider-specific ID (e.g., cf_image_id, s3 key, ipfs hash)
	ProviderAssetID *string `gorm:"column:provider_asset_id;type:text;uniqueIndex:idx_media_assets_provider_asset_id,priority:2"`
	// ProviderMetadata stores provider-specific data as JSON (e.g., cloudflare account info, s3 bucket details)
	ProviderMetadata datatypes.JSON `gorm:"column:provider_metadata;type:jsonb"`

	// Variant URLs (actual URLs, not URIs)
	// VariantURLs stores different size/quality variants as JSON (e.g., {"thumbnail": "https://...", "medium": "https://...", "original": "https://..."})
	VariantURLs datatypes.JSON `gorm:"column:variant_urls;not null;type:jsonb"`

	// Timestamps
	// CreatedAt is the timestamp when this record was created
	CreatedAt time.Time `gorm:"column:created_at;not null;default:now();type:timestamptz;index:idx_media_assets_created_at"`
	// UpdatedAt is the timestamp when this record was last updated
	UpdatedAt time.Time `gorm:"column:updated_at;not null;default:now();type:timestamptz"`
}

// TableName specifies the table name for the MediaAsset model
func (MediaAsset) TableName() string {
	return "media_assets"
}

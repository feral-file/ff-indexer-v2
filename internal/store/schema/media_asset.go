package schema

import (
	"time"

	"gorm.io/datatypes"
)

// Role represents the purpose of a media asset
type Role string

const (
	// RoleImage represents the primary static image for the token
	RoleImage Role = "image"
	// RoleAnimation represents animated content (video, HTML, etc.)
	RoleAnimation Role = "animation"
	// RolePoster represents a thumbnail/poster frame for animated content
	RolePoster Role = "poster"
)

// Status represents the processing state of a media asset
type Status string

const (
	// StatusPending indicates the media asset is queued for processing
	StatusPending Status = "pending"
	// StatusReady indicates the media asset has been successfully processed and cached
	StatusReady Status = "ready"
	// StatusFailed indicates processing failed (e.g., unreachable URL, unsupported format)
	StatusFailed Status = "failed"
)

// MediaAsset represents the media_assets table - manages media files with Cloudflare CDN integration
type MediaAsset struct {
	// ID is the internal database primary key
	ID int64 `gorm:"column:id;primaryKey;autoIncrement"`
	// TokenID references the token this media belongs to
	TokenID int64 `gorm:"column:token_id;not null;uniqueIndex:idx_media_assets_token_role,priority:1"`
	// Role identifies the media type (image, animation, poster)
	Role Role `gorm:"column:role;not null;type:text;uniqueIndex:idx_media_assets_token_role,priority:2"`
	// SourceURL is the original URL where the media was fetched from
	SourceURL *string `gorm:"column:source_url;type:text"`
	// ContentHash is the hash of the media content for deduplication and integrity checking
	ContentHash *string `gorm:"column:content_hash;type:text"`
	// CFImageID is the Cloudflare Images ID after uploading to CDN
	CFImageID *string `gorm:"column:cf_image_id;type:text"`
	// CFVariantMap stores Cloudflare image variant URLs (e.g., {"thumb": "url", "fit": "url"})
	CFVariantMap datatypes.JSON `gorm:"column:cf_variant_map;type:jsonb"`
	// Status indicates the processing state (pending, ready, failed)
	Status Status `gorm:"column:status;not null;default:'pending';type:text"`
	// LastCheckedAt is the timestamp when the media was last verified or processed
	LastCheckedAt *time.Time `gorm:"column:last_checked_at;type:timestamptz"`
	// CreatedAt is the timestamp when this record was created
	CreatedAt time.Time `gorm:"column:created_at;default:now();type:timestamptz"`

	// Associations
	Token Token `gorm:"foreignKey:TokenID;constraint:OnDelete:CASCADE"`
}

// TableName specifies the table name for the MediaAsset model
func (MediaAsset) TableName() string {
	return "media_assets"
}

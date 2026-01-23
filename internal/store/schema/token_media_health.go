package schema

import (
	"time"
)

// MediaHealthStatus represents the health status of a media URL
type MediaHealthStatus string

const (
	// MediaHealthStatusUnknown indicates the media has not been checked yet
	MediaHealthStatusUnknown MediaHealthStatus = "unknown"
	// MediaHealthStatusHealthy indicates the media is accessible
	MediaHealthStatusHealthy MediaHealthStatus = "healthy"
	// MediaHealthStatusBroken indicates the media is not accessible
	MediaHealthStatusBroken MediaHealthStatus = "broken"
)

// MediaHealthSource represents the source of the media URL
type MediaHealthSource string

const (
	// MediaHealthSourceMetadataImage indicates the media came from metadata_image
	MediaHealthSourceMetadataImage MediaHealthSource = "metadata_image"
	// MediaHealthSourceMetadataAnimation indicates the media came from metadata_animation
	MediaHealthSourceMetadataAnimation MediaHealthSource = "metadata_animation"
	// MediaHealthSourceEnrichmentImage indicates the media came from enrichment_image
	MediaHealthSourceEnrichmentImage MediaHealthSource = "enrichment_image"
	// MediaHealthSourceEnrichmentAnimation indicates the media came from enrichment_animation
	MediaHealthSourceEnrichmentAnimation MediaHealthSource = "enrichment_animation"
)

// String returns the string representation of the health status
func (s MediaHealthStatus) String() string {
	return string(s)
}

// TokenMediaHealth represents the token_media_health table
// Tracks health check status for media URLs associated with tokens
type TokenMediaHealth struct {
	// ID is the internal database primary key
	ID uint64 `gorm:"column:id;primaryKey;autoIncrement"`

	// TokenID is the foreign key to tokens table
	TokenID uint64 `gorm:"column:token_id;not null"`

	// MediaURL is the URL being checked for health
	MediaURL string `gorm:"column:media_url;not null;type:text"`

	// MediaSource indicates where this URL came from: metadata_image, metadata_animation, enrichment_image, enrichment_animation
	MediaSource MediaHealthSource `gorm:"column:media_source;not null;type:text"`

	// HealthStatus is the current health status
	HealthStatus MediaHealthStatus `gorm:"column:health_status;not null;type:media_health_status;default:unknown"`

	// LastCheckedAt is the timestamp of the last health check
	LastCheckedAt time.Time `gorm:"column:last_checked_at;not null;default:now();type:timestamptz"`

	// LastError stores the error message from the last failed check (NULL if healthy)
	LastError *string `gorm:"column:last_error;type:text"`

	// Timestamps
	CreatedAt time.Time `gorm:"column:created_at;not null;default:now();type:timestamptz"`
	UpdatedAt time.Time `gorm:"column:updated_at;not null;default:now();type:timestamptz"`
}

// TableName specifies the table name for the TokenMediaHealth model
func (TokenMediaHealth) TableName() string {
	return "token_media_health"
}

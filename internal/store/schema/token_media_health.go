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

// MediaFailureReason is the machine-readable cause recorded when a probe marks a URL broken.
//
// Reason: last_error is a human-facing message and unusable for aggregation; these constants
// let the fleet-wide delta ("reported healthy" vs "actually valid") be queried per failure
// class. Stored as TEXT (not a Postgres enum) so new probe rules are additive without a
// migration. Constraints: values are part of the persisted contract — rename requires a data
// migration. The "render_" prefix is reserved for the L1 render probe: rows gated with a
// render_% reason are healed only by the render probe, never by the byte-level sweep.
type MediaFailureReason string

const (
	// MediaFailureHTTPStatus indicates a non-2xx HTTP response.
	MediaFailureHTTPStatus MediaFailureReason = "http_status"
	// MediaFailureDNS indicates DNS resolution failure for the URL's host.
	MediaFailureDNS MediaFailureReason = "dns"
	// MediaFailureSSRF indicates the SSRF policy refused the fetch.
	MediaFailureSSRF MediaFailureReason = "ssrf"
	// MediaFailureTypeMismatch indicates the declared Content-Type claims image/video/audio
	// but the body sniffs as text (e.g. an HTML error page served as image/png).
	MediaFailureTypeMismatch MediaFailureReason = "type_mismatch"
	// MediaFailureContainerInvalid indicates the body sniffed as a known media container
	// whose header failed to parse (truncated/corrupt PNG, JPEG, MP4, ...).
	MediaFailureContainerInvalid MediaFailureReason = "container_invalid"
	// MediaFailureDirectoryListing indicates the body is an IPFS/Kubo gateway directory
	// listing — a directory CID, not an artwork (feral-file#3482).
	MediaFailureDirectoryListing MediaFailureReason = "directory_listing"
	// MediaFailureKnownErrorPage indicates the body matched a configured known-bad page
	// marker (gateway error pages served with HTTP 200).
	MediaFailureKnownErrorPage MediaFailureReason = "known_error_page"
	// MediaFailureZeroLength indicates an empty body or Content-Length: 0.
	MediaFailureZeroLength MediaFailureReason = "zero_length"
	// MediaFailureTruncated indicates the body ended before the declared Content-Length.
	MediaFailureTruncated MediaFailureReason = "truncated"
	// MediaFailureInvalidURL indicates the URL failed basic parsing; no fetch attempted.
	MediaFailureInvalidURL MediaFailureReason = "invalid_url"
	// MediaFailureUnsupportedScheme indicates a non-HTTP(S) scheme reached the checker
	// (a URI that escaped gateway normalization at ingest); no fetch attempted.
	MediaFailureUnsupportedScheme MediaFailureReason = "unsupported_scheme"
	// MediaFailureTransport indicates a transport-level fetch failure with no more
	// specific taxonomy entry (TLS, protocol, non-retryable connection errors).
	// Deliberately coarse; last_error carries the specific message.
	MediaFailureTransport MediaFailureReason = "transport"
	// MediaFailureDataURIInvalid indicates a data: URI that failed RFC 2397 parsing.
	MediaFailureDataURIInvalid MediaFailureReason = "data_uri_invalid"
	// MediaFailureUnsupportedMimeType indicates a data: URI declaring a mime type
	// outside the supported set.
	MediaFailureUnsupportedMimeType MediaFailureReason = "unsupported_mime_type"
)

// String returns the string representation of the failure reason
func (r MediaFailureReason) String() string {
	return string(r)
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

	// MediaURLHash is the MD5 hash of MediaURL for efficient indexing
	MediaURLHash string `gorm:"column:media_url_hash;not null;type:text"`

	// MediaSource indicates where this URL came from: metadata_image, metadata_animation, enrichment_image, enrichment_animation
	MediaSource MediaHealthSource `gorm:"column:media_source;not null;type:text"`

	// HealthStatus is the current health status
	HealthStatus MediaHealthStatus `gorm:"column:health_status;not null;type:media_health_status;default:unknown"`

	// LastCheckedAt is the timestamp of the last health check
	LastCheckedAt time.Time `gorm:"column:last_checked_at;not null;default:now();type:timestamptz"`

	// LastError stores the error message from the last failed check (NULL if healthy)
	LastError *string `gorm:"column:last_error;type:text"`

	// FailureReason is the machine-readable cause of the last broken verdict (see
	// MediaFailureReason). NULL only for healthy and unknown rows: every persisted
	// broken verdict carries a reason, so NULL-with-broken cannot be confused with
	// "not yet probed" (rows written before this contract may still hold NULL until
	// their next sweep re-probes them).
	FailureReason *string `gorm:"column:failure_reason;type:text"`

	// ObservedContentType is the Content-Type header observed on the last probe.
	ObservedContentType *string `gorm:"column:observed_content_type;type:text"`

	// SniffedContentType is the content type detected from the first bytes of the body on
	// the last probe. Drives render-probe class selection (HTML vs image vs video).
	SniffedContentType *string `gorm:"column:sniffed_content_type;type:text"`

	// Timestamps
	CreatedAt time.Time `gorm:"column:created_at;not null;default:now();type:timestamptz"`
	UpdatedAt time.Time `gorm:"column:updated_at;not null;default:now();type:timestamptz"`
}

// TableName specifies the table name for the TokenMediaHealth model
func (TokenMediaHealth) TableName() string {
	return "token_media_health"
}

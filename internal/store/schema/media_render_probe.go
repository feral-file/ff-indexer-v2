package schema

import (
	"time"
)

// RenderProbeVerdict classifies what headless chromium painted for a media URL.
type RenderProbeVerdict string

const (
	// RenderProbeVerdictRenderedOK indicates a non-degenerate frame was captured.
	RenderProbeVerdictRenderedOK RenderProbeVerdict = "rendered_ok"
	// RenderProbeVerdictBlank indicates the captured frame is near-uniform (no visible output).
	RenderProbeVerdictBlank RenderProbeVerdict = "blank"
	// RenderProbeVerdictStalled indicates the page failed to load or screenshot within the timeout.
	RenderProbeVerdictStalled RenderProbeVerdict = "stalled"
	// RenderProbeVerdictKnownBadFingerprint indicates the frame matched a configured
	// known-bad render (directory listing, gateway error page, placeholder image).
	RenderProbeVerdictKnownBadFingerprint RenderProbeVerdict = "known_bad_fingerprint"
)

// String returns the string representation of the verdict.
func (v RenderProbeVerdict) String() string {
	return string(v)
}

// RenderFailureReason values written to token_media_health.failure_reason when the render
// probe gates viewability. The render_ prefix is reserved in the L0 taxonomy (migration
// 022): rows carrying these reasons are healed only by the render probe, never by the
// byte-level sweep, which would otherwise flap them back to healthy.
const (
	// RenderFailureBlank gates after consecutive blank frames.
	RenderFailureBlank = "render_blank"
	// RenderFailureStalled gates after consecutive load failures/timeouts.
	RenderFailureStalled = "render_stalled"
	// RenderFailureKnownBad gates immediately on a known-bad fingerprint match.
	RenderFailureKnownBad = "render_known_bad"
)

// MediaRenderProbe represents the media_render_probes table — one L1 render observation
// row per media URL.
//
// Reason: render outcome is a property of the URL (same rendering for every token sharing
// it), mirroring the media_url_hash-wide semantics of token_media_health updates.
// Constraints: Phash/BaselinePhash store the 64-bit DCT perceptual hash's bit pattern as
// int64 (two's complement); readers reinterpret as uint64. BaselinePhash is never
// overwritten after the first successful capture — drift comparison is deferred
// (feral-file#3485), the baseline exists so it can be enabled against real history later.
type MediaRenderProbe struct {
	// MediaURLHash is the MD5 hash of MediaURL (primary key, same keying as token_media_health)
	MediaURLHash string `gorm:"column:media_url_hash;primaryKey"`

	// MediaURL is the URL that was rendered
	MediaURL string `gorm:"column:media_url;not null;type:text"`

	// Phash is the 64-bit DCT perceptual hash of the latest capture (nil when capture failed)
	Phash *int64 `gorm:"column:phash"`

	// BaselinePhash is the pHash of the first successful capture, never overwritten
	BaselinePhash *int64 `gorm:"column:baseline_phash"`

	// EngineVersion is the browser identity (User-Agent) at capture time
	EngineVersion *string `gorm:"column:engine_version;type:text"`

	// Viewport is the capture viewport as "WxH"
	Viewport *string `gorm:"column:viewport;type:text"`

	// Verdict is the classification of the last probe
	Verdict RenderProbeVerdict `gorm:"column:verdict;not null;type:render_probe_verdict"`

	// ConsecutiveFailures counts successive blank/stalled probes (debounce state;
	// known_bad_fingerprint gates immediately and does not use this counter)
	ConsecutiveFailures int `gorm:"column:consecutive_failures;not null;default:0"`

	// HealthGated records whether this probe currently holds a render_% gate on the
	// URL's token_media_health rows.
	//
	// Reason: Verdict and ConsecutiveFailures are mutable, so deriving "is it gated" from
	// them loses the marker as soon as a later verdict overwrites the one that gated —
	// a fingerprint gate followed by a stall below the debounce threshold, for instance.
	// The health row would then stay broken forever, because L0 never re-checks render_%
	// rows and the probe no longer recognizes it as gated. Cleared only after a
	// successful release, so a failed release is retried rather than forgotten.
	HealthGated bool `gorm:"column:health_gated;not null;default:false"`

	// LastError is the render failure detail (nil on rendered_ok)
	LastError *string `gorm:"column:last_error;type:text"`

	// CapturedAt is the last successful screenshot time (nil when never captured)
	CapturedAt *time.Time `gorm:"column:captured_at;type:timestamptz"`

	// NextCheckAt is the sweeper's work-queue cursor
	NextCheckAt time.Time `gorm:"column:next_check_at;not null;default:now();type:timestamptz"`

	// Timestamps
	CreatedAt time.Time `gorm:"column:created_at;not null;default:now();type:timestamptz"`
	UpdatedAt time.Time `gorm:"column:updated_at;not null;default:now();type:timestamptz"`
}

// TableName specifies the table name for the MediaRenderProbe model
func (MediaRenderProbe) TableName() string {
	return "media_render_probes"
}

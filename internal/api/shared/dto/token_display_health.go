package dto

import (
	"mime"
	"strings"

	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

// ApplyHealthyMediaURLs filters the display URLs produced by MergeTokenDisplay against the
// token's known health rows so the API never returns a URL that is broken.
//
// Reason: MergeTokenDisplay is a pure metadata/enrichment merge with no health knowledge.
// A token can be viewable=true while animation_url points to a dead gateway (e.g. an IPFS
// gateway that returned 404 after the token was indexed). FF1 loads animation_url first for
// generative works, so this causes black-screen regressions (#96).
//
// Selection rule (mirrors MergeTokenDisplay precedence):
//   - animation_url: prefer enrichment-animation if healthy; else metadata-animation if healthy;
//     else nil (omit the field — do not serve a broken URL).
//   - image_url: prefer the healthy metadata image for OpenSea image playback,
//     then fall back to healthy enrichment. Animated works retain the vendor
//     poster because clients have no separate image MIME field.
//
// Constraints:
//   - If healthRows is empty (no rows yet probed) the display is returned unchanged so tokens
//     that have never been health-checked are not silently hidden.
//   - Only URLs whose health row is explicitly "healthy" are served. Both "broken" and "unknown"
//     rows suppress the URL. "unknown" rows are treated as unconfirmed — returning an unverified
//     URL would be a false positive, because the health state is indeterminate and the URL may
//     well be broken. A URL is only surfaced once the health probe has confirmed it is reachable.
func ApplyHealthyMediaURLs(display *TokenDisplayResponse, healthRows []schema.TokenMediaHealth) *TokenDisplayResponse {
	if display == nil {
		return nil
	}
	// No health data yet: return unchanged so unchecked tokens are still visible.
	if len(healthRows) == 0 {
		return display
	}

	// Index rows by source for O(1) lookup.
	type healthKey = schema.MediaHealthSource
	statusBySource := make(map[healthKey]schema.MediaHealthStatus, len(healthRows))
	urlBySource := make(map[healthKey]string, len(healthRows))
	mimeBySource := make(map[healthKey]*string, len(healthRows))
	for _, row := range healthRows {
		// When a source has multiple rows (shouldn't happen in normal operation, but
		// possible during propagation windows), prefer healthy over broken.
		existing, seen := statusBySource[row.MediaSource]
		if !seen || (existing != schema.MediaHealthStatusHealthy && row.HealthStatus == schema.MediaHealthStatusHealthy) {
			statusBySource[row.MediaSource] = row.HealthStatus
			urlBySource[row.MediaSource] = row.MediaURL
			mimeBySource[row.MediaSource] = row.ObservedContentType
		}
	}

	out := *display // shallow copy — we only replace pointer fields

	// --- animation_url ---
	// Precedence: enrichment-animation > metadata-animation > nil
	enrichAnimStatus, hasEnrichAnim := statusBySource[schema.MediaHealthSourceEnrichmentAnimation]
	metaAnimStatus, hasMetaAnim := statusBySource[schema.MediaHealthSourceMetadataAnimation]

	if hasEnrichAnim || hasMetaAnim {
		// At least one animation health row exists: apply health filtering.
		switch {
		case hasEnrichAnim && enrichAnimStatus == schema.MediaHealthStatusHealthy:
			enrichURL := urlBySource[schema.MediaHealthSourceEnrichmentAnimation]
			out.AnimationURL = &enrichURL
		case hasMetaAnim && metaAnimStatus == schema.MediaHealthStatusHealthy:
			metaURL := urlBySource[schema.MediaHealthSourceMetadataAnimation]
			out.AnimationURL = &metaURL
		default:
			// No confirmed-healthy animation URL found. This covers both "broken" (known dead)
			// and "unknown" (not yet probed) — omit rather than serve an unverified URL.
			out.AnimationURL = nil
		}
	}
	// If no animation health rows exist at all, keep the merged value unchanged (unchecked).

	// --- image_url ---
	// A healthy OpenSea copy is still useful when the original is unavailable.
	enrichImgStatus, hasEnrichImg := statusBySource[schema.MediaHealthSourceEnrichmentImage]
	metaImgStatus, hasMetaImg := statusBySource[schema.MediaHealthSourceMetadataImage]

	var imageSource healthKey
	if hasEnrichImg || hasMetaImg {
		switch {
		case out.preferMetadataImage && (out.AnimationURL == nil || *out.AnimationURL == "") && metaImgStatus == schema.MediaHealthStatusHealthy &&
			!isSVGImage(urlBySource[schema.MediaHealthSourceMetadataImage], mimeBySource[schema.MediaHealthSourceMetadataImage]) &&
			!isSVGImage(urlBySource[schema.MediaHealthSourceMetadataImage], out.metadataImageMIME):
			metaURL := urlBySource[schema.MediaHealthSourceMetadataImage]
			out.ImageURL = &metaURL
			imageSource = schema.MediaHealthSourceMetadataImage
		case hasEnrichImg && enrichImgStatus == schema.MediaHealthStatusHealthy:
			enrichURL := urlBySource[schema.MediaHealthSourceEnrichmentImage]
			out.ImageURL = &enrichURL
			imageSource = schema.MediaHealthSourceEnrichmentImage
		case hasMetaImg && metaImgStatus == schema.MediaHealthStatusHealthy:
			metaURL := urlBySource[schema.MediaHealthSourceMetadataImage]
			out.ImageURL = &metaURL
			imageSource = schema.MediaHealthSourceMetadataImage
		default:
			out.ImageURL = nil
		}
	}

	// A source-level MIME may describe its broken animation. Prefer the
	// selected image's own probe so extensionless originals remain decodable,
	// while keeping animation MIME intact whenever an animation survives.
	if out.preferMetadataImage && imageSource != "" && (out.AnimationURL == nil || *out.AnimationURL == "") {
		out.MimeType = out.metadataImageMIME
		if imageSource == schema.MediaHealthSourceEnrichmentImage {
			out.MimeType = out.enrichmentImageMIME
		}
		if observed := mimeBySource[imageSource]; observed != nil {
			if imageMIME, _, err := mime.ParseMediaType(*observed); err == nil && strings.HasPrefix(imageMIME, "image/") {
				out.MimeType = &imageMIME
			}
		}
	}

	return &out
}

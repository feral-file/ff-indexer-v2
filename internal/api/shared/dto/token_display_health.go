package dto

import "github.com/feral-file/ff-indexer-v2/internal/store/schema"

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
//   - image_url: same for image sources.
//
// Constraints:
//   - If healthRows is empty (no rows yet probed) the display is returned unchanged so tokens
//     that have never been health-checked are not silently hidden.
//   - Only rows with health_status = "broken" are filtered out; "unknown" rows are kept to
//     avoid hiding tokens whose URLs have not been checked yet.
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
	for _, row := range healthRows {
		// When a source has multiple rows (shouldn't happen in normal operation, but
		// possible during propagation windows), prefer healthy over broken.
		existing, seen := statusBySource[row.MediaSource]
		if !seen || (existing != schema.MediaHealthStatusHealthy && row.HealthStatus == schema.MediaHealthStatusHealthy) {
			statusBySource[row.MediaSource] = row.HealthStatus
			urlBySource[row.MediaSource] = row.MediaURL
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
			// All known animation URLs are broken — omit rather than serve a dead URL.
			out.AnimationURL = nil
		}
	}
	// If no animation health rows exist, keep the merged value unchanged (unchecked).

	// --- image_url ---
	// Precedence: enrichment-image > metadata-image > nil
	enrichImgStatus, hasEnrichImg := statusBySource[schema.MediaHealthSourceEnrichmentImage]
	metaImgStatus, hasMetaImg := statusBySource[schema.MediaHealthSourceMetadataImage]

	if hasEnrichImg || hasMetaImg {
		switch {
		case hasEnrichImg && enrichImgStatus == schema.MediaHealthStatusHealthy:
			enrichURL := urlBySource[schema.MediaHealthSourceEnrichmentImage]
			out.ImageURL = &enrichURL
		case hasMetaImg && metaImgStatus == schema.MediaHealthStatusHealthy:
			metaURL := urlBySource[schema.MediaHealthSourceMetadataImage]
			out.ImageURL = &metaURL
		default:
			out.ImageURL = nil
		}
	}

	return &out
}

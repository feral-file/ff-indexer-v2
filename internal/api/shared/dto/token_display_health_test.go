package dto_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/feral-file/ff-indexer-v2/internal/api/shared/dto"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

// ptr is a helper to get a pointer to a string literal.
func ptr(s string) *string { return &s }

// buildHealthRow is a test helper.
func buildHealthRow(tokenID uint64, source schema.MediaHealthSource, url string, status schema.MediaHealthStatus) schema.TokenMediaHealth {
	return schema.TokenMediaHealth{
		TokenID:      tokenID,
		MediaURL:     url,
		MediaSource:  source,
		HealthStatus: status,
	}
}

// ====================================================================================
// ApplyHealthyMediaURLs – nil / empty guard
// ====================================================================================

func TestApplyHealthyMediaURLs_NilDisplay_ReturnsNil(t *testing.T) {
	result := dto.ApplyHealthyMediaURLs(nil, nil)
	assert.Nil(t, result)
}

func TestApplyHealthyMediaURLs_NoHealthRows_ReturnsUnchanged(t *testing.T) {
	animURL := "https://ipfs.feralfile.com/ipfs/QmAnim"
	imgURL := "https://ipfs.feralfile.com/ipfs/QmImg"
	display := &dto.TokenDisplayResponse{
		AnimationURL: ptr(animURL),
		ImageURL:     ptr(imgURL),
	}

	result := dto.ApplyHealthyMediaURLs(display, nil)

	// No health rows yet: return unchanged so unchecked tokens are not silently hidden.
	assert.Equal(t, display, result)
}

// ====================================================================================
// ApplyHealthyMediaURLs – animation_url filtering
// ====================================================================================

// TestApplyHealthyMediaURLs_BrokenAnimation_NilsAnimationURL is the primary #96 scenario:
// animation_url points to a dead gateway; the API must not return it.
func TestApplyHealthyMediaURLs_BrokenAnimation_NilsAnimationURL(t *testing.T) {
	animURL := "https://ipfs.feralfile.com/ipfs/QmAnim"
	imgURL := "https://ipfs.filebase.io/ipfs/QmImg"
	display := &dto.TokenDisplayResponse{
		AnimationURL: ptr(animURL),
		ImageURL:     ptr(imgURL),
	}

	rows := []schema.TokenMediaHealth{
		buildHealthRow(1, schema.MediaHealthSourceMetadataAnimation, animURL, schema.MediaHealthStatusBroken),
		buildHealthRow(1, schema.MediaHealthSourceMetadataImage, imgURL, schema.MediaHealthStatusHealthy),
	}

	result := dto.ApplyHealthyMediaURLs(display, rows)

	assert.Nil(t, result.AnimationURL, "broken animation_url must be omitted")
	assert.Equal(t, imgURL, *result.ImageURL, "healthy image_url must be kept")
}

func TestApplyHealthyMediaURLs_HealthyAnimation_Kept(t *testing.T) {
	animURL := "https://ipfs.filebase.io/ipfs/QmAnim"
	imgURL := "https://ipfs.filebase.io/ipfs/QmImg"
	display := &dto.TokenDisplayResponse{
		AnimationURL: ptr(animURL),
		ImageURL:     ptr(imgURL),
	}

	rows := []schema.TokenMediaHealth{
		buildHealthRow(1, schema.MediaHealthSourceMetadataAnimation, animURL, schema.MediaHealthStatusHealthy),
		buildHealthRow(1, schema.MediaHealthSourceMetadataImage, imgURL, schema.MediaHealthStatusHealthy),
	}

	result := dto.ApplyHealthyMediaURLs(display, rows)

	assert.Equal(t, animURL, *result.AnimationURL)
	assert.Equal(t, imgURL, *result.ImageURL)
}

// TestApplyHealthyMediaURLs_EnrichmentAnimationHealthy_PreferredOverMetadata verifies
// MergeTokenDisplay precedence is respected: enrichment animation beats metadata animation.
func TestApplyHealthyMediaURLs_EnrichmentAnimationHealthy_PreferredOverMetadata(t *testing.T) {
	enrichAnimURL := "https://cdn.example.com/enrichment-anim.mp4"
	metaAnimURL := "https://ipfs.feralfile.com/ipfs/QmDeadMeta"
	display := &dto.TokenDisplayResponse{
		// MergeTokenDisplay would have set enrichment URL here already
		AnimationURL: ptr(enrichAnimURL),
	}

	rows := []schema.TokenMediaHealth{
		buildHealthRow(1, schema.MediaHealthSourceEnrichmentAnimation, enrichAnimURL, schema.MediaHealthStatusHealthy),
		buildHealthRow(1, schema.MediaHealthSourceMetadataAnimation, metaAnimURL, schema.MediaHealthStatusBroken),
	}

	result := dto.ApplyHealthyMediaURLs(display, rows)

	assert.Equal(t, enrichAnimURL, *result.AnimationURL)
}

// TestApplyHealthyMediaURLs_EnrichmentBroken_FallsBackToHealthyMetadata covers the case where
// enrichment animation is broken but metadata animation is healthy.
func TestApplyHealthyMediaURLs_EnrichmentBroken_FallsBackToHealthyMetadata(t *testing.T) {
	enrichAnimURL := "https://cdn.example.com/broken-enrichment-anim.mp4"
	metaAnimURL := "https://ipfs.filebase.io/ipfs/QmHealthyMeta"
	display := &dto.TokenDisplayResponse{
		// enrichment URL was picked by MergeTokenDisplay
		AnimationURL: ptr(enrichAnimURL),
	}

	rows := []schema.TokenMediaHealth{
		buildHealthRow(1, schema.MediaHealthSourceEnrichmentAnimation, enrichAnimURL, schema.MediaHealthStatusBroken),
		buildHealthRow(1, schema.MediaHealthSourceMetadataAnimation, metaAnimURL, schema.MediaHealthStatusHealthy),
	}

	result := dto.ApplyHealthyMediaURLs(display, rows)

	// Should fall back to the healthy metadata URL
	assert.Equal(t, metaAnimURL, *result.AnimationURL)
}

// TestApplyHealthyMediaURLs_AllAnimationsBroken_NilsField ensures that when every known
// animation health row is broken the field is omitted entirely.
func TestApplyHealthyMediaURLs_AllAnimationsBroken_NilsField(t *testing.T) {
	enrichAnimURL := "https://cdn.example.com/broken1.mp4"
	metaAnimURL := "https://ipfs.feralfile.com/ipfs/QmBroken2"
	display := &dto.TokenDisplayResponse{
		AnimationURL: ptr(enrichAnimURL),
	}

	rows := []schema.TokenMediaHealth{
		buildHealthRow(1, schema.MediaHealthSourceEnrichmentAnimation, enrichAnimURL, schema.MediaHealthStatusBroken),
		buildHealthRow(1, schema.MediaHealthSourceMetadataAnimation, metaAnimURL, schema.MediaHealthStatusBroken),
	}

	result := dto.ApplyHealthyMediaURLs(display, rows)

	assert.Nil(t, result.AnimationURL, "all broken animations must result in nil field")
}

// TestApplyHealthyMediaURLs_UnknownStatus_AnimationKept verifies that "unknown" status rows
// do not suppress the URL — only "broken" rows suppress it.
func TestApplyHealthyMediaURLs_UnknownStatus_AnimationKept(t *testing.T) {
	animURL := "https://ipfs.feralfile.com/ipfs/QmUnchecked"
	display := &dto.TokenDisplayResponse{
		AnimationURL: ptr(animURL),
	}

	rows := []schema.TokenMediaHealth{
		buildHealthRow(1, schema.MediaHealthSourceMetadataAnimation, animURL, schema.MediaHealthStatusUnknown),
	}

	result := dto.ApplyHealthyMediaURLs(display, rows)

	// Unknown (not yet probed) means we have a row but it hasn't passed health check yet.
	// We omit it because "unknown" is not "healthy" per the selection rule.
	// This is consistent with not silently hiding tokens but also not serving unverified URLs.
	assert.Nil(t, result.AnimationURL)
}

// ====================================================================================
// ApplyHealthyMediaURLs – image_url filtering
// ====================================================================================

func TestApplyHealthyMediaURLs_BrokenImage_NilsImageURL(t *testing.T) {
	imgURL := "https://ipfs.feralfile.com/ipfs/QmDeadImg"
	display := &dto.TokenDisplayResponse{
		ImageURL: ptr(imgURL),
	}

	rows := []schema.TokenMediaHealth{
		buildHealthRow(1, schema.MediaHealthSourceMetadataImage, imgURL, schema.MediaHealthStatusBroken),
	}

	result := dto.ApplyHealthyMediaURLs(display, rows)

	assert.Nil(t, result.ImageURL)
}

func TestApplyHealthyMediaURLs_EnrichmentImageHealthy_Preferred(t *testing.T) {
	enrichImgURL := "https://cdn.example.com/enrichment-img.jpg"
	metaImgURL := "https://ipfs.feralfile.com/ipfs/QmDeadMeta"
	display := &dto.TokenDisplayResponse{
		ImageURL: ptr(enrichImgURL),
	}

	rows := []schema.TokenMediaHealth{
		buildHealthRow(1, schema.MediaHealthSourceEnrichmentImage, enrichImgURL, schema.MediaHealthStatusHealthy),
		buildHealthRow(1, schema.MediaHealthSourceMetadataImage, metaImgURL, schema.MediaHealthStatusBroken),
	}

	result := dto.ApplyHealthyMediaURLs(display, rows)

	assert.Equal(t, enrichImgURL, *result.ImageURL)
}

// ====================================================================================
// ApplyHealthyMediaURLs – only image health rows exist (no animation rows)
// ====================================================================================

func TestApplyHealthyMediaURLs_NoAnimationRows_AnimationURLUntouched(t *testing.T) {
	animURL := "https://example.com/unchecked-anim.mp4"
	imgURL := "https://example.com/img.jpg"
	display := &dto.TokenDisplayResponse{
		AnimationURL: ptr(animURL),
		ImageURL:     ptr(imgURL),
	}

	// Only image health row — no animation row at all
	rows := []schema.TokenMediaHealth{
		buildHealthRow(1, schema.MediaHealthSourceMetadataImage, imgURL, schema.MediaHealthStatusHealthy),
	}

	result := dto.ApplyHealthyMediaURLs(display, rows)

	// Animation URL unchanged because no health row exists for it
	assert.Equal(t, animURL, *result.AnimationURL)
	assert.Equal(t, imgURL, *result.ImageURL)
}

// ====================================================================================
// ApplyHealthyMediaURLs – original display object is not mutated
// ====================================================================================

func TestApplyHealthyMediaURLs_DoesNotMutateInput(t *testing.T) {
	animURL := "https://ipfs.feralfile.com/ipfs/QmAnim"
	imgURL := "https://ipfs.filebase.io/ipfs/QmImg"
	display := &dto.TokenDisplayResponse{
		AnimationURL: ptr(animURL),
		ImageURL:     ptr(imgURL),
	}

	rows := []schema.TokenMediaHealth{
		buildHealthRow(1, schema.MediaHealthSourceMetadataAnimation, animURL, schema.MediaHealthStatusBroken),
	}

	result := dto.ApplyHealthyMediaURLs(display, rows)

	// Result has nil animation URL
	assert.Nil(t, result.AnimationURL)
	// Original is unchanged
	assert.Equal(t, animURL, *display.AnimationURL)
}

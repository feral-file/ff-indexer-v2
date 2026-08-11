package store

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

// TestComputeFinalModerationStatusPreservesUnknownVendorVerdict pins the
// rolling-upgrade case: a status this binary does not have a severity entry
// for (written by a newer deployment, e.g. right after a migration adds a new
// moderation kind but before every worker is redeployed) must not lose the
// "most severe" comparison to ModerationStatusNone's zero severity. Before the
// fix, an unrecognized status also ranked 0, tied with the loop's ModerationStatusNone
// starting value, and the strict ">" comparison never promoted it — silently
// unhiding a token a newer verdict said should stay hidden.
func TestComputeFinalModerationStatusPreservesUnknownVendorVerdict(t *testing.T) {
	future := schema.ModerationStatus("nsfw")

	rows := []schema.TokenModerationVerdict{
		{Source: schema.ModerationSourceOpenSea, Verdict: future},
	}

	final := computeFinalModerationStatus(rows)

	assert.Equal(t, future, final, "an unrecognized non-none verdict must be preserved, not silently downgraded to none")
	assert.True(t, final.IsModerated(), "a preserved unknown verdict must still hide the token")
}

// TestComputeFinalModerationStatusUnknownOutranksKnownSpam documents the
// fail-closed choice made alongside the fix above: when one source's verdict
// is unrecognized and another's is a known ModerationStatusSpam, the unknown
// verdict is the one materialized. Both hide the token (IsModerated is true
// either way), so this only affects which literal value tokens.moderation_status
// ends up holding until every writer understands the new status.
func TestComputeFinalModerationStatusUnknownOutranksKnownSpam(t *testing.T) {
	future := schema.ModerationStatus("nsfw")

	rows := []schema.TokenModerationVerdict{
		{Source: schema.ModerationSourceObjkt, Verdict: schema.ModerationStatusSpam},
		{Source: schema.ModerationSourceOpenSea, Verdict: future},
	}

	final := computeFinalModerationStatus(rows)

	assert.Equal(t, future, final)
}

// TestComputeFinalModerationStatusAllNoneStaysNone is the baseline: with no
// verdict rows (or only ModerationStatusNone rows), the token stays visible.
func TestComputeFinalModerationStatusAllNoneStaysNone(t *testing.T) {
	rows := []schema.TokenModerationVerdict{
		{Source: schema.ModerationSourceOpenSea, Verdict: schema.ModerationStatusNone},
		{Source: schema.ModerationSourceObjkt, Verdict: schema.ModerationStatusNone},
	}

	final := computeFinalModerationStatus(rows)

	assert.Equal(t, schema.ModerationStatusNone, final)
	assert.False(t, final.IsModerated())
}

// TestComputeFinalModerationStatusFeralFileWinsOverUnknown confirms the
// feralfile override still short-circuits ahead of everything else, including
// an unrecognized vendor status, per the function's documented precedence.
func TestComputeFinalModerationStatusFeralFileWinsOverUnknown(t *testing.T) {
	future := schema.ModerationStatus("nsfw")

	rows := []schema.TokenModerationVerdict{
		{Source: schema.ModerationSourceOpenSea, Verdict: future},
		{Source: schema.ModerationSourceFeralFile, Verdict: schema.ModerationStatusNone},
	}

	final := computeFinalModerationStatus(rows)

	assert.Equal(t, schema.ModerationStatusNone, final)
}

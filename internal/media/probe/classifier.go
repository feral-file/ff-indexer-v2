package probe

import (
	"fmt"
	"image"
	"strconv"
	"strings"

	"github.com/feral-file/ff-indexer-v2/internal/media/phash"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

// Fingerprint identifies a known-bad render: a page that paints fine but is not the
// artwork (gateway directory listing, error page, placeholder image). Operators capture
// the offending render once, record its pHash, and every URL rendering within
// MaxDistance of it is gated immediately — no baseline or history needed.
type Fingerprint struct {
	// Hash is the 64-bit DCT pHash of the known-bad render.
	Hash uint64
	// MaxDistance is the Hamming tolerance for a match (small: 4-8; a loose tolerance
	// here hides real art, the worst failure mode).
	MaxDistance int
	// Label names the fingerprint for last_error and operator triage.
	Label string
}

// ParseFingerprint converts an operator-supplied hex pHash (with or without 0x prefix)
// into a Fingerprint, rejecting values that cannot be a 64-bit hash so a config typo
// fails startup validation instead of silently never matching.
func ParseFingerprint(phashHex string, maxDistance int, label string) (Fingerprint, error) {
	cleaned := strings.TrimPrefix(strings.TrimSpace(strings.ToLower(phashHex)), "0x")
	if cleaned == "" || len(cleaned) > 16 {
		return Fingerprint{}, fmt.Errorf("fingerprint %q: phash must be 1-16 hex digits, got %q", label, phashHex)
	}
	hash, err := strconv.ParseUint(cleaned, 16, 64)
	if err != nil {
		return Fingerprint{}, fmt.Errorf("fingerprint %q: invalid phash hex %q: %w", label, phashHex, err)
	}
	if maxDistance < 0 || maxDistance > 64 {
		return Fingerprint{}, fmt.Errorf("fingerprint %q: max_distance must be 0-64, got %d", label, maxDistance)
	}
	return Fingerprint{Hash: hash, MaxDistance: maxDistance, Label: label}, nil
}

// Classification is the pure-function verdict over one captured frame.
type Classification struct {
	// Verdict is rendered_ok, blank, or known_bad_fingerprint ("stalled" is the caller's
	// mapping of a render error and never produced here — no frame, nothing to classify).
	Verdict schema.RenderProbeVerdict
	// Phash is the frame's 64-bit DCT perceptual hash, persisted with every capture.
	Phash uint64
	// Variance is the frame's normalized luminance variance (blank signal).
	Variance float64
	// MatchedLabel is the fingerprint label on a known_bad_fingerprint verdict ("" otherwise).
	MatchedLabel string
}

// Classify computes the frame's pHash and variance and applies the verdict ladder:
// known-bad fingerprint match first (unambiguous, gates immediately), then blank
// (near-uniform frame, debounced by the caller), else rendered_ok.
//
// Reason: fingerprint match precedes blank — error pages are usually *not* blank, and a
// blank fingerprint entry (if an operator ever adds one) should still report the more
// specific label.
func Classify(img image.Image, fingerprints []Fingerprint, blankVarianceThreshold float64) (Classification, error) {
	hash, err := phash.Compute(img)
	if err != nil {
		return Classification{}, err
	}

	c := Classification{
		Verdict:  schema.RenderProbeVerdictRenderedOK,
		Phash:    hash,
		Variance: phash.Variance(img),
	}

	for _, fp := range fingerprints {
		if phash.Distance(hash, fp.Hash) <= fp.MaxDistance {
			c.Verdict = schema.RenderProbeVerdictKnownBadFingerprint
			c.MatchedLabel = fp.Label
			return c, nil
		}
	}

	if c.Variance < blankVarianceThreshold {
		c.Verdict = schema.RenderProbeVerdictBlank
	}

	return c, nil
}

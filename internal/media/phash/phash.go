// Package phash computes 64-bit DCT perceptual hashes and frame statistics for the L1
// render probe.
//
// Reason: a screenshot alone cannot be compared or classified; the pHash gives renders a
// stable fingerprint (known-bad matching now, drift detection later per feral-file#3485)
// and the luminance variance detects blank frames without any baseline.
//
// Constraints: pure Go with no cgo dependency, deliberately — the CGO_ENABLED=0
// lightweight build must compile it, and a future CI visual-regression harness reuses it
// against committed fixtures. The hash algorithm is corona10/goimagehash's PerceptionHash
// (32x32 grayscale DCT, top-left 8x8, median threshold); the exact algorithm and bit
// order are part of the persisted contract — changing the library or its version
// invalidates every stored hash, so treat upgrades as re-baselining events.
package phash

import (
	"fmt"
	"image"
	"math/bits"

	"github.com/corona10/goimagehash"
)

// Compute returns the 64-bit DCT perceptual hash of img.
func Compute(img image.Image) (uint64, error) {
	h, err := goimagehash.PerceptionHash(img)
	if err != nil {
		return 0, fmt.Errorf("perception hash: %w", err)
	}
	return h.GetHash(), nil
}

// Distance returns the Hamming distance between two 64-bit perceptual hashes (0 =
// identical, 64 = every bit differs). Only meaningful when both hashes were computed by
// the same algorithm, engine, and viewport.
func Distance(a, b uint64) int {
	return bits.OnesCount64(a ^ b)
}

// MaxVariance is the upper bound of what Variance can return.
//
// Variance is a population variance over channel values normalized to [0,1], and such a
// variance is maximized at 0.25 by the extreme distribution (half the samples at 0, half
// at 1). Any blank threshold at or above this classifies every frame as blank, so callers
// validating operator input should reject thresholds outside [0, MaxVariance).
const MaxVariance = 0.25

// Variance returns the image's largest per-channel population variance, normalized to
// the [0,1] channel range (0 = perfectly uniform frame).
//
// Reason: blank-frame detection must not depend on a baseline or a fingerprint table — a
// uniform black, white, or solid-color capture has near-zero variance on first
// observation. The threshold is configuration (render_probe.blank_variance_threshold);
// this function only measures.
//
// Trade-offs: the maximum across R, G and B rather than luminance. Luma-only detection
// mistakes vivid art for a blank frame whenever the palette varies in hue but not
// brightness — a full-viewport #0af→#f0a gradient has a luma spread of 0.13 (variance
// ~0.0014, at the default threshold) despite being obviously non-blank, and false blanks
// hide real artwork, the worst outcome in this design. A frame is only blank when every
// channel is flat, so taking the max is both stricter and cheaper to reason about than
// summing channel variances. Caught by the live-chromium smoke test, which the mocked
// unit tests could not surface.
//
// Constraints: samples every 4th pixel in each dimension, keeping a 1024x1024 frame under
// ~66k samples with no meaningful loss for a uniformity test.
func Variance(img image.Image) float64 {
	bounds := img.Bounds()
	if bounds.Empty() {
		return 0
	}

	const stride = 4
	var sum, sumSq [3]float64
	var n int
	for y := bounds.Min.Y; y < bounds.Max.Y; y += stride {
		for x := bounds.Min.X; x < bounds.Max.X; x += stride {
			r, g, b, _ := img.At(x, y).RGBA()
			// 16-bit channels normalized to [0,1].
			channels := [3]float64{float64(r) / 65535.0, float64(g) / 65535.0, float64(b) / 65535.0}
			for i, v := range channels {
				sum[i] += v
				sumSq[i] += v * v
			}
			n++
		}
	}
	if n == 0 {
		return 0
	}

	var maxVar float64
	for i := range sum {
		mean := sum[i] / float64(n)
		if v := sumSq[i]/float64(n) - mean*mean; v > maxVar {
			maxVar = v
		}
	}
	return maxVar
}

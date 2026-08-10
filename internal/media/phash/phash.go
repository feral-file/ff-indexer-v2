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

// Variance returns the population variance of the image's luminance, normalized to the
// [0,1] luma range (0 = perfectly uniform frame).
//
// Reason: blank-frame detection must not depend on a baseline or a fingerprint table — a
// uniform black, white, or solid-color capture has near-zero variance on first
// observation. The threshold is configuration (render_probe.blank_variance_threshold);
// this function only measures. Sampling every 4th pixel in each dimension keeps a
// 1024x1024 frame under ~66k samples with no meaningful loss for a uniformity test.
func Variance(img image.Image) float64 {
	bounds := img.Bounds()
	if bounds.Empty() {
		return 0
	}

	const stride = 4
	var sum, sumSq float64
	var n int
	for y := bounds.Min.Y; y < bounds.Max.Y; y += stride {
		for x := bounds.Min.X; x < bounds.Max.X; x += stride {
			r, g, b, _ := img.At(x, y).RGBA()
			// Rec. 601 luma on 16-bit channels, normalized to [0,1].
			luma := (0.299*float64(r) + 0.587*float64(g) + 0.114*float64(b)) / 65535.0
			sum += luma
			sumSq += luma * luma
			n++
		}
	}
	if n == 0 {
		return 0
	}
	mean := sum / float64(n)
	return sumSq/float64(n) - mean*mean
}

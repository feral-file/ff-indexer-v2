package phash_test

import (
	"image"
	"image/color"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/media/phash"
)

// solidImage returns a uniform frame of the given color.
func solidImage(w, h int, c color.Color) image.Image {
	img := image.NewRGBA(image.Rect(0, 0, w, h))
	for y := range h {
		for x := range w {
			img.Set(x, y, c)
		}
	}
	return img
}

// gradientImage returns a horizontal black-to-white gradient.
func gradientImage(w, h int) image.Image {
	img := image.NewRGBA(image.Rect(0, 0, w, h))
	for y := range h {
		for x := range w {
			v := uint8(x * 255 / (w - 1)) // #nosec G115 -- bounded to 0-255 by construction (x < w)
			img.Set(x, y, color.RGBA{v, v, v, 255})
		}
	}
	return img
}

// checkerImage returns a high-contrast checkerboard with the given cell size.
func checkerImage(w, h, cell int) image.Image {
	img := image.NewRGBA(image.Rect(0, 0, w, h))
	for y := range h {
		for x := range w {
			if ((x/cell)+(y/cell))%2 == 0 {
				img.Set(x, y, color.White)
			} else {
				img.Set(x, y, color.Black)
			}
		}
	}
	return img
}

func TestCompute_deterministic(t *testing.T) {
	img := gradientImage(256, 256)
	h1, err := phash.Compute(img)
	require.NoError(t, err)
	h2, err := phash.Compute(img)
	require.NoError(t, err)
	assert.Equal(t, h1, h2, "same image must hash identically")
}

func TestCompute_distinguishesStructurallyDifferentImages(t *testing.T) {
	gradient, err := phash.Compute(gradientImage(256, 256))
	require.NoError(t, err)
	checker, err := phash.Compute(checkerImage(256, 256, 32))
	require.NoError(t, err)

	assert.NotEqual(t, gradient, checker)
	assert.Greater(t, phash.Distance(gradient, checker), 8,
		"structurally different images should be far apart")
}

func TestCompute_robustToScale(t *testing.T) {
	// The same visual content at different resolutions must land near each other —
	// that's the property that makes fingerprint matching viable across viewports.
	small, err := phash.Compute(gradientImage(128, 128))
	require.NoError(t, err)
	large, err := phash.Compute(gradientImage(512, 512))
	require.NoError(t, err)

	assert.LessOrEqual(t, phash.Distance(small, large), 6,
		"same content at different scales should be close")
}

func TestDistance(t *testing.T) {
	assert.Equal(t, 0, phash.Distance(0xFFFF, 0xFFFF))
	assert.Equal(t, 64, phash.Distance(0, ^uint64(0)))
	assert.Equal(t, 1, phash.Distance(0b1000, 0b0000))
	// Symmetry
	a, b := uint64(0x12345678), uint64(0x87654321)
	assert.Equal(t, phash.Distance(a, b), phash.Distance(b, a))
}

func TestVariance_blankFrames(t *testing.T) {
	// Uniform frames of any color are near-zero variance — the blank-detection signal.
	assert.InDelta(t, 0, phash.Variance(solidImage(256, 256, color.Black)), 1e-9)
	assert.InDelta(t, 0, phash.Variance(solidImage(256, 256, color.White)), 1e-9)
	assert.InDelta(t, 0, phash.Variance(solidImage(256, 256, color.RGBA{40, 0, 60, 255})), 1e-9)
}

func TestVariance_contentFrames(t *testing.T) {
	// Real content sits orders of magnitude above any sane blank threshold.
	assert.Greater(t, phash.Variance(gradientImage(256, 256)), 0.01)
	assert.Greater(t, phash.Variance(checkerImage(256, 256, 32)), 0.1)
}

func TestVariance_emptyImage(t *testing.T) {
	assert.Equal(t, 0.0, phash.Variance(image.NewRGBA(image.Rect(0, 0, 0, 0))))
}

package probe_test

import (
	"image"
	"image/color"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/media/phash"
	"github.com/feral-file/ff-indexer-v2/internal/media/probe"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

const blankThreshold = 0.001

func solidFrame(c color.Color) image.Image {
	img := image.NewRGBA(image.Rect(0, 0, 128, 128))
	for y := range 128 {
		for x := range 128 {
			img.Set(x, y, c)
		}
	}
	return img
}

func gradientFrame() image.Image {
	img := image.NewRGBA(image.Rect(0, 0, 128, 128))
	for y := range 128 {
		for x := range 128 {
			v := uint8(x * 255 / 127)
			img.Set(x, y, color.RGBA{v, uint8(y * 255 / 127), v, 255})
		}
	}
	return img
}

func TestClassify_renderedOK(t *testing.T) {
	c, err := probe.Classify(gradientFrame(), nil, blankThreshold)
	require.NoError(t, err)
	assert.Equal(t, schema.RenderProbeVerdictRenderedOK, c.Verdict)
	assert.NotZero(t, c.Phash)
	assert.Greater(t, c.Variance, blankThreshold)
	assert.Empty(t, c.MatchedLabel)
}

func TestClassify_blankFrames(t *testing.T) {
	for _, tc := range []struct {
		name  string
		frame image.Image
	}{
		{"black", solidFrame(color.Black)},
		{"white", solidFrame(color.White)},
		{"solid color", solidFrame(color.RGBA{12, 34, 56, 255})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := probe.Classify(tc.frame, nil, blankThreshold)
			require.NoError(t, err)
			assert.Equal(t, schema.RenderProbeVerdictBlank, c.Verdict)
		})
	}
}

func TestClassify_knownBadFingerprint(t *testing.T) {
	frame := gradientFrame()
	hash, err := phash.Compute(frame)
	require.NoError(t, err)

	fingerprints := []probe.Fingerprint{
		{Hash: hash, MaxDistance: 4, Label: "kubo-dir-listing"},
	}

	c, err := probe.Classify(frame, fingerprints, blankThreshold)
	require.NoError(t, err)
	assert.Equal(t, schema.RenderProbeVerdictKnownBadFingerprint, c.Verdict)
	assert.Equal(t, "kubo-dir-listing", c.MatchedLabel)
}

func TestClassify_fingerprintHammingBoundary(t *testing.T) {
	frame := gradientFrame()
	hash, err := phash.Compute(frame)
	require.NoError(t, err)

	// Flip exactly 5 bits: inside MaxDistance 5, outside MaxDistance 4.
	flipped := hash ^ 0b11111

	within := []probe.Fingerprint{{Hash: flipped, MaxDistance: 5, Label: "within"}}
	c, err := probe.Classify(frame, within, blankThreshold)
	require.NoError(t, err)
	assert.Equal(t, schema.RenderProbeVerdictKnownBadFingerprint, c.Verdict, "distance == MaxDistance matches")

	outside := []probe.Fingerprint{{Hash: flipped, MaxDistance: 4, Label: "outside"}}
	c, err = probe.Classify(frame, outside, blankThreshold)
	require.NoError(t, err)
	assert.Equal(t, schema.RenderProbeVerdictRenderedOK, c.Verdict, "distance > MaxDistance does not match")
}

func TestClassify_fingerprintPrecedesBlank(t *testing.T) {
	// A blank frame that also matches a fingerprint reports the more specific label.
	frame := solidFrame(color.Black)
	hash, err := phash.Compute(frame)
	require.NoError(t, err)

	c, err := probe.Classify(frame, []probe.Fingerprint{{Hash: hash, MaxDistance: 2, Label: "black-placeholder"}}, blankThreshold)
	require.NoError(t, err)
	assert.Equal(t, schema.RenderProbeVerdictKnownBadFingerprint, c.Verdict)
	assert.Equal(t, "black-placeholder", c.MatchedLabel)
}

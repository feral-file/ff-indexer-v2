//go:build cgo

package rasterizer_test

import (
	"bytes"
	"context"
	"errors"
	"image"
	"image/color"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/media/rasterizer"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
)

func init() {
	// Initialize logger for testing
	_ = logger.Initialize(logger.Config{
		Debug: true,
	})
}

const testSVG = `<?xml version="1.0" encoding="UTF-8"?>
<svg width="100" height="100" xmlns="http://www.w3.org/2000/svg">
  <rect width="100" height="100" fill="red"/>
  <circle cx="50" cy="50" r="30" fill="blue"/>
</svg>`

// createTestImage creates a simple test image
func createTestImage() image.Image {
	img := image.NewRGBA(image.Rect(0, 0, 100, 100))
	// Fill with red color
	for y := range 100 {
		for x := range 100 {
			img.Set(x, y, color.RGBA{R: 255, A: 255})
		}
	}
	return img
}

func TestNewRasterizer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockResvg := mocks.NewMockResvgClient(ctrl)
	mockEncoder := mocks.NewMockImageEncoder(ctrl)

	tests := []struct {
		name   string
		config *rasterizer.Config
	}{
		{
			name:   "with nil config",
			config: nil,
		},
		{
			name: "with custom config",
			config: &rasterizer.Config{
				Width: 1024,
			},
		},
		{
			name: "with zero width",
			config: &rasterizer.Config{
				Width: 0,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := rasterizer.NewRasterizer(mockResvg, mockEncoder, nil, tt.config)
			assert.NotNil(t, r, "NewRasterizer should return a non-nil rasterizer")
		})
	}
}

func TestRasterize_PNG(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockResvg := mocks.NewMockResvgClient(ctrl)
	mockEncoder := mocks.NewMockImageEncoder(ctrl)

	ctx := context.Background()
	testImg := createTestImage()

	// Setup expectations - using default width (0)
	mockResvg.EXPECT().
		Render([]byte(testSVG), 0).
		Return(testImg, nil)

	mockEncoder.EXPECT().
		EncodePNG(gomock.Any(), testImg).
		DoAndReturn(func(w *bytes.Buffer, img image.Image) error {
			// Write PNG signature
			w.Write([]byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A})
			return nil
		})

	r := rasterizer.NewRasterizer(mockResvg, mockEncoder, nil, nil)
	result, err := r.Rasterize(ctx, []byte(testSVG))

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Greater(t, len(result), 0)

	// PNG files start with specific magic bytes
	assert.Equal(t, byte(0x89), result[0])
	assert.Equal(t, byte(0x50), result[1]) // 'P'
	assert.Equal(t, byte(0x4E), result[2]) // 'N'
	assert.Equal(t, byte(0x47), result[3]) // 'G'
}

func TestRasterize_InvalidSVG(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockResvg := mocks.NewMockResvgClient(ctrl)
	mockEncoder := mocks.NewMockImageEncoder(ctrl)

	ctx := context.Background()
	invalidSVG := []byte("not an svg")

	mockResvg.EXPECT().
		Render(invalidSVG, 0).
		Return(nil, errors.New("invalid SVG"))

	r := rasterizer.NewRasterizer(mockResvg, mockEncoder, nil, nil)
	_, err := r.Rasterize(ctx, invalidSVG)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to render SVG")
}

func TestRasterize_EncodingError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockResvg := mocks.NewMockResvgClient(ctrl)
	mockEncoder := mocks.NewMockImageEncoder(ctrl)

	ctx := context.Background()
	testImg := createTestImage()

	// Setup expectations
	mockResvg.EXPECT().
		Render([]byte(testSVG), 0).
		Return(testImg, nil)

	mockEncoder.EXPECT().
		EncodePNG(gomock.Any(), testImg).
		Return(errors.New("encoding failed"))

	r := rasterizer.NewRasterizer(mockResvg, mockEncoder, nil, nil)
	_, err := r.Rasterize(ctx, []byte(testSVG))

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to encode PNG")
}

// TestRasterize_FeDisplacementMap_NoBrowser verifies that SVGs containing feDisplacementMap
// (the only confirmed SIGABRT trigger in resvg) return ErrUnsupportedSVGFilter when no browser
// is available, instead of calling resvg.Render and aborting the process.
// Note: feTurbulence and feComposite alone are not hard-blocked — they go through browser-preferred
// routing but fall back to resvg if no browser is available (non-fatal degraded output).
func TestRasterize_FeDisplacementMap_NoBrowser(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockResvg := mocks.NewMockResvgClient(ctrl)
	mockEncoder := mocks.NewMockImageEncoder(ctrl)
	// resvg.Render must NEVER be called for SVGs containing feDisplacementMap.
	mockResvg.EXPECT().Render(gomock.Any(), gomock.Any()).Times(0)

	crashSVGs := []struct {
		name string
		svg  string
	}{
		{
			name: "feDisplacementMap alone",
			svg:  `<svg xmlns="http://www.w3.org/2000/svg"><filter id="f"><feDisplacementMap in="SourceGraphic" scale="10"/></filter></svg>`,
		},
		{
			name: "feDisplacementMap+feTurbulence combination",
			svg:  `<svg xmlns="http://www.w3.org/2000/svg"><filter id="f"><feTurbulence type="fractalNoise" baseFrequency="0.9"/><feDisplacementMap in="SourceGraphic" scale="10"/></filter></svg>`,
		},
	}

	for _, tc := range crashSVGs {
		t.Run(tc.name, func(t *testing.T) {
			// Browser disabled: enableBrowser=false, browserRasterizer=nil.
			r := rasterizer.NewRasterizer(mockResvg, mockEncoder, nil, nil)
			_, err := r.Rasterize(context.Background(), []byte(tc.svg))
			require.ErrorIs(t, err, rasterizer.ErrUnsupportedSVGFilter,
				"must return ErrUnsupportedSVGFilter, not call resvg.Render which would SIGABRT")
		})
	}
}

// TestRasterize_FeDisplacementMap_MentionedButNotUsed verifies that the crash guard is
// XML-aware: the string "feDisplacementMap" appearing only in a comment or <desc> text node
// does NOT block the SVG — resvg is called normally because no actual element is present.
// Regression for the original strings.Contains over-blocking that would fail valid SVGs.
func TestRasterize_FeDisplacementMap_MentionedButNotUsed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockResvg := mocks.NewMockResvgClient(ctrl)
	mockEncoder := mocks.NewMockImageEncoder(ctrl)
	testImg := createTestImage()

	safeSVGs := []struct {
		name string
		svg  string
	}{
		{
			name: "feDisplacementMap in XML comment only",
			svg:  `<svg xmlns="http://www.w3.org/2000/svg"><!-- feDisplacementMap technique --><rect width="10" height="10" fill="red"/></svg>`,
		},
		{
			name: "feDisplacementMap in desc element only",
			svg:  `<svg xmlns="http://www.w3.org/2000/svg"><desc>Uses feDisplacementMap approach</desc><rect width="10" height="10" fill="blue"/></svg>`,
		},
	}

	for _, tc := range safeSVGs {
		t.Run(tc.name, func(t *testing.T) {
			svgBytes := []byte(tc.svg)
			// resvg MUST be called — text mention of feDisplacementMap is not a crash risk.
			mockResvg.EXPECT().Render(svgBytes, 0).Return(testImg, nil)
			mockEncoder.EXPECT().EncodePNG(gomock.Any(), testImg).DoAndReturn(func(w *bytes.Buffer, img image.Image) error {
				w.Write([]byte{0x89, 0x50, 0x4E, 0x47})
				return nil
			})
			r := rasterizer.NewRasterizer(mockResvg, mockEncoder, nil, nil)
			_, err := r.Rasterize(context.Background(), svgBytes)
			require.NoError(t, err, "feDisplacementMap text mention must not block resvg")
		})
	}
}

// TestRasterize_FeTurbulenceFeComposite_FallsBackToResvg verifies that SVGs containing
// feTurbulence or feComposite (without feDisplacementMap) are NOT hard-blocked — they attempt
// browser rendering first, then fall back to resvg when no browser is available. This avoids
// silently skipping valid SVGs that happen to use those filter primitives without hitting the
// confirmed crash path.
func TestRasterize_FeTurbulenceFeComposite_FallsBackToResvg(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockResvg := mocks.NewMockResvgClient(ctrl)
	mockEncoder := mocks.NewMockImageEncoder(ctrl)
	testImg := createTestImage()

	browserPreferredSVGs := []struct {
		name string
		svg  string
	}{
		{
			name: "feTurbulence alone",
			svg:  `<svg xmlns="http://www.w3.org/2000/svg"><filter id="f"><feTurbulence type="fractalNoise" baseFrequency="0.9"/></filter></svg>`,
		},
		{
			name: "feComposite alone",
			svg:  `<svg xmlns="http://www.w3.org/2000/svg"><filter id="f"><feComposite in="SourceGraphic" in2="BackgroundImage" operator="over"/></filter></svg>`,
		},
	}

	for _, tc := range browserPreferredSVGs {
		t.Run(tc.name, func(t *testing.T) {
			svgBytes := []byte(tc.svg)
			// No browser available: resvg.Render IS called as fallback (degraded but not fatal).
			mockResvg.EXPECT().Render(svgBytes, 0).Return(testImg, nil)
			mockEncoder.EXPECT().EncodePNG(gomock.Any(), testImg).DoAndReturn(func(w *bytes.Buffer, img image.Image) error {
				w.Write([]byte{0x89, 0x50, 0x4E, 0x47})
				return nil
			})
			r := rasterizer.NewRasterizer(mockResvg, mockEncoder, nil, nil)
			_, err := r.Rasterize(context.Background(), svgBytes)
			require.NoError(t, err, "feTurbulence/feComposite alone must not return ErrUnsupportedSVGFilter")
		})
	}
}

// TestRasterize_FeDisplacementMap_WithBrowser verifies that crash-inducing SVGs are forwarded
// to the browser renderer when one is available, and never sent to resvg.
func TestRasterize_FeDisplacementMap_WithBrowser(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockResvg := mocks.NewMockResvgClient(ctrl)
	mockEncoder := mocks.NewMockImageEncoder(ctrl)
	mockBrowser := mocks.NewMockBrowserRasterizer(ctrl)

	// resvg.Render must NEVER be called.
	mockResvg.EXPECT().Render(gomock.Any(), gomock.Any()).Times(0)

	svg := []byte(`<svg xmlns="http://www.w3.org/2000/svg"><filter id="f"><feDisplacementMap in="SourceGraphic" scale="10"/></filter></svg>`)
	expectedPNG := []byte{0x89, 0x50, 0x4E, 0x47}

	mockBrowser.EXPECT().
		RasterizeSVG(gomock.Any(), svg, 0).
		Return(expectedPNG, nil)

	r := rasterizer.NewRasterizer(mockResvg, mockEncoder, mockBrowser, &rasterizer.Config{EnableBrowserFallback: true})
	result, err := r.Rasterize(context.Background(), svg)
	require.NoError(t, err)
	require.Equal(t, expectedPNG, result)
}

// TestRasterize_StandardSVG_UsesResvg confirms that a plain SVG (no crash-inducing features,
// no browser-preference features) still takes the fast resvg path.
func TestRasterize_StandardSVG_UsesResvg(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockResvg := mocks.NewMockResvgClient(ctrl)
	mockEncoder := mocks.NewMockImageEncoder(ctrl)
	testImg := createTestImage()

	svg := []byte(`<svg xmlns="http://www.w3.org/2000/svg"><rect width="10" height="10" fill="red"/></svg>`)

	mockResvg.EXPECT().Render(svg, 0).Return(testImg, nil)
	mockEncoder.EXPECT().EncodePNG(gomock.Any(), testImg).DoAndReturn(func(w *bytes.Buffer, img image.Image) error {
		w.Write([]byte{0x89, 0x50, 0x4E, 0x47})
		return nil
	})

	r := rasterizer.NewRasterizer(mockResvg, mockEncoder, nil, nil)
	result, err := r.Rasterize(context.Background(), svg)
	require.NoError(t, err)
	require.NotEmpty(t, result)
}

func TestRasterize_WithCustomConfig(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockResvg := mocks.NewMockResvgClient(ctrl)
	mockEncoder := mocks.NewMockImageEncoder(ctrl)

	ctx := context.Background()
	testImg := createTestImage()

	// Test that custom config is passed to resvg
	customWidth := 1024

	mockResvg.EXPECT().
		Render([]byte(testSVG), customWidth).
		Return(testImg, nil)

	mockEncoder.EXPECT().
		EncodePNG(gomock.Any(), testImg).
		DoAndReturn(func(w *bytes.Buffer, img image.Image) error {
			w.Write([]byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A})
			return nil
		})

	r := rasterizer.NewRasterizer(mockResvg, mockEncoder, nil, &rasterizer.Config{
		Width: customWidth,
	})

	result, err := r.Rasterize(ctx, []byte(testSVG))

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Greater(t, len(result), 0)
}

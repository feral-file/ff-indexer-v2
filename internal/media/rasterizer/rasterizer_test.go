//go:build cgo

package rasterizer_test

import (
	"bytes"
	"context"
	"errors"
	"image"
	"image/color"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
			r := rasterizer.NewRasterizer(mockResvg, mockEncoder, tt.config)
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

	r := rasterizer.NewRasterizer(mockResvg, mockEncoder, nil)
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

	r := rasterizer.NewRasterizer(mockResvg, mockEncoder, nil)
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

	r := rasterizer.NewRasterizer(mockResvg, mockEncoder, nil)
	_, err := r.Rasterize(ctx, []byte(testSVG))

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to encode PNG")
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

	r := rasterizer.NewRasterizer(mockResvg, mockEncoder, &rasterizer.Config{
		Width: customWidth,
	})

	result, err := r.Rasterize(ctx, []byte(testSVG))

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Greater(t, len(result), 0)
}

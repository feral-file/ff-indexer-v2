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

func TestRasterize_UsesBrowserForForeignObject(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockResvg := mocks.NewMockResvgClient(ctrl)
	mockEncoder := mocks.NewMockImageEncoder(ctrl)
	mockBrowser := mocks.NewMockBrowserRasterizer(ctrl)

	ctx := context.Background()

	// SVG with foreignObject (requires browser) - must have closing > for detection
	foreignObjectSVG := `<svg width="200" height="200" xmlns="http://www.w3.org/2000/svg">
		<foreignObject> x="10" y="10" width="100" height="100">
			<div xmlns="http://www.w3.org/1999/xhtml">
				<p>HTML content</p>
			</div>
		</foreignObject>
	</svg>`

	// Should use browser (not resvg)
	mockBrowser.EXPECT().
		RasterizeSVG(ctx, []byte(foreignObjectSVG), 0).
		Return([]byte{0x89, 0x50, 0x4E, 0x47}, nil)

	// No resvg calls should be made

	r := rasterizer.NewRasterizer(mockResvg, mockEncoder, mockBrowser, &rasterizer.Config{
		EnableBrowserFallback: true,
	})

	result, err := r.Rasterize(ctx, []byte(foreignObjectSVG))

	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestRasterize_UsesBrowserForScript(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockResvg := mocks.NewMockResvgClient(ctrl)
	mockEncoder := mocks.NewMockImageEncoder(ctrl)
	mockBrowser := mocks.NewMockBrowserRasterizer(ctrl)

	ctx := context.Background()

	// SVG with script tag (requires browser)
	scriptSVG := `<svg width="200" height="200" xmlns="http://www.w3.org/2000/svg">
		<circle id="myCircle" cx="50" cy="50" r="30" fill="blue"/>
		<script>
			document.getElementById('myCircle').setAttribute('fill', 'red');
		</script>
	</svg>`

	// Should use browser
	mockBrowser.EXPECT().
		RasterizeSVG(ctx, []byte(scriptSVG), 0).
		Return([]byte{0x89, 0x50, 0x4E, 0x47}, nil)

	r := rasterizer.NewRasterizer(mockResvg, mockEncoder, mockBrowser, &rasterizer.Config{
		EnableBrowserFallback: true,
	})

	result, err := r.Rasterize(ctx, []byte(scriptSVG))

	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestRasterize_UsesBrowserForCSSAnimation(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockResvg := mocks.NewMockResvgClient(ctrl)
	mockEncoder := mocks.NewMockImageEncoder(ctrl)
	mockBrowser := mocks.NewMockBrowserRasterizer(ctrl)

	ctx := context.Background()

	// SVG with CSS animation (requires browser)
	animationSVG := `<svg width="200" height="200" xmlns="http://www.w3.org/2000/svg">
		<style>
			@keyframes rotate {
				from { transform: rotate(0deg); }
				to { transform: rotate(360deg); }
			}
			.spinning { animation: rotate 2s infinite; }
		</style>
		<circle class="spinning" cx="100" cy="100" r="50" fill="blue"/>
	</svg>`

	// Should use browser
	mockBrowser.EXPECT().
		RasterizeSVG(ctx, []byte(animationSVG), 0).
		Return([]byte{0x89, 0x50, 0x4E, 0x47}, nil)

	r := rasterizer.NewRasterizer(mockResvg, mockEncoder, mockBrowser, &rasterizer.Config{
		EnableBrowserFallback: true,
	})

	result, err := r.Rasterize(ctx, []byte(animationSVG))

	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestRasterize_UsesBrowserForSMILAnimation(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockResvg := mocks.NewMockResvgClient(ctrl)
	mockEncoder := mocks.NewMockImageEncoder(ctrl)
	mockBrowser := mocks.NewMockBrowserRasterizer(ctrl)

	ctx := context.Background()

	// SVG with SMIL animation (requires browser) - must have closing > for detection
	smilSVG := `<svg width="200" height="200" xmlns="http://www.w3.org/2000/svg">
		<circle cx="50" cy="50" r="30" fill="blue">
			<animate> attributeName="cx" from="50" to="150" dur="2s" repeatCount="indefinite"/>
		</circle>
	</svg>`

	// Should use browser
	mockBrowser.EXPECT().
		RasterizeSVG(ctx, []byte(smilSVG), 0).
		Return([]byte{0x89, 0x50, 0x4E, 0x47}, nil)

	r := rasterizer.NewRasterizer(mockResvg, mockEncoder, mockBrowser, &rasterizer.Config{
		EnableBrowserFallback: true,
	})

	result, err := r.Rasterize(ctx, []byte(smilSVG))

	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestRasterize_FallbackToResvgWhenBrowserDisabled(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockResvg := mocks.NewMockResvgClient(ctrl)
	mockEncoder := mocks.NewMockImageEncoder(ctrl)
	mockBrowser := mocks.NewMockBrowserRasterizer(ctrl)

	ctx := context.Background()
	testImg := createTestImage()

	// SVG that requires browser, but browser is disabled
	scriptSVG := `<svg width="200" height="200" xmlns="http://www.w3.org/2000/svg">
		<script>console.log('test');</script>
		<circle cx="50" cy="50" r="30" fill="blue"/>
	</svg>`

	// Should fall back to resvg
	mockResvg.EXPECT().
		Render([]byte(scriptSVG), 0).
		Return(testImg, nil)

	mockEncoder.EXPECT().
		EncodePNG(gomock.Any(), testImg).
		DoAndReturn(func(w *bytes.Buffer, img image.Image) error {
			w.Write([]byte{0x89, 0x50, 0x4E, 0x47})
			return nil
		})

	// Browser is disabled
	r := rasterizer.NewRasterizer(mockResvg, mockEncoder, mockBrowser, &rasterizer.Config{
		EnableBrowserFallback: false,
	})

	result, err := r.Rasterize(ctx, []byte(scriptSVG))

	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestRasterize_FallbackToResvgWhenBrowserNotConfigured(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockResvg := mocks.NewMockResvgClient(ctrl)
	mockEncoder := mocks.NewMockImageEncoder(ctrl)

	ctx := context.Background()
	testImg := createTestImage()

	// SVG that requires browser, but browser is nil
	scriptSVG := `<svg width="200" height="200" xmlns="http://www.w3.org/2000/svg">
		<script>console.log('test');</script>
		<circle cx="50" cy="50" r="30" fill="blue"/>
	</svg>`

	// Should fall back to resvg
	mockResvg.EXPECT().
		Render([]byte(scriptSVG), 0).
		Return(testImg, nil)

	mockEncoder.EXPECT().
		EncodePNG(gomock.Any(), testImg).
		DoAndReturn(func(w *bytes.Buffer, img image.Image) error {
			w.Write([]byte{0x89, 0x50, 0x4E, 0x47})
			return nil
		})

	// Browser is nil (not configured)
	r := rasterizer.NewRasterizer(mockResvg, mockEncoder, nil, &rasterizer.Config{
		EnableBrowserFallback: true,
	})

	result, err := r.Rasterize(ctx, []byte(scriptSVG))

	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestRasterize_BrowserError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockResvg := mocks.NewMockResvgClient(ctrl)
	mockEncoder := mocks.NewMockImageEncoder(ctrl)
	mockBrowser := mocks.NewMockBrowserRasterizer(ctrl)

	ctx := context.Background()

	// SVG that requires browser
	scriptSVG := `<svg width="200" height="200" xmlns="http://www.w3.org/2000/svg">
		<script>console.log('test');</script>
	</svg>`

	// Browser fails
	mockBrowser.EXPECT().
		RasterizeSVG(ctx, []byte(scriptSVG), 0).
		Return(nil, errors.New("browser rendering failed"))

	r := rasterizer.NewRasterizer(mockResvg, mockEncoder, mockBrowser, &rasterizer.Config{
		EnableBrowserFallback: true,
	})

	result, err := r.Rasterize(ctx, []byte(scriptSVG))

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "browser rendering failed")
}

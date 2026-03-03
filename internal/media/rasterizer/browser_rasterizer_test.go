package rasterizer_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/media/rasterizer"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
)

func TestNewBrowserRasterizer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockChromedp := mocks.NewMockChromedpClient(ctrl)
	mockXML := mocks.NewMockXML(ctrl)
	mockFS := mocks.NewMockFileSystem(ctrl)

	tests := []struct {
		name   string
		config *rasterizer.BrowserRasterizerConfig
		setup  func()
	}{
		{
			name:   "with nil config (uses defaults)",
			config: nil,
			setup: func() {
				mockChromedp.EXPECT().
					NewExecAllocator(gomock.Any(), gomock.Any()).
					Return(context.Background(), func() {})
			},
		},
		{
			name: "with custom config",
			config: &rasterizer.BrowserRasterizerConfig{
				Width:     1024,
				TimeoutMs: 5000,
			},
			setup: func() {
				mockChromedp.EXPECT().
					NewExecAllocator(gomock.Any(), gomock.Any()).
					Return(context.Background(), func() {})
			},
		},
		{
			name: "with headless false",
			config: &rasterizer.BrowserRasterizerConfig{
				Width:     800,
				TimeoutMs: 10000,
			},
			setup: func() {
				mockChromedp.EXPECT().
					NewExecAllocator(gomock.Any(), gomock.Any()).
					Return(context.Background(), func() {})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setup()

			rast := rasterizer.NewBrowserRasterizer(mockChromedp, mockXML, mockFS, tt.config)
			assert.NotNil(t, rast)
		})
	}
}

func TestBrowserRasterizer_RasterizeSVG_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	// SVG with explicit width/height (will be parsed, no browser call needed for dimensions)
	svgData := []byte(`<svg width="100" height="200"><circle cx="50" cy="50" r="40" fill="red"/></svg>`)

	mockChromedp := mocks.NewMockChromedpClient(ctrl)
	mockXML := mocks.NewMockXML(ctrl)
	mockFS := mocks.NewMockFileSystem(ctrl)

	// Setup: NewBrowserRasterizer calls NewExecAllocator
	allocCtx := context.Background()
	mockChromedp.EXPECT().
		NewExecAllocator(gomock.Any(), gomock.Any()).
		Return(allocCtx, func() {})

	// Setup: RasterizeSVG creates a browser context
	browserCtx := context.Background()
	mockChromedp.EXPECT().
		NewContext(gomock.Any()).
		Return(browserCtx, func() {})

	// Setup: parseSVGDimensions calls xml.Unmarshal
	mockXML.EXPECT().
		Unmarshal(svgData, gomock.Any()).
		DoAndReturn(func(data []byte, v interface{}) error {
			// Simulate successful XML parsing with width="100" height="200"
			svg := v.(*struct {
				Width   string `xml:"width,attr"`
				Height  string `xml:"height,attr"`
				ViewBox string `xml:"viewBox,attr"`
			})
			svg.Width = "100"
			svg.Height = "200"
			return nil
		})

	// Setup: Mock chromedp action methods
	mockChromedp.EXPECT().Navigate(gomock.Any()).Return(nil)
	mockChromedp.EXPECT().WaitReady("svg, body", gomock.Any()).Return(nil)
	mockChromedp.EXPECT().EmulateViewport(int64(800), int64(1600)).Return(nil)
	mockChromedp.EXPECT().Evaluate(gomock.Any(), gomock.Any()).Return(nil).Times(3) // Set dimensions, trigger resize, simulate click
	mockChromedp.EXPECT().Sleep(5 * time.Second).Return(nil)
	mockChromedp.EXPECT().FullScreenshot(gomock.Any(), 100).Return(nil)

	// Setup: Run is called with the actions (context + 8 actions = 9 arguments total)
	mockChromedp.EXPECT().
		Run(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	// Create rasterizer
	rast := rasterizer.NewBrowserRasterizer(mockChromedp, mockXML, mockFS, &rasterizer.BrowserRasterizerConfig{
		Width:     800,
		TimeoutMs: 10000,
	})

	// Execute
	_, err := rast.RasterizeSVG(ctx, svgData, 800)

	// Note: With mocks, chromedp.FullScreenshot can't populate the buffer
	// We verify that the call succeeded without errors, indicating proper flow
	require.NoError(t, err)
}

func TestBrowserRasterizer_RasterizeSVG_GetDimensionsError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	// SVG without dimensions - parsing will fail, so browser fallback will be called
	svgData := []byte(`<svg><circle cx="50" cy="50" r="40"/></svg>`)

	mockChromedp := mocks.NewMockChromedpClient(ctrl)
	mockXML := mocks.NewMockXML(ctrl)
	mockFS := mocks.NewMockFileSystem(ctrl)

	// Setup
	allocCtx := context.Background()
	mockChromedp.EXPECT().
		NewExecAllocator(gomock.Any(), gomock.Any()).
		Return(allocCtx, func() {})

	browserCtx := context.Background()
	mockChromedp.EXPECT().
		NewContext(gomock.Any()).
		Return(browserCtx, func() {})

	// Setup: XML parsing returns empty attributes (no width/height/viewBox)
	mockXML.EXPECT().
		Unmarshal(svgData, gomock.Any()).
		DoAndReturn(func(data []byte, v interface{}) error {
			// Simulate SVG without width/height/viewBox attributes
			return nil
		})

	// Since XML parsing fails, it will call getSVGDimensionsFromBrowser
	// Mock the action methods for browser dimension detection
	mockChromedp.EXPECT().Navigate(gomock.Any()).Return(nil)
	mockChromedp.EXPECT().WaitReady("svg, body", gomock.Any()).Return(nil)
	mockChromedp.EXPECT().Evaluate(gomock.Any(), gomock.Any()).Return(nil)

	// Simulate error getting dimensions from browser
	mockChromedp.EXPECT().
		Run(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(errors.New("failed to evaluate SVG dimensions"))

	// Create rasterizer
	rast := rasterizer.NewBrowserRasterizer(mockChromedp, mockXML, mockFS, nil)

	// Execute
	result, err := rast.RasterizeSVG(ctx, svgData, 800)

	// Verify
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to get SVG dimensions")
}

func TestBrowserRasterizer_RasterizeSVG_RenderError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	// SVG with dimensions (parsing succeeds, but rendering fails)
	svgData := []byte(`<svg width="100" height="200"><circle cx="50" cy="50" r="40"/></svg>`)

	mockChromedp := mocks.NewMockChromedpClient(ctrl)
	mockXML := mocks.NewMockXML(ctrl)
	mockFS := mocks.NewMockFileSystem(ctrl)

	// Setup
	allocCtx := context.Background()
	mockChromedp.EXPECT().
		NewExecAllocator(gomock.Any(), gomock.Any()).
		Return(allocCtx, func() {})

	browserCtx := context.Background()
	mockChromedp.EXPECT().
		NewContext(gomock.Any()).
		Return(browserCtx, func() {})

	// Setup: XML parsing succeeds
	mockXML.EXPECT().
		Unmarshal(svgData, gomock.Any()).
		DoAndReturn(func(data []byte, v interface{}) error {
			svg := v.(*struct {
				Width   string `xml:"width,attr"`
				Height  string `xml:"height,attr"`
				ViewBox string `xml:"viewBox,attr"`
			})
			svg.Width = "100"
			svg.Height = "200"
			return nil
		})

	// Setup: Mock action methods
	mockChromedp.EXPECT().Navigate(gomock.Any()).Return(nil)
	mockChromedp.EXPECT().WaitReady("svg, body", gomock.Any()).Return(nil)
	mockChromedp.EXPECT().EmulateViewport(int64(800), int64(1600)).Return(nil)
	mockChromedp.EXPECT().Evaluate(gomock.Any(), gomock.Any()).Return(nil).Times(3) // Set dimensions, trigger resize, simulate click
	mockChromedp.EXPECT().Sleep(5 * time.Second).Return(nil)
	mockChromedp.EXPECT().FullScreenshot(gomock.Any(), 100).Return(nil)

	// Dimensions are parsed from XML, so only one Run call (renderSVG) which fails
	mockChromedp.EXPECT().
		Run(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(errors.New("screenshot failed"))

	// Create rasterizer
	rast := rasterizer.NewBrowserRasterizer(mockChromedp, mockXML, mockFS, nil)

	// Execute
	result, err := rast.RasterizeSVG(ctx, svgData, 800)

	// Verify
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to render SVG with chromedp")
}

func TestBrowserRasterizer_RasterizeSVG_UsesDefaultWidth(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	// SVG with dimensions
	svgData := []byte(`<svg width="100" height="200"><circle cx="50" cy="50" r="40"/></svg>`)

	mockChromedp := mocks.NewMockChromedpClient(ctrl)
	mockXML := mocks.NewMockXML(ctrl)
	mockFS := mocks.NewMockFileSystem(ctrl)

	// Setup
	allocCtx := context.Background()
	mockChromedp.EXPECT().
		NewExecAllocator(gomock.Any(), gomock.Any()).
		Return(allocCtx, func() {})

	browserCtx := context.Background()
	mockChromedp.EXPECT().
		NewContext(gomock.Any()).
		Return(browserCtx, func() {})

	// Setup: XML parsing succeeds
	mockXML.EXPECT().
		Unmarshal(svgData, gomock.Any()).
		DoAndReturn(func(data []byte, v interface{}) error {
			svg := v.(*struct {
				Width   string `xml:"width,attr"`
				Height  string `xml:"height,attr"`
				ViewBox string `xml:"viewBox,attr"`
			})
			svg.Width = "100"
			svg.Height = "200"
			return nil
		})

	// Setup: Mock action methods - width=0 will use default 1024, so viewport is scaled
	mockChromedp.EXPECT().Navigate(gomock.Any()).Return(nil)
	mockChromedp.EXPECT().WaitReady("svg, body", gomock.Any()).Return(nil)
	mockChromedp.EXPECT().EmulateViewport(int64(1024), int64(2048)).Return(nil)
	mockChromedp.EXPECT().Evaluate(gomock.Any(), gomock.Any()).Return(nil).Times(3) // Set dimensions, trigger resize, simulate click
	mockChromedp.EXPECT().Sleep(5 * time.Second).Return(nil)
	mockChromedp.EXPECT().FullScreenshot(gomock.Any(), 100).Return(nil)

	// Mock Run call (dimensions parsed from XML, only one Run for rendering with context + 8 actions = 9 arguments)
	mockChromedp.EXPECT().Run(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

	// Create rasterizer with custom width
	rast := rasterizer.NewBrowserRasterizer(mockChromedp, mockXML, mockFS, &rasterizer.BrowserRasterizerConfig{
		Width:     1024,
		TimeoutMs: 10000,
	})

	// Execute with width=0 (should use configured default of 1024)
	_, err := rast.RasterizeSVG(ctx, svgData, 0)

	// Verify no error (width was used from config)
	require.NoError(t, err)
}

func TestBrowserRasterizer_Close(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockChromedp := mocks.NewMockChromedpClient(ctrl)
	mockXML := mocks.NewMockXML(ctrl)
	mockFS := mocks.NewMockFileSystem(ctrl)

	cancelCalled := false
	mockChromedp.EXPECT().
		NewExecAllocator(gomock.Any(), gomock.Any()).
		Return(context.Background(), func() { cancelCalled = true })

	rast := rasterizer.NewBrowserRasterizer(mockChromedp, mockXML, mockFS, nil)

	// Close should call the cancel function
	err := rast.Close()
	assert.NoError(t, err)
	assert.True(t, cancelCalled, "allocCancel should have been called")
}

func TestBrowserRasterizer_RasterizeSVG_LargeSVGUsesTempFile(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	// Create a large SVG that will exceed data URI limit (> 1MB when base64 encoded)
	// Base64 encoding increases size by ~33%, so raw SVG needs to be ~750KB+
	largeSVGContent := strings.Repeat("<circle cx='50' cy='50' r='40'/>", 30000) // ~900KB
	svgData := []byte(fmt.Sprintf(`<svg width="100" height="200">%s</svg>`, largeSVGContent))

	mockChromedp := mocks.NewMockChromedpClient(ctrl)
	mockXML := mocks.NewMockXML(ctrl)
	mockFS := mocks.NewMockFileSystem(ctrl)

	// Setup: NewBrowserRasterizer calls NewExecAllocator
	allocCtx := context.Background()
	mockChromedp.EXPECT().
		NewExecAllocator(gomock.Any(), gomock.Any()).
		Return(allocCtx, func() {})

	// Setup: RasterizeSVG creates a browser context
	browserCtx := context.Background()
	mockChromedp.EXPECT().
		NewContext(gomock.Any()).
		Return(browserCtx, func() {})

	// Setup: parseSVGDimensions calls xml.Unmarshal
	mockXML.EXPECT().
		Unmarshal(gomock.Any(), gomock.Any()).
		DoAndReturn(func(data []byte, v interface{}) error {
			svg := v.(*struct {
				Width   string `xml:"width,attr"`
				Height  string `xml:"height,attr"`
				ViewBox string `xml:"viewBox,attr"`
			})
			svg.Width = "100"
			svg.Height = "200"
			return nil
		})

	// Setup: Large SVG triggers temp file creation
	// Create a real temp file for the test
	realTempFile, err := os.CreateTemp("", "test-svg-*.html")
	require.NoError(t, err)
	defer func() {
		_ = os.Remove(realTempFile.Name())
	}()

	mockFS.EXPECT().
		CreateTemp("", "svg-*.html").
		Return(realTempFile, nil)

	mockFS.EXPECT().
		WriteFile(realTempFile, gomock.Any()).
		Return(100, nil)

	mockFS.EXPECT().
		Close(realTempFile).
		Return(nil)

	mockFS.EXPECT().
		Remove(gomock.Any()).
		Return(nil)

	// Setup: Mock chromedp action methods
	mockChromedp.EXPECT().Navigate(gomock.Any()).Return(nil)
	mockChromedp.EXPECT().WaitReady("svg, body", gomock.Any()).Return(nil)
	mockChromedp.EXPECT().EmulateViewport(int64(800), int64(1600)).Return(nil)
	mockChromedp.EXPECT().Evaluate(gomock.Any(), gomock.Any()).Return(nil).Times(3) // Set dimensions, trigger resize, simulate click
	mockChromedp.EXPECT().Sleep(5 * time.Second).Return(nil)
	mockChromedp.EXPECT().FullScreenshot(gomock.Any(), 100).Return(nil)

	mockChromedp.EXPECT().
		Run(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	// Create rasterizer
	rast := rasterizer.NewBrowserRasterizer(mockChromedp, mockXML, mockFS, &rasterizer.BrowserRasterizerConfig{
		Width:     800,
		TimeoutMs: 10000,
	})

	// Execute
	_, err = rast.RasterizeSVG(ctx, svgData, 800)

	require.NoError(t, err)
}

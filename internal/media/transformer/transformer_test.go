//go:build cgo

package transformer_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/config"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/media/transformer"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
)

func init() {
	// Initialize logger for testing
	_ = logger.Initialize(logger.Config{
		Debug: true,
	})
}

// Small 1x1 pixel JPEG (for testing)
var testJPEG = []byte{
	0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, 0x4A, 0x46, 0x49, 0x46, 0x00, 0x01,
	0x01, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0xFF, 0xDB, 0x00, 0x43,
	0x00, 0x08, 0x06, 0x06, 0x07, 0x06, 0x05, 0x08, 0x07, 0x07, 0x07, 0x09,
	0x09, 0x08, 0x0A, 0x0C, 0x14, 0x0D, 0x0C, 0x0B, 0x0B, 0x0C, 0x19, 0x12,
	0x13, 0x0F, 0x14, 0x1D, 0x1A, 0x1F, 0x1E, 0x1D, 0x1A, 0x1C, 0x1C, 0x20,
	0x24, 0x2E, 0x27, 0x20, 0x22, 0x2C, 0x23, 0x1C, 0x1C, 0x28, 0x37, 0x29,
	0x2C, 0x30, 0x31, 0x34, 0x34, 0x34, 0x1F, 0x27, 0x39, 0x3D, 0x38, 0x32,
	0x3C, 0x2E, 0x33, 0x34, 0x32, 0xFF, 0xC0, 0x00, 0x0B, 0x08, 0x00, 0x01,
	0x00, 0x01, 0x01, 0x01, 0x11, 0x00, 0xFF, 0xC4, 0x00, 0x14, 0x00, 0x01,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x03, 0xFF, 0xC4, 0x00, 0x14, 0x10, 0x01, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0xFF, 0xDA, 0x00, 0x08, 0x01, 0x01, 0x00, 0x00, 0x3F, 0x00,
	0x1F, 0xFF, 0xD9,
}

// Small 1x1 pixel PNG with alpha (for testing)
var testPNG = []byte{
	0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D,
	0x49, 0x48, 0x44, 0x52, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
	0x08, 0x06, 0x00, 0x00, 0x00, 0x1F, 0x15, 0xC4, 0x89, 0x00, 0x00, 0x00,
	0x0A, 0x49, 0x44, 0x41, 0x54, 0x78, 0x9C, 0x63, 0x00, 0x01, 0x00, 0x00,
	0x05, 0x00, 0x01, 0x0D, 0x0A, 0x2D, 0xB4, 0x00, 0x00, 0x00, 0x00, 0x49,
	0x45, 0x4E, 0x44, 0xAE, 0x42, 0x60, 0x82,
}

// createDefaultConfig creates a default test configuration
func createDefaultConfig() config.TransformConfig {
	return config.TransformConfig{
		TargetImageSize:           9 * 1024 * 1024,
		TargetImagePixels:         45000000, // 45MP
		MaxImageDimension:         3840,
		MaxAnimatedImageDimension: 2048,
		MinImageDimension:         800,
		MinAnimatedImageDimension: 400,
		ResizeStepPercentage:      20,
		InitialQuality:            95,
		MinQuality:                70,
		QualityStep:               5,
		MaxInputBytes:             100 * 1024 * 1024,
		MaxDecodedPixels:          50000000, // 50MP
		TransformTimeout:          60 * time.Second,
		WorkerConcurrency:         1, // Use 1 for deterministic tests
	}
}

func TestNewTransformer(t *testing.T) {
	tests := []struct {
		name   string
		config config.TransformConfig
	}{
		{
			name:   "with full config",
			config: createDefaultConfig(),
		},
		{
			name: "with minimal config",
			config: config.TransformConfig{
				WorkerConcurrency: 1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create new mocks for each subtest
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
			mockIO := mocks.NewMockIO(ctrl)
			mockVips := mocks.NewMockVipsClient(ctrl)

			// Mock Startup and Shutdown
			mockVips.EXPECT().Startup(gomock.Any()).Times(1)
			mockVips.EXPECT().Shutdown().Times(1)

			tr := transformer.NewTransformer(tt.config, mockHTTPClient, mockIO, mockVips)
			assert.NotNil(t, tr, "NewTransformer should return a non-nil transformer")

			// Test Close
			err := tr.Close()
			assert.NoError(t, err)
		})
	}
}

func TestTransform_Success_JPEG(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockIO := mocks.NewMockIO(ctrl)
	mockVips := mocks.NewMockVipsClient(ctrl)
	mockSource := mocks.NewMockVipsSource(ctrl)
	mockImage := mocks.NewMockVipsImage(ctrl)

	ctx := context.Background()
	sourceURL := "https://example.com/image.jpg"

	// Mock HTTP response
	mockResp := &http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(bytes.NewReader(testJPEG)),
	}

	mockHTTPClient.EXPECT().
		GetResponse(gomock.Any(), sourceURL, nil).
		Return(mockResp, nil)

	// Mock vips operations
	mockVips.EXPECT().Startup(gomock.Any()).Times(1)
	mockVips.EXPECT().NewSource(gomock.Any()).Return(mockSource)
	mockSource.EXPECT().Close().Times(1)

	mockVips.EXPECT().NewImageFromSource(mockSource, gomock.Any()).Return(mockImage, nil)

	mockImage.EXPECT().Width().Return(100).AnyTimes()
	mockImage.EXPECT().Height().Return(100).AnyTimes()
	mockImage.EXPECT().HasAlpha().Return(false)
	mockImage.EXPECT().Pages().Return(1).AnyTimes() // Still image
	mockImage.EXPECT().Close().Times(1)

	// Mock encode
	mockImage.EXPECT().JpegsaveBuffer(gomock.Any()).Return(testJPEG, nil)

	mockVips.EXPECT().Shutdown().Times(1)

	cfg := createDefaultConfig()
	tr := transformer.NewTransformer(cfg, mockHTTPClient, mockIO, mockVips)
	defer func() {
		_ = tr.Close()
	}()

	input := &transformer.TransformInput{
		SourceURL:  sourceURL,
		IsAnimated: false,
	}

	result, err := tr.Transform(ctx, input)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Greater(t, len(result.Data), 0)
	assert.Equal(t, "image/jpeg", result.ContentType)
	assert.Contains(t, result.Filename, ".jpg")
	assert.Equal(t, 100, result.Width)
	assert.Equal(t, 100, result.Height)
}

func TestTransform_Success_PNG_WithAlpha(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockIO := mocks.NewMockIO(ctrl)
	mockVips := mocks.NewMockVipsClient(ctrl)
	mockSource := mocks.NewMockVipsSource(ctrl)
	mockImage := mocks.NewMockVipsImage(ctrl)

	ctx := context.Background()
	sourceURL := "https://example.com/image.png"

	// Mock HTTP response
	mockResp := &http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(bytes.NewReader(testPNG)),
	}

	mockHTTPClient.EXPECT().
		GetResponse(gomock.Any(), sourceURL, nil).
		Return(mockResp, nil)

	// Mock vips operations
	mockVips.EXPECT().Startup(gomock.Any()).Times(1)
	mockVips.EXPECT().NewSource(gomock.Any()).Return(mockSource)
	mockSource.EXPECT().Close().Times(1)

	mockVips.EXPECT().NewImageFromSource(mockSource, gomock.Any()).Return(mockImage, nil)

	mockImage.EXPECT().Width().Return(100).AnyTimes()
	mockImage.EXPECT().Height().Return(100).AnyTimes()
	mockImage.EXPECT().HasAlpha().Return(true)      // PNG with alpha
	mockImage.EXPECT().Pages().Return(1).AnyTimes() // Still image
	mockImage.EXPECT().Close().Times(1)

	// Mock WebP encode (since it has alpha)
	mockImage.EXPECT().WebpsaveBuffer(gomock.Any()).Return(testPNG, nil)

	mockVips.EXPECT().Shutdown().Times(1)

	cfg := createDefaultConfig()
	tr := transformer.NewTransformer(cfg, mockHTTPClient, mockIO, mockVips)
	defer func() {
		_ = tr.Close()
	}()

	input := &transformer.TransformInput{
		SourceURL:  sourceURL,
		IsAnimated: false,
	}

	result, err := tr.Transform(ctx, input)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Greater(t, len(result.Data), 0)
	// PNG with alpha should be converted to WebP
	assert.Equal(t, "image/webp", result.ContentType)
	assert.Contains(t, result.Filename, ".webp")
}

func TestTransform_HTTPError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockIO := mocks.NewMockIO(ctrl)
	mockVips := mocks.NewMockVipsClient(ctrl)

	ctx := context.Background()
	sourceURL := "https://example.com/image.jpg"

	// Mock HTTP error
	mockHTTPClient.EXPECT().
		GetResponse(gomock.Any(), sourceURL, nil).
		Return(nil, errors.New("network error"))

	mockVips.EXPECT().Startup(gomock.Any()).Times(1)
	mockVips.EXPECT().Shutdown().Times(1)

	cfg := createDefaultConfig()
	tr := transformer.NewTransformer(cfg, mockHTTPClient, mockIO, mockVips)
	defer func() {
		_ = tr.Close()
	}()

	input := &transformer.TransformInput{
		SourceURL:  sourceURL,
		IsAnimated: false,
	}

	_, err := tr.Transform(ctx, input)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get response")
}

func TestTransform_HTTPStatusError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockIO := mocks.NewMockIO(ctrl)
	mockVips := mocks.NewMockVipsClient(ctrl)

	ctx := context.Background()
	sourceURL := "https://example.com/notfound.jpg"

	// Mock HTTP 404 response
	mockResp := &http.Response{
		StatusCode: 404,
		Body:       io.NopCloser(bytes.NewReader([]byte{})),
	}

	mockHTTPClient.EXPECT().
		GetResponse(gomock.Any(), sourceURL, nil).
		Return(mockResp, nil)

	mockVips.EXPECT().Startup(gomock.Any()).Times(1)
	mockVips.EXPECT().Shutdown().Times(1)

	cfg := createDefaultConfig()
	tr := transformer.NewTransformer(cfg, mockHTTPClient, mockIO, mockVips)
	defer func() {
		_ = tr.Close()
	}()

	input := &transformer.TransformInput{
		SourceURL:  sourceURL,
		IsAnimated: false,
	}

	_, err := tr.Transform(ctx, input)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected status code: 404")
}

func TestTransform_InputTooLarge(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockIO := mocks.NewMockIO(ctrl)
	mockVips := mocks.NewMockVipsClient(ctrl)

	ctx := context.Background()
	sourceURL := "https://example.com/huge.jpg"

	// Mock response with size exceeding MaxInputBytes
	mockResp := &http.Response{
		StatusCode:    200,
		ContentLength: 200 * 1024 * 1024, // 200MB
		Body:          io.NopCloser(bytes.NewReader(testJPEG)),
	}

	mockHTTPClient.EXPECT().
		GetResponse(gomock.Any(), sourceURL, nil).
		Return(mockResp, nil)

	mockVips.EXPECT().Startup(gomock.Any()).Times(1)
	mockVips.EXPECT().Shutdown().Times(1)

	cfg := createDefaultConfig()
	cfg.MaxInputBytes = 100 * 1024 * 1024 // 100MB limit
	tr := transformer.NewTransformer(cfg, mockHTTPClient, mockIO, mockVips)
	defer func() {
		_ = tr.Close()
	}()

	input := &transformer.TransformInput{
		SourceURL:  sourceURL,
		IsAnimated: false,
	}

	_, err := tr.Transform(ctx, input)

	assert.Error(t, err)
	assert.ErrorIs(t, err, transformer.ErrInputTooLarge)
}

func TestTransform_InvalidImage(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockIO := mocks.NewMockIO(ctrl)
	mockVips := mocks.NewMockVipsClient(ctrl)
	mockSource := mocks.NewMockVipsSource(ctrl)

	ctx := context.Background()
	sourceURL := "https://example.com/corrupted.jpg"

	// Mock response with invalid image data
	invalidData := []byte("not an image")
	mockResp := &http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(bytes.NewReader(invalidData)),
	}

	mockHTTPClient.EXPECT().
		GetResponse(gomock.Any(), sourceURL, nil).
		Return(mockResp, nil)

	// Mock vips operations - loading will fail
	mockVips.EXPECT().Startup(gomock.Any()).Times(1)
	mockVips.EXPECT().NewSource(gomock.Any()).Return(mockSource)
	mockSource.EXPECT().Close().Times(1)

	mockVips.EXPECT().NewImageFromSource(mockSource, gomock.Any()).Return(nil, errors.New("invalid image format"))

	mockVips.EXPECT().Shutdown().Times(1)

	cfg := createDefaultConfig()
	tr := transformer.NewTransformer(cfg, mockHTTPClient, mockIO, mockVips)
	defer func() {
		_ = tr.Close()
	}()

	input := &transformer.TransformInput{
		SourceURL:  sourceURL,
		IsAnimated: false,
	}

	_, err := tr.Transform(ctx, input)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to load image")
}

func TestTransform_ContextTimeout(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockIO := mocks.NewMockIO(ctrl)
	mockVips := mocks.NewMockVipsClient(ctrl)

	ctx := context.Background()
	sourceURL := "https://example.com/image.jpg"

	// Mock slow HTTP response that will exceed timeout
	mockHTTPClient.EXPECT().
		GetResponse(gomock.Any(), sourceURL, nil).
		DoAndReturn(func(ctx context.Context, url string, headers map[string]string) (*http.Response, error) {
			// Sleep longer than the transform timeout
			time.Sleep(100 * time.Millisecond)

			return nil, context.DeadlineExceeded
		})

	mockVips.EXPECT().Startup(gomock.Any()).Times(1)
	mockVips.EXPECT().Shutdown().Times(1)

	cfg := createDefaultConfig()
	cfg.TransformTimeout = 10 * time.Millisecond // Very short timeout
	tr := transformer.NewTransformer(cfg, mockHTTPClient, mockIO, mockVips)
	defer func() {
		_ = tr.Close()
	}()

	input := &transformer.TransformInput{
		SourceURL:  sourceURL,
		IsAnimated: false,
	}

	_, err := tr.Transform(ctx, input)

	assert.Error(t, err)
	// Context deadline should have been exceeded
	assert.True(t, errors.Is(err, context.DeadlineExceeded) || err != nil)
}

func TestTransform_AnimatedImage(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockIO := mocks.NewMockIO(ctrl)
	mockVips := mocks.NewMockVipsClient(ctrl)
	mockSource := mocks.NewMockVipsSource(ctrl)
	mockImage := mocks.NewMockVipsImage(ctrl)

	ctx := context.Background()
	sourceURL := "https://example.com/animation.gif"

	// Mock HTTP response
	mockResp := &http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(bytes.NewReader(testJPEG)),
	}

	mockHTTPClient.EXPECT().
		GetResponse(gomock.Any(), sourceURL, nil).
		Return(mockResp, nil)

	// Mock vips operations
	mockVips.EXPECT().Startup(gomock.Any()).Times(1)
	mockVips.EXPECT().NewSource(gomock.Any()).Return(mockSource)
	mockSource.EXPECT().Close().Times(1)

	mockVips.EXPECT().NewImageFromSource(mockSource, gomock.Any()).Return(mockImage, nil)

	mockImage.EXPECT().Width().Return(100).AnyTimes()
	mockImage.EXPECT().Height().Return(100).AnyTimes()
	mockImage.EXPECT().Pages().Return(10).AnyTimes()      // Multiple pages = animated
	mockImage.EXPECT().PageHeight().Return(10).AnyTimes() // Height of each frame
	mockImage.EXPECT().Close().Times(1)

	// Mock WebP encode (animated images always use WebP)
	mockImage.EXPECT().WebpsaveBuffer(gomock.Any()).Return(testJPEG, nil)

	mockVips.EXPECT().Shutdown().Times(1)

	cfg := createDefaultConfig()
	tr := transformer.NewTransformer(cfg, mockHTTPClient, mockIO, mockVips)
	defer func() {
		_ = tr.Close()
	}()

	input := &transformer.TransformInput{
		SourceURL:  sourceURL,
		IsAnimated: true, // Mark as animated
	}

	result, err := tr.Transform(ctx, input)

	require.NoError(t, err)
	require.NotNil(t, result)
	// Animated images should always output WebP
	assert.Equal(t, "image/webp", result.ContentType)
	assert.Contains(t, result.Filename, ".webp")
}

func TestTransform_NilInput(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockIO := mocks.NewMockIO(ctrl)
	mockVips := mocks.NewMockVipsClient(ctrl)

	ctx := context.Background()

	mockVips.EXPECT().Startup(gomock.Any()).Times(1)
	mockVips.EXPECT().Shutdown().Times(1)

	cfg := createDefaultConfig()
	tr := transformer.NewTransformer(cfg, mockHTTPClient, mockIO, mockVips)
	defer func() {
		_ = tr.Close()
	}()

	_, err := tr.Transform(ctx, nil)

	assert.Error(t, err)
}

func TestTransform_EmptySourceURL(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockIO := mocks.NewMockIO(ctrl)
	mockVips := mocks.NewMockVipsClient(ctrl)

	ctx := context.Background()

	mockVips.EXPECT().Startup(gomock.Any()).Times(1)
	mockVips.EXPECT().Shutdown().Times(1)

	cfg := createDefaultConfig()
	tr := transformer.NewTransformer(cfg, mockHTTPClient, mockIO, mockVips)
	defer func() {
		_ = tr.Close()
	}()

	input := &transformer.TransformInput{
		SourceURL:  "",
		IsAnimated: false,
	}

	_, err := tr.Transform(ctx, input)

	assert.Error(t, err)
}

func TestTransform_TooManyPixels(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockIO := mocks.NewMockIO(ctrl)
	mockVips := mocks.NewMockVipsClient(ctrl)
	mockSource := mocks.NewMockVipsSource(ctrl)
	mockImage := mocks.NewMockVipsImage(ctrl)

	ctx := context.Background()
	sourceURL := "https://example.com/huge-resolution.jpg"

	// Mock HTTP response with small file size but huge dimensions
	mockResp := &http.Response{
		StatusCode:    200,
		ContentLength: 1024, // Small file
		Body:          io.NopCloser(bytes.NewReader(testJPEG)),
	}

	mockHTTPClient.EXPECT().
		GetResponse(gomock.Any(), sourceURL, nil).
		Return(mockResp, nil)

	// Mock vips operations
	mockVips.EXPECT().Startup(gomock.Any()).Times(1)
	mockVips.EXPECT().NewSource(gomock.Any()).Return(mockSource)
	mockSource.EXPECT().Close().Times(1)

	mockVips.EXPECT().NewImageFromSource(mockSource, gomock.Any()).Return(mockImage, nil)

	// Return dimensions that exceed max pixels (300MP > 50MP default)
	mockImage.EXPECT().Width().Return(20000).AnyTimes()
	mockImage.EXPECT().Height().Return(15000).AnyTimes()
	mockImage.EXPECT().HasAlpha().Return(false)     // Needed before pixel check
	mockImage.EXPECT().Pages().Return(1).AnyTimes() // Still image
	mockImage.EXPECT().Close().Times(1)

	mockVips.EXPECT().Shutdown().Times(1)

	cfg := createDefaultConfig()
	tr := transformer.NewTransformer(cfg, mockHTTPClient, mockIO, mockVips)
	defer func() {
		_ = tr.Close()
	}()

	input := &transformer.TransformInput{
		SourceURL:  sourceURL,
		IsAnimated: false,
	}

	_, err := tr.Transform(ctx, input)

	assert.Error(t, err)
	assert.ErrorIs(t, err, transformer.ErrTooManyPixels)
}

func TestTransform_FirstEncodingMeetsTarget(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockIO := mocks.NewMockIO(ctrl)
	mockVips := mocks.NewMockVipsClient(ctrl)
	mockSource := mocks.NewMockVipsSource(ctrl)
	mockImage := mocks.NewMockVipsImage(ctrl)

	ctx := context.Background()
	sourceURL := "https://example.com/image.jpg"

	// Mock HTTP response
	mockResp := &http.Response{
		StatusCode:    200,
		ContentLength: 5 * 1024 * 1024, // 5MB
		Body:          io.NopCloser(bytes.NewReader(testJPEG)),
	}

	mockHTTPClient.EXPECT().
		GetResponse(gomock.Any(), sourceURL, nil).
		Return(mockResp, nil)

	// Mock vips operations
	mockVips.EXPECT().Startup(gomock.Any()).Times(1)
	mockVips.EXPECT().NewSource(gomock.Any()).Return(mockSource)
	mockSource.EXPECT().Close().Times(1)

	mockVips.EXPECT().NewImageFromSource(mockSource, gomock.Any()).Return(mockImage, nil)

	mockImage.EXPECT().Width().Return(2000).AnyTimes()
	mockImage.EXPECT().Height().Return(1500).AnyTimes()
	mockImage.EXPECT().HasAlpha().Return(false)
	mockImage.EXPECT().Pages().Return(1).AnyTimes() // Still image
	mockImage.EXPECT().Close().Times(1)

	// First encoding already meets target (no resize/compress needed)
	smallOutput := make([]byte, 3*1024*1024) // 3MB < 9MB target
	mockImage.EXPECT().JpegsaveBuffer(gomock.Any()).Return(smallOutput, nil).Times(1)

	mockVips.EXPECT().Shutdown().Times(1)

	cfg := createDefaultConfig()
	tr := transformer.NewTransformer(cfg, mockHTTPClient, mockIO, mockVips)
	defer func() {
		_ = tr.Close()
	}()

	input := &transformer.TransformInput{
		SourceURL:  sourceURL,
		IsAnimated: false,
	}

	result, err := tr.Transform(ctx, input)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, 2000, result.Width)
	assert.Equal(t, 1500, result.Height)
	assert.LessOrEqual(t, result.TransformedSize, int64(3*1024*1024))
}

func TestTransform_ResizeMeetsTarget(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockIO := mocks.NewMockIO(ctrl)
	mockVips := mocks.NewMockVipsClient(ctrl)
	mockSource := mocks.NewMockVipsSource(ctrl)
	mockImage := mocks.NewMockVipsImage(ctrl)

	ctx := context.Background()
	sourceURL := "https://example.com/large-image.jpg"

	// Mock HTTP response
	mockResp := &http.Response{
		StatusCode:    200,
		ContentLength: 15 * 1024 * 1024, // 15MB > 9MB target
		Body:          io.NopCloser(bytes.NewReader(testJPEG)),
	}

	mockHTTPClient.EXPECT().
		GetResponse(gomock.Any(), sourceURL, nil).
		Return(mockResp, nil)

	// Mock vips operations
	mockVips.EXPECT().Startup(gomock.Any()).Times(1)
	mockVips.EXPECT().NewSource(gomock.Any()).Return(mockSource)
	mockSource.EXPECT().Close().Times(1)

	mockVips.EXPECT().NewImageFromSource(mockSource, gomock.Any()).Return(mockImage, nil)

	mockImage.EXPECT().Width().Return(4000).AnyTimes()
	mockImage.EXPECT().Height().Return(3000).AnyTimes()
	mockImage.EXPECT().HasAlpha().Return(false)
	mockImage.EXPECT().Pages().Return(1).AnyTimes() // Still image
	mockImage.EXPECT().Close().Times(1)

	// First encoding is too large (15MB)
	// After resize, meets target (7MB < 9MB)
	largeOutput := make([]byte, 15*1024*1024)
	resizedOutput := make([]byte, 7*1024*1024)

	gomock.InOrder(
		// First encode - too large
		mockImage.EXPECT().JpegsaveBuffer(gomock.Any()).Return(largeOutput, nil).Times(1),
		// Resize once
		mockImage.EXPECT().Resize(gomock.Any(), gomock.Any()).Return(nil).Times(1),
		// Second encode - now fits
		mockImage.EXPECT().JpegsaveBuffer(gomock.Any()).Return(resizedOutput, nil).Times(1),
	)

	mockVips.EXPECT().Shutdown().Times(1)

	cfg := createDefaultConfig()
	tr := transformer.NewTransformer(cfg, mockHTTPClient, mockIO, mockVips)
	defer func() {
		_ = tr.Close()
	}()

	input := &transformer.TransformInput{
		SourceURL:  sourceURL,
		IsAnimated: false,
	}

	result, err := tr.Transform(ctx, input)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, int64(len(resizedOutput)), result.TransformedSize)
}

func TestTransform_CompressionMeetsTarget(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockIO := mocks.NewMockIO(ctrl)
	mockVips := mocks.NewMockVipsClient(ctrl)
	mockSource := mocks.NewMockVipsSource(ctrl)
	mockImage := mocks.NewMockVipsImage(ctrl)

	ctx := context.Background()
	sourceURL := "https://example.com/image.jpg"

	// Mock HTTP response
	mockResp := &http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(bytes.NewReader(testJPEG)),
	}

	mockHTTPClient.EXPECT().
		GetResponse(gomock.Any(), sourceURL, nil).
		Return(mockResp, nil)

	// Mock vips operations
	mockVips.EXPECT().Startup(gomock.Any()).Times(1)
	mockVips.EXPECT().NewSource(gomock.Any()).Return(mockSource)
	mockSource.EXPECT().Close().Times(1)

	mockVips.EXPECT().NewImageFromSource(mockSource, gomock.Any()).Return(mockImage, nil)

	// At minimum dimension
	mockImage.EXPECT().Width().Return(600).AnyTimes()
	mockImage.EXPECT().Height().Return(600).AnyTimes()
	mockImage.EXPECT().HasAlpha().Return(false)
	mockImage.EXPECT().Pages().Return(1).AnyTimes() // Still image
	mockImage.EXPECT().Close().Times(1)

	// Simulate compression iterations
	// Quality 95: 11MB (too large)
	// Quality 85: 10MB (still too large)
	// Quality 75: 8MB (meets target)
	output1 := make([]byte, 11*1024*1024)
	output2 := make([]byte, 10*1024*1024)
	output3 := make([]byte, 8*1024*1024)

	gomock.InOrder(
		mockImage.EXPECT().JpegsaveBuffer(gomock.Any()).Return(output1, nil).Times(1),
		mockImage.EXPECT().JpegsaveBuffer(gomock.Any()).Return(output2, nil).Times(1),
		mockImage.EXPECT().JpegsaveBuffer(gomock.Any()).Return(output3, nil).Times(1),
	)

	mockVips.EXPECT().Shutdown().Times(1)

	cfg := createDefaultConfig()
	cfg.MinImageDimension = 600 // Can't resize below this
	tr := transformer.NewTransformer(cfg, mockHTTPClient, mockIO, mockVips)
	defer func() {
		_ = tr.Close()
	}()

	input := &transformer.TransformInput{
		SourceURL:  sourceURL,
		IsAnimated: false,
	}

	result, err := tr.Transform(ctx, input)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, int64(len(output3)), result.TransformedSize)
}

func TestTransform_MultipleResizeIterations(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockIO := mocks.NewMockIO(ctrl)
	mockVips := mocks.NewMockVipsClient(ctrl)
	mockSource := mocks.NewMockVipsSource(ctrl)
	mockImage := mocks.NewMockVipsImage(ctrl)

	ctx := context.Background()
	sourceURL := "https://example.com/very-large.jpg"

	// Mock HTTP response
	mockResp := &http.Response{
		StatusCode:    200,
		ContentLength: 20 * 1024 * 1024, // 20MB > 9MB target
		Body:          io.NopCloser(bytes.NewReader(testJPEG)),
	}

	mockHTTPClient.EXPECT().
		GetResponse(gomock.Any(), sourceURL, nil).
		Return(mockResp, nil)

	// Mock vips operations
	mockVips.EXPECT().Startup(gomock.Any()).Times(1)
	mockVips.EXPECT().NewSource(gomock.Any()).Return(mockSource)
	mockSource.EXPECT().Close().Times(1)

	mockVips.EXPECT().NewImageFromSource(mockSource, gomock.Any()).Return(mockImage, nil)

	mockImage.EXPECT().Width().Return(5000).AnyTimes()
	mockImage.EXPECT().Height().Return(4000).AnyTimes()
	mockImage.EXPECT().HasAlpha().Return(false)
	mockImage.EXPECT().Pages().Return(1).AnyTimes() // Still image
	mockImage.EXPECT().Close().Times(1)

	// Simulate multiple resize iterations
	// First encode: 20MB (too large)
	// After resize 1: 15MB (still too large)
	// After resize 2: 8MB (meets target)
	output1 := make([]byte, 20*1024*1024)
	output2 := make([]byte, 15*1024*1024)
	output3 := make([]byte, 8*1024*1024)

	gomock.InOrder(
		// First encode - too large
		mockImage.EXPECT().JpegsaveBuffer(gomock.Any()).Return(output1, nil).Times(1),
		// First resize
		mockImage.EXPECT().Resize(gomock.Any(), gomock.Any()).Return(nil).Times(1),
		// Second encode - still too large
		mockImage.EXPECT().JpegsaveBuffer(gomock.Any()).Return(output2, nil).Times(1),
		// Second resize
		mockImage.EXPECT().Resize(gomock.Any(), gomock.Any()).Return(nil).Times(1),
		// Third encode - now fits
		mockImage.EXPECT().JpegsaveBuffer(gomock.Any()).Return(output3, nil).Times(1),
	)

	mockVips.EXPECT().Shutdown().Times(1)

	cfg := createDefaultConfig()
	tr := transformer.NewTransformer(cfg, mockHTTPClient, mockIO, mockVips)
	defer func() {
		_ = tr.Close()
	}()

	input := &transformer.TransformInput{
		SourceURL:  sourceURL,
		IsAnimated: false,
	}

	result, err := tr.Transform(ctx, input)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, int64(len(output3)), result.TransformedSize)
}

func TestTransform_CannotMeetTargetSize(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockIO := mocks.NewMockIO(ctrl)
	mockVips := mocks.NewMockVipsClient(ctrl)
	mockSource := mocks.NewMockVipsSource(ctrl)
	mockImage := mocks.NewMockVipsImage(ctrl)

	ctx := context.Background()
	sourceURL := "https://example.com/incompressible.jpg"

	// Mock HTTP response
	mockResp := &http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(bytes.NewReader(testJPEG)),
	}

	mockHTTPClient.EXPECT().
		GetResponse(gomock.Any(), sourceURL, nil).
		Return(mockResp, nil)

	// Mock vips operations
	mockVips.EXPECT().Startup(gomock.Any()).Times(1)
	mockVips.EXPECT().NewSource(gomock.Any()).Return(mockSource)
	mockSource.EXPECT().Close().Times(1)

	mockVips.EXPECT().NewImageFromSource(mockSource, gomock.Any()).Return(mockImage, nil)

	// Already at minimum dimension
	mockImage.EXPECT().Width().Return(500).AnyTimes()
	mockImage.EXPECT().Height().Return(500).AnyTimes()
	mockImage.EXPECT().HasAlpha().Return(false)
	mockImage.EXPECT().Pages().Return(1).AnyTimes() // Still image
	mockImage.EXPECT().Close().Times(1)

	// All encodings still exceed target (incompressible image)
	// Will try:
	// - Initial encode at quality 95 (attempt 1)
	// - Compression loop: 90, 85, 80, 75, 70 (attempts 2-6)
	largeOutput := make([]byte, 12*1024*1024) // Always 12MB > 9MB target

	// Expect 6 encode attempts total (1 initial + 5 compression steps)
	mockImage.EXPECT().JpegsaveBuffer(gomock.Any()).Return(largeOutput, nil).Times(6)

	mockVips.EXPECT().Shutdown().Times(1)

	cfg := createDefaultConfig()
	cfg.MinImageDimension = 500 // Can't resize below this
	tr := transformer.NewTransformer(cfg, mockHTTPClient, mockIO, mockVips)
	defer func() {
		_ = tr.Close()
	}()

	input := &transformer.TransformInput{
		SourceURL:  sourceURL,
		IsAnimated: false,
	}

	_, err := tr.Transform(ctx, input)

	assert.Error(t, err)
	assert.ErrorIs(t, err, transformer.ErrCannotMeetTargetSize)
}

func TestTransform_AnimationCannotFit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockIO := mocks.NewMockIO(ctrl)
	mockVips := mocks.NewMockVipsClient(ctrl)
	mockSource := mocks.NewMockVipsSource(ctrl)
	mockImage := mocks.NewMockVipsImage(ctrl)

	ctx := context.Background()
	sourceURL := "https://example.com/huge-animation.gif"

	// Mock HTTP response
	mockResp := &http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(bytes.NewReader(testJPEG)),
	}

	mockHTTPClient.EXPECT().
		GetResponse(gomock.Any(), sourceURL, nil).
		Return(mockResp, nil)

	// Mock vips operations
	mockVips.EXPECT().Startup(gomock.Any()).Times(1)
	mockVips.EXPECT().NewSource(gomock.Any()).Return(mockSource)
	mockSource.EXPECT().Close().Times(1)

	mockVips.EXPECT().NewImageFromSource(mockSource, gomock.Any()).Return(mockImage, nil)

	// Already at minimum animated dimension
	mockImage.EXPECT().Width().Return(300).AnyTimes()
	mockImage.EXPECT().Height().Return(300).AnyTimes()
	mockImage.EXPECT().Pages().Return(50).AnyTimes()     // Animated
	mockImage.EXPECT().PageHeight().Return(6).AnyTimes() // 300/50 = 6 pixels per frame
	mockImage.EXPECT().Close().Times(1)

	// All encodings still exceed target (incompressible animation)
	// Will try:
	// - Initial encode at quality 95 (attempt 1)
	// - Compression loop: 90, 85, 80, 75, 70 (attempts 2-6)
	largeOutput := make([]byte, 55*1024*1024) // Always > 50MB target

	// Expect 6 WebP encode attempts total (1 initial + 5 compression steps)
	mockImage.EXPECT().WebpsaveBuffer(gomock.Any()).Return(largeOutput, nil).Times(6)

	mockVips.EXPECT().Shutdown().Times(1)

	cfg := createDefaultConfig()
	cfg.MinImageDimension = 300 // Can't resize below this
	tr := transformer.NewTransformer(cfg, mockHTTPClient, mockIO, mockVips)
	defer func() {
		_ = tr.Close()
	}()

	input := &transformer.TransformInput{
		SourceURL:  sourceURL,
		IsAnimated: true,
	}

	_, err := tr.Transform(ctx, input)

	assert.Error(t, err)
	assert.ErrorIs(t, err, transformer.ErrAnimationCannotFit)
}

func TestTransform_MultipleCompressionIterations(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockIO := mocks.NewMockIO(ctrl)
	mockVips := mocks.NewMockVipsClient(ctrl)
	mockSource := mocks.NewMockVipsSource(ctrl)
	mockImage := mocks.NewMockVipsImage(ctrl)

	ctx := context.Background()
	sourceURL := "https://example.com/image.jpg"

	// Mock HTTP response
	mockResp := &http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(bytes.NewReader(testJPEG)),
	}

	mockHTTPClient.EXPECT().
		GetResponse(gomock.Any(), sourceURL, nil).
		Return(mockResp, nil)

	// Mock vips operations
	mockVips.EXPECT().Startup(gomock.Any()).Times(1)
	mockVips.EXPECT().NewSource(gomock.Any()).Return(mockSource)
	mockSource.EXPECT().Close().Times(1)

	mockVips.EXPECT().NewImageFromSource(mockSource, gomock.Any()).Return(mockImage, nil)

	// At minimum dimension
	mockImage.EXPECT().Width().Return(600).AnyTimes()
	mockImage.EXPECT().Height().Return(600).AnyTimes()
	mockImage.EXPECT().HasAlpha().Return(false)
	mockImage.EXPECT().Close().Times(1)

	// Simulate compression iterations
	// Quality 95 (initial): 11MB (too large)
	// Quality 90: 10MB (still too large)
	// Quality 85: 8MB (meets target)
	output1 := make([]byte, 11*1024*1024)
	output2 := make([]byte, 10*1024*1024)
	output3 := make([]byte, 8*1024*1024)

	gomock.InOrder(
		// Initial encode at quality 95
		mockImage.EXPECT().JpegsaveBuffer(gomock.Any()).Return(output1, nil).Times(1),
		// Compression starts at quality 90
		mockImage.EXPECT().JpegsaveBuffer(gomock.Any()).Return(output2, nil).Times(1),
		// Compression at quality 85 - meets target
		mockImage.EXPECT().JpegsaveBuffer(gomock.Any()).Return(output3, nil).Times(1),
	)

	mockVips.EXPECT().Shutdown().Times(1)

	cfg := createDefaultConfig()
	cfg.MinImageDimension = 600 // Can't resize below this
	tr := transformer.NewTransformer(cfg, mockHTTPClient, mockIO, mockVips)
	defer func() {
		_ = tr.Close()
	}()

	input := &transformer.TransformInput{
		SourceURL:  sourceURL,
		IsAnimated: false,
	}

	result, err := tr.Transform(ctx, input)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, int64(len(output3)), result.TransformedSize)
}

func TestTransform_ResizeThenCompress(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockIO := mocks.NewMockIO(ctrl)
	mockVips := mocks.NewMockVipsClient(ctrl)
	mockSource := mocks.NewMockVipsSource(ctrl)
	mockImage := mocks.NewMockVipsImage(ctrl)

	ctx := context.Background()
	sourceURL := "https://example.com/large.jpg"

	// Mock HTTP response
	mockResp := &http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(bytes.NewReader(testJPEG)),
	}

	mockHTTPClient.EXPECT().
		GetResponse(gomock.Any(), sourceURL, nil).
		Return(mockResp, nil)

	// Mock vips operations
	mockVips.EXPECT().Startup(gomock.Any()).Times(1)
	mockVips.EXPECT().NewSource(gomock.Any()).Return(mockSource)
	mockSource.EXPECT().Close().Times(1)

	mockVips.EXPECT().NewImageFromSource(mockSource, gomock.Any()).Return(mockImage, nil)

	mockImage.EXPECT().Width().Return(3000).AnyTimes()
	mockImage.EXPECT().Height().Return(2000).AnyTimes()
	mockImage.EXPECT().HasAlpha().Return(false)
	mockImage.EXPECT().Pages().Return(1).AnyTimes() // Still image
	mockImage.EXPECT().Close().Times(1)

	// Simulate: resize helps but not enough, then compress
	// Quality 95 (initial): 20MB (too large)
	// After resize (quality 95): 10MB (still > 9MB, can't resize more)
	// Compression at quality 90: 8MB (meets target)
	output1 := make([]byte, 20*1024*1024)
	output2 := make([]byte, 10*1024*1024)
	output3 := make([]byte, 8*1024*1024)

	gomock.InOrder(
		// Initial encode at quality 95 - too large
		mockImage.EXPECT().JpegsaveBuffer(gomock.Any()).Return(output1, nil).Times(1),
		// Resize once
		mockImage.EXPECT().Resize(gomock.Any(), gomock.Any()).Return(nil).Times(1),
		// Encode after resize at quality 95 - still too large but can't resize more
		mockImage.EXPECT().JpegsaveBuffer(gomock.Any()).Return(output2, nil).Times(1),
		// Compression at quality 90 - meets target
		mockImage.EXPECT().JpegsaveBuffer(gomock.Any()).Return(output3, nil).Times(1),
	)

	mockVips.EXPECT().Shutdown().Times(1)

	cfg := createDefaultConfig()
	cfg.MinImageDimension = 2400 // After one resize (3000 * 0.8 = 2400), can't go further
	tr := transformer.NewTransformer(cfg, mockHTTPClient, mockIO, mockVips)
	defer func() {
		_ = tr.Close()
	}()

	input := &transformer.TransformInput{
		SourceURL:  sourceURL,
		IsAnimated: false,
	}

	result, err := tr.Transform(ctx, input)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, int64(len(output3)), result.TransformedSize)
}

func TestTransform_AnimatedMultipleResize(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockIO := mocks.NewMockIO(ctrl)
	mockVips := mocks.NewMockVipsClient(ctrl)
	mockSource := mocks.NewMockVipsSource(ctrl)
	mockImage := mocks.NewMockVipsImage(ctrl)

	ctx := context.Background()
	sourceURL := "https://example.com/large-animation.gif"

	// Mock HTTP response
	mockResp := &http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(bytes.NewReader(testJPEG)),
	}

	mockHTTPClient.EXPECT().
		GetResponse(gomock.Any(), sourceURL, nil).
		Return(mockResp, nil)

	// Mock vips operations
	mockVips.EXPECT().Startup(gomock.Any()).Times(1)
	mockVips.EXPECT().NewSource(gomock.Any()).Return(mockSource)
	mockSource.EXPECT().Close().Times(1)

	mockVips.EXPECT().NewImageFromSource(mockSource, gomock.Any()).Return(mockImage, nil)

	// Track dimension changes through resize operations
	// Initial: 1000x1000 with 2 frames (500px per frame) = 2MP (within 45MP target, but file size too large)
	// After resize 1: 800x800 with 2 frames (400px per frame) = 1.28MP (meets both targets)
	currentWidth := 1000
	currentHeight := 1000
	pages := 2

	mockImage.EXPECT().Width().DoAndReturn(func() int {
		return currentWidth
	}).AnyTimes()

	mockImage.EXPECT().Height().DoAndReturn(func() int {
		return currentHeight
	}).AnyTimes()

	mockImage.EXPECT().Pages().Return(pages).AnyTimes()
	mockImage.EXPECT().PageHeight().DoAndReturn(func() int {
		// PageHeight should be height/pages for animated images
		return currentHeight / pages
	}).AnyTimes()
	mockImage.EXPECT().Close().Times(1)

	// Simulate resize iteration for animation
	// Initial: 15MB, 2MP total pixels (within pixel target, but exceeds size target of 9MB)
	// After resize 1: 7MB, 1.28MP (meets both targets)
	output1 := make([]byte, 15*1024*1024)
	output2 := make([]byte, 7*1024*1024)

	gomock.InOrder(
		// First encode - too large in size
		mockImage.EXPECT().WebpsaveBuffer(gomock.Any()).Return(output1, nil).Times(1),
		// First resize: scale 0.8 -> 1000 * 0.8 = 800
		mockImage.EXPECT().Resize(gomock.Any(), gomock.Any()).DoAndReturn(func(scale float64, opts interface{}) error {
			currentWidth = 800
			currentHeight = 800
			return nil
		}).Times(1),
		// Update page height after first resize (800/2 = 400)
		mockImage.EXPECT().SetPageHeight(800/pages).Return(nil).Times(1),
		// Second encode - meets both targets
		mockImage.EXPECT().WebpsaveBuffer(gomock.Any()).Return(output2, nil).Times(1),
	)

	mockVips.EXPECT().Shutdown().Times(1)

	cfg := createDefaultConfig()
	tr := transformer.NewTransformer(cfg, mockHTTPClient, mockIO, mockVips)
	defer func() {
		_ = tr.Close()
	}()

	input := &transformer.TransformInput{
		SourceURL:  sourceURL,
		IsAnimated: true,
	}

	result, err := tr.Transform(ctx, input)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, int64(len(output2)), result.TransformedSize)
	assert.Equal(t, 800, result.Width)
	assert.Equal(t, 400, result.Height)                      // Frame height: 800/2 = 400
	assert.Equal(t, "large-animation.webp", result.Filename) // Filename is derived from source URL
	assert.Equal(t, "image/webp", result.ContentType)
}

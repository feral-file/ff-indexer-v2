//go:build cgo

package rasterizer

import (
	"bytes"
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
)

// Rasterizer handles SVG to PNG conversion using resvg
//
//go:generate mockgen -source=rasterizer.go -destination=../../mocks/media_rasterizer.go -package=mocks -mock_names=Rasterizer=MockRasterizer
type Rasterizer interface {
	// Rasterize converts an SVG to a PNG image
	Rasterize(ctx context.Context, svgData []byte) ([]byte, error)
}

type rasterizer struct {
	resvgClient  adapter.ResvgClient
	imageEncoder adapter.ImageEncoder
	width        int
}

// Config holds configuration for the rasterizer
type Config struct {
	// Width is the target width for rasterization (0 = use SVG natural size)
	// Height is automatically calculated to maintain aspect ratio using ScaleBestFit
	Width int
}

// NewRasterizer creates a new SVG rasterizer instance
func NewRasterizer(resvgClient adapter.ResvgClient, imageEncoder adapter.ImageEncoder, cfg *Config) Rasterizer {
	if cfg == nil {
		cfg = &Config{}
	}

	return &rasterizer{
		resvgClient:  resvgClient,
		imageEncoder: imageEncoder,
		width:        cfg.Width,
	}
}

// Rasterize converts SVG data to PNG format
func (r *rasterizer) Rasterize(ctx context.Context, svgData []byte) ([]byte, error) {
	logger.InfoCtx(ctx, "Starting SVG rasterization to PNG",
		zap.Int("svgSize", len(svgData)),
		zap.Int("targetWidth", r.width),
	)

	// Render SVG using resvg with best fit scaling
	img, err := r.resvgClient.Render(svgData, r.width)
	if err != nil {
		logger.ErrorCtx(ctx, err)
		return nil, fmt.Errorf("failed to render SVG: %w", err)
	}

	// Get the rendered image dimensions
	bounds := img.Bounds()
	renderedWidth := bounds.Dx()
	renderedHeight := bounds.Dy()

	logger.InfoCtx(ctx, "SVG rendered",
		zap.Int("renderedWidth", renderedWidth),
		zap.Int("renderedHeight", renderedHeight),
	)

	// Encode to PNG
	var buf bytes.Buffer
	logger.InfoCtx(ctx, "Encoding to PNG")
	if err := r.imageEncoder.EncodePNG(&buf, img); err != nil {
		return nil, fmt.Errorf("failed to encode PNG: %w", err)
	}

	result := buf.Bytes()
	logger.InfoCtx(ctx, "SVG rasterization completed",
		zap.Int("outputSize", len(result)),
	)

	return result, nil
}

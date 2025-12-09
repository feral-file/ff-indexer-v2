//go:build cgo

package adapter

import (
	"image"

	"github.com/xo/resvg"
)

// ResvgClient defines an interface for SVG rendering using resvg
//
//go:generate mockgen -source=resvg.go -destination=../mocks/resvg.go -package=mocks -mock_names=ResvgClient=MockResvgClient
type ResvgClient interface {
	// Render renders SVG data to an image with specified width (0 = use SVG natural size)
	// Uses ScaleBestFit mode to maintain aspect ratio
	Render(data []byte, width int) (image.Image, error)
}

// RealResvgClient implements ResvgClient using the actual resvg library
type RealResvgClient struct{}

// NewResvgClient creates a new real resvg client
func NewResvgClient() ResvgClient {
	return &RealResvgClient{}
}

// Render renders SVG data to an image using resvg with best fit scaling
func (c *RealResvgClient) Render(data []byte, width int) (image.Image, error) {
	var resvgOpts []resvg.Option

	// Always use best fit scaling to maintain aspect ratio
	resvgOpts = append(resvgOpts, resvg.WithScaleMode(resvg.ScaleBestFit))

	// Add width if specified
	if width > 0 {
		resvgOpts = append(resvgOpts, resvg.WithWidth(width))
	}

	return resvg.Render(data, resvgOpts...)
}

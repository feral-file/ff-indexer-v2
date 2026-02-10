//go:build cgo

package adapter

import (
	"io"

	"github.com/cshum/vipsgen/vips"
)

// VipsImage wraps vips.Image to provide a mockable interface
//
//go:generate mockgen -source=vips.go -destination=../mocks/vips.go -package=mocks -mock_names=VipsClient=MockVipsClient,VipsImage=MockVipsImage,VipsSource=MockVipsSource
//go:generate sh -c "printf '//go:build cgo\n\n' | cat - ../mocks/vips.go > ../mocks/vips.go.tmp && mv ../mocks/vips.go.tmp ../mocks/vips.go"
type VipsImage interface {
	Width() int
	Height() int
	HasAlpha() bool
	Pages() int
	PageHeight() int
	SetPageHeight(height int) error
	Resize(scale float64, options *vips.ResizeOptions) error
	ExtractArea(left, top, width, height int) error
	JpegsaveBuffer(options *vips.JpegsaveBufferOptions) ([]byte, error)
	WebpsaveBuffer(options *vips.WebpsaveBufferOptions) ([]byte, error)
	Close()
}

// VipsSource wraps vips.Source to provide a mockable interface
type VipsSource interface {
	Close()
}

// VipsClient defines an interface for libvips operations to enable mocking
type VipsClient interface {
	// Startup initializes libvips
	Startup(config *vips.Config)

	// Shutdown shuts down libvips
	Shutdown()

	// NewSource creates a new vips source from an io.ReadCloser
	NewSource(reader io.ReadCloser) VipsSource

	// NewImageFromSource loads an image from a source
	NewImageFromSource(source VipsSource, options *vips.LoadOptions) (VipsImage, error)
}

// RealVipsClient implements VipsClient using the actual vipsgen/vips library
type RealVipsClient struct{}

// NewVipsClient creates a new real vips client implementation
func NewVipsClient() VipsClient {
	return &RealVipsClient{}
}

func (v *RealVipsClient) Startup(config *vips.Config) {
	vips.Startup(config)
}

func (v *RealVipsClient) Shutdown() {
	vips.Shutdown()
}

func (v *RealVipsClient) NewSource(reader io.ReadCloser) VipsSource {
	return &realVipsSource{source: vips.NewSource(reader)}
}

func (v *RealVipsClient) NewImageFromSource(source VipsSource, options *vips.LoadOptions) (VipsImage, error) {
	realSource := source.(*realVipsSource)
	img, err := vips.NewImageFromSource(realSource.source, options)
	if err != nil {
		return nil, err
	}
	return &realVipsImage{image: img}, nil
}

// realVipsImage wraps vips.Image
type realVipsImage struct {
	image *vips.Image
}

func (i *realVipsImage) Width() int {
	return i.image.Width()
}

func (i *realVipsImage) Height() int {
	return i.image.Height()
}

func (i *realVipsImage) HasAlpha() bool {
	return i.image.HasAlpha()
}

func (i *realVipsImage) Pages() int {
	return i.image.Pages()
}

func (i *realVipsImage) PageHeight() int {
	return i.image.PageHeight()
}

func (i *realVipsImage) SetPageHeight(height int) error {
	return i.image.SetPageHeight(height)
}

func (i *realVipsImage) Resize(scale float64, options *vips.ResizeOptions) error {
	return i.image.Resize(scale, options)
}

func (i *realVipsImage) ExtractArea(left, top, width, height int) error {
	return i.image.ExtractArea(left, top, width, height)
}

func (i *realVipsImage) JpegsaveBuffer(options *vips.JpegsaveBufferOptions) ([]byte, error) {
	return i.image.JpegsaveBuffer(options)
}

func (i *realVipsImage) WebpsaveBuffer(options *vips.WebpsaveBufferOptions) ([]byte, error) {
	return i.image.WebpsaveBuffer(options)
}

func (i *realVipsImage) Close() {
	i.image.Close()
}

// realVipsSource wraps vips.Source
type realVipsSource struct {
	source *vips.Source
}

func (s *realVipsSource) Close() {
	s.source.Close()
}

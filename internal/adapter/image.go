package adapter

import (
	"image"
	"image/jpeg"
	"image/png"
	"io"
)

// ImageEncoder defines an interface for encoding images
//
//go:generate mockgen -source=image.go -destination=../mocks/image.go -package=mocks -mock_names=ImageEncoder=MockImageEncoder
type ImageEncoder interface {
	// EncodePNG encodes an image to PNG format
	EncodePNG(w io.Writer, img image.Image) error
	// EncodeJPEG encodes an image to JPEG format with specified quality
	EncodeJPEG(w io.Writer, img image.Image, quality int) error
}

// RealImageEncoder implements ImageEncoder using standard library
type RealImageEncoder struct{}

// NewImageEncoder creates a new real image encoder
func NewImageEncoder() ImageEncoder {
	return &RealImageEncoder{}
}

// EncodePNG encodes an image to PNG format
func (e *RealImageEncoder) EncodePNG(w io.Writer, img image.Image) error {
	return png.Encode(w, img)
}

// EncodeJPEG encodes an image to JPEG format with specified quality
func (e *RealImageEncoder) EncodeJPEG(w io.Writer, img image.Image, quality int) error {
	return jpeg.Encode(w, img, &jpeg.Options{Quality: quality})
}

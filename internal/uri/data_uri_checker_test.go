package uri_test

import (
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/feral-file/ff-indexer-v2/internal/uri"
)

func TestDataURIChecker_Check(t *testing.T) {
	checker := uri.NewDataURIChecker()

	// Valid PNG image (1x1 red pixel)
	pngData := []byte{
		0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, // PNG signature
		0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44, 0x52, // IHDR chunk
		0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
		0x08, 0x02, 0x00, 0x00, 0x00, 0x90, 0x77, 0x53,
		0xDE, 0x00, 0x00, 0x00, 0x0C, 0x49, 0x44, 0x41,
		0x54, 0x08, 0xD7, 0x63, 0xF8, 0xCF, 0xC0, 0x00,
		0x00, 0x03, 0x01, 0x01, 0x00, 0x18, 0xDD, 0x8D,
		0xB4, 0x00, 0x00, 0x00, 0x00, 0x49, 0x45, 0x4E,
		0x44, 0xAE, 0x42, 0x60, 0x82,
	}
	validPNGBase64 := base64.StdEncoding.EncodeToString(pngData)

	// Valid JPEG image (minimal valid JPEG)
	jpegData := []byte{
		0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, 0x4A, 0x46, // JPEG signature
		0x49, 0x46, 0x00, 0x01, 0x01, 0x01, 0x00, 0x48,
		0x00, 0x48, 0x00, 0x00, 0xFF, 0xDB, 0x00, 0x43,
		0x00, 0x08, 0x06, 0x06, 0x07, 0x06, 0x05, 0x08,
		0x07, 0x07, 0x07, 0x09, 0x09, 0x08, 0x0A, 0x0C,
		0x14, 0x0D, 0x0C, 0x0B, 0x0B, 0x0C, 0x19, 0x12,
		0x13, 0x0F, 0x14, 0x1D, 0x1A, 0x1F, 0x1E, 0x1D,
		0x1A, 0x1C, 0x1C, 0x20, 0x24, 0x2E, 0x27, 0x20,
		0x22, 0x2C, 0x23, 0x1C, 0x1C, 0x28, 0x37, 0x29,
		0x2C, 0x30, 0x31, 0x34, 0x34, 0x34, 0x1F, 0x27,
		0x39, 0x3D, 0x38, 0x32, 0x3C, 0x2E, 0x33, 0x34,
		0x32, 0xFF, 0xC0, 0x00, 0x0B, 0x08, 0x00, 0x01,
		0x00, 0x01, 0x01, 0x01, 0x11, 0x00, 0xFF, 0xC4,
		0x00, 0x14, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x03, 0xFF, 0xDA, 0x00, 0x08,
		0x01, 0x01, 0x00, 0x00, 0x3F, 0x00, 0x37, 0xFF,
		0xD9,
	}
	validJPEGBase64 := base64.StdEncoding.EncodeToString(jpegData)

	// Valid SVG image
	svgData := `<svg xmlns="http://www.w3.org/2000/svg" width="100" height="100"><circle cx="50" cy="50" r="40" fill="red"/></svg>`
	validSVGBase64 := base64.StdEncoding.EncodeToString([]byte(svgData))

	// Valid GIF image (1x1 transparent pixel)
	gifData := []byte{
		0x47, 0x49, 0x46, 0x38, 0x39, 0x61, // GIF89a signature
		0x01, 0x00, 0x01, 0x00, 0x80, 0x00,
		0x00, 0xFF, 0xFF, 0xFF, 0x00, 0x00,
		0x00, 0x21, 0xF9, 0x04, 0x01, 0x00,
		0x00, 0x00, 0x00, 0x2C, 0x00, 0x00,
		0x00, 0x00, 0x01, 0x00, 0x01, 0x00,
		0x00, 0x02, 0x02, 0x44, 0x01, 0x00,
		0x3B,
	}
	validGIFBase64 := base64.StdEncoding.EncodeToString(gifData)

	tests := []struct {
		name                   string
		dataURI                string
		expectValid            bool
		expectError            *string
		expectMimeType         string
		expectDeclaredMimeType string
	}{
		{
			name:                   "valid PNG with base64",
			dataURI:                "data:image/png;base64," + validPNGBase64,
			expectValid:            true,
			expectMimeType:         "image/png",
			expectDeclaredMimeType: "image/png",
		},
		{
			name:                   "valid JPEG with base64",
			dataURI:                "data:image/jpeg;base64," + validJPEGBase64,
			expectValid:            true,
			expectMimeType:         "image/jpeg",
			expectDeclaredMimeType: "image/jpeg",
		},
		{
			name:                   "valid SVG with base64",
			dataURI:                "data:image/svg+xml;base64," + validSVGBase64,
			expectValid:            true,
			expectMimeType:         "image/svg+xml",
			expectDeclaredMimeType: "image/svg+xml",
		},
		{
			name:                   "valid GIF with base64",
			dataURI:                "data:image/gif;base64," + validGIFBase64,
			expectValid:            true,
			expectMimeType:         "image/gif",
			expectDeclaredMimeType: "image/gif",
		},
		{
			name:        "missing data: prefix",
			dataURI:     "image/png;base64," + validPNGBase64,
			expectValid: false,
			expectError: strPtr("invalid data URI: must start with 'data:'"),
		},
		{
			name:        "missing comma separator",
			dataURI:     "data:image/png;base64" + validPNGBase64,
			expectValid: false,
			expectError: strPtr("invalid data URI format: missing comma separator"),
		},
		{
			name:        "empty data",
			dataURI:     "data:image/png;base64,",
			expectValid: false,
			expectError: strPtr("invalid data URI: empty data"),
		},
		{
			name:        "invalid base64 encoding",
			dataURI:     "data:image/png;base64,!!!invalid!!!",
			expectValid: false,
			expectError: strPtr("failed to decode base64: illegal base64 data at input byte 0"),
		},
		{
			name:                   "unsupported mime type - text",
			dataURI:                "data:text/plain;base64," + base64.StdEncoding.EncodeToString([]byte("hello")),
			expectValid:            false,
			expectError:            strPtr("unsupported mime type: text/plain (only image/* and video/* are supported)"),
			expectDeclaredMimeType: "text/plain",
		},
		{
			name:                   "unsupported mime type - application/json",
			dataURI:                "data:application/json;base64," + base64.StdEncoding.EncodeToString([]byte(`{"test":"data"}`)),
			expectValid:            false,
			expectError:            strPtr("unsupported mime type: application/json (only image/* and video/* are supported)"),
			expectDeclaredMimeType: "application/json",
		},
		{
			name:                   "mime type mismatch - declared PNG but is JPEG",
			dataURI:                "data:image/png;base64," + validJPEGBase64,
			expectValid:            false,
			expectError:            strPtr("mime type mismatch: declared image/png but detected image/jpeg"),
			expectMimeType:         "image/jpeg",
			expectDeclaredMimeType: "image/png",
		},
		{
			name:                   "mime type mismatch - declared image but content is text",
			dataURI:                "data:image/png;base64," + base64.StdEncoding.EncodeToString([]byte("not an image")),
			expectValid:            false,
			expectError:            strPtr("mime type mismatch: declared image/png but detected text/plain; charset=utf-8"),
			expectMimeType:         "text/plain; charset=utf-8",
			expectDeclaredMimeType: "image/png",
		},
		{
			name:                   "valid PNG without explicit mime type",
			dataURI:                "data:;base64," + validPNGBase64,
			expectValid:            false,
			expectError:            strPtr("unsupported mime type: text/plain (only image/* and video/* are supported)"),
			expectDeclaredMimeType: "text/plain",
		},
		{
			name:                   "valid PNG with charset parameter",
			dataURI:                "data:image/png;charset=utf-8;base64," + validPNGBase64,
			expectValid:            true,
			expectMimeType:         "image/png",
			expectDeclaredMimeType: "image/png",
		},
		{
			name:                   "SVG with image/svg mime type (without +xml)",
			dataURI:                "data:image/svg;base64," + validSVGBase64,
			expectValid:            true,
			expectMimeType:         "image/svg+xml",
			expectDeclaredMimeType: "image/svg",
		},
		{
			name:                   "case insensitive mime type",
			dataURI:                "data:IMAGE/PNG;base64," + validPNGBase64,
			expectValid:            true,
			expectMimeType:         "image/png",
			expectDeclaredMimeType: "IMAGE/PNG",
		},
		{
			name:                   "mime type with spaces",
			dataURI:                "data: image/png ;base64," + validPNGBase64,
			expectValid:            true,
			expectMimeType:         "image/png",
			expectDeclaredMimeType: "image/png",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := checker.Check(tt.dataURI)

			assert.Equal(t, tt.expectValid, result.Valid, "Valid mismatch")

			if tt.expectError != nil {
				assert.NotNil(t, result.Error, "Expected error but got nil")
				if result.Error != nil {
					assert.Equal(t, *tt.expectError, *result.Error, "Error message mismatch")
				}
			} else {
				assert.Nil(t, result.Error, "Expected no error but got: %v", result.Error)
			}

			if tt.expectMimeType != "" {
				assert.Equal(t, tt.expectMimeType, result.MimeType, "MimeType mismatch")
			}

			if tt.expectDeclaredMimeType != "" {
				assert.Equal(t, tt.expectDeclaredMimeType, result.DeclaredMimeType, "DeclaredMimeType mismatch")
			}
		})
	}
}

func TestDataURIChecker_Check_VideoFormats(t *testing.T) {
	checker := uri.NewDataURIChecker()

	// Valid MP4 video (minimal valid MP4 with ftyp box)
	mp4Data := []byte{
		0x00, 0x00, 0x00, 0x20, 0x66, 0x74, 0x79, 0x70, // ftyp box
		0x69, 0x73, 0x6F, 0x6D, 0x00, 0x00, 0x02, 0x00,
		0x69, 0x73, 0x6F, 0x6D, 0x69, 0x73, 0x6F, 0x32,
		0x61, 0x76, 0x63, 0x31, 0x6D, 0x70, 0x34, 0x31,
	}
	validMP4Base64 := base64.StdEncoding.EncodeToString(mp4Data)

	tests := []struct {
		name           string
		dataURI        string
		expectValid    bool
		expectMimeType string
	}{
		{
			name:           "valid MP4 video",
			dataURI:        "data:video/mp4;base64," + validMP4Base64,
			expectValid:    true,
			expectMimeType: "video/mp4",
		},
		{
			name:        "video mime type mismatch - declared webm but is mp4",
			dataURI:     "data:video/webm;base64," + validMP4Base64,
			expectValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := checker.Check(tt.dataURI)

			assert.Equal(t, tt.expectValid, result.Valid, "Valid mismatch")

			if tt.expectMimeType != "" {
				assert.Equal(t, tt.expectMimeType, result.MimeType, "MimeType mismatch")
			}
		})
	}
}

func TestDataURIChecker_Check_EdgeCases(t *testing.T) {
	checker := uri.NewDataURIChecker()

	// Very small PNG (corrupted but has PNG signature)
	smallPNG := []byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A}
	smallPNGBase64 := base64.StdEncoding.EncodeToString(smallPNG)

	tests := []struct {
		name        string
		dataURI     string
		expectValid bool
	}{
		{
			name:        "empty metadata with comma",
			dataURI:     "data:,test",
			expectValid: false, // text/plain is default, not image/video
		},
		{
			name:        "only base64 flag without mime type",
			dataURI:     "data:;base64," + smallPNGBase64,
			expectValid: false, // defaults to text/plain
		},
		{
			name:        "multiple semicolons in metadata",
			dataURI:     "data:image/png;charset=utf-8;base64," + smallPNGBase64,
			expectValid: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := checker.Check(tt.dataURI)
			assert.Equal(t, tt.expectValid, result.Valid, "Valid mismatch")
		})
	}
}

// Helper function to create string pointers
func strPtr(s string) *string {
	return &s
}

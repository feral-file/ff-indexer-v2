package uri

import (
	"fmt"
	"strings"

	"github.com/gabriel-vasile/mimetype"

	"github.com/feral-file/ff-indexer-v2/internal/types"
)

// DataURICheckResult represents the result of validating a data URI
type DataURICheckResult struct {
	Valid            bool
	Error            *string
	MimeType         string // Detected mime type from content
	DeclaredMimeType string // Declared mime type in URI
}

// DataURIChecker defines the interface for checking data URI validity
//
//go:generate mockgen -source=data_uri_checker.go -destination=../mocks/data_uri_checker.go -package=mocks -mock_names=DataURIChecker=MockDataURIChecker
type DataURIChecker interface {
	// Check validates a data URI according to RFC 2397
	// It checks:
	// 1. Format follows RFC 2397
	// 2. Mime type is image/* or video/*
	// 3. Content matches declared mime type using magic numbers
	Check(dataURI string) DataURICheckResult
}

type dataURIChecker struct{}

// NewDataURIChecker creates a new data URI checker
func NewDataURIChecker() DataURIChecker {
	return &dataURIChecker{}
}

// Check validates a data URI
func (c *dataURIChecker) Check(dataURI string) DataURICheckResult {
	// 1. Parse the data URI
	parsed, err := types.ParseDataURI(dataURI)
	if err != nil {
		errMsg := err.Error()
		return DataURICheckResult{
			Valid: false,
			Error: &errMsg,
		}
	}

	// 2. Validate mime type is image/* or video/*
	if !isImageOrVideoMimeType(parsed.MimeType) {
		errMsg := fmt.Sprintf("unsupported mime type: %s (only image/* and video/* are supported)", parsed.MimeType)
		return DataURICheckResult{
			Valid:            false,
			Error:            &errMsg,
			DeclaredMimeType: parsed.MimeType,
		}
	}

	// 3. Verify the data is not empty
	if len(parsed.DecodedData) == 0 {
		errMsg := "invalid data URI: empty data"
		return DataURICheckResult{
			Valid:            false,
			Error:            &errMsg,
			DeclaredMimeType: parsed.MimeType,
		}
	}

	// 4. Detect actual mime type from content using magic numbers
	detectedMime := mimetype.Detect(parsed.DecodedData)
	detectedMimeType := detectedMime.String()

	// 5. Check if declared mime type matches detected mime type
	if !mimeTypesMatch(parsed.MimeType, detectedMimeType) {
		errMsg := fmt.Sprintf("mime type mismatch: declared %s but detected %s", parsed.MimeType, detectedMimeType)
		return DataURICheckResult{
			Valid:            false,
			Error:            &errMsg,
			DeclaredMimeType: parsed.MimeType,
			MimeType:         detectedMimeType,
		}
	}

	return DataURICheckResult{
		Valid:            true,
		MimeType:         detectedMimeType,
		DeclaredMimeType: parsed.MimeType,
	}
}

// isImageOrVideoMimeType checks if a mime type is image/* or video/*
func isImageOrVideoMimeType(mimeType string) bool {
	mimeType = strings.ToLower(strings.TrimSpace(mimeType))
	return strings.HasPrefix(mimeType, "image/") || strings.HasPrefix(mimeType, "video/")
}

// mimeTypesMatch checks if two mime types match
// It performs a flexible comparison that accounts for:
// - Case differences
// - Generic types (e.g., image/svg matches image/svg+xml)
func mimeTypesMatch(declared, detected string) bool {
	declared = strings.ToLower(strings.TrimSpace(declared))
	detected = strings.ToLower(strings.TrimSpace(detected))

	// Exact match
	if declared == detected {
		return true
	}

	// Handle SVG special case: image/svg and image/svg+xml are equivalent
	if (declared == "image/svg" && detected == "image/svg+xml") ||
		(declared == "image/svg+xml" && detected == "image/svg") {
		return true
	}

	// Extract base type without parameters (e.g., "image/png; charset=utf-8" -> "image/png")
	declaredBase := strings.Split(declared, ";")[0]
	detectedBase := strings.Split(detected, ";")[0]

	return strings.TrimSpace(declaredBase) == strings.TrimSpace(detectedBase)
}

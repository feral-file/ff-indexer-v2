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
	// 2. Mime type is supported (image/*, video/*, text/*, application/json, application/pdf)
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

	// 2. Validate mime type is supported
	if !isSupportedMimeType(parsed.MimeType) {
		errMsg := fmt.Sprintf("unsupported mime type: %s (supported types: image/*, video/*, text/*, application/json, application/pdf)", parsed.MimeType)
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

// isSupportedMimeType checks if a mime type is supported
// Supported types: image/*, video/*, text/*, application/json, application/pdf
func isSupportedMimeType(mimeType string) bool {
	mimeType = strings.ToLower(strings.TrimSpace(mimeType))
	return strings.HasPrefix(mimeType, "image/") ||
		strings.HasPrefix(mimeType, "video/") ||
		strings.HasPrefix(mimeType, "text/") ||
		mimeType == "application/json" ||
		mimeType == "application/pdf"
}

// mimeTypesMatch checks if two mime types match
// It performs a flexible comparison that accounts for:
// - Case differences
// - Generic types (e.g., image/svg matches image/svg+xml)
// - Text-based formats (text/*, application/json) where detection may vary
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
	declaredBase = strings.TrimSpace(declaredBase)
	detectedBase = strings.TrimSpace(detectedBase)

	// Check base types match
	if declaredBase == detectedBase {
		return true
	}

	// For text-based formats, be more lenient as detection is unreliable
	// If declared is text/html, text/plain, or application/json, accept if detected is any text/* type
	if (declaredBase == "text/html" || declaredBase == "text/plain" || declaredBase == "application/json") &&
		strings.HasPrefix(detectedBase, "text/") {
		return true
	}

	// If declared is application/json and detected is text/plain, accept
	if declaredBase == "application/json" && detectedBase == "text/plain" {
		return true
	}

	return false
}

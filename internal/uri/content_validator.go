package uri

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/gabriel-vasile/mimetype"
)

// FailureReason is the machine-readable cause a probe attaches to a broken verdict.
//
// Reason: mirrors schema.MediaFailureReason string-for-string instead of importing
// store/schema, following the existing uri.HealthStatus / schema.MediaHealthStatus
// precedent — the uri package stays a pure network-probe layer with no persistence
// dependency. Constraints: values must stay in sync with the schema constants; they are
// part of the persisted contract (token_media_health.failure_reason).
type FailureReason string

const (
	// FailureHTTPStatus indicates a non-2xx HTTP response.
	FailureHTTPStatus FailureReason = "http_status"
	// FailureDNS indicates DNS resolution failure.
	FailureDNS FailureReason = "dns"
	// FailureSSRF indicates the SSRF policy refused the fetch.
	FailureSSRF FailureReason = "ssrf"
	// FailureTypeMismatch indicates declared image/video/audio but a text body.
	FailureTypeMismatch FailureReason = "type_mismatch"
	// FailureContainerInvalid indicates a recognized media container whose header fails to parse.
	FailureContainerInvalid FailureReason = "container_invalid"
	// FailureDirectoryListing indicates an IPFS gateway directory listing (feral-file#3482).
	FailureDirectoryListing FailureReason = "directory_listing"
	// FailureKnownErrorPage indicates the body matched a configured known-bad page marker.
	FailureKnownErrorPage FailureReason = "known_error_page"
	// FailureZeroLength indicates an empty body.
	FailureZeroLength FailureReason = "zero_length"
	// FailureTruncated indicates the body ended before the declared length.
	FailureTruncated FailureReason = "truncated"
)

// String returns the string representation of the failure reason.
func (r FailureReason) String() string {
	return string(r)
}

// ContentVerdict is the outcome of validating a probe's body bytes.
// Declared and Sniffed are populated whenever available, including on OK verdicts, so the
// caller can persist them for observability and render-probe class selection.
type ContentVerdict struct {
	OK            bool
	FailureReason FailureReason // empty when OK
	Detail        string        // human-facing message for last_error; empty when OK
	Declared      string        // Content-Type header, normalized (lowercase, no parameters)
	Sniffed       string        // magic-byte-detected type, normalized
}

// ContentValidator validates the first bytes of a media URL's body against its declared
// Content-Type.
//
// Reason: a 2xx status alone proved wrong twice (feral-file#3482 directory listings,
// ff-indexer-v2#76 gateway error pages served with 200) — "healthy" must mean "the bytes
// look like the media we think they are", not "some server answered".
// Trade-offs: every rule errs healthy on ambiguity; a false broken hides a real artwork
// from FF1 (worse than letting a broken one through until the render probe catches it).
// Constraints: pure functions over an in-memory prefix (no I/O) so the same validator
// serves the URL checker, the gateway fallback probes, and tests.
//
//go:generate mockgen -source=content_validator.go -destination=../mocks/content_validator.go -package=mocks -mock_names=ContentValidator=MockContentValidator
type ContentValidator interface {
	// Validate inspects body (the first bytes of the response, capped at probeMaxBytes)
	// against the declared Content-Type header. totalLength is the full resource length
	// when known (Content-Range total or Content-Length) and -1 when unknown.
	Validate(declaredContentType string, body []byte, totalLength int64) ContentVerdict
}

type contentValidator struct {
	probeMaxBytes   int
	knownBadMarkers []string // lowercase substrings from config; matched against HTML bodies
}

// NewContentValidator creates a content validator. knownBadMarkers are case-insensitive
// substrings identifying gateway error pages served with HTTP 200 (operator-editable via
// config, no deploy needed for a new gateway quirk).
func NewContentValidator(probeMaxBytes int, knownBadMarkers []string) ContentValidator {
	lowered := make([]string, 0, len(knownBadMarkers))
	for _, m := range knownBadMarkers {
		if m = strings.ToLower(strings.TrimSpace(m)); m != "" {
			lowered = append(lowered, m)
		}
	}
	return &contentValidator{probeMaxBytes: probeMaxBytes, knownBadMarkers: lowered}
}

// builtinDirectoryMarkers identify IPFS/Kubo gateway directory listings. These pages are
// well-formed HTML with a 200 status — declared and sniffed types agree, so only body
// markers can catch them. Markers come from Kubo's dir-index-html template and are matched
// case-insensitively.
var builtinDirectoryMarkers = []string{
	"index of /ipfs",
	"index of /ipns",
	"ipfs-hash", // CSS class on every row of Kubo's dir-index-html listing
}

// Validate applies the rule ladder; the first failing rule wins, anything inconclusive is OK.
func (v *contentValidator) Validate(declaredContentType string, body []byte, totalLength int64) ContentVerdict {
	declared := normalizeContentType(declaredContentType)

	// 1. Empty body: nothing to display, regardless of what headers claim.
	if len(body) == 0 || totalLength == 0 {
		return ContentVerdict{
			FailureReason: FailureZeroLength,
			Detail:        "empty response body",
			Declared:      declared,
		}
	}

	// 2. Truncated: the connection ended before both the declared length and our read cap —
	// the server does not have the full bytes it advertised.
	if totalLength > 0 && int64(len(body)) < totalLength && len(body) < v.probeMaxBytes {
		return ContentVerdict{
			FailureReason: FailureTruncated,
			Detail:        fmt.Sprintf("body ended at %d of %d declared bytes", len(body), totalLength),
			Declared:      declared,
		}
	}

	sniffed := normalizeContentType(mimetype.Detect(body).String())
	verdict := ContentVerdict{OK: true, Declared: declared, Sniffed: sniffed}

	// 3–5 only apply to HTML bodies; a real media payload cannot be a directory listing
	// or a gateway error page.
	if sniffed == "text/html" {
		lowerBody := strings.ToLower(string(body))
		if marker := firstMarker(lowerBody, builtinDirectoryMarkers); marker != "" {
			verdict.OK = false
			verdict.FailureReason = FailureDirectoryListing
			verdict.Detail = fmt.Sprintf("IPFS gateway directory listing (marker %q)", marker)
			return verdict
		}
		if marker := firstMarker(lowerBody, v.knownBadMarkers); marker != "" {
			verdict.OK = false
			verdict.FailureReason = FailureKnownErrorPage
			verdict.Detail = fmt.Sprintf("known gateway error page (marker %q)", marker)
			return verdict
		}
	}

	// 6. Declared media but text body: an HTML/plain-text payload cannot be the image,
	// video, or audio the metadata promised (the classic 200-with-error-page case).
	// Declared HTML/JSON/text is never flagged — HTML artworks are legitimate.
	if isMediaClass(declared) && (sniffed == "text/html" || sniffed == "text/plain") {
		verdict.OK = false
		verdict.FailureReason = FailureTypeMismatch
		verdict.Detail = fmt.Sprintf("declared %s but body is %s", declared, sniffed)
		return verdict
	}

	// 7. Container header parse for formats where sniffing alone is too shallow: the
	// magic bytes match on the first few bytes, so a corrupt header still sniffs as the
	// format. Only recognized containers with enough bytes are checked; video is left to
	// the sniffer (mimetype's MP4 detection already validates the ftyp box).
	if reason, detail := validateContainer(sniffed, body); reason != "" {
		verdict.OK = false
		verdict.FailureReason = reason
		verdict.Detail = detail
		return verdict
	}

	return verdict
}

// normalizeContentType lowercases and strips parameters ("Image/PNG; charset=x" → "image/png").
func normalizeContentType(ct string) string {
	ct = strings.ToLower(strings.TrimSpace(ct))
	return strings.TrimSpace(strings.Split(ct, ";")[0])
}

// isMediaClass reports whether a declared type promises binary media content.
func isMediaClass(declared string) bool {
	return strings.HasPrefix(declared, "image/") ||
		strings.HasPrefix(declared, "video/") ||
		strings.HasPrefix(declared, "audio/")
}

// firstMarker returns the first marker contained in lowerBody, or "".
func firstMarker(lowerBody string, markers []string) string {
	for _, m := range markers {
		if strings.Contains(lowerBody, m) {
			return m
		}
	}
	return ""
}

// validateContainer parses the header of recognized image containers. Returns ("", "")
// when the container is valid, unrecognized, or there are not enough bytes to judge —
// inconclusive errs healthy.
func validateContainer(sniffed string, body []byte) (FailureReason, string) {
	var ok bool
	var detail string
	switch sniffed {
	case "image/png":
		ok, detail = validPNGHeader(body)
	case "image/gif":
		ok, detail = validGIFHeader(body)
	case "image/webp":
		ok, detail = validWebPHeader(body)
	case "image/jpeg":
		ok, detail = validJPEGStructure(body)
	default:
		return "", ""
	}
	if ok {
		return "", ""
	}
	return FailureContainerInvalid, detail
}

// validPNGHeader checks the PNG signature is followed by a well-formed IHDR chunk with
// non-zero dimensions (signature 8B + length 4B + "IHDR" 4B + width 4B + height 4B = 24B).
func validPNGHeader(body []byte) (bool, string) {
	if len(body) < 24 {
		return true, "" // not enough bytes to judge
	}
	if !bytes.Equal(body[12:16], []byte("IHDR")) {
		return false, "PNG signature without IHDR chunk"
	}
	width := binary.BigEndian.Uint32(body[16:20])
	height := binary.BigEndian.Uint32(body[20:24])
	if width == 0 || height == 0 {
		return false, fmt.Sprintf("PNG with zero dimension (%dx%d)", width, height)
	}
	return true, ""
}

// validGIFHeader checks the GIF logical screen descriptor has non-zero dimensions
// (header 6B + width 2B + height 2B = 10B).
func validGIFHeader(body []byte) (bool, string) {
	if len(body) < 10 {
		return true, ""
	}
	width := binary.LittleEndian.Uint16(body[6:8])
	height := binary.LittleEndian.Uint16(body[8:10])
	if width == 0 || height == 0 {
		return false, fmt.Sprintf("GIF with zero dimension (%dx%d)", width, height)
	}
	return true, ""
}

// validWebPHeader checks the RIFF container declares the WEBP fourcc and a plausible
// chunk size ("RIFF" 4B + size 4B + "WEBP" 4B = 12B).
func validWebPHeader(body []byte) (bool, string) {
	if len(body) < 12 {
		return true, ""
	}
	if !bytes.Equal(body[8:12], []byte("WEBP")) {
		return false, "RIFF container without WEBP fourcc"
	}
	if binary.LittleEndian.Uint32(body[4:8]) == 0 {
		return false, "RIFF container with zero size"
	}
	return true, ""
}

// validJPEGStructure walks JPEG marker segments from SOI as far as the probe window
// allows. A byte where a marker must start that is not 0xFF means the stream is corrupt;
// running out of window while the structure is still consistent is inconclusive (OK).
func validJPEGStructure(body []byte) (bool, string) {
	// SOI already implied by the sniffer; walk segments after the first two bytes.
	i := 2
	for i+4 <= len(body) {
		if body[i] != 0xFF {
			return false, fmt.Sprintf("JPEG segment marker expected at offset %d", i)
		}
		marker := body[i+1]
		// Standalone markers without a length field (TEM, RSTn) and padding bytes.
		if marker == 0xFF {
			i++
			continue
		}
		if marker == 0x01 || (marker >= 0xD0 && marker <= 0xD7) {
			i += 2
			continue
		}
		// SOS: entropy-coded data follows — structure beyond here is not walkable.
		if marker == 0xDA {
			return true, ""
		}
		segLen := int(binary.BigEndian.Uint16(body[i+2 : i+4]))
		if segLen < 2 {
			return false, fmt.Sprintf("JPEG segment with invalid length %d at offset %d", segLen, i)
		}
		i += 2 + segLen
	}
	return true, "" // ran out of window with consistent structure
}

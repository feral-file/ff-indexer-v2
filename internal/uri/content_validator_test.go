package uri_test

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/feral-file/ff-indexer-v2/internal/uri"
)

const testProbeMaxBytes = 32 * 1024

// minimalPNG returns a valid PNG signature + IHDR header with the given dimensions.
func minimalPNG(width, height uint32) []byte {
	buf := []byte{0x89, 'P', 'N', 'G', 0x0D, 0x0A, 0x1A, 0x0A} // signature
	buf = append(buf, 0x00, 0x00, 0x00, 0x0D)                  // IHDR length (13)
	buf = append(buf, []byte("IHDR")...)
	buf = binary.BigEndian.AppendUint32(buf, width)
	buf = binary.BigEndian.AppendUint32(buf, height)
	buf = append(buf, 8, 2, 0, 0, 0)          // bit depth, color type, compression, filter, interlace
	buf = append(buf, 0x00, 0x00, 0x00, 0x00) // CRC (not validated by the parser)
	return buf
}

// minimalGIF returns a GIF89a header + logical screen descriptor with the given dimensions.
func minimalGIF(width, height uint16) []byte {
	buf := []byte("GIF89a")
	buf = binary.LittleEndian.AppendUint16(buf, width)
	buf = binary.LittleEndian.AppendUint16(buf, height)
	buf = append(buf, 0x00, 0x00, 0x00) // GCT flags, background, aspect
	return buf
}

// minimalWebP returns a RIFF/WEBP header with a plausible chunk size.
func minimalWebP() []byte {
	buf := []byte("RIFF")
	buf = binary.LittleEndian.AppendUint32(buf, 1024)
	buf = append(buf, []byte("WEBP")...)
	buf = append(buf, []byte("VP8 ")...)
	return buf
}

// minimalJPEG returns SOI + APP0(JFIF) + SOS so the structure walk succeeds.
func minimalJPEG() []byte {
	buf := []byte{0xFF, 0xD8}                  // SOI
	buf = append(buf, 0xFF, 0xE0, 0x00, 0x10)  // APP0, length 16 (2 length bytes + 14 payload)
	buf = append(buf, []byte("JFIF\x00")...)   // identifier (5 bytes)
	buf = append(buf, make([]byte, 16-2-5)...) // rest of APP0 payload
	buf = append(buf, 0xFF, 0xDA, 0x00, 0x0C)  // SOS
	buf = append(buf, make([]byte, 10)...)     // entropy data
	return buf
}

func kuboDirectoryListing() []byte {
	return []byte(`<!DOCTYPE html><html><head><title>/ipfs/QmFoo</title></head>
<body><h1>Index of /ipfs/QmFoo</h1>
<table><tr><td class="ipfs-hash">QmBar</td></tr></table></body></html>`)
}

// modernKuboListingWindow is a faithful reduction of the first probe window of a Kubo
// 0.13+ dir-index-html listing as served by ipfs.io and dweb.link (captured 2026-08-11):
// the meta description sits at ~byte 70 and the path-as-title after the inlined favicon,
// while the file table — every "Index of" heading and class="ipfs-hash" cell — sits
// hundreds of KB later, past the 32KB probe window. Detection must succeed on this head
// alone.
func modernKuboListingWindow() []byte {
	return []byte(`<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="description" content="A directory of content-addressed files hosted on IPFS.">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <link rel="shortcut icon" href="data:image/x-icon;base64,AAABAAEAEBAAAAEAIABoBAAAFgAAACgAAAAQAAAAIAAAACAAAAABACAAAAAAAAAEAAA=">
  <title>/ipfs/QmdmQXB2mzChmMeKY47C43LxUdg1NDJ5MWcKMKxDu7RgQm/</title>
  <style>.flex{display:flex}.flex-wrap{flex-flow:wrap}.nowrap{white-space:nowrap}</style>
</head>
<body><main><header class="flex flex-wrap"><div><strong>`)
}

func TestContentValidator_Validate(t *testing.T) {
	v := uri.NewContentValidator(testProbeMaxBytes, []string{"gateway time-out", "504 Gateway Time-out"})

	tests := []struct {
		name        string
		declared    string
		body        []byte
		totalLength int64
		wantOK      bool
		wantReason  uri.FailureReason
		wantSniffed string
	}{
		// --- healthy paths ---
		{
			name:        "valid PNG declared as PNG",
			declared:    "image/png",
			body:        minimalPNG(100, 100),
			totalLength: -1,
			wantOK:      true,
			wantSniffed: "image/png",
		},
		{
			name:        "valid GIF",
			declared:    "image/gif",
			body:        minimalGIF(10, 10),
			totalLength: -1,
			wantOK:      true,
			wantSniffed: "image/gif",
		},
		{
			name:        "valid WebP",
			declared:    "image/webp",
			body:        minimalWebP(),
			totalLength: -1,
			wantOK:      true,
			wantSniffed: "image/webp",
		},
		{
			name:        "valid JPEG",
			declared:    "image/jpeg",
			body:        minimalJPEG(),
			totalLength: -1,
			wantOK:      true,
			wantSniffed: "image/jpeg",
		},
		{
			name:        "HTML artwork declared as HTML is never flagged",
			declared:    "text/html; charset=utf-8",
			body:        []byte("<!DOCTYPE html><html><body><canvas></canvas></body></html>"),
			totalLength: -1,
			wantOK:      true,
			wantSniffed: "text/html",
		},
		{
			name:        "declared PNG sniffed as different image class errs healthy",
			declared:    "image/png",
			body:        minimalGIF(5, 5), // mislabeling across image formats is common on gateways
			totalLength: -1,
			wantOK:      true,
			wantSniffed: "image/gif",
		},
		{
			name:        "SVG content",
			declared:    "image/svg+xml",
			body:        []byte(`<?xml version="1.0"?><svg xmlns="http://www.w3.org/2000/svg"><rect/></svg>`),
			totalLength: -1,
			wantOK:      true,
			wantSniffed: "image/svg+xml",
		},
		{
			name:        "empty declared type with valid body errs healthy",
			declared:    "",
			body:        minimalPNG(64, 64),
			totalLength: -1,
			wantOK:      true,
			wantSniffed: "image/png",
		},
		{
			name:        "full 32KB window read of a larger resource is not truncated",
			declared:    "image/png",
			body:        append(minimalPNG(2048, 2048), make([]byte, testProbeMaxBytes-33)...)[:testProbeMaxBytes],
			totalLength: 10 << 20, // 10MB total
			wantOK:      true,
			wantSniffed: "image/png",
		},

		// --- broken paths ---
		{
			name:        "empty body",
			declared:    "image/png",
			body:        nil,
			totalLength: 0,
			wantOK:      false,
			wantReason:  uri.FailureZeroLength,
		},
		{
			name:        "body shorter than declared length",
			declared:    "image/png",
			body:        minimalPNG(100, 100), // 38 bytes
			totalLength: 500_000,
			wantOK:      false,
			wantReason:  uri.FailureTruncated,
		},
		{
			name:        "Kubo directory listing (feral-file#3482)",
			declared:    "text/html",
			body:        kuboDirectoryListing(),
			totalLength: -1,
			wantOK:      false,
			wantReason:  uri.FailureDirectoryListing,
			wantSniffed: "text/html",
		},
		{
			name:        "configured known error page marker",
			declared:    "text/html",
			body:        []byte("<html><head><title>504 Gateway Time-out</title></head><body>nginx</body></html>"),
			totalLength: -1,
			wantOK:      false,
			wantReason:  uri.FailureKnownErrorPage,
			wantSniffed: "text/html",
		},
		{
			name:        "declared image but HTML error body (bug #76 class)",
			declared:    "image/png",
			body:        []byte("<!DOCTYPE html><html><body>Something went wrong</body></html>"),
			totalLength: -1,
			wantOK:      false,
			wantReason:  uri.FailureTypeMismatch,
			wantSniffed: "text/html",
		},
		{
			name:        "declared video but plain text body",
			declared:    "video/mp4",
			body:        []byte("this CID is not pinned anymore, sorry"),
			totalLength: -1,
			wantOK:      false,
			wantReason:  uri.FailureTypeMismatch,
			wantSniffed: "text/plain",
		},
		{
			name:        "PNG signature with corrupt IHDR",
			declared:    "image/png",
			body:        append([]byte{0x89, 'P', 'N', 'G', 0x0D, 0x0A, 0x1A, 0x0A}, bytes.Repeat([]byte{0xAB}, 32)...),
			totalLength: -1,
			wantOK:      false,
			wantReason:  uri.FailureContainerInvalid,
			wantSniffed: "image/png",
		},
		{
			name:        "PNG with zero dimensions",
			declared:    "image/png",
			body:        minimalPNG(0, 100),
			totalLength: -1,
			wantOK:      false,
			wantReason:  uri.FailureContainerInvalid,
		},
		{
			name:        "GIF with zero dimensions",
			declared:    "image/gif",
			body:        minimalGIF(0, 0),
			totalLength: -1,
			wantOK:      false,
			wantReason:  uri.FailureContainerInvalid,
		},
		{
			name: "JPEG with corrupted segment structure",
			// Sniffs as image/jpeg (FF D8 FF prefix) but the byte where the next segment
			// marker must start after APP0 is not 0xFF.
			declared:    "image/jpeg",
			body:        []byte{0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x04, 0x01, 0x02, 0x00, 0x00, 0x00, 0x00},
			totalLength: -1,
			wantOK:      false,
			wantReason:  uri.FailureContainerInvalid,
			wantSniffed: "image/jpeg",
		},

		// --- inconclusive errs healthy ---
		{
			name:        "PNG prefix too short to judge",
			declared:    "image/png",
			body:        []byte{0x89, 'P', 'N', 'G', 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00},
			totalLength: -1,
			wantOK:      true,
		},
		{
			name:        "unknown binary type is not judged",
			declared:    "application/octet-stream",
			body:        bytes.Repeat([]byte{0x42}, 64),
			totalLength: -1,
			wantOK:      true,
		},

		// --- JSON error bodies are the same 200-with-error class as HTML ones ---
		{
			name:        "declared image but JSON error body",
			declared:    "image/png",
			body:        []byte(`{"error":"upstream fetch failed","status":200,"detail":"cid not found"}`),
			totalLength: -1,
			wantOK:      false,
			wantReason:  uri.FailureTypeMismatch,
		},
		{
			name:        "declared video but JSON error body",
			declared:    "video/mp4",
			body:        []byte(`{"error":"gateway timeout"}`),
			totalLength: -1,
			wantOK:      false,
			wantReason:  uri.FailureTypeMismatch,
		},
		{
			name:        "declared JSON with JSON body is never flagged",
			declared:    "application/json",
			body:        []byte(`{"name":"token #1"}`),
			totalLength: -1,
			wantOK:      true,
		},

		// --- text-based media subtypes are exempt from the text-body mismatch ---
		// These verdicts must hold under ANY sniff outcome: the sniffer inspects only
		// its own internal window (3072 bytes in the pinned mimetype version), so a
		// text format whose distinguishing token sits past that window sniffs as
		// text/plain, and a version bump can shift classifications. Assert verdicts,
		// never sniffed types.
		{
			name:        "SVG with large preamble before the root element",
			declared:    "image/svg+xml",
			body:        append([]byte("<!--"+strings.Repeat("license ", 512)+"-->\n"), []byte(`<svg xmlns="http://www.w3.org/2000/svg"><rect width="10" height="10"/></svg>`)...),
			totalLength: -1,
			wantOK:      true,
		},
		{
			name:        "SVG with XML declaration and large preamble",
			declared:    "image/svg+xml",
			body:        append([]byte(`<?xml version="1.0" encoding="UTF-8"?><!--`+strings.Repeat("c", 4000)+"-->"), []byte(`<svg xmlns="http://www.w3.org/2000/svg"><rect/></svg>`)...),
			totalLength: -1,
			wantOK:      true,
		},
		{
			name:        "ASCII PNM is a text image format",
			declared:    "image/x-portable-anymap",
			body:        []byte("P3\n2 2\n255\n255 0 0  0 255 0\n0 0 255  255 255 255\n"),
			totalLength: -1,
			wantOK:      true,
		},
		{
			name:        "XBM is C source text",
			declared:    "image/x-xbitmap",
			body:        []byte("#define img_width 8\n#define img_height 8\nstatic unsigned char img_bits[] = { 0xFF };"),
			totalLength: -1,
			wantOK:      true,
		},
		{
			name:        "HLS playlist is a text audio format",
			declared:    "audio/x-mpegurl",
			body:        []byte("#EXTM3U\n#EXT-X-VERSION:3\n#EXTINF:10,\nseg0.ts\n"),
			totalLength: -1,
			wantOK:      true,
		},

		// --- directory-listing detection: structural conjunction, not lone substrings ---
		{
			name:        "modern Kubo listing detected from the probe window alone",
			declared:    "text/html",
			body:        modernKuboListingWindow(),
			totalLength: -1,
			wantOK:      false,
			wantReason:  uri.FailureDirectoryListing,
		},
		{
			name:        "HTML artwork using an ipfs-hash class is not a listing",
			declared:    "text/html",
			body:        []byte(`<!DOCTYPE html><html><head><title>chain study #4</title></head><body><div class="ipfs-hash">QmSeed42</div><script>render()</script></body></html>`),
			totalLength: -1,
			wantOK:      true,
		},
		{
			name:        "HTML artwork mentioning index of /ipfs is not a listing",
			declared:    "text/html",
			body:        []byte(`<!DOCTYPE html><html><head><title>archive piece</title></head><body><p>sourced from an index of /ipfs snapshots</p></body></html>`),
			totalLength: -1,
			wantOK:      true,
		},
		{
			name:        "path-as-title alone is not a listing",
			declared:    "text/html",
			body:        []byte(`<!DOCTYPE html><html><head><title>/ipfs/QmFoo/</title></head><body><canvas></canvas></body></html>`),
			totalLength: -1,
			wantOK:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := v.Validate(tt.declared, tt.body, tt.totalLength)
			assert.Equal(t, tt.wantOK, got.OK)
			if !tt.wantOK {
				assert.Equal(t, tt.wantReason, got.FailureReason)
				assert.NotEmpty(t, got.Detail)
			} else {
				assert.Empty(t, string(got.FailureReason))
				assert.Empty(t, got.Detail)
			}
			if tt.wantSniffed != "" {
				assert.Equal(t, tt.wantSniffed, got.Sniffed)
			}
		})
	}
}

func TestContentValidator_MarkerNormalization(t *testing.T) {
	// Markers are matched case-insensitively and trimmed; empty markers are dropped.
	v := uri.NewContentValidator(testProbeMaxBytes, []string{"  BLOCKED CONTENT  ", ""})
	got := v.Validate("text/html", []byte("<html><body>blocked content</body></html>"), -1)
	assert.False(t, got.OK)
	assert.Equal(t, uri.FailureKnownErrorPage, got.FailureReason)
}

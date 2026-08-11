package uri_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/security/ssrf"
	"github.com/feral-file/ff-indexer-v2/internal/uri"
)

// mockRetryableError is a mock error that implements net.Error interface
type mockRetryableError struct{}

func (e *mockRetryableError) Error() string {
	return "temporary network error"
}

func (e *mockRetryableError) Temporary() bool {
	return true
}

func (e *mockRetryableError) Timeout() bool {
	return true // This makes it retryable
}

// probeRangeHeader is the Range header the content-validating probe sends
// (uri.DefaultProbeMaxBytes window).
var probeRangeHeader = map[string]string{"Range": fmt.Sprintf("bytes=0-%d", uri.DefaultProbeMaxBytes-1)}

// httpResp builds a response with a readable body, optional Content-Type and extra headers.
func httpResp(status int, contentType string, body []byte, extra map[string]string) *http.Response {
	h := http.Header{}
	if contentType != "" {
		h.Set("Content-Type", contentType)
	}
	for k, v := range extra {
		h.Set(k, v)
	}
	return &http.Response{
		StatusCode:    status,
		Header:        h,
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
	}
}

// passthroughIO makes the IO mock actually read bodies, which the content-validating
// probe requires (the old checker discarded them).
func passthroughIO(mockIO *mocks.MockIO) {
	mockIO.EXPECT().
		ReadAll(gomock.Any()).
		DoAndReturn(func(r io.Reader) ([]byte, error) { return io.ReadAll(r) }).
		AnyTimes()
}

func defaultConfig() *uri.Config {
	return &uri.Config{
		IPFSGateways:        []string{"https://ipfs.io"},
		ArweaveGateways:     []string{"https://arweave.net"},
		OnChFSGateways:      []string{"https://onchfs.fxhash2.xyz"},
		KnownBadPageMarkers: []string{"504 gateway time-out"},
	}
}

func strPtr(s string) *string { return &s }

func TestURLChecker_Check(t *testing.T) {
	cid := "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG"

	tests := []struct {
		name           string
		url            string
		setupMocks     func(*mocks.MockHTTPClient, *mocks.MockIO)
		config         *uri.Config
		expectedStatus uri.HealthStatus
		expectedURL    *string
		expectedReason uri.FailureReason
		expectSSRF     bool
	}{
		{
			name: "healthy 206 with valid PNG content",
			url:  "https://example.com/art.png",
			setupMocks: func(m *mocks.MockHTTPClient, mio *mocks.MockIO) {
				passthroughIO(mio)
				m.EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://example.com/art.png", probeRangeHeader).
					Return(httpResp(http.StatusPartialContent, "image/png", minimalPNG(64, 64),
						map[string]string{"Content-Range": "bytes 0-32/33"}), nil) // minimalPNG is 33 bytes
			},
			expectedStatus: uri.HealthStatusHealthy,
		},
		{
			name: "healthy 200 when server ignores Range",
			url:  "https://example.com/art.png",
			setupMocks: func(m *mocks.MockHTTPClient, mio *mocks.MockIO) {
				passthroughIO(mio)
				m.EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://example.com/art.png", probeRangeHeader).
					Return(httpResp(http.StatusOK, "image/png", minimalPNG(64, 64), nil), nil)
			},
			expectedStatus: uri.HealthStatusHealthy,
		},
		{
			name: "203 from a transforming proxy with valid content is healthy (2xx contract)",
			url:  "https://example.com/proxied.png",
			setupMocks: func(m *mocks.MockHTTPClient, mio *mocks.MockIO) {
				passthroughIO(mio)
				m.EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://example.com/proxied.png", probeRangeHeader).
					Return(httpResp(http.StatusNonAuthoritativeInfo, "image/png", minimalPNG(64, 64), nil), nil)
			},
			expectedStatus: uri.HealthStatusHealthy,
		},
		{
			name: "204 empty response is broken as zero_length, not http_status",
			url:  "https://example.com/nothing.png",
			setupMocks: func(m *mocks.MockHTTPClient, mio *mocks.MockIO) {
				passthroughIO(mio)
				m.EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://example.com/nothing.png", probeRangeHeader).
					Return(httpResp(http.StatusNoContent, "image/png", nil, nil), nil)
			},
			expectedStatus: uri.HealthStatusBroken,
			expectedReason: uri.FailureZeroLength,
		},
		{
			name: "mid-body read failure with declared length is broken as truncated, not transient",
			url:  "https://example.com/cut-short.png",
			setupMocks: func(m *mocks.MockHTTPClient, mio *mocks.MockIO) {
				// The server declares 500KB but the connection dies after a partial read:
				// conclusive truncation evidence, so the partial bytes must reach the
				// validator instead of being discarded as a transient error (a
				// consistently truncating server would otherwise stay healthy forever —
				// the sweeper never persists transient results).
				resp := httpResp(http.StatusOK, "image/png", minimalPNG(64, 64), nil)
				resp.ContentLength = 500_000
				m.EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://example.com/cut-short.png", probeRangeHeader).
					Return(resp, nil)
				mio.EXPECT().
					ReadAll(gomock.Any()).
					DoAndReturn(func(r io.Reader) ([]byte, error) {
						partial, _ := io.ReadAll(r)
						return partial, io.ErrUnexpectedEOF
					})
			},
			expectedStatus: uri.HealthStatusBroken,
			expectedReason: uri.FailureTruncated,
		},
		{
			name: "mid-body read failure without length evidence stays transient",
			url:  "https://example.com/flaky-read.png",
			setupMocks: func(m *mocks.MockHTTPClient, mio *mocks.MockIO) {
				resp := httpResp(http.StatusOK, "image/png", minimalPNG(64, 64), nil)
				resp.ContentLength = -1 // chunked/unknown: the shortfall could be weather
				m.EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://example.com/flaky-read.png", probeRangeHeader).
					Return(resp, nil)
				mio.EXPECT().
					ReadAll(gomock.Any()).
					Return([]byte{0x89, 'P'}, io.ErrUnexpectedEOF)
			},
			expectedStatus: uri.HealthStatusTransientError,
		},
		{
			name: "206 with mid-file Content-Range retries unranged and validates the true prefix",
			url:  "https://example.com/mid-range.png",
			setupMocks: func(m *mocks.MockHTTPClient, mio *mocks.MockIO) {
				passthroughIO(mio)
				// Misbehaving gateway answers with bytes from the middle of the file —
				// not the prefix the validator's checks are defined over.
				m.EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://example.com/mid-range.png", probeRangeHeader).
					Return(httpResp(http.StatusPartialContent, "image/png", []byte("mid-file garbage"),
						map[string]string{"Content-Range": "bytes 1000-33767/500000"}), nil)
				m.EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://example.com/mid-range.png", nil).
					Return(httpResp(http.StatusOK, "image/png", minimalPNG(64, 64), nil), nil)
			},
			expectedStatus: uri.HealthStatusHealthy,
		},
		{
			name: "206 without Content-Range cannot bypass directory-listing detection",
			url:  "https://example.com/gateway/dir-listing",
			setupMocks: func(m *mocks.MockHTTPClient, mio *mocks.MockIO) {
				passthroughIO(mio)
				// Malformed 206 (no Content-Range) carrying innocuous bytes: they are
				// not trusted as the prefix; the unranged retry reveals the listing.
				m.EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://example.com/gateway/dir-listing", probeRangeHeader).
					Return(httpResp(http.StatusPartialContent, "image/png", minimalPNG(64, 64), nil), nil)
				m.EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://example.com/gateway/dir-listing", nil).
					Return(httpResp(http.StatusOK, "text/html", kuboDirectoryListing(), nil), nil)
			},
			expectedStatus: uri.HealthStatusBroken,
			expectedReason: uri.FailureDirectoryListing,
		},
		{
			name: "206 with malformed range end retries unranged (full grammar required)",
			url:  "https://example.com/bad-end.png",
			setupMocks: func(m *mocks.MockHTTPClient, mio *mocks.MockIO) {
				passthroughIO(mio)
				// Prefix-plausible but malformed: "bytes 0-" alone must not buy trust.
				m.EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://example.com/bad-end.png", probeRangeHeader).
					Return(httpResp(http.StatusPartialContent, "image/png", []byte("untrusted bytes"),
						map[string]string{"Content-Range": "bytes 0-not-a-range/500000"}), nil)
				m.EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://example.com/bad-end.png", nil).
					Return(httpResp(http.StatusOK, "image/png", minimalPNG(64, 64), nil), nil)
			},
			expectedStatus: uri.HealthStatusHealthy,
		},
		{
			name: "206 on the unranged retry is protocol-broken even with a plausible from-zero range",
			url:  "https://example.com/liar.png",
			setupMocks: func(m *mocks.MockHTTPClient, mio *mocks.MockIO) {
				// First 206 has a mid-file range → retry unranged; the gateway answers
				// 206 again with a valid-looking from-zero range. A 206 to a request
				// without Range is never trusted.
				m.EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://example.com/liar.png", probeRangeHeader).
					Return(httpResp(http.StatusPartialContent, "image/png", []byte("mid-file"),
						map[string]string{"Content-Range": "bytes 1000-33767/500000"}), nil)
				m.EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://example.com/liar.png", nil).
					Return(httpResp(http.StatusPartialContent, "image/png", minimalPNG(64, 64),
						map[string]string{"Content-Range": "bytes 0-32/500000"}), nil)
			},
			expectedStatus: uri.HealthStatusBroken,
			expectedReason: uri.FailureHTTPStatus,
		},
		{
			name: "206 satisfying a shorter from-zero range than requested is healthy (promised length, not total)",
			url:  "https://example.com/short-range.png",
			setupMocks: func(m *mocks.MockHTTPClient, mio *mocks.MockIO) {
				passthroughIO(mio)
				// The gateway legitimately satisfies 0-9999 of a 500KB object and
				// delivers exactly those 10000 bytes. Judging truncation against the
				// 500000 total would false-broken valid media.
				body := append(minimalPNG(64, 64), make([]byte, 10_000-33)...)
				m.EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://example.com/short-range.png", probeRangeHeader).
					Return(httpResp(http.StatusPartialContent, "image/png", body,
						map[string]string{"Content-Range": "bytes 0-9999/500000"}), nil)
			},
			expectedStatus: uri.HealthStatusHealthy,
		},
		{
			name: "valid 206 too short to classify retries unranged (err-healthy)",
			url:  "https://example.com/one-byte-range.gif",
			setupMocks: func(m *mocks.MockHTTPClient, mio *mocks.MockIO) {
				passthroughIO(mio)
				// A valid from-zero 206 promising a single byte of a 5MB GIF: "G"
				// sniffs as text/plain, so validating this prefix would false-broken
				// real media as type_mismatch. The probe must fetch the true prefix.
				m.EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://example.com/one-byte-range.gif", probeRangeHeader).
					Return(httpResp(http.StatusPartialContent, "image/gif", []byte("G"),
						map[string]string{"Content-Range": "bytes 0-0/5000000"}), nil)
				m.EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://example.com/one-byte-range.gif", nil).
					Return(httpResp(http.StatusOK, "image/gif", minimalGIF(10, 10), nil), nil)
			},
			expectedStatus: uri.HealthStatusHealthy,
		},
		{
			name: "tiny 206 that is the complete resource is conclusive, no retry",
			url:  "https://example.com/tiny-error.png",
			setupMocks: func(m *mocks.MockHTTPClient, mio *mocks.MockIO) {
				passthroughIO(mio)
				// bytes 0-9/10: the prefix IS the whole resource, so a text body for a
				// declared image is the complete evidence, not an undersized window.
				m.EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://example.com/tiny-error.png", probeRangeHeader).
					Return(httpResp(http.StatusPartialContent, "image/png", []byte("not found\n"),
						map[string]string{"Content-Range": "bytes 0-9/10"}), nil)
			},
			expectedStatus: uri.HealthStatusBroken,
			expectedReason: uri.FailureTypeMismatch,
		},
		{
			name: "206 delivering fewer bytes than its own range promised is truncated",
			url:  "https://example.com/undelivered.png",
			setupMocks: func(m *mocks.MockHTTPClient, mio *mocks.MockIO) {
				passthroughIO(mio)
				// Promised 10000 bytes (0-9999), delivered 33: short of the response's
				// own promise and of the probe cap → truncated.
				m.EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://example.com/undelivered.png", probeRangeHeader).
					Return(httpResp(http.StatusPartialContent, "image/png", minimalPNG(64, 64),
						map[string]string{"Content-Range": "bytes 0-9999/500000"}), nil)
			},
			expectedStatus: uri.HealthStatusBroken,
			expectedReason: uri.FailureTruncated,
		},
		{
			name: "416 falls back to unranged GET",
			url:  "https://example.com/art.gif",
			setupMocks: func(m *mocks.MockHTTPClient, mio *mocks.MockIO) {
				passthroughIO(mio)
				m.EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://example.com/art.gif", probeRangeHeader).
					Return(httpResp(http.StatusRequestedRangeNotSatisfiable, "", nil, nil), nil)
				m.EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://example.com/art.gif", nil).
					Return(httpResp(http.StatusOK, "image/gif", minimalGIF(10, 10), nil), nil)
			},
			expectedStatus: uri.HealthStatusHealthy,
		},
		{
			name: "404 is broken with http_status reason",
			url:  "https://example.com/gone.png",
			setupMocks: func(m *mocks.MockHTTPClient, _ *mocks.MockIO) {
				m.EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://example.com/gone.png", probeRangeHeader).
					Return(httpResp(http.StatusNotFound, "", nil, nil), nil)
			},
			expectedStatus: uri.HealthStatusBroken,
			expectedReason: uri.FailureHTTPStatus,
		},
		{
			name: "429 is transient",
			url:  "https://example.com/busy.png",
			setupMocks: func(m *mocks.MockHTTPClient, _ *mocks.MockIO) {
				m.EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://example.com/busy.png", probeRangeHeader).
					Return(httpResp(http.StatusTooManyRequests, "", nil, nil), nil)
			},
			expectedStatus: uri.HealthStatusTransientError,
		},
		{
			name: "200 HTML error page declared as image is broken (bug #76 class)",
			url:  "https://example.com/broken.png",
			setupMocks: func(m *mocks.MockHTTPClient, mio *mocks.MockIO) {
				passthroughIO(mio)
				m.EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://example.com/broken.png", probeRangeHeader).
					Return(httpResp(http.StatusOK, "image/png",
						[]byte("<!DOCTYPE html><html><body>oops</body></html>"), nil), nil)
			},
			expectedStatus: uri.HealthStatusBroken,
			expectedReason: uri.FailureTypeMismatch,
		},
		{
			name: "200 known error page marker is broken",
			url:  "https://example.com/timeout",
			setupMocks: func(m *mocks.MockHTTPClient, mio *mocks.MockIO) {
				passthroughIO(mio)
				m.EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://example.com/timeout", probeRangeHeader).
					Return(httpResp(http.StatusOK, "text/html",
						[]byte("<html><title>504 Gateway Time-out</title></html>"), nil), nil)
			},
			expectedStatus: uri.HealthStatusBroken,
			expectedReason: uri.FailureKnownErrorPage,
		},
		{
			name: "retryable transport error is transient",
			url:  "https://example.com/flaky.png",
			setupMocks: func(m *mocks.MockHTTPClient, _ *mocks.MockIO) {
				m.EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://example.com/flaky.png", probeRangeHeader).
					Return(nil, &mockRetryableError{})
			},
			expectedStatus: uri.HealthStatusTransientError,
		},
		{
			name: "non-retryable transport error is broken without a reason",
			url:  "https://example.com/dead.png",
			setupMocks: func(m *mocks.MockHTTPClient, _ *mocks.MockIO) {
				m.EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://example.com/dead.png", probeRangeHeader).
					Return(nil, assert.AnError)
			},
			expectedStatus: uri.HealthStatusBroken,
			expectedReason: "",
		},
		{
			name: "DNS resolution failure is broken with dns reason, not SSRF-blocked",
			url:  "https://no-such-host.example/art.png",
			setupMocks: func(m *mocks.MockHTTPClient, _ *mocks.MockIO) {
				m.EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://no-such-host.example/art.png", probeRangeHeader).
					Return(nil, fmt.Errorf("lookup failed: %w", ssrf.ErrResolutionFailed))
			},
			expectedStatus: uri.HealthStatusBroken,
			expectedReason: uri.FailureDNS,
			expectSSRF:     false,
		},
		{
			name: "invalid URL format",
			url:  "not a url",
			setupMocks: func(_ *mocks.MockHTTPClient, _ *mocks.MockIO) {
			},
			expectedStatus: uri.HealthStatusBroken,
		},
		{
			name: "non-HTTP scheme is broken",
			url:  "ftp://example.com/file",
			setupMocks: func(_ *mocks.MockHTTPClient, _ *mocks.MockIO) {
			},
			expectedStatus: uri.HealthStatusBroken,
		},
		{
			name: "IPFS gateway URL healthy directly - no fallback probes",
			url:  "https://gateway.pinata.cloud/ipfs/" + cid,
			setupMocks: func(m *mocks.MockHTTPClient, mio *mocks.MockIO) {
				passthroughIO(mio)
				m.EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://gateway.pinata.cloud/ipfs/"+cid, probeRangeHeader).
					Return(httpResp(http.StatusOK, "image/png", minimalPNG(32, 32), nil), nil)
			},
			expectedStatus: uri.HealthStatusHealthy,
		},
		{
			name: "IPFS gateway URL broken - fallback gateway with valid content wins",
			url:  "https://gateway.pinata.cloud/ipfs/" + cid,
			setupMocks: func(m *mocks.MockHTTPClient, mio *mocks.MockIO) {
				passthroughIO(mio)
				m.EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://gateway.pinata.cloud/ipfs/"+cid, probeRangeHeader).
					Return(httpResp(http.StatusNotFound, "", nil, nil), nil)
				m.EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://ipfs.io/ipfs/"+cid, probeRangeHeader).
					Return(httpResp(http.StatusOK, "image/png", minimalPNG(32, 32), nil), nil)
			},
			expectedStatus: uri.HealthStatusHealthy,
			expectedURL:    strPtr("https://ipfs.io/ipfs/" + cid),
		},
		{
			name: "directory CID is not rescued by fallback gateways serving the same listing (feral-file#3482)",
			url:  "https://gateway.pinata.cloud/ipfs/" + cid,
			setupMocks: func(m *mocks.MockHTTPClient, mio *mocks.MockIO) {
				passthroughIO(mio)
				m.EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://gateway.pinata.cloud/ipfs/"+cid, probeRangeHeader).
					Return(httpResp(http.StatusOK, "text/html", kuboDirectoryListing(), nil), nil)
				m.EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://ipfs.io/ipfs/"+cid, probeRangeHeader).
					Return(httpResp(http.StatusOK, "text/html", kuboDirectoryListing(), nil), nil)
			},
			expectedStatus: uri.HealthStatusBroken,
			expectedReason: uri.FailureDirectoryListing,
		},
		{
			name: "IPFS fallback with retryable gateway errors keeps the direct broken result, not transient",
			url:  "https://gateway.pinata.cloud/ipfs/" + cid,
			setupMocks: func(m *mocks.MockHTTPClient, mio *mocks.MockIO) {
				passthroughIO(mio)
				m.EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://gateway.pinata.cloud/ipfs/"+cid, probeRangeHeader).
					Return(httpResp(http.StatusNotFound, "", nil, nil), nil)
				m.EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://ipfs.io/ipfs/"+cid, probeRangeHeader).
					Return(nil, &mockRetryableError{})
			},
			expectedStatus: uri.HealthStatusBroken,
			expectedReason: uri.FailureHTTPStatus,
		},
		{
			name: "Arweave gateway URL broken - fallback resolves",
			url:  "https://ar-io.net/sKqjvP7jFwM5HLZmyJQC_9l5hN7TVIYhT6MvSHDqwo0",
			setupMocks: func(m *mocks.MockHTTPClient, mio *mocks.MockIO) {
				passthroughIO(mio)
				m.EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://ar-io.net/sKqjvP7jFwM5HLZmyJQC_9l5hN7TVIYhT6MvSHDqwo0", probeRangeHeader).
					Return(nil, assert.AnError)
				m.EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://arweave.net/sKqjvP7jFwM5HLZmyJQC_9l5hN7TVIYhT6MvSHDqwo0", probeRangeHeader).
					Return(httpResp(http.StatusOK, "image/webp", minimalWebP(), nil), nil)
			},
			expectedStatus: uri.HealthStatusHealthy,
			expectedURL:    strPtr("https://arweave.net/sKqjvP7jFwM5HLZmyJQC_9l5hN7TVIYhT6MvSHDqwo0"),
		},
		{
			name: "OnChFS URL broken - fallback resolves",
			url:  "https://onchfs.example.com/a1b2c3d4e5f67890abcdef1234567890abcdef1234567890abcdef1234567890",
			setupMocks: func(m *mocks.MockHTTPClient, mio *mocks.MockIO) {
				passthroughIO(mio)
				m.EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://onchfs.example.com/a1b2c3d4e5f67890abcdef1234567890abcdef1234567890abcdef1234567890", probeRangeHeader).
					Return(httpResp(http.StatusInternalServerError, "", nil, nil), nil)
				m.EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://onchfs.fxhash2.xyz/a1b2c3d4e5f67890abcdef1234567890abcdef1234567890abcdef1234567890", probeRangeHeader).
					Return(httpResp(http.StatusOK, "image/png", minimalPNG(16, 16), nil), nil)
			},
			expectedStatus: uri.HealthStatusHealthy,
			expectedURL:    strPtr("https://onchfs.fxhash2.xyz/a1b2c3d4e5f67890abcdef1234567890abcdef1234567890abcdef1234567890"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockHTTP := mocks.NewMockHTTPClient(ctrl)
			mockIO := mocks.NewMockIO(ctrl)
			tt.setupMocks(mockHTTP, mockIO)

			cfg := tt.config
			if cfg == nil {
				cfg = defaultConfig()
			}
			checker := uri.NewURLChecker(mockHTTP, mockIO, cfg)
			result := checker.Check(context.Background(), tt.url)

			assert.Equal(t, tt.expectedStatus, result.Status)
			assert.Equal(t, tt.expectSSRF, result.SSRFBlocked)
			if tt.expectedURL != nil {
				require.NotNil(t, result.WorkingURL)
				assert.Equal(t, *tt.expectedURL, *result.WorkingURL)
			}
			if tt.expectedReason != "" {
				assert.Equal(t, tt.expectedReason, result.FailureReason)
			}
			if tt.expectedStatus != uri.HealthStatusHealthy {
				require.NotNil(t, result.Error)
			}
		})
	}
}

// TestURLChecker_healthyResultCarriesContentTypes ensures observed/sniffed types are
// populated on healthy verdicts too — they drive render-probe class selection downstream.
func TestURLChecker_healthyResultCarriesContentTypes(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTP := mocks.NewMockHTTPClient(ctrl)
	mockIO := mocks.NewMockIO(ctrl)
	passthroughIO(mockIO)

	mockHTTP.EXPECT().
		GetResponseNoRetry(gomock.Any(), "https://example.com/art.png", probeRangeHeader).
		Return(httpResp(http.StatusOK, "Image/PNG; charset=binary", minimalPNG(64, 64), nil), nil)

	checker := uri.NewURLChecker(mockHTTP, mockIO, defaultConfig())
	result := checker.Check(context.Background(), "https://example.com/art.png")

	require.Equal(t, uri.HealthStatusHealthy, result.Status)
	assert.Equal(t, "image/png", result.ObservedContentType) // normalized
	assert.Equal(t, "image/png", result.SniffedContentType)
}

// SSRF policy refusals are final: no gateway fallback may re-probe public gateways and
// rewrite a blocklisted origin as healthy.
func TestURLChecker_ssrfBlocked_skipsGatewayFallback(t *testing.T) {
	cid := "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG"

	tests := []struct {
		name string
		url  string
	}{
		{name: "IPFS gateway URL", url: "http://127.0.0.1/ipfs/" + cid},
		{name: "Arweave gateway URL", url: "http://127.0.0.1/sKqjvP7jFwM5HLZmyJQC_9l5hN7TVIYhT6MvSHDqwo0"},
		{name: "OnChFS URL", url: "https://onchfs.fxhash2.xyz/a1b2c3d4e5f67890abcdef1234567890abcdef1234567890abcdef1234567890"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockHTTP := mocks.NewMockHTTPClient(ctrl)
			mockIO := mocks.NewMockIO(ctrl)

			// Exactly one direct probe; no gateway probes may follow.
			mockHTTP.EXPECT().
				GetResponseNoRetry(gomock.Any(), tt.url, probeRangeHeader).
				Return(nil, fmt.Errorf("blocked: %w", ssrf.ErrBlocked)).
				Times(1)

			checker := uri.NewURLChecker(mockHTTP, mockIO, defaultConfig())
			result := checker.Check(context.Background(), tt.url)

			require.Equal(t, uri.HealthStatusBroken, result.Status)
			require.True(t, result.SSRFBlocked)
			assert.Equal(t, uri.FailureSSRF, result.FailureReason)
			require.Nil(t, result.WorkingURL)
			require.NotNil(t, result.Error)
		})
	}
}

// An SSRF block on a fallback gateway probe surfaces as SSRF-blocked (policy wins over
// the direct result).
func TestURLChecker_ssrfBlocked_onGatewayProbe(t *testing.T) {
	cid := "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG"
	directURL := "https://gateway.pinata.cloud/ipfs/" + cid

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTP := mocks.NewMockHTTPClient(ctrl)
	mockIO := mocks.NewMockIO(ctrl)
	passthroughIO(mockIO)

	mockHTTP.EXPECT().
		GetResponseNoRetry(gomock.Any(), directURL, probeRangeHeader).
		Return(httpResp(http.StatusNotFound, "", nil, nil), nil)
	mockHTTP.EXPECT().
		GetResponseNoRetry(gomock.Any(), "https://ipfs.io/ipfs/"+cid, probeRangeHeader).
		Return(nil, fmt.Errorf("blocked: %w", ssrf.ErrBlocked))

	checker := uri.NewURLChecker(mockHTTP, mockIO, defaultConfig())
	result := checker.Check(context.Background(), directURL)

	require.Equal(t, uri.HealthStatusBroken, result.Status)
	require.True(t, result.SSRFBlocked)
}

// Redirect-cap exhaustion from the SSRF HTTP client wraps ssrf.ErrBlocked and must be
// treated as a policy refusal.
func TestURLChecker_redirectLimitExhaustion_markedAsSSRFBlocked(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTP := mocks.NewMockHTTPClient(ctrl)
	mockIO := mocks.NewMockIO(ctrl)

	mockHTTP.EXPECT().
		GetResponseNoRetry(gomock.Any(), "https://example.com/loop", probeRangeHeader).
		Return(nil, fmt.Errorf("stopped after 5 redirects: %w", ssrf.ErrBlocked))

	checker := uri.NewURLChecker(mockHTTP, mockIO, defaultConfig())
	result := checker.Check(context.Background(), "https://example.com/loop")

	require.Equal(t, uri.HealthStatusBroken, result.Status)
	require.True(t, result.SSRFBlocked)
}

// TestGatewayFallbackPreservesDirectDiagnostics pins the diagnostics contract on the
// fallback-success path: the healthy result carries the direct probe's failure reason
// and content-type observations, so a caller whose URL promotion later fails can persist
// the canonical URL's real diagnosis instead of a reasonless broken row.
func TestGatewayFallbackPreservesDirectDiagnostics(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	cid := "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG"

	mockHTTP := mocks.NewMockHTTPClient(ctrl)
	mockIO := mocks.NewMockIO(ctrl)
	passthroughIO(mockIO)
	mockHTTP.EXPECT().
		GetResponseNoRetry(gomock.Any(), "https://gateway.pinata.cloud/ipfs/"+cid, probeRangeHeader).
		Return(httpResp(http.StatusOK, "text/html", kuboDirectoryListing(), nil), nil)
	mockHTTP.EXPECT().
		GetResponseNoRetry(gomock.Any(), "https://ipfs.io/ipfs/"+cid, probeRangeHeader).
		Return(httpResp(http.StatusOK, "image/png", minimalPNG(32, 32), nil), nil)

	checker := uri.NewURLChecker(mockHTTP, mockIO, defaultConfig())
	result := checker.Check(context.Background(), "https://gateway.pinata.cloud/ipfs/"+cid)

	assert.Equal(t, uri.HealthStatusHealthy, result.Status)
	require.NotNil(t, result.WorkingURL)
	assert.Equal(t, "https://ipfs.io/ipfs/"+cid, *result.WorkingURL)
	assert.Equal(t, uri.FailureDirectoryListing, result.FailureReason,
		"direct probe's diagnosis must survive fallback success")
	assert.Equal(t, "text/html", result.ObservedContentType)
	assert.Equal(t, "text/html", result.SniffedContentType)
	assert.Nil(t, result.Error, "a healthy result must not carry an error message")
	// The winning gateway's own validated observations travel separately: they belong
	// to the promoted row, not the failed direct URL.
	assert.Equal(t, "image/png", result.WorkingURLObserved,
		"the promoted URL's observed type comes from the fallback probe")
	assert.Equal(t, "image/png", result.WorkingURLSniffed,
		"the promoted URL's sniffed type comes from the fallback probe")
}

// TestProbeWindowClampedToClassifiableFloor pins the config floor: a probe_max_bytes
// below the sniff window would make every ranged probe an undersized prefix — the exact
// state the undersized-206 retry exists to avoid — so positive values below the floor
// are clamped up.
func TestProbeWindowClampedToClassifiableFloor(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTP := mocks.NewMockHTTPClient(ctrl)
	mockIO := mocks.NewMockIO(ctrl)
	passthroughIO(mockIO)
	cfg := defaultConfig()
	cfg.ProbeMaxBytes = 100 // below the floor; must be raised to 512
	mockHTTP.EXPECT().
		GetResponseNoRetry(gomock.Any(), "https://example.com/a.png",
			map[string]string{"Range": "bytes=0-511"}).
		Return(httpResp(http.StatusOK, "image/png", minimalPNG(32, 32), nil), nil)

	checker := uri.NewURLChecker(mockHTTP, mockIO, cfg)
	result := checker.Check(context.Background(), "https://example.com/a.png")
	assert.Equal(t, uri.HealthStatusHealthy, result.Status)
}

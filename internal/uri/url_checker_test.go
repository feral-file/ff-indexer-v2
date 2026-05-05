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

func TestURLChecker_Check(t *testing.T) {
	tests := []struct {
		name           string
		url            string
		setupMocks     func(*mocks.MockHTTPClient, *mocks.MockIO)
		config         *uri.Config
		expectedStatus uri.HealthStatus
		expectedURL    *string
		expectedError  *string
	}{
		{
			name: "valid HTTPS URL - HEAD succeeds",
			url:  "https://example.com/image.png",
			config: &uri.Config{
				IPFSGateways:    []string{"https://ipfs.io"},
				ArweaveGateways: []string{"https://arweave.net"},
				OnChFSGateways:  []string{"https://onchfs.fxhash2.xyz"},
			},
			setupMocks: func(mockHTTP *mocks.MockHTTPClient, mockIO *mocks.MockIO) {
				mockResp := &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewReader(nil)),
				}
				mockHTTP.
					EXPECT().
					HeadNoRetry(gomock.Any(), "https://example.com/image.png").
					Return(mockResp, nil)
			},
			expectedStatus: uri.HealthStatusHealthy,
			expectedURL:    nil,
			expectedError:  nil,
		},
		{
			name: "valid HTTP URL - HEAD succeeds",
			url:  "http://example.com/image.png",
			config: &uri.Config{
				IPFSGateways:    []string{"https://ipfs.io"},
				ArweaveGateways: []string{"https://arweave.net"},
				OnChFSGateways:  []string{"https://onchfs.fxhash2.xyz"},
			},
			setupMocks: func(mockHTTP *mocks.MockHTTPClient, mockIO *mocks.MockIO) {
				mockResp := &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewReader(nil)),
				}
				mockHTTP.
					EXPECT().
					HeadNoRetry(gomock.Any(), "http://example.com/image.png").
					Return(mockResp, nil)
			},
			expectedStatus: uri.HealthStatusHealthy,
			expectedURL:    nil,
			expectedError:  nil,
		},
		{
			name: "IPFS gateway URL - original URL works, no fallback needed",
			url:  "https://ipfs.io/ipfs/QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG",
			config: &uri.Config{
				IPFSGateways:    []string{"https://ipfs.io", "https://gateway.pinata.cloud"},
				ArweaveGateways: []string{"https://arweave.net"},
				OnChFSGateways:  []string{"https://onchfs.fxhash2.xyz"},
			},
			setupMocks: func(mockHTTP *mocks.MockHTTPClient, mockIO *mocks.MockIO) {
				// Original URL works via HTTP check
				mockResp := &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewReader(nil)),
				}
				mockHTTP.
					EXPECT().
					HeadNoRetry(gomock.Any(), "https://ipfs.io/ipfs/QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG").
					Return(mockResp, nil)
			},
			expectedStatus: uri.HealthStatusHealthy,
			expectedURL:    nil, // No WorkingURL since original URL works
			expectedError:  nil,
		},
		{
			name: "IPFS gateway URL - original fails, resolves to working gateway",
			url:  "https://ipfs.io/ipfs/QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG",
			config: &uri.Config{
				IPFSGateways:    []string{"https://ipfs.io", "https://gateway.pinata.cloud"},
				ArweaveGateways: []string{"https://arweave.net"},
				OnChFSGateways:  []string{"https://onchfs.fxhash2.xyz"},
			},
			setupMocks: func(mockHTTP *mocks.MockHTTPClient, mockIO *mocks.MockIO) {
				// Original URL fails via HEAD (first call)
				mockHTTP.
					EXPECT().
					HeadNoRetry(gomock.Any(), "https://ipfs.io/ipfs/QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG").
					Return(nil, assert.AnError).
					Times(1)

				// GET with Range also fails
				mockHTTP.
					EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://ipfs.io/ipfs/QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG", map[string]string{"Range": "bytes=0-1023"}).
					Return(nil, assert.AnError)

				// Now fallback to IPFS gateway resolution - first gateway succeeds (second call to same URL)
				mockResp1 := &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewReader(nil)),
				}
				mockHTTP.
					EXPECT().
					Head(gomock.Any(), "https://ipfs.io/ipfs/QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG").
					Return(mockResp1, nil).
					Times(1)

				// Second gateway may or may not be called (runs in parallel)
				mockResp2 := &http.Response{
					StatusCode: http.StatusNotFound,
					Body:       io.NopCloser(bytes.NewReader(nil)),
				}
				mockHTTP.
					EXPECT().
					Head(gomock.Any(), "https://gateway.pinata.cloud/ipfs/QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG").
					Return(mockResp2, nil).
					AnyTimes()
			},
			expectedStatus: uri.HealthStatusHealthy,
			expectedURL:    stringPtr("https://ipfs.io/ipfs/QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG"),
			expectedError:  nil,
		},
		{
			name: "Arweave gateway URL - original URL works, no fallback needed",
			url:  "https://arweave.net/sKqjvP7jFwM5HLZmyJQC_9l5hN7TVIYhT6MvSHDqwo0",
			config: &uri.Config{
				IPFSGateways:    []string{"https://ipfs.io"},
				ArweaveGateways: []string{"https://arweave.net", "https://ar-io.net"},
				OnChFSGateways:  []string{"https://onchfs.fxhash2.xyz"},
			},
			setupMocks: func(mockHTTP *mocks.MockHTTPClient, mockIO *mocks.MockIO) {
				// Original URL works via HTTP check
				mockResp := &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewReader(nil)),
				}
				mockHTTP.
					EXPECT().
					HeadNoRetry(gomock.Any(), "https://arweave.net/sKqjvP7jFwM5HLZmyJQC_9l5hN7TVIYhT6MvSHDqwo0").
					Return(mockResp, nil)
			},
			expectedStatus: uri.HealthStatusHealthy,
			expectedURL:    nil, // No WorkingURL since original URL works
			expectedError:  nil,
		},
		{
			name: "Arweave gateway URL - original fails, resolves to working gateway",
			url:  "https://arweave.net/sKqjvP7jFwM5HLZmyJQC_9l5hN7TVIYhT6MvSHDqwo0",
			config: &uri.Config{
				IPFSGateways:    []string{"https://ipfs.io"},
				ArweaveGateways: []string{"https://arweave.net", "https://ar-io.net"},
				OnChFSGateways:  []string{"https://onchfs.fxhash2.xyz"},
			},
			setupMocks: func(mockHTTP *mocks.MockHTTPClient, mockIO *mocks.MockIO) {
				// Original URL fails via HEAD (first call)
				mockHTTP.
					EXPECT().
					HeadNoRetry(gomock.Any(), "https://arweave.net/sKqjvP7jFwM5HLZmyJQC_9l5hN7TVIYhT6MvSHDqwo0").
					Return(nil, assert.AnError).
					Times(1)

				// GET with Range also fails
				mockHTTP.
					EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://arweave.net/sKqjvP7jFwM5HLZmyJQC_9l5hN7TVIYhT6MvSHDqwo0", map[string]string{"Range": "bytes=0-1023"}).
					Return(nil, assert.AnError)

				// Now fallback to Arweave gateway resolution - first gateway succeeds (second call to same URL)
				mockResp1 := &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewReader(nil)),
				}
				mockHTTP.
					EXPECT().
					Head(gomock.Any(), "https://arweave.net/sKqjvP7jFwM5HLZmyJQC_9l5hN7TVIYhT6MvSHDqwo0").
					Return(mockResp1, nil).
					Times(1)

				// Second gateway may or may not be called (runs in parallel)
				mockResp2 := &http.Response{
					StatusCode: http.StatusNotFound,
					Body:       io.NopCloser(bytes.NewReader(nil)),
				}
				mockHTTP.
					EXPECT().
					Head(gomock.Any(), "https://ar-io.net/sKqjvP7jFwM5HLZmyJQC_9l5hN7TVIYhT6MvSHDqwo0").
					Return(mockResp2, nil).
					AnyTimes()
			},
			expectedStatus: uri.HealthStatusHealthy,
			expectedURL:    stringPtr("https://arweave.net/sKqjvP7jFwM5HLZmyJQC_9l5hN7TVIYhT6MvSHDqwo0"),
			expectedError:  nil,
		},
		{
			name: "OnChFS URL - original URL works, no fallback needed",
			url:  "https://onchfs.fxhash2.xyz/a1b2c3d4e5f67890abcdef1234567890abcdef1234567890abcdef1234567890",
			config: &uri.Config{
				IPFSGateways:    []string{"https://ipfs.io"},
				ArweaveGateways: []string{"https://arweave.net"},
				OnChFSGateways:  []string{"https://onchfs.fxhash2.xyz"},
			},
			setupMocks: func(mockHTTP *mocks.MockHTTPClient, mockIO *mocks.MockIO) {
				// Original URL works via HTTP check
				mockResp := &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewReader(nil)),
				}
				mockHTTP.
					EXPECT().
					HeadNoRetry(gomock.Any(), "https://onchfs.fxhash2.xyz/a1b2c3d4e5f67890abcdef1234567890abcdef1234567890abcdef1234567890").
					Return(mockResp, nil)
			},
			expectedStatus: uri.HealthStatusHealthy,
			expectedURL:    nil,
			expectedError:  nil,
		},
		{
			name: "OnChFS URL - original fails, assumes healthy (fallback behavior)",
			url:  "https://onchfs.fxhash2.xyz/a1b2c3d4e5f67890abcdef1234567890abcdef1234567890abcdef1234567890",
			config: &uri.Config{
				IPFSGateways:    []string{"https://ipfs.io"},
				ArweaveGateways: []string{"https://arweave.net"},
				OnChFSGateways:  []string{"https://onchfs.fxhash2.xyz"},
			},
			setupMocks: func(mockHTTP *mocks.MockHTTPClient, mockIO *mocks.MockIO) {
				// Original URL fails via HEAD
				mockHTTP.
					EXPECT().
					HeadNoRetry(gomock.Any(), "https://onchfs.fxhash2.xyz/a1b2c3d4e5f67890abcdef1234567890abcdef1234567890abcdef1234567890").
					Return(nil, assert.AnError)

				// GET with Range also fails
				mockHTTP.
					EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://onchfs.fxhash2.xyz/a1b2c3d4e5f67890abcdef1234567890abcdef1234567890abcdef1234567890", map[string]string{"Range": "bytes=0-1023"}).
					Return(nil, assert.AnError)
			},
			expectedStatus: uri.HealthStatusHealthy,
			expectedURL:    nil,
			expectedError:  nil,
		},
		{
			name: "invalid URL - not HTTP/HTTPS",
			url:  "ipfs://QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG",
			config: &uri.Config{
				IPFSGateways:    []string{"https://ipfs.io"},
				ArweaveGateways: []string{"https://arweave.net"},
				OnChFSGateways:  []string{"https://onchfs.fxhash2.xyz"},
			},
			setupMocks:     func(mockHTTP *mocks.MockHTTPClient, mockIO *mocks.MockIO) {},
			expectedStatus: uri.HealthStatusBroken,
			expectedURL:    nil,
			expectedError:  stringPtr("only HTTP/HTTPS URLs are supported"),
		},
		{
			name: "invalid URL format",
			url:  "not-a-url",
			config: &uri.Config{
				IPFSGateways:    []string{"https://ipfs.io"},
				ArweaveGateways: []string{"https://arweave.net"},
				OnChFSGateways:  []string{"https://onchfs.fxhash2.xyz"},
			},
			setupMocks:     func(mockHTTP *mocks.MockHTTPClient, mockIO *mocks.MockIO) {},
			expectedStatus: uri.HealthStatusBroken,
			expectedURL:    nil,
			expectedError:  stringPtr("invalid URL format"),
		},
		{
			name: "IPFS gateway - no working gateway",
			url:  "https://ipfs.io/ipfs/QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG",
			config: &uri.Config{
				IPFSGateways:    []string{"https://ipfs.io", "https://gateway.pinata.cloud"},
				ArweaveGateways: []string{"https://arweave.net"},
				OnChFSGateways:  []string{"https://onchfs.fxhash2.xyz"},
			},
			setupMocks: func(mockHTTP *mocks.MockHTTPClient, mockIO *mocks.MockIO) {
				// Original URL fails via HEAD (first call)
				mockHTTP.
					EXPECT().
					HeadNoRetry(gomock.Any(), "https://ipfs.io/ipfs/QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG").
					Return(nil, assert.AnError).
					Times(1)

				// GET with Range also fails
				mockHTTP.
					EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://ipfs.io/ipfs/QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG", map[string]string{"Range": "bytes=0-1023"}).
					Return(nil, assert.AnError)

				// Now fallback to IPFS gateway resolution - all gateways fail (parallel calls)
				mockResp1 := &http.Response{
					StatusCode: http.StatusNotFound,
					Body:       io.NopCloser(bytes.NewReader(nil)),
				}
				mockHTTP.
					EXPECT().
					Head(gomock.Any(), "https://ipfs.io/ipfs/QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG").
					Return(mockResp1, nil).
					Times(1)

				mockResp2 := &http.Response{
					StatusCode: http.StatusNotFound,
					Body:       io.NopCloser(bytes.NewReader(nil)),
				}
				mockHTTP.
					EXPECT().
					Head(gomock.Any(), "https://gateway.pinata.cloud/ipfs/QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG").
					Return(mockResp2, nil)
			},
			expectedStatus: uri.HealthStatusBroken,
			expectedURL:    nil,
			expectedError:  stringPtr("no working IPFS gateway found for CID: QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG"),
		},
		{
			name: "Arweave gateway - no working gateway",
			url:  "https://arweave.net/sKqjvP7jFwM5HLZmyJQC_9l5hN7TVIYhT6MvSHDqwo0",
			config: &uri.Config{
				IPFSGateways:    []string{"https://ipfs.io"},
				ArweaveGateways: []string{"https://arweave.net"},
				OnChFSGateways:  []string{"https://onchfs.fxhash2.xyz"},
			},
			setupMocks: func(mockHTTP *mocks.MockHTTPClient, mockIO *mocks.MockIO) {
				// Original URL fails via HEAD (first call)
				mockHTTP.
					EXPECT().
					HeadNoRetry(gomock.Any(), "https://arweave.net/sKqjvP7jFwM5HLZmyJQC_9l5hN7TVIYhT6MvSHDqwo0").
					Return(nil, assert.AnError).
					Times(1)

				// GET with Range also fails
				mockHTTP.
					EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://arweave.net/sKqjvP7jFwM5HLZmyJQC_9l5hN7TVIYhT6MvSHDqwo0", map[string]string{"Range": "bytes=0-1023"}).
					Return(nil, assert.AnError)

				// Now fallback to Arweave gateway resolution - gateway fails (second call)
				mockResp := &http.Response{
					StatusCode: http.StatusNotFound,
					Body:       io.NopCloser(bytes.NewReader(nil)),
				}
				mockHTTP.
					EXPECT().
					Head(gomock.Any(), "https://arweave.net/sKqjvP7jFwM5HLZmyJQC_9l5hN7TVIYhT6MvSHDqwo0").
					Return(mockResp, nil).
					Times(1)
			},
			expectedStatus: uri.HealthStatusBroken,
			expectedURL:    nil,
			expectedError:  stringPtr("no working Arweave gateway found for TX: sKqjvP7jFwM5HLZmyJQC_9l5hN7TVIYhT6MvSHDqwo0"),
		},
		{
			name: "regular HTTPS URL - HEAD fails, GET with Range succeeds with 206",
			url:  "https://example.com/image.png",
			config: &uri.Config{
				IPFSGateways:    []string{"https://ipfs.io"},
				ArweaveGateways: []string{"https://arweave.net"},
				OnChFSGateways:  []string{"https://onchfs.fxhash2.xyz"},
			},
			setupMocks: func(mockHTTP *mocks.MockHTTPClient, mockIO *mocks.MockIO) {
				// HEAD request fails
				mockHTTP.
					EXPECT().
					HeadNoRetry(gomock.Any(), "https://example.com/image.png").
					Return(nil, assert.AnError)

				// GET with Range succeeds with 206 Partial Content
				mockResp := &http.Response{
					StatusCode: http.StatusPartialContent,
					Body:       io.NopCloser(bytes.NewReader([]byte("partial content"))),
				}
				mockHTTP.
					EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://example.com/image.png", map[string]string{"Range": "bytes=0-1023"}).
					Return(mockResp, nil)

				mockIO.
					EXPECT().
					Discard(mockResp.Body).
					Return(nil)
			},
			expectedStatus: uri.HealthStatusHealthy,
			expectedURL:    nil,
			expectedError:  nil,
		},
		{
			name: "regular HTTPS URL - HEAD fails, GET with Range returns 200 OK",
			url:  "https://example.com/image.png",
			config: &uri.Config{
				IPFSGateways:    []string{"https://ipfs.io"},
				ArweaveGateways: []string{"https://arweave.net"},
				OnChFSGateways:  []string{"https://onchfs.fxhash2.xyz"},
			},
			setupMocks: func(mockHTTP *mocks.MockHTTPClient, mockIO *mocks.MockIO) {
				// HEAD request fails
				mockHTTP.
					EXPECT().
					HeadNoRetry(gomock.Any(), "https://example.com/image.png").
					Return(nil, assert.AnError)

				// GET with Range returns 200 (server doesn't support range)
				mockResp := &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewReader([]byte("full content"))),
				}
				mockHTTP.
					EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://example.com/image.png", map[string]string{"Range": "bytes=0-1023"}).
					Return(mockResp, nil)

				mockIO.
					EXPECT().
					Discard(mockResp.Body).
					Return(nil)
			},
			expectedStatus: uri.HealthStatusHealthy,
			expectedURL:    nil,
			expectedError:  nil,
		},
		{
			name: "regular HTTPS URL - HEAD fails, GET with Range returns 416, fallback to GET without Range",
			url:  "https://example.com/image.png",
			config: &uri.Config{
				IPFSGateways:    []string{"https://ipfs.io"},
				ArweaveGateways: []string{"https://arweave.net"},
				OnChFSGateways:  []string{"https://onchfs.fxhash2.xyz"},
			},
			setupMocks: func(mockHTTP *mocks.MockHTTPClient, mockIO *mocks.MockIO) {
				// HEAD request fails
				mockHTTP.
					EXPECT().
					HeadNoRetry(gomock.Any(), "https://example.com/image.png").
					Return(nil, assert.AnError)

				// GET with Range returns 416 Range Not Satisfiable
				mockResp1 := &http.Response{
					StatusCode: http.StatusRequestedRangeNotSatisfiable,
					Body:       io.NopCloser(bytes.NewReader(nil)),
				}
				mockHTTP.
					EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://example.com/image.png", map[string]string{"Range": "bytes=0-1023"}).
					Return(mockResp1, nil)

				mockIO.
					EXPECT().
					Discard(mockResp1.Body).
					Return(nil)

				// Fallback to GET without Range succeeds
				mockResp2 := &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewReader([]byte("full content"))),
				}
				mockHTTP.
					EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://example.com/image.png", nil).
					Return(mockResp2, nil)

				mockIO.
					EXPECT().
					Discard(mockResp2.Body).
					Return(nil)
			},
			expectedStatus: uri.HealthStatusHealthy,
			expectedURL:    nil,
			expectedError:  nil,
		},
		{
			name: "regular HTTPS URL - HEAD fails, GET with Range returns 404, fallback to GET without Range",
			url:  "https://example.com/image.png",
			config: &uri.Config{
				IPFSGateways:    []string{"https://ipfs.io"},
				ArweaveGateways: []string{"https://arweave.net"},
				OnChFSGateways:  []string{"https://onchfs.fxhash2.xyz"},
			},
			setupMocks: func(mockHTTP *mocks.MockHTTPClient, mockIO *mocks.MockIO) {
				// HEAD request fails
				mockHTTP.
					EXPECT().
					HeadNoRetry(gomock.Any(), "https://example.com/image.png").
					Return(nil, assert.AnError)

				// GET with Range returns 404
				mockResp1 := &http.Response{
					StatusCode: http.StatusNotFound,
					Body:       io.NopCloser(bytes.NewReader(nil)),
				}
				mockHTTP.
					EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://example.com/image.png", map[string]string{"Range": "bytes=0-1023"}).
					Return(mockResp1, nil)

				mockIO.
					EXPECT().
					Discard(mockResp1.Body).
					Return(nil)

				// Fallback to GET without Range also fails
				mockResp2 := &http.Response{
					StatusCode: http.StatusNotFound,
					Body:       io.NopCloser(bytes.NewReader(nil)),
				}
				mockHTTP.
					EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://example.com/image.png", nil).
					Return(mockResp2, nil)

				mockIO.
					EXPECT().
					Discard(mockResp2.Body).
					Return(nil)
			},
			expectedStatus: uri.HealthStatusBroken,
			expectedURL:    nil,
			expectedError:  stringPtr("HTTP 404"),
		},
		{
			name: "regular HTTPS URL - transient error from GET with Range",
			url:  "https://example.com/image.png",
			config: &uri.Config{
				IPFSGateways:    []string{"https://ipfs.io"},
				ArweaveGateways: []string{"https://arweave.net"},
				OnChFSGateways:  []string{"https://onchfs.fxhash2.xyz"},
			},
			setupMocks: func(mockHTTP *mocks.MockHTTPClient, mockIO *mocks.MockIO) {
				// HEAD request fails
				mockHTTP.
					EXPECT().
					HeadNoRetry(gomock.Any(), "https://example.com/image.png").
					Return(nil, assert.AnError)

				// GET with Range returns retryable error
				mockHTTP.
					EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://example.com/image.png", map[string]string{"Range": "bytes=0-1023"}).
					Return(nil, &mockRetryableError{})
			},
			expectedStatus: uri.HealthStatusTransientError,
			expectedURL:    nil,
			expectedError:  stringPtr("temporary network error"),
		},
		{
			name: "regular HTTPS URL - non-retryable error from GET with Range",
			url:  "https://example.com/image.png",
			config: &uri.Config{
				IPFSGateways:    []string{"https://ipfs.io"},
				ArweaveGateways: []string{"https://arweave.net"},
				OnChFSGateways:  []string{"https://onchfs.fxhash2.xyz"},
			},
			setupMocks: func(mockHTTP *mocks.MockHTTPClient, mockIO *mocks.MockIO) {
				// HEAD request fails
				mockHTTP.
					EXPECT().
					HeadNoRetry(gomock.Any(), "https://example.com/image.png").
					Return(nil, assert.AnError)

				// GET with Range returns non-retryable error
				mockHTTP.
					EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://example.com/image.png", map[string]string{"Range": "bytes=0-1023"}).
					Return(nil, assert.AnError)
			},
			expectedStatus: uri.HealthStatusBroken,
			expectedURL:    nil,
			expectedError:  stringPtr("assert.AnError"),
		},
		{
			name: "regular HTTPS URL - GET with Range returns 429, transient error",
			url:  "https://example.com/image.png",
			config: &uri.Config{
				IPFSGateways:    []string{"https://ipfs.io"},
				ArweaveGateways: []string{"https://arweave.net"},
				OnChFSGateways:  []string{"https://onchfs.fxhash2.xyz"},
			},
			setupMocks: func(mockHTTP *mocks.MockHTTPClient, mockIO *mocks.MockIO) {
				mockHTTP.
					EXPECT().
					HeadNoRetry(gomock.Any(), "https://example.com/image.png").
					Return(nil, assert.AnError)

				mockResp := &http.Response{
					StatusCode: http.StatusTooManyRequests,
					Body:       io.NopCloser(bytes.NewReader(nil)),
				}
				mockHTTP.
					EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://example.com/image.png", map[string]string{"Range": "bytes=0-1023"}).
					Return(mockResp, nil)

				mockIO.
					EXPECT().
					Discard(mockResp.Body).
					Return(nil)
			},
			expectedStatus: uri.HealthStatusTransientError,
			expectedURL:    nil,
			expectedError:  stringPtr("rate limited (429)"),
		},
		{
			name: "regular HTTPS URL - GET without Range returns 429, transient error",
			url:  "https://example.com/image.png",
			config: &uri.Config{
				IPFSGateways:    []string{"https://ipfs.io"},
				ArweaveGateways: []string{"https://arweave.net"},
				OnChFSGateways:  []string{"https://onchfs.fxhash2.xyz"},
			},
			setupMocks: func(mockHTTP *mocks.MockHTTPClient, mockIO *mocks.MockIO) {
				mockHTTP.
					EXPECT().
					HeadNoRetry(gomock.Any(), "https://example.com/image.png").
					Return(nil, assert.AnError)

				// GET with Range returns 404, triggers fallback
				mockResp1 := &http.Response{
					StatusCode: http.StatusNotFound,
					Body:       io.NopCloser(bytes.NewReader(nil)),
				}
				mockHTTP.
					EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://example.com/image.png", map[string]string{"Range": "bytes=0-1023"}).
					Return(mockResp1, nil)

				mockIO.
					EXPECT().
					Discard(mockResp1.Body).
					Return(nil)

				// Fallback GET without Range returns 429
				mockResp2 := &http.Response{
					StatusCode: http.StatusTooManyRequests,
					Body:       io.NopCloser(bytes.NewReader(nil)),
				}
				mockHTTP.
					EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://example.com/image.png", nil).
					Return(mockResp2, nil)

				mockIO.
					EXPECT().
					Discard(mockResp2.Body).
					Return(nil)
			},
			expectedStatus: uri.HealthStatusTransientError,
			expectedURL:    nil,
			expectedError:  stringPtr("rate limited (429)"),
		},
		{
			name: "regular HTTPS URL - transient error from GET without Range",
			url:  "https://example.com/image.png",
			config: &uri.Config{
				IPFSGateways:    []string{"https://ipfs.io"},
				ArweaveGateways: []string{"https://arweave.net"},
				OnChFSGateways:  []string{"https://onchfs.fxhash2.xyz"},
			},
			setupMocks: func(mockHTTP *mocks.MockHTTPClient, mockIO *mocks.MockIO) {
				// HEAD request fails
				mockHTTP.
					EXPECT().
					HeadNoRetry(gomock.Any(), "https://example.com/image.png").
					Return(nil, assert.AnError)

				// GET with Range returns 500
				mockResp1 := &http.Response{
					StatusCode: http.StatusInternalServerError,
					Body:       io.NopCloser(bytes.NewReader(nil)),
				}
				mockHTTP.
					EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://example.com/image.png", map[string]string{"Range": "bytes=0-1023"}).
					Return(mockResp1, nil)

				mockIO.
					EXPECT().
					Discard(mockResp1.Body).
					Return(nil)

				// Fallback to GET without Range returns retryable error
				mockHTTP.
					EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://example.com/image.png", nil).
					Return(nil, &mockRetryableError{})
			},
			expectedStatus: uri.HealthStatusTransientError,
			expectedURL:    nil,
			expectedError:  stringPtr("temporary network error"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockHTTP := mocks.NewMockHTTPClient(ctrl)
			mockIO := mocks.NewMockIO(ctrl)
			if tt.setupMocks != nil {
				tt.setupMocks(mockHTTP, mockIO)
			}

			checker := uri.NewURLChecker(mockHTTP, mockIO, tt.config)
			result := checker.Check(context.Background(), tt.url)

			assert.Equal(t, tt.expectedStatus, result.Status)
			if tt.expectedURL != nil {
				assert.NotNil(t, result.WorkingURL)
				assert.Equal(t, *tt.expectedURL, *result.WorkingURL)
			} else {
				assert.Nil(t, result.WorkingURL)
			}
			if tt.expectedError != nil {
				assert.NotNil(t, result.Error)
				assert.Contains(t, *result.Error, *tt.expectedError)
			} else {
				assert.Nil(t, result.Error)
			}
		})
	}
}

func stringPtr(s string) *string {
	return &s
}

func TestURLChecker_SSrfBlocked_shortCircuitsOnChFS(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockHTTP := mocks.NewMockHTTPClient(ctrl)
	mockIO := mocks.NewMockIO(ctrl)

	hash := "a1b2c3d4e5f67890abcdef1234567890abcdef1234567890abcdef1234567890"
	testURL := "https://onchfs.fxhash2.xyz/" + hash

	mockHTTP.EXPECT().
		HeadNoRetry(gomock.Any(), testURL).
		Return(nil, fmt.Errorf("blocked: %w", ssrf.ErrBlocked))

	cfg := &uri.Config{
		IPFSGateways:    []string{"https://ipfs.io"},
		ArweaveGateways: []string{"https://arweave.net"},
		OnChFSGateways:  []string{"https://onchfs.fxhash2.xyz"},
	}

	checker := uri.NewURLChecker(mockHTTP, mockIO, cfg)
	result := checker.Check(context.Background(), testURL)

	require.Equal(t, uri.HealthStatusBroken, result.Status)
	require.True(t, result.SSRFBlocked)
	require.NotNil(t, result.Error)
}

func TestURLChecker_SSrfBlocked_regularHTTPS_onHEAD_skipsGET(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTP := mocks.NewMockHTTPClient(ctrl)
	mockIO := mocks.NewMockIO(ctrl)

	u := "https://example.com/asset.bin"
	mockHTTP.EXPECT().
		HeadNoRetry(gomock.Any(), u).
		Return(nil, fmt.Errorf("policy: %w", ssrf.ErrBlocked))

	cfg := &uri.Config{
		IPFSGateways:    []string{"https://ipfs.io"},
		ArweaveGateways: []string{"https://arweave.net"},
		OnChFSGateways:  []string{"https://onchfs.fxhash2.xyz"},
	}
	checker := uri.NewURLChecker(mockHTTP, mockIO, cfg)
	result := checker.Check(context.Background(), u)

	require.Equal(t, uri.HealthStatusBroken, result.Status)
	require.True(t, result.SSRFBlocked)
	require.NotNil(t, result.Error)
}

func TestURLChecker_SSrfBlocked_regularHTTPS_onGETWithRange(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTP := mocks.NewMockHTTPClient(ctrl)
	mockIO := mocks.NewMockIO(ctrl)

	u := "https://example.com/asset.bin"
	rangeHdr := map[string]string{"Range": "bytes=0-1023"}

	mockHTTP.EXPECT().
		HeadNoRetry(gomock.Any(), u).
		Return(nil, assert.AnError)

	mockHTTP.EXPECT().
		GetResponseNoRetry(gomock.Any(), u, rangeHdr).
		Return(nil, fmt.Errorf("blocked: %w", ssrf.ErrBlocked))

	cfg := &uri.Config{
		IPFSGateways:    []string{"https://ipfs.io"},
		ArweaveGateways: []string{"https://arweave.net"},
		OnChFSGateways:  []string{"https://onchfs.fxhash2.xyz"},
	}
	checker := uri.NewURLChecker(mockHTTP, mockIO, cfg)
	result := checker.Check(context.Background(), u)

	require.Equal(t, uri.HealthStatusBroken, result.Status)
	require.True(t, result.SSRFBlocked)
	require.NotNil(t, result.Error)
}

func TestURLChecker_SSrfBlocked_regularHTTPS_onGETWithoutRange(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTP := mocks.NewMockHTTPClient(ctrl)
	mockIO := mocks.NewMockIO(ctrl)

	u := "https://example.com/asset.bin"
	rangeHdr := map[string]string{"Range": "bytes=0-1023"}

	mockHTTP.EXPECT().
		HeadNoRetry(gomock.Any(), u).
		Return(nil, assert.AnError)

	mockResp416 := &http.Response{
		StatusCode: http.StatusRequestedRangeNotSatisfiable,
		Body:       io.NopCloser(bytes.NewReader(nil)),
	}
	mockHTTP.EXPECT().
		GetResponseNoRetry(gomock.Any(), u, rangeHdr).
		Return(mockResp416, nil)

	mockIO.EXPECT().Discard(mockResp416.Body).Return(nil)

	mockHTTP.EXPECT().
		GetResponseNoRetry(gomock.Any(), u, nil).
		Return(nil, fmt.Errorf("blocked: %w", ssrf.ErrBlocked))

	cfg := &uri.Config{
		IPFSGateways:    []string{"https://ipfs.io"},
		ArweaveGateways: []string{"https://arweave.net"},
		OnChFSGateways:  []string{"https://onchfs.fxhash2.xyz"},
	}
	checker := uri.NewURLChecker(mockHTTP, mockIO, cfg)
	result := checker.Check(context.Background(), u)

	require.Equal(t, uri.HealthStatusBroken, result.Status)
	require.True(t, result.SSRFBlocked)
	require.NotNil(t, result.Error)
}

func TestURLChecker_HEAD_retryableError_stillAttemptsGETWithRange(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTP := mocks.NewMockHTTPClient(ctrl)
	mockIO := mocks.NewMockIO(ctrl)

	u := "https://example.com/asset.bin"
	rangeHdr := map[string]string{"Range": "bytes=0-1023"}

	mockHTTP.EXPECT().
		HeadNoRetry(gomock.Any(), u).
		Return(nil, &mockRetryableError{})

	mockResp := &http.Response{
		StatusCode: http.StatusPartialContent,
		Body:       io.NopCloser(bytes.NewReader([]byte("x"))),
	}
	mockHTTP.EXPECT().
		GetResponseNoRetry(gomock.Any(), u, rangeHdr).
		Return(mockResp, nil)

	mockIO.EXPECT().Discard(mockResp.Body).Return(nil)

	cfg := &uri.Config{
		IPFSGateways:    []string{"https://ipfs.io"},
		ArweaveGateways: []string{"https://arweave.net"},
		OnChFSGateways:  []string{"https://onchfs.fxhash2.xyz"},
	}
	checker := uri.NewURLChecker(mockHTTP, mockIO, cfg)
	result := checker.Check(context.Background(), u)

	require.Equal(t, uri.HealthStatusHealthy, result.Status)
	require.False(t, result.SSRFBlocked)
}

func TestURLChecker_IPFS_fallback_retryableGatewayErrors_mapsToBrokenNotTransient(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTP := mocks.NewMockHTTPClient(ctrl)
	mockIO := mocks.NewMockIO(ctrl)

	cidURL := "https://ipfs.io/ipfs/QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG"
	rangeHdr := map[string]string{"Range": "bytes=0-1023"}

	mockHTTP.EXPECT().
		HeadNoRetry(gomock.Any(), cidURL).
		Return(nil, assert.AnError).
		Times(1)

	mockHTTP.EXPECT().
		GetResponseNoRetry(gomock.Any(), cidURL, rangeHdr).
		Return(nil, assert.AnError)

	retryErr := &mockRetryableError{}
	mockHTTP.EXPECT().
		Head(gomock.Any(), "https://ipfs.io/ipfs/QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG").
		Return(nil, retryErr).
		MinTimes(1)

	mockHTTP.EXPECT().
		Head(gomock.Any(), "https://gateway.pinata.cloud/ipfs/QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG").
		Return(nil, retryErr).
		MinTimes(1)

	cfg := &uri.Config{
		IPFSGateways:    []string{"https://ipfs.io", "https://gateway.pinata.cloud"},
		ArweaveGateways: []string{"https://arweave.net"},
		OnChFSGateways:  []string{"https://onchfs.fxhash2.xyz"},
	}
	checker := uri.NewURLChecker(mockHTTP, mockIO, cfg)
	result := checker.Check(context.Background(), cidURL)

	require.Equal(t, uri.HealthStatusBroken, result.Status)
	require.False(t, result.SSRFBlocked)
	require.NotNil(t, result.Error)
	require.Contains(t, *result.Error, "no working IPFS gateway found")
}

func TestURLChecker_IPFS_fallback_SSrfBlocked_onGatewayProbe(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTP := mocks.NewMockHTTPClient(ctrl)
	mockIO := mocks.NewMockIO(ctrl)

	cidURL := "https://ipfs.io/ipfs/QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG"
	rangeHdr := map[string]string{"Range": "bytes=0-1023"}

	mockHTTP.EXPECT().
		HeadNoRetry(gomock.Any(), cidURL).
		Return(nil, assert.AnError)

	mockHTTP.EXPECT().
		GetResponseNoRetry(gomock.Any(), cidURL, rangeHdr).
		Return(nil, assert.AnError)

	mockHTTP.EXPECT().
		Head(gomock.Any(), "https://ipfs.io/ipfs/QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG").
		Return(nil, fmt.Errorf("blocked: %w", ssrf.ErrBlocked)).
		Times(1)

	cfg := &uri.Config{
		IPFSGateways:    []string{"https://ipfs.io"},
		ArweaveGateways: []string{"https://arweave.net"},
		OnChFSGateways:  []string{"https://onchfs.fxhash2.xyz"},
	}
	checker := uri.NewURLChecker(mockHTTP, mockIO, cfg)
	result := checker.Check(context.Background(), cidURL)

	require.Equal(t, uri.HealthStatusBroken, result.Status)
	require.True(t, result.SSRFBlocked)
	require.NotNil(t, result.Error)
}

func TestURLChecker_Arweave_fallback_SSrfBlocked_onGatewayProbe(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTP := mocks.NewMockHTTPClient(ctrl)
	mockIO := mocks.NewMockIO(ctrl)

	txURL := "https://arweave.net/sKqjvP7jFwM5HLZmyJQC_9l5hN7TVIYhT6MvSHDqwo0"
	rangeHdr := map[string]string{"Range": "bytes=0-1023"}

	mockHTTP.EXPECT().
		HeadNoRetry(gomock.Any(), txURL).
		Return(nil, assert.AnError)

	mockHTTP.EXPECT().
		GetResponseNoRetry(gomock.Any(), txURL, rangeHdr).
		Return(nil, assert.AnError)

	mockHTTP.EXPECT().
		Head(gomock.Any(), "https://arweave.net/sKqjvP7jFwM5HLZmyJQC_9l5hN7TVIYhT6MvSHDqwo0").
		Return(nil, fmt.Errorf("blocked: %w", ssrf.ErrBlocked)).
		Times(1)

	cfg := &uri.Config{
		IPFSGateways:    []string{"https://ipfs.io"},
		ArweaveGateways: []string{"https://arweave.net"},
		OnChFSGateways:  []string{"https://onchfs.fxhash2.xyz"},
	}
	checker := uri.NewURLChecker(mockHTTP, mockIO, cfg)
	result := checker.Check(context.Background(), txURL)

	require.Equal(t, uri.HealthStatusBroken, result.Status)
	require.True(t, result.SSRFBlocked)
	require.NotNil(t, result.Error)
}

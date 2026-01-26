package uri_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"os"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/uri"
)

func TestMain(m *testing.M) {
	// Initialize logger for tests
	err := logger.Initialize(logger.Config{
		Debug: false,
	})
	if err != nil {
		panic(err)
	}

	code := m.Run()
	os.Exit(code)
}

func TestResolver_Resolve(t *testing.T) {
	tests := []struct {
		name        string
		uri         string
		setupMocks  func(*mocks.MockHTTPClient)
		config      *uri.Config
		expected    string
		expectedErr string // Error message to assert, empty means no error expected
	}{
		{
			name: "regular HTTP URL",
			uri:  "http://example.com/metadata.json",
			config: &uri.Config{
				IPFSGateways:    []string{"https://ipfs.io"},
				ArweaveGateways: []string{"https://arweave.net"},
			},
			expected:    "http://example.com/metadata.json",
			expectedErr: "",
		},
		{
			name: "regular HTTPS URL",
			uri:  "https://example.com/path/to/resource",
			config: &uri.Config{
				IPFSGateways:    []string{"https://ipfs.io"},
				ArweaveGateways: []string{"https://arweave.net"},
			},
			expected:    "https://example.com/path/to/resource",
			expectedErr: "",
		},
		{
			name: "IPFS URI",
			uri:  "ipfs://QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG",
			config: &uri.Config{
				IPFSGateways: []string{"https://ipfs.io", "https://gateway.pinata.cloud"},
			},
			setupMocks: func(mockHTTP *mocks.MockHTTPClient) {
				// First gateway fails
				mockResp1 := &http.Response{
					StatusCode: http.StatusNotFound,
					Body:       io.NopCloser(bytes.NewReader(nil)),
				}
				mockHTTP.
					EXPECT().
					HeadNoRetry(gomock.Any(), "https://ipfs.io/ipfs/QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG").
					Return(mockResp1, nil)

				// Second gateway succeeds
				mockResp2 := &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewReader(nil)),
				}
				mockHTTP.
					EXPECT().
					HeadNoRetry(gomock.Any(), "https://gateway.pinata.cloud/ipfs/QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG").
					Return(mockResp2, nil)
			},
			expected:    "https://gateway.pinata.cloud/ipfs/QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG",
			expectedErr: "",
		},
		{
			name: "IPFS gateway URL",
			uri:  "https://ipfs.io/ipfs/QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG",
			config: &uri.Config{
				IPFSGateways: []string{"https://ipfs.io", "https://gateway.pinata.cloud"},
			},
			setupMocks: func(mockHTTP *mocks.MockHTTPClient) {
			},
			expected:    "https://ipfs.io/ipfs/QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG",
			expectedErr: "",
		},
		{
			name: "Arweave URI",
			uri:  "ar://abc123",
			config: &uri.Config{
				ArweaveGateways: []string{"https://arweave.net", "https://ar-io.net"},
			},
			setupMocks: func(mockHTTP *mocks.MockHTTPClient) {
				// Mock both gateways since resolver tries them in parallel
				// First gateway succeeds - this ensures deterministic behavior
				mockResp1 := &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewReader(nil)),
				}
				mockHTTP.
					EXPECT().
					HeadNoRetry(gomock.Any(), "https://arweave.net/abc123").
					Return(mockResp1, nil)

				// Second gateway fails to ensure deterministic behavior
				// (only first gateway succeeds, so it will always be returned)
				// Use AnyTimes() since the resolver may return early when first gateway succeeds
				mockResp2 := &http.Response{
					StatusCode: http.StatusNotFound,
					Body:       io.NopCloser(bytes.NewReader(nil)),
				}
				mockHTTP.
					EXPECT().
					HeadNoRetry(gomock.Any(), "https://ar-io.net/abc123").
					Return(mockResp2, nil).
					AnyTimes()
			},
			expected:    "https://arweave.net/abc123",
			expectedErr: "",
		},
		{
			name: "IPFS URI - no gateways configured",
			uri:  "ipfs://QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG",
			config: &uri.Config{
				IPFSGateways: []string{},
			},
			expectedErr: "no IPFS gateways configured",
		},
		{
			name: "IPFS URI - no working gateway",
			uri:  "ipfs://QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG",
			config: &uri.Config{
				IPFSGateways: []string{"https://ipfs.io", "https://gateway.pinata.cloud"},
			},
			setupMocks: func(mockHTTP *mocks.MockHTTPClient) {
				// All gateways fail
				mockResp1 := &http.Response{
					StatusCode: http.StatusNotFound,
					Body:       io.NopCloser(bytes.NewReader(nil)),
				}
				mockHTTP.
					EXPECT().
					HeadNoRetry(gomock.Any(), "https://ipfs.io/ipfs/QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG").
					Return(mockResp1, nil)

				mockResp2 := &http.Response{
					StatusCode: http.StatusNotFound,
					Body:       io.NopCloser(bytes.NewReader(nil)),
				}
				mockHTTP.
					EXPECT().
					HeadNoRetry(gomock.Any(), "https://gateway.pinata.cloud/ipfs/QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG").
					Return(mockResp2, nil)
			},
			expectedErr: "no working IPFS gateway found for CID: QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG",
		},
		{
			name: "IPFS URI - network error",
			uri:  "ipfs://QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG",
			config: &uri.Config{
				IPFSGateways: []string{"https://ipfs.io"},
			},
			setupMocks: func(mockHTTP *mocks.MockHTTPClient) {
				mockHTTP.
					EXPECT().
					HeadNoRetry(gomock.Any(), gomock.Any()).
					Return(nil, assert.AnError)
			},
			expectedErr: "no working IPFS gateway found for CID: QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG",
		},
		{
			name: "Arweave URI - no gateways configured",
			uri:  "ar://abc123",
			config: &uri.Config{
				ArweaveGateways: []string{},
			},
			expectedErr: "no Arweave gateways configured",
		},
		{
			name: "Arweave URI - no working gateway",
			uri:  "ar://abc123",
			config: &uri.Config{
				ArweaveGateways: []string{"https://arweave.net"},
			},
			setupMocks: func(mockHTTP *mocks.MockHTTPClient) {
				mockResp := &http.Response{
					StatusCode: http.StatusNotFound,
					Body:       io.NopCloser(bytes.NewReader(nil)),
				}
				mockHTTP.
					EXPECT().
					HeadNoRetry(gomock.Any(), "https://arweave.net/abc123").
					Return(mockResp, nil)
			},
			expectedErr: "no working Arweave gateway found for TX: abc123",
		},
		{
			name: "Arweave URI - network error",
			uri:  "ar://abc123",
			config: &uri.Config{
				ArweaveGateways: []string{"https://arweave.net"},
			},
			setupMocks: func(mockHTTP *mocks.MockHTTPClient) {
				mockHTTP.
					EXPECT().
					HeadNoRetry(gomock.Any(), gomock.Any()).
					Return(nil, assert.AnError)
			},
			expectedErr: "no working Arweave gateway found for TX: abc123",
		},
		{
			name: "OnChFS URI",
			uri:  "onchfs://QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG",
			config: &uri.Config{
				OnChFSGateways: []string{"https://onchfs.fxhash2.xyz", "https://onchfs-backup.example.com"},
			},
			setupMocks: func(mockHTTP *mocks.MockHTTPClient) {
				// Mock both gateways since resolver tries them in parallel
				// First gateway succeeds - this ensures deterministic behavior
				mockResp1 := &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewReader(nil)),
				}
				mockHTTP.
					EXPECT().
					HeadNoRetry(gomock.Any(), "https://onchfs.fxhash2.xyz/QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG").
					Return(mockResp1, nil)

				// Second gateway fails to ensure deterministic behavior
				// Use AnyTimes() since the resolver may return early when first gateway succeeds
				mockResp2 := &http.Response{
					StatusCode: http.StatusNotFound,
					Body:       io.NopCloser(bytes.NewReader(nil)),
				}
				mockHTTP.
					EXPECT().
					HeadNoRetry(gomock.Any(), "https://onchfs-backup.example.com/QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG").
					Return(mockResp2, nil).
					AnyTimes()
			},
			expected:    "https://onchfs.fxhash2.xyz/QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG",
			expectedErr: "",
		},
		{
			name: "OnChFS URI - no gateways configured",
			uri:  "onchfs://QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG",
			config: &uri.Config{
				OnChFSGateways: []string{},
			},
			expectedErr: "no OnChFS gateways configured",
		},
		{
			name: "OnChFS URI - no working gateway",
			uri:  "onchfs://QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG",
			config: &uri.Config{
				OnChFSGateways: []string{"https://onchfs.fxhash2.xyz"},
			},
			setupMocks: func(mockHTTP *mocks.MockHTTPClient) {
				mockResp := &http.Response{
					StatusCode: http.StatusNotFound,
					Body:       io.NopCloser(bytes.NewReader(nil)),
				}
				mockHTTP.
					EXPECT().
					HeadNoRetry(gomock.Any(), "https://onchfs.fxhash2.xyz/QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG").
					Return(mockResp, nil)
			},
			expectedErr: "no working OnChFS gateway found for hash: QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG",
		},
		{
			name: "OnChFS URI - network error",
			uri:  "onchfs://QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG",
			config: &uri.Config{
				OnChFSGateways: []string{"https://onchfs.fxhash2.xyz"},
			},
			setupMocks: func(mockHTTP *mocks.MockHTTPClient) {
				mockHTTP.
					EXPECT().
					HeadNoRetry(gomock.Any(), gomock.Any()).
					Return(nil, assert.AnError)
			},
			expectedErr: "no working OnChFS gateway found for hash: QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockHTTP := mocks.NewMockHTTPClient(ctrl)
			if tt.setupMocks != nil {
				tt.setupMocks(mockHTTP)
			}

			resolver := uri.NewResolver(mockHTTP, tt.config)
			result, err := resolver.Resolve(context.Background(), tt.uri)

			if tt.expectedErr != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
				assert.Empty(t, result)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

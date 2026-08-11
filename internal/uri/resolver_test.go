package uri_test

import (
	"context"
	"net/http"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

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

// Gateway probing is parallel and returns on first success, so the losing gateway's
// request may never happen — all gateway expectations use AnyTimes() (the strict-mock
// race fixed in PR #103 for the old HEAD-based resolver applies identically here).
func TestResolver_Resolve(t *testing.T) {
	cid := "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG"
	txID := "sKqjvP7jFwM5HLZmyJQC_9l5hN7TVIYhT6MvSHDqwo0"
	onchHash := "a1b2c3d4e5f67890abcdef1234567890abcdef1234567890abcdef1234567890"

	tests := []struct {
		name        string
		uri         string
		setupMocks  func(*mocks.MockHTTPClient, *mocks.MockIO)
		config      *uri.Config
		expected    string
		expectedErr string // Error message substring to assert, empty means no error expected
	}{
		{
			name:     "regular HTTP URL passes through without probing",
			uri:      "http://example.com/metadata.json",
			config:   defaultConfig(),
			expected: "http://example.com/metadata.json",
		},
		{
			name:     "regular HTTPS URL passes through without probing",
			uri:      "https://example.com/path/to/resource",
			config:   defaultConfig(),
			expected: "https://example.com/path/to/resource",
		},
		{
			name: "IPFS URI resolves to gateway serving valid content",
			uri:  "ipfs://" + cid,
			config: &uri.Config{
				IPFSGateways: []string{"https://ipfs.io", "https://gateway.pinata.cloud"},
			},
			setupMocks: func(m *mocks.MockHTTPClient, mio *mocks.MockIO) {
				passthroughIO(mio)
				// First gateway 404s so only the second can win (deterministic outcome).
				m.EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://ipfs.io/ipfs/"+cid, probeRangeHeader).
					Return(httpResp(http.StatusNotFound, "", nil, nil), nil).
					AnyTimes()
				m.EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://gateway.pinata.cloud/ipfs/"+cid, probeRangeHeader).
					Return(httpResp(http.StatusOK, "image/png", minimalPNG(32, 32), nil), nil).
					AnyTimes()
			},
			expected: "https://gateway.pinata.cloud/ipfs/" + cid,
		},
		{
			name: "IPFS URI: gateway serving a directory listing is not selected (feral-file#3482)",
			uri:  "ipfs://" + cid,
			config: &uri.Config{
				IPFSGateways: []string{"https://ipfs.io", "https://gateway.pinata.cloud"},
			},
			setupMocks: func(m *mocks.MockHTTPClient, mio *mocks.MockIO) {
				passthroughIO(mio)
				// First gateway 200s a directory listing — previously this won on bare HEAD.
				m.EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://ipfs.io/ipfs/"+cid, probeRangeHeader).
					Return(httpResp(http.StatusOK, "text/html", kuboDirectoryListing(), nil), nil).
					AnyTimes()
				m.EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://gateway.pinata.cloud/ipfs/"+cid, probeRangeHeader).
					Return(httpResp(http.StatusOK, "image/png", minimalPNG(32, 32), nil), nil).
					AnyTimes()
			},
			expected: "https://gateway.pinata.cloud/ipfs/" + cid,
		},
		{
			name: "IPFS URI: no gateway serves valid content",
			uri:  "ipfs://" + cid,
			config: &uri.Config{
				IPFSGateways: []string{"https://ipfs.io"},
			},
			setupMocks: func(m *mocks.MockHTTPClient, mio *mocks.MockIO) {
				passthroughIO(mio)
				m.EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://ipfs.io/ipfs/"+cid, probeRangeHeader).
					Return(httpResp(http.StatusOK, "text/html", kuboDirectoryListing(), nil), nil).
					AnyTimes()
			},
			expectedErr: "no working IPFS gateway found",
		},
		{
			name: "Arweave URI resolves",
			uri:  "ar://" + txID,
			config: &uri.Config{
				ArweaveGateways: []string{"https://arweave.net"},
			},
			setupMocks: func(m *mocks.MockHTTPClient, mio *mocks.MockIO) {
				passthroughIO(mio)
				m.EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://arweave.net/"+txID, probeRangeHeader).
					Return(httpResp(http.StatusOK, "image/webp", minimalWebP(), nil), nil).
					AnyTimes()
			},
			expected: "https://arweave.net/" + txID,
		},
		{
			name: "Arweave URI: no working gateway",
			uri:  "ar://" + txID,
			config: &uri.Config{
				ArweaveGateways: []string{"https://arweave.net"},
			},
			setupMocks: func(m *mocks.MockHTTPClient, _ *mocks.MockIO) {
				m.EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://arweave.net/"+txID, probeRangeHeader).
					Return(httpResp(http.StatusNotFound, "", nil, nil), nil).
					AnyTimes()
			},
			expectedErr: "no working Arweave gateway found",
		},
		{
			name: "OnChFS URI resolves",
			uri:  "onchfs://" + onchHash,
			config: &uri.Config{
				OnChFSGateways: []string{"https://onchfs.fxhash2.xyz"},
			},
			setupMocks: func(m *mocks.MockHTTPClient, mio *mocks.MockIO) {
				passthroughIO(mio)
				m.EXPECT().
					GetResponseNoRetry(gomock.Any(), "https://onchfs.fxhash2.xyz/"+onchHash, probeRangeHeader).
					Return(httpResp(http.StatusOK, "image/png", minimalPNG(16, 16), nil), nil).
					AnyTimes()
			},
			expected: "https://onchfs.fxhash2.xyz/" + onchHash,
		},
		{
			name:        "IPFS URI with no gateways configured",
			uri:         "ipfs://" + cid,
			config:      &uri.Config{},
			expectedErr: "no IPFS gateways configured",
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

			resolver := uri.NewResolver(mockHTTP, mockIO, tt.config)
			result, err := resolver.Resolve(context.Background(), tt.uri)

			if tt.expectedErr != "" {
				assert.ErrorContains(t, err, tt.expectedErr)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

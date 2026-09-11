package uri_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/security/ssrf"
	"github.com/feral-file/ff-indexer-v2/internal/uri"
)

// Unsupported retired addresses cannot migrate to a supported playback gateway.
// Even valid image bytes from the native probe must not keep those URLs healthy.
func TestURLChecker_RejectsHealthyUnsupportedRetiredGatewayURLs(t *testing.T) {
	for _, source := range []string{
		"https://ipfs.io/ipns/example.org/art.html?seed=1#view",
		"https://example.ipns.dweb.link/art.html",
		"https://example.IPNS.Dweb.Link.:8443/art.html",
		"https://inbrowser.link/ipfs/not-a-cid/art.html",
		"https://not-a-cid.ipfs.ipfs.io/art.html",
		"https://ipfs.io/",
		"https://user@ipfs.io/ipns/example.org/art.html",
		"https://user@ipfs.io/ipfs/QmVJn8AG9x22BrbUaUj2CAQFtKMozSHyLvgV2X6X8dmtPw",
	} {
		t.Run(source, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			client := mocks.NewMockHTTPClient(ctrl)
			mio := mocks.NewMockIO(ctrl)
			passthroughIO(mio)
			client.EXPECT().GetResponseNoRetry(gomock.Any(), source, probeRangeHeader).
				Return(httpResp(http.StatusOK, "image/png", minimalPNG(32, 32), nil), nil)
			checker := uri.NewURLChecker(client, mio, &uri.Config{
				IPFSGateways: []string{"https://ipfs.filebase.io"},
			})

			result := checker.Check(context.Background(), source)
			require.Equal(t, uri.HealthStatusBroken, result.Status)
			require.Equal(t, uri.FailureGatewayRetired, result.FailureReason)
			require.NotNil(t, result.Error)
			require.Contains(t, *result.Error, "unsupported gateway address")
			require.Nil(t, result.WorkingURL)
			require.False(t, result.SSRFBlocked)
			require.Equal(t, "image/png", result.ObservedContentType)
			require.Equal(t, "image/png", result.SniffedContentType)
		})
	}
}

// Retirement must not turn transient failures into persisted broken verdicts or
// overwrite the specific cause of an already-failed probe, including SSRF/DNS.
func TestURLChecker_UnsupportedRetiredGatewayPreservesProbeFailures(t *testing.T) {
	for _, tc := range []struct {
		name        string
		statusCode  int
		probeError  error
		status      uri.HealthStatus
		reason      uri.FailureReason
		ssrfBlocked bool
	}{
		{"throttled", http.StatusTooManyRequests, nil, uri.HealthStatusTransientError, uri.FailureHTTPStatus, false},
		{"missing", http.StatusNotFound, nil, uri.HealthStatusBroken, uri.FailureHTTPStatus, false},
		{"blocked", 0, ssrf.ErrBlocked, uri.HealthStatusBroken, uri.FailureSSRF, true},
		{"dns", 0, ssrf.ErrResolutionFailed, uri.HealthStatusBroken, uri.FailureDNS, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			client := mocks.NewMockHTTPClient(ctrl)
			mio := mocks.NewMockIO(ctrl)
			passthroughIO(mio)
			const source = "https://ipfs.io/ipns/example.org/art.html"
			var response *http.Response
			if tc.probeError == nil {
				response = httpResp(tc.statusCode, "text/plain", []byte("unavailable"), nil)
			}
			client.EXPECT().GetResponseNoRetry(gomock.Any(), source, probeRangeHeader).
				Return(response, tc.probeError)
			checker := uri.NewURLChecker(client, mio, &uri.Config{
				IPFSGateways: []string{"https://ipfs.filebase.io"},
			})

			result := checker.Check(context.Background(), source)
			require.Equal(t, tc.status, result.Status)
			require.Equal(t, tc.reason, result.FailureReason)
			require.Equal(t, tc.ssrfBlocked, result.SSRFBlocked)
			require.NotNil(t, result.Error)
			require.Nil(t, result.WorkingURL)
		})
	}
}

// The retirement policy is limited to the named gateways; ordinary IPNS URLs
// and hostname lookalikes retain the direct content-validated verdict.
func TestURLChecker_NonRetiredIPNSURLRemainsHealthy(t *testing.T) {
	for _, source := range []string{
		"https://gateway.example.org/ipns/example.org/art.html",
		"https://ipfs.io.example.org/ipns/example.org/art.html",
		"https://ipfs.io@example.org/ipns/example.org/art.html",
	} {
		t.Run(source, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			client := mocks.NewMockHTTPClient(ctrl)
			mio := mocks.NewMockIO(ctrl)
			passthroughIO(mio)
			client.EXPECT().GetResponseNoRetry(gomock.Any(), source, probeRangeHeader).
				Return(httpResp(http.StatusOK, "image/png", minimalPNG(32, 32), nil), nil)
			checker := uri.NewURLChecker(client, mio, &uri.Config{})

			result := checker.Check(context.Background(), source)
			require.Equal(t, uri.HealthStatusHealthy, result.Status)
			require.Empty(t, result.FailureReason)
			require.Nil(t, result.Error)
			require.Nil(t, result.WorkingURL)
		})
	}
}

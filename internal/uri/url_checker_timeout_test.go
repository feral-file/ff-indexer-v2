package uri_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/uri"
)

// clientTimeoutErr reproduces what http.Client returns when its Timeout fires: a
// *url.Error wrapping context.DeadlineExceeded.
func clientTimeoutErr(u string) error {
	return &url.Error{Op: "Get", URL: u, Err: fmt.Errorf("%w (Client.Timeout exceeded while awaiting headers)", context.DeadlineExceeded)}
}

// A probe that runs out of time says nothing about the URL. Seen in production: filebase
// answering a cold CID slowly under full-speed sweeping was persisted broken/transport and
// flipped viewable tokens to unviewable while the same URL served moments later.
func TestURLChecker_ProbeTimeoutIsTransient(t *testing.T) {
	const source = "https://example.com/art.png"
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"http.Client timeout", clientTimeoutErr(source)},
		{"bare deadline exceeded", fmt.Errorf("Get %q: %w", source, context.DeadlineExceeded)},
		{"context canceled", fmt.Errorf("Get %q: %w", source, context.Canceled)},
		{"net.Error timeout", &url.Error{Op: "Get", URL: source, Err: &mockRetryableError{}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			client := mocks.NewMockHTTPClient(ctrl)
			mio := mocks.NewMockIO(ctrl)
			passthroughIO(mio)
			client.EXPECT().GetResponseNoRetry(gomock.Any(), source, probeRangeHeader).Return(nil, tc.err)
			checker := uri.NewURLChecker(client, mio, &uri.Config{})

			result := checker.Check(context.Background(), source)
			require.Equal(t, uri.HealthStatusTransientError, result.Status)
			require.Equal(t, uri.FailureTransport, result.FailureReason)
			require.Nil(t, result.WorkingURL)
		})
	}
}

// Failures that are answers, not a missing answer, stay broken.
func TestURLChecker_NonTimeoutTransportFailureStaysBroken(t *testing.T) {
	const source = "https://example.com/art.png"
	ctrl := gomock.NewController(t)
	client := mocks.NewMockHTTPClient(ctrl)
	mio := mocks.NewMockIO(ctrl)
	passthroughIO(mio)
	client.EXPECT().GetResponseNoRetry(gomock.Any(), source, probeRangeHeader).
		Return(nil, &url.Error{Op: "Get", URL: source, Err: errors.New("tls: failed to verify certificate: x509: certificate has expired")})
	checker := uri.NewURLChecker(client, mio, &uri.Config{})

	result := checker.Check(context.Background(), source)
	require.Equal(t, uri.HealthStatusBroken, result.Status)
	require.Equal(t, uri.FailureTransport, result.FailureReason)
}

// A timed-out IPFS gateway URL still falls back to a pool gateway that answers.
func TestURLChecker_TimedOutGatewayPromotesReplacement(t *testing.T) {
	const cid = "QmVJn8AG9x22BrbUaUj2CAQFtKMozSHyLvgV2X6X8dmtPw"
	const source = "https://ipfs.filebase.io/ipfs/" + cid + "/1754.png"
	ctrl := gomock.NewController(t)
	client := mocks.NewMockHTTPClient(ctrl)
	mio := mocks.NewMockIO(ctrl)
	passthroughIO(mio)
	client.EXPECT().GetResponseNoRetry(gomock.Any(), source, probeRangeHeader).Return(nil, clientTimeoutErr(source))
	client.EXPECT().GetResponseNoRetry(gomock.Any(), "https://ipfs.raribleuserdata.com/ipfs/"+cid+"/1754.png", probeRangeHeader).
		Return(httpResp(http.StatusOK, "image/png", minimalPNG(32, 32), nil), nil)
	checker := uri.NewURLChecker(client, mio, &uri.Config{IPFSGateways: []string{"https://ipfs.raribleuserdata.com"}})

	result := checker.Check(context.Background(), source)
	require.Equal(t, uri.HealthStatusHealthy, result.Status)
	require.NotNil(t, result.WorkingURL)
	require.Equal(t, "https://ipfs.raribleuserdata.com/ipfs/"+cid+"/1754.png", *result.WorkingURL)
}

// With no replacement, a timed-out gateway URL stays transient (deferred by the sweep),
// never broken.
func TestURLChecker_TimedOutGatewayWithoutReplacementStaysTransient(t *testing.T) {
	const cid = "QmVJn8AG9x22BrbUaUj2CAQFtKMozSHyLvgV2X6X8dmtPw"
	const source = "https://ipfs.filebase.io/ipfs/" + cid
	ctrl := gomock.NewController(t)
	client := mocks.NewMockHTTPClient(ctrl)
	mio := mocks.NewMockIO(ctrl)
	passthroughIO(mio)
	client.EXPECT().GetResponseNoRetry(gomock.Any(), source, probeRangeHeader).Return(nil, clientTimeoutErr(source))
	pool := "https://ipfs.raribleuserdata.com/ipfs/" + cid
	client.EXPECT().GetResponseNoRetry(gomock.Any(), pool, probeRangeHeader).Return(nil, clientTimeoutErr(pool))
	checker := uri.NewURLChecker(client, mio, &uri.Config{IPFSGateways: []string{"https://ipfs.raribleuserdata.com"}})

	result := checker.Check(context.Background(), source)
	require.Equal(t, uri.HealthStatusTransientError, result.Status)
	require.Nil(t, result.WorkingURL)
}

// A retired gateway that times out with no replacement is gateway_retired (#160), not a
// deferred transient: retirement still decides for retired hosts.
func TestURLChecker_TimedOutRetiredGatewayIsRetired(t *testing.T) {
	const cid = "QmVJn8AG9x22BrbUaUj2CAQFtKMozSHyLvgV2X6X8dmtPw"
	const source = "https://ipfs.io/ipfs/" + cid
	ctrl := gomock.NewController(t)
	client := mocks.NewMockHTTPClient(ctrl)
	mio := mocks.NewMockIO(ctrl)
	passthroughIO(mio)
	client.EXPECT().GetResponseNoRetry(gomock.Any(), source, probeRangeHeader).Return(nil, clientTimeoutErr(source))
	pool := "https://ipfs.raribleuserdata.com/ipfs/" + cid
	client.EXPECT().GetResponseNoRetry(gomock.Any(), pool, probeRangeHeader).Return(nil, clientTimeoutErr(pool))
	checker := uri.NewURLChecker(client, mio, &uri.Config{IPFSGateways: []string{"https://ipfs.raribleuserdata.com"}})

	result := checker.Check(context.Background(), source)
	require.Equal(t, uri.HealthStatusBroken, result.Status)
	require.Equal(t, uri.FailureGatewayRetired, result.FailureReason)
}

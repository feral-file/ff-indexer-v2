package uri_test

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/security/ssrf"
	"github.com/feral-file/ff-indexer-v2/internal/uri"
)

// A successful non-browser fetch no longer proves these hosts are suitable for
// playback: they redirect browser navigations to the service-worker viewer.
func TestURLChecker_MigratesBrowserGatewayEvenWhenDirectFetchSucceeds(t *testing.T) {
	const cid = "bafybeidhq4d52l3kozhe5upfl2bpu5smjcvrg7g3unf2hpzjvjfbb3wp3y"
	const ref = cid + "/frame%20one.gif?x=1&x=2#frame"
	for _, source := range []string{
		"https://ipfs.io/ipfs/" + ref,
		"https://dweb.link/ipfs/" + ref,
		"https://" + cid + ".ipfs.inbrowser.link/frame%20one.gif?x=1&x=2#frame",
		"https://" + cid + ".IPFS.InBrowser.Link/frame%20one.gif?x=1&x=2#frame",
		"https://" + cid + ".ipfs.inbrowser.link./frame%20one.gif?x=1&x=2#frame",
		"https://" + strings.ToUpper(cid) + ".IPFS.InBrowser.Link.:8443/frame%20one.gif?x=1&x=2#frame",
	} {
		t.Run(source, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			client := mocks.NewMockHTTPClient(ctrl)
			mio := mocks.NewMockIO(ctrl)
			passthroughIO(mio)
			client.EXPECT().GetResponseNoRetry(gomock.Any(), source, probeRangeHeader).
				Return(httpResp(http.StatusOK, "image/png", minimalPNG(32, 32), nil), nil)
			const target = "https://ipfs.feralfile.com/ipfs/" + ref
			client.EXPECT().GetResponseNoRetry(gomock.Any(), target, probeRangeHeader).
				Return(httpResp(http.StatusOK, "image/png", minimalPNG(32, 32), nil), nil)
			checker := uri.NewURLChecker(client, mio, &uri.Config{
				IPFSGateways: []string{"https://ipfs.io", "https://ipfs.feralfile.com"},
			})

			result := checker.Check(context.Background(), source)
			require.Equal(t, uri.HealthStatusHealthy, result.Status)
			require.NotNil(t, result.WorkingURL)
			require.Equal(t, target, *result.WorkingURL)
			require.Equal(t, "gateway_retired", result.FailureReason.String())
			require.Equal(t, "image/png", result.WorkingURLSniffed)
		})
	}
}

// Absolute DNS names must migrate through the resolver with the exact artwork
// suffix, including empty query/fragment delimiters and an explicit source port.
func TestResolver_MigratesAbsoluteCIDSubdomainsPreservingReference(t *testing.T) {
	const cid = "bafybeidhq4d52l3kozhe5upfl2bpu5smjcvrg7g3unf2hpzjvjfbb3wp3y"
	for _, host := range []string{cid + ".ipfs.inbrowser.link.", strings.ToUpper(cid) + ".IPFS.InBrowser.Link.:8443"} {
		for _, suffix := range []string{"", "/frame%20one.gif?x=1&x=2#frame", "?", "#"} {
			t.Run(host+suffix, func(t *testing.T) {
				ctrl := gomock.NewController(t)
				client := mocks.NewMockHTTPClient(ctrl)
				mio := mocks.NewMockIO(ctrl)
				passthroughIO(mio)
				target := "https://ipfs.filebase.io/ipfs/" + cid + suffix
				client.EXPECT().GetResponseNoRetry(gomock.Any(), target, probeRangeHeader).
					Return(httpResp(http.StatusOK, "image/png", minimalPNG(32, 32), nil), nil)
				resolver := uri.NewResolver(client, mio, &uri.Config{
					IPFSGateways: []string{"https://ipfs.io", "https://ipfs.filebase.io"},
				})

				resolved, err := resolver.Resolve(context.Background(), "https://"+host+suffix)
				require.NoError(t, err)
				require.Equal(t, target, resolved)
			})
		}
	}
}

func TestURLChecker_HealthyRetiredGatewaySurvivesBlockedReplacements(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := mocks.NewMockHTTPClient(ctrl)
	mio := mocks.NewMockIO(ctrl)
	passthroughIO(mio)
	const cid = "QmVJn8AG9x22BrbUaUj2CAQFtKMozSHyLvgV2X6X8dmtPw"
	const source = "https://ipfs.io/ipfs/" + cid
	client.EXPECT().GetResponseNoRetry(gomock.Any(), source, probeRangeHeader).
		Return(httpResp(http.StatusOK, "image/png", minimalPNG(32, 32), nil), nil)
	client.EXPECT().GetResponseNoRetry(gomock.Any(), "https://ipfs.feralfile.com/ipfs/"+cid, probeRangeHeader).
		Return(nil, ssrf.ErrBlocked)
	checker := uri.NewURLChecker(client, mio, &uri.Config{
		IPFSGateways: []string{"https://ipfs.feralfile.com"},
	})
	result := checker.Check(context.Background(), source)
	require.Equal(t, uri.HealthStatusHealthy, result.Status)
	require.Nil(t, result.WorkingURL)
	require.Nil(t, result.Error)
	require.Empty(t, result.FailureReason)
	require.False(t, result.SSRFBlocked)
	require.Equal(t, "image/png", result.ObservedContentType)
	require.Equal(t, "image/png", result.SniffedContentType)
}

// Migration is conditional on a verified replacement. A temporary alternate
// outage must not hide a source that still serves valid media to this probe.
func TestURLChecker_RetiredGatewayWithoutReplacementKeepsDirectVerdict(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := mocks.NewMockHTTPClient(ctrl)
	mio := mocks.NewMockIO(ctrl)
	passthroughIO(mio)
	const cid = "QmVJn8AG9x22BrbUaUj2CAQFtKMozSHyLvgV2X6X8dmtPw"
	const source = "https://ipfs.io/ipfs/" + cid
	client.EXPECT().GetResponseNoRetry(gomock.Any(), source, probeRangeHeader).
		Return(httpResp(http.StatusOK, "image/png", minimalPNG(32, 32), nil), nil)
	client.EXPECT().GetResponseNoRetry(gomock.Any(), "https://ipfs.feralfile.com/ipfs/"+cid, probeRangeHeader).
		Return(httpResp(http.StatusNotFound, "text/plain", []byte("not pinned"), nil), nil)
	checker := uri.NewURLChecker(client, mio, &uri.Config{
		IPFSGateways: []string{"https://ipfs.io", "https://ipfs.feralfile.com"},
	})
	result := checker.Check(context.Background(), source)
	require.Equal(t, uri.HealthStatusHealthy, result.Status)
	require.Nil(t, result.WorkingURL)
	require.Empty(t, result.FailureReason)
}

func TestResolver_RejectsBrowserOnlyCandidatesAndRepairsStoredHTTPURLs(t *testing.T) {
	const cid = "QmVJn8AG9x22BrbUaUj2CAQFtKMozSHyLvgV2X6X8dmtPw"
	for _, source := range []string{"ipfs://" + cid, "https://ipfs.io/ipfs/" + cid} {
		t.Run(source, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			client := mocks.NewMockHTTPClient(ctrl)
			mio := mocks.NewMockIO(ctrl)
			passthroughIO(mio)
			const target = "https://ipfs.filebase.io/ipfs/" + cid
			client.EXPECT().GetResponseNoRetry(gomock.Any(), target, probeRangeHeader).
				Return(httpResp(http.StatusOK, "image/png", minimalPNG(32, 32), nil), nil)
			resolver := uri.NewResolver(client, mio, &uri.Config{
				IPFSGateways: []string{"https://ipfs.io", "https://dweb.link", "https://ipfs.filebase.io"},
			})
			resolved, err := resolver.Resolve(context.Background(), source)
			require.NoError(t, err)
			require.Equal(t, target, resolved)
		})
	}
}

// Unsupported retired addresses cannot be returned as if a replacement had
// validated. In particular, IPNS is outside the CID-only migration contract.
func TestResolver_RejectsUnsupportedRetiredGatewayURLs(t *testing.T) {
	for _, source := range []string{
		"https://ipfs.io/ipns/example.org/art.html?seed=1#view",
		"https://example.ipns.dweb.link/art.html",
		"https://inbrowser.link/ipfs/not-a-cid/art.html",
		"https://ipfs.io/",
	} {
		t.Run(source, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			resolver := uri.NewResolver(mocks.NewMockHTTPClient(ctrl), mocks.NewMockIO(ctrl), &uri.Config{
				IPFSGateways: []string{"https://ipfs.filebase.io"},
			})
			resolved, err := resolver.Resolve(context.Background(), source)
			require.ErrorIs(t, err, uri.ErrRetiredGatewayReplacement)
			require.Empty(t, resolved)
		})
	}
}

// Retirement classification must retain the probe's policy failure for callers
// that also distinguish SSRF refusals from DNS resolution failures.
func TestResolver_RetiredGatewayReplacementPreservesCause(t *testing.T) {
	for _, cause := range []error{ssrf.ErrBlocked, ssrf.ErrResolutionFailed} {
		t.Run(cause.Error(), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			client := mocks.NewMockHTTPClient(ctrl)
			const cid = "QmVJn8AG9x22BrbUaUj2CAQFtKMozSHyLvgV2X6X8dmtPw"
			client.EXPECT().GetResponseNoRetry(gomock.Any(), "https://ipfs.filebase.io/ipfs/"+cid, probeRangeHeader).
				Return(nil, cause)
			resolver := uri.NewResolver(client, mocks.NewMockIO(ctrl), &uri.Config{
				IPFSGateways: []string{"https://ipfs.filebase.io"},
			})
			resolved, err := resolver.Resolve(context.Background(), "https://ipfs.io/ipfs/"+cid)
			require.ErrorIs(t, err, uri.ErrRetiredGatewayReplacement)
			require.ErrorIs(t, err, cause)
			require.Empty(t, resolved)
		})
	}
}

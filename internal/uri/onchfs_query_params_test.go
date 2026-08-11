package uri_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/uri"
)

// TestURLChecker_OnChFSFallbackPreservesQueryParams pins the guarantee issue #76 found
// missing: when a stored OnChFS media URL fails, the gateway fallback must re-probe the
// resource the player actually loads, not a different one.
//
// Reason: fxhash OnChFS artworks are addressed by content hash *plus* query parameters
// (fxhash/fxiteration/fxminter). A gateway can serve the bare hash while failing the exact
// iteration a viewer requests, so probing only "<gateway>/<hash>" reports healthy for media
// that never renders — the false-green signal issue #76 reported.
//
// Trade-offs: this asserts on the probe URL through a mocked HTTP client rather than a live
// gateway. onchfs.fxhash2.xyz is intermittently flaky (~10-50% 5xx per URL across repeated
// sampling of 121 production tokens, with no URL failing consistently), so no live URL is a
// stable oracle and a network-backed assertion would flap.
//
// Constraints: under the content-validating checker the direct probe is a single ranged GET;
// the gateway fallback rebuilds candidate URLs from the full reference, so for the URL's own
// gateway the candidate is the player URL again. The strict mock makes any request for the
// bare-hash URL a test failure — the trap this test exists to catch.
func TestURLChecker_OnChFSFallbackPreservesQueryParams(t *testing.T) {
	t.Parallel()

	const (
		gateway = "https://onchfs.fxhash2.xyz"
		hash    = "1639e8c989cddd2c0f9e951b126a21b1964309854c0a8e0a9b1193a655c6712a"
		// The URL the player loads: a specific iteration of the artwork.
		playerURL = gateway + "/" + hash +
			"/?fxhash=ooN5RUSuuSUEAxBcAAyHT6BrmT4tR5hFuhc5wmZw7TLeFuUaxWV" +
			"&fxiteration=131&fxminter=tz1cbJ2fHK4Tv7yES7Tq9dc9k8gXArnk7DyE"
	)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockHTTP := mocks.NewMockHTTPClient(ctrl)
	mockIO := mocks.NewMockIO(ctrl)

	// The player's URL is broken on this gateway: the direct probe 500s, and the gateway
	// fallback — which must carry the full reference — asks for the same player URL and
	// sees the same 500. No expectation exists for the bare-hash URL, so a query-dropping
	// probe fails the strict mock instead of finding the trap 200.
	mockHTTP.EXPECT().
		GetResponseNoRetry(gomock.Any(), playerURL, probeRangeHeader).
		Return(httpResp(http.StatusInternalServerError, "", nil, nil), nil).
		Times(2) // direct check + gateway-fallback probe of the same canonical URL

	checker := uri.NewURLChecker(mockHTTP, mockIO, &uri.Config{
		OnChFSGateways: []string{gateway},
	})

	result := checker.Check(context.Background(), playerURL)

	require.Equal(t, uri.HealthStatusBroken, result.Status,
		"media the player cannot load must not be reported healthy: the gateway serves the bare "+
			"hash but returns 500 for the iteration the player requests")
	assert.Nil(t, result.WorkingURL,
		"a query-less gateway URL is not a working replacement for a parameterised artwork URL")
}

// TestOnChFSGatewayRef covers what survives into a gateway probe, including the fragment that
// some fxhash tokens carry (observed in production metadata as "#0x405a8000...").
func TestOnChFSGatewayRef(t *testing.T) {
	t.Parallel()

	const hash = "1639e8c989cddd2c0f9e951b126a21b1964309854c0a8e0a9b1193a655c6712a"

	tests := []struct {
		name   string
		rawURL string
		want   string
	}{
		{
			name:   "bare hash is unchanged",
			rawURL: "https://onchfs.fxhash2.xyz/" + hash,
			want:   hash,
		},
		{
			name:   "trailing slash and query are preserved",
			rawURL: "https://onchfs.fxhash2.xyz/" + hash + "/?fxiteration=131&fxminter=tz1cb",
			want:   hash + "/?fxiteration=131&fxminter=tz1cb",
		},
		{
			name:   "fragment is preserved for the returned URL",
			rawURL: "https://onchfs.fxhash2.xyz/" + hash + "/?fxiteration=131#0x405a800000000000",
			want:   hash + "/?fxiteration=131#0x405a800000000000",
		},
		{
			// url.Parse keeps RawQuery verbatim, so a bad escape reaches the gateway as-is
			// rather than being silently dropped along with the rest of the parameters.
			name:   "malformed escape in query is passed through",
			rawURL: "https://onchfs.fxhash2.xyz/" + hash + "/?x=%zz",
			want:   hash + "/?x=%zz",
		},
		{
			name:   "unparseable URL degrades to the hash",
			rawURL: "https://onchfs.fxhash2.xyz/" + hash + "/\x7f",
			want:   hash,
		},
		{
			name:   "path without the expected hash degrades to the hash",
			rawURL: "https://onchfs.fxhash2.xyz/other/path",
			want:   hash,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, uri.OnChFSGatewayRef(tt.rawURL, hash))
		})
	}
}

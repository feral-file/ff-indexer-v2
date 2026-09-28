package uri_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/security/ssrf"
	"github.com/feral-file/ff-indexer-v2/internal/uri"
)

const timeoutTestCID = "QmVJn8AG9x22BrbUaUj2CAQFtKMozSHyLvgV2X6X8dmtPw"

// realHeaderTimeoutErr returns the error a real http.Client produces when its Timeout
// fires before response headers arrive (a stalling server), so tests classify the exact
// production error chain rather than a hand-built look-alike.
func realHeaderTimeoutErr(t *testing.T) error {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()
	_, err := (&http.Client{Timeout: 30 * time.Millisecond}).Get(srv.URL)
	require.Error(t, err)
	return err
}

// realBodyTimeoutErr returns the error a real http.Client produces when its Timeout fires
// while the body is still being read (headers sent, then the server stalls).
func realBodyTimeoutErr(t *testing.T) error {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte{0x89, 'P'})
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer srv.Close()
	resp, err := (&http.Client{Timeout: 50 * time.Millisecond}).Get(srv.URL)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	_, err = io.ReadAll(resp.Body)
	require.Error(t, err)
	return err
}

func newTimeoutTestChecker(t *testing.T, pool []string) (*mocks.MockHTTPClient, uri.URLChecker) {
	t.Helper()
	ctrl := gomock.NewController(t)
	client := mocks.NewMockHTTPClient(ctrl)
	mio := mocks.NewMockIO(ctrl)
	passthroughIO(mio)
	return client, uri.NewURLChecker(client, mio, &uri.Config{IPFSGateways: pool})
}

// quietDeadlineError satisfies errors.Is(context.DeadlineExceeded) without saying so in its
// message: classification must follow the error chain, not the text.
type quietDeadlineError struct{}

func (quietDeadlineError) Error() string        { return "gateway went quiet" }
func (quietDeadlineError) Is(target error) bool { return target == context.DeadlineExceeded }

// A probe that runs out of time says nothing about the URL. Seen in production: filebase
// answering a cold CID slowly under full-speed sweeping was persisted broken/transport and
// flipped viewable tokens to unviewable while the same URL served moments later.
func TestURLChecker_ProbeTimeoutIsTransient(t *testing.T) {
	const source = "https://example.com/art.png"
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"real http.Client timeout awaiting headers", realHeaderTimeoutErr(t)},
		{"bare deadline exceeded", fmt.Errorf("Get %q: %w", source, context.DeadlineExceeded)},
		{"context canceled", fmt.Errorf("Get %q: %w", source, context.Canceled)},
		{"deadline in the chain, not in the message", &url.Error{Op: "Get", URL: source, Err: quietDeadlineError{}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, checker := newTimeoutTestChecker(t, nil)
			client.EXPECT().GetResponseNoRetry(gomock.Any(), source, probeRangeHeader).Return(nil, tc.err)

			result := checker.Check(context.Background(), source)
			require.Equal(t, uri.HealthStatusTransientError, result.Status)
			require.Equal(t, uri.FailureTransport, result.FailureReason)
			require.Nil(t, result.WorkingURL)
		})
	}
}

// The check follows the error chain: text that merely mentions a deadline is not one.
func TestURLChecker_TimeoutWordingWithoutTimeoutStaysBroken(t *testing.T) {
	const source = "https://example.com/art.png"
	client, checker := newTimeoutTestChecker(t, nil)
	client.EXPECT().GetResponseNoRetry(gomock.Any(), source, probeRangeHeader).
		Return(nil, &url.Error{Op: "Get", URL: source, Err: errors.New("upstream said: context deadline exceeded; request canceled")})

	result := checker.Check(context.Background(), source)
	require.Equal(t, uri.HealthStatusBroken, result.Status)
	require.Equal(t, uri.FailureTransport, result.FailureReason)
}

// Failures that are answers, not a missing answer, stay broken.
func TestURLChecker_NonTimeoutTransportFailureStaysBroken(t *testing.T) {
	const source = "https://example.com/art.png"
	client, checker := newTimeoutTestChecker(t, nil)
	client.EXPECT().GetResponseNoRetry(gomock.Any(), source, probeRangeHeader).
		Return(nil, &url.Error{Op: "Get", URL: source, Err: errors.New("tls: failed to verify certificate: x509: certificate has expired")})

	result := checker.Check(context.Background(), source)
	require.Equal(t, uri.HealthStatusBroken, result.Status)
	require.Equal(t, uri.FailureTransport, result.FailureReason)
}

// SSRF refusals, completed DNS lookups and limiter refusals keep their meaning even when
// a context error is also in the chain; only a lookup cut short by our deadline is transient.
func TestURLChecker_TimeoutClassificationPrecedence(t *testing.T) {
	const source = "https://example.com/art.png"
	for _, tc := range []struct {
		name   string
		err    error
		status uri.HealthStatus
		reason uri.FailureReason
		ssrf   bool
	}{
		{"SSRF refusal wins over a deadline", fmt.Errorf("%w: %w", ssrf.ErrBlocked, context.DeadlineExceeded), uri.HealthStatusBroken, uri.FailureSSRF, true},
		{"completed DNS lookup stays dns", fmt.Errorf("%w: no such host", ssrf.ErrResolutionFailed), uri.HealthStatusBroken, uri.FailureDNS, false},
		{"DNS lookup cut short by our deadline is transient", fmt.Errorf("%w: lookup example.com: %w", ssrf.ErrResolutionFailed, context.DeadlineExceeded), uri.HealthStatusTransientError, uri.FailureTransport, false},
		{"limiter refusal stays transient", fmt.Errorf("%w: example.com: queue timeout", adapter.ErrRateLimitWait), uri.HealthStatusTransientError, uri.FailureTransport, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, checker := newTimeoutTestChecker(t, nil)
			client.EXPECT().GetResponseNoRetry(gomock.Any(), source, probeRangeHeader).Return(nil, tc.err)

			result := checker.Check(context.Background(), source)
			require.Equal(t, tc.status, result.Status)
			require.Equal(t, tc.reason, result.FailureReason)
			require.Equal(t, tc.ssrf, result.SSRFBlocked)
		})
	}
}

// A timeout while the body is still arriving is not truncation, even when the response
// declared a length: the server did not stop early, our budget did.
func TestURLChecker_BodyReadTimeoutIsTransientNotTruncated(t *testing.T) {
	const source = "https://example.com/art.png"
	bodyErr := realBodyTimeoutErr(t)
	client, checker := newTimeoutTestChecker(t, nil)
	resp := httpResp(http.StatusPartialContent, "image/png", nil, map[string]string{
		"Content-Range": fmt.Sprintf("bytes 0-%d/5000000", uri.DefaultProbeMaxBytes-1),
	})
	resp.Body = io.NopCloser(io.MultiReader(strings.NewReader("\x89PNG\r\n\x1a\n"), errReader{bodyErr}))
	client.EXPECT().GetResponseNoRetry(gomock.Any(), source, probeRangeHeader).Return(resp, nil)

	result := checker.Check(context.Background(), source)
	require.Equal(t, uri.HealthStatusTransientError, result.Status)
	require.Equal(t, uri.FailureTransport, result.FailureReason)
}

// A server that really stops short of its declared length is still truncated.
func TestURLChecker_BodyEndingEarlyIsStillTruncated(t *testing.T) {
	const source = "https://example.com/art.png"
	client, checker := newTimeoutTestChecker(t, nil)
	resp := httpResp(http.StatusPartialContent, "image/png", nil, map[string]string{
		"Content-Range": fmt.Sprintf("bytes 0-%d/5000000", uri.DefaultProbeMaxBytes-1),
	})
	resp.Body = io.NopCloser(io.MultiReader(strings.NewReader("\x89PNG\r\n\x1a\n"), errReader{io.ErrUnexpectedEOF}))
	client.EXPECT().GetResponseNoRetry(gomock.Any(), source, probeRangeHeader).Return(resp, nil)

	result := checker.Check(context.Background(), source)
	require.Equal(t, uri.HealthStatusBroken, result.Status)
	require.Equal(t, uri.FailureTruncated, result.FailureReason)
}

type errReader struct{ err error }

func (r errReader) Read([]byte) (int, error) { return 0, r.err }

// Gateway fallback after a timed-out direct probe: a gateway that answers promotes; enough
// conclusive answers condemn; inconclusive ones (5xx, timeouts, a lone answer when the pool
// has more gateways) keep the verdict transient.
func TestURLChecker_TimedOutGatewayFallback(t *testing.T) {
	const path = "/ipfs/" + timeoutTestCID + "/1754.png"
	const source = "https://ipfs.filebase.io" + path
	a, b := "https://gw-a.example", "https://gw-b.example"
	notFound := func() (*http.Response, error) {
		return httpResp(http.StatusNotFound, "text/plain", []byte("no link named 1754.png"), nil), nil
	}
	badGateway := func() (*http.Response, error) {
		return httpResp(http.StatusGatewayTimeout, "text/plain", []byte("retrieval timeout"), nil), nil
	}
	empty := func() (*http.Response, error) { return httpResp(http.StatusOK, "image/png", []byte{}, nil), nil }
	timedOut := func() (*http.Response, error) { return nil, realHeaderTimeoutErr(t) }
	served := func() (*http.Response, error) {
		return httpResp(http.StatusOK, "image/png", minimalPNG(32, 32), nil), nil
	}

	for _, tc := range []struct {
		name      string
		source    string
		pool      []string
		answers   map[string]func() (*http.Response, error)
		status    uri.HealthStatus
		reason    uri.FailureReason
		promoteTo string
	}{
		// The promoted result carries the direct probe's reason by design (it becomes the
		// original row's cause if propagation fails).
		{"a gateway serves it: promoted", source, []string{a, b}, map[string]func() (*http.Response, error){a: served, b: timedOut}, uri.HealthStatusHealthy, uri.FailureTransport, a + path},
		{"two gateways answer 404: broken", source, []string{a, b}, map[string]func() (*http.Response, error){a: notFound, b: notFound}, uri.HealthStatusBroken, uri.FailureHTTPStatus, ""},
		{"two gateways serve empty bodies: broken", source, []string{a, b}, map[string]func() (*http.Response, error){a: empty, b: empty}, uri.HealthStatusBroken, uri.FailureZeroLength, ""},
		{"one 404, one timeout: not enough, transient", source, []string{a, b}, map[string]func() (*http.Response, error){a: notFound, b: timedOut}, uri.HealthStatusTransientError, uri.FailureTransport, ""},
		{"two gateways 5xx: gateway state, transient", source, []string{a, b}, map[string]func() (*http.Response, error){a: badGateway, b: badGateway}, uri.HealthStatusTransientError, uri.FailureTransport, ""},
		{"single-gateway pool 404: broken", source, []string{a}, map[string]func() (*http.Response, error){a: notFound}, uri.HealthStatusBroken, uri.FailureHTTPStatus, ""},
		{"all time out: transient", source, []string{a, b}, map[string]func() (*http.Response, error){a: timedOut, b: timedOut}, uri.HealthStatusTransientError, uri.FailureTransport, ""},
		{"retired source keeps gateway_retired", "https://ipfs.io" + path, []string{a, b}, map[string]func() (*http.Response, error){a: notFound, b: notFound}, uri.HealthStatusBroken, uri.FailureGatewayRetired, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, checker := newTimeoutTestChecker(t, tc.pool)
			client.EXPECT().GetResponseNoRetry(gomock.Any(), tc.source, probeRangeHeader).Return(nil, realHeaderTimeoutErr(t))
			for gw, answer := range tc.answers {
				client.EXPECT().GetResponseNoRetry(gomock.Any(), gw+path, probeRangeHeader).DoAndReturn(
					func(context.Context, string, map[string]string) (*http.Response, error) { return answer() }).AnyTimes()
			}

			result := checker.Check(context.Background(), tc.source)
			require.Equal(t, tc.status, result.Status)
			require.Equal(t, tc.reason, result.FailureReason)
			if tc.promoteTo != "" {
				require.NotNil(t, result.WorkingURL)
				require.Equal(t, tc.promoteTo, *result.WorkingURL)
			} else {
				require.Nil(t, result.WorkingURL)
			}
		})
	}
}

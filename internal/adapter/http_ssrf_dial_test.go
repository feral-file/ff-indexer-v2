package adapter

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/security/ssrf"
)

// reboundValidator plays a DNS-rebinding hostname: the URL check passes, as it does when
// the first lookup returns a public address, while the dial reaches whatever the URL
// really points at. The dial guard is the real ssrf.Validator's.
type reboundValidator struct {
	*ssrf.Validator
}

func (reboundValidator) ValidateHTTPURL(context.Context, string) error { return nil }

func TestHTTPClientWithSSRFRefusesBlockedPeerAtDialTime(t *testing.T) {
	var hits atomic.Int32
	internal := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte("internal secret"))
	}))
	defer internal.Close()

	t.Run("direct request", func(t *testing.T) {
		client := NewHTTPClientWithSSRF(5*time.Second, reboundValidator{ssrf.NewValidator(ssrf.Options{})}, 3)
		_, err := client.GetResponseNoRetry(context.Background(), internal.URL, nil)
		require.ErrorIs(t, err, ssrf.ErrBlocked)
	})

	t.Run("redirect hop", func(t *testing.T) {
		// The first hop listens on ::1, which is allowlisted; it redirects to the
		// internal server on 127.0.0.1, which is not.
		ln, err := net.Listen("tcp", "[::1]:0")
		if err != nil {
			t.Skipf("IPv6 loopback unavailable: %v", err)
		}
		redirector := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, internal.URL, http.StatusFound)
		}))
		redirector.Listener = ln
		redirector.Start()
		defer redirector.Close()

		v := ssrf.NewValidator(ssrf.Options{AllowIPs: []netip.Addr{netip.MustParseAddr("::1")}})
		client := NewHTTPClientWithSSRF(5*time.Second, reboundValidator{v}, 3)
		_, err = client.GetResponseNoRetry(context.Background(), redirector.URL, nil)
		require.ErrorIs(t, err, ssrf.ErrBlocked)
	})

	assert.Zero(t, hits.Load(), "the internal server must never receive a request")

	t.Run("allowlisted IP still connects", func(t *testing.T) {
		v := ssrf.NewValidator(ssrf.Options{AllowIPs: []netip.Addr{netip.MustParseAddr("127.0.0.1")}})
		client := NewHTTPClientWithSSRF(5*time.Second, v, 3)
		resp, err := client.GetResponseNoRetry(context.Background(), internal.URL, nil)
		require.NoError(t, err)
		_ = resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})
}

package adapter

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/security/ssrf"
)

// allowAllValidator permits every URL (used only to exercise redirect counting).
type allowAllValidator struct{}

func (allowAllValidator) ValidateHTTPURL(context.Context, string) error { return nil }

func TestNewHTTPClientWithSSRF_blocksLoopback(t *testing.T) {
	t.Parallel()
	v := ssrf.NewValidator(ssrf.Options{})
	client := NewHTTPClientWithSSRF(2*time.Second, v, 3)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	_, err := client.GetResponseNoRetry(context.Background(), srv.URL, nil)
	require.Error(t, err)
	require.True(t, errors.Is(err, ssrf.ErrBlocked), "got %v", err)
}

func TestNewHTTPClientWithSSRF_maxRedirects(t *testing.T) {
	t.Parallel()
	var hopCount int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hopCount++
		if hopCount < 10 {
			http.Redirect(w, r, "/", http.StatusFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	client := NewHTTPClientWithSSRF(5*time.Second, allowAllValidator{}, 3)
	_, err := client.GetResponseNoRetry(context.Background(), srv.URL, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "stopped after 3 redirects")
}

func TestNewHTTPClientWithSSRF_allowsThreeHopChain(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/":
			http.Redirect(w, r, "/a", http.StatusFound)
		case "/a":
			http.Redirect(w, r, "/b", http.StatusFound)
		case "/b":
			http.Redirect(w, r, "/c", http.StatusFound)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	t.Cleanup(srv.Close)

	client := NewHTTPClientWithSSRF(5*time.Second, allowAllValidator{}, 3)
	resp, err := client.GetResponseNoRetry(context.Background(), srv.URL, nil)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
}

func TestNewHTTPClientWithSSRF_twoHopCapBlocksThird(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/":
			http.Redirect(w, r, "/a", http.StatusFound)
		case "/a":
			http.Redirect(w, r, "/b", http.StatusFound)
		case "/b":
			http.Redirect(w, r, "/c", http.StatusFound)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	t.Cleanup(srv.Close)

	client := NewHTTPClientWithSSRF(5*time.Second, allowAllValidator{}, 2)
	_, err := client.GetResponseNoRetry(context.Background(), srv.URL, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "stopped after 2 redirects")
}

func TestNewHTTPClientWithSSRF_zeroRedirectsBlocksLocation(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			http.Redirect(w, r, "/b", http.StatusFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	client := NewHTTPClientWithSSRF(5*time.Second, allowAllValidator{}, 0)
	_, err := client.GetResponseNoRetry(context.Background(), srv.URL, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "stopped after 0 redirects")
}

type redirectBlockValidator struct {
	calls int
}

func (v *redirectBlockValidator) ValidateHTTPURL(_ context.Context, _ string) error {
	v.calls++
	if v.calls >= 2 {
		return fmt.Errorf("%w: second hop denied", ssrf.ErrBlocked)
	}
	return nil
}

func TestNewHTTPClientWithSSRF_redirectValidated(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			http.Redirect(w, r, "/b", http.StatusFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	v := &redirectBlockValidator{}
	client := NewHTTPClientWithSSRF(5*time.Second, v, 5)
	_, err := client.GetResponseNoRetry(context.Background(), srv.URL, nil)
	require.Error(t, err)
	require.True(t, errors.Is(err, ssrf.ErrBlocked), "got %v", err)
}

func TestNewHTTPClientWithSSRF_nilValidator_fetchesWithoutBlock(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	client := NewHTTPClientWithSSRF(5*time.Second, nil, 99)
	resp, err := client.GetResponseNoRetry(context.Background(), srv.URL, nil)
	require.NoError(t, err)
	require.NotNil(t, resp.Body)
	require.NoError(t, resp.Body.Close())
}

package uri

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

type stubHTTPClient struct {
	headFn func(ctx context.Context, url string) (*http.Response, error)
}

func (s *stubHTTPClient) GetAndUnmarshal(ctx context.Context, url string, result interface{}) error {
	panic("unexpected call")
}
func (s *stubHTTPClient) GetResponse(ctx context.Context, url string, headers map[string]string) (*http.Response, error) {
	panic("unexpected call")
}
func (s *stubHTTPClient) GetResponseNoRetry(ctx context.Context, url string, headers map[string]string) (*http.Response, error) {
	panic("unexpected call")
}
func (s *stubHTTPClient) GetBytes(ctx context.Context, url string, headers map[string]string) ([]byte, error) {
	panic("unexpected call")
}
func (s *stubHTTPClient) GetPartialBytes(ctx context.Context, url string, maxBytes int) ([]byte, error) {
	panic("unexpected call")
}
func (s *stubHTTPClient) GetPartialBytesNoRetry(ctx context.Context, url string, maxBytes int) ([]byte, error) {
	panic("unexpected call")
}
func (s *stubHTTPClient) PostBytes(ctx context.Context, url string, headers map[string]string, body io.Reader) ([]byte, error) {
	panic("unexpected call")
}
func (s *stubHTTPClient) PostNoRetry(ctx context.Context, url string, headers map[string]string, body io.Reader) (*http.Response, error) {
	panic("unexpected call")
}
func (s *stubHTTPClient) Head(ctx context.Context, url string) (*http.Response, error) {
	panic("unexpected call")
}
func (s *stubHTTPClient) HeadNoRetry(ctx context.Context, url string) (*http.Response, error) {
	if s.headFn != nil {
		return s.headFn(ctx, url)
	}
	panic("unexpected call")
}

type stubIO struct{}

func (s *stubIO) ReadAll(r io.Reader) ([]byte, error) { panic("unexpected call") }
func (s *stubIO) Discard(r io.Reader) error           { _, _ = io.Copy(io.Discard, r); return nil }

func TestURLChecker_Check_BlocksHostnameResolvingToLoopback(t *testing.T) {
	origLookup := lookupIPAddrs
	lookupIPAddrs = func(ctx context.Context, host string) ([]net.IP, error) {
		if host == "safe-looking.example" {
			return []net.IP{net.ParseIP("127.0.0.1")}, nil
		}
		return nil, nil
	}
	defer func() { lookupIPAddrs = origLookup }()

	checker := NewURLChecker(&stubHTTPClient{}, &stubIO{}, &Config{})
	result := checker.Check(context.Background(), "https://safe-looking.example/internal")

	assert.Equal(t, HealthStatusBroken, result.Status)
	assert.NotNil(t, result.Error)
	assert.Contains(t, *result.Error, "blocked address")
}

func TestURLChecker_Check_AllowsHostnameResolvingToPublicIP(t *testing.T) {
	origLookup := lookupIPAddrs
	lookupIPAddrs = func(ctx context.Context, host string) ([]net.IP, error) {
		if host == "safe-looking.example" {
			return []net.IP{net.ParseIP("93.184.216.34")}, nil
		}
		return nil, nil
	}
	defer func() { lookupIPAddrs = origLookup }()

	httpClient := &stubHTTPClient{headFn: func(ctx context.Context, url string) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(nil))}, nil
	}}

	checker := NewURLChecker(httpClient, &stubIO{}, &Config{})
	result := checker.Check(context.Background(), "https://safe-looking.example/image.png")

	assert.Equal(t, HealthStatusHealthy, result.Status)
	assert.Nil(t, result.Error)
}

package adapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"syscall"
	"time"

	"github.com/cenkalti/backoff/v4"

	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/logger"
)

// HTTPClient defines an interface for HTTP client operations to enable mocking
//
//go:generate mockgen -source=http.go -destination=../mocks/http.go -package=mocks -mock_names=HTTPClient=MockHTTPClient
type HTTPClient interface {
	// Get performs a GET request and unmarshals the response into result
	Get(ctx context.Context, url string, result interface{}) error

	// Post performs a POST request and returns the response body
	Post(ctx context.Context, url string, contentType string, body io.Reader) ([]byte, error)

	// Head performs a HEAD request
	// The caller is responsible for closing the response body
	Head(ctx context.Context, url string) (*http.Response, error)

	// GetPartialContent performs a GET request with Range header to fetch partial content
	// Returns the partial content as bytes
	GetPartialContent(ctx context.Context, url string, maxBytes int) ([]byte, error)

	// GetWithResponse performs a GET request and returns the full HTTP response
	// The caller is responsible for checking status code and closing the response body
	GetWithResponse(ctx context.Context, url string) (*http.Response, error)

	// GetWithHeaders performs a GET request with custom headers and returns the response body
	GetWithHeaders(ctx context.Context, url string, headers map[string]string) ([]byte, error)
}

// RealHTTPClient implements HTTPClient using the standard http package
type RealHTTPClient struct {
	client *http.Client
}

// NewHTTPClient creates a new real HTTP client
func NewHTTPClient(timeout time.Duration) HTTPClient {
	return &RealHTTPClient{
		client: &http.Client{
			Timeout: timeout,
		},
	}
}

// isRetryableError determines if an error should trigger a retry
func isRetryableError(err error) bool {
	// Context cancellation and deadline exceeded are not retryable
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	// Check for url.Error (wraps most HTTP client errors)
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		// Check the underlying error
		err = urlErr.Err
	}

	// Check for net.Error interface (network-related errors)
	var netErr net.Error
	if errors.As(err, &netErr) {
		// Timeout errors are retryable
		if netErr.Timeout() {
			return true
		}
	}

	// Check for specific network errors that are retryable
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		// Connection refused is retryable (server might be temporarily down)
		if errors.Is(opErr.Err, syscall.ECONNREFUSED) {
			return true
		}
		// Connection reset by peer is retryable
		if errors.Is(opErr.Err, syscall.ECONNRESET) {
			return true
		}
		// Network unreachable is retryable
		if errors.Is(opErr.Err, syscall.ENETUNREACH) {
			return true
		}
		// Host unreachable is retryable
		if errors.Is(opErr.Err, syscall.EHOSTUNREACH) {
			return true
		}
	}

	// DNS errors might be retryable
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		// Temporary DNS errors are retryable
		if dnsErr.Temporary() {
			return true
		}
		// DNS timeouts are retryable
		if dnsErr.Timeout() {
			return true
		}
		// Permanent DNS errors (e.g., no such host) are not retryable
		return false
	}

	// By default, unknown errors are not retryable to avoid infinite retries
	return false
}

// doRequestWithRetryAndResponse executes an HTTP request with exponential backoff retry for rate limiting
// and returns the final response. The caller is responsible for closing the response body.
func (c *RealHTTPClient) doRequestWithRetryAndResponse(ctx context.Context, req *http.Request) (*http.Response, error) {
	var finalResp *http.Response

	operation := func() error {
		resp, err := c.client.Do(req)
		if err != nil {
			// Check if the error is retryable
			if isRetryableError(err) {
				logger.WarnCtx(ctx, "retryable error encountered", zap.Error(err), zap.String("url", req.URL.String()))
				return fmt.Errorf("retryable error: %w", err)
			}
			// Permanent errors should not be retried
			logger.WarnCtx(ctx, "permanent error encountered", zap.Error(err), zap.String("url", req.URL.String()))
			return backoff.Permanent(fmt.Errorf("permanent error: %w", err))
		}

		// Handle rate limiting - retry with backoff
		if resp.StatusCode == http.StatusTooManyRequests {
			if err := resp.Body.Close(); err != nil {
				logger.WarnCtx(ctx, "failed to close response body", zap.Error(err), zap.String("url", req.URL.String()))
			}
			logger.WarnCtx(ctx, "rate limited, retrying with backoff", zap.String("url", req.URL.String()))
			return fmt.Errorf("rate limited (429), retrying")
		}

		// Store the final response (redirects are handled automatically by the client)
		finalResp = resp
		return nil
	}

	// Configure exponential backoff
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = 5 * time.Second
	b.MaxInterval = 30 * time.Second
	b.MaxElapsedTime = 5 * time.Minute // Total retry duration
	b.Multiplier = 2.0
	b.RandomizationFactor = 0.5 // Add jitter to prevent thundering herd

	// Execute with retry and context support
	if err := backoff.Retry(operation, backoff.WithContext(b, ctx)); err != nil {
		return nil, fmt.Errorf("request failed after retries: %w", err)
	}

	return finalResp, nil
}

// doRequestWithRetry executes an HTTP request with exponential backoff retry for rate limiting
// and returns the response body as bytes
func (c *RealHTTPClient) doRequestWithRetry(ctx context.Context, req *http.Request) ([]byte, error) {
	resp, err := c.doRequestWithRetryAndResponse(ctx, req)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			logger.WarnCtx(ctx, "failed to close response body", zap.Error(err), zap.String("url", req.URL.String()))
		}
	}()

	// Check status code for non-2xx responses
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(body))
	}

	// Read the response body
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	return respBody, nil
}

// Get performs a GET request and unmarshals the response into result
// Implements exponential backoff retry for rate limiting (429) responses
func (c *RealHTTPClient) Get(ctx context.Context, url string, result interface{}) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	respBody, err := c.doRequestWithRetry(ctx, req)
	if err != nil {
		return err
	}

	// Decode the response
	if err := json.Unmarshal(respBody, result); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	return nil
}

// Post performs a POST request and returns the response body
// Implements exponential backoff retry for rate limiting (429) responses
func (c *RealHTTPClient) Post(ctx context.Context, url string, contentType string, body io.Reader) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, body)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	return c.doRequestWithRetry(ctx, req)
}

// Head performs a HEAD request
// The caller is responsible for closing the response body
func (c *RealHTTPClient) Head(ctx context.Context, url string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	return c.doRequestWithRetryAndResponse(ctx, req)
}

// GetPartialContent performs a GET request with Range header to fetch partial content
// Returns the partial content as bytes
func (c *RealHTTPClient) GetPartialContent(ctx context.Context, url string, maxBytes int) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Set Range header to fetch only the first maxBytes
	req.Header.Set("Range", fmt.Sprintf("bytes=0-%d", maxBytes-1))

	resp, err := c.doRequestWithRetryAndResponse(ctx, req)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			logger.WarnCtx(ctx, "failed to close response body", zap.Error(err), zap.String("url", req.URL.String()))
		}
	}()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("unexpected status code %d", resp.StatusCode)
	}

	// Read the response body (limited by Range header or entire content if server doesn't support Range)
	body, err := io.ReadAll(io.LimitReader(resp.Body, int64(maxBytes)))
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	return body, nil
}

// GetWithResponse performs a GET request and returns the full HTTP response
// The caller is responsible for checking status code and closing the response body
func (c *RealHTTPClient) GetWithResponse(ctx context.Context, url string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	return c.doRequestWithRetryAndResponse(ctx, req)
}

// GetWithHeaders performs a GET request with custom headers and returns the response body
// Implements exponential backoff retry for rate limiting (429) responses
func (c *RealHTTPClient) GetWithHeaders(ctx context.Context, url string, headers map[string]string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Set custom headers
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	return c.doRequestWithRetry(ctx, req)
}

package adapter

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/cenkalti/backoff/v4"

	logger "github.com/bitmark-inc/autonomy-logger"
	"go.uber.org/zap"
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

// doRequestWithRetry executes an HTTP request with exponential backoff retry for rate limiting
func (c *RealHTTPClient) doRequestWithRetry(ctx context.Context, req *http.Request) ([]byte, error) {
	var respBody []byte

	operation := func() error {
		resp, err := c.client.Do(req)
		if err != nil {
			// Network errors are retryable
			return fmt.Errorf("failed to perform request: %w", err)
		}
		defer func() {
			if err := resp.Body.Close(); err != nil {
				logger.Warn("failed to close response body", zap.Error(err), zap.String("url", req.URL.String()))
			}
		}()

		// Handle rate limiting - retry with backoff
		if resp.StatusCode == http.StatusTooManyRequests {
			logger.Warn("rate limited, retrying with backoff", zap.String("url", req.URL.String()))
			return fmt.Errorf("rate limited (429), retrying")
		}

		// Other non-OK status codes are permanent errors
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			return backoff.Permanent(fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(body)))
		}

		// Read the response body
		respBody, err = io.ReadAll(resp.Body)
		if err != nil {
			return backoff.Permanent(fmt.Errorf("failed to read response body: %w", err))
		}

		return nil
	}

	// Configure exponential backoff
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = 2 * time.Second
	b.MaxInterval = 30 * time.Second
	b.MaxElapsedTime = 1 * time.Minute // Total retry duration
	b.Multiplier = 2.0
	b.RandomizationFactor = 0.5 // Add jitter to prevent thundering herd

	// Execute with retry and context support
	if err := backoff.Retry(operation, backoff.WithContext(b, ctx)); err != nil {
		return nil, fmt.Errorf("request failed after retries: %w", err)
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

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to perform request: %w", err)
	}

	return resp, nil
}

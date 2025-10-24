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
	Get(ctx context.Context, url string, result interface{}) error
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

// Get performs a GET request and unmarshals the response into result
// Implements exponential backoff retry for rate limiting (429) responses
func (c *RealHTTPClient) Get(ctx context.Context, url string, result interface{}) error {
	operation := func() error {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			// Request creation errors are permanent (non-retryable)
			return backoff.Permanent(fmt.Errorf("failed to create request: %w", err))
		}

		resp, err := c.client.Do(req)
		if err != nil {
			// Network errors are retryable
			return fmt.Errorf("failed to perform request: %w", err)
		}
		defer func() {
			if err := resp.Body.Close(); err != nil {
				logger.Warn("failed to close response body", zap.Error(err), zap.String("url", url))
			}
		}()

		// Handle rate limiting - retry with backoff
		if resp.StatusCode == http.StatusTooManyRequests {
			logger.Warn("rate limited, retrying with backoff", zap.String("url", url))
			return fmt.Errorf("rate limited (429), retrying")
		}

		// Other non-OK status codes are permanent errors
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			return backoff.Permanent(fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(body)))
		}

		// Decode the response
		if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
			return backoff.Permanent(fmt.Errorf("failed to decode response: %w", err))
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
		return fmt.Errorf("request failed after retries: %w", err)
	}

	return nil
}

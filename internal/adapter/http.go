package adapter

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

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
func (c *RealHTTPClient) Get(ctx context.Context, url string, result interface{}) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to perform request: %w", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			logger.Warn("failed to close response body", zap.Error(err), zap.String("url", url))
		}
	}()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(body))
	}

	if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	return nil
}

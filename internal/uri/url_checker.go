package uri

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/security/ssrf"
	"github.com/feral-file/ff-indexer-v2/internal/types"
)

// HealthStatus represents the result of a health check
type HealthStatus string

const (
	// HealthStatusHealthy indicates the URL is accessible
	HealthStatusHealthy HealthStatus = "healthy"
	// HealthStatusBroken indicates the URL is not accessible
	HealthStatusBroken HealthStatus = "broken"
	// HealthStatusTransientError indicates a temporary error that should be retried
	HealthStatusTransientError HealthStatus = "transient_error"
)

// HealthCheckResult represents the result of checking a URL's health
type HealthCheckResult struct {
	Status      HealthStatus
	WorkingURL  *string // Alternative working URL if found (for IPFS/Arweave/OnChFS)
	Error       *string // Error message if broken
	SSRFBlocked bool    // True when ssrf.ErrBlocked refused the fetch (policy); false for DNS (ErrResolutionFailed) or transport errors
}

// URLChecker defines the interface for checking URL health
//
//go:generate mockgen -source=url_checker.go -destination=../mocks/url_checker.go -package=mocks -mock_names=URLChecker=MockURLChecker
type URLChecker interface {
	// Check performs a health check on a URL
	// Returns the health status, an alternative working URL if found, and any error
	Check(ctx context.Context, url string) HealthCheckResult
}

type urlChecker struct {
	httpClient      adapter.HTTPClient
	io              adapter.IO
	ipfsGateways    []string
	arweaveGateways []string
	onchfsGateways  []string
}

// NewURLChecker creates a new health checker
func NewURLChecker(httpClient adapter.HTTPClient, io adapter.IO, config *Config) URLChecker {
	return &urlChecker{
		httpClient:      httpClient,
		io:              io,
		ipfsGateways:    config.IPFSGateways,
		arweaveGateways: config.ArweaveGateways,
		onchfsGateways:  config.OnChFSGateways,
	}
}

// Check performs a health check on a URL
// This checker only handles HTTP/HTTPS URLs, not URI schemes like ipfs://, ar://, onchfs://
func (c *urlChecker) Check(ctx context.Context, url string) HealthCheckResult {
	// Validate that this is an HTTP/HTTPS URL
	if !types.IsValidURL(url) {
		errMsg := "invalid URL format"
		return HealthCheckResult{
			Status: HealthStatusBroken,
			Error:  &errMsg,
		}
	}

	// Only accept HTTP/HTTPS URLs
	if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
		errMsg := "only HTTP/HTTPS URLs are supported"
		return HealthCheckResult{
			Status: HealthStatusBroken,
			Error:  &errMsg,
		}
	}

	// 1. Always try the HTTP URL first
	result := c.checkHTTPS(ctx, url)

	// 2. SSRF policy refusal is final: do not run IPFS/Arweave/OnChFS fallbacks that could
	// re-probe public gateways and rewrite a blocklisted origin as "healthy".
	if result.SSRFBlocked {
		return result
	}

	// 3. If healthy, return immediately
	if result.Status == HealthStatusHealthy {
		return result
	}

	// 4. If broken or transient error, try fallback resolution for known gateway types

	// Check if it's an IPFS gateway URL - resolve with CID
	if isIPFS, cid := types.IsIPFSGatewayURL(url); isIPFS {
		logger.InfoCtx(ctx, "HTTP check failed, trying IPFS gateway resolution", zap.String("url", url), zap.String("cid", cid))
		return c.checkIPFSGateway(ctx, cid)
	}

	// Check if it's an Arweave gateway URL - resolve with tx ID
	if isArweave, txID := types.IsArweaveGatewayURL(url); isArweave {
		logger.InfoCtx(ctx, "HTTP check failed, trying Arweave gateway resolution", zap.String("url", url), zap.String("txID", txID))
		return c.checkArweaveGateway(ctx, txID)
	}

	// Check if it's an OnChFS URL - resolve hash across configured gateways
	if isOnChFS, hash := types.IsOnChFSGatewayURL(url); isOnChFS {
		logger.InfoCtx(ctx, "HTTP check failed, trying OnChFS gateway resolution", zap.String("url", url), zap.String("hash", hash))
		return c.checkOnChFSGateway(ctx, hash)
	}

	// 5. For other HTTP URLs, return the original result
	return result
}

// checkIPFSGateway resolves IPFS CID across multiple gateways and returns the first working one
func (c *urlChecker) checkIPFSGateway(ctx context.Context, cid string) HealthCheckResult {
	workingURL, err := FindWorkingIPFSGateway(ctx, c.httpClient, cid, c.ipfsGateways)
	if err != nil {
		return mapOutboundFetchErr(err, false)
	}

	return HealthCheckResult{
		Status:     HealthStatusHealthy,
		WorkingURL: &workingURL,
	}
}

// checkArweaveGateway resolves Arweave tx ID across multiple gateways and returns the first working one
func (c *urlChecker) checkArweaveGateway(ctx context.Context, txID string) HealthCheckResult {
	workingURL, err := FindWorkingArweaveGateway(ctx, c.httpClient, txID, c.arweaveGateways)
	if err != nil {
		return mapOutboundFetchErr(err, false)
	}

	return HealthCheckResult{
		Status:     HealthStatusHealthy,
		WorkingURL: &workingURL,
	}
}

// checkOnChFSGateway resolves an OnChFS content hash across configured gateways and returns the first working URL.
func (c *urlChecker) checkOnChFSGateway(ctx context.Context, hash string) HealthCheckResult {
	workingURL, err := FindWorkingOnChFSGateway(ctx, c.httpClient, hash, c.onchfsGateways)
	if err != nil {
		return mapOutboundFetchErr(err, false)
	}

	return HealthCheckResult{
		Status:     HealthStatusHealthy,
		WorkingURL: &workingURL,
	}
}

// healthResultFromSSRF maps SSRF policy failures to a broken result with SSRFBlocked set.
func healthResultFromSSRF(err error) (HealthCheckResult, bool) {
	if errors.Is(err, ssrf.ErrBlocked) {
		msg := err.Error()
		return HealthCheckResult{
			Status:      HealthStatusBroken,
			Error:       &msg,
			SSRFBlocked: true,
		}, true
	}
	return HealthCheckResult{}, false
}

// mapOutboundFetchErr maps HTTP client fetch errors to HealthCheckResult.
//
// SSRF policy failures (ErrBlocked) yield broken + SSRFBlocked. DNS resolution failures
// (ErrResolutionFailed) yield broken without SSRFBlocked so bad or unresolvable hosts are not
// retried every sweep tick (scheduled sweeps can still pick the row up later).
// When classifyTransient is true, retryable transport errors map to transient_error; when false,
// they stay broken (used for IPFS/Arweave/OnChFS gateway aggregation).
func mapOutboundFetchErr(err error, classifyTransient bool) HealthCheckResult {
	if hr, ok := healthResultFromSSRF(err); ok {
		return hr
	}
	if errors.Is(err, ssrf.ErrResolutionFailed) {
		msg := err.Error()
		return HealthCheckResult{
			Status: HealthStatusBroken,
			Error:  &msg,
		}
	}
	if classifyTransient && adapter.IsHTTPRetryableError(err) {
		msg := err.Error()
		return HealthCheckResult{
			Status: HealthStatusTransientError,
			Error:  &msg,
		}
	}
	msg := err.Error()
	return HealthCheckResult{
		Status: HealthStatusBroken,
		Error:  &msg,
	}
}

// checkHTTPS checks regular HTTPS URLs
func (c *urlChecker) checkHTTPS(ctx context.Context, url string) HealthCheckResult {
	// 1. Try HEAD request first
	resp, err := c.httpClient.HeadNoRetry(ctx, url)
	if err != nil {
		if hr, ok := healthResultFromSSRF(err); ok {
			return hr
		}
	} else if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if resp.Body != nil {
			_ = resp.Body.Close()
		}
		return HealthCheckResult{
			Status: HealthStatusHealthy,
		}
	}

	// Close the failed HEAD response
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}

	logger.InfoCtx(ctx, "HEAD request failed, trying GET with Range", zap.String("url", url), zap.Error(err))

	// 2. Try GET with Range header
	return c.checkWithRange(ctx, url)
}

// checkWithRange performs a GET request with Range header to minimize data transfer
func (c *urlChecker) checkWithRange(ctx context.Context, url string) HealthCheckResult {
	headers := map[string]string{
		"Range": "bytes=0-1023", // Request only first 1KB
	}

	resp, err := c.httpClient.GetResponseNoRetry(ctx, url, headers)
	if err != nil {
		return mapOutboundFetchErr(err, true)
	}
	defer func() {
		if resp.Body != nil {
			// Discard and close body without reading
			_ = c.io.Discard(resp.Body)
			_ = resp.Body.Close()
		}
	}()

	switch resp.StatusCode {
	case http.StatusPartialContent: // 206 - Range request accepted
		return HealthCheckResult{
			Status: HealthStatusHealthy,
		}

	case http.StatusOK: // 200 - Server doesn't support range, but file exists
		return HealthCheckResult{
			Status: HealthStatusHealthy,
		}

	case http.StatusRequestedRangeNotSatisfiable: // 416 - Range not satisfiable
		// Try without range
		logger.InfoCtx(ctx, "Range not satisfiable, trying HEAD without range", zap.String("url", url))
		return c.checkWithoutRange(ctx, url)

	case http.StatusTooManyRequests: // 429 - Rate limited
		errMsg := "rate limited (429)"
		return HealthCheckResult{
			Status: HealthStatusTransientError,
			Error:  &errMsg,
		}

	default:
		// Try one more time without range for other status codes
		logger.InfoCtx(ctx, "GET with Range failed, trying without range", zap.String("url", url), zap.Int("status", resp.StatusCode))
		return c.checkWithoutRange(ctx, url)
	}
}

// checkWithoutRange performs a GET request without Range header as final fallback
func (c *urlChecker) checkWithoutRange(ctx context.Context, url string) HealthCheckResult {
	resp, err := c.httpClient.GetResponseNoRetry(ctx, url, nil)
	if err != nil {
		return mapOutboundFetchErr(err, true)
	}
	defer func() {
		if resp.Body != nil {
			// Discard and close body without reading
			_ = c.io.Discard(resp.Body)
			_ = resp.Body.Close()
		}
	}()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return HealthCheckResult{
			Status: HealthStatusHealthy,
		}
	}

	if resp.StatusCode == http.StatusTooManyRequests { // 429 - Rate limited
		errMsg := "rate limited (429)"
		return HealthCheckResult{
			Status: HealthStatusTransientError,
			Error:  &errMsg,
		}
	}

	errMsg := fmt.Sprintf("HTTP %d", resp.StatusCode)
	return HealthCheckResult{
		Status: HealthStatusBroken,
		Error:  &errMsg,
	}
}

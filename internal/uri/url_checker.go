package uri

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
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
	// HealthStatusHealthy indicates the URL is accessible and its content validates
	HealthStatusHealthy HealthStatus = "healthy"
	// HealthStatusBroken indicates the URL is not accessible or serves invalid content
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

	// FailureReason is the machine-readable broken cause ("" when healthy/transient or
	// when the failure is an unclassified transport error).
	FailureReason FailureReason
	// ObservedContentType is the normalized Content-Type header from the probe ("" when
	// the fetch never returned headers).
	ObservedContentType string
	// SniffedContentType is the magic-byte-detected type of the body prefix ("" when no
	// body was read).
	SniffedContentType string
}

// FailureReasonPtr returns the failure reason as a nullable string for persistence
// (nil when no classified reason).
func (r HealthCheckResult) FailureReasonPtr() *string {
	if r.FailureReason == "" {
		return nil
	}
	s := r.FailureReason.String()
	return &s
}

// ObservedContentTypePtr returns the observed Content-Type as a nullable string for
// persistence (nil when the probe never saw headers).
func (r HealthCheckResult) ObservedContentTypePtr() *string {
	if r.ObservedContentType == "" {
		return nil
	}
	s := r.ObservedContentType
	return &s
}

// SniffedContentTypePtr returns the sniffed content type as a nullable string for
// persistence (nil when no body was read).
func (r HealthCheckResult) SniffedContentTypePtr() *string {
	if r.SniffedContentType == "" {
		return nil
	}
	s := r.SniffedContentType
	return &s
}

// URLChecker defines the interface for checking URL health
//
//go:generate mockgen -source=url_checker.go -destination=../mocks/url_checker.go -package=mocks -mock_names=URLChecker=MockURLChecker
type URLChecker interface {
	// Check performs a health check on a URL
	// Returns the health status, an alternative working URL if found, and any error
	Check(ctx context.Context, url string) HealthCheckResult
}

// contentProbe performs a single validated ranged GET against a URL.
//
// Reason: the previous flow (HEAD first, then a 1KB ranged GET whose body was discarded)
// declared any 2xx healthy without reading a byte — the mechanism behind directory-listing
// and error-page-with-200 false positives (feral-file#3482, #76, #96). One ranged GET
// returns status, headers, and the first bytes in a single round trip, which is fewer
// requests than the old HEAD-then-GET pair. Shared by URLChecker and Resolver so direct
// checks and gateway-fallback probes apply identical validation.
type contentProbe struct {
	httpClient adapter.HTTPClient
	io         adapter.IO
	validator  ContentValidator
	maxBytes   int
}

// probeResult pairs the health verdict with the raw fetch error (when the failure happened
// at the transport layer) so gateway aggregation can preserve ssrf.ErrBlocked /
// ssrf.ErrResolutionFailed sentinel classes that the string-typed HealthCheckResult loses.
type probeResult struct {
	hcr      HealthCheckResult
	fetchErr error // non-nil only for transport-level failures
}

// probe fetches up to maxBytes of the URL and validates the content.
func (p *contentProbe) probe(ctx context.Context, url string, withRange bool) probeResult {
	var headers map[string]string
	if withRange {
		headers = map[string]string{"Range": fmt.Sprintf("bytes=0-%d", p.maxBytes-1)}
	}

	resp, err := p.httpClient.GetResponseNoRetry(ctx, url, headers)
	if err != nil {
		return probeResult{hcr: mapOutboundFetchErr(err, true), fetchErr: err}
	}
	// Close without draining: the body may be arbitrarily large when a server ignores the
	// Range header, and fully discarding it would download the whole file per check.
	// Sacrificing connection reuse is the cheaper trade for a fleet-wide sweeper.
	defer func() {
		if resp.Body != nil {
			_ = resp.Body.Close()
		}
	}()

	switch {
	// A 206 whose Content-Range is absent, malformed, or does not start at byte 0 is not
	// serving the requested resource prefix — validating those bytes would let a
	// misbehaving gateway bypass the prefix-based checks (magic bytes, directory-listing
	// and error-page markers). Retry without Range: the unranged read is still capped at
	// maxBytes and yields the true prefix.
	case resp.StatusCode == http.StatusPartialContent && !partialRangeStartsAtZero(resp):
		if withRange {
			logger.InfoCtx(ctx, "206 without a valid from-zero Content-Range, retrying without range",
				zap.String("url", url),
				zap.String("content_range", resp.Header.Get("Content-Range")),
			)
			return p.probe(ctx, url, false)
		}
		// A 206 answer to a range-less request is itself protocol-broken.
		errMsg := fmt.Sprintf("206 with invalid Content-Range %q to an unranged request", resp.Header.Get("Content-Range"))
		return probeResult{hcr: HealthCheckResult{
			Status:        HealthStatusBroken,
			Error:         &errMsg,
			FailureReason: FailureHTTPStatus,
		}}

	// The whole 2xx range is a fetch success (matching the documented L0 contract and the
	// pre-content-validation checker): 203 arrives via transforming proxies with valid
	// media, and 204's empty body is a content verdict (zero_length), not an HTTP one.
	// 416/429 sit outside 2xx, so the explicit cases below are unaffected.
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		body, readErr := p.io.ReadAll(io.LimitReader(resp.Body, int64(p.maxBytes)))
		total := totalLength(resp)
		if readErr != nil && !isConclusiveTruncation(len(body), total, p.maxBytes) {
			// The connection died mid-body with no length evidence: a transport
			// condition, retried next sweep.
			errMsg := readErr.Error()
			return probeResult{hcr: HealthCheckResult{
				Status: HealthStatusTransientError,
				Error:  &errMsg,
			}}
		}
		// readErr with conclusive length evidence falls through: the server advertised
		// more bytes than it delivered, which is truncation, not weather. The partial
		// body goes through the validator so the verdict is the same broken/truncated a
		// cleanly-short body gets — otherwise consistently truncated media stays
		// transient forever, and the sweeper never persists transient results, leaving a
		// stale healthy row to keep the token viewable.

		verdict := p.validator.Validate(resp.Header.Get("Content-Type"), body, total)
		result := HealthCheckResult{
			Status:              HealthStatusHealthy,
			ObservedContentType: verdict.Declared,
			SniffedContentType:  verdict.Sniffed,
		}
		if !verdict.OK {
			result.Status = HealthStatusBroken
			result.FailureReason = verdict.FailureReason
			result.Error = &verdict.Detail
		}
		return probeResult{hcr: result}

	case resp.StatusCode == http.StatusRequestedRangeNotSatisfiable && withRange:
		// Server rejects Range outright: retry once without it.
		logger.InfoCtx(ctx, "Range not satisfiable, retrying without range", zap.String("url", url))
		return p.probe(ctx, url, false)

	case resp.StatusCode == http.StatusTooManyRequests:
		errMsg := "rate limited (429)"
		return probeResult{hcr: HealthCheckResult{
			Status: HealthStatusTransientError,
			Error:  &errMsg,
		}}

	default:
		errMsg := fmt.Sprintf("HTTP %d", resp.StatusCode)
		return probeResult{hcr: HealthCheckResult{
			Status:        HealthStatusBroken,
			Error:         &errMsg,
			FailureReason: FailureHTTPStatus,
		}}
	}
}

// gatewayProbe adapts probe for gateway candidate selection: nil means the candidate URL
// serves validated content. Transport errors keep their sentinel classes for
// noteGatewayProbeFailure precedence.
func (p *contentProbe) gatewayProbe(ctx context.Context, url string) error {
	result := p.probe(ctx, url, true)
	if result.fetchErr != nil {
		return result.fetchErr
	}
	if result.hcr.Status != HealthStatusHealthy {
		if result.hcr.Error != nil {
			return errors.New(*result.hcr.Error)
		}
		return fmt.Errorf("gateway probe failed with status %s", result.hcr.Status)
	}
	return nil
}

// partialRangeStartsAtZero reports whether a 206's Content-Range declares a satisfied
// range beginning at byte 0 ("bytes 0-<end>/<total or *>"). Only from-zero ranges carry
// the resource prefix the content validator's checks are defined over.
func partialRangeStartsAtZero(resp *http.Response) bool {
	after, ok := strings.CutPrefix(strings.TrimSpace(resp.Header.Get("Content-Range")), "bytes ")
	if !ok {
		return false
	}
	start, _, ok := strings.Cut(after, "-")
	return ok && strings.TrimSpace(start) == "0"
}

// isConclusiveTruncation reports whether a mid-body read failure is evidence of a
// truncated resource rather than transient network weather: the response declared a
// total length, fewer bytes than that arrived, and the read stopped short of the probe
// cap (so the shortfall is the server's, not the cap's).
func isConclusiveTruncation(got int, total int64, maxBytes int) bool {
	return total > 0 && int64(got) < total && got < maxBytes
}

// totalLength extracts the full resource length: the Content-Range total for 206
// responses, Content-Length otherwise, -1 when unknown.
func totalLength(resp *http.Response) int64 {
	if resp.StatusCode == http.StatusPartialContent {
		// Content-Range: bytes 0-32767/12345678 (total may be "*" when unknown)
		cr := resp.Header.Get("Content-Range")
		if idx := strings.LastIndex(cr, "/"); idx >= 0 {
			if total, err := strconv.ParseInt(cr[idx+1:], 10, 64); err == nil {
				return total
			}
		}
		return -1
	}
	return resp.ContentLength
}

type urlChecker struct {
	probe           *contentProbe
	ipfsGateways    []string
	arweaveGateways []string
	onchfsGateways  []string
}

// NewURLChecker creates a new health checker. The content validator is built from the
// config's probe settings (probe window size and known-bad page markers).
func NewURLChecker(httpClient adapter.HTTPClient, io adapter.IO, config *Config) URLChecker {
	return &urlChecker{
		probe: &contentProbe{
			httpClient: httpClient,
			io:         io,
			validator:  NewContentValidator(config.probeMaxBytes(), config.KnownBadPageMarkers),
			maxBytes:   config.probeMaxBytes(),
		},
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

	// 1. Always try the URL directly first (validated ranged GET)
	result := c.probe.probe(ctx, url, true).hcr

	// 2. SSRF policy refusal is final: do not run IPFS/Arweave/OnChFS fallbacks that could
	// re-probe public gateways and rewrite a blocklisted origin as "healthy".
	if result.SSRFBlocked {
		return result
	}

	// 3. If healthy, return immediately
	if result.Status == HealthStatusHealthy {
		return result
	}

	// 4. If broken or transient error, try fallback resolution for known gateway types.
	// The fallback probes validate content too, so a directory CID does not get
	// "rescued" by another gateway serving the same listing.

	// Check if it's an IPFS gateway URL - resolve with CID
	if isIPFS, cid := types.IsIPFSGatewayURL(url); isIPFS {
		logger.InfoCtx(ctx, "Direct check failed, trying IPFS gateway resolution", zap.String("url", url), zap.String("cid", cid))
		return c.checkGatewayFallback(ctx, result, func(ctx context.Context) (string, error) {
			return FindWorkingIPFSGateway(ctx, c.probe.gatewayProbe, cid, c.ipfsGateways)
		})
	}

	// Check if it's an Arweave gateway URL - resolve with tx ID
	if isArweave, txID := types.IsArweaveGatewayURL(url); isArweave {
		logger.InfoCtx(ctx, "Direct check failed, trying Arweave gateway resolution", zap.String("url", url), zap.String("txID", txID))
		return c.checkGatewayFallback(ctx, result, func(ctx context.Context) (string, error) {
			return FindWorkingArweaveGateway(ctx, c.probe.gatewayProbe, txID, c.arweaveGateways)
		})
	}

	// Check if it's an OnChFS URL - resolve the same resource across configured gateways.
	// The reference keeps the fxhash query parameters so alternative gateways are asked for
	// the artwork iteration a player loads, not just the bare content hash (issue #76).
	if isOnChFS, hash := types.IsOnChFSGatewayURL(url); isOnChFS {
		ref := OnChFSGatewayRef(url, hash)
		logger.InfoCtx(ctx, "Direct check failed, trying OnChFS gateway resolution", zap.String("url", url), zap.String("ref", ref))
		return c.checkGatewayFallback(ctx, result, func(ctx context.Context) (string, error) {
			return FindWorkingOnChFSGateway(ctx, c.probe.gatewayProbe, ref, c.onchfsGateways)
		})
	}

	// 5. For other HTTP URLs, return the original result
	return result
}

// checkGatewayFallback runs a gateway resolution and maps its outcome to a health result.
// When every gateway fails, the direct probe's result is returned (not the aggregate
// resolution error): the direct result carries the more specific failure_reason and
// content-type observations for the canonical URL.
func (c *urlChecker) checkGatewayFallback(ctx context.Context, direct HealthCheckResult, find func(ctx context.Context) (string, error)) HealthCheckResult {
	workingURL, err := find(ctx)
	if err != nil {
		if hr, ok := healthResultFromSSRF(err); ok {
			return hr
		}
		return direct
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
			Status:        HealthStatusBroken,
			Error:         &msg,
			SSRFBlocked:   true,
			FailureReason: FailureSSRF,
		}, true
	}
	return HealthCheckResult{}, false
}

// mapOutboundFetchErr maps HTTP client fetch errors to HealthCheckResult.
//
// SSRF policy failures (ErrBlocked, including redirect-cap exhaustion from the SSRF HTTP
// client) yield broken + SSRFBlocked. DNS resolution failures (ErrResolutionFailed) yield
// broken without SSRFBlocked so bad or unresolvable hosts are not retried every sweep tick
// (scheduled sweeps can still pick the row up later). When classifyTransient is true,
// retryable transport errors map to transient_error; when false, they stay broken (used
// for IPFS/Arweave/OnChFS gateway aggregation). Unclassified transport errors keep an
// empty FailureReason — the taxonomy only records causes it can actually distinguish.
func mapOutboundFetchErr(err error, classifyTransient bool) HealthCheckResult {
	if hr, ok := healthResultFromSSRF(err); ok {
		return hr
	}
	if errors.Is(err, ssrf.ErrResolutionFailed) {
		msg := err.Error()
		return HealthCheckResult{
			Status:        HealthStatusBroken,
			Error:         &msg,
			FailureReason: FailureDNS,
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

package uri

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"

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

	// WorkingURLObserved / WorkingURLSniffed are the winning fallback gateway's own
	// validated content observations, set only when WorkingURL is. They are distinct
	// from the fields above (which describe the failed DIRECT probe, kept for
	// propagation-failure diagnostics): callers persist these on the promoted row,
	// whose bytes the fallback probe just content-validated.
	WorkingURLObserved string
	WorkingURLSniffed  string
}

// WorkingURLObservedPtr returns the promoted URL's observed Content-Type as a nullable
// string for persistence (nil when unknown).
func (r HealthCheckResult) WorkingURLObservedPtr() *string {
	if r.WorkingURLObserved == "" {
		return nil
	}
	s := r.WorkingURLObserved
	return &s
}

// WorkingURLSniffedPtr returns the promoted URL's sniffed content type as a nullable
// string for persistence (nil when unknown).
func (r HealthCheckResult) WorkingURLSniffedPtr() *string {
	if r.WorkingURLSniffed == "" {
		return nil
	}
	s := r.WorkingURLSniffed
	return &s
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
	// A 206 answer to a range-less request is protocol-broken regardless of how
	// plausible its Content-Range looks — trusting it would let a gateway serve
	// different bytes on the retry path than on a normal fetch.
	case resp.StatusCode == http.StatusPartialContent && !withRange:
		errMsg := fmt.Sprintf("206 with Content-Range %q to an unranged request", resp.Header.Get("Content-Range"))
		return probeResult{hcr: HealthCheckResult{
			Status:        HealthStatusBroken,
			Error:         &errMsg,
			FailureReason: FailureHTTPStatus,
		}}

	// A ranged 206 is trusted only when its Content-Range parses under the full
	// single-range grammar AND starts at byte 0 — anything else is not serving the
	// resource prefix the validator's checks (magic bytes, directory-listing and
	// error-page markers) are defined over. Retry without Range: the unranged read is
	// still capped at maxBytes and yields the true prefix.
	case resp.StatusCode == http.StatusPartialContent:
		start, end, total, ok := parseContentRange(resp.Header.Get("Content-Range"))
		if !ok || start != 0 {
			logger.InfoCtx(ctx, "206 without a valid from-zero Content-Range, retrying without range",
				zap.String("url", url),
				zap.String("content_range", resp.Header.Get("Content-Range")),
			)
			return p.probe(ctx, url, false)
		}
		// This response promises exactly end+1 bytes — NOT the full object length. A
		// server may legitimately satisfy a from-zero range shorter than requested, so
		// truncation is judged against what this response promised, never against the
		// Content-Range total (which would false-broken valid media).
		promised := end + 1
		// A valid prefix shorter than the sniff window that is also shorter than the
		// resource itself is not the evidence the validator's rules are defined over:
		// a few leading bytes of binary media sniff as text/plain, and the mismatch
		// rule would false-broken real media (err-healthy contract). Retry without
		// Range for the true prefix. A prefix that IS the whole resource
		// (promised == total) is complete evidence and is validated as-is, whatever
		// its size.
		if promised < minClassifiableBytes && (total < 0 || promised < total) {
			logger.InfoCtx(ctx, "206 prefix too short to classify, retrying without range",
				zap.String("url", url),
				zap.Int64("promised_bytes", promised),
			)
			return p.probe(ctx, url, false)
		}
		return p.readAndValidate(resp, promised)

	// The whole 2xx range is a fetch success (matching the documented L0 contract and the
	// pre-content-validation checker): 203 arrives via transforming proxies with valid
	// media, and 204's empty body is a content verdict (zero_length), not an HTTP one.
	// 416/429 sit outside 2xx, so the explicit cases below are unaffected.
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return p.readAndValidate(resp, resp.ContentLength)

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

// gatewayProbeResult probes a gateway candidate and returns the validated result on
// success. A non-nil error means the candidate is unusable; transport errors keep their
// sentinel classes for noteGatewayProbeFailure precedence.
func (p *contentProbe) gatewayProbeResult(ctx context.Context, url string) (HealthCheckResult, error) {
	result := p.probe(ctx, url, true)
	if result.fetchErr != nil {
		return HealthCheckResult{}, result.fetchErr
	}
	if result.hcr.Status != HealthStatusHealthy {
		if result.hcr.Error != nil {
			return HealthCheckResult{}, errors.New(*result.hcr.Error)
		}
		return HealthCheckResult{}, fmt.Errorf("gateway probe failed with status %s", result.hcr.Status)
	}
	return result.hcr, nil
}

// gatewayProbe adapts gatewayProbeResult for callers that only select a candidate
// (the resolver): nil means the candidate URL serves validated content.
func (p *contentProbe) gatewayProbe(ctx context.Context, url string) error {
	_, err := p.gatewayProbeResult(ctx, url)
	return err
}

// readAndValidate reads up to maxBytes of the body and runs content validation.
// promisedLength is how many bytes THIS response undertook to deliver (end-start+1 for a
// 206, Content-Length otherwise, -1 unknown); truncation is judged against it.
func (p *contentProbe) readAndValidate(resp *http.Response, promisedLength int64) probeResult {
	body, readErr := p.io.ReadAll(io.LimitReader(resp.Body, int64(p.maxBytes)))
	if readErr != nil && !isConclusiveTruncation(len(body), promisedLength, p.maxBytes) {
		// The connection died mid-body with no length evidence: a transport condition,
		// retried next sweep.
		errMsg := readErr.Error()
		return probeResult{hcr: HealthCheckResult{
			Status: HealthStatusTransientError,
			Error:  &errMsg,
		}}
	}
	// readErr with conclusive length evidence falls through: the server delivered fewer
	// bytes than this response promised, which is truncation, not weather. The partial
	// body goes through the validator so the verdict is the same broken/truncated a
	// cleanly-short body gets — otherwise consistently truncated media stays transient
	// forever, and the sweeper never persists transient results, leaving a stale healthy
	// row to keep the token viewable.

	verdict := p.validator.Validate(resp.Header.Get("Content-Type"), body, promisedLength)
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
}

// minClassifiableBytes is the smallest valid 206 prefix worth handing to content
// validation when the resource is larger than it. 512 bytes is the standard content
// sniffing window (net/http DetectContentType); every container magic signature and the
// text-vs-binary judgment fit comfortably within it, while a shorter prefix of binary
// media can sniff as text and false-broken real artworks.
const minClassifiableBytes = 512

// parseContentRange parses a Content-Range header under the full satisfied single-range
// grammar: "bytes <start>-<end>/<total|*>" with digit-only positions, start <= end, and
// (when numeric) total > end. Anything else — including the prefix-plausible
// "bytes 0-not-a-range/500000" — is rejected so a misbehaving gateway cannot get
// arbitrary bytes trusted as the resource prefix. total is -1 for the unknown-length
// form ("*").
func parseContentRange(header string) (start, end, total int64, ok bool) {
	after, found := strings.CutPrefix(strings.TrimSpace(header), "bytes ")
	if !found {
		return 0, 0, 0, false
	}
	rangePart, totalPart, found := strings.Cut(after, "/")
	if !found {
		return 0, 0, 0, false
	}
	startStr, endStr, found := strings.Cut(rangePart, "-")
	if !found {
		return 0, 0, 0, false
	}
	start, errStart := strconv.ParseInt(strings.TrimSpace(startStr), 10, 64)
	end, errEnd := strconv.ParseInt(strings.TrimSpace(endStr), 10, 64)
	if errStart != nil || errEnd != nil || start < 0 || end < start {
		return 0, 0, 0, false
	}
	total = -1
	if totalPart = strings.TrimSpace(totalPart); totalPart != "*" {
		var errTotal error
		total, errTotal = strconv.ParseInt(totalPart, 10, 64)
		if errTotal != nil || total <= end {
			return 0, 0, 0, false
		}
	}
	return start, end, total, true
}

// isConclusiveTruncation reports whether a mid-body read failure is evidence of a
// truncated resource rather than transient network weather: the response promised a
// length, fewer bytes than that arrived, and the read stopped short of the probe cap
// (so the shortfall is the server's, not the cap's).
func isConclusiveTruncation(got int, promisedLength int64, maxBytes int) bool {
	return promisedLength > 0 && int64(got) < promisedLength && got < maxBytes
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
		return c.checkGatewayFallback(ctx, result, func(ctx context.Context, probe GatewayProbe) (string, error) {
			return FindWorkingIPFSGateway(ctx, probe, cid, c.ipfsGateways)
		})
	}

	// Check if it's an Arweave gateway URL - resolve with tx ID
	if isArweave, txID := types.IsArweaveGatewayURL(url); isArweave {
		logger.InfoCtx(ctx, "Direct check failed, trying Arweave gateway resolution", zap.String("url", url), zap.String("txID", txID))
		return c.checkGatewayFallback(ctx, result, func(ctx context.Context, probe GatewayProbe) (string, error) {
			return FindWorkingArweaveGateway(ctx, probe, txID, c.arweaveGateways)
		})
	}

	// Check if it's an OnChFS URL - resolve the same resource across configured gateways.
	// The reference keeps the fxhash query parameters so alternative gateways are asked for
	// the artwork iteration a player loads, not just the bare content hash (issue #76).
	if isOnChFS, hash := types.IsOnChFSGatewayURL(url); isOnChFS {
		ref := OnChFSGatewayRef(url, hash)
		logger.InfoCtx(ctx, "Direct check failed, trying OnChFS gateway resolution", zap.String("url", url), zap.String("ref", ref))
		return c.checkGatewayFallback(ctx, result, func(ctx context.Context, probe GatewayProbe) (string, error) {
			return FindWorkingOnChFSGateway(ctx, probe, ref, c.onchfsGateways)
		})
	}

	// 5. For other HTTP URLs, return the original result
	return result
}

// checkGatewayFallback runs a gateway resolution and maps its outcome to a health result.
// When every gateway fails, the direct probe's result is returned (not the aggregate
// resolution error): the direct result carries the more specific failure_reason and
// content-type observations for the canonical URL.
func (c *urlChecker) checkGatewayFallback(ctx context.Context, direct HealthCheckResult, find func(ctx context.Context, probe GatewayProbe) (string, error)) HealthCheckResult {
	// Record each successful candidate's validated observations so the winner's can be
	// returned. The candidates are probed concurrently, hence the mutex; keyed by URL
	// because findWorkingGateway reports only the winning URL, not which probe won.
	var mu sync.Mutex
	observations := make(map[string]HealthCheckResult)
	recordingProbe := func(ctx context.Context, url string) error {
		hcr, err := c.probe.gatewayProbeResult(ctx, url)
		if err != nil {
			return err
		}
		mu.Lock()
		observations[url] = hcr
		mu.Unlock()
		return nil
	}

	workingURL, err := find(ctx, recordingProbe)
	if err != nil {
		if hr, ok := healthResultFromSSRF(err); ok {
			return hr
		}
		return direct
	}

	// The direct probe's diagnostics ride along on the healthy result. Reason: callers
	// promote WorkingURL via UpdateMediaURLAndPropagate, and when that promotion fails
	// they persist the ORIGINAL URL as broken — without these fields that row would
	// carry no failure reason or content-type observations, silently degrading the
	// per-reason breakdown exactly on propagation failures. Error is deliberately not
	// carried: a healthy result with a non-nil Error would be ambiguous to consumers,
	// and healthy persistence clears diagnostics anyway.
	//
	// The winning gateway's own validated observations travel separately: they belong
	// to the PROMOTED row, whose bytes were just content-validated — leaving them NULL
	// would falsify the documented "not yet probed" meaning and starve render-probe
	// class selection until a much later sweep.
	mu.Lock()
	winner := observations[workingURL]
	mu.Unlock()
	return HealthCheckResult{
		Status:              HealthStatusHealthy,
		WorkingURL:          &workingURL,
		FailureReason:       direct.FailureReason,
		ObservedContentType: direct.ObservedContentType,
		SniffedContentType:  direct.SniffedContentType,
		WorkingURLObserved:  winner.ObservedContentType,
		WorkingURLSniffed:   winner.SniffedContentType,
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

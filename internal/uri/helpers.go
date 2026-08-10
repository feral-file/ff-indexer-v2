package uri

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/security/ssrf"
)

// OnChFSGatewayRef derives the gateway-relative reference for an OnChFS gateway URL: the
// content hash plus any path suffix, query string and fragment that address one artwork
// iteration. Pass the result to FindWorkingOnChFSGateway so alternative gateways are probed
// for the resource a player would load.
//
// Reason: types.IsOnChFSGatewayURL reports only the 64-hex hash. Rebuilding a probe URL from
// that alone silently drops the fxhash/fxiteration/fxminter parameters, which both mis-reports
// health (issue #76) and would rewrite the stored URL to one that no longer identifies the
// iteration.
//
// Constraints: falls back to hash when rawURL cannot be parsed or its path does not carry the
// expected hash, so a malformed stored URL degrades to the previous behavior instead of
// producing a nonsensical probe.
func OnChFSGatewayRef(rawURL, hash string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return hash
	}

	ref := strings.TrimPrefix(u.EscapedPath(), "/")
	if !strings.HasPrefix(ref, hash) {
		return hash
	}

	if u.RawQuery != "" {
		ref += "?" + u.RawQuery
	}
	if f := u.EscapedFragment(); f != "" {
		ref += "#" + f
	}

	return ref
}

// noteGatewayProbeFailure records SSRF policy blocks vs DNS resolution failures from parallel gateway HEAD probes.
// ErrBlocked takes precedence when surfacing an error to callers.
func noteGatewayProbeFailure(err error, blocked *error, resolution *error) {
	if err == nil {
		return
	}
	if errors.Is(err, ssrf.ErrBlocked) {
		*blocked = err
	}
	if errors.Is(err, ssrf.ErrResolutionFailed) {
		*resolution = err
	}
}

// FindWorkingIPFSGateway finds a working IPFS gateway for the given CID
// It tries all gateways in parallel and returns the first working one
func FindWorkingIPFSGateway(ctx context.Context, httpClient adapter.HTTPClient, cid string, gateways []string) (string, error) {
	if len(gateways) == 0 {
		return "", fmt.Errorf("no IPFS gateways configured")
	}

	logger.InfoCtx(ctx, "Finding working IPFS gateway", zap.String("cid", cid), zap.Int("gateways", len(gateways)))

	// Try all gateways in parallel
	type result struct {
		url string
		err error
	}

	resultCh := make(chan result, len(gateways))
	var wg sync.WaitGroup

	// Test each gateway with HEAD request
	for _, gateway := range gateways {
		wg.Add(1)
		go func(gw string) {
			defer wg.Done()

			url := fmt.Sprintf("%s/ipfs/%s", gw, cid)
			resp, err := httpClient.Head(ctx, url)
			if err != nil {
				resultCh <- result{err: err}
				return
			}
			if err := resp.Body.Close(); err != nil {
				logger.WarnCtx(ctx, "failed to close response body", zap.Error(err), zap.String("url", url))
			}

			if resp.StatusCode == http.StatusOK {
				resultCh <- result{url: url}
			} else {
				resultCh <- result{err: fmt.Errorf("gateway returned status %d", resp.StatusCode)}
			}
		}(gateway)
	}

	// Wait for all goroutines in a separate goroutine
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// Return the first successful result
	var blockedErr, resolutionErr error
	for res := range resultCh {
		if res.err == nil {
			logger.InfoCtx(ctx, "Found working IPFS gateway", zap.String("url", res.url))
			return res.url, nil
		}
		noteGatewayProbeFailure(res.err, &blockedErr, &resolutionErr)
	}

	if blockedErr != nil {
		return "", blockedErr
	}
	if resolutionErr != nil {
		return "", resolutionErr
	}

	return "", fmt.Errorf("no working IPFS gateway found for CID: %s", cid)
}

// FindWorkingArweaveGateway finds a working Arweave gateway for the given transaction ID
// It tries all gateways in parallel and returns the first working one
func FindWorkingArweaveGateway(ctx context.Context, httpClient adapter.HTTPClient, txID string, gateways []string) (string, error) {
	if len(gateways) == 0 {
		return "", fmt.Errorf("no Arweave gateways configured")
	}

	logger.InfoCtx(ctx, "Finding working Arweave gateway", zap.String("txID", txID), zap.Int("gateways", len(gateways)))

	// Try all gateways in parallel
	type result struct {
		url string
		err error
	}

	resultCh := make(chan result, len(gateways))
	var wg sync.WaitGroup

	// Test each gateway with HEAD request
	for _, gateway := range gateways {
		wg.Add(1)
		go func(gw string) {
			defer wg.Done()

			url := fmt.Sprintf("%s/%s", gw, txID)
			resp, err := httpClient.Head(ctx, url)
			if err != nil {
				resultCh <- result{err: err}
				return
			}
			if err := resp.Body.Close(); err != nil {
				logger.WarnCtx(ctx, "failed to close response body", zap.Error(err), zap.String("url", url))
			}

			if resp.StatusCode == http.StatusOK {
				resultCh <- result{url: url}
			} else {
				resultCh <- result{err: fmt.Errorf("gateway returned status %d", resp.StatusCode)}
			}
		}(gateway)
	}

	// Wait for all goroutines in a separate goroutine
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// Return the first successful result
	var blockedErr, resolutionErr error
	for res := range resultCh {
		if res.err == nil {
			logger.InfoCtx(ctx, "Found working Arweave gateway", zap.String("url", res.url))
			return res.url, nil
		}
		noteGatewayProbeFailure(res.err, &blockedErr, &resolutionErr)
	}

	if blockedErr != nil {
		return "", blockedErr
	}
	if resolutionErr != nil {
		return "", resolutionErr
	}

	return "", fmt.Errorf("no working Arweave gateway found for TX: %s", txID)
}

// FindWorkingOnChFSGateway finds a working OnChFS gateway for the given resource reference.
// It tries all gateways in parallel and returns the first working one.
//
// The ref parameter is a gateway-relative reference: a Keccak-256 hash (64 hex characters)
// optionally followed by a path suffix, query string and fragment.
//
// Reason: fxhash OnChFS artworks are addressed by content hash *plus* query parameters
// (fxhash/fxiteration/fxminter). A gateway can serve the bare hash while failing the specific
// iteration a viewer requests, so probing the hash alone answers a different question than
// "can this media be played" — see issue #76.
//
// Constraints: ref must be passed through verbatim. Callers that hold a full gateway URL should
// derive it with OnChFSGatewayRef rather than reducing the URL to its hash. Any fragment is
// carried for the returned URL only; net/http never puts a fragment on the wire.
func FindWorkingOnChFSGateway(ctx context.Context, httpClient adapter.HTTPClient, ref string, gateways []string) (string, error) {
	if len(gateways) == 0 {
		return "", fmt.Errorf("no OnChFS gateways configured")
	}

	logger.InfoCtx(ctx, "Finding working OnChFS gateway", zap.String("ref", ref), zap.Int("gateways", len(gateways)))

	// Try all gateways in parallel
	type result struct {
		url string
		err error
	}

	resultCh := make(chan result, len(gateways))
	var wg sync.WaitGroup

	// Test each gateway with HEAD request
	for _, gateway := range gateways {
		wg.Add(1)
		go func(gw string) {
			defer wg.Done()

			url := fmt.Sprintf("%s/%s", gw, ref)
			resp, err := httpClient.Head(ctx, url)
			if err != nil {
				resultCh <- result{err: err}
				return
			}
			if err := resp.Body.Close(); err != nil {
				logger.WarnCtx(ctx, "failed to close response body", zap.Error(err), zap.String("url", url))
			}

			if resp.StatusCode == http.StatusOK {
				resultCh <- result{url: url}
			} else {
				resultCh <- result{err: fmt.Errorf("gateway returned status %d", resp.StatusCode)}
			}
		}(gateway)
	}

	// Wait for all goroutines in a separate goroutine
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// Return the first successful result
	var blockedErr, resolutionErr error
	for res := range resultCh {
		if res.err == nil {
			logger.InfoCtx(ctx, "Found working OnChFS gateway", zap.String("url", res.url))
			return res.url, nil
		}
		noteGatewayProbeFailure(res.err, &blockedErr, &resolutionErr)
	}

	if blockedErr != nil {
		return "", blockedErr
	}
	if resolutionErr != nil {
		return "", resolutionErr
	}

	return "", fmt.Errorf("no working OnChFS gateway found for ref: %s", ref)
}

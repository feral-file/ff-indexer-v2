package uri

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"go.uber.org/zap"

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

// GatewayProbe validates that a candidate gateway URL serves usable content, returning nil
// on success.
//
// Reason: gateway selection previously accepted the first candidate answering 200 to a bare
// HEAD — which is exactly how a directory CID's listing page became the stored "working"
// URL (feral-file#3482). Probes now perform the same validated ranged GET as direct health
// checks, so a gateway only wins if its bytes look like media. Constraints: transport
// errors must preserve ssrf.ErrBlocked / ssrf.ErrResolutionFailed sentinel classes (via
// errors.Is) for noteGatewayProbeFailure precedence.
type GatewayProbe func(ctx context.Context, url string) error

// noteGatewayProbeFailure records SSRF policy blocks vs DNS resolution failures from parallel gateway probes.
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

// findWorkingGateway probes all candidate URLs in parallel and returns the first that
// validates. label names the gateway family for logs and the not-found error.
//
// The three exported wrappers existed as near-identical copies before being collapsed
// here; only the candidate URL format differs per family.
func findWorkingGateway(ctx context.Context, probe GatewayProbe, candidateURLs []string, label, id string) (string, error) {
	if len(candidateURLs) == 0 {
		return "", fmt.Errorf("no %s gateways configured", label)
	}

	logger.InfoCtx(ctx, "Finding working gateway",
		zap.String("family", label),
		zap.String("id", id),
		zap.Int("gateways", len(candidateURLs)),
	)

	type result struct {
		url string
		err error
	}

	resultCh := make(chan result, len(candidateURLs))
	var wg sync.WaitGroup

	for _, url := range candidateURLs {
		wg.Add(1)
		go func(u string) {
			defer wg.Done()
			if err := probe(ctx, u); err != nil {
				resultCh <- result{err: err}
				return
			}
			resultCh <- result{url: u}
		}(url)
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
			logger.InfoCtx(ctx, "Found working gateway", zap.String("family", label), zap.String("url", res.url))
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

	return "", fmt.Errorf("no working %s gateway found for %s", label, id)
}

// FindWorkingIPFSGateway finds a working IPFS gateway for the given CID.
// It probes all gateways in parallel and returns the first whose content validates.
func FindWorkingIPFSGateway(ctx context.Context, probe GatewayProbe, cid string, gateways []string) (string, error) {
	urls := make([]string, len(gateways))
	for i, gw := range gateways {
		urls[i] = fmt.Sprintf("%s/ipfs/%s", gw, cid)
	}
	return findWorkingGateway(ctx, probe, urls, "IPFS", cid)
}

// FindWorkingArweaveGateway finds a working Arweave gateway for the given transaction ID.
// It probes all gateways in parallel and returns the first whose content validates.
func FindWorkingArweaveGateway(ctx context.Context, probe GatewayProbe, txID string, gateways []string) (string, error) {
	urls := make([]string, len(gateways))
	for i, gw := range gateways {
		urls[i] = fmt.Sprintf("%s/%s", gw, txID)
	}
	return findWorkingGateway(ctx, probe, urls, "Arweave", txID)
}

// FindWorkingOnChFSGateway finds a working OnChFS gateway for the given resource reference.
// It probes all gateways in parallel and returns the first whose content validates.
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
func FindWorkingOnChFSGateway(ctx context.Context, probe GatewayProbe, ref string, gateways []string) (string, error) {
	urls := make([]string, len(gateways))
	for i, gw := range gateways {
		urls[i] = fmt.Sprintf("%s/%s", gw, ref)
	}
	return findWorkingGateway(ctx, probe, urls, "OnChFS", ref)
}

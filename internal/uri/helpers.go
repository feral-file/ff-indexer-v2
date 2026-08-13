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

// errDirectoryListing marks a gateway probe that failed because the candidate URL served
// an IPFS directory listing instead of media. It classifies the resource, not the
// gateway: a listing means the requested ref addresses a directory, so probing other
// gateways with the same bare ref can only yield more listings — the useful retry is the
// directory's index.html entry point (feral-file#3482).
var errDirectoryListing = errors.New("served a directory listing")

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
//
// sawDirectoryListing reports whether any candidate failed because it served an IPFS
// directory listing (errDirectoryListing). It is returned as a value rather than wrapped
// into the aggregate error so the signal survives error-class precedence (an SSRF/DNS
// error winning the aggregate) and cannot be silently lost by error-message rewording.
// Only FindWorkingIPFSGateway acts on it; the other families ignore it.
func findWorkingGateway(ctx context.Context, probe GatewayProbe, candidateURLs []string, label, id string) (string, bool, error) {
	if len(candidateURLs) == 0 {
		return "", false, fmt.Errorf("no %s gateways configured", label)
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
	sawDirectoryListing := false
	for res := range resultCh {
		if res.err == nil {
			logger.InfoCtx(ctx, "Found working gateway", zap.String("family", label), zap.String("url", res.url))
			return res.url, sawDirectoryListing, nil
		}
		noteGatewayProbeFailure(res.err, &blockedErr, &resolutionErr)
		sawDirectoryListing = sawDirectoryListing || errors.Is(res.err, errDirectoryListing)
	}

	err := fmt.Errorf("no working %s gateway found for %s", label, id)
	if blockedErr != nil {
		err = blockedErr
	} else if resolutionErr != nil {
		err = resolutionErr
	}
	return "", sawDirectoryListing, err
}

// FindWorkingIPFSGateway finds a working IPFS gateway for the given ref (a CID
// optionally followed by a path). It probes all gateways in parallel and returns the
// first whose content validates.
//
// When selection fails and at least one candidate served a directory listing, the ref
// addresses an IPFS directory, not a file — the standard shape of hic-et-nunc-era Tezos
// artworks (mimeType application/x-directory), whose playable document lives at
// index.html inside the directory (feral-file#3482). One retry probes <ref>/index.html
// and returns that URL when it validates, so the stored canonical URL is the artwork's
// entry point rather than the listing.
//
// Reason: hooking the retry here rather than in the resolver or the metadata enhancers
// covers every caller with one mechanism — metadata resolution stores playable URLs for
// newly indexed tokens, and the health checker's gateway fallback promotes the entry
// point over stored bare directory URLs, healing existing rows on their next sweep.
// Trade-offs: a directory ref costs one extra probe round, and a directory with no
// index.html pays it on every sweep re-check — accepted as rare and cheap over
// persisting a dedicated failure reason for it. The retry keys on the listing verdict,
// not the token's declared application/x-directory mime type, so a gateway that answers
// directory refs with something other than a Kubo listing (404, custom template) does
// not trigger it; mime-driven resolution would need metadata plumbed into every caller
// and is left as a follow-up. Constraints: index.html is the only entry point tried —
// guessing further names risks storing a non-entry file as the artwork; refs already
// targeting index.html are not retried.
func FindWorkingIPFSGateway(ctx context.Context, probe GatewayProbe, ref string, gateways []string) (string, error) {
	url, sawDirectoryListing, err := findWorkingGateway(ctx, probe, candidateURLs(gateways, "%s/ipfs/%s", ref), "IPFS", ref)
	if err == nil || !sawDirectoryListing {
		return url, err
	}
	entryRef, ok := directoryEntryRef(ref)
	if !ok {
		return "", err
	}
	logger.InfoCtx(ctx, "IPFS ref served directory listings, retrying with its index.html entry point",
		zap.String("ref", ref))
	entryURL, _, entryErr := findWorkingGateway(ctx, probe, candidateURLs(gateways, "%s/ipfs/%s", entryRef), "IPFS", entryRef)
	if entryErr != nil {
		// Wrap both rounds: round 1 may carry the ssrf.ErrBlocked / ErrResolutionFailed
		// sentinel that callers classify with errors.Is, and dropping it would persist a
		// blocklisted origin as an ordinary broken row.
		return "", fmt.Errorf("directory ref has no working index.html entry point: %w (bare ref: %w)", entryErr, err)
	}
	return entryURL, nil
}

// candidateURLs builds one probe URL per gateway; format is the family's URL shape
// ("%s/ipfs/%s" for IPFS, "%s/%s" for Arweave and OnChFS).
func candidateURLs(gateways []string, format, ref string) []string {
	urls := make([]string, len(gateways))
	for i, gw := range gateways {
		urls[i] = fmt.Sprintf(format, gw, ref)
	}
	return urls
}

// directoryEntryRef rewrites a directory ref to its index.html entry point, keeping any
// query and fragment: fxhash-style artworks address an iteration via query parameters
// (the same contract OnChFSGatewayRef preserves, issue #76), so the entry point must be
// inserted before them, not appended after. Not ok when the ref already targets
// index.html — there is nothing further to try.
func directoryEntryRef(ref string) (string, bool) {
	path, rest := ref, ""
	if i := strings.IndexAny(ref, "?#"); i >= 0 {
		path, rest = ref[:i], ref[i:]
	}
	path = strings.TrimSuffix(path, "/")
	if path == "" || strings.HasSuffix(path, "/index.html") {
		return "", false
	}
	return path + "/index.html" + rest, true
}

// FindWorkingArweaveGateway finds a working Arweave gateway for the given transaction ID.
// It probes all gateways in parallel and returns the first whose content validates.
func FindWorkingArweaveGateway(ctx context.Context, probe GatewayProbe, txID string, gateways []string) (string, error) {
	url, _, err := findWorkingGateway(ctx, probe, candidateURLs(gateways, "%s/%s", txID), "Arweave", txID)
	return url, err
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
	url, _, err := findWorkingGateway(ctx, probe, candidateURLs(gateways, "%s/%s", ref), "OnChFS", ref)
	return url, err
}

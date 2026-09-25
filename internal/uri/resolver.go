package uri

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/types"
)

// DefaultProbeMaxBytes is the default size of the validated probe window. 32KB is enough
// for every container-header check and for directory-listing/error-page markers, while
// keeping a full-corpus sweep's bandwidth bounded.
const DefaultProbeMaxBytes = 32 * 1024

// ErrRetiredGatewayReplacement marks a retired HTTP gateway without a validated
// replacement. Callers must preserve stored metadata and enrichment for retry,
// rather than treating this as missing metadata and selecting a generic vendor.
var ErrRetiredGatewayReplacement = errors.New("no validated replacement for retired gateway")

// Config holds configuration for the URI resolver and URL checker
type Config struct {
	// IPFSGateways is the list of IPFS gateways to try
	IPFSGateways []string
	// ArweaveGateways is the list of Arweave gateways to try
	ArweaveGateways []string
	// OnChFSGateways is the list of OnChFS gateways to try
	OnChFSGateways []string
	// ProbeMaxBytes caps how many body bytes a health probe reads for content validation
	// (0 = DefaultProbeMaxBytes)
	ProbeMaxBytes int
	// KnownBadPageMarkers are case-insensitive substrings identifying gateway error pages
	// served with HTTP 200 (operator-editable, matched against HTML bodies only)
	KnownBadPageMarkers []string
}

// probeMaxBytes returns the configured probe window with the default applied.
func (c *Config) probeMaxBytes() int {
	if c.ProbeMaxBytes <= 0 {
		return DefaultProbeMaxBytes
	}
	// A window below the sniff floor would make every ranged probe an undersized
	// prefix — the exact state the undersized-206 retry exists to avoid (a few leading
	// bytes of binary media sniff as text and false-broken real artworks). Clamp up
	// rather than reject: a low value is a bandwidth preference, not a semantic choice,
	// and failing the deploy over it helps nobody.
	if c.ProbeMaxBytes < minClassifiableBytes {
		return minClassifiableBytes
	}
	return c.ProbeMaxBytes
}

// Resolver defines the interface for resolving URIs
//
//go:generate mockgen -source=resolver.go -destination=../mocks/uri_resolver.go -package=mocks -mock_names=Resolver=MockURIResolver
type Resolver interface {
	// Resolve resolves the URI to a canonical URL
	// It handles special URL schemes like ipfs://, ar:// and onchfs://
	// Returns the canonical URL
	// It probes candidate gateway URLs with a validated ranged GET (content must look
	// like media, not a directory listing or error page) and returns the first that passes
	// An ipfs:// ref whose gateways all serve directory listings is a directory artifact
	// (application/x-directory): it is retried once with its index.html entry point, so
	// directory artworks resolve to their playable document (feral-file#3482)
	// If no gateway serves valid content, it returns an error
	// Retired HTTP URLs wrap ErrRetiredGatewayReplacement on failure, including
	// unsupported address forms such as IPNS that cannot be rewritten by CID.
	Resolve(ctx context.Context, uri string) (string, error)
}

type resolver struct {
	probe  *contentProbe
	config *Config
}

// NewResolver creates a URI resolver. It shares the URL checker's content-validating
// probe so gateway selection cannot store a URL that direct health checks would reject
// (previously a bare HEAD here accepted directory listings — feral-file#3482).
func NewResolver(httpClient adapter.HTTPClient, io adapter.IO, config *Config) Resolver {
	return &resolver{
		probe: &contentProbe{
			httpClient: httpClient,
			io:         io,
			validator:  NewContentValidator(config.probeMaxBytes(), config.KnownBadPageMarkers),
			maxBytes:   config.probeMaxBytes(),
		},
		config: config,
	}
}

// Resolve selects validated gateways for content-addressed and retired URLs.
// A retired URL without a validated equivalent returns a classified error so
// callers preserve stored data instead of falling back to generic vendor media.
func (r *resolver) Resolve(ctx context.Context, uri string) (string, error) {
	// Handle IPFS URLs
	if cid, ok := strings.CutPrefix(uri, "ipfs://"); ok {
		return FindWorkingIPFSGateway(ctx, r.probe.gatewayProbe, cid, r.config.IPFSGateways)
	}

	// Handle Arweave URLs
	if txID, ok := strings.CutPrefix(uri, "ar://"); ok {
		return FindWorkingArweaveGateway(ctx, r.probe.gatewayProbe, txID, r.config.ArweaveGateways)
	}

	// Handle OnChFS URLs
	if hash, ok := strings.CutPrefix(uri, "onchfs://"); ok {
		return FindWorkingOnChFSGateway(ctx, r.probe.gatewayProbe, hash, r.config.OnChFSGateways)
	}

	// Vendor metadata may already contain an HTTP gateway URL. Treat a retired
	// gateway like its original IPFS reference rather than returning it verbatim.
	if types.IsBrowserIPFSGateway(uri) {
		ok, ref := types.IsIPFSGatewayURL(uri)
		if !ok {
			return "", fmt.Errorf("%w: unsupported gateway address", ErrRetiredGatewayReplacement)
		}
		resolved, err := FindWorkingIPFSGateway(ctx, r.probe.gatewayProbe, ref, r.config.IPFSGateways)
		if err != nil {
			return "", fmt.Errorf("%w: %w", ErrRetiredGatewayReplacement, err)
		}
		return resolved, nil
	}

	// Regular HTTP(S) URL. Moved hosts are rewritten so rebuilds never restore
	// a URL whose origin no longer resolves.
	return types.MigrateFeralFileCDN(uri), nil
}

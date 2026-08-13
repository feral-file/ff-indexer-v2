package uri

import (
	"context"
	"strings"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
)

// DefaultProbeMaxBytes is the default size of the validated probe window. 32KB is enough
// for every container-header check and for directory-listing/error-page markers, while
// keeping a full-corpus sweep's bandwidth bounded.
const DefaultProbeMaxBytes = 32 * 1024

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

	// Regular HTTP(S) URL
	return uri, nil
}

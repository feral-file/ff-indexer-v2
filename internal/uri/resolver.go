package uri

import (
	"context"
	"strings"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
)

// Config holds configuration for the URI resolver
type Config struct {
	// IPFSGateways is the list of IPFS gateways to try
	IPFSGateways []string
	// ArweaveGateways is the list of Arweave gateways to try
	ArweaveGateways []string
	// OnChFSGateways is the list of OnChFS gateways to try
	OnChFSGateways []string
}

// Resolver defines the interface for resolving URIs
//
//go:generate mockgen -source=resolver.go -destination=../mocks/uri_resolver.go -package=mocks -mock_names=Resolver=MockURIResolver
type Resolver interface {
	// Resolve resolves the URI to a canonical URL
	// It handles special URL schemes like ipfs://, ar:// and onchfs://
	// Returns the canonical URL
	// It will make a HEAD request to the URL to check if it is accessible
	// If the URL is not accessible, it will return an error
	Resolve(ctx context.Context, uri string) (string, error)
}

type resolver struct {
	httpClient adapter.HTTPClient
	config     *Config
}

func NewResolver(httpClient adapter.HTTPClient, config *Config) Resolver {
	return &resolver{
		httpClient: httpClient,
		config:     config,
	}
}

func (r *resolver) Resolve(ctx context.Context, uri string) (string, error) {
	// Handle IPFS URLs
	if cid, ok := strings.CutPrefix(uri, "ipfs://"); ok {
		return r.resolveIPFS(ctx, cid)
	}

	// Handle Arweave URLs
	if txID, ok := strings.CutPrefix(uri, "ar://"); ok {
		return r.resolveArweave(ctx, txID)
	}

	// Handle OnChFS URLs
	if hash, ok := strings.CutPrefix(uri, "onchfs://"); ok {
		return r.resolveOnChFS(ctx, hash)
	}

	// Regular HTTP(S) URL
	return uri, nil
}

// resolveIPFS finds a working IPFS gateway for the given CID
func (r *resolver) resolveIPFS(ctx context.Context, cid string) (string, error) {
	return FindWorkingIPFSGateway(ctx, r.httpClient, cid, r.config.IPFSGateways)
}

// resolveArweave finds a working Arweave gateway for the given transaction ID
func (r *resolver) resolveArweave(ctx context.Context, txID string) (string, error) {
	return FindWorkingArweaveGateway(ctx, r.httpClient, txID, r.config.ArweaveGateways)
}

// resolveOnChFS finds a working OnChFS gateway for the given Keccak-256 hash
func (r *resolver) resolveOnChFS(ctx context.Context, hash string) (string, error) {
	return FindWorkingOnChFSGateway(ctx, r.httpClient, hash, r.config.OnChFSGateways)
}

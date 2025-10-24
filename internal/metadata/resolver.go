package metadata

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	logger "github.com/bitmark-inc/autonomy-logger"
	"github.com/gowebpki/jcs"

	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum"
	"github.com/feral-file/ff-indexer-v2/internal/providers/tezos"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

// NormalizedMetadata represents the normalized metadata
type NormalizedMetadata struct {
	Raw       map[string]interface{} `json:"raw"`
	Image     string                 `json:"image"`
	Animation string                 `json:"animation"`
	Name      string                 `json:"name"`
	Artists   schema.Artists         `json:"artists"`
}

// RawHash returns the hash of the raw metadata and the raw metadata itself
func (n *NormalizedMetadata) RawHash() ([]byte, []byte, error) {
	metadataJSON, err := json.Marshal(n.Raw)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal metadata: %w", err)
	}

	canonicalizedMetadata, err := jcs.Transform(metadataJSON)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to canonicalize metadata: %w", err)
	}
	hash := sha256.Sum256(canonicalizedMetadata)
	return hash[:], metadataJSON, nil
}

// Resolver defines the interface for resolving metadata from a tokenCID
//
//go:generate mockgen -source=resolver.go -destination=../mocks/metadata_resolver.go -package=mocks -mock_names=Resolver=MockMetadataResolver
type Resolver interface {
	Resolve(ctx context.Context, tokenCID domain.TokenCID) (*NormalizedMetadata, error)
}

type resolver struct {
	ethClient  ethereum.EthereumClient
	tzClient   tezos.TzKTClient
	httpClient adapter.HTTPClient
	json       adapter.JSON
	clock      adapter.Clock
}

func NewResolver(ethClient ethereum.EthereumClient, tzClient tezos.TzKTClient, httpClient adapter.HTTPClient, json adapter.JSON, clock adapter.Clock) Resolver {
	return &resolver{
		ethClient:  ethClient,
		tzClient:   tzClient,
		httpClient: httpClient,
		json:       json,
		clock:      clock,
	}
}

func (r *resolver) Resolve(ctx context.Context, tokenCID domain.TokenCID) (*NormalizedMetadata, error) {
	_, standard, contractAddress, tokenNumber := tokenCID.Parse()
	var metadataURI string
	var err error

	switch standard {
	case domain.StandardERC721:
		metadataURI, err = r.ethClient.ERC721TokenURI(ctx, contractAddress, tokenNumber)
	case domain.StandardERC1155:
		metadataURI, err = r.ethClient.ERC1155URI(ctx, contractAddress, tokenNumber)
	case domain.StandardFA2:
		// For Tezos FA2, TzKT API provides metadata directly
		metadata, err := r.tzClient.GetTokenMetadata(ctx, contractAddress, tokenNumber)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch FA2 metadata: %w", err)
		}
		if metadata == nil {
			return nil, nil
		}

		// Normalize the metadata based on TZIP-21
		return r.normalizeTZIP21Metadata(ctx, metadata)
	default:
		return nil, fmt.Errorf("unsupported standard: %s", standard)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to fetch metadata URI: %w", err)
	}

	// Process the URI and fetch the actual metadata
	processedURI := processMetadataURI(metadataURI, tokenNumber)
	metadata, err := r.fetchMetadataFromURI(ctx, processedURI)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch metadata from URI %s: %w", processedURI, err)
	}

	// Normalize the metadata based on OpenSea Metadata Standard
	return r.normalizeOpenSeaMetadataStandard(metadata), nil
}

// normalizeTZIP21Metadata normalizes the metadata follow the TZIP21 specs
// https://tzip.tezosagora.org/proposal/tzip-21/
func (r *resolver) normalizeTZIP21Metadata(ctx context.Context, metadata map[string]interface{}) (*NormalizedMetadata, error) {
	var displayUri string
	var artifactUri string
	var name string
	var creators []string
	if d, ok := metadata["displayUri"].(string); ok {
		displayUri = d
	}
	if a, ok := metadata["artifactUri"].(string); ok {
		artifactUri = a
	}
	if n, ok := metadata["name"].(string); ok {
		name = n
	}
	if c, ok := metadata["creators"]; ok {
		// Marshal to json
		cj, err := r.json.Marshal(c)
		if err != nil {
			return nil, err
		}

		// Unmarshal to creator array
		err = r.json.Unmarshal(cj, &creators)
		if err != nil {
			logger.WarnWithContext(ctx, "fail to unmarshal creators metadata", zap.Error(err), zap.Any("creators", c))
		}
	}

	// Convert creators to artists
	var artists schema.Artists
	for _, creator := range creators {
		artists = append(artists, schema.Artist{Name: creator})
	}

	normalizedMetadata := &NormalizedMetadata{
		Raw:       metadata,
		Image:     uriToGateway(displayUri),
		Animation: uriToGateway(artifactUri),
		Name:      name,
		Artists:   artists,
	}

	return normalizedMetadata, nil
}

// normalizeOpenSeaMetadataStandard normalizes the metadata follow the OpenSea metadata standard
// https://docs.opensea.io/docs/metadata-standards
func (r *resolver) normalizeOpenSeaMetadataStandard(metadata map[string]interface{}) *NormalizedMetadata {
	var image string
	var animationURL string
	var name string
	var artists schema.Artists
	if i, ok := metadata["image"].(string); ok {
		image = i
	}
	if a, ok := metadata["animation_url"].(string); ok {
		animationURL = a
	}
	if n, ok := metadata["name"].(string); ok {
		name = n
	}

	// ArtBlocks uses generator_url for animation URL
	if g, ok := metadata["generator_url"].(string); ok {
		animationURL = g
	}

	// Resolve the artist from the metadata
	if a := resolveArtist(metadata); a != "" {
		artists = schema.Artists{{Name: a}}
	}

	normalizedMetadata := &NormalizedMetadata{
		Raw:       metadata,
		Image:     uriToGateway(image),
		Animation: uriToGateway(animationURL),
		Name:      name,
		Artists:   artists,
	}

	return normalizedMetadata
}

// resolveArtist resolves the artist from the metadata
func resolveArtist(metadata map[string]interface{}) string {
	// Resolve from `artist` field
	artist, ok := metadata["artist"].(string)
	if ok {
		return artist
	}

	// Resolve from `traits` field
	traits, ok := metadata["traits"].([]interface{})
	if ok {
		for _, trait := range traits {
			traitMap, ok := trait.(map[string]interface{})
			if !ok {
				continue
			}

			traitType, ok := traitMap["trait_type"].(string)
			if !ok || (traitType != "artist" && traitType != "Artist" && traitType != "Creator" && traitType != "creator") {
				continue
			}

			artist, ok = traitMap["value"].(string)
			if ok {
				return artist
			}
		}
	}

	// Resolve from `collection_name` field
	collectionName, ok := metadata["collection_name"].(string)
	if ok && collectionName != "" {
		parts := strings.Split(collectionName, " by ")
		if len(parts) > 1 {
			artist = parts[1]
			return artist
		}
	}

	// Resolve from `createdBy` field
	artist, ok = metadata["createdBy"].(string)
	if ok {
		return artist
	}

	// Resolve from `created_by` field
	artist, ok = metadata["created_by"].(string)
	if ok {
		return artist
	}

	// Resolve from `creator` field
	creator, ok := metadata["creator"].(string)
	if ok {
		return creator
	}

	return ""
}

// processMetadataURI processes the metadata URI to handle different protocols and formats
// For Ethereum:
// - URI can be https://, ipfs://, ar://, or data:
// - If HTTP includes /ipfs/, fallback to ipfs:// to avoid private gateway
// - For ipfs:// or ar://, use well-known gateways
func processMetadataURI(uri, tokenNumber string) string {
	// Replace {id} placeholder with actual token number (ERC1155 standard)
	uri = strings.ReplaceAll(uri, "{id}", tokenNumber)

	// If the URI contains /ipfs/ in an HTTP URL, convert to ipfs:// protocol
	if strings.HasPrefix(uri, "http") && strings.Contains(uri, "/ipfs/") {
		// Extract the IPFS hash from the URL
		parts := strings.Split(uri, "/ipfs/")
		if len(parts) > 1 {
			uri = "ipfs://" + parts[1]
		}
	}

	return uri
}

// fetchMetadataFromURI fetches metadata from a given URI, handling different protocols
func (r *resolver) fetchMetadataFromURI(ctx context.Context, uri string) (map[string]interface{}, error) {
	switch {
	case strings.HasPrefix(uri, "data:"):
		return r.parseDataURI(uri)
	case strings.HasPrefix(uri, "ipfs://"):
		return r.fetchFromIPFS(ctx, uri)
	case strings.HasPrefix(uri, "ar://"):
		return r.fetchFromArweave(ctx, uri)
	case strings.HasPrefix(uri, "http://") || strings.HasPrefix(uri, "https://"):
		return r.fetchFromHTTP(ctx, uri)
	default:
		return nil, fmt.Errorf("unsupported URI scheme: %s", uri)
	}
}

// parseDataURI parses a data URI and returns the metadata
func (r *resolver) parseDataURI(uri string) (map[string]interface{}, error) {
	// data:application/json;base64,<encoded data>
	// or data:application/json,<json data>
	if !strings.HasPrefix(uri, "data:") {
		return nil, fmt.Errorf("invalid data URI")
	}

	parts := strings.SplitN(uri[5:], ",", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid data URI format")
	}

	dataType := parts[0]
	data := parts[1]

	if strings.Contains(dataType, "base64") {
		// Decode base64
		decoded, err := base64.StdEncoding.DecodeString(data)
		if err != nil {
			return nil, fmt.Errorf("failed to decode base64: %w", err)
		}
		data = string(decoded)
	}

	var metadata map[string]interface{}
	if err := r.json.Unmarshal([]byte(data), &metadata); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	return metadata, nil
}

// fetchFromIPFS fetches metadata from IPFS using multiple gateways in parallel
func (r *resolver) fetchFromIPFS(ctx context.Context, uri string) (map[string]interface{}, error) {
	// Extract the IPFS hash
	ipfsHash := strings.TrimPrefix(uri, "ipfs://")

	// Well-known IPFS gateways
	gateways := []string{
		"https://ipfs.io/ipfs/",
		"https://cloudflare-ipfs.com/ipfs/",
		"https://nftstorage.link/ipfs/",
		"https://gateway.pinata.cloud/ipfs/",
		"https://dweb.link/ipfs/",
		"https://ipfs.feralfile.com/ipfs/",
	}

	// Try gateways in parallel, return first successful result
	type result struct {
		metadata map[string]interface{}
		err      error
	}

	results := make(chan result, len(gateways))

	for _, gateway := range gateways {
		go func(gw string) {
			url := gw + ipfsHash
			metadata, err := r.fetchFromHTTP(ctx, url)
			results <- result{metadata: metadata, err: err}
		}(gateway)
	}

	// Wait for first successful result
	for range gateways {
		res := <-results
		if res.err == nil {
			return res.metadata, nil
		}
	}

	return nil, fmt.Errorf("failed to fetch from all IPFS gateways")
}

// fetchFromArweave fetches metadata from Arweave using multiple gateways in parallel
func (r *resolver) fetchFromArweave(ctx context.Context, uri string) (map[string]interface{}, error) {
	// Extract the Arweave transaction ID
	arTxID := strings.TrimPrefix(uri, "ar://")

	// Well-known Arweave gateways
	gateways := []string{
		"https://arweave.net/",
		"https://arweave.dev/",
	}

	// Try gateways in parallel, return first successful result
	type result struct {
		metadata map[string]interface{}
		err      error
	}

	results := make(chan result, len(gateways))

	for _, gateway := range gateways {
		go func(gw string) {
			url := gw + arTxID
			metadata, err := r.fetchFromHTTP(ctx, url)
			results <- result{metadata: metadata, err: err}
		}(gateway)
	}

	// Wait for first successful result
	for range gateways {
		res := <-results
		if res.err == nil {
			return res.metadata, nil
		}
	}

	return nil, fmt.Errorf("failed to fetch from all Arweave gateways")
}

// fetchFromHTTP fetches metadata from an HTTP(S) URL
func (r *resolver) fetchFromHTTP(ctx context.Context, url string) (map[string]interface{}, error) {
	var metadata map[string]interface{}
	err := r.httpClient.Get(ctx, url, &metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch URL: %w", err)
	}

	return metadata, nil
}

// uriToGateway converts a URI to a gateway URL
func uriToGateway(uri string) string {
	if after, ok := strings.CutPrefix(uri, "ipfs://"); ok {
		return fmt.Sprintf("%s/ipfs/%s", domain.DEFAULT_IPFS_GATEWAY, after)
	}
	if after, ok := strings.CutPrefix(uri, "ar://"); ok {
		return fmt.Sprintf("%s/%s", domain.DEFAULT_ARWEAVE_GATEWAY, after)
	}
	return uri
}

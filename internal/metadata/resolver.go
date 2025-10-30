package metadata

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"

	logger "github.com/bitmark-inc/autonomy-logger"
	"github.com/gowebpki/jcs"

	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum"
	"github.com/feral-file/ff-indexer-v2/internal/providers/tezos"
	"github.com/feral-file/ff-indexer-v2/internal/store"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
	"github.com/feral-file/ff-indexer-v2/internal/uri"
)

// Artist represents an artist/creator with their decentralized identifier and name
type Artist struct {
	DID  domain.DID `json:"did"`
	Name string     `json:"name"`
}

// Publisher represents the publisher of the token
type Publisher struct {
	Name *PublisherName `json:"name,omitempty"`
	URL  *string        `json:"url,omitempty"`
}

// NormalizedMetadata represents the normalized metadata
type NormalizedMetadata struct {
	Raw         map[string]interface{} `json:"raw"`
	Image       string                 `json:"image"`
	Animation   string                 `json:"animation"`
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	Artists     []Artist               `json:"artists"`
	Publisher   *Publisher             `json:"publisher"`
	MimeType    *string                `json:"mime_type,omitempty"`
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

// PublisherChainConfig represents chain-specific configuration for a publisher
type PublisherChainConfig struct {
	DeployerAddresses   []string `json:"deployer_addresses"`
	CollectionAddresses []string `json:"collection_addresses"`
}

type PublisherName string

const (
	// PublisherNameArtBlocks represents the name of the Art Blocks publisher
	PublisherNameArtBlocks PublisherName = "Art Blocks"
	// PublisherNameFXHash represents the name of the fxhash publisher
	PublisherNameFXHash PublisherName = "fxhash"
	// PublisherNameFeralFile represents the name of the Feral File publisher
	PublisherNameFeralFile PublisherName = "Feral File"
	// PublisherNameFoundation represents the name of the Foundation publisher
	PublisherNameFoundation PublisherName = "Foundation"
	// PublisherNameSuperRare represents the name of the SuperRare publisher
	PublisherNameSuperRare PublisherName = "SuperRare"
)

// PublisherInfo represents a publisher entry in the registry
type PublisherInfo struct {
	Name   PublisherName                   `json:"name"`
	URL    string                          `json:"url"`
	Chains map[string]PublisherChainConfig `json:"chains"` // key is chain ID like "eip155:1" or "tez:mainnet"
}

// PublisherRegistryData represents the structure of the registry JSON file
type PublisherRegistryData struct {
	Version    int               `json:"version"`
	MinBlocks  map[string]uint64 `json:"min_blocks,omitempty"` // Optional: earliest block to search per chain (e.g., "eip155:1": 13492497)
	Publishers []PublisherInfo   `json:"publishers"`
}

// PublisherRegistry is a registry of publishers with in-memory lookup
type PublisherRegistry struct {
	data *PublisherRegistryData
	// Fast lookup maps: chain:contract -> publisher
	collectionToPublisher map[string]*PublisherInfo
	// chain:contract -> deployer lookup (for contracts deployed by known deployers)
	deployerToPublisher map[string]*PublisherInfo
}

// PublisherNameToVendor converts a publisher name to a vendor
func PublisherNameToVendor(publisherName PublisherName) schema.Vendor {
	switch publisherName {
	case PublisherNameArtBlocks:
		return schema.VendorArtBlocks
	case PublisherNameFXHash:
		return schema.VendorFXHash
	case PublisherNameFeralFile:
		return schema.VendorFeralFile
	case PublisherNameFoundation:
		return schema.VendorFoundation
	case PublisherNameSuperRare:
		return schema.VendorSuperRare
	default:
		return ""
	}
}

// Resolver defines the interface for resolving metadata from a tokenCID
//
//go:generate mockgen -source=resolver.go -destination=../mocks/metadata_resolver.go -package=mocks -mock_names=Resolver=MockMetadataResolver
type Resolver interface {
	// LoadDeployerCacheFromDB loads the deployer cache from the database
	LoadDeployerCacheFromDB(ctx context.Context) error

	// Resolve resolves the metadata for a given token CID
	Resolve(ctx context.Context, tokenCID domain.TokenCID) (*NormalizedMetadata, error)
}

type resolver struct {
	ethClient         ethereum.EthereumClient
	tzClient          tezos.TzKTClient
	httpClient        adapter.HTTPClient
	uriResolver       uri.Resolver
	json              adapter.JSON
	clock             adapter.Clock
	store             store.Store
	registry          *PublisherRegistry
	deployerCache     map[string]string // key: "chain:contract", value: deployer address
	deployerCacheLock sync.RWMutex
}

func NewResolver(ethClient ethereum.EthereumClient, tzClient tezos.TzKTClient, httpClient adapter.HTTPClient, uriResolver uri.Resolver, json adapter.JSON, clock adapter.Clock, store store.Store, registry *PublisherRegistry) Resolver {
	return &resolver{
		ethClient:     ethClient,
		tzClient:      tzClient,
		httpClient:    httpClient,
		uriResolver:   uriResolver,
		json:          json,
		clock:         clock,
		store:         store,
		registry:      registry,
		deployerCache: make(map[string]string),
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
		return r.normalizeTZIP21Metadata(ctx, tokenCID, metadata)
	default:
		return nil, fmt.Errorf("unsupported standard: %s", standard)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to fetch metadata URI: %w", err)
	}

	// Process the URI and fetch the actual metadata
	if standard == domain.StandardERC1155 {
		metadataURI = strings.ReplaceAll(metadataURI, "{id}", tokenNumber) // ERC1155 placeholder for token number
	}
	metadata, err := r.fetchMetadataFromURI(ctx, metadataURI)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch metadata from URI %s: %w", metadataURI, err)
	}

	// Normalize the metadata based on OpenSea Metadata Standard
	return r.normalizeOpenSeaMetadataStandard(ctx, tokenCID, metadata), nil
}

// normalizeTZIP21Metadata normalizes the metadata follow the TZIP21 specs
// https://tzip.tezosagora.org/proposal/tzip-21/
func (r *resolver) normalizeTZIP21Metadata(ctx context.Context, tokenCID domain.TokenCID, metadata map[string]interface{}) (*NormalizedMetadata, error) {
	// Parse the token CID to get the chain ID
	chainID, _, _, _ := tokenCID.Parse()

	var displayUri string
	var artifactUri string
	var name string
	var description string
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
	if d, ok := metadata["description"].(string); ok {
		description = d
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
	var artists []Artist
	for _, creator := range creators {
		name := creator
		if n := resolveArtistName(metadata); n != "" {
			name = n
		}

		artists = append(artists, Artist{DID: domain.NewDID(creator, chainID), Name: name})
	}

	// Resolve the publisher from the token CID
	publisher := r.resolvePublisher(ctx, tokenCID)

	normalizedMetadata := &NormalizedMetadata{
		Raw:         metadata,
		Image:       uriToGateway(displayUri),
		Animation:   uriToGateway(artifactUri),
		Name:        name,
		Description: description,
		Artists:     artists,
		Publisher:   publisher,
	}

	// Detect mime type from animation_url or image_url
	normalizedMetadata.MimeType = detectMimeType(ctx, r.httpClient, r.uriResolver, &normalizedMetadata.Animation, &normalizedMetadata.Image)

	return normalizedMetadata, nil
}

// normalizeOpenSeaMetadataStandard normalizes the metadata follow the OpenSea metadata standard
// https://docs.opensea.io/docs/metadata-standards
func (r *resolver) normalizeOpenSeaMetadataStandard(ctx context.Context, tokenCID domain.TokenCID, metadata map[string]interface{}) *NormalizedMetadata {
	var image string
	var animationURL string
	var name string
	var artists []Artist
	var description string
	if i, ok := metadata["image"].(string); ok {
		image = i
	}
	if a, ok := metadata["animation_url"].(string); ok {
		animationURL = a
	}
	if n, ok := metadata["name"].(string); ok {
		name = n
	}
	if d, ok := metadata["description"].(string); ok {
		description = d
	}

	// Resolve the artist from the metadata
	if a := resolveArtistName(metadata); a != "" {
		artists = []Artist{{Name: a}}
	}

	// Resolve the publisher from the token CID
	publisher := r.resolvePublisher(ctx, tokenCID)

	normalizedMetadata := &NormalizedMetadata{
		Raw:         metadata,
		Image:       uriToGateway(image),
		Animation:   uriToGateway(animationURL),
		Name:        name,
		Description: description,
		Artists:     artists,
		Publisher:   publisher,
	}

	// Detect mime type from animation_url or image_url
	normalizedMetadata.MimeType = detectMimeType(ctx, r.httpClient, r.uriResolver, &normalizedMetadata.Animation, &normalizedMetadata.Image)

	return normalizedMetadata
}

// resolveArtistName resolves the artist from the metadata
func resolveArtistName(metadata map[string]interface{}) string {
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

// fetchMetadataFromURI fetches metadata from a given URI, handling different protocols
func (r *resolver) fetchMetadataFromURI(ctx context.Context, uri string) (map[string]interface{}, error) {
	switch {
	case strings.HasPrefix(uri, "data:"):
		return r.parseDataURI(uri)
	case strings.HasPrefix(uri, "ipfs://"), strings.HasPrefix(uri, "ar://"), strings.HasPrefix(uri, "http://"), strings.HasPrefix(uri, "https://"):
		// Use URI resolver to find a working gateway
		resolvedURL, err := r.uriResolver.Resolve(ctx, uri)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve URI: %w", err)
		}
		// Fetch metadata from the resolved URL
		return r.fetchFromHTTP(ctx, resolvedURL)
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

// LoadDeployerCacheFromDB loads all cached deployer addresses from the database
func (r *resolver) LoadDeployerCacheFromDB(ctx context.Context) error {
	if r.store == nil {
		return nil
	}

	// Get all deployer cache entries from DB
	cacheEntries, err := r.store.GetAllKeyValuesByPrefix(ctx, "deployer:")
	if err != nil {
		return fmt.Errorf("failed to load deployer cache from DB: %w", err)
	}

	r.deployerCacheLock.Lock()
	defer r.deployerCacheLock.Unlock()

	for key, value := range cacheEntries {
		// Key format: "deployer:chain:contract" -> extract "chain:contract"
		cacheKey := strings.TrimPrefix(key, "deployer:")
		r.deployerCache[cacheKey] = value
	}

	logger.Info("Loaded deployer cache from DB", zap.Int("count", len(cacheEntries)))
	return nil
}

// getContractDeployer gets the deployer address for a contract with caching
// Returns the deployer address (may be empty if not found), or error if lookup failed
func (r *resolver) getContractDeployer(ctx context.Context, chainID domain.Chain, contractAddress string) (string, error) {
	cacheKey := fmt.Sprintf("%s:%s", strings.ToLower(string(chainID)), strings.ToLower(contractAddress))

	// Check in-memory cache first (includes both successful and failed lookups)
	r.deployerCacheLock.RLock()
	if deployer, ok := r.deployerCache[cacheKey]; ok {
		r.deployerCacheLock.RUnlock()
		// Cache hit - return immediately without blockchain lookup
		return deployer, nil
	}
	r.deployerCacheLock.RUnlock()

	// Determine minBlock from registry if available
	minBlock := uint64(0)
	if r.registry != nil && r.registry.data.MinBlocks != nil {
		// Look up the minimum block for this chain
		if mb, ok := r.registry.data.MinBlocks[string(chainID)]; ok {
			minBlock = mb
		}
	}

	// Fetch from blockchain
	var deployer string
	var err error

	switch chainID {
	case domain.ChainEthereumMainnet, domain.ChainEthereumSepolia:
		deployer, err = r.ethClient.GetContractDeployer(ctx, contractAddress, minBlock)
	case domain.ChainTezosMainnet, domain.ChainTezosGhostnet:
		deployer, err = r.tzClient.GetContractDeployer(ctx, contractAddress)
	default:
		return "", fmt.Errorf("unsupported chain: %s", chainID)
	}

	if err != nil {
		return "", fmt.Errorf("failed to get contract deployer: %w", err)
	}

	logger.InfoWithContext(ctx, "Fetched deployer from blockchain",
		zap.String("chain", string(chainID)),
		zap.String("contract", contractAddress),
		zap.String("deployer", deployer),
		zap.Bool("found", deployer != ""),
		zap.Uint64("min_block", minBlock))

	// Cache the result in memory (even if empty - this prevents repeated failed lookups)
	r.deployerCacheLock.Lock()
	r.deployerCache[cacheKey] = deployer
	r.deployerCacheLock.Unlock()

	// Persist to database (even if empty - critical for persistence across restarts)
	dbKey := fmt.Sprintf("deployer:%s", cacheKey)
	if err := r.store.SetKeyValue(ctx, dbKey, deployer); err != nil {
		logger.WarnWithContext(ctx, "failed to cache deployer in DB",
			zap.Error(err),
			zap.String("key", dbKey))
	}

	return deployer, nil
}

// LoadPublisherRegistry loads the publisher registry from a JSON file
func LoadPublisherRegistry(filePath string) (*PublisherRegistry, error) {
	// Read the file using the absolute path
	data, err := os.ReadFile(filePath) //nolint:gosec,G304 // This should be a trusted file
	if err != nil {
		return nil, fmt.Errorf("failed to read registry file: %w", err)
	}

	// Parse JSON
	var registryData PublisherRegistryData
	if err := json.Unmarshal(data, &registryData); err != nil {
		return nil, fmt.Errorf("failed to parse registry JSON: %w", err)
	}

	// Build lookup maps
	registry := &PublisherRegistry{
		data:                  &registryData,
		collectionToPublisher: make(map[string]*PublisherInfo),
		deployerToPublisher:   make(map[string]*PublisherInfo),
	}

	for i := range registryData.Publishers {
		publisher := &registryData.Publishers[i]
		for chainID, chainConfig := range publisher.Chains {
			// Normalize chain ID format
			normalizedChainID := strings.ToLower(chainID)

			// Index collection addresses
			for _, addr := range chainConfig.CollectionAddresses {
				normalizedAddr := strings.ToLower(addr)
				key := fmt.Sprintf("%s:%s", normalizedChainID, normalizedAddr)
				registry.collectionToPublisher[key] = publisher
			}

			// Index deployer addresses
			for _, addr := range chainConfig.DeployerAddresses {
				normalizedAddr := strings.ToLower(addr)
				key := fmt.Sprintf("%s:%s", normalizedChainID, normalizedAddr)
				registry.deployerToPublisher[key] = publisher
			}
		}
	}

	return registry, nil
}

// lookupPublisherByCollection looks up a publisher by collection address
func (r *PublisherRegistry) lookupPublisherByCollection(chainID domain.Chain, contractAddress string) *PublisherInfo {
	key := fmt.Sprintf("%s:%s", strings.ToLower(string(chainID)), strings.ToLower(contractAddress))
	return r.collectionToPublisher[key]
}

// lookupPublisherByDeployer looks up a publisher by deployer address
func (r *PublisherRegistry) lookupPublisherByDeployer(chainID domain.Chain, deployerAddress string) *PublisherInfo {
	key := fmt.Sprintf("%s:%s", strings.ToLower(string(chainID)), strings.ToLower(deployerAddress))
	return r.deployerToPublisher[key]
}

// resolvePublisher resolves the publisher from the metadata
func (r *resolver) resolvePublisher(ctx context.Context, tokenCID domain.TokenCID) *Publisher {
	if r.registry == nil {
		return nil
	}

	chainID, _, contractAddress, _ := tokenCID.Parse()

	// First, check if the contract is in the collection addresses
	if publisher := r.registry.lookupPublisherByCollection(chainID, contractAddress); publisher != nil {
		name := publisher.Name
		url := publisher.URL
		return &Publisher{
			Name: &name,
			URL:  &url,
		}
	}

	// Second, get the deployer and check if it's in the deployer addresses
	deployer, err := r.getContractDeployer(ctx, chainID, contractAddress)
	if err != nil {
		logger.WarnWithContext(ctx, "failed to get contract deployer",
			zap.Error(err),
			zap.String("chain", string(chainID)),
			zap.String("contract", contractAddress))
		return nil
	}

	if deployer == "" {
		return nil
	}

	// Check if deployer is in registry
	if publisher := r.registry.lookupPublisherByDeployer(chainID, deployer); publisher != nil {
		name := publisher.Name
		url := publisher.URL
		return &Publisher{
			Name: &name,
			URL:  &url,
		}
	}

	return nil
}

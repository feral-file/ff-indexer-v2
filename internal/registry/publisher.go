package registry

import (
	"fmt"
	"strings"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
)

// PublisherRegistry defines the interface for publisher operations
//
//go:generate mockgen -source=publisher.go -destination=../mocks/publisher_registry.go -package=mocks -mock_names=PublisherRegistry=MockPublisherRegistry
type PublisherRegistry interface {
	// LookupPublisherByCollection looks up a publisher by collection address
	LookupPublisherByCollection(chainID domain.Chain, contractAddress string) *PublisherInfo

	// LookupPublisherByDeployer looks up a publisher by deployer address
	LookupPublisherByDeployer(chainID domain.Chain, deployerAddress string) *PublisherInfo

	// GetMinBlock returns the minimum block number used to search for deployer addresses for a given chain, if configured
	GetMinBlock(chainID domain.Chain) (uint64, bool)

	// IsSharedContract reports whether the registry lists the contract as one that many
	// artists mint on (a platform's shared minting contract). Such a contract is never a
	// release by itself; see SharedContract.
	IsSharedContract(chainID domain.Chain, contractAddress string) bool
}

// PublisherName represents a publisher name constant
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

// PublisherChainConfig represents chain-specific configuration for a publisher
type PublisherChainConfig struct {
	DeployerAddresses   []string `json:"deployer_addresses"`
	CollectionAddresses []string `json:"collection_addresses"`
}

// PublisherInfo represents a publisher entry in the registry
type PublisherInfo struct {
	Name   PublisherName                   `json:"name"`
	URL    string                          `json:"url"`
	Chains map[string]PublisherChainConfig `json:"chains"` // key is chain ID like "eip155:1" or "tez:mainnet"
}

// SharedContract is a contract that many artists mint on: a platform's shared minting
// contract (SuperRare, Foundation, Rarible, Zora, the OpenSea Shared Storefront, hic et
// nunc's OBJKT). Such a contract is never a series by itself. A collection-level vendor
// (OpenSea, objkt) files every token on it under one collection, so a release taken from
// that vendor would group unrelated works by different artists as one series; the
// enhancer drops it (ff-indexer-v2 #176). A release that names a real series inside the
// contract (an Art Blocks project, an fxhash generative token) still stands.
//
// The list is independent of the publisher entries on purpose: marking a contract shared
// must not attribute a publisher to its tokens, which the API exposes.
type SharedContract struct {
	// Name is a human label for maintainers (JSON has no comments); lookups ignore it.
	Name    string `json:"name"`
	Address string `json:"address"`
}

// PublisherRegistryData represents the structure of the registry JSON file
type PublisherRegistryData struct {
	Version   int               `json:"version"`
	MinBlocks map[string]uint64 `json:"min_blocks,omitempty"` // Optional: earliest block to search per chain (e.g., "eip155:1": 13492497)
	// SharedContracts optionally lists shared minting contracts per chain ID (e.g. "eip155:1").
	SharedContracts map[string][]SharedContract `json:"shared_contracts,omitempty"`
	Publishers      []PublisherInfo             `json:"publishers"`
}

// publisherRegistry is the internal implementation of PublisherRegistry interface
type publisherRegistry struct {
	data *PublisherRegistryData
	// Fast lookup maps: chain:contract -> publisher
	collectionToPublisher map[string]*PublisherInfo
	// chain:contract -> deployer lookup (for contracts deployed by known deployers)
	deployerToPublisher map[string]*PublisherInfo
	// chain:contract set of shared minting contracts
	sharedContracts map[string]struct{}
}

// PublisherRegistryLoader defines the interface for loading publisher registries from files
//
//go:generate mockgen -source=publisher.go -destination=../mocks/publisher_registry.go -package=mocks -mock_names=PublisherRegistryLoader=MockPublisherRegistryLoader
type PublisherRegistryLoader interface {
	// Load loads the publisher registry from a JSON file
	Load(filePath string) (PublisherRegistry, error)
}

// publisherRegistryLoader is the internal implementation of PublisherRegistryLoader interface
type publisherRegistryLoader struct {
	fs   adapter.FileSystem
	json adapter.JSON
}

// NewPublisherRegistryLoader creates a new PublisherRegistryLoader with injected dependencies
func NewPublisherRegistryLoader(fs adapter.FileSystem, json adapter.JSON) PublisherRegistryLoader {
	return &publisherRegistryLoader{
		fs:   fs,
		json: json,
	}
}

// Load loads the publisher registry from a JSON file
func (l *publisherRegistryLoader) Load(filePath string) (PublisherRegistry, error) {
	// Read the file using the file system interface
	data, err := l.fs.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read registry file: %w", err)
	}

	// Parse JSON using the JSON adapter
	var registryData PublisherRegistryData
	if err := l.json.Unmarshal(data, &registryData); err != nil {
		return nil, fmt.Errorf("failed to parse registry JSON: %w", err)
	}

	// Build lookup maps
	registry := &publisherRegistry{
		data:                  &registryData,
		collectionToPublisher: make(map[string]*PublisherInfo),
		deployerToPublisher:   make(map[string]*PublisherInfo),
		sharedContracts:       sharedContractSet(registryData.SharedContracts),
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

// sharedContractSet indexes the registry's shared contracts by the same lowercased
// "chain:address" key the publisher lookups use.
func sharedContractSet(shared map[string][]SharedContract) map[string]struct{} {
	set := make(map[string]struct{})
	for chainID, contracts := range shared {
		for _, contract := range contracts {
			set[strings.ToLower(chainID)+":"+strings.ToLower(contract.Address)] = struct{}{}
		}
	}
	return set
}

// LookupPublisherByCollection looks up a publisher by collection address
func (r *publisherRegistry) LookupPublisherByCollection(chainID domain.Chain, contractAddress string) *PublisherInfo {
	if r == nil {
		return nil
	}
	key := fmt.Sprintf("%s:%s", strings.ToLower(string(chainID)), strings.ToLower(contractAddress))
	return r.collectionToPublisher[key]
}

// LookupPublisherByDeployer looks up a publisher by deployer address
func (r *publisherRegistry) LookupPublisherByDeployer(chainID domain.Chain, deployerAddress string) *PublisherInfo {
	if r == nil {
		return nil
	}
	key := fmt.Sprintf("%s:%s", strings.ToLower(string(chainID)), strings.ToLower(deployerAddress))
	return r.deployerToPublisher[key]
}

// GetMinBlock returns the minimum block number used to search for deployer addresses for a given chain, if configured
func (r *publisherRegistry) GetMinBlock(chainID domain.Chain) (uint64, bool) {
	if r == nil || r.data == nil || r.data.MinBlocks == nil {
		return 0, false
	}
	minBlock, ok := r.data.MinBlocks[string(chainID)]
	return minBlock, ok
}

// IsSharedContract reports whether the contract is a listed shared minting contract.
// Lookup is case-insensitive like the publisher lookups; a nil registry lists none.
func (r *publisherRegistry) IsSharedContract(chainID domain.Chain, contractAddress string) bool {
	if r == nil {
		return false
	}
	_, ok := r.sharedContracts[strings.ToLower(string(chainID))+":"+strings.ToLower(contractAddress)]
	return ok
}

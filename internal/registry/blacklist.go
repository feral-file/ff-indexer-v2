package registry

import (
	"fmt"
	"strings"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
)

// BlacklistRegistry defines the interface for blacklist operations
//
//go:generate mockgen -source=blacklist.go -destination=../mocks/blacklist_registry.go -package=mocks -mock_names=BlacklistRegistry=MockBlacklistRegistry
type BlacklistRegistry interface {
	// IsBlacklisted checks if a contract address is blacklisted for a given chain
	IsBlacklisted(chainID domain.Chain, contractAddress string) bool

	// IsTokenCIDBlacklisted checks if a TokenCID is blacklisted
	IsTokenCIDBlacklisted(tokenCID domain.TokenCID) bool
}

// BlacklistData represents the structure of the blacklist.json file
// Key format: "chain_id" -> list of contract addresses
type BlacklistData map[string][]string

// blacklistRegistry is the internal implementation of Registry interface
type blacklistRegistry struct {
	data *BlacklistData
	// Fast lookup map: "chain:contract" -> true
	contracts map[string]bool
}

// BlacklistRegistryLoader defines the interface for loading blacklist registries from files
//
//go:generate mockgen -source=blacklist.go -destination=../mocks/blacklist_registry.go -package=mocks -mock_names=BlacklistRegistryLoader=MockBlacklistRegistryLoader
type BlacklistRegistryLoader interface {
	// Load loads the blacklist registry from a JSON file
	Load(filePath string) (BlacklistRegistry, error)
}

// blacklistRegistryLoader is the internal implementation of BlacklistRegistryLoader interface
type blacklistRegistryLoader struct {
	fs   adapter.FileSystem
	json adapter.JSON
}

// NewBlacklistRegistryLoader creates a new BlacklistRegistryLoader with injected dependencies
func NewBlacklistRegistryLoader(fs adapter.FileSystem, json adapter.JSON) BlacklistRegistryLoader {
	return &blacklistRegistryLoader{
		fs:   fs,
		json: json,
	}
}

// Load loads the blacklist registry from a JSON file
func (l *blacklistRegistryLoader) Load(filePath string) (BlacklistRegistry, error) {
	// Read the file using the file system interface
	data, err := l.fs.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read blacklist file: %w", err)
	}

	// Parse JSON using the JSON adapter
	var blacklistData BlacklistData
	if err := l.json.Unmarshal(data, &blacklistData); err != nil {
		return nil, fmt.Errorf("failed to parse blacklist JSON: %w", err)
	}

	// Build lookup map
	bl := &blacklistRegistry{
		data:      &blacklistData,
		contracts: make(map[string]bool),
	}

	for chainID, addresses := range blacklistData {
		// Normalize chain ID format
		normalizedChainID := strings.ToLower(chainID)

		// Index contract addresses
		for _, addr := range addresses {
			normalizedAddr := strings.ToLower(addr)
			key := fmt.Sprintf("%s:%s", normalizedChainID, normalizedAddr)
			bl.contracts[key] = true
		}
	}

	return bl, nil
}

// IsBlacklisted checks if a contract address is blacklisted for a given chain
func (b *blacklistRegistry) IsBlacklisted(chainID domain.Chain, contractAddress string) bool {
	if b == nil {
		return false
	}
	key := fmt.Sprintf("%s:%s", strings.ToLower(string(chainID)), strings.ToLower(contractAddress))
	return b.contracts[key]
}

// IsTokenCIDBlacklisted checks if a TokenCID is blacklisted
func (b *blacklistRegistry) IsTokenCIDBlacklisted(tokenCID domain.TokenCID) bool {
	if b == nil {
		return false
	}
	chainID, _, contractAddress, _ := tokenCID.Parse()
	return b.IsBlacklisted(chainID, contractAddress)
}

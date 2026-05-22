package adapter

import (
	"context"
	"fmt"
	"io/fs"

	"go.uber.org/zap"

	ethadapter "github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
)

// StandardOperations defines the Ethereum client methods used by standard adapters.
type StandardOperations interface {
	ERC721OwnerOf(ctx context.Context, contractAddress, tokenNumber string) (string, error)
	ERC721TokenURI(ctx context.Context, contractAddress, tokenNumber string) (string, error)
	ERC1155URI(ctx context.Context, contractAddress, tokenNumber string) (string, error)
	ERC1155TokenExists(ctx context.Context, contractAddress, tokenNumber string) (bool, error)
}

// AdapterRegistry routes contract calls to configured, standard, or fallback adapters.
type AdapterRegistry struct {
	contractAdapters map[string]ContractAdapter
	contractConfigs  map[string]ContractConfig
	standardAdapters map[domain.ChainStandard]ContractAdapter
	fallbackAdapter  ContractAdapter
}

// NewAdapterRegistry loads contract configuration and builds the adapter lookup table.
func NewAdapterRegistry(
	fsys fs.FS,
	ethClient ethadapter.EthClient,
	ops StandardOperations,
) (*AdapterRegistry, error) {
	abiRegistry, err := NewABIRegistry(fsys)
	if err != nil {
		return nil, fmt.Errorf("load ABI registry: %w", err)
	}

	cfg, err := LoadContractsConfig(fsys)
	if err != nil {
		return nil, fmt.Errorf("load contracts config: %w", err)
	}

	registry := &AdapterRegistry{
		contractAdapters: make(map[string]ContractAdapter, len(cfg.Contracts)),
		contractConfigs:  make(map[string]ContractConfig, len(cfg.Contracts)),
		standardAdapters: map[domain.ChainStandard]ContractAdapter{
			domain.StandardERC721:  NewERC721StandardAdapter(ops),
			domain.StandardERC1155: NewERC1155StandardAdapter(ops),
		},
		fallbackAdapter: NewFallbackAdapter(),
	}

	overridesByChain := make(map[domain.Chain]int)
	for _, entry := range cfg.Contracts {
		adp, err := BuildGenericAdapterFromConfig(entry, abiRegistry, ethClient)
		if err != nil {
			return nil, fmt.Errorf("build adapter for %s: %w", entry.Name, err)
		}

		key := contractKey(entry.Chain, entry.Address)
		registry.contractAdapters[key] = adp
		registry.contractConfigs[key] = entry
		overridesByChain[entry.Chain]++
	}

	for chain, count := range overridesByChain {
		logger.InfoCtx(context.Background(), "Loaded contract adapter overrides",
			zap.String("chain", string(chain)),
			zap.Int("count", count),
		)
	}

	return registry, nil
}

// GetAdapter returns the adapter for a chain, contract, and token standard.
//
// Lookup order: configured contract override, standard adapter, fallback adapter.
func (r *AdapterRegistry) GetAdapter(
	chain domain.Chain,
	contractAddress string,
	standard domain.ChainStandard,
) ContractAdapter {
	key := contractKey(chain, contractAddress)
	if adp, ok := r.contractAdapters[key]; ok {
		return adp
	}
	if adp, ok := r.standardAdapters[standard]; ok {
		return adp
	}
	return r.fallbackAdapter
}

// ContractOverrideCount returns the number of configured contract overrides.
func (r *AdapterRegistry) ContractOverrideCount() int {
	return len(r.contractAdapters)
}

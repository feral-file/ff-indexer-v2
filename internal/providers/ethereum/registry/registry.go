// Package registry routes Ethereum contract requests to standard or configured adapters.
package registry

import (
	"context"
	"errors"
	"fmt"
	"io/fs"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"go.uber.org/zap"

	ethadapter "github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/block"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/adapters"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/helpers"
)

// AdapterRegistry routes contract calls to configured or standard adapters.
type AdapterRegistry struct {
	contractAdapters map[string]adapters.ContractAdapter
	contractConfigs  map[string]ContractConfig
	standardAdapters map[domain.ChainStandard]adapters.ContractAdapter
}

// NewAdapterRegistry loads contract configuration and builds the adapter lookup table.
func NewAdapterRegistry(
	fsys fs.FS,
	ethClient ethadapter.EthClient,
	clock ethadapter.Clock,
	blockProvider block.BlockProvider,
	pagination *helpers.PaginationHelper,
	chainID domain.Chain,
) (*AdapterRegistry, error) {
	abiRegistry, err := helpers.NewABIRegistry(fsys)
	if err != nil {
		return nil, fmt.Errorf("load ABI registry: %w", err)
	}

	cfg, err := LoadContractsConfigWithABIRegistry(fsys, abiRegistry)
	if err != nil {
		return nil, fmt.Errorf("load contracts config: %w", err)
	}

	registry := &AdapterRegistry{
		contractAdapters: make(map[string]adapters.ContractAdapter, len(cfg.Contracts)),
		contractConfigs:  make(map[string]ContractConfig, len(cfg.Contracts)),
		standardAdapters: map[domain.ChainStandard]adapters.ContractAdapter{
			domain.StandardERC721:  adapters.NewERC721Adapter(ethClient, pagination, chainID, blockProvider, clock),
			domain.StandardERC1155: adapters.NewERC1155Adapter(ethClient, pagination, chainID, blockProvider, clock),
		},
	}

	overridesByChain := make(map[domain.Chain]int)
	for _, entry := range cfg.Contracts {
		adp, err := BuildGenericAdapterFromConfig(entry, abiRegistry, ethClient, pagination, chainID, blockProvider, clock)
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
func (r *AdapterRegistry) GetAdapter(
	chain domain.Chain,
	contractAddress string,
	standard domain.ChainStandard,
) (adapters.ContractAdapter, error) {
	key := contractKey(chain, contractAddress)
	if adp, ok := r.contractAdapters[key]; ok {
		return adp, nil
	}
	if adp, ok := r.standardAdapters[standard]; ok {
		return adp, nil
	}
	return nil, adapters.ErrUnsupportedContractStandard
}

// SupportsProvenance reports whether full on-chain provenance indexing is supported for a contract.
func (r *AdapterRegistry) SupportsProvenance(
	chain domain.Chain,
	contractAddress string,
	standard domain.ChainStandard,
) (bool, error) {
	adp, err := r.GetAdapter(chain, contractAddress, standard)
	if err != nil {
		return false, err
	}
	return adp.SupportsProvenance(), nil
}

// ContractOverrideCount returns the number of configured contract overrides.
func (r *AdapterRegistry) ContractOverrideCount() int {
	return len(r.contractAdapters)
}

// GetContractAdapter returns a configured contract override adapter when present.
func (r *AdapterRegistry) GetContractAdapter(chain domain.Chain, contractAddress string) (adapters.ContractAdapter, bool) {
	key := contractKey(chain, contractAddress)
	adp, ok := r.contractAdapters[key]
	return adp, ok
}

// GetContractStandard returns the configured token standard for a contract override.
func (r *AdapterRegistry) GetContractStandard(chain domain.Chain, contractAddress string) (domain.ChainStandard, bool) {
	key := contractKey(chain, contractAddress)
	entry, ok := r.contractConfigs[key]
	if !ok {
		return "", false
	}
	return entry.Standard, true
}

// GetAllCustomEventSignatures returns all custom event signatures across configured contracts.
func (r *AdapterRegistry) GetAllCustomEventSignatures() []common.Hash {
	var signatures []common.Hash
	seen := make(map[common.Hash]struct{})

	for _, adp := range r.contractAdapters {
		for _, sig := range adp.GetEventSignatures() {
			if _, exists := seen[sig]; exists {
				continue
			}
			seen[sig] = struct{}{}
			signatures = append(signatures, sig)
		}
	}

	return signatures
}

// GetStandardAdapter returns the standard adapter for a token standard.
func (r *AdapterRegistry) GetStandardAdapter(standard domain.ChainStandard) (adapters.ContractAdapter, bool) {
	adp, ok := r.standardAdapters[standard]
	return adp, ok
}

// ParseEvent routes event parsing to the appropriate adapter.
func (r *AdapterRegistry) ParseEvent(
	ctx context.Context,
	chain domain.Chain,
	vLog types.Log,
	event *domain.BlockchainEvent,
) (*domain.BlockchainEvent, error) {
	if adp, ok := r.GetContractAdapter(chain, vLog.Address.Hex()); ok {
		parsed, err := adp.ParseEvent(ctx, vLog, event)
		if err == nil || !errors.Is(err, adapters.ErrUnknownEvent) {
			return parsed, err
		}
	}

	for _, standard := range []domain.ChainStandard{domain.StandardERC721, domain.StandardERC1155} {
		adp, ok := r.GetStandardAdapter(standard)
		if !ok {
			continue
		}
		for _, sig := range adp.GetEventSignatures() {
			if len(vLog.Topics) > 0 && vLog.Topics[0] == sig {
				return adp.ParseEvent(ctx, vLog, event)
			}
		}
	}

	return nil, fmt.Errorf("unknown event signature: %s", vLog.Topics[0].Hex())
}

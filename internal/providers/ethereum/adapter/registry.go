package adapter

import (
	"context"
	"fmt"
	"io/fs"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"

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

// AdapterRegistry routes contract calls to configured or standard adapters.
type AdapterRegistry struct {
	contractAdapters map[string]ContractAdapter
	contractConfigs  map[string]ContractConfig
	standardAdapters map[domain.ChainStandard]ContractAdapter
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

	cfg, err := LoadContractsConfigWithABIRegistry(fsys, abiRegistry)
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
// Lookup order: configured contract override, then standard adapter for the declared token standard.
// Returns ErrUnsupportedContractStandard when neither applies.
func (r *AdapterRegistry) GetAdapter(
	chain domain.Chain,
	contractAddress string,
	standard domain.ChainStandard,
) (ContractAdapter, error) {
	key := contractKey(chain, contractAddress)
	if adp, ok := r.contractAdapters[key]; ok {
		return adp, nil
	}
	if adp, ok := r.standardAdapters[standard]; ok {
		return adp, nil
	}
	return nil, ErrUnsupportedContractStandard
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
func (r *AdapterRegistry) GetContractAdapter(chain domain.Chain, contractAddress string) (ContractAdapter, bool) {
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
//
// Reason: ethSubscriber needs a global topic filter that includes legacy contract events.
func (r *AdapterRegistry) GetAllCustomEventSignatures() []common.Hash {
	var signatures []common.Hash
	seen := make(map[common.Hash]struct{})

	for _, adp := range r.contractAdapters {
		for _, eventCfg := range adp.GetProvenanceEventConfigs() {
			sig := crypto.Keccak256Hash([]byte(eventCfg.Signature))
			if _, exists := seen[sig]; exists {
				continue
			}
			seen[sig] = struct{}{}
			signatures = append(signatures, sig)
		}
	}

	return signatures
}

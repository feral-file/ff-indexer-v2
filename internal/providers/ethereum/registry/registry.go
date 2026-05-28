// Package registry routes Ethereum contract requests to standard or configured adapters.
package registry

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"strings"

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
			domain.StandardERC721:  adapters.NewERC721Adapter(ethClient, pagination, blockProvider, chainID),
			domain.StandardERC1155: adapters.NewERC1155Adapter(ethClient, pagination, chainID, blockProvider),
		},
	}

	overridesByChain := make(map[domain.Chain]int)
	for _, entry := range cfg.Contracts {
		adp, err := BuildGenericAdapterFromConfig(entry, abiRegistry, ethClient, pagination, blockProvider, chainID)
		if err != nil {
			return nil, fmt.Errorf("build adapter for %s: %w", entry.Name, err)
		}

		key := contractKey(entry.Chain, entry.Address)
		registry.contractAdapters[key] = adp
		registry.contractConfigs[key] = entry
		overridesByChain[entry.Chain]++

		if entry.OwnershipModel == adapters.OwnershipSingleOwner && len(entry.Adapter.Events) == 0 {
			logger.WarnCtx(context.Background(),
				"Legacy contract configured with limited indexing mode (no provenance events)",
				zap.String("contract", entry.Name),
				zap.String("address", entry.Address),
				zap.String("chain", string(entry.Chain)),
				zap.String("ownership_model", string(entry.OwnershipModel)),
				zap.String("indexing_mode", "current_state_only"),
				zap.Strings("disabled_capabilities", []string{
					"address_owner_sweeps",
					"realtime_event_ingestion",
					"full_provenance_history",
					"ownership_change_webhooks",
				}),
				zap.Strings("supported_capabilities", []string{
					"explicit_token_indexing",
					"current_owner_snapshot",
					"metadata_indexing",
				}),
			)
		}
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
// Configured contract overrides are resolved by (chain, address) first. When the requested
// CID standard differs from the configured contract's derived CID standard, lookup fails
// with ErrConfiguredStandardMismatch so callers cannot persist externally invalid token rows.
// Falls back to standard adapters when no override exists. Returns ErrUnsupportedContractStandard
// if no adapter is found.
func (r *AdapterRegistry) GetAdapter(
	chain domain.Chain,
	contractAddress string,
	standard domain.ChainStandard,
) (adapters.ContractAdapter, error) {
	key := contractKey(chain, contractAddress)
	if adp, ok := r.contractAdapters[key]; ok {
		entry := r.contractConfigs[key]
		expectedStandard := entry.CIDStandard()
		if expectedStandard != standard {
			return nil, fmt.Errorf("configured contract %q expects standard %q, got %q: %w",
				entry.Name, expectedStandard, standard, adapters.ErrConfiguredStandardMismatch)
		}
		return adp, nil
	}
	if adp, ok := r.standardAdapters[standard]; ok {
		return adp, nil
	}
	return nil, adapters.ErrUnsupportedContractStandard
}

// SupportsProvenance reports whether full on-chain provenance indexing is supported for a contract.
//
// Returns true if the adapter can parse historical ownership events (Transfer, TransferSingle,
// TransferBatch, or configured custom events). Returns false if only current-state queries
// are available.
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
// Returns (adapter, true) if a custom configuration exists for this chain+address.
// Returns (nil, false) if no override is configured (caller should use standard adapter).
func (r *AdapterRegistry) GetContractAdapter(chain domain.Chain, contractAddress string) (adapters.ContractAdapter, bool) {
	key := contractKey(chain, contractAddress)
	adp, ok := r.contractAdapters[key]
	return adp, ok
}

// GetContractCIDStandard returns the derived CID standard for a configured contract override.
func (r *AdapterRegistry) GetContractCIDStandard(chain domain.Chain, contractAddress string) (domain.ChainStandard, bool) {
	key := contractKey(chain, contractAddress)
	entry, ok := r.contractConfigs[key]
	if !ok {
		return "", false
	}
	return entry.CIDStandard(), true
}

// GetAllCustomEventSignatures returns all custom event signatures across configured contracts.
// Deduplicates signatures that appear in multiple contracts.
func (r *AdapterRegistry) GetAllCustomEventSignatures() []common.Hash {
	return r.collectCustomEventSignatures(nil)
}

// GetCustomEventSignaturesForChain returns custom event signatures for configured contracts on a chain.
// Used by the subscriber so custom legacy topics are not subscribed on unrelated networks.
func (r *AdapterRegistry) GetCustomEventSignaturesForChain(chain domain.Chain) []common.Hash {
	return r.collectCustomEventSignatures(&chain)
}

func (r *AdapterRegistry) collectCustomEventSignatures(chain *domain.Chain) []common.Hash {
	var signatures []common.Hash
	seen := make(map[common.Hash]struct{})

	for key, entry := range r.contractConfigs {
		if chain != nil && entry.Chain != *chain {
			continue
		}

		adp, ok := r.contractAdapters[key]
		if !ok {
			continue
		}

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

// IsConfiguredContract reports whether chain+address has a configured contract override.
func (r *AdapterRegistry) IsConfiguredContract(chain domain.Chain, contractAddress string) bool {
	_, ok := r.GetContractAdapter(chain, contractAddress)
	return ok
}

// IsKnownCustomEventSignatureForChain reports whether topic0 matches a configured legacy signature on chain.
func (r *AdapterRegistry) IsKnownCustomEventSignatureForChain(chain domain.Chain, topic common.Hash) bool {
	for _, sig := range r.GetCustomEventSignaturesForChain(chain) {
		if sig == topic {
			return true
		}
	}
	return false
}

// GetProvenanceContractsForChain returns configured contracts with provenance support for a chain.
//
// Only includes contracts that have custom events configured (SupportsProvenance() == true).
// The map keys are lowercase contract addresses for consistent comparison.
//
// This is used by GetTokenCIDsByOwnerAndBlockRange to discover all GenericAdapters that
// should be queried in parallel with standard ERC721/ERC1155 adapters.
func (r *AdapterRegistry) GetProvenanceContractsForChain(chain domain.Chain) map[string]adapters.ContractAdapter {
	result := make(map[string]adapters.ContractAdapter)

	for key, entry := range r.contractConfigs {
		if entry.Chain != chain {
			continue
		}

		adp, ok := r.contractAdapters[key]
		if !ok || !adp.SupportsProvenance() {
			continue
		}

		result[strings.ToLower(common.HexToAddress(entry.Address).Hex())] = adp
	}

	return result
}

// GetStandardAdapter returns the standard adapter for a token standard.
// Returns (adapter, true) for ERC721 or ERC1155, (nil, false) for unknown standards.
// Used by the client to discover standard adapters for ownership queries.
func (r *AdapterRegistry) GetStandardAdapter(standard domain.ChainStandard) (adapters.ContractAdapter, bool) {
	adp, ok := r.standardAdapters[standard]
	return adp, ok
}

// ParseEvent routes event parsing to the appropriate adapter.
//
// First tries configured contract overrides, then falls back to standard adapters based on
// event signature matching. Returns the parsed event or ErrUnknownEvent if no adapter recognizes
// the log.
//
// Contract overrides take priority to handle legacy contracts that emit standard-looking events
// with non-standard semantics.
func (r *AdapterRegistry) ParseEvent(
	ctx context.Context,
	vLog types.Log,
	chain domain.Chain,
) (*domain.BlockchainEvent, error) {
	if adp, ok := r.GetContractAdapter(chain, vLog.Address.Hex()); ok {
		parsed, err := adp.ParseEvent(ctx, vLog)
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
				return adp.ParseEvent(ctx, vLog)
			}
		}
	}

	if len(vLog.Topics) > 0 &&
		r.IsKnownCustomEventSignatureForChain(chain, vLog.Topics[0]) &&
		!r.IsConfiguredContract(chain, vLog.Address.Hex()) {
		logger.DebugCtx(ctx, "Skipping custom legacy signature from unconfigured contract",
			zap.String("chain", string(chain)),
			zap.String("signature", vLog.Topics[0].Hex()),
			zap.String("address", vLog.Address.Hex()),
			zap.Uint64("block", vLog.BlockNumber),
			zap.Uint("logIndex", vLog.Index))
		return nil, nil
	}

	return nil, fmt.Errorf("unknown event signature: %s", vLog.Topics[0].Hex())
}

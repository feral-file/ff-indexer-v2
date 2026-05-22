package adapter

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"strings"

	"github.com/ethereum/go-ethereum/common"

	ethadapter "github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
)

const contractsFile = "contracts.json"

// ContractsConfig is the top-level contracts.json schema.
type ContractsConfig struct {
	Contracts []ContractConfig `json:"contracts"`
}

// ContractConfig describes a single contract override entry.
type ContractConfig struct {
	Chain       domain.Chain         `json:"chain"`
	Address     string               `json:"address"`
	Name        string               `json:"name"`
	Standard    domain.ChainStandard `json:"standard"`
	Adapter     AdapterConfig        `json:"adapter"`
	Constraints ContractConstraints  `json:"constraints"`
}

// AdapterConfig holds declarative method routing for a contract.
type AdapterConfig struct {
	Existence MethodConfig   `json:"existence"`
	Owner     MethodConfig   `json:"owner"`
	Metadata  MetadataConfig `json:"metadata"`
}

// MethodConfig describes a contract method call used by GenericAdapter.
type MethodConfig struct {
	Method           string   `json:"method"`
	ABI              string   `json:"abi"`
	Params           []string `json:"params"`
	SuccessCondition string   `json:"success_condition"`
}

// MetadataConfig describes metadata routing for a contract.
type MetadataConfig struct {
	Source string        `json:"source"`
	Method *MethodConfig `json:"method,omitempty"`
}

// ContractConstraints holds optional validation constraints for contract entries.
type ContractConstraints struct {
	TokenIDMax *int64 `json:"token_id_max"`
}

// LoadContractsConfig reads and validates contracts.json from the provided filesystem.
func LoadContractsConfig(fsys fs.FS) (*ContractsConfig, error) {
	content, err := fs.ReadFile(fsys, contractsFile)
	if err != nil {
		return nil, fmt.Errorf("read contracts config: %w", err)
	}

	var cfg ContractsConfig
	if err := json.Unmarshal(content, &cfg); err != nil {
		return nil, fmt.Errorf("parse contracts config: %w", err)
	}

	if err := validateContractsConfig(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func validateContractsConfig(cfg *ContractsConfig) error {
	seen := make(map[string]struct{}, len(cfg.Contracts))

	for i := range cfg.Contracts {
		entry := &cfg.Contracts[i]
		if err := validateContractEntry(entry); err != nil {
			return fmt.Errorf("contracts[%d]: %w", i, err)
		}

		key := contractKey(entry.Chain, entry.Address)
		if _, ok := seen[key]; ok {
			return fmt.Errorf("contracts[%d]: duplicate contract entry for %s", i, key)
		}
		seen[key] = struct{}{}
	}

	return nil
}

func validateContractEntry(entry *ContractConfig) error {
	if entry.Chain == "" {
		return fmt.Errorf("chain is required")
	}
	if entry.Address == "" {
		return fmt.Errorf("address is required")
	}
	if !common.IsHexAddress(entry.Address) {
		return fmt.Errorf("invalid address: %s", entry.Address)
	}
	if entry.Standard == "" {
		return fmt.Errorf("standard is required")
	}
	if entry.Adapter.Existence.Method == "" {
		return fmt.Errorf("adapter.existence.method is required")
	}
	if entry.Adapter.Existence.ABI == "" {
		return fmt.Errorf("adapter.existence.abi is required")
	}
	if entry.Adapter.Owner.Method == "" {
		return fmt.Errorf("adapter.owner.method is required")
	}
	if entry.Adapter.Owner.ABI == "" {
		return fmt.Errorf("adapter.owner.abi is required")
	}

	return nil
}

func contractKey(chain domain.Chain, address string) string {
	return fmt.Sprintf("%s:%s", chain, strings.ToLower(common.HexToAddress(address).Hex()))
}

func buildMethodCall(cfg MethodConfig, abiRegistry *ABIRegistry) (*MethodCall, error) {
	contractABI, err := abiRegistry.Get(cfg.ABI)
	if err != nil {
		return nil, err
	}

	return &MethodCall{
		Method:           cfg.Method,
		ABI:              contractABI,
		Params:           cfg.Params,
		SuccessCondition: SuccessCondition(cfg.SuccessCondition),
	}, nil
}

func buildMetadataConfig(cfg MetadataConfig, abiRegistry *ABIRegistry) (ContractMetadataConfig, error) {
	source := MetadataSource(cfg.Source)
	if source == "" {
		source = MetadataSourceVendorOnly
	}

	metadata := ContractMetadataConfig{Source: source}
	if cfg.Method != nil {
		method, err := buildMethodCall(*cfg.Method, abiRegistry)
		if err != nil {
			return ContractMetadataConfig{}, err
		}
		metadata.Method = method
	}

	return metadata, nil
}

// BuildGenericAdapterFromConfig constructs a GenericAdapter for a validated contract entry.
func BuildGenericAdapterFromConfig(
	entry ContractConfig,
	abiRegistry *ABIRegistry,
	ethClient ethadapter.EthClient,
) (ContractAdapter, error) {
	existence, err := buildMethodCall(entry.Adapter.Existence, abiRegistry)
	if err != nil {
		return nil, fmt.Errorf("existence method: %w", err)
	}

	owner, err := buildMethodCall(entry.Adapter.Owner, abiRegistry)
	if err != nil {
		return nil, fmt.Errorf("owner method: %w", err)
	}

	metadata, err := buildMetadataConfig(entry.Adapter.Metadata, abiRegistry)
	if err != nil {
		return nil, fmt.Errorf("metadata config: %w", err)
	}

	return NewGenericAdapter(
		existence,
		owner,
		metadata,
		ethClient,
		false,
	), nil
}

// IsVendorOnlyMetadata reports whether a configured contract skips on-chain metadata.
func (r *AdapterRegistry) IsVendorOnlyMetadata(chain domain.Chain, contractAddress string) bool {
	key := contractKey(chain, contractAddress)
	entry, ok := r.contractConfigs[key]
	if !ok {
		return false
	}

	source := MetadataSource(entry.Adapter.Metadata.Source)
	if source == "" {
		return true
	}

	return source == MetadataSourceVendorOnly
}

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

	// Load ABI registry for validation
	abiRegistry, err := NewABIRegistry(fsys)
	if err != nil {
		return nil, fmt.Errorf("load ABI registry for validation: %w", err)
	}

	if err := validateContractsConfig(&cfg, abiRegistry); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// LoadContractsConfigWithABIRegistry reads and validates contracts.json using a pre-loaded ABI registry.
//
// Reason: Avoid loading the ABI registry twice during registry initialization.
func LoadContractsConfigWithABIRegistry(fsys fs.FS, abiRegistry *ABIRegistry) (*ContractsConfig, error) {
	content, err := fs.ReadFile(fsys, contractsFile)
	if err != nil {
		return nil, fmt.Errorf("read contracts config: %w", err)
	}

	var cfg ContractsConfig
	if err := json.Unmarshal(content, &cfg); err != nil {
		return nil, fmt.Errorf("parse contracts config: %w", err)
	}

	if err := validateContractsConfig(&cfg, abiRegistry); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func validateContractsConfig(cfg *ContractsConfig, abiRegistry *ABIRegistry) error {
	seen := make(map[string]struct{}, len(cfg.Contracts))

	for i := range cfg.Contracts {
		entry := &cfg.Contracts[i]
		if err := validateContractEntry(entry, abiRegistry); err != nil {
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

func validateContractEntry(entry *ContractConfig, abiRegistry *ABIRegistry) error {
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

	// Validate existence method configuration
	if err := validateMethodConfig(entry.Adapter.Existence, abiRegistry, "adapter.existence"); err != nil {
		return err
	}

	// Validate owner method configuration
	if err := validateMethodConfig(entry.Adapter.Owner, abiRegistry, "adapter.owner"); err != nil {
		return err
	}

	// Validate metadata configuration
	if err := validateMetadataConfig(entry.Adapter.Metadata, abiRegistry); err != nil {
		return err
	}

	return nil
}

// validateMethodConfig validates that a method exists in its declared ABI and has valid success condition.
func validateMethodConfig(cfg MethodConfig, abiRegistry *ABIRegistry, fieldPath string) error {
	contractABI, err := abiRegistry.Get(cfg.ABI)
	if err != nil {
		return fmt.Errorf("%s: ABI %q not found: %w", fieldPath, cfg.ABI, err)
	}

	if _, ok := contractABI.Methods[cfg.Method]; !ok {
		return fmt.Errorf("%s: method %q not found in ABI %q", fieldPath, cfg.Method, cfg.ABI)
	}

	// Validate success condition if specified
	if cfg.SuccessCondition != "" {
		switch SuccessCondition(cfg.SuccessCondition) {
		case SuccessNoRevert, SuccessAddressNonZero:
			// Valid
		default:
			return fmt.Errorf("%s: invalid success_condition %q (allowed: no_revert, address_nonzero)", fieldPath, cfg.SuccessCondition)
		}
	}

	return nil
}

// validateMetadataConfig validates metadata source and method configuration.
func validateMetadataConfig(cfg MetadataConfig, abiRegistry *ABIRegistry) error {
	// Validate metadata source
	if cfg.Source != "" {
		switch MetadataSource(cfg.Source) {
		case MetadataSourceOnChain, MetadataSourceVendorOnly:
			// Valid
		default:
			return fmt.Errorf("adapter.metadata.source: invalid value %q (allowed: on_chain, vendor_only)", cfg.Source)
		}
	}

	// If on-chain metadata with method, validate the method
	if cfg.Source == string(MetadataSourceOnChain) && cfg.Method != nil {
		if err := validateMethodConfig(*cfg.Method, abiRegistry, "adapter.metadata.method"); err != nil {
			return err
		}
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
		entry.Constraints,
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

package adapter

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"regexp"
	"strings"

	"github.com/ethereum/go-ethereum/common"

	ethadapter "github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
)

var eventSignaturePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*\([^)]+\)$`)

const contractsFile = "contracts.json"

// ContractsConfig is the top-level contracts.json schema.
type ContractsConfig struct {
	Contracts []ContractConfig `json:"contracts"`
}

// ContractConfig describes a single contract override entry.
type ContractConfig struct {
	Chain    domain.Chain         `json:"chain"`
	Address  string               `json:"address"`
	Name     string               `json:"name"`
	Standard domain.ChainStandard `json:"standard"`
	Adapter  AdapterConfig        `json:"adapter"`
}

// AdapterConfig holds declarative method routing for a contract.
type AdapterConfig struct {
	Existence MethodConfig   `json:"existence"`
	Owner     MethodConfig   `json:"owner"`
	Metadata  MetadataConfig `json:"metadata"`
	Events    []EventConfig  `json:"events,omitempty"`
}

// EventConfig describes a custom event for provenance indexing.
type EventConfig struct {
	Signature          string            `json:"signature"`
	MapToStandardEvent domain.EventType  `json:"mapToStandardEvent"`
	IndexedParams      []string          `json:"indexedParams"`
	DataParams         []string          `json:"dataParams"`
	ParameterMappings  map[string]string `json:"parameterMappings"`
}

// Supported event field names for parameterMappings values.
const (
	EventFieldFromAddress = "FromAddress"
	EventFieldToAddress   = "ToAddress"
	EventFieldTokenNumber = "TokenNumber"
	EventFieldQuantity    = "Quantity"
)

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

	seenSignatures := make(map[string]int)
	for i, eventCfg := range entry.Adapter.Events {
		if err := validateEventConfig(eventCfg, fmt.Sprintf("adapter.events[%d]", i)); err != nil {
			return err
		}
		if prevIdx, exists := seenSignatures[eventCfg.Signature]; exists {
			return fmt.Errorf("adapter.events[%d]: duplicate event signature %q (also defined at adapter.events[%d])", i, eventCfg.Signature, prevIdx)
		}
		seenSignatures[eventCfg.Signature] = i
	}

	return nil
}

func validateEventConfig(cfg EventConfig, fieldPath string) error {
	if cfg.Signature == "" {
		return fmt.Errorf("%s: signature is required", fieldPath)
	}
	if !eventSignaturePattern.MatchString(cfg.Signature) {
		return fmt.Errorf("%s: invalid signature format %q", fieldPath, cfg.Signature)
	}
	if cfg.MapToStandardEvent == "" {
		return fmt.Errorf("%s: mapToStandardEvent is required", fieldPath)
	}
	switch cfg.MapToStandardEvent {
	case domain.EventTypeTransfer, domain.EventTypeMint, domain.EventTypeBurn, domain.EventTypeMetadataUpdate:
	default:
		return fmt.Errorf("%s: invalid mapToStandardEvent %q (allowed: transfer, mint, burn, metadata_update)", fieldPath, cfg.MapToStandardEvent)
	}
	if len(cfg.ParameterMappings) == 0 {
		return fmt.Errorf("%s: parameterMappings is required", fieldPath)
	}

	paramNames := make(map[string]struct{}, len(cfg.IndexedParams)+len(cfg.DataParams))
	for _, name := range cfg.IndexedParams {
		if name == "" {
			return fmt.Errorf("%s: empty parameter name in indexedParams", fieldPath)
		}
		if _, exists := paramNames[name]; exists {
			return fmt.Errorf("%s: duplicate parameter name %q in indexedParams", fieldPath, name)
		}
		paramNames[name] = struct{}{}
	}
	for i, name := range cfg.DataParams {
		if name == "" {
			return fmt.Errorf("%s: empty parameter name in dataParams", fieldPath)
		}
		// Check for duplicates within dataParams FIRST (before checking paramNames)
		for j := 0; j < i; j++ {
			if cfg.DataParams[j] == name {
				return fmt.Errorf("%s: duplicate parameter name %q in dataParams", fieldPath, name)
			}
		}
		// Then check for duplicates across indexed/data
		if _, existsInIndexed := paramNames[name]; existsInIndexed {
			return fmt.Errorf("%s: duplicate parameter name %q across indexedParams and dataParams", fieldPath, name)
		}
		paramNames[name] = struct{}{}
	}

	usedTargetFields := make(map[string]string)
	for paramName, eventField := range cfg.ParameterMappings {
		if _, ok := paramNames[paramName]; !ok {
			return fmt.Errorf("%s: parameterMappings references unknown parameter %q", fieldPath, paramName)
		}
		switch eventField {
		case EventFieldFromAddress, EventFieldToAddress, EventFieldTokenNumber, EventFieldQuantity:
		default:
			return fmt.Errorf("%s: invalid parameterMappings target %q for parameter %q (allowed: FromAddress, ToAddress, TokenNumber, Quantity)", fieldPath, eventField, paramName)
		}

		if existingParam, exists := usedTargetFields[eventField]; exists {
			return fmt.Errorf("%s: duplicate target field %q mapped from both %q and %q", fieldPath, eventField, existingParam, paramName)
		}
		usedTargetFields[eventField] = paramName
	}

	for paramName := range paramNames {
		if _, ok := cfg.ParameterMappings[paramName]; !ok {
			return fmt.Errorf("%s: parameter %q is not mapped in parameterMappings", fieldPath, paramName)
		}
	}

	if err := validateSemanticRequirements(cfg, usedTargetFields, fieldPath); err != nil {
		return err
	}

	return nil
}

func validateSemanticRequirements(cfg EventConfig, mappedFields map[string]string, fieldPath string) error {
	hasTokenNumber := mappedFields[EventFieldTokenNumber] != ""
	hasToAddress := mappedFields[EventFieldToAddress] != ""
	hasFromAddress := mappedFields[EventFieldFromAddress] != ""

	switch cfg.MapToStandardEvent {
	case domain.EventTypeTransfer:
		if !hasTokenNumber {
			return fmt.Errorf("%s: transfer events require TokenNumber mapping", fieldPath)
		}
		if !hasFromAddress {
			return fmt.Errorf("%s: transfer events require FromAddress mapping", fieldPath)
		}
		if !hasToAddress {
			return fmt.Errorf("%s: transfer events require ToAddress mapping", fieldPath)
		}
	case domain.EventTypeMint:
		if !hasTokenNumber {
			return fmt.Errorf("%s: mint events require TokenNumber mapping", fieldPath)
		}
		if !hasToAddress {
			return fmt.Errorf("%s: mint events require ToAddress mapping", fieldPath)
		}
	case domain.EventTypeBurn:
		if !hasTokenNumber {
			return fmt.Errorf("%s: burn events require TokenNumber mapping", fieldPath)
		}
		if !hasFromAddress {
			return fmt.Errorf("%s: burn events require FromAddress mapping", fieldPath)
		}
	case domain.EventTypeMetadataUpdate:
		if !hasTokenNumber {
			return fmt.Errorf("%s: metadata_update events require TokenNumber mapping", fieldPath)
		}
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

	if MetadataSource(cfg.Source) == MetadataSourceOnChain {
		if cfg.Method == nil {
			return fmt.Errorf("adapter.metadata.method is required when source is on_chain")
		}
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

	supportsProvenance := len(entry.Adapter.Events) > 0

	return NewGenericAdapter(
		existence,
		owner,
		metadata,
		ethClient,
		supportsProvenance,
		entry.Adapter.Events,
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

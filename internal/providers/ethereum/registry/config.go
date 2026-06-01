package registry

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"regexp"
	"strings"

	"github.com/ethereum/go-ethereum/common"

	ethadapter "github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/block"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/adapters"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/helpers"
)

var eventSignaturePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*\([^)]+\)$`)

const contractsFile = "contracts.json"

// ContractsConfig is the top-level contracts.json schema.
type ContractsConfig struct {
	Contracts []ContractConfig `json:"contracts"`
}

// ContractConfig describes a single contract override entry.
type ContractConfig struct {
	Chain          domain.Chain            `json:"chain"`
	Address        string                  `json:"address"`
	Name           string                  `json:"name"`
	OwnershipModel adapters.OwnershipModel `json:"ownership_model"`
	Adapter        AdapterConfig           `json:"adapter"`
}

// CIDStandard returns the token standard label used in Token CIDs and API responses.
// Derived from ownership_model: single_owner -> erc721, multi_holder -> erc1155.
func (c *ContractConfig) CIDStandard() domain.ChainStandard {
	return adapters.CIDStandardFromOwnershipModel(c.OwnershipModel)
}

// AdapterConfig holds declarative method routing for a contract.
type AdapterConfig struct {
	Existence MethodConfig           `json:"existence"`
	Owner     MethodConfig           `json:"owner"`
	Metadata  MetadataConfig         `json:"metadata"`
	Events    []adapters.EventConfig `json:"events,omitempty"`
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

// LoadContractsConfig reads and validates contracts.json from the provided filesystem.
func LoadContractsConfig(fsys fs.FS) (*ContractsConfig, error) {
	abiRegistry, err := helpers.NewABIRegistry(fsys)
	if err != nil {
		return nil, fmt.Errorf("load ABI registry for validation: %w", err)
	}
	return LoadContractsConfigWithABIRegistry(fsys, abiRegistry)
}

// LoadContractsConfigWithABIRegistry reads and validates contracts.json using a pre-loaded ABI registry.
func LoadContractsConfigWithABIRegistry(fsys fs.FS, abiRegistry *helpers.ABIRegistry) (*ContractsConfig, error) {
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

func validateContractsConfig(cfg *ContractsConfig, abiRegistry *helpers.ABIRegistry) error {
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

func validateContractEntry(entry *ContractConfig, abiRegistry *helpers.ABIRegistry) error {
	if entry.Chain == "" {
		return fmt.Errorf("chain is required")
	}
	if entry.Address == "" {
		return fmt.Errorf("address is required")
	}
	if !common.IsHexAddress(entry.Address) {
		return fmt.Errorf("invalid address: %s", entry.Address)
	}
	if entry.OwnershipModel == "" {
		return fmt.Errorf("ownership_model is required")
	}
	switch entry.OwnershipModel {
	case adapters.OwnershipSingleOwner, adapters.OwnershipMultiHolder:
	default:
		return fmt.Errorf(
			"unsupported ownership_model %q (allowed: single_owner, multi_holder)",
			entry.OwnershipModel,
		)
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

	if err := validateMethodConfig(entry.Adapter.Existence, abiRegistry, "adapter.existence"); err != nil {
		return err
	}
	if err := validateMethodConfig(entry.Adapter.Owner, abiRegistry, "adapter.owner"); err != nil {
		return err
	}
	if err := validateMetadataConfig(entry.Adapter.Metadata, abiRegistry); err != nil {
		return err
	}

	seenSignatures := make(map[string]int)
	for i, eventCfg := range entry.Adapter.Events {
		if err := validateEventConfig(eventCfg, entry.CIDStandard(), fmt.Sprintf("adapter.events[%d]", i)); err != nil {
			return err
		}
		if prevIdx, exists := seenSignatures[eventCfg.Signature]; exists {
			return fmt.Errorf("adapter.events[%d]: duplicate event signature %q (also defined at adapter.events[%d])", i, eventCfg.Signature, prevIdx)
		}
		seenSignatures[eventCfg.Signature] = i
	}

	// multi_holder balance indexing replays provenance logs; without configured events there is no
	// supported path to discover holders or compute balances for legacy overrides.
	if entry.OwnershipModel == adapters.OwnershipMultiHolder && len(entry.Adapter.Events) == 0 {
		return fmt.Errorf(
			"adapter.events is required for ownership_model %q (multi-holder balance indexing replays provenance events)",
			entry.OwnershipModel,
		)
	}

	return nil
}

func validateEventConfig(cfg adapters.EventConfig, standard domain.ChainStandard, fieldPath string) error {
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
		for j := 0; j < i; j++ {
			if cfg.DataParams[j] == name {
				return fmt.Errorf("%s: duplicate parameter name %q in dataParams", fieldPath, name)
			}
		}
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
		case adapters.EventFieldFromAddress, adapters.EventFieldToAddress, adapters.EventFieldTokenNumber, adapters.EventFieldQuantity:
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

	// Build set of indexed parameter names for provenance validation
	indexedParams := make(map[string]bool, len(cfg.IndexedParams))
	for _, param := range cfg.IndexedParams {
		indexedParams[param] = true
	}

	return validateSemanticRequirements(cfg, usedTargetFields, indexedParams, standard, fieldPath)
}

func validateSemanticRequirements(
	cfg adapters.EventConfig,
	mappedFields map[string]string,
	indexedParams map[string]bool,
	standard domain.ChainStandard,
	fieldPath string,
) error {
	hasTokenNumber := mappedFields[adapters.EventFieldTokenNumber] != ""
	hasToAddress := mappedFields[adapters.EventFieldToAddress] != ""
	hasFromAddress := mappedFields[adapters.EventFieldFromAddress] != ""
	hasQuantity := mappedFields[adapters.EventFieldQuantity] != ""

	// Helper to check if a mapped field comes from an indexed parameter
	isIndexed := func(field string) bool {
		paramName, exists := mappedFields[field]
		return exists && indexedParams[paramName]
	}

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
		// For ERC1155-style contracts, Quantity is required for accurate balance tracking
		if standard == domain.StandardERC1155 && !hasQuantity {
			return fmt.Errorf("%s: transfer events for ERC1155-style contracts require Quantity mapping", fieldPath)
		}
		// For ownership tracking, FromAddress and ToAddress must be indexed
		if !isIndexed(adapters.EventFieldFromAddress) {
			return fmt.Errorf("%s: transfer events require FromAddress to be an indexed parameter for ownership tracking", fieldPath)
		}
		if !isIndexed(adapters.EventFieldToAddress) {
			return fmt.Errorf("%s: transfer events require ToAddress to be an indexed parameter for ownership tracking", fieldPath)
		}
	case domain.EventTypeMint:
		if !hasTokenNumber {
			return fmt.Errorf("%s: mint events require TokenNumber mapping", fieldPath)
		}
		if !hasToAddress {
			return fmt.Errorf("%s: mint events require ToAddress mapping", fieldPath)
		}
		// For ERC1155-style contracts, Quantity is required for accurate balance tracking
		if standard == domain.StandardERC1155 && !hasQuantity {
			return fmt.Errorf("%s: mint events for ERC1155-style contracts require Quantity mapping", fieldPath)
		}
		// For ownership tracking, ToAddress must be indexed
		if !isIndexed(adapters.EventFieldToAddress) {
			return fmt.Errorf("%s: mint events require ToAddress to be an indexed parameter for ownership tracking", fieldPath)
		}
	case domain.EventTypeBurn:
		if !hasTokenNumber {
			return fmt.Errorf("%s: burn events require TokenNumber mapping", fieldPath)
		}
		if !hasFromAddress {
			return fmt.Errorf("%s: burn events require FromAddress mapping", fieldPath)
		}
		// For ERC1155-style contracts, Quantity is required for accurate balance tracking
		if standard == domain.StandardERC1155 && !hasQuantity {
			return fmt.Errorf("%s: burn events for ERC1155-style contracts require Quantity mapping", fieldPath)
		}
		// For ownership tracking, FromAddress must be indexed
		if !isIndexed(adapters.EventFieldFromAddress) {
			return fmt.Errorf("%s: burn events require FromAddress to be an indexed parameter for ownership tracking", fieldPath)
		}
	case domain.EventTypeMetadataUpdate:
		if !hasTokenNumber {
			return fmt.Errorf("%s: metadata_update events require TokenNumber mapping", fieldPath)
		}
	}

	return nil
}

func validateMethodConfig(cfg MethodConfig, abiRegistry *helpers.ABIRegistry, fieldPath string) error {
	contractABI, err := abiRegistry.Get(cfg.ABI)
	if err != nil {
		return fmt.Errorf("%s: ABI %q not found: %w", fieldPath, cfg.ABI, err)
	}

	method, ok := contractABI.Methods[cfg.Method]
	if !ok {
		return fmt.Errorf("%s: method %q not found in ABI %q", fieldPath, cfg.Method, cfg.ABI)
	}

	// Validate parameter count matches ABI inputs
	if len(cfg.Params) != len(method.Inputs) {
		return fmt.Errorf("%s: method %q expects %d parameters, got %d",
			fieldPath, cfg.Method, len(method.Inputs), len(cfg.Params))
	}

	// Validate parameter placeholders are supported
	supportedPlaceholders := map[string]bool{
		"${tokenId}": true,
	}
	for i, param := range cfg.Params {
		if strings.HasPrefix(param, "${") && strings.HasSuffix(param, "}") {
			if !supportedPlaceholders[param] {
				return fmt.Errorf("%s: unsupported placeholder %q in params[%d] (allowed: ${tokenId})",
					fieldPath, param, i)
			}
		}
	}

	// Validate return types for specific method roles
	// Owner and existence methods must return a single address
	if strings.Contains(fieldPath, "owner") || strings.Contains(fieldPath, "existence") {
		if len(method.Outputs) != 1 {
			return fmt.Errorf("%s: method %q must return exactly one value, got %d",
				fieldPath, cfg.Method, len(method.Outputs))
		}
		outputType := method.Outputs[0].Type.String()
		if outputType != "address" {
			return fmt.Errorf("%s: method %q must return address, got %s",
				fieldPath, cfg.Method, outputType)
		}
	}

	if cfg.SuccessCondition != "" {
		switch adapters.SuccessCondition(cfg.SuccessCondition) {
		case adapters.SuccessNoRevert, adapters.SuccessAddressNonZero:
		default:
			return fmt.Errorf("%s: invalid success_condition %q (allowed: no_revert, address_nonzero)", fieldPath, cfg.SuccessCondition)
		}
	}

	return nil
}

func validateMetadataConfig(cfg MetadataConfig, abiRegistry *helpers.ABIRegistry) error {
	if cfg.Source != "" {
		switch adapters.MetadataSource(cfg.Source) {
		case adapters.MetadataSourceOnChain, adapters.MetadataSourceVendorOnly:
		default:
			return fmt.Errorf("adapter.metadata.source: invalid value %q (allowed: on_chain, vendor_only)", cfg.Source)
		}
	}

	if adapters.MetadataSource(cfg.Source) == adapters.MetadataSourceOnChain {
		if cfg.Method == nil {
			return fmt.Errorf("adapter.metadata.method is required when source is on_chain")
		}
		if err := validateMethodConfig(*cfg.Method, abiRegistry, "adapter.metadata.method"); err != nil {
			return err
		}

		// Validate metadata method returns string
		contractABI, err := abiRegistry.Get(cfg.Method.ABI)
		if err != nil {
			return fmt.Errorf("adapter.metadata.method: ABI %q not found: %w", cfg.Method.ABI, err)
		}

		method, ok := contractABI.Methods[cfg.Method.Method]
		if !ok {
			return fmt.Errorf("adapter.metadata.method: method %q not found in ABI", cfg.Method.Method)
		}

		if len(method.Outputs) != 1 {
			return fmt.Errorf("adapter.metadata.method: method %q must return exactly one value, got %d",
				cfg.Method.Method, len(method.Outputs))
		}

		outputType := method.Outputs[0].Type.String()
		if outputType != "string" {
			return fmt.Errorf("adapter.metadata.method: method %q must return string, got %s",
				cfg.Method.Method, outputType)
		}
	}

	return nil
}

func contractKey(chain domain.Chain, address string) string {
	return fmt.Sprintf("%s:%s", chain, strings.ToLower(common.HexToAddress(address).Hex()))
}

func buildMethodCall(cfg MethodConfig, abiRegistry *helpers.ABIRegistry) (*adapters.MethodCall, error) {
	contractABI, err := abiRegistry.Get(cfg.ABI)
	if err != nil {
		return nil, err
	}

	return &adapters.MethodCall{
		Method:           cfg.Method,
		ABI:              contractABI,
		Params:           cfg.Params,
		SuccessCondition: adapters.SuccessCondition(cfg.SuccessCondition),
	}, nil
}

func buildMetadataConfig(cfg MetadataConfig, abiRegistry *helpers.ABIRegistry) (adapters.ContractMetadataConfig, error) {
	source := adapters.MetadataSource(cfg.Source)
	if source == "" {
		source = adapters.MetadataSourceVendorOnly
	}

	metadata := adapters.ContractMetadataConfig{Source: source}
	if cfg.Method != nil {
		method, err := buildMethodCall(*cfg.Method, abiRegistry)
		if err != nil {
			return adapters.ContractMetadataConfig{}, err
		}
		metadata.Method = method
	}

	return metadata, nil
}

// BuildGenericAdapterFromConfig constructs a GenericAdapter for a validated contract entry.
func BuildGenericAdapterFromConfig(
	entry ContractConfig,
	abiRegistry *helpers.ABIRegistry,
	ethClient ethadapter.EthClient,
	pagination *helpers.PaginationHelper,
	blockProvider block.BlockProvider,
	chainID domain.Chain,
) (adapters.ContractAdapter, error) {
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

	return adapters.NewGenericAdapter(
		entry.Address,
		entry.OwnershipModel,
		existence,
		owner,
		metadata,
		ethClient,
		pagination,
		blockProvider,
		supportsProvenance,
		entry.Adapter.Events,
		chainID,
	), nil
}

// IsVendorOnlyMetadata reports whether a configured contract skips on-chain metadata.
func (r *AdapterRegistry) IsVendorOnlyMetadata(chain domain.Chain, contractAddress string) bool {
	key := contractKey(chain, contractAddress)
	entry, ok := r.contractConfigs[key]
	if !ok {
		return false
	}

	source := adapters.MetadataSource(entry.Adapter.Metadata.Source)
	if source == "" {
		return true
	}

	return source == adapters.MetadataSourceVendorOnly
}

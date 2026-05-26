// Package adapters provides standard and config-driven contract adapters for the Ethereum provider.
package adapters

import (
	"context"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
)

// ContractAdapter defines contract-specific token operations and event parsing.
type ContractAdapter interface {
	TokenExists(ctx context.Context, contractAddress, tokenNumber string) (bool, error)
	TokenOwner(ctx context.Context, contractAddress, tokenNumber string) (string, error)
	TokenURI(ctx context.Context, contractAddress, tokenNumber string) (string, error)
	SupportsProvenance() bool
	GetEventSignatures() []common.Hash
	ParseEvent(ctx context.Context, vLog types.Log, event *domain.BlockchainEvent) (*domain.BlockchainEvent, error)
	GetStandard() domain.ChainStandard
	// GetTokenEvents fetches all historical events for a specific token
	// Returns events in ascending order of timestamp
	GetTokenEvents(ctx context.Context, contractAddress, tokenNumber string) ([]domain.BlockchainEvent, error)
}

// SuccessCondition describes how to interpret a contract call result for existence checks.
type SuccessCondition string

const (
	SuccessNoRevert       SuccessCondition = "no_revert"
	SuccessAddressNonZero SuccessCondition = "address_nonzero"
)

// MetadataSource describes where token metadata should be fetched from.
type MetadataSource string

const (
	MetadataSourceOnChain    MetadataSource = "on_chain"
	MetadataSourceVendorOnly MetadataSource = "vendor_only"
)

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

// MethodCall describes a declarative contract method invocation.
type MethodCall struct {
	Method           string
	ABI              abi.ABI
	Params           []string
	SuccessCondition SuccessCondition
}

// ContractMetadataConfig holds metadata routing configuration for a contract adapter.
type ContractMetadataConfig struct {
	Source MetadataSource
	Method *MethodCall
}

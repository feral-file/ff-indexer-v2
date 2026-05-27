// Package adapters provides standard and config-driven contract adapters for the Ethereum provider.
package adapters

import (
	"context"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/registry"
)

// ContractAdapter defines contract-specific token operations and event parsing.
//
// Each adapter encapsulates the logic for a specific token standard or custom contract,
// handling both on-chain queries and historical event processing. Adapters are stateless
// and rely on the Ethereum client for RPC calls and the pagination helper for log queries.
type ContractAdapter interface {
	// TokenExists checks whether a token exists in the contract.
	//
	// Implementations typically call ownerOf or a custom existence check method.
	// Returns false if the call reverts or if a custom success condition fails.
	// Does not distinguish between "never minted" vs "burned" - both return false.
	TokenExists(ctx context.Context, contractAddress, tokenNumber string) (bool, error)

	// TokenOwner returns the current owner address for a token.
	//
	// For ERC721, this calls ownerOf. For ERC1155, this calls balanceOf and returns
	// the holder if balance > 0. For configured contracts, this calls the configured
	// owner method from contracts.json.
	//
	// Returns the zero address if the token does not exist or has been burned.
	// The caller must normalize the returned address to lowercase for comparison.
	TokenOwner(ctx context.Context, contractAddress, tokenNumber string) (string, error)

	// TokenURI returns the metadata URI for a token.
	//
	// For ERC721, this calls tokenURI. For ERC1155, this calls uri. For configured
	// contracts with on_chain metadata source, this calls the configured method.
	// For configured contracts with vendor_only source, this returns empty string.
	//
	// The returned URI may require vendor-specific resolution (e.g., IPFS gateway,
	// base64 data URIs, or vendor API enrichment).
	TokenURI(ctx context.Context, contractAddress, tokenNumber string) (string, error)

	// SupportsProvenance reports whether the adapter can parse historical ownership events.
	//
	// Standard adapters (ERC721, ERC1155) always return true. Generic adapters return
	// true only if provenance events are configured in contracts.json.
	//
	// If false, the indexer cannot track historical ownership or build token provenance
	// chains for this contract. Tokens will be visible only via current state queries.
	SupportsProvenance() bool

	// GetEventSignatures returns the keccak256 hashes of events this adapter recognizes.
	//
	// Used by the subscriber to filter relevant logs before passing them to ParseEvent.
	// Standard adapters return Transfer event signatures. Generic adapters return the
	// signatures of all configured events.
	GetEventSignatures() []common.Hash

	// ParseEvent decodes a raw log into a standardized blockchain event.
	//
	// The passed event struct may be partially populated (contract, block, log index).
	// The adapter extracts from/to/tokenNumber and other fields, then returns the
	// completed event or nil if the log is not relevant.
	//
	// Standard adapters parse ERC721/ERC1155 Transfer events. Generic adapters parse
	// configured custom events and map them to standard event types using the configured
	// parameterMappings in contracts.json.
	//
	// Returns nil without error if the log does not match expected event shapes.
	ParseEvent(ctx context.Context, vLog types.Log, event *domain.BlockchainEvent) (*domain.BlockchainEvent, error)

	// GetStandard returns the token standard this adapter implements.
	//
	// Used by the client to construct TokenCIDs and by the generic adapter to select
	// ownership tracking logic (ERC721: last-transfer-wins, ERC1155: balance accumulation).
	GetStandard() domain.ChainStandard

	// GetTokenEvents fetches all historical events for a specific token.
	//
	// Queries Transfer/TransferSingle/TransferBatch events (or configured custom events)
	// for the given token from contract deployment to latest block, then parses them
	// into standardized events.
	//
	// Returns events in ascending timestamp order. The caller can use this to build
	// the full provenance chain for a token.
	GetTokenEvents(ctx context.Context, contractAddress, tokenNumber string) ([]domain.BlockchainEvent, error)

	// GetTokensByOwner returns all tokens owned by the address within the block range.
	//
	// Reason: Full adapter-centric ownership tracking where each adapter handles its own
	// standard-specific query and tracking logic, enabling parallel execution and clean
	// separation of concerns.
	//
	// Constraints:
	// - Each adapter processes the entire block range independently (no stop-early optimization)
	// - Does not apply any limit; the caller aggregates across all adapters and applies
	//   global limits at block boundaries
	// - Blacklist filtering is applied during tracking, not after aggregation
	//
	// Trade-offs:
	// - Simplicity: Each adapter owns its complete ownership logic without client coordination
	// - Performance: Processes full range even when limit is small; caller should chunk large
	//   ranges at the workflow layer if this becomes expensive
	// - Correctness: Ensures each standard's ownership semantics (last-transfer-wins vs
	//   balance-accumulation) are consistently applied without client-side mixing
	//
	// Ownership semantics:
	// - ERC721: last-transfer-wins (owned if most recent Transfer has to==owner)
	// - ERC1155: balance-accumulation (owned if net balance > 0 after all transfers)
	// - Configured contracts: use standard field from contracts.json to select logic
	//
	// Returns all owned tokens with their block numbers. The block number represents the
	// last observed ownership-affecting event for that token within the range.
	GetTokensByOwner(
		ctx context.Context,
		ownerAddress string,
		fromBlock uint64,
		toBlock uint64,
		blacklist registry.BlacklistRegistry,
	) ([]domain.TokenWithBlock, error)
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

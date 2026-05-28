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

// OwnershipModel distinguishes single-owner vs multi-holder token semantics for indexing.
type OwnershipModel string

const (
	// OwnershipSingleOwner uses last-transfer-wins ownership tracking (ERC-721-shaped).
	OwnershipSingleOwner OwnershipModel = "single_owner"
	// OwnershipMultiHolder uses balance-accumulation ownership tracking (ERC-1155-shaped).
	OwnershipMultiHolder OwnershipModel = "multi_holder"
)

// CIDStandardFromOwnershipModel maps ownership semantics to the token CID/API standard label.
func CIDStandardFromOwnershipModel(model OwnershipModel) domain.ChainStandard {
	switch model {
	case OwnershipMultiHolder:
		return domain.StandardERC1155
	case OwnershipSingleOwner:
		return domain.StandardERC721
	default:
		return domain.StandardERC721
	}
}

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
	// ERC721 calls ownerOf and may error on revert. ERC1155 returns an error because
	// single-owner lookup is unsupported. Configured contracts call the configured
	// owner method; zero owner with address_nonzero existence semantics returns
	// domain.ErrTokenNotFoundOnChain.
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

	// ParseEvent decodes a raw log into a complete standardized blockchain event.
	//
	// Timestamp is resolved from the log when present; otherwise BlockProvider lookup is used.
	// Chain is taken from the adapter's configured chain ID.
	//
	// Returns (nil, nil) for intentionally skipped logs (e.g. ERC20 Transfer, ERC1155
	// TransferBatch). Generic adapters return ErrUnknownEvent when topic0 is unrecognized.
	ParseEvent(ctx context.Context, vLog types.Log) (*domain.BlockchainEvent, error)

	// GetStandard returns the chain standard label for this adapter.
	//
	// Standard adapters return ERC721 or ERC1155. Generic adapters return the CID
	// standard derived from ownership_model.
	GetStandard() domain.ChainStandard

	// OwnershipModel returns whether this adapter uses single-owner or multi-holder semantics.
	OwnershipModel() OwnershipModel

	// GetTokenBalances fetches current balances for all holders of a token.
	//
	// Only supported for multi_holder adapters. ERC1155 uses best-effort event replay;
	// generic multi_holder adapters replay configured events. Single-owner adapters
	// return an error.
	GetTokenBalances(ctx context.Context, contractAddress, tokenNumber string) (map[string]string, error)

	// GetTokenBalancesForAddresses fetches balances for a specific set of addresses.
	//
	// ERC1155 uses balanceOfBatch for on-chain accuracy. ERC721 returns "1" when a
	// requested address is the current owner. Configured contracts use the configured
	// owner method for single_owner or replay configured events for multi_holder.
	//
	// Intended for full provenance indexing where accuracy matters, unlike
	// GetTokenBalances which is best-effort for owner discovery.
	//
	// Returns map[ownerAddress]balance, excluding zero balances.
	GetTokenBalancesForAddresses(
		ctx context.Context,
		contractAddress, tokenNumber string,
		addresses []string,
	) (map[string]string, error)

	// GetOwnerBalanceAndEvents fetches balance and ownership-affecting events for one owner.
	//
	// Only supported for multi_holder adapters. ERC1155 combines on-chain balanceOf with
	// historical TransferSingle/TransferBatch logs. Generic multi_holder adapters replay
	// configured events only.
	GetOwnerBalanceAndEvents(
		ctx context.Context,
		contractAddress, tokenNumber, ownerAddress string,
	) (balance string, events []domain.BlockchainEvent, err error)

	// GetTokenEvents fetches historical events for a specific token.
	//
	// Standard adapters query from genesis to latest and filter by token ID. ERC1155 token
	// history currently includes TransferSingle and URI events only (TransferBatch is not
	// parsed yet). Generic adapters query configured custom events.
	//
	// Returns events in ascending block/log order.
	GetTokenEvents(ctx context.Context, contractAddress, tokenNumber string) ([]domain.BlockchainEvent, error)

	// GetOwnerLogs fetches raw ownership-affecting logs for an owner within a block range.
	//
	// The client merges logs from all adapters and runs unified replay with a global held-token
	// limit. Adapters do not apply limits or compute final ownership snapshots.
	GetOwnerLogs(
		ctx context.Context,
		ownerAddress string,
		fromBlock uint64,
		toBlock uint64,
	) ([]types.Log, error)

	// GetTokensByOwner returns tokens owned by the address at the end of the block range.
	//
	// Convenience wrapper for adapter tests: GetOwnerLogs followed by full-range replay without
	// a global limit. Production owner scans use client-side ReplayOwnerTokensWithLimit instead.
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

package adapters

import (
	"context"
	"fmt"
	"math/big"
	"sort"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"go.uber.org/zap"

	ethadapter "github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/block"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/helpers"
	"github.com/feral-file/ff-indexer-v2/internal/registry"
)

// ERC721Adapter handles standard ERC-721 token operations and event parsing.
type ERC721Adapter struct {
	ethClient     ethadapter.EthClient
	pagination    *helpers.PaginationHelper
	blockProvider block.BlockProvider
	chainID       domain.Chain
}

// NewERC721Adapter creates an adapter for standard ERC-721 contracts.
func NewERC721Adapter(
	ethClient ethadapter.EthClient,
	pagination *helpers.PaginationHelper,
	blockProvider block.BlockProvider,
	chainID domain.Chain,
) *ERC721Adapter {
	return &ERC721Adapter{
		ethClient:     ethClient,
		pagination:    pagination,
		blockProvider: blockProvider,
		chainID:       chainID,
	}
}

// GetStandard returns the ERC-721 chain standard.
func (a *ERC721Adapter) GetStandard() domain.ChainStandard {
	return domain.StandardERC721
}

// OwnershipModel returns single-owner semantics for ERC-721 tokens.
func (a *ERC721Adapter) OwnershipModel() OwnershipModel {
	return OwnershipSingleOwner
}

// GetTokenBalances is unsupported for single-owner ERC-721 tokens.
func (a *ERC721Adapter) GetTokenBalances(_ context.Context, _, _ string) (map[string]string, error) {
	return nil, fmt.Errorf("GetTokenBalances not supported for single-owner tokens")
}

// GetTokenBalancesForAddresses checks if any provided address is the current owner.
//
// For ERC-721 single-owner tokens, returns {"owner": "1"} if one of the addresses
// matches the current owner, otherwise returns an empty map.
//
// Reason: Full provenance indexing queries all addresses discovered from events;
// this method allows consistent adapter-based balance queries for both ERC-721 and ERC-1155.
//
// Trade-offs: Makes an ownerOf call regardless of address list size (always single owner).
//
// Constraints: Returns at most one entry (single owner), or empty if token is burned/non-existent.
func (a *ERC721Adapter) GetTokenBalancesForAddresses(
	ctx context.Context,
	contractAddress, tokenNumber string,
	addresses []string,
) (map[string]string, error) {
	if len(addresses) == 0 {
		return make(map[string]string), nil
	}

	// Get current owner
	owner, err := helpers.ERC721OwnerOf(ctx, a.ethClient, contractAddress, tokenNumber)
	if err != nil {
		if helpers.IsExecutionRevert(err) {
			// Token doesn't exist or is burned
			return make(map[string]string), nil
		}
		return nil, fmt.Errorf("failed to get ERC721 owner: %w", err)
	}

	// Normalize for comparison
	ownerLower := common.HexToAddress(owner).Hex()

	// Check if current owner is in the address list
	for _, addr := range addresses {
		addrLower := common.HexToAddress(addr).Hex()
		if addrLower == ownerLower && ownerLower != domain.ETHEREUM_ZERO_ADDRESS {
			return map[string]string{ownerLower: "1"}, nil
		}
	}

	// Current owner is not in the provided address list
	return make(map[string]string), nil
}

// GetOwnerBalanceAndEvents is unsupported for single-owner ERC-721 tokens.
func (a *ERC721Adapter) GetOwnerBalanceAndEvents(
	_ context.Context, _, _, _ string,
) (string, []domain.BlockchainEvent, error) {
	return "", nil, fmt.Errorf("GetOwnerBalanceAndEvents not supported for single-owner tokens")
}

// TokenExists checks existence via ownerOf, treating execution reverts as non-existence.
func (a *ERC721Adapter) TokenExists(ctx context.Context, contractAddress, tokenNumber string) (bool, error) {
	_, err := helpers.ERC721OwnerOf(ctx, a.ethClient, contractAddress, tokenNumber)
	if err != nil {
		if helpers.IsExecutionRevert(err) {
			return false, nil
		}
		return false, fmt.Errorf("failed to check ERC721 token existence: %w", err)
	}
	return true, nil
}

// TokenOwner returns the current ERC-721 owner.
func (a *ERC721Adapter) TokenOwner(ctx context.Context, contractAddress, tokenNumber string) (string, error) {
	return helpers.ERC721OwnerOf(ctx, a.ethClient, contractAddress, tokenNumber)
}

// TokenURI returns the ERC-721 tokenURI value.
func (a *ERC721Adapter) TokenURI(ctx context.Context, contractAddress, tokenNumber string) (string, error) {
	return helpers.ERC721TokenURI(ctx, a.ethClient, contractAddress, tokenNumber)
}

// SupportsProvenance reports that standard ERC-721 provenance indexing is supported.
func (a *ERC721Adapter) SupportsProvenance() bool {
	return true
}

// GetEventSignatures returns standard ERC-721 and EIP-4906 event topic hashes.
func (a *ERC721Adapter) GetEventSignatures() []common.Hash {
	return []common.Hash{
		helpers.TransferEventSignature,
		helpers.EIP4906MetadataUpdateEventSignature,
		helpers.EIP4906BatchMetadataUpdateEventSignature,
	}
}

// GetTokenEvents fetches all historical events for a specific ERC-721 token.
// For ERC-721, tokenId is indexed (topic[3]), so we can filter directly at the RPC level.
// Returns events in ascending order of timestamp.
func (a *ERC721Adapter) GetTokenEvents(ctx context.Context, contractAddress, tokenNumber string) ([]domain.BlockchainEvent, error) {
	// Parse token number to big.Int
	tokenID, ok := new(big.Int).SetString(tokenNumber, 10)
	if !ok {
		return nil, fmt.Errorf("invalid token number: %s", tokenNumber)
	}

	contractAddr := common.HexToAddress(contractAddress)
	tokenIDHash := common.BigToHash(tokenID)

	// For ERC721, tokenId is indexed (topic[3]), so we can filter directly
	query := ethereum.FilterQuery{
		FromBlock: big.NewInt(0), // From genesis
		ToBlock:   nil,           // To latest
		Addresses: []common.Address{contractAddr},
		Topics: [][]common.Hash{
			{
				helpers.TransferEventSignature,              // Transfer events
				helpers.EIP4906MetadataUpdateEventSignature, // MetadataUpdate events
				//helpers.EIP4906BatchMetadataUpdateEventSignature, // FIXME: Handle batch metadata updates properly
			},
			nil,           // Any from address
			nil,           // Any to address
			{tokenIDHash}, // Specific token ID
		},
	}

	// Fetch logs with pagination to handle Infura's 10k limitation
	logs, err := a.pagination.FilterLogsWithPagination(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to filter logs: %w", err)
	}

	// Parse logs and convert to BlockchainEvent
	events := make([]domain.BlockchainEvent, 0)
	for _, vLog := range logs {
		parsed, err := a.ParseEvent(ctx, vLog)
		if err != nil {
			return nil, fmt.Errorf("parse event log at block %d index %d: %w", vLog.BlockNumber, vLog.Index, err)
		}
		if parsed == nil {
			continue
		}

		events = append(events, *parsed)
	}

	// Sort events by block number, transaction index, and log index for deterministic ordering
	sort.SliceStable(events, func(i, j int) bool {
		if events[i].BlockNumber != events[j].BlockNumber {
			return events[i].BlockNumber < events[j].BlockNumber
		}
		if events[i].TxIndex != events[j].TxIndex {
			return events[i].TxIndex < events[j].TxIndex
		}
		return events[i].LogIndex < events[j].LogIndex
	})

	return events, nil
}

// GetOwnerLogs fetches ERC-721 Transfer logs where the owner is sender or recipient.
func (a *ERC721Adapter) GetOwnerLogs(
	ctx context.Context,
	ownerAddress string,
	fromBlock uint64,
	toBlock uint64,
) ([]types.Log, error) {
	owner := common.HexToAddress(ownerAddress)
	ownerHash := common.BytesToHash(owner.Bytes())

	queries := []ethereum.FilterQuery{
		{
			FromBlock: new(big.Int).SetUint64(fromBlock),
			ToBlock:   new(big.Int).SetUint64(toBlock),
			Topics: [][]common.Hash{
				{helpers.TransferEventSignature},
				{ownerHash},
			},
		},
		{
			FromBlock: new(big.Int).SetUint64(fromBlock),
			ToBlock:   new(big.Int).SetUint64(toBlock),
			Topics: [][]common.Hash{
				{helpers.TransferEventSignature},
				nil,
				{ownerHash},
			},
		},
	}

	logs, err := filterLogsInParallel(ctx, a.pagination, queries)
	if err != nil {
		return nil, fmt.Errorf("failed to query ERC721 logs: %w", err)
	}

	return logs, nil
}

// GetTokensByOwner returns ERC721 tokens owned by the address at the end of the block range.
func (a *ERC721Adapter) GetTokensByOwner(
	ctx context.Context,
	ownerAddress string,
	fromBlock uint64,
	toBlock uint64,
	blacklist registry.BlacklistRegistry,
) ([]domain.TokenWithBlock, error) {
	owner := common.HexToAddress(ownerAddress)

	logs, err := a.GetOwnerLogs(ctx, ownerAddress, fromBlock, toBlock)
	if err != nil {
		return nil, err
	}

	logs = deduplicateLogs(logs)
	sortLogsAscending(logs)

	return trackERC721OwnershipFromLogs(a.chainID, owner, logs, blacklist), nil
}

// ParseEvent parses standard ERC-721 and EIP-4906 events.
//
// ERC20 transfers are identified and skipped BEFORE timestamp lookup to prevent
// irrelevant ERC20 activity from tearing down the subscription when timestamp
// lookups fail. ERC20 Transfer(address,address,uint256) has 3 topics; ERC721
// Transfer(address,address,uint256) has 4 topics (indexed tokenId).
func (a *ERC721Adapter) ParseEvent(ctx context.Context, vLog types.Log) (*domain.BlockchainEvent, error) {
	if len(vLog.Topics) == 0 {
		return nil, fmt.Errorf("event log has no topics")
	}

	// For Transfer events, identify and skip ERC20 BEFORE BaseEventFromLog.
	// This prevents transient timestamp lookup failures from crashing the
	// subscription on irrelevant ERC20 noise.
	if vLog.Topics[0] == helpers.TransferEventSignature {
		if len(vLog.Topics) == 3 {
			// ERC20 Transfer - skip without error
			logger.DebugCtx(ctx, "Skipping ERC20 transfer event (pre-parse)",
				zap.String("contract", vLog.Address.Hex()),
				zap.String("txHash", vLog.TxHash.Hex()))
			return nil, nil
		}
		if len(vLog.Topics) != 4 {
			return nil, fmt.Errorf("invalid Transfer event: expected 3 or 4 topics, got %d", len(vLog.Topics))
		}
	}

	base, err := helpers.BaseEventFromLog(ctx, a.chainID, vLog, a.blockProvider)
	if err != nil {
		return nil, err
	}

	switch vLog.Topics[0] {
	case helpers.TransferEventSignature:
		// Topic count already validated above - this is ERC721
		parsed, err := helpers.ParseERC721TransferLog(vLog, base)
		if err != nil {
			return nil, err
		}
		return parsed, nil
	case helpers.EIP4906MetadataUpdateEventSignature:
		if len(vLog.Topics) != 1 {
			return nil, fmt.Errorf("invalid MetadataUpdate event: expected 1 topic, got %d", len(vLog.Topics))
		}
		if len(vLog.Data) < 32 {
			return nil, fmt.Errorf("invalid MetadataUpdate event: insufficient data")
		}
		event := base
		event.Standard = domain.StandardERC721
		event.TokenNumber = new(big.Int).SetBytes(vLog.Data[0:32]).String()
		event.EventType = domain.EventTypeMetadataUpdate
		event.Quantity = "1"
		return &event, nil
	case helpers.EIP4906BatchMetadataUpdateEventSignature:
		if len(vLog.Topics) != 1 {
			return nil, fmt.Errorf("invalid BatchMetadataUpdate event: expected 1 topic, got %d", len(vLog.Topics))
		}
		if len(vLog.Data) < 64 {
			return nil, fmt.Errorf("invalid BatchMetadataUpdate event: insufficient data")
		}
		fromTokenID := new(big.Int).SetBytes(vLog.Data[0:32])
		toTokenID := new(big.Int).SetBytes(vLog.Data[32:64])
		event := base
		event.Standard = domain.StandardERC721
		event.TokenNumber = fromTokenID.String()
		event.ToTokenNumber = toTokenID.String()
		event.EventType = domain.EventTypeMetadataUpdateRange
		event.Quantity = "1"
		return &event, nil
	default:
		return nil, ErrUnknownEvent
	}
}

var _ ContractAdapter = (*ERC721Adapter)(nil)

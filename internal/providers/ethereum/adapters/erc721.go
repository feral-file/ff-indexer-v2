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
)

// ERC721Adapter handles standard ERC-721 token operations and event parsing.
type ERC721Adapter struct {
	ethClient     ethadapter.EthClient
	pagination    *helpers.PaginationHelper
	chainID       domain.Chain
	blockProvider block.BlockProvider
	clock         ethadapter.Clock
}

// NewERC721Adapter creates an adapter for standard ERC-721 contracts.
func NewERC721Adapter(
	ethClient ethadapter.EthClient,
	pagination *helpers.PaginationHelper,
	chainID domain.Chain,
	blockProvider block.BlockProvider,
	clock ethadapter.Clock,
) *ERC721Adapter {
	return &ERC721Adapter{
		ethClient:     ethClient,
		pagination:    pagination,
		chainID:       chainID,
		blockProvider: blockProvider,
		clock:         clock,
	}
}

// GetStandard returns the ERC-721 chain standard.
func (a *ERC721Adapter) GetStandard() domain.ChainStandard {
	return domain.StandardERC721
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
		blockHash := vLog.BlockHash.Hex()

		// Get block timestamp
		timestamp, err := a.blockProvider.GetBlockTimestamp(ctx, vLog.BlockNumber)
		if err != nil {
			// Fallback to current time if timestamp fetch fails
			timestamp = a.clock.Now()
			logger.WarnCtx(ctx, "Failed to fetch block timestamp, using current time",
				zap.Uint64("blockNumber", vLog.BlockNumber),
				zap.Error(err))
		}

		event := &domain.BlockchainEvent{
			Chain:           a.chainID,
			ContractAddress: vLog.Address.Hex(),
			TxHash:          vLog.TxHash.Hex(),
			BlockNumber:     vLog.BlockNumber,
			BlockHash:       &blockHash,
			TxIndex:         uint64(vLog.TxIndex), //nolint:gosec,G115
			LogIndex:        uint64(vLog.Index),   //nolint:gosec,G115
			Timestamp:       timestamp,
		}

		parsed, err := a.ParseEvent(ctx, vLog, event)
		if err != nil {
			// Log error but continue processing
			logger.WarnCtx(ctx, "Failed to parse event log", zap.Error(err))
			continue
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

// ParseEvent parses standard ERC-721 and EIP-4906 events.
func (a *ERC721Adapter) ParseEvent(ctx context.Context, vLog types.Log, event *domain.BlockchainEvent) (*domain.BlockchainEvent, error) {
	if len(vLog.Topics) == 0 {
		return nil, fmt.Errorf("event log has no topics")
	}

	switch vLog.Topics[0] {
	case helpers.TransferEventSignature:
		parsed, err := helpers.ParseERC721TransferLog(vLog, event)
		if err != nil {
			return nil, err
		}
		if parsed == nil {
			logger.DebugCtx(ctx, "Skipping ERC20 transfer event",
				zap.String("contract", vLog.Address.Hex()),
				zap.String("txHash", vLog.TxHash.Hex()))
			return nil, nil
		}
		return parsed, nil
	case helpers.EIP4906MetadataUpdateEventSignature:
		if len(vLog.Topics) != 1 {
			return nil, fmt.Errorf("invalid MetadataUpdate event: expected 1 topic, got %d", len(vLog.Topics))
		}
		if len(vLog.Data) < 32 {
			return nil, fmt.Errorf("invalid MetadataUpdate event: insufficient data")
		}
		event.Standard = domain.StandardERC721
		event.TokenNumber = new(big.Int).SetBytes(vLog.Data[0:32]).String()
		event.EventType = domain.EventTypeMetadataUpdate
		event.Quantity = "1"
		return event, nil
	case helpers.EIP4906BatchMetadataUpdateEventSignature:
		if len(vLog.Topics) != 1 {
			return nil, fmt.Errorf("invalid BatchMetadataUpdate event: expected 1 topic, got %d", len(vLog.Topics))
		}
		if len(vLog.Data) < 64 {
			return nil, fmt.Errorf("invalid BatchMetadataUpdate event: insufficient data")
		}
		fromTokenID := new(big.Int).SetBytes(vLog.Data[0:32])
		toTokenID := new(big.Int).SetBytes(vLog.Data[32:64])
		event.Standard = domain.StandardERC721
		event.TokenNumber = fromTokenID.String()
		event.ToTokenNumber = toTokenID.String()
		event.EventType = domain.EventTypeMetadataUpdateRange
		event.Quantity = "1"
		return event, nil
	default:
		return nil, ErrUnknownEvent
	}
}

var _ ContractAdapter = (*ERC721Adapter)(nil)

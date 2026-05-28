package adapters

import (
	"context"
	"fmt"
	"math/big"
	"sort"
	"time"

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

// ERC1155Adapter handles standard ERC-1155 token operations and event parsing.
type ERC1155Adapter struct {
	ethClient     ethadapter.EthClient
	pagination    *helpers.PaginationHelper
	chainID       domain.Chain
	blockProvider block.BlockProvider
}

// NewERC1155Adapter creates an adapter for standard ERC-1155 contracts.
func NewERC1155Adapter(
	ethClient ethadapter.EthClient,
	pagination *helpers.PaginationHelper,
	chainID domain.Chain,
	blockProvider block.BlockProvider,
) *ERC1155Adapter {
	return &ERC1155Adapter{
		ethClient:     ethClient,
		pagination:    pagination,
		chainID:       chainID,
		blockProvider: blockProvider,
	}
}

// GetStandard returns the ERC-1155 chain standard.
func (a *ERC1155Adapter) GetStandard() domain.ChainStandard {
	return domain.StandardERC1155
}

// OwnershipModel returns multi-holder semantics for ERC-1155 tokens.
func (a *ERC1155Adapter) OwnershipModel() OwnershipModel {
	return OwnershipMultiHolder
}

// GetTokenBalances fetches all holder balances by replaying standard ERC-1155 transfer events.
func (a *ERC1155Adapter) GetTokenBalances(
	ctx context.Context,
	contractAddress, tokenNumber string,
) (map[string]string, error) {
	return helpers.ERC1155ReplayBalances(ctx, a.pagination, a.blockProvider, contractAddress, tokenNumber)
}

// GetOwnerBalanceAndEvents fetches balance and events for a specific ERC-1155 owner.
func (a *ERC1155Adapter) GetOwnerBalanceAndEvents(
	ctx context.Context,
	contractAddress, tokenNumber, ownerAddress string,
) (string, []domain.BlockchainEvent, error) {
	return helpers.ERC1155BalanceAndEventsForOwner(
		ctx,
		a.ethClient,
		a.pagination,
		a.blockProvider,
		a.chainID,
		time.Now,
		contractAddress,
		tokenNumber,
		ownerAddress,
	)
}

// TokenExists checks existence via recent transfer scan and balance checks.
func (a *ERC1155Adapter) TokenExists(ctx context.Context, contractAddress, tokenNumber string) (bool, error) {
	return helpers.ERC1155TokenExists(ctx, a.ethClient, a.pagination, a.blockProvider, contractAddress, tokenNumber)
}

// TokenOwner is unsupported for fungible ERC-1155 tokens.
func (a *ERC1155Adapter) TokenOwner(ctx context.Context, contractAddress, tokenNumber string) (string, error) {
	return "", fmt.Errorf("ERC1155 does not support single-owner lookup")
}

// TokenURI returns the ERC-1155 uri value.
func (a *ERC1155Adapter) TokenURI(ctx context.Context, contractAddress, tokenNumber string) (string, error) {
	return helpers.ERC1155URI(ctx, a.ethClient, contractAddress, tokenNumber)
}

// SupportsProvenance reports that standard ERC-1155 provenance indexing is supported.
func (a *ERC1155Adapter) SupportsProvenance() bool {
	return true
}

// GetEventSignatures returns standard ERC-1155 event topic hashes.
func (a *ERC1155Adapter) GetEventSignatures() []common.Hash {
	return []common.Hash{
		helpers.ERC1155TransferSingleEventSignature,
		helpers.ERC1155TransferBatchEventSignature,
		helpers.ERC1155URIEventSignature,
	}
}

// GetTokenEvents fetches all historical events for a specific ERC-1155 token.
// For ERC-1155, token ID is in data, not topics, so we fetch all events for this contract
// and filter by token ID client-side. Returns events in ascending order of timestamp.
func (a *ERC1155Adapter) GetTokenEvents(ctx context.Context, contractAddress, tokenNumber string) ([]domain.BlockchainEvent, error) {
	// Parse token number to big.Int
	_, ok := new(big.Int).SetString(tokenNumber, 10)
	if !ok {
		return nil, fmt.Errorf("invalid token number: %s", tokenNumber)
	}

	contractAddr := common.HexToAddress(contractAddress)

	// For ERC1155, token ID is in data, not topics, so we fetch all events for this contract
	// and filter by token ID later
	query := ethereum.FilterQuery{
		FromBlock: big.NewInt(0),
		ToBlock:   nil,
		Addresses: []common.Address{contractAddr},
		Topics: [][]common.Hash{
			{
				helpers.ERC1155TransferSingleEventSignature, // TransferSingle events
				//helpers.ERC1155TransferBatchEventSignature,  // FIXME: Handle batch transfers properly
				helpers.ERC1155URIEventSignature, // URI events (metadata updates)
			},
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
			// Log error but continue processing
			logger.WarnCtx(ctx, "Failed to parse event log", zap.Error(err))
			continue
		}
		if parsed == nil {
			continue
		}

		// Filter by token number
		if parsed.TokenNumber != tokenNumber {
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

// GetTokensByOwner returns ERC1155 tokens owned by the address within the block range.
func (a *ERC1155Adapter) GetTokensByOwner(
	ctx context.Context,
	ownerAddress string,
	fromBlock uint64,
	toBlock uint64,
	blacklist registry.BlacklistRegistry,
) ([]domain.TokenWithBlock, error) {
	owner := common.HexToAddress(ownerAddress)
	ownerHash := common.BytesToHash(owner.Bytes())

	queries := []ethereum.FilterQuery{
		{
			FromBlock: new(big.Int).SetUint64(fromBlock),
			ToBlock:   new(big.Int).SetUint64(toBlock),
			Topics: [][]common.Hash{
				{helpers.ERC1155TransferSingleEventSignature},
				nil,
				{ownerHash},
			},
		},
		{
			FromBlock: new(big.Int).SetUint64(fromBlock),
			ToBlock:   new(big.Int).SetUint64(toBlock),
			Topics: [][]common.Hash{
				{helpers.ERC1155TransferSingleEventSignature},
				nil,
				nil,
				{ownerHash},
			},
		},
		{
			FromBlock: new(big.Int).SetUint64(fromBlock),
			ToBlock:   new(big.Int).SetUint64(toBlock),
			Topics: [][]common.Hash{
				{helpers.ERC1155TransferBatchEventSignature},
				nil,
				{ownerHash},
			},
		},
		{
			FromBlock: new(big.Int).SetUint64(fromBlock),
			ToBlock:   new(big.Int).SetUint64(toBlock),
			Topics: [][]common.Hash{
				{helpers.ERC1155TransferBatchEventSignature},
				nil,
				nil,
				{ownerHash},
			},
		},
	}

	logs, err := filterLogsInParallel(ctx, a.pagination, queries)
	if err != nil {
		return nil, fmt.Errorf("failed to query ERC1155 logs: %w", err)
	}

	logs = deduplicateLogs(logs)
	sortLogsAscending(logs)

	return trackERC1155OwnershipFromLogs(a.chainID, owner, logs, blacklist), nil
}

// ParseEvent parses standard ERC-1155 events.
func (a *ERC1155Adapter) ParseEvent(ctx context.Context, vLog types.Log) (*domain.BlockchainEvent, error) {
	if len(vLog.Topics) == 0 {
		return nil, fmt.Errorf("event log has no topics")
	}

	base, err := helpers.BaseEventFromLog(ctx, a.chainID, vLog, a.blockProvider)
	if err != nil {
		return nil, err
	}

	switch vLog.Topics[0] {
	case helpers.ERC1155TransferSingleEventSignature:
		if len(vLog.Topics) != 4 {
			return nil, fmt.Errorf("invalid ERC1155 TransferSingle event: expected 4 topics, got %d", len(vLog.Topics))
		}
		if len(vLog.Data) < 64 {
			return nil, fmt.Errorf("invalid ERC1155 TransferSingle event: insufficient data")
		}

		event := base
		event.Standard = domain.StandardERC1155
		fromAddress := common.BytesToAddress(vLog.Topics[2].Bytes()).Hex()
		event.FromAddress = &fromAddress
		toAddress := common.BytesToAddress(vLog.Topics[3].Bytes()).Hex()
		event.ToAddress = &toAddress
		event.TokenNumber = new(big.Int).SetBytes(vLog.Data[0:32]).String()
		event.Quantity = new(big.Int).SetBytes(vLog.Data[32:64]).String()
		event.EventType = domain.TransferEventType(event.FromAddress, event.ToAddress)
		return &event, nil
	case helpers.ERC1155TransferBatchEventSignature:
		logger.DebugCtx(ctx, "Skipping ERC1155 TransferBatch event",
			zap.String("contract", vLog.Address.Hex()),
			zap.String("txHash", vLog.TxHash.Hex()))
		return nil, nil
	case helpers.ERC1155URIEventSignature:
		if len(vLog.Topics) != 2 {
			return nil, fmt.Errorf("invalid URI event: expected 2 topics, got %d", len(vLog.Topics))
		}
		event := base
		event.Standard = domain.StandardERC1155
		event.TokenNumber = new(big.Int).SetBytes(vLog.Topics[1].Bytes()).String()
		event.EventType = domain.EventTypeMetadataUpdate
		event.Quantity = "1"
		return &event, nil
	default:
		return nil, ErrUnknownEvent
	}
}

var _ ContractAdapter = (*ERC1155Adapter)(nil)

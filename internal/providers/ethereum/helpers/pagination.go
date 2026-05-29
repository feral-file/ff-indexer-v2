package helpers

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/core/types"
	"go.uber.org/zap"

	ethadapter "github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/block"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
)

// PaginationHelper paginates eth_getLogs queries with adaptive step sizing and retry.
type PaginationHelper struct {
	ethClient     ethadapter.EthClient
	clock         ethadapter.Clock
	blockProvider block.BlockProvider
}

// NewPaginationHelper creates a helper for paginated log queries.
func NewPaginationHelper(
	ethClient ethadapter.EthClient,
	clock ethadapter.Clock,
	blockProvider block.BlockProvider,
) *PaginationHelper {
	return &PaginationHelper{
		ethClient:     ethClient,
		clock:         clock,
		blockProvider: blockProvider,
	}
}

// FilterLogsWithPagination fetches logs across a block range using adaptive pagination.
func (h *PaginationHelper) FilterLogsWithPagination(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
	timeoutCtx := ctx
	var cancel context.CancelFunc
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		timeoutCtx, cancel = context.WithTimeout(ctx, time.Minute)
		defer cancel()
	}

	if query.BlockHash != nil {
		return h.ethClient.FilterLogs(timeoutCtx, query)
	}

	var fromBlock, toBlock *big.Int
	if query.FromBlock != nil {
		fromBlock = query.FromBlock
	} else {
		fromBlock = big.NewInt(0)
	}

	if query.ToBlock != nil {
		toBlock = query.ToBlock
	} else {
		latestBlockNum, err := h.blockProvider.GetLatestBlock(timeoutCtx)
		if err != nil {
			return nil, fmt.Errorf("failed to get latest block: %w", err)
		}
		toBlock = new(big.Int).SetUint64(latestBlockNum)
	}

	var allLogs []types.Log
	currentFrom := new(big.Int).Set(fromBlock)
	stepSize := h.calculateStepSize(timeoutCtx, query)

	for currentFrom.Cmp(toBlock) <= 0 {
		select {
		case <-timeoutCtx.Done():
			logger.WarnCtx(ctx, "Context deadline exceeded during log pagination, returning partial logs",
				zap.Int("partialLogsCount", len(allLogs)),
				zap.Uint64("processedUpToBlock", currentFrom.Uint64()-1),
				zap.Uint64("targetToBlock", toBlock.Uint64()),
			)
			return allLogs, timeoutCtx.Err()
		default:
		}

		currentTo := new(big.Int).Add(currentFrom, new(big.Int).SetUint64(stepSize))
		if currentTo.Cmp(toBlock) > 0 {
			currentTo.Set(toBlock)
		}

		rangeQuery := query
		rangeQuery.FromBlock = new(big.Int).Set(currentFrom)
		rangeQuery.ToBlock = currentTo

		logs, err := h.getLogsWithRetry(timeoutCtx, rangeQuery, stepSize)
		if err != nil {
			if timeoutCtx.Err() != nil {
				return allLogs, timeoutCtx.Err()
			}
			return nil, fmt.Errorf("failed to get logs for range %d-%d: %w", currentFrom.Uint64(), currentTo.Uint64(), err)
		}

		allLogs = append(allLogs, logs...)
		currentFrom.SetUint64(currentTo.Uint64() + 1)
	}

	return allLogs, nil
}

func (h *PaginationHelper) calculateStepSize(ctx context.Context, query ethereum.FilterQuery) uint64 {
	const (
		defaultStepSize         = uint64(1_000_000)
		erc721TokenStepSize     = uint64(30_000_000)
		erc721OwnerStepSize     = uint64(10_000_000)
		erc1155OwnerStepSize    = uint64(10_000_000)
		erc1155ContractStepSize = uint64(5_000_000)
	)

	if len(query.Topics) == 0 {
		return defaultStepSize
	}

	eventSignatures := query.Topics[0]
	if len(eventSignatures) == 0 {
		return defaultStepSize
	}

	hasERC721Transfer := false
	hasERC1155Transfer := false
	for _, sig := range eventSignatures {
		if sig == TransferEventSignature || sig == EIP4906MetadataUpdateEventSignature {
			hasERC721Transfer = true
		}
		if sig == ERC1155TransferSingleEventSignature || sig == ERC1155TransferBatchEventSignature || sig == ERC1155URIEventSignature {
			hasERC1155Transfer = true
		}
	}

	hasTokenIDIndexed := false
	hasOwnerIndexed := false

	if hasERC721Transfer {
		if len(query.Topics) > 1 && len(query.Topics[1]) > 0 {
			hasOwnerIndexed = true
		}
		if len(query.Topics) > 2 && len(query.Topics[2]) > 0 {
			hasOwnerIndexed = true
		}
		if len(query.Topics) > 3 && len(query.Topics[3]) > 0 {
			hasTokenIDIndexed = true
		}
	} else if hasERC1155Transfer {
		if len(query.Topics) > 2 && len(query.Topics[2]) > 0 {
			hasOwnerIndexed = true
		}
		if len(query.Topics) > 3 && len(query.Topics[3]) > 0 {
			hasOwnerIndexed = true
		}
	}

	switch {
	case hasERC721Transfer && hasTokenIDIndexed:
		return erc721TokenStepSize
	case hasERC721Transfer && hasOwnerIndexed:
		return erc721OwnerStepSize
	case hasERC1155Transfer && hasOwnerIndexed:
		return erc1155OwnerStepSize
	case hasERC1155Transfer:
		logger.DebugCtx(ctx, "Using medium step size for ERC1155 contract query",
			zap.Uint64("stepSize", erc1155ContractStepSize))
		return erc1155ContractStepSize
	default:
		return defaultStepSize
	}
}

func (h *PaginationHelper) getLogsWithRetry(ctx context.Context, query ethereum.FilterQuery, stepSize uint64) ([]types.Log, error) {
	originalStepSize := stepSize
	currentStepSize := stepSize

	var allLogs []types.Log
	currentFrom := new(big.Int).Set(query.FromBlock)

	for currentFrom.Cmp(query.ToBlock) <= 0 {
		select {
		case <-ctx.Done():
			return allLogs, ctx.Err()
		default:
		}

		currentTo := new(big.Int).Add(currentFrom, new(big.Int).SetUint64(currentStepSize-1))
		if currentTo.Cmp(query.ToBlock) > 0 {
			currentTo.Set(query.ToBlock)
		}

		queryCopy := query
		queryCopy.FromBlock = new(big.Int).Set(currentFrom)
		queryCopy.ToBlock = new(big.Int).Set(currentTo)

		logs, err := h.ethClient.FilterLogs(ctx, queryCopy)
		if err == nil {
			allLogs = append(allLogs, logs...)
			currentFrom.SetUint64(currentTo.Uint64() + 1)
			currentStepSize = originalStepSize
			continue
		}

		if ctx.Err() != nil {
			return allLogs, ctx.Err()
		}

		if !IsTooManyResultsError(err) {
			return nil, err
		}

		// Cannot split further - return explicit error
		if currentFrom.Cmp(currentTo) == 0 {
			return nil, fmt.Errorf("too many results in single block %d: %w", currentFrom.Uint64(), err)
		}

		currentStepSize = currentStepSize / 2
		if currentStepSize == 0 {
			return nil, fmt.Errorf("step size exhausted at block range [%d, %d]: %w", currentFrom.Uint64(), currentTo.Uint64(), err)
		}

		h.clock.Sleep(time.Second)
	}

	return allLogs, nil
}

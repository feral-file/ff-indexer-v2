package workflows

import (
	"fmt"
	"time"

	logger "github.com/bitmark-inc/autonomy-logger"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
)

// IndexTokenOwners indexes tokens for multiple addresses sequentially
func (w *workerCore) IndexTokenOwners(ctx workflow.Context, addresses []string) error {
	logger.Info("Starting token owners indexing",
		zap.Strings("addresses", addresses),
		zap.Int("addressCount", len(addresses)),
	)

	// Process each address sequentially and wait for completion
	for _, address := range addresses {
		logger.Info("Processing address", zap.String("address", address))

		// Configure child workflow options
		childWorkflowOptions := workflow.ChildWorkflowOptions{
			WorkflowID:               fmt.Sprintf("index-token-owner-%s", address),
			WorkflowExecutionTimeout: 30 * time.Minute,
			WorkflowIDReusePolicy:    enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
			ParentClosePolicy:        enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
		}
		childCtx := workflow.WithChildOptions(ctx, childWorkflowOptions)

		// Execute child workflow and wait for completion
		err := workflow.ExecuteChildWorkflow(childCtx, w.IndexTokenOwner, address).Get(ctx, nil)
		if err != nil {
			logger.Error(fmt.Errorf("failed to index tokens for address: %w", err),
				zap.String("address", address),
			)
			// Continue with next address even if one fails
			continue
		}

		logger.Info("Completed indexing tokens for address",
			zap.String("address", address),
		)
	}

	logger.Info("Token owners indexing completed",
		zap.Int("addressCount", len(addresses)),
	)

	return nil
}

// IndexTokenOwner indexes all tokens held by a single address
// This is the parent workflow that delegates to blockchain-specific child workflows
func (w *workerCore) IndexTokenOwner(ctx workflow.Context, address string) error {
	logger.Info("Starting token owner indexing",
		zap.String("address", address),
	)

	// Determine blockchain from address format
	blockchain := domain.AddressToBlockchain(address)

	// Configure child workflow options
	childWorkflowOptions := workflow.ChildWorkflowOptions{
		WorkflowID:               fmt.Sprintf("index-token-owner-%s-%s", blockchain, address),
		WorkflowExecutionTimeout: 30 * time.Minute,
		WorkflowIDReusePolicy:    enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		ParentClosePolicy:        enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
	}
	childCtx := workflow.WithChildOptions(ctx, childWorkflowOptions)

	var err error
	switch blockchain {
	case domain.BlockchainTezos:
		err = workflow.ExecuteChildWorkflow(childCtx, w.IndexTezosTokenOwner, address).Get(ctx, nil)
	case domain.BlockchainEthereum:
		err = workflow.ExecuteChildWorkflow(childCtx, w.IndexEthereumTokenOwner, address).Get(ctx, nil)
	default:
		err = fmt.Errorf("unsupported blockchain for address: %s", address)
	}

	if err != nil {
		logger.Error(fmt.Errorf("failed to index tokens for owner: %w", err),
			zap.String("address", address),
			zap.String("blockchain", string(blockchain)),
		)
		return err
	}

	logger.Info("Token owner indexing completed",
		zap.String("address", address),
		zap.String("blockchain", string(blockchain)),
	)

	return nil
}

// IndexTezosTokenOwner indexes all tokens held by a Tezos address using bi-directional block range sweeping
func (w *workerCore) IndexTezosTokenOwner(ctx workflow.Context, address string) error {
	logger.Info("Starting Tezos token owner indexing with bi-directional block range sweeping",
		zap.String("address", address),
		zap.Uint64("chunkSize", w.config.TezosTokenSweepBlockChunkSize),
		zap.Uint64("startBlock", w.config.TezosTokenSweepStartBlock),
	)

	// Configure activity options
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)
	chainID := w.config.TezosChainID

	// Step 1: Ensure watched address record exists
	err := workflow.ExecuteActivity(ctx, w.executor.EnsureWatchedAddressExists, address, chainID, "workflow", "token_owner_indexing").Get(ctx, nil)
	if err != nil {
		logger.Error(fmt.Errorf("failed to ensure watched address exists: %w", err),
			zap.String("address", address),
			zap.String("chainID", string(chainID)),
		)
		return err
	}

	// Step 2: Get current indexing block range for this address and chain
	var rangeResult *BlockRangeResult
	err = workflow.ExecuteActivity(ctx, w.executor.GetIndexingBlockRangeForAddress, address, chainID).Get(ctx, &rangeResult)
	if err != nil {
		logger.Error(fmt.Errorf("failed to get indexing block range: %w", err),
			zap.String("address", address),
			zap.String("chainID", string(chainID)),
		)
		return err
	}

	storedMinBlock := rangeResult.MinBlock
	storedMaxBlock := rangeResult.MaxBlock

	logger.Info("Retrieved stored block range",
		zap.String("address", address),
		zap.Uint64("storedMinBlock", storedMinBlock),
		zap.Uint64("storedMaxBlock", storedMaxBlock),
	)

	// Step 3: Get the current latest block from TzKT
	var latestBlock uint64
	err = workflow.ExecuteActivity(ctx, w.executor.GetLatestTezosBlock).Get(ctx, &latestBlock)
	if err != nil {
		logger.Error(fmt.Errorf("failed to get latest Tezos block: %w", err),
			zap.String("address", address),
		)
		return err
	}

	logger.Info("Retrieved latest block from TzKT",
		zap.String("address", address),
		zap.Uint64("latestBlock", latestBlock),
	)

	// Step 4: Determine sweeping strategy
	if storedMinBlock == 0 && storedMaxBlock == 0 {
		// First run: No previous indexing exists
		// Query from current block backwards, continuing until reaching start block
		logger.Info("First run detected, sweeping from current block backwards",
			zap.Uint64("latestBlock", latestBlock),
		)

		fromBlock := latestBlock
		chunkProcessed := 0

		for fromBlock > w.config.TezosTokenSweepStartBlock {
			toBlock := fromBlock
			fromBlock = max(fromBlock-w.config.TezosTokenSweepBlockChunkSize+1, w.config.TezosTokenSweepStartBlock)

			logger.Info("Processing chunk (newest to oldest)",
				zap.String("address", address),
				zap.Uint64("fromBlock", fromBlock),
				zap.Uint64("toBlock", toBlock),
			)

			err := w.processTezosBlockChunk(ctx, address, fromBlock, toBlock)
			if err != nil {
				return err
			}
			chunkProcessed++

			// Update the stored range after successful chunk
			// For first run, we set max on first chunk and keep updating min as we go backward
			if chunkProcessed == 1 {
				// First chunk - this becomes our max_block
				err = workflow.ExecuteActivity(ctx, w.executor.UpdateIndexingBlockRangeForAddress, address, chainID, fromBlock, toBlock).Get(ctx, nil)
			} else {
				// Subsequent chunks - only update min_block
				err = workflow.ExecuteActivity(ctx, w.executor.UpdateIndexingBlockRangeForAddress, address, chainID, fromBlock, storedMaxBlock).Get(ctx, nil)
			}
			if err != nil {
				logger.Error(fmt.Errorf("failed to update indexing block range: %w", err),
					zap.String("address", address),
					zap.Uint64("fromBlock", fromBlock),
					zap.Uint64("toBlock", toBlock),
				)
				return err
			}

			logger.Info("Updated indexing block range after chunk",
				zap.Uint64("fromBlock", fromBlock),
				zap.Uint64("toBlock", toBlock),
			)

			// Update storedMaxBlock for next iteration (only on first chunk)
			if chunkProcessed == 1 {
				storedMaxBlock = toBlock
			}

			fromBlock--
		}

		logger.Info("Completed first run sweep")
	} else {
		// Subsequent run: Sweep both directions (both from newest to oldest)
		logger.Info("Subsequent run detected, sweeping in both directions",
			zap.Uint64("storedMinBlock", storedMinBlock),
			zap.Uint64("storedMaxBlock", storedMaxBlock),
			zap.Uint64("latestBlock", latestBlock),
		)

		// Part A: Sweep forward (new blocks): from stored max_block upward to latest block
		if latestBlock > storedMaxBlock {
			logger.Info("Sweeping forward for new blocks (upward from stored max to latest)",
				zap.Uint64("storedMaxBlock", storedMaxBlock),
				zap.Uint64("latestBlock", latestBlock),
			)

			toBlock := storedMaxBlock

			for toBlock < latestBlock {
				fromBlock := toBlock + 1
				toBlock = min(fromBlock+w.config.TezosTokenSweepBlockChunkSize-1, latestBlock)

				logger.Info("Processing forward sweep chunk",
					zap.String("address", address),
					zap.Uint64("fromBlock", fromBlock),
					zap.Uint64("toBlock", toBlock),
				)

				err := w.processTezosBlockChunk(ctx, address, fromBlock, toBlock)
				if err != nil {
					return err
				}

				// Update max_block after each successful chunk to avoid gaps
				err = workflow.ExecuteActivity(ctx, w.executor.UpdateIndexingBlockRangeForAddress, address, chainID, storedMinBlock, toBlock).Get(ctx, nil)
				if err != nil {
					logger.Error(fmt.Errorf("failed to update max block: %w", err),
						zap.String("address", address),
						zap.Uint64("newMaxBlock", toBlock),
					)
					return err
				}

				logger.Info("Updated max block after forward sweep chunk",
					zap.Uint64("newMaxBlock", toBlock),
				)
			}

			logger.Info("Completed forward sweep")

			// Update storedMaxBlock for backward sweep
			storedMaxBlock = latestBlock
		}

		// Part B: Sweep backward (historical data): from stored min_block down to start block (newest to oldest)
		if storedMinBlock > w.config.TezosTokenSweepStartBlock {
			logger.Info("Sweeping backward for historical data (newest to oldest)",
				zap.Uint64("storedMinBlock", storedMinBlock),
				zap.Uint64("startBlock", w.config.TezosTokenSweepStartBlock),
			)

			fromBlock := storedMinBlock - 1

			for fromBlock > w.config.TezosTokenSweepStartBlock {
				toBlock := fromBlock
				fromBlock = max(fromBlock-w.config.TezosTokenSweepBlockChunkSize+1, w.config.TezosTokenSweepStartBlock)

				logger.Info("Processing backward sweep chunk",
					zap.String("address", address),
					zap.Uint64("fromBlock", fromBlock),
					zap.Uint64("toBlock", toBlock),
				)

				err := w.processTezosBlockChunk(ctx, address, fromBlock, toBlock)
				if err != nil {
					return err
				}

				// Update min_block after successful chunk
				err = workflow.ExecuteActivity(ctx, w.executor.UpdateIndexingBlockRangeForAddress, address, chainID, fromBlock, storedMaxBlock).Get(ctx, nil)
				if err != nil {
					logger.Error(fmt.Errorf("failed to update min block: %w", err),
						zap.String("address", address),
						zap.Uint64("newMinBlock", fromBlock),
					)
					return err
				}

				logger.Info("Updated min block after backward sweep chunk",
					zap.Uint64("newMinBlock", fromBlock),
				)

				fromBlock--
			}

			logger.Info("Completed backward sweep")
		}
	}

	logger.Info("Tezos token owner indexing completed", zap.String("address", address))

	return nil
}

// processTezosBlockChunk processes a single block range chunk for Tezos
func (w *workerCore) processTezosBlockChunk(ctx workflow.Context, address string, fromBlock, toBlock uint64) error {
	logger.Info("Processing Tezos block range chunk",
		zap.String("address", address),
		zap.Uint64("fromBlock", fromBlock),
		zap.Uint64("toBlock", toBlock),
	)

	// Query token CIDs for this block range
	var tokenCIDs []domain.TokenCID
	err := workflow.ExecuteActivity(ctx, w.executor.GetTezosTokenCIDsByAccountWithinBlockRange, address, fromBlock, toBlock).Get(ctx, &tokenCIDs)
	if err != nil {
		logger.Error(fmt.Errorf("failed to get Tezos token CIDs for block range: %w", err),
			zap.String("address", address),
			zap.Uint64("fromBlock", fromBlock),
			zap.Uint64("toBlock", toBlock),
		)
		return err
	}

	logger.Info("Retrieved token CIDs for block range",
		zap.String("address", address),
		zap.Uint64("fromBlock", fromBlock),
		zap.Uint64("toBlock", toBlock),
		zap.Int("tokenCount", len(tokenCIDs)),
	)

	// If tokens found, trigger child workflow to index them
	if len(tokenCIDs) > 0 {
		indexTokensWorkflowOptions := workflow.ChildWorkflowOptions{
			WorkflowExecutionTimeout: 15 * time.Minute,
			ParentClosePolicy:        enums.PARENT_CLOSE_POLICY_ABANDON,
		}
		indexTokensCtx := workflow.WithChildOptions(ctx, indexTokensWorkflowOptions)

		// Execute and wait for this chunk to complete before moving to next chunk
		err = workflow.ExecuteChildWorkflow(indexTokensCtx, w.IndexTokens, tokenCIDs).Get(ctx, nil)
		if err != nil {
			logger.Error(fmt.Errorf("failed to trigger index tokens workflow: %w", err),
				zap.String("address", address),
				zap.Uint64("fromBlock", fromBlock),
				zap.Uint64("toBlock", toBlock),
				zap.Int("tokenCount", len(tokenCIDs)),
			)
			return err
		}

		logger.Info("Completed indexing tokens for chunk",
			zap.String("address", address),
			zap.Uint64("fromBlock", fromBlock),
			zap.Uint64("toBlock", toBlock),
			zap.Int("tokenCount", len(tokenCIDs)),
		)
	}

	return nil
}

// IndexEthereumTokenOwner indexes all tokens held by an Ethereum address using bi-directional block range sweeping
func (w *workerCore) IndexEthereumTokenOwner(ctx workflow.Context, address string) error {
	logger.Info("Starting Ethereum token owner indexing with bi-directional block range sweeping",
		zap.String("address", address),
		zap.Uint64("chunkSize", w.config.EthereumTokenSweepBlockChunkSize),
		zap.Uint64("startBlock", w.config.EthereumTokenSweepStartBlock),
	)

	// Configure activity options
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)
	chainID := w.config.EthereumChainID

	// Step 1: Ensure watched address record exists
	err := workflow.ExecuteActivity(ctx, w.executor.EnsureWatchedAddressExists, address, chainID, "workflow", "token_owner_indexing").Get(ctx, nil)
	if err != nil {
		logger.Error(fmt.Errorf("failed to ensure watched address exists: %w", err),
			zap.String("address", address),
			zap.String("chainID", string(chainID)),
		)
		return err
	}

	// Step 2: Get current indexing block range for this address and chain
	var rangeResult *BlockRangeResult
	err = workflow.ExecuteActivity(ctx, w.executor.GetIndexingBlockRangeForAddress, address, chainID).Get(ctx, &rangeResult)
	if err != nil {
		logger.Error(fmt.Errorf("failed to get indexing block range: %w", err),
			zap.String("address", address),
			zap.String("chainID", string(chainID)),
		)
		return err
	}

	storedMinBlock := rangeResult.MinBlock
	storedMaxBlock := rangeResult.MaxBlock

	logger.Info("Retrieved stored block range",
		zap.String("address", address),
		zap.Uint64("storedMinBlock", storedMinBlock),
		zap.Uint64("storedMaxBlock", storedMaxBlock),
	)

	// Step 3: Get the current latest block from blockchain
	var latestBlock uint64
	err = workflow.ExecuteActivity(ctx, w.executor.GetLatestEthereumBlock).Get(ctx, &latestBlock)
	if err != nil {
		logger.Error(fmt.Errorf("failed to get latest block: %w", err),
			zap.String("address", address),
		)
		return err
	}

	logger.Info("Retrieved latest block from blockchain",
		zap.String("address", address),
		zap.Uint64("latestBlock", latestBlock),
	)

	// Step 4: Determine sweeping strategy
	if storedMinBlock == 0 && storedMaxBlock == 0 {
		// First run: No previous indexing exists
		// Query from current block backwards, continuing until reaching start block
		logger.Info("First run detected, sweeping from current block backwards",
			zap.Uint64("latestBlock", latestBlock),
		)

		fromBlock := latestBlock
		chunkProcessed := 0

		for fromBlock > w.config.EthereumTokenSweepStartBlock {
			toBlock := fromBlock
			fromBlock = max(fromBlock-w.config.EthereumTokenSweepBlockChunkSize+1, w.config.EthereumTokenSweepStartBlock)

			logger.Info("Processing chunk (newest to oldest)",
				zap.String("address", address),
				zap.Uint64("fromBlock", fromBlock),
				zap.Uint64("toBlock", toBlock),
			)

			err := w.processEthereumBlockChunk(ctx, address, fromBlock, toBlock)
			if err != nil {
				return err
			}
			chunkProcessed++

			// Update the stored range after successful chunk
			// For first run, we set max on first chunk and keep updating min as we go backward
			if chunkProcessed == 1 {
				// First chunk - this becomes our max_block
				err = workflow.ExecuteActivity(ctx, w.executor.UpdateIndexingBlockRangeForAddress, address, chainID, fromBlock, toBlock).Get(ctx, nil)
			} else {
				// Subsequent chunks - only update min_block
				err = workflow.ExecuteActivity(ctx, w.executor.UpdateIndexingBlockRangeForAddress, address, chainID, fromBlock, storedMaxBlock).Get(ctx, nil)
			}
			if err != nil {
				logger.Error(fmt.Errorf("failed to update indexing block range: %w", err),
					zap.String("address", address),
					zap.Uint64("fromBlock", fromBlock),
					zap.Uint64("toBlock", toBlock),
				)
				return err
			}

			logger.Info("Updated indexing block range after chunk",
				zap.Uint64("fromBlock", fromBlock),
				zap.Uint64("toBlock", toBlock),
			)

			// Update storedMaxBlock for next iteration (only on first chunk)
			if chunkProcessed == 1 {
				storedMaxBlock = toBlock
			}

			fromBlock--
		}

		logger.Info("Completed first run sweep")
	} else {
		// Subsequent run: Sweep both directions
		logger.Info("Subsequent run detected, sweeping in both directions",
			zap.Uint64("storedMinBlock", storedMinBlock),
			zap.Uint64("storedMaxBlock", storedMaxBlock),
			zap.Uint64("latestBlock", latestBlock),
		)

		// Part A: Sweep forward (new blocks): from stored max_block upward to latest block
		if latestBlock > storedMaxBlock {
			logger.Info("Sweeping forward for new blocks (upward from stored max to latest)",
				zap.Uint64("storedMaxBlock", storedMaxBlock),
				zap.Uint64("latestBlock", latestBlock),
			)

			toBlock := storedMaxBlock

			for toBlock < latestBlock {
				fromBlock := toBlock + 1
				toBlock = min(fromBlock+w.config.EthereumTokenSweepBlockChunkSize-1, latestBlock)

				logger.Info("Processing forward sweep chunk",
					zap.String("address", address),
					zap.Uint64("fromBlock", fromBlock),
					zap.Uint64("toBlock", toBlock),
				)

				err := w.processEthereumBlockChunk(ctx, address, fromBlock, toBlock)
				if err != nil {
					return err
				}

				// Update max_block after each successful chunk to avoid gaps
				err = workflow.ExecuteActivity(ctx, w.executor.UpdateIndexingBlockRangeForAddress, address, chainID, storedMinBlock, toBlock).Get(ctx, nil)
				if err != nil {
					logger.Error(fmt.Errorf("failed to update max block: %w", err),
						zap.String("address", address),
						zap.Uint64("newMaxBlock", toBlock),
					)
					return err
				}

				logger.Info("Updated max block after forward sweep chunk",
					zap.Uint64("newMaxBlock", toBlock),
				)
			}

			logger.Info("Completed forward sweep")

			// Update storedMaxBlock for backward sweep
			storedMaxBlock = latestBlock
		}

		// Part B: Sweep backward (historical data): from stored min_block down to start block (newest to oldest)
		if storedMinBlock > w.config.EthereumTokenSweepStartBlock {
			logger.Info("Sweeping backward for historical data (newest to oldest)",
				zap.Uint64("storedMinBlock", storedMinBlock),
				zap.Uint64("startBlock", w.config.EthereumTokenSweepStartBlock),
			)

			fromBlock := storedMinBlock - 1

			for fromBlock > w.config.EthereumTokenSweepStartBlock {
				toBlock := fromBlock
				fromBlock = max(fromBlock-w.config.EthereumTokenSweepBlockChunkSize+1, w.config.EthereumTokenSweepStartBlock)

				logger.Info("Processing backward sweep chunk",
					zap.String("address", address),
					zap.Uint64("fromBlock", fromBlock),
					zap.Uint64("toBlock", toBlock),
				)

				err := w.processEthereumBlockChunk(ctx, address, fromBlock, toBlock)
				if err != nil {
					return err
				}

				// Update min_block after successful chunk
				err = workflow.ExecuteActivity(ctx, w.executor.UpdateIndexingBlockRangeForAddress, address, chainID, fromBlock, storedMaxBlock).Get(ctx, nil)
				if err != nil {
					logger.Error(fmt.Errorf("failed to update min block: %w", err),
						zap.String("address", address),
						zap.Uint64("newMinBlock", fromBlock),
					)
					return err
				}

				logger.Info("Updated min block after backward sweep chunk",
					zap.Uint64("newMinBlock", fromBlock),
				)

				fromBlock--
			}

			logger.Info("Completed backward sweep")
		}
	}

	logger.Info("Ethereum token owner indexing completed", zap.String("address", address))

	return nil
}

// processEthereumBlockChunk processes a single block range chunk for Ethereum
func (w *workerCore) processEthereumBlockChunk(ctx workflow.Context, address string, fromBlock, toBlock uint64) error {
	logger.Info("Processing Ethereum block range chunk",
		zap.String("address", address),
		zap.Uint64("fromBlock", fromBlock),
		zap.Uint64("toBlock", toBlock),
	)

	// Query token CIDs for this block range
	var tokenCIDs []domain.TokenCID
	err := workflow.ExecuteActivity(ctx, w.executor.GetEthereumTokenCIDsByOwnerWithinBlockRange, address, fromBlock, toBlock).Get(ctx, &tokenCIDs)
	if err != nil {
		logger.Error(fmt.Errorf("failed to get Ethereum token CIDs for block range: %w", err),
			zap.String("address", address),
			zap.Uint64("fromBlock", fromBlock),
			zap.Uint64("toBlock", toBlock),
		)
		return err
	}

	logger.Info("Retrieved token CIDs for block range",
		zap.String("address", address),
		zap.Uint64("fromBlock", fromBlock),
		zap.Uint64("toBlock", toBlock),
		zap.Int("tokenCount", len(tokenCIDs)),
	)

	// If tokens found, trigger child workflow to index them
	if len(tokenCIDs) > 0 {
		indexTokensWorkflowOptions := workflow.ChildWorkflowOptions{
			WorkflowExecutionTimeout: 15 * time.Minute,
			ParentClosePolicy:        enums.PARENT_CLOSE_POLICY_ABANDON,
		}
		indexTokensCtx := workflow.WithChildOptions(ctx, indexTokensWorkflowOptions)

		// Execute and wait for this chunk to complete before moving to next chunk
		err = workflow.ExecuteChildWorkflow(indexTokensCtx, w.IndexTokens, tokenCIDs).Get(ctx, nil)
		if err != nil {
			logger.Error(fmt.Errorf("failed to trigger index tokens workflow: %w", err),
				zap.String("address", address),
				zap.Uint64("fromBlock", fromBlock),
				zap.Uint64("toBlock", toBlock),
				zap.Int("tokenCount", len(tokenCIDs)),
			)
			return err
		}

		logger.Info("Completed indexing tokens for chunk",
			zap.String("address", address),
			zap.Uint64("fromBlock", fromBlock),
			zap.Uint64("toBlock", toBlock),
			zap.Int("tokenCount", len(tokenCIDs)),
		)
	}

	return nil
}

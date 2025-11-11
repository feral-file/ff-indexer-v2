package workflows

import (
	"fmt"
	"sort"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
)

const (
	// TOKEN_INDEXING_CHUNK_SIZE is the number of tokens to process in a single batch
	// This balances between resumability (smaller chunks) and efficiency (fewer workflow calls)
	TOKEN_INDEXING_CHUNK_SIZE = 20
)

// IndexTokenOwners indexes tokens for multiple addresses sequentially
func (w *workerCore) IndexTokenOwners(ctx workflow.Context, addresses []string) error {
	logger.InfoWf(ctx, "Starting token owners indexing",
		zap.Strings("addresses", addresses),
		zap.Int("addressCount", len(addresses)),
	)

	// Process each address sequentially and wait for completion
	for _, address := range addresses {
		logger.InfoWf(ctx, "Processing address", zap.String("address", address))

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
			logger.ErrorWf(ctx,
				fmt.Errorf("failed to index tokens for address"),
				zap.Error(err),
				zap.String("address", address),
			)
			// Continue with next address even if one fails
			continue
		}

		logger.InfoWf(ctx, "Completed indexing tokens for address",
			zap.String("address", address),
		)
	}

	logger.InfoWf(ctx, "Token owners indexing completed",
		zap.Int("addressCount", len(addresses)),
	)

	return nil
}

// IndexTokenOwner indexes all tokens held by a single address
// This is the parent workflow that delegates to blockchain-specific child workflows
func (w *workerCore) IndexTokenOwner(ctx workflow.Context, address string) error {
	logger.InfoWf(ctx, "Starting token owner indexing",
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
		logger.ErrorWf(ctx,
			fmt.Errorf("failed to index tokens for owner"),
			zap.Error(err),
			zap.String("address", address),
			zap.String("blockchain", string(blockchain)),
		)
		return err
	}

	logger.InfoWf(ctx, "Token owner indexing completed",
		zap.String("address", address),
		zap.String("blockchain", string(blockchain)),
	)

	return nil
}

// sortTokensByBlock sorts tokens by block number in the specified order
func sortTokensByBlock(tokens []domain.TokenWithBlock, descending bool) {
	sort.Slice(tokens, func(i, j int) bool {
		if descending {
			return tokens[i].BlockNumber > tokens[j].BlockNumber
		}
		return tokens[i].BlockNumber < tokens[j].BlockNumber
	})
}

// chunkTokensByCount splits tokens into chunks of specified size
func chunkTokensByCount(tokens []domain.TokenWithBlock, chunkSize int) [][]domain.TokenWithBlock {
	var chunks [][]domain.TokenWithBlock
	for i := 0; i < len(tokens); i += chunkSize {
		end := min(i+chunkSize, len(tokens))
		chunks = append(chunks, tokens[i:end])
	}
	return chunks
}

// tokenChunkInfo holds extracted information from a token chunk
type tokenChunkInfo struct {
	tokenCIDs []domain.TokenCID
	minBlock  uint64
	maxBlock  uint64
}

// extractChunkInfo extracts TokenCIDs and block range from a token chunk in a single pass
func extractChunkInfo(tokens []domain.TokenWithBlock) tokenChunkInfo {
	if len(tokens) == 0 {
		return tokenChunkInfo{tokenCIDs: []domain.TokenCID{}, minBlock: 0, maxBlock: 0}
	}

	info := tokenChunkInfo{
		tokenCIDs: make([]domain.TokenCID, len(tokens)),
		minBlock:  tokens[0].BlockNumber,
		maxBlock:  tokens[0].BlockNumber,
	}

	for i, token := range tokens {
		info.tokenCIDs[i] = token.TokenCID
		if token.BlockNumber < info.minBlock {
			info.minBlock = token.BlockNumber
		}
		if token.BlockNumber > info.maxBlock {
			info.maxBlock = token.BlockNumber
		}
	}

	return info
}

// IndexTezosTokenOwner indexes all tokens held by a Tezos address
// Uses bi-directional block range sweeping: backward first (historical), then forward (latest updates)
func (w *workerCore) IndexTezosTokenOwner(ctx workflow.Context, address string) error {
	logger.InfoWf(ctx, "Starting Tezos token owner indexing",
		zap.String("address", address),
		zap.Uint64("startBlock", w.config.TezosTokenSweepStartBlock),
	)

	// Configure activity options
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 1,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)
	chainID := w.config.TezosChainID

	// Step 1: Ensure watched address record exists
	err := workflow.ExecuteActivity(ctx, w.executor.EnsureWatchedAddressExists, address, chainID, "workflow", "token_owner_indexing").Get(ctx, nil)
	if err != nil {
		logger.ErrorWf(ctx,
			fmt.Errorf("failed to ensure watched address exists"),
			zap.Error(err),
			zap.String("address", address),
			zap.String("chainID", string(chainID)),
		)
		return err
	}

	// Step 2: Get current indexing block range for this address and chain
	var rangeResult *BlockRangeResult
	err = workflow.ExecuteActivity(ctx, w.executor.GetIndexingBlockRangeForAddress, address, chainID).Get(ctx, &rangeResult)
	if err != nil {
		logger.ErrorWf(ctx,
			fmt.Errorf("failed to get indexing block range"),
			zap.Error(err),
			zap.String("address", address),
			zap.String("chainID", string(chainID)),
		)
		return err
	}

	storedMinBlock := rangeResult.MinBlock
	storedMaxBlock := rangeResult.MaxBlock

	logger.InfoWf(ctx, "Retrieved stored block range",
		zap.String("address", address),
		zap.Uint64("storedMinBlock", storedMinBlock),
		zap.Uint64("storedMaxBlock", storedMaxBlock),
	)

	// Step 3: Get the current latest block from TzKT
	var latestBlock uint64
	err = workflow.ExecuteActivity(ctx, w.executor.GetLatestTezosBlock).Get(ctx, &latestBlock)
	if err != nil {
		logger.ErrorWf(ctx,
			fmt.Errorf("failed to get latest Tezos block"),
			zap.Error(err),
			zap.String("address", address),
		)
		return err
	}

	logger.InfoWf(ctx, "Retrieved latest block from TzKT",
		zap.String("address", address),
		zap.Uint64("latestBlock", latestBlock),
	)

	// Step 4: Determine sweeping strategy
	if storedMinBlock == 0 && storedMaxBlock == 0 {
		// First run: No previous indexing exists
		// Fetch entire range from start to latest, process in chunks
		logger.InfoWf(ctx, "First run detected, fetching all tokens from start to latest",
			zap.Uint64("startBlock", w.config.TezosTokenSweepStartBlock),
			zap.Uint64("latestBlock", latestBlock),
		)

		var allTokens []domain.TokenWithBlock
		err = workflow.ExecuteActivity(ctx, w.executor.GetTezosTokenCIDsByAccountWithinBlockRange,
			address, w.config.TezosTokenSweepStartBlock, latestBlock).Get(ctx, &allTokens)
		if err != nil {
			logger.ErrorWf(ctx,
				fmt.Errorf("failed to fetch tokens"),
				zap.Error(err),
				zap.String("address", address),
			)
			return err
		}

		logger.InfoWf(ctx, "Retrieved all tokens for first run",
			zap.String("address", address),
			zap.Int("tokenCount", len(allTokens)),
		)

		// Sort by block number (descending - newest first) and process in chunks
		sortTokensByBlock(allTokens, true)
		chunks := chunkTokensByCount(allTokens, TOKEN_INDEXING_CHUNK_SIZE)

		// Store the actual scanned block range (not token block range)
		scannedMinBlock := w.config.TezosTokenSweepStartBlock
		scannedMaxBlock := latestBlock

		for i, chunk := range chunks {
			info := extractChunkInfo(chunk)

			logger.InfoWf(ctx, "Processing token chunk",
				zap.Int("chunkIndex", i+1),
				zap.Int("totalChunks", len(chunks)),
				zap.Int("tokenCount", len(info.tokenCIDs)),
				zap.Uint64("tokenMinBlock", info.minBlock),
				zap.Uint64("tokenMaxBlock", info.maxBlock),
			)

			// Index tokens
			if err := w.indexTokenChunk(ctx, info.tokenCIDs); err != nil {
				return err
			}

			// Update block range after each successful chunk for resumability
			// For first run descending: set max on first chunk, then progressively update min
			if i == 0 {
				// First chunk - establish the max_block (scanned range end)
				err = workflow.ExecuteActivity(ctx, w.executor.UpdateIndexingBlockRangeForAddress,
					address, chainID, info.minBlock, scannedMaxBlock).Get(ctx, nil)
			} else {
				// Subsequent chunks - progressively update min_block toward start
				err = workflow.ExecuteActivity(ctx, w.executor.UpdateIndexingBlockRangeForAddress,
					address, chainID, info.minBlock, storedMaxBlock).Get(ctx, nil)
			}
			if err != nil {
				logger.Error(fmt.Errorf("failed to update block range: %w", err), zap.String("address", address))
				return err
			}

			if i == 0 {
				storedMaxBlock = scannedMaxBlock
			}

			logger.InfoWf(ctx, "Updated block range after chunk",
				zap.Uint64("currentMinBlock", info.minBlock),
				zap.Uint64("currentMaxBlock", storedMaxBlock),
			)
		}

		// Final update to ensure we mark the complete scanned range
		err = workflow.ExecuteActivity(ctx, w.executor.UpdateIndexingBlockRangeForAddress,
			address, chainID, scannedMinBlock, scannedMaxBlock).Get(ctx, nil)
		if err != nil {
			logger.ErrorWf(ctx,
				fmt.Errorf("failed to update final block range"),
				zap.Error(err),
				zap.String("address", address),
			)
			return err
		}

		logger.InfoWf(ctx, "Completed first run sweep",
			zap.Uint64("scannedMinBlock", scannedMinBlock),
			zap.Uint64("scannedMaxBlock", scannedMaxBlock),
		)
	} else {
		// Subsequent run: Sweep backward first (historical), then forward (latest updates)
		logger.InfoWf(ctx, "Subsequent run detected, sweeping backward then forward",
			zap.Uint64("storedMinBlock", storedMinBlock),
			zap.Uint64("storedMaxBlock", storedMaxBlock),
			zap.Uint64("latestBlock", latestBlock),
		)

		// Part A: Sweep backward (historical data) - FIRST
		if storedMinBlock > w.config.TezosTokenSweepStartBlock {
			logger.InfoWf(ctx, "Sweeping backward for historical data",
				zap.Uint64("startBlock", w.config.TezosTokenSweepStartBlock),
				zap.Uint64("storedMinBlock", storedMinBlock),
			)

			var backwardTokens []domain.TokenWithBlock
			err = workflow.ExecuteActivity(ctx, w.executor.GetTezosTokenCIDsByAccountWithinBlockRange,
				address, w.config.TezosTokenSweepStartBlock, storedMinBlock-1).Get(ctx, &backwardTokens)
			if err != nil {
				logger.ErrorWf(ctx,
					fmt.Errorf("failed to fetch backward tokens"),
					zap.Error(err),
					zap.String("address", address),
				)
				return err
			}

			logger.InfoWf(ctx, "Retrieved backward sweep tokens",
				zap.String("address", address),
				zap.Int("tokenCount", len(backwardTokens)),
			)

			// Sort by block (descending - newest first) and process in chunks
			sortTokensByBlock(backwardTokens, true)
			chunks := chunkTokensByCount(backwardTokens, TOKEN_INDEXING_CHUNK_SIZE)

			// Store the actual scanned block range (not token block range)
			scannedMinBlock := w.config.TezosTokenSweepStartBlock
			scannedMaxBlock := storedMinBlock - 1

			for i, chunk := range chunks {
				info := extractChunkInfo(chunk)

				logger.InfoWf(ctx, "Processing backward chunk",
					zap.Int("chunkIndex", i+1),
					zap.Int("totalChunks", len(chunks)),
					zap.Int("tokenCount", len(info.tokenCIDs)),
					zap.Uint64("tokenMinBlock", info.minBlock),
					zap.Uint64("tokenMaxBlock", info.maxBlock),
				)

				if err := w.indexTokenChunk(ctx, info.tokenCIDs); err != nil {
					return err
				}

				// Update min_block after each successful chunk for resumability
				// Progressively update min_block toward start
				err = workflow.ExecuteActivity(ctx, w.executor.UpdateIndexingBlockRangeForAddress,
					address, chainID, info.minBlock, storedMaxBlock).Get(ctx, nil)
				if err != nil {
					logger.ErrorWf(ctx,
						fmt.Errorf("failed to update min block"),
						zap.Error(err),
						zap.String("address", address),
					)
					return err
				}

				logger.InfoWf(ctx, "Updated min block after chunk", zap.Uint64("currentMinBlock", info.minBlock))
			}

			// Final update to ensure we mark the complete scanned range
			err = workflow.ExecuteActivity(ctx, w.executor.UpdateIndexingBlockRangeForAddress,
				address, chainID, scannedMinBlock, storedMaxBlock).Get(ctx, nil)
			if err != nil {
				logger.ErrorWf(ctx,
					fmt.Errorf("failed to update final min block"),
					zap.Error(err),
					zap.String("address", address),
				)
				return err
			}

			storedMinBlock = scannedMinBlock
			logger.InfoWf(ctx, "Completed backward sweep",
				zap.Uint64("scannedMinBlock", scannedMinBlock),
				zap.Uint64("scannedMaxBlock", scannedMaxBlock),
			)
		}

		// Part B: Sweep forward (latest updates) - SECOND
		if latestBlock > storedMaxBlock {
			logger.InfoWf(ctx, "Sweeping forward for latest updates",
				zap.Uint64("storedMaxBlock", storedMaxBlock),
				zap.Uint64("latestBlock", latestBlock),
			)

			var forwardTokens []domain.TokenWithBlock
			err = workflow.ExecuteActivity(ctx, w.executor.GetTezosTokenCIDsByAccountWithinBlockRange,
				address, storedMaxBlock+1, latestBlock).Get(ctx, &forwardTokens)
			if err != nil {
				logger.ErrorWf(ctx,
					fmt.Errorf("failed to fetch forward tokens"),
					zap.Error(err),
					zap.String("address", address),
				)
				return err
			}

			logger.InfoWf(ctx, "Retrieved forward sweep tokens",
				zap.String("address", address),
				zap.Int("tokenCount", len(forwardTokens)),
			)

			// Sort by block (ascending - oldest first) and process in chunks
			sortTokensByBlock(forwardTokens, false)
			chunks := chunkTokensByCount(forwardTokens, TOKEN_INDEXING_CHUNK_SIZE)

			// Store the actual scanned block range (not token block range)
			scannedMinBlock := storedMaxBlock + 1
			scannedMaxBlock := latestBlock

			for i, chunk := range chunks {
				info := extractChunkInfo(chunk)

				logger.InfoWf(ctx, "Processing forward chunk",
					zap.Int("chunkIndex", i+1),
					zap.Int("totalChunks", len(chunks)),
					zap.Int("tokenCount", len(info.tokenCIDs)),
					zap.Uint64("tokenMinBlock", info.minBlock),
					zap.Uint64("tokenMaxBlock", info.maxBlock),
				)

				if err := w.indexTokenChunk(ctx, info.tokenCIDs); err != nil {
					return err
				}

				// Update max_block after each successful chunk for resumability
				// Progressively update max_block toward latest
				err = workflow.ExecuteActivity(ctx, w.executor.UpdateIndexingBlockRangeForAddress,
					address, chainID, storedMinBlock, info.maxBlock).Get(ctx, nil)
				if err != nil {
					logger.ErrorWf(ctx,
						fmt.Errorf("failed to update max block"),
						zap.Error(err),
						zap.String("address", address),
					)
					return err
				}

				logger.InfoWf(ctx, "Updated max block after chunk", zap.Uint64("currentMaxBlock", info.maxBlock))
			}

			// Final update to ensure we mark the complete scanned range
			err = workflow.ExecuteActivity(ctx, w.executor.UpdateIndexingBlockRangeForAddress,
				address, chainID, storedMinBlock, scannedMaxBlock).Get(ctx, nil)
			if err != nil {
				logger.ErrorWf(ctx,
					fmt.Errorf("failed to update final max block"),
					zap.Error(err),
					zap.String("address", address),
				)
				return err
			}

			logger.InfoWf(ctx, "Completed forward sweep",
				zap.Uint64("scannedMinBlock", scannedMinBlock),
				zap.Uint64("scannedMaxBlock", scannedMaxBlock),
			)
		}
	}

	logger.InfoWf(ctx, "Tezos token owner indexing completed", zap.String("address", address))
	return nil
}

// IndexEthereumTokenOwner indexes all tokens held by an Ethereum address
// Uses bi-directional block range sweeping: backward first (historical), then forward (latest updates)
func (w *workerCore) IndexEthereumTokenOwner(ctx workflow.Context, address string) error {
	logger.InfoWf(ctx, "Starting Ethereum token owner indexing",
		zap.String("address", address),
		zap.Uint64("startBlock", w.config.EthereumTokenSweepStartBlock),
	)

	// Configure activity options
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 1,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)
	chainID := w.config.EthereumChainID

	// Step 1: Ensure watched address record exists
	err := workflow.ExecuteActivity(ctx, w.executor.EnsureWatchedAddressExists, address, chainID, "workflow", "token_owner_indexing").Get(ctx, nil)
	if err != nil {
		logger.ErrorWf(ctx,
			fmt.Errorf("failed to ensure watched address exists"),
			zap.Error(err),
			zap.String("address", address),
			zap.String("chainID", string(chainID)),
		)
		return err
	}

	// Step 2: Get current indexing block range for this address and chain
	var rangeResult *BlockRangeResult
	err = workflow.ExecuteActivity(ctx, w.executor.GetIndexingBlockRangeForAddress, address, chainID).Get(ctx, &rangeResult)
	if err != nil {
		logger.ErrorWf(ctx,
			fmt.Errorf("failed to get indexing block range"),
			zap.Error(err),
			zap.String("address", address),
			zap.String("chainID", string(chainID)),
		)
		return err
	}

	storedMinBlock := rangeResult.MinBlock
	storedMaxBlock := rangeResult.MaxBlock

	logger.InfoWf(ctx, "Retrieved stored block range",
		zap.String("address", address),
		zap.Uint64("storedMinBlock", storedMinBlock),
		zap.Uint64("storedMaxBlock", storedMaxBlock),
	)

	// Step 3: Get the current latest block from blockchain
	var latestBlock uint64
	err = workflow.ExecuteActivity(ctx, w.executor.GetLatestEthereumBlock).Get(ctx, &latestBlock)
	if err != nil {
		logger.ErrorWf(ctx,
			fmt.Errorf("failed to get latest block"),
			zap.Error(err),
			zap.String("address", address),
		)
		return err
	}

	logger.InfoWf(ctx, "Retrieved latest block from blockchain",
		zap.String("address", address),
		zap.Uint64("latestBlock", latestBlock),
	)

	// Step 4: Determine sweeping strategy
	if storedMinBlock == 0 && storedMaxBlock == 0 {
		// First run: No previous indexing exists
		// Fetch entire range from start to latest, process in chunks
		logger.InfoWf(ctx, "First run detected, fetching all tokens from start to latest",
			zap.Uint64("startBlock", w.config.EthereumTokenSweepStartBlock),
			zap.Uint64("latestBlock", latestBlock),
		)

		var allTokens []domain.TokenWithBlock
		err = workflow.ExecuteActivity(ctx, w.executor.GetEthereumTokenCIDsByOwnerWithinBlockRange,
			address, w.config.EthereumTokenSweepStartBlock, latestBlock).Get(ctx, &allTokens)
		if err != nil {
			logger.ErrorWf(ctx,
				fmt.Errorf("failed to fetch tokens"),
				zap.Error(err),
				zap.String("address", address),
			)
			return err
		}

		logger.InfoWf(ctx, "Retrieved all tokens for first run",
			zap.String("address", address),
			zap.Int("tokenCount", len(allTokens)),
		)

		// Sort by block number (descending - newest first) and process in chunks
		sortTokensByBlock(allTokens, true)
		chunks := chunkTokensByCount(allTokens, TOKEN_INDEXING_CHUNK_SIZE)

		// Store the actual scanned block range (not token block range)
		scannedMinBlock := w.config.EthereumTokenSweepStartBlock
		scannedMaxBlock := latestBlock

		for i, chunk := range chunks {
			info := extractChunkInfo(chunk)

			logger.InfoWf(ctx, "Processing token chunk",
				zap.Int("chunkIndex", i+1),
				zap.Int("totalChunks", len(chunks)),
				zap.Int("tokenCount", len(info.tokenCIDs)),
				zap.Uint64("tokenMinBlock", info.minBlock),
				zap.Uint64("tokenMaxBlock", info.maxBlock),
			)

			// Index tokens
			if err := w.indexTokenChunk(ctx, info.tokenCIDs); err != nil {
				return err
			}

			// Update block range after each successful chunk for resumability
			// For first run descending: set max on first chunk, then progressively update min
			if i == 0 {
				// First chunk - establish the max_block (scanned range end)
				err = workflow.ExecuteActivity(ctx, w.executor.UpdateIndexingBlockRangeForAddress,
					address, chainID, info.minBlock, scannedMaxBlock).Get(ctx, nil)
			} else {
				// Subsequent chunks - progressively update min_block toward start
				err = workflow.ExecuteActivity(ctx, w.executor.UpdateIndexingBlockRangeForAddress,
					address, chainID, info.minBlock, storedMaxBlock).Get(ctx, nil)
			}
			if err != nil {
				logger.ErrorWf(ctx,
					fmt.Errorf("failed to update block range"),
					zap.Error(err),
					zap.String("address", address),
				)
				return err
			}

			if i == 0 {
				storedMaxBlock = scannedMaxBlock
			}

			logger.InfoWf(ctx, "Updated block range after chunk",
				zap.Uint64("currentMinBlock", info.minBlock),
				zap.Uint64("currentMaxBlock", storedMaxBlock),
			)
		}

		// Final update to ensure we mark the complete scanned range
		err = workflow.ExecuteActivity(ctx, w.executor.UpdateIndexingBlockRangeForAddress,
			address, chainID, scannedMinBlock, scannedMaxBlock).Get(ctx, nil)
		if err != nil {
			logger.ErrorWf(ctx,
				fmt.Errorf("failed to update final block range"),
				zap.Error(err),
				zap.String("address", address),
			)
			return err
		}

		logger.InfoWf(ctx, "Completed first run sweep",
			zap.Uint64("scannedMinBlock", scannedMinBlock),
			zap.Uint64("scannedMaxBlock", scannedMaxBlock),
		)
	} else {
		// Subsequent run: Sweep backward first (historical), then forward (latest updates)
		logger.InfoWf(ctx, "Subsequent run detected, sweeping backward then forward",
			zap.Uint64("storedMinBlock", storedMinBlock),
			zap.Uint64("storedMaxBlock", storedMaxBlock),
			zap.Uint64("latestBlock", latestBlock),
		)

		// Part A: Sweep backward (historical data) - FIRST
		if storedMinBlock > w.config.EthereumTokenSweepStartBlock {
			logger.InfoWf(ctx, "Sweeping backward for historical data",
				zap.Uint64("startBlock", w.config.EthereumTokenSweepStartBlock),
				zap.Uint64("storedMinBlock", storedMinBlock),
			)

			var backwardTokens []domain.TokenWithBlock
			err = workflow.ExecuteActivity(ctx, w.executor.GetEthereumTokenCIDsByOwnerWithinBlockRange,
				address, w.config.EthereumTokenSweepStartBlock, storedMinBlock-1).Get(ctx, &backwardTokens)
			if err != nil {
				logger.ErrorWf(ctx,
					fmt.Errorf("failed to fetch backward tokens"),
					zap.Error(err),
					zap.String("address", address),
				)
				return err
			}

			logger.InfoWf(ctx, "Retrieved backward sweep tokens",
				zap.String("address", address),
				zap.Int("tokenCount", len(backwardTokens)),
			)

			// Sort by block (descending - newest first) and process in chunks
			sortTokensByBlock(backwardTokens, true)
			chunks := chunkTokensByCount(backwardTokens, TOKEN_INDEXING_CHUNK_SIZE)

			// Store the actual scanned block range (not token block range)
			scannedMinBlock := w.config.EthereumTokenSweepStartBlock
			scannedMaxBlock := storedMinBlock - 1

			for i, chunk := range chunks {
				info := extractChunkInfo(chunk)

				logger.InfoWf(ctx, "Processing backward chunk",
					zap.Int("chunkIndex", i+1),
					zap.Int("totalChunks", len(chunks)),
					zap.Int("tokenCount", len(info.tokenCIDs)),
					zap.Uint64("tokenMinBlock", info.minBlock),
					zap.Uint64("tokenMaxBlock", info.maxBlock),
				)

				if err := w.indexTokenChunk(ctx, info.tokenCIDs); err != nil {
					return err
				}

				// Update min_block after each successful chunk for resumability
				// Progressively update min_block toward start
				err = workflow.ExecuteActivity(ctx, w.executor.UpdateIndexingBlockRangeForAddress,
					address, chainID, info.minBlock, storedMaxBlock).Get(ctx, nil)
				if err != nil {
					logger.ErrorWf(ctx,
						fmt.Errorf("failed to update min block"),
						zap.Error(err),
						zap.String("address", address),
					)
					return err
				}

				logger.InfoWf(ctx, "Updated min block after chunk", zap.Uint64("currentMinBlock", info.minBlock))
			}

			// Final update to ensure we mark the complete scanned range
			err = workflow.ExecuteActivity(ctx, w.executor.UpdateIndexingBlockRangeForAddress,
				address, chainID, scannedMinBlock, storedMaxBlock).Get(ctx, nil)
			if err != nil {
				logger.ErrorWf(ctx,
					fmt.Errorf("failed to update final min block"),
					zap.Error(err),
					zap.String("address", address),
				)
				return err
			}

			storedMinBlock = scannedMinBlock
			logger.InfoWf(ctx, "Completed backward sweep",
				zap.Uint64("scannedMinBlock", scannedMinBlock),
				zap.Uint64("scannedMaxBlock", scannedMaxBlock),
			)
		}

		// Part B: Sweep forward (latest updates) - SECOND
		if latestBlock > storedMaxBlock {
			logger.InfoWf(ctx, "Sweeping forward for latest updates",
				zap.Uint64("storedMaxBlock", storedMaxBlock),
				zap.Uint64("latestBlock", latestBlock),
			)

			var forwardTokens []domain.TokenWithBlock
			err = workflow.ExecuteActivity(ctx, w.executor.GetEthereumTokenCIDsByOwnerWithinBlockRange,
				address, storedMaxBlock+1, latestBlock).Get(ctx, &forwardTokens)
			if err != nil {
				logger.ErrorWf(ctx,
					fmt.Errorf("failed to fetch forward tokens"),
					zap.Error(err),
					zap.String("address", address),
				)
				return err
			}

			logger.InfoWf(ctx, "Retrieved forward sweep tokens",
				zap.String("address", address),
				zap.Int("tokenCount", len(forwardTokens)),
			)

			// Sort by block (ascending - oldest first) and process in chunks
			sortTokensByBlock(forwardTokens, false)
			chunks := chunkTokensByCount(forwardTokens, TOKEN_INDEXING_CHUNK_SIZE)

			// Store the actual scanned block range (not token block range)
			scannedMinBlock := storedMaxBlock + 1
			scannedMaxBlock := latestBlock

			for i, chunk := range chunks {
				info := extractChunkInfo(chunk)

				logger.InfoWf(ctx, "Processing forward chunk",
					zap.Int("chunkIndex", i+1),
					zap.Int("totalChunks", len(chunks)),
					zap.Int("tokenCount", len(info.tokenCIDs)),
					zap.Uint64("tokenMinBlock", info.minBlock),
					zap.Uint64("tokenMaxBlock", info.maxBlock),
				)

				if err := w.indexTokenChunk(ctx, info.tokenCIDs); err != nil {
					return err
				}

				// Update max_block after each successful chunk for resumability
				// Progressively update max_block toward latest
				err = workflow.ExecuteActivity(ctx, w.executor.UpdateIndexingBlockRangeForAddress,
					address, chainID, storedMinBlock, info.maxBlock).Get(ctx, nil)
				if err != nil {
					logger.ErrorWf(ctx,
						fmt.Errorf("failed to update max block"),
						zap.Error(err),
						zap.String("address", address),
					)
					return err
				}

				logger.InfoWf(ctx, "Updated max block after chunk", zap.Uint64("currentMaxBlock", info.maxBlock))
			}

			// Final update to ensure we mark the complete scanned range
			err = workflow.ExecuteActivity(ctx, w.executor.UpdateIndexingBlockRangeForAddress,
				address, chainID, storedMinBlock, scannedMaxBlock).Get(ctx, nil)
			if err != nil {
				logger.ErrorWf(ctx,
					fmt.Errorf("failed to update final max block"),
					zap.Error(err),
					zap.String("address", address),
				)
				return err
			}

			logger.InfoWf(ctx, "Completed forward sweep",
				zap.Uint64("scannedMinBlock", scannedMinBlock),
				zap.Uint64("scannedMaxBlock", scannedMaxBlock),
			)
		}
	}

	logger.InfoWf(ctx, "Ethereum token owner indexing completed", zap.String("address", address))
	return nil
}

// indexTokenChunk indexes a chunk of tokens using the IndexTokens workflow
func (w *workerCore) indexTokenChunk(ctx workflow.Context, tokenCIDs []domain.TokenCID) error {
	if len(tokenCIDs) == 0 {
		return nil
	}

	indexTokensWorkflowOptions := workflow.ChildWorkflowOptions{
		WorkflowExecutionTimeout: 15 * time.Minute,
		ParentClosePolicy:        enums.PARENT_CLOSE_POLICY_ABANDON,
	}
	indexTokensCtx := workflow.WithChildOptions(ctx, indexTokensWorkflowOptions)

	err := workflow.ExecuteChildWorkflow(indexTokensCtx, w.IndexTokens, tokenCIDs).Get(ctx, nil)
	if err != nil {
		logger.ErrorWf(ctx,
			fmt.Errorf("failed to index tokens"),
			zap.Error(err),
			zap.Int("tokenCount", len(tokenCIDs)),
		)
		return err
	}

	return nil
}

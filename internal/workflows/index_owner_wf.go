package workflows

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/store"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
	"github.com/feral-file/ff-indexer-v2/internal/types"
)

const (
	// TOKEN_INDEXING_CHUNK_SIZE is the number of tokens to process in a single batch
	// This balances between resumability (smaller chunks) and efficiency (fewer workflow calls)
	TOKEN_INDEXING_CHUNK_SIZE = 50
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
			WorkflowExecutionTimeout: 15*24*time.Hour + 15*time.Minute, // 15 days + 15 minutes to cover the child workflow timeout
			WorkflowIDReusePolicy:    enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
			ParentClosePolicy:        enums.PARENT_CLOSE_POLICY_TERMINATE,
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
			return err
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
// It manages the job lifecycle (creation, completion, failure, cancellation)
func (w *workerCore) IndexTokenOwner(ctx workflow.Context, address string) error {
	logger.InfoWf(ctx, "Starting token owner indexing",
		zap.String("address", address),
	)

	// Get workflow ID for job tracking
	workflowID := w.temporalWorkflow.GetExecutionID(ctx)

	// Determine blockchain from address format
	blockchain := types.AddressToBlockchain(address)

	// Get chainID based on blockchain type
	var chainID domain.Chain
	switch blockchain {
	case domain.BlockchainTezos:
		chainID = w.config.TezosChainID
	case domain.BlockchainEthereum:
		chainID = w.config.EthereumChainID
	default:
		return fmt.Errorf("unsupported blockchain for address: %s", address)
	}

	// Configure activity options
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 10 * time.Second,
			MaximumAttempts: 2,
		},
	}
	activityCtx := workflow.WithActivityOptions(ctx, activityOptions)

	// Defer to handle cancellation gracefully
	defer func() {
		if w.temporalWorkflow.GetCurrentHistoryLength(ctx) > 0 {
			// Check if workflow is being canceled
			if ctx.Err() != nil && (errors.Is(ctx.Err(), workflow.ErrCanceled) || strings.Contains(ctx.Err().Error(), "canceled")) {
				logger.InfoWf(ctx, "Workflow canceled, updating job status",
					zap.String("address", address),
					zap.String("workflowID", workflowID))

				// Use detached context for cleanup activity
				detachedCtx, _ := workflow.NewDisconnectedContext(ctx)
				detachedCtx = workflow.WithActivityOptions(detachedCtx, activityOptions)

				if err := workflow.ExecuteActivity(detachedCtx, w.executor.UpdateIndexingJobStatus,
					workflowID, schema.IndexingJobStatusCanceled, workflow.Now(ctx)).Get(detachedCtx, nil); err != nil {
					logger.ErrorWf(ctx, fmt.Errorf("failed to update job status to canceled"), zap.Error(err))
				}
			}
		}
	}()

	// Create or update job status to 'running' at start
	err := workflow.ExecuteActivity(activityCtx, w.executor.CreateIndexingJob,
		address, chainID, workflowID, nil).Get(ctx, nil)
	if err != nil {
		logger.ErrorWf(ctx, fmt.Errorf("failed to create/update job"), zap.Error(err))
		// Don't fail workflow if job tracking fails
	}

	// Update job status to 'running'
	if err := workflow.ExecuteActivity(activityCtx, w.executor.UpdateIndexingJobStatus,
		workflowID, schema.IndexingJobStatusRunning, workflow.Now(ctx)).Get(ctx, nil); err != nil {
		logger.ErrorWf(ctx, fmt.Errorf("failed to update job status to running"), zap.Error(err))
	}

	// Configure child workflow options
	childWorkflowOptions := workflow.ChildWorkflowOptions{
		WorkflowID:               fmt.Sprintf("index-token-owner-%s-%s", string(blockchain), address),
		WorkflowExecutionTimeout: 24*time.Hour + 5*time.Minute, // 24 hours + 5 minutes to cover the quota reset time
		WorkflowIDReusePolicy:    enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		ParentClosePolicy:        enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
	}
	childCtx := workflow.WithChildOptions(ctx, childWorkflowOptions)

	// Execute child workflow based on blockchain
	switch blockchain {
	case domain.BlockchainTezos:
		err = workflow.ExecuteChildWorkflow(childCtx, w.IndexTezosTokenOwner, address, &workflowID).Get(ctx, nil)
	case domain.BlockchainEthereum:
		err = workflow.ExecuteChildWorkflow(childCtx, w.IndexEthereumTokenOwner, address, &workflowID).Get(ctx, nil)
	default:
		err = fmt.Errorf("unsupported blockchain for address: %s", address)
	}

	if err != nil {
		// Check if error is due to cancellation
		if errors.Is(err, workflow.ErrCanceled) || strings.Contains(err.Error(), "canceled") {
			// Update job status to 'canceled' when child workflow is canceled
			if err := workflow.ExecuteActivity(activityCtx, w.executor.UpdateIndexingJobStatus,
				workflowID, schema.IndexingJobStatusCanceled, workflow.Now(ctx)).Get(ctx, nil); err != nil {
				logger.ErrorWf(ctx, fmt.Errorf("failed to update job status to canceled"), zap.Error(err))
			}
			return errors.New("workflow canceled")
		}

		logger.ErrorWf(ctx,
			fmt.Errorf("failed to index tokens for owner"),
			zap.Error(err),
			zap.String("address", address),
			zap.String("blockchain", string(blockchain)),
		)

		// Update job status to 'failed'
		if err := workflow.ExecuteActivity(activityCtx, w.executor.UpdateIndexingJobStatus,
			workflowID, schema.IndexingJobStatusFailed, workflow.Now(ctx)).Get(ctx, nil); err != nil {
			logger.ErrorWf(ctx, fmt.Errorf("failed to update job status to failed"), zap.Error(err))
		}
		return err
	}

	logger.InfoWf(ctx, "Token owner indexing completed",
		zap.String("address", address),
		zap.String("blockchain", string(blockchain)),
	)

	// Update job status to 'completed'
	if err := workflow.ExecuteActivity(activityCtx, w.executor.UpdateIndexingJobStatus,
		workflowID, schema.IndexingJobStatusCompleted, workflow.Now(ctx)).Get(ctx, nil); err != nil {
		logger.ErrorWf(ctx, fmt.Errorf("failed to update job status to completed"), zap.Error(err))
		// Don't fail workflow
	}

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

// findLastCompleteBlockIndex finds the index of the last token that completes a block
// within the allowed quota count. This ensures we never split tokens from the same block.
// Returns the index+1 (suitable for slicing). If the first block exceeds quota, returns
// all tokens from that block to ensure forward progress.
//
// This function works correctly regardless of token sort order (ascending or descending)
// because it uses block number equality checks rather than comparisons.
//
// Examples:
// - tokens=[{block:100}, {block:100}, {block:101}], allowedCount=2 -> returns 2 (include both block 100 tokens)
// - tokens=[{block:100}, {block:100}, {block:100}], allowedCount=2 -> returns 3 (include all of first block despite exceeding quota)
// - tokens=[{block:100}, {block:101}, {block:101}], allowedCount=2 -> returns 1 (only complete block 100)
func findLastCompleteBlockIndex(tokens []domain.TokenWithBlock, allowedCount int) int {
	if allowedCount <= 0 || len(tokens) == 0 {
		return 0
	}

	if allowedCount >= len(tokens) {
		return len(tokens) // All tokens fit within quota
	}

	// Find the block number at the quota boundary
	boundaryBlock := tokens[allowedCount-1].BlockNumber

	// Scan forward to find where this block ends
	// (in case there are more tokens from the same block after the quota boundary)
	lastIndex := allowedCount - 1
	for i := allowedCount; i < len(tokens); i++ {
		if tokens[i].BlockNumber == boundaryBlock {
			lastIndex = i
		} else {
			break // Different block, we've found the end
		}
	}

	// If including all tokens from this block would exceed quota significantly,
	// we should instead stop at the previous complete block
	if lastIndex >= allowedCount {
		// Scan backward to find the end of the previous block
		for i := allowedCount - 2; i >= 0; i-- {
			if tokens[i].BlockNumber != boundaryBlock {
				return i + 1 // End of previous block
			}
		}
		// If we get here, all tokens up to allowedCount are from the same block
		// We have to choose: index them all or none
		// Best practice: index the entire first block even if it exceeds quota
		// to ensure forward progress
		return lastIndex + 1
	}

	return lastIndex + 1
}

// IndexTezosTokenOwner indexes all tokens held by a Tezos address
// Uses bi-directional block range sweeping: backward first (historical), then forward (latest updates)
// jobID is optional and used for job status tracking during quota pauses
func (w *workerCore) IndexTezosTokenOwner(ctx workflow.Context, address string, jobID *string) error {
	logger.InfoWf(ctx, "Starting Tezos token owner indexing",
		zap.String("address", address),
		zap.Uint64("startBlock", w.config.TezosTokenSweepStartBlock),
	)

	// Configure activity options
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 10 * time.Second,
			MaximumAttempts: 2,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)
	chainID := w.config.TezosChainID

	// Step 1: Ensure watched address record exists
	err := workflow.ExecuteActivity(ctx, w.executor.EnsureWatchedAddressExists, address, chainID, w.config.BudgetedIndexingDefaultDailyQuota).Get(ctx, nil)
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
			logger.InfoWf(ctx, "Processing token chunk",
				zap.Int("chunkIndex", i+1),
				zap.Int("totalChunks", len(chunks)),
				zap.Int("tokenCount", len(chunk)),
			)

			// Process chunk with quota checking
			shouldContinue, actualMinBlock, actualMaxBlock, quotaResetAt, err := w.processChunkWithQuota(ctx, address, chainID, chunk,
				fmt.Sprintf("first run chunk %d/%d", i+1, len(chunks)), jobID)
			if err != nil {
				return err
			}

			// Update block range after each successful chunk for resumability
			// Use ACTUAL block range of indexed tokens, not the scanned range
			// This ensures we don't skip tokens if quota exhausts mid-chunk
			if actualMinBlock != nil {
				if i == 0 {
					// First chunk - establish the max_block (scanned range end)
					err = workflow.ExecuteActivity(ctx, w.executor.UpdateIndexingBlockRangeForAddress,
						address, chainID, *actualMinBlock, scannedMaxBlock).Get(ctx, nil)
					storedMaxBlock = scannedMaxBlock
				} else {
					// Subsequent chunks - progressively update min_block toward start
					err = workflow.ExecuteActivity(ctx, w.executor.UpdateIndexingBlockRangeForAddress,
						address, chainID, *actualMinBlock, storedMaxBlock).Get(ctx, nil)
				}
				if err != nil {
					logger.Error(fmt.Errorf("failed to update block range: %w", err), zap.String("address", address))
					return err
				}

				logger.InfoWf(ctx, "Updated block range after chunk",
					zap.Uint64("actualMinBlock", *actualMinBlock),
					zap.Uint64("actualMaxBlock", *actualMaxBlock),
					zap.Uint64("storedMinBlock", *actualMinBlock),
					zap.Uint64("storedMaxBlock", *actualMaxBlock),
				)
			}

			// Check if we should continue after updating block range
			if !shouldContinue {
				// Quota exhausted after this chunk - block range already updated above with actual indexed range
				// On continue-as-new, we'll start from actualMinBlock and fetch remaining tokens
				logger.InfoWf(ctx, "Quota exhausted after processing chunk, will continue-as-new",
					zap.Int("chunksCompleted", i+1),
					zap.Int("totalChunks", len(chunks)),
				)
				if err := w.handleQuotaExhausted(ctx, address, quotaResetAt, jobID); err != nil {
					return err
				}
				// Continue-as-new to reset event history and resume indexing
				return workflow.NewContinueAsNewError(ctx, w.IndexTezosTokenOwner, address, jobID)
			}
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
				logger.InfoWf(ctx, "Processing backward chunk",
					zap.Int("chunkIndex", i+1),
					zap.Int("totalChunks", len(chunks)),
					zap.Int("tokenCount", len(chunk)),
				)

				// Process chunk with quota checking
				shouldContinue, actualMinBlock, _, quotaResetAt, err := w.processChunkWithQuota(ctx, address, chainID, chunk,
					fmt.Sprintf("backward chunk %d/%d", i+1, len(chunks)), jobID)
				if err != nil {
					return err
				}

				// Update min_block after each successful chunk for resumability
				// Use actual min block of indexed tokens
				if actualMinBlock != nil {
					err = workflow.ExecuteActivity(ctx, w.executor.UpdateIndexingBlockRangeForAddress,
						address, chainID, *actualMinBlock, storedMaxBlock).Get(ctx, nil)
					if err != nil {
						logger.ErrorWf(ctx,
							fmt.Errorf("failed to update min block"),
							zap.Error(err),
							zap.String("address", address),
						)
						return err
					}
					logger.InfoWf(ctx, "Updated min block after chunk",
						zap.Uint64("actualMinBlock", *actualMinBlock))
				}

				// Check if we should continue after updating block range
				if !shouldContinue {
					// Quota exhausted after this chunk - block range already updated above
					logger.InfoWf(ctx, "Quota exhausted during backward sweep, will continue-as-new",
						zap.Int("chunksCompleted", i+1),
						zap.Int("totalChunks", len(chunks)),
					)
					if err := w.handleQuotaExhausted(ctx, address, quotaResetAt, jobID); err != nil {
						return err
					}
					// Continue-as-new to reset event history and resume indexing
					return workflow.NewContinueAsNewError(ctx, w.IndexTezosTokenOwner, address, jobID)
				}
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
				logger.InfoWf(ctx, "Processing forward chunk",
					zap.Int("chunkIndex", i+1),
					zap.Int("totalChunks", len(chunks)),
					zap.Int("tokenCount", len(chunk)),
				)

				// Process chunk with quota checking
				shouldContinue, _, actualMaxBlock, quotaResetAt, err := w.processChunkWithQuota(ctx, address, chainID, chunk,
					fmt.Sprintf("forward chunk %d/%d", i+1, len(chunks)), jobID)
				if err != nil {
					return err
				}

				// Update max_block after each successful chunk for resumability
				// Use actual max block of indexed tokens
				if actualMaxBlock != nil {
					err = workflow.ExecuteActivity(ctx, w.executor.UpdateIndexingBlockRangeForAddress,
						address, chainID, storedMinBlock, *actualMaxBlock).Get(ctx, nil)
					if err != nil {
						logger.ErrorWf(ctx,
							fmt.Errorf("failed to update max block"),
							zap.Error(err),
							zap.String("address", address),
						)
						return err
					}

					logger.InfoWf(ctx, "Updated max block after chunk",
						zap.Uint64("actualMaxBlock", *actualMaxBlock))
				}

				// Check if we should continue after updating block range
				if !shouldContinue {
					// Quota exhausted after this chunk - block range already updated above
					logger.InfoWf(ctx, "Quota exhausted during forward sweep, will continue-as-new",
						zap.Int("chunksCompleted", i+1),
						zap.Int("totalChunks", len(chunks)),
					)
					if err := w.handleQuotaExhausted(ctx, address, quotaResetAt, jobID); err != nil {
						return err
					}
					// Continue-as-new to reset event history and resume indexing
					return workflow.NewContinueAsNewError(ctx, w.IndexTezosTokenOwner, address, jobID)
				}
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
func (w *workerCore) IndexEthereumTokenOwner(ctx workflow.Context, address string, jobID *string) error {
	logger.InfoWf(ctx, "Starting Ethereum token owner indexing",
		zap.String("address", address),
		zap.Uint64("startBlock", w.config.EthereumTokenSweepStartBlock),
	)

	// Configure activity options
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 10 * time.Second,
			MaximumAttempts: 2,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)
	chainID := w.config.EthereumChainID

	// Step 1: Ensure watched address record exists
	err := workflow.ExecuteActivity(ctx, w.executor.EnsureWatchedAddressExists, address, chainID, w.config.BudgetedIndexingDefaultDailyQuota).Get(ctx, nil)
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
			logger.InfoWf(ctx, "Processing token chunk",
				zap.Int("chunkIndex", i+1),
				zap.Int("totalChunks", len(chunks)),
				zap.Int("tokenCount", len(chunk)),
			)

			// Process chunk with quota checking
			shouldContinue, actualMinBlock, actualMaxBlock, quotaResetAt, err := w.processChunkWithQuota(ctx, address, chainID, chunk,
				fmt.Sprintf("first run chunk %d/%d", i+1, len(chunks)), jobID)
			if err != nil {
				return err
			}

			// Update block range after each successful chunk for resumability
			// Use ACTUAL block range of indexed tokens, not the scanned range
			if actualMinBlock != nil {
				if i == 0 {
					// First chunk - establish the max_block (scanned range end)
					err = workflow.ExecuteActivity(ctx, w.executor.UpdateIndexingBlockRangeForAddress,
						address, chainID, *actualMinBlock, scannedMaxBlock).Get(ctx, nil)
					storedMaxBlock = scannedMaxBlock
				} else {
					// Subsequent chunks - progressively update min_block toward start
					err = workflow.ExecuteActivity(ctx, w.executor.UpdateIndexingBlockRangeForAddress,
						address, chainID, *actualMinBlock, storedMaxBlock).Get(ctx, nil)
				}
				if err != nil {
					logger.ErrorWf(ctx,
						fmt.Errorf("failed to update block range"),
						zap.Error(err),
						zap.String("address", address),
					)
					return err
				}

				logger.InfoWf(ctx, "Updated block range after chunk",
					zap.Uint64("actualMinBlock", *actualMinBlock),
					zap.Uint64("actualMaxBlock", *actualMaxBlock),
				)
			}

			// Check if we should continue after updating block range
			if !shouldContinue {
				// Quota exhausted after this chunk - block range already updated above with actual indexed range
				logger.InfoWf(ctx, "Quota exhausted after processing chunk, will continue-as-new",
					zap.Int("chunksCompleted", i+1),
					zap.Int("totalChunks", len(chunks)),
				)
				if err := w.handleQuotaExhausted(ctx, address, quotaResetAt, jobID); err != nil {
					return err
				}
				// Continue-as-new to reset event history and resume indexing
				return workflow.NewContinueAsNewError(ctx, w.IndexEthereumTokenOwner, address, jobID)
			}
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
				logger.InfoWf(ctx, "Processing backward chunk",
					zap.Int("chunkIndex", i+1),
					zap.Int("totalChunks", len(chunks)),
					zap.Int("tokenCount", len(chunk)),
				)

				// Process chunk with quota checking
				shouldContinue, actualMinBlock, _, quotaResetAt, err := w.processChunkWithQuota(ctx, address, chainID, chunk,
					fmt.Sprintf("backward chunk %d/%d", i+1, len(chunks)), jobID)
				if err != nil {
					return err
				}

				// Update min_block after each successful chunk for resumability
				// Use actual min block of indexed tokens
				if actualMinBlock != nil {
					err = workflow.ExecuteActivity(ctx, w.executor.UpdateIndexingBlockRangeForAddress,
						address, chainID, *actualMinBlock, storedMaxBlock).Get(ctx, nil)
					if err != nil {
						logger.ErrorWf(ctx,
							fmt.Errorf("failed to update min block"),
							zap.Error(err),
							zap.String("address", address),
						)
						return err
					}

					logger.InfoWf(ctx, "Updated min block after chunk",
						zap.Uint64("actualMinBlock", *actualMinBlock))
				}

				// Check if we should continue after updating block range
				if !shouldContinue {
					// Quota exhausted after this chunk - block range already updated above
					logger.InfoWf(ctx, "Quota exhausted during backward sweep, will continue-as-new",
						zap.Int("chunksCompleted", i+1),
						zap.Int("totalChunks", len(chunks)),
					)
					if err := w.handleQuotaExhausted(ctx, address, quotaResetAt, jobID); err != nil {
						return err
					}
					// Continue-as-new to reset event history and resume indexing
					return workflow.NewContinueAsNewError(ctx, w.IndexEthereumTokenOwner, address, jobID)
				}
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
				logger.InfoWf(ctx, "Processing forward chunk",
					zap.Int("chunkIndex", i+1),
					zap.Int("totalChunks", len(chunks)),
					zap.Int("tokenCount", len(chunk)),
				)

				// Process chunk with quota checking
				shouldContinue, _, actualMaxBlock, quotaResetAt, err := w.processChunkWithQuota(ctx, address, chainID, chunk,
					fmt.Sprintf("forward chunk %d/%d", i+1, len(chunks)), jobID)
				if err != nil {
					return err
				}

				// Update max_block after each successful chunk for resumability
				// Use actual max block of indexed tokens
				if actualMaxBlock != nil {
					err = workflow.ExecuteActivity(ctx, w.executor.UpdateIndexingBlockRangeForAddress,
						address, chainID, storedMinBlock, *actualMaxBlock).Get(ctx, nil)
					if err != nil {
						logger.ErrorWf(ctx,
							fmt.Errorf("failed to update max block"),
							zap.Error(err),
							zap.String("address", address),
						)
						return err
					}

					logger.InfoWf(ctx, "Updated max block after chunk",
						zap.Uint64("actualMaxBlock", *actualMaxBlock))
				}

				// Check if we should continue after updating block range
				if !shouldContinue {
					// Quota exhausted after this chunk - block range already updated above
					logger.InfoWf(ctx, "Quota exhausted during forward sweep, will continue-as-new",
						zap.Int("chunksCompleted", i+1),
						zap.Int("totalChunks", len(chunks)),
					)
					if err := w.handleQuotaExhausted(ctx, address, quotaResetAt, jobID); err != nil {
						return err
					}
					// Continue-as-new to reset event history and resume indexing
					return workflow.NewContinueAsNewError(ctx, w.IndexEthereumTokenOwner, address, jobID)
				}
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
// For owner-specific indexing, pass the address to enable efficient ERC1155 indexing
func (w *workerCore) indexTokenChunk(ctx workflow.Context, tokenCIDs []domain.TokenCID, address *string) error {
	if len(tokenCIDs) == 0 {
		return nil
	}

	indexTokensWorkflowOptions := workflow.ChildWorkflowOptions{
		WorkflowExecutionTimeout: 15 * time.Minute,
	}
	indexTokensCtx := workflow.WithChildOptions(ctx, indexTokensWorkflowOptions)

	err := workflow.ExecuteChildWorkflow(indexTokensCtx, w.IndexTokens, tokenCIDs, address).Get(ctx, nil)
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

// processChunkWithQuota handles quota checking, chunk processing, and usage increment
// Returns (shouldContinue bool, actualMinBlock uint64, actualMaxBlock uint64, quotaResetAt time.Time, error)
// - shouldContinue=false means quota exhausted, need to sleep+continue-as-new
// - actualMinBlock/actualMaxBlock indicate the actual block range of tokens that were indexed, nil means nothing was indexed
// - quotaResetAt is the time when quota will reset (only valid when shouldContinue=false)
func (w *workerCore) processChunkWithQuota(
	ctx workflow.Context,
	address string,
	chainID domain.Chain,
	tokens []domain.TokenWithBlock,
	chunkInfo string, // e.g., "forward chunk 1/5" for logging
	jobID *string, // optional job ID for progress tracking
) (bool, *uint64, *uint64, time.Time, error) {
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 10 * time.Second,
			MaximumAttempts: 2,
		},
	}
	activityCtx := workflow.WithActivityOptions(ctx, activityOptions)

	requestedCount := len(tokens)
	allowedCount := requestedCount
	var quotaResetAt time.Time
	if w.config.BudgetedIndexingModeEnabled {
		// Check quota
		var quotaStatus *store.QuotaInfo
		err := workflow.ExecuteActivity(activityCtx, w.executor.GetQuotaInfo, address, chainID).Get(ctx, &quotaStatus)
		if err != nil {
			return false, nil, nil, time.Time{}, fmt.Errorf("failed to check quota: %w", err)
		}

		quotaResetAt = quotaStatus.QuotaResetAt

		if quotaStatus.QuotaExhausted {
			logger.InfoWf(ctx, "Quota exhausted, will sleep until reset and continue-as-new",
				zap.String("address", address),
				zap.Time("quotaResetAt", quotaStatus.QuotaResetAt),
				zap.String("chunkInfo", chunkInfo),
			)
			return false, nil, nil, quotaResetAt, nil // Signal to caller: quota exhausted, need to sleep+continue-as-new
		}

		// Return the minimum of requested and remaining
		if quotaStatus.RemainingQuota < requestedCount {
			allowedCount = quotaStatus.RemainingQuota
		}
	}

	// Limit chunk to allowed count ensuring complete blocks
	actualTokens := tokens
	wasLimited := false
	if allowedCount < requestedCount {
		// Find the last complete block within quota
		completeBlockIndex := findLastCompleteBlockIndex(tokens, allowedCount)

		if completeBlockIndex <= 0 {
			// Edge case: even the first block exceeds quota
			// We still process it to ensure forward progress, but log a warning
			logger.WarnWf(ctx, "First block exceeds quota, will process it anyway to ensure forward progress",
				zap.Int("allowedCount", allowedCount),
				zap.String("chunkInfo", chunkInfo),
			)
			// Find end of first block
			firstBlockNum := tokens[0].BlockNumber
			completeBlockIndex = 1
			for completeBlockIndex < len(tokens) && tokens[completeBlockIndex].BlockNumber == firstBlockNum {
				completeBlockIndex++
			}
		}

		actualTokens = tokens[:completeBlockIndex]
		wasLimited = completeBlockIndex < len(tokens)

		if wasLimited {
			logger.InfoWf(ctx, "Limiting chunk to complete blocks within quota",
				zap.Int("requested", len(tokens)),
				zap.Int("allowed", allowedCount),
				zap.Int("actual", len(actualTokens)),
				zap.String("chunkInfo", chunkInfo),
			)
		}
	}

	// Extract info from actual tokens we'll index (with correct block range)
	actualInfo := extractChunkInfo(actualTokens)

	// Index tokens
	if err := w.indexTokenChunk(ctx, actualInfo.tokenCIDs, &address); err != nil {
		return false, nil, nil, time.Time{}, err
	}

	if w.config.BudgetedIndexingModeEnabled {
		// Increment usage counter after successful indexing
		count := len(actualInfo.tokenCIDs)
		err := workflow.ExecuteActivity(activityCtx, w.executor.IncrementTokensIndexed, address, chainID, count).Get(ctx, nil)
		if err != nil {
			logger.ErrorWf(ctx, fmt.Errorf("failed to increment token usage"),
				zap.Error(err),
				zap.String("address", address),
				zap.Int("count", count),
			)
			return false, nil, nil, time.Time{}, err
		}

		logger.InfoWf(ctx, "Incremented token usage for budgeted indexing mode",
			zap.String("address", address),
			zap.Int("count", count),
		)
	}

	// Update job progress after successful chunk processing
	// Note: The store layer accumulates tokens_processed, so we pass the incremental count (chunk size)
	if jobID != nil {
		chunkSize := len(actualInfo.tokenCIDs)
		logger.InfoWf(ctx, "Updating job progress (incremental)",
			zap.String("jobID", *jobID),
			zap.Int("chunkTokens", chunkSize),
			zap.Uint64("minBlock", actualInfo.minBlock),
			zap.Uint64("maxBlock", actualInfo.maxBlock),
		)

		err := workflow.ExecuteActivity(activityCtx, w.executor.UpdateIndexingJobProgress,
			*jobID, chunkSize, actualInfo.minBlock, actualInfo.maxBlock).Get(ctx, nil)
		if err != nil {
			logger.ErrorWf(ctx, fmt.Errorf("failed to update job progress"),
				zap.Error(err),
				zap.String("jobID", *jobID),
				zap.Int("chunkTokens", chunkSize))
			// Don't fail workflow if progress tracking fails
		}
	}

	// If we had to limit due to quota, return the actual range indexed and signal quota exhaustion
	if wasLimited {
		logger.InfoWf(ctx, "Quota exhausted after partial chunk",
			zap.Int("indexed", len(actualInfo.tokenCIDs)),
			zap.Int("total", len(tokens)),
			zap.Uint64("actualMinBlock", actualInfo.minBlock),
			zap.Uint64("actualMaxBlock", actualInfo.maxBlock),
		)
		return false, &actualInfo.minBlock, &actualInfo.maxBlock, quotaResetAt, nil // Quota exhausted, return actual range
	}

	return true, &actualInfo.minBlock, &actualInfo.maxBlock, time.Time{}, nil // shouldContinue=true, success
}

// handleQuotaExhausted sleeps until quota reset and returns error to trigger continue-as-new
// Returns temporal.NewContinueAsNewError to signal the workflow should restart
// jobID is optional and used for updating job status during quota pauses
func (w *workerCore) handleQuotaExhausted(ctx workflow.Context, address string, quotaResetAt time.Time, jobID *string) error {
	if !w.config.BudgetedIndexingModeEnabled {
		return nil // No quota management if budgeted mode is disabled
	}

	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 10 * time.Second,
			MaximumAttempts: 2,
		},
	}
	activityCtx := workflow.WithActivityOptions(ctx, activityOptions)

	if jobID != nil {
		// Update job status to 'paused' before sleeping
		err := workflow.ExecuteActivity(activityCtx, w.executor.UpdateIndexingJobStatus,
			*jobID, schema.IndexingJobStatusPaused, workflow.Now(ctx)).Get(ctx, nil)
		if err != nil {
			logger.ErrorWf(ctx, fmt.Errorf("failed to update job status to paused"), zap.Error(err))
			// Don't fail workflow
		}
	}

	// Calculate sleep duration until quota reset
	now := workflow.Now(ctx)
	sleepDuration := quotaResetAt.Sub(now)

	if sleepDuration > 0 {
		logger.InfoWf(ctx, "Sleeping until quota reset before continue-as-new",
			zap.String("address", address),
			zap.Duration("sleepDuration", sleepDuration),
			zap.Time("quotaResetAt", quotaResetAt),
		)
		if err := workflow.Sleep(ctx, sleepDuration); err != nil {
			return err
		}
		logger.InfoWf(ctx, "Quota reset complete, preparing to continue-as-new",
			zap.String("address", address),
		)
	} else {
		logger.InfoWf(ctx, "Quota already reset, proceeding to continue-as-new immediately",
			zap.String("address", address),
		)
	}

	if jobID != nil {
		// Update the job status to 'running'
		err := workflow.ExecuteActivity(activityCtx, w.executor.UpdateIndexingJobStatus,
			*jobID, schema.IndexingJobStatusRunning, workflow.Now(ctx)).Get(ctx, nil)
		if err != nil {
			logger.ErrorWf(ctx, fmt.Errorf("failed to update job status to running"), zap.Error(err))
			// Don't fail workflow
		}
	}

	return nil
}

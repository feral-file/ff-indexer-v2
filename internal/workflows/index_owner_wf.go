package workflows

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/providers/jobs"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
	"github.com/feral-file/ff-indexer-v2/internal/types"
)

// IndexTokenOwner indexes all tokens held by a single address
// This is the parent workflow that delegates to blockchain-specific child workflows
// It manages the job lifecycle (creation, completion, failure, cancellation)
func (w *coreWorkflows) IndexTokenOwner(ctx context.Context, address string) error {
	logger.InfoCtx(ctx, "Starting token owner indexing",
		zap.String("address", address),
	)

	var queueJobID int64
	var haveQueueJobID bool
	if id, ok := jobs.JobIDFromContext(ctx); ok {
		queueJobID = id
		haveQueueJobID = true
	}

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

	// Defer to handle cancellation gracefully
	defer func() {
		if ctx.Err() != nil && (errors.Is(ctx.Err(), context.Canceled) || strings.Contains(ctx.Err().Error(), "canceled")) {
			if !haveQueueJobID {
				return
			}
			logger.InfoCtx(ctx, "Job canceled, updating indexing status",
				zap.String("address", address),
				zap.Int64("job_id", queueJobID))

			sticky := context.WithoutCancel(ctx)
			if err := w.executor.UpdateIndexingJobStatus(sticky,
				queueJobID, schema.IndexingJobStatusCanceled, time.Now().UTC()); err != nil {
				logger.ErrorCtx(ctx, fmt.Errorf("failed to update job status to canceled"), zap.Error(err))
			}
		}
	}()

	if haveQueueJobID {
		if err := w.executor.CreateIndexingJob(ctx, address, chainID, queueJobID); err != nil {
			logger.ErrorCtx(ctx, fmt.Errorf("failed to create/update job record"), zap.Error(err))
		}
		if err := w.executor.UpdateIndexingJobStatus(ctx,
			queueJobID, schema.IndexingJobStatusRunning, time.Now().UTC()); err != nil {
			logger.ErrorCtx(ctx, fmt.Errorf("failed to update job status to running"), zap.Error(err))
		}
	}

	var childJobID *int64
	if haveQueueJobID {
		x := queueJobID
		childJobID = &x
	}
	var err error
	// Execute child workflow based on blockchain
	switch blockchain {
	case domain.BlockchainTezos:
		err = w.IndexTezosTokenOwner(ctx, address, childJobID)
	case domain.BlockchainEthereum:
		err = w.IndexEthereumTokenOwner(ctx, address, childJobID)
	default:
		err = fmt.Errorf("unsupported blockchain for address: %s", address)
	}

	if err != nil {
		if errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "canceled") {
			if haveQueueJobID {
				sticky := context.WithoutCancel(ctx)
				if uerr := w.executor.UpdateIndexingJobStatus(sticky,
					queueJobID, schema.IndexingJobStatusCanceled, time.Now().UTC()); uerr != nil {
					logger.ErrorCtx(ctx, fmt.Errorf("failed to update job status to canceled"), zap.Error(uerr))
				}
			}
			return err
		}

		var re *jobs.RescheduleError
		if errors.As(err, &re) {
			return err
		}

		logger.ErrorCtx(ctx,
			fmt.Errorf("failed to index tokens for owner"),
			zap.Error(err),
			zap.String("address", address),
			zap.String("blockchain", string(blockchain)),
		)

		if haveQueueJobID {
			if uerr := w.executor.UpdateIndexingJobStatus(ctx,
				queueJobID, schema.IndexingJobStatusFailed, time.Now().UTC()); uerr != nil {
				logger.ErrorCtx(ctx, fmt.Errorf("failed to update job status to failed"), zap.Error(uerr))
			}
		}
		return err
	}

	logger.InfoCtx(ctx, "Token owner indexing completed",
		zap.String("address", address),
		zap.String("blockchain", string(blockchain)),
	)

	if haveQueueJobID {
		if uerr := w.executor.UpdateIndexingJobStatus(ctx,
			queueJobID, schema.IndexingJobStatusCompleted, time.Now().UTC()); uerr != nil {
			logger.ErrorCtx(ctx, fmt.Errorf("failed to update job status to completed"), zap.Error(uerr))
		}
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

func normalizeOwnerBatchTarget(target int, defaultTarget int) int {
	if target <= 0 {
		return defaultTarget
	}
	return target
}

// chunkTokensByTargetBlockAligned splits tokens into chunks based on token-count targets,
// while never splitting tokens from the same block.
//
// For the first chunk, it uses firstBatchTarget. For subsequent chunks, it uses subsequentBatchTarget.
// Each chunk may exceed its target to include the full boundary block (block-aligned "overflow" is allowed).
//
// Callers must ensure tokens are sorted/grouped by block number first.
func chunkTokensByTargetBlockAligned(tokens []domain.TokenWithBlock, firstBatchTarget int, subsequentBatchTarget int) [][]domain.TokenWithBlock {
	firstBatchTarget = normalizeOwnerBatchTarget(firstBatchTarget, 20)
	subsequentBatchTarget = normalizeOwnerBatchTarget(subsequentBatchTarget, 1)

	var chunks [][]domain.TokenWithBlock

	chunkStart := 0
	chunkIndex := 0
	for chunkStart < len(tokens) {
		target := subsequentBatchTarget
		if chunkIndex == 0 {
			target = firstBatchTarget
		}

		relativeEnd := findLastCompleteBlockIndexByTarget(tokens[chunkStart:], target)
		if relativeEnd <= 0 {
			// Safety net: this should be impossible because target >= 1 and tokens[chunkStart:] is non-empty.
			// Keep a guard to guarantee forward progress even if future changes violate this invariant.
			relativeEnd = len(tokens) - chunkStart
		}

		chunkEnd := chunkStart + relativeEnd
		chunks = append(chunks, tokens[chunkStart:chunkEnd])
		chunkStart = chunkEnd
		chunkIndex++
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

// findLastCompleteBlockIndexByQuota finds the index of the last token that completes a block
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
func findLastCompleteBlockIndexByQuota(tokens []domain.TokenWithBlock, allowedCount int) int {
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

// findLastCompleteBlockIndexByTarget finds the index of the last token that completes a block
// after taking at least targetCount tokens. This ensures we never split tokens from the same block.
//
// Unlike findLastCompleteBlockIndexByQuota, this function allows exceeding targetCount to include
// all tokens from the boundary block (block-aligned "overflow" is allowed).
//
// Returns the index+1 (suitable for slicing).
//
// This function works correctly regardless of token sort order (ascending or descending)
// as long as tokens are grouped by block number (i.e., all tokens from the same block are contiguous).
func findLastCompleteBlockIndexByTarget(tokens []domain.TokenWithBlock, targetCount int) int {
	if targetCount <= 0 || len(tokens) == 0 {
		return 0
	}

	if targetCount >= len(tokens) {
		return len(tokens)
	}

	boundaryBlock := tokens[targetCount-1].BlockNumber

	end := targetCount
	for end < len(tokens) && tokens[end].BlockNumber == boundaryBlock {
		end++
	}

	return end
}

// IndexTezosTokenOwner indexes all tokens held by a Tezos address
// Uses bi-directional block range sweeping: backward first (historical), then forward (latest updates)
// jobID is optional and used for job status tracking during quota pauses
func (w *coreWorkflows) IndexTezosTokenOwner(ctx context.Context, address string, jobID *int64) error {
	logger.InfoCtx(ctx, "Starting Tezos token owner indexing",
		zap.String("address", address),
		zap.Uint64("startBlock", w.config.TezosTokenSweepStartBlock),
	)

	chainID := w.config.TezosChainID

	// Step 1: Ensure watched address record exists
	err := w.executor.EnsureWatchedAddressExists(ctx, address, chainID, w.config.BudgetedIndexingDefaultDailyQuota)
	if err != nil {
		logger.ErrorCtx(ctx,
			fmt.Errorf("failed to ensure watched address exists"),
			zap.Error(err),
			zap.String("address", address),
			zap.String("chainID", string(chainID)),
		)
		return err
	}

	// Step 2: Get current indexing block range for this address and chain
	var rangeResult *BlockRangeResult
	rangeResult, err = w.executor.GetIndexingBlockRangeForAddress(ctx, address, chainID)
	if err != nil {
		logger.ErrorCtx(ctx,
			fmt.Errorf("failed to get indexing block range"),
			zap.Error(err),
			zap.String("address", address),
			zap.String("chainID", string(chainID)),
		)
		return err
	}

	storedMinBlock := rangeResult.MinBlock
	storedMaxBlock := rangeResult.MaxBlock

	logger.InfoCtx(ctx, "Retrieved stored block range",
		zap.String("address", address),
		zap.Uint64("storedMinBlock", storedMinBlock),
		zap.Uint64("storedMaxBlock", storedMaxBlock),
	)

	// Step 3: Get the current latest block from TzKT
	var latestBlock uint64
	latestBlock, err = w.executor.GetLatestTezosBlock(ctx)
	if err != nil {
		logger.ErrorCtx(ctx,
			fmt.Errorf("failed to get latest Tezos block"),
			zap.Error(err),
			zap.String("address", address),
		)
		return err
	}

	logger.InfoCtx(ctx, "Retrieved latest block from TzKT",
		zap.String("address", address),
		zap.Uint64("latestBlock", latestBlock),
	)

	// Step 4: Determine sweeping strategy
	if storedMinBlock == 0 && storedMaxBlock == 0 {
		// First run: No previous indexing exists
		// Fetch entire range from start to latest, process in chunks
		logger.InfoCtx(ctx, "First run detected, fetching all tokens from start to latest",
			zap.Uint64("startBlock", w.config.TezosTokenSweepStartBlock),
			zap.Uint64("latestBlock", latestBlock),
		)

		var allTokens []domain.TokenWithBlock
		allTokens, err = w.executor.GetTezosTokenCIDsByAccountWithinBlockRange(ctx, address, w.config.TezosTokenSweepStartBlock, latestBlock)
		if err != nil {
			logger.ErrorCtx(ctx,
				fmt.Errorf("failed to fetch tokens"),
				zap.Error(err),
				zap.String("address", address),
			)
			return err
		}

		logger.InfoCtx(ctx, "Retrieved all tokens for first run",
			zap.String("address", address),
			zap.Int("tokenCount", len(allTokens)),
		)

		// Sort by block number (descending - newest first) and process in chunks
		sortTokensByBlock(allTokens, true)
		chunks := chunkTokensByTargetBlockAligned(allTokens, w.config.TezosOwnerFirstBatchTarget, w.config.TezosOwnerSubsequentBatchTarget)

		// Store the actual scanned block range (not token block range)
		scannedMinBlock := w.config.TezosTokenSweepStartBlock
		scannedMaxBlock := latestBlock

		for i, chunk := range chunks {
			logger.InfoCtx(ctx, "Processing token chunk",
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
					err = w.executor.UpdateIndexingBlockRangeForAddress(ctx, address, chainID, *actualMinBlock, scannedMaxBlock)
					storedMaxBlock = scannedMaxBlock
				} else {
					// Subsequent chunks - progressively update min_block toward start
					err = w.executor.UpdateIndexingBlockRangeForAddress(ctx, address, chainID, *actualMinBlock, storedMaxBlock)
				}
				if err != nil {
					logger.ErrorCtx(ctx, fmt.Errorf("failed to update block range: %w", err), zap.String("address", address))
					return err
				}

				logger.InfoCtx(ctx, "Updated block range after chunk",
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
				logger.InfoCtx(ctx, "Quota exhausted after processing chunk, will continue-as-new",
					zap.Int("chunksCompleted", i+1),
					zap.Int("totalChunks", len(chunks)),
				)
				return w.returnQuotaReschedule(ctx, jobID, quotaResetAt)
			}

			// Add a brief delay to prevent exceeding third-party service rate limits
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(2 * time.Second):
			}
		}

		// Final update to ensure we mark the complete scanned range
		err = w.executor.UpdateIndexingBlockRangeForAddress(ctx, address, chainID, scannedMinBlock, scannedMaxBlock)
		if err != nil {
			logger.ErrorCtx(ctx,
				fmt.Errorf("failed to update final block range"),
				zap.Error(err),
				zap.String("address", address),
			)
			return err
		}

		logger.InfoCtx(ctx, "Completed first run sweep",
			zap.Uint64("scannedMinBlock", scannedMinBlock),
			zap.Uint64("scannedMaxBlock", scannedMaxBlock),
		)
	} else {
		// Subsequent run: Sweep backward first (historical), then forward (latest updates)
		logger.InfoCtx(ctx, "Subsequent run detected, sweeping backward then forward",
			zap.Uint64("storedMinBlock", storedMinBlock),
			zap.Uint64("storedMaxBlock", storedMaxBlock),
			zap.Uint64("latestBlock", latestBlock),
		)

		// Part A: Sweep backward (historical data) - FIRST
		if storedMinBlock > w.config.TezosTokenSweepStartBlock {
			logger.InfoCtx(ctx, "Sweeping backward for historical data",
				zap.Uint64("startBlock", w.config.TezosTokenSweepStartBlock),
				zap.Uint64("storedMinBlock", storedMinBlock),
			)

			var backwardTokens []domain.TokenWithBlock
			backwardTokens, err = w.executor.GetTezosTokenCIDsByAccountWithinBlockRange(ctx, address, w.config.TezosTokenSweepStartBlock, storedMinBlock-1)
			if err != nil {
				logger.ErrorCtx(ctx,
					fmt.Errorf("failed to fetch backward tokens"),
					zap.Error(err),
					zap.String("address", address),
				)
				return err
			}

			logger.InfoCtx(ctx, "Retrieved backward sweep tokens",
				zap.String("address", address),
				zap.Int("tokenCount", len(backwardTokens)),
			)

			// Sort by block (descending - newest first) and process in chunks
			sortTokensByBlock(backwardTokens, true)
			chunks := chunkTokensByTargetBlockAligned(backwardTokens, w.config.TezosOwnerSubsequentBatchTarget, w.config.TezosOwnerSubsequentBatchTarget)

			// Store the actual scanned block range (not token block range)
			scannedMinBlock := w.config.TezosTokenSweepStartBlock
			scannedMaxBlock := storedMinBlock - 1

			for i, chunk := range chunks {
				logger.InfoCtx(ctx, "Processing backward chunk",
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
					err = w.executor.UpdateIndexingBlockRangeForAddress(ctx, address, chainID, *actualMinBlock, storedMaxBlock)
					if err != nil {
						logger.ErrorCtx(ctx,
							fmt.Errorf("failed to update min block"),
							zap.Error(err),
							zap.String("address", address),
						)
						return err
					}
					logger.InfoCtx(ctx, "Updated min block after chunk",
						zap.Uint64("actualMinBlock", *actualMinBlock))
				}

				// Check if we should continue after updating block range
				if !shouldContinue {
					// Quota exhausted after this chunk - block range already updated above
					logger.InfoCtx(ctx, "Quota exhausted during backward sweep, will continue-as-new",
						zap.Int("chunksCompleted", i+1),
						zap.Int("totalChunks", len(chunks)),
					)
					return w.returnQuotaReschedule(ctx, jobID, quotaResetAt)
				}

				// Add a brief delay to prevent exceeding third-party service rate limits
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(2 * time.Second):
				}
			}

			// Final update to ensure we mark the complete scanned range
			err = w.executor.UpdateIndexingBlockRangeForAddress(ctx, address, chainID, scannedMinBlock, storedMaxBlock)
			if err != nil {
				logger.ErrorCtx(ctx,
					fmt.Errorf("failed to update final min block"),
					zap.Error(err),
					zap.String("address", address),
				)
				return err
			}

			storedMinBlock = scannedMinBlock
			logger.InfoCtx(ctx, "Completed backward sweep",
				zap.Uint64("scannedMinBlock", scannedMinBlock),
				zap.Uint64("scannedMaxBlock", scannedMaxBlock),
			)
		}

		// Part B: Sweep forward (latest updates) - SECOND
		if latestBlock > storedMaxBlock {
			logger.InfoCtx(ctx, "Sweeping forward for latest updates",
				zap.Uint64("storedMaxBlock", storedMaxBlock),
				zap.Uint64("latestBlock", latestBlock),
			)

			var forwardTokens []domain.TokenWithBlock
			forwardTokens, err = w.executor.GetTezosTokenCIDsByAccountWithinBlockRange(ctx, address, storedMaxBlock+1, latestBlock)
			if err != nil {
				logger.ErrorCtx(ctx,
					fmt.Errorf("failed to fetch forward tokens"),
					zap.Error(err),
					zap.String("address", address),
				)
				return err
			}

			logger.InfoCtx(ctx, "Retrieved forward sweep tokens",
				zap.String("address", address),
				zap.Int("tokenCount", len(forwardTokens)),
			)

			// Sort by block (ascending - oldest first) and process in chunks
			sortTokensByBlock(forwardTokens, false)
			chunks := chunkTokensByTargetBlockAligned(forwardTokens, w.config.TezosOwnerSubsequentBatchTarget, w.config.TezosOwnerSubsequentBatchTarget)

			// Store the actual scanned block range (not token block range)
			scannedMinBlock := storedMaxBlock + 1
			scannedMaxBlock := latestBlock

			for i, chunk := range chunks {
				logger.InfoCtx(ctx, "Processing forward chunk",
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
					err = w.executor.UpdateIndexingBlockRangeForAddress(ctx, address, chainID, storedMinBlock, *actualMaxBlock)
					if err != nil {
						logger.ErrorCtx(ctx,
							fmt.Errorf("failed to update max block"),
							zap.Error(err),
							zap.String("address", address),
						)
						return err
					}

					logger.InfoCtx(ctx, "Updated max block after chunk",
						zap.Uint64("actualMaxBlock", *actualMaxBlock))
				}

				// Check if we should continue after updating block range
				if !shouldContinue {
					// Quota exhausted after this chunk - block range already updated above
					logger.InfoCtx(ctx, "Quota exhausted during forward sweep, will continue-as-new",
						zap.Int("chunksCompleted", i+1),
						zap.Int("totalChunks", len(chunks)),
					)
					return w.returnQuotaReschedule(ctx, jobID, quotaResetAt)
				}

				// Add a brief delay to prevent exceeding third-party service rate limits
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(2 * time.Second):
				}
			}

			// Final update to ensure we mark the complete scanned range
			err = w.executor.UpdateIndexingBlockRangeForAddress(ctx, address, chainID, storedMinBlock, scannedMaxBlock)
			if err != nil {
				logger.ErrorCtx(ctx,
					fmt.Errorf("failed to update final max block"),
					zap.Error(err),
					zap.String("address", address),
				)
				return err
			}

			logger.InfoCtx(ctx, "Completed forward sweep",
				zap.Uint64("scannedMinBlock", scannedMinBlock),
				zap.Uint64("scannedMaxBlock", scannedMaxBlock),
			)
		}
	}

	logger.InfoCtx(ctx, "Tezos token owner indexing completed", zap.String("address", address))

	return nil
}

// IndexEthereumTokenOwner indexes all tokens held by an Ethereum address
// Uses bi-directional block range sweeping: backward first (historical), then forward (latest updates)
func (w *coreWorkflows) IndexEthereumTokenOwner(ctx context.Context, address string, jobID *int64) error {
	logger.InfoCtx(ctx, "Starting Ethereum token owner indexing",
		zap.String("address", address),
		zap.Uint64("startBlock", w.config.EthereumTokenSweepStartBlock),
	)

	chainID := w.config.EthereumChainID

	// Step 1: Ensure watched address record exists
	err := w.executor.EnsureWatchedAddressExists(ctx, address, chainID, w.config.BudgetedIndexingDefaultDailyQuota)
	if err != nil {
		logger.ErrorCtx(ctx,
			fmt.Errorf("failed to ensure watched address exists"),
			zap.Error(err),
			zap.String("address", address),
			zap.String("chainID", string(chainID)),
		)
		return err
	}

	// Step 2: Get current indexing block range for this address and chain
	var rangeResult *BlockRangeResult
	rangeResult, err = w.executor.GetIndexingBlockRangeForAddress(ctx, address, chainID)
	if err != nil {
		logger.ErrorCtx(ctx,
			fmt.Errorf("failed to get indexing block range"),
			zap.Error(err),
			zap.String("address", address),
			zap.String("chainID", string(chainID)),
		)
		return err
	}

	storedMinBlock := rangeResult.MinBlock
	storedMaxBlock := rangeResult.MaxBlock

	logger.InfoCtx(ctx, "Retrieved stored block range",
		zap.String("address", address),
		zap.Uint64("storedMinBlock", storedMinBlock),
		zap.Uint64("storedMaxBlock", storedMaxBlock),
	)

	// Step 3: Get the current latest block from blockchain
	var latestBlock uint64
	latestBlock, err = w.executor.GetLatestEthereumBlock(ctx)
	if err != nil {
		logger.ErrorCtx(ctx,
			fmt.Errorf("failed to get latest block"),
			zap.Error(err),
			zap.String("address", address),
		)
		return err
	}

	logger.InfoCtx(ctx, "Retrieved latest block from blockchain",
		zap.String("address", address),
		zap.Uint64("latestBlock", latestBlock),
	)

	limit := w.config.BudgetedIndexingDefaultDailyQuota + 1 // always get one more token to check if there are more tokens to index if quota is exhausted
	if !w.config.BudgetedIndexingModeEnabled {
		limit = math.MaxInt
	}

	// Step 4: Determine sweeping strategy
	if storedMinBlock == 0 && storedMaxBlock == 0 {
		// First run: No previous indexing exists
		// Fetch entire range from start to latest, process in chunks
		logger.InfoCtx(ctx, "First run detected, fetching all tokens from start to latest",
			zap.Uint64("startBlock", w.config.EthereumTokenSweepStartBlock),
			zap.Uint64("latestBlock", latestBlock),
		)

		var allTokens []domain.TokenWithBlock
		var allTokensResult domain.TokenWithBlockRangeResult
		allTokensResult, err = w.executor.GetEthereumTokenCIDsByOwnerWithinBlockRange(ctx, address, w.config.EthereumTokenSweepStartBlock, latestBlock, limit, domain.BlockScanOrderDesc)
		if err != nil {
			logger.ErrorCtx(ctx,
				fmt.Errorf("failed to fetch tokens"),
				zap.Error(err),
				zap.String("address", address),
			)
			return err
		}
		allTokens = allTokensResult.Tokens

		logger.InfoCtx(ctx, "Retrieved all tokens for first run",
			zap.String("address", address),
			zap.Int("tokenCount", len(allTokens)),
		)

		// Sort by block number (descending - newest first) and process in chunks
		sortTokensByBlock(allTokens, true)
		chunks := chunkTokensByTargetBlockAligned(allTokens, w.config.EthereumOwnerFirstBatchTarget, w.config.EthereumOwnerSubsequentBatchTarget)

		// Store the actual scanned block range (not token block range)
		scannedMinBlock := allTokensResult.EffectiveFromBlock
		scannedMaxBlock := allTokensResult.EffectiveToBlock

		for i, chunk := range chunks {
			logger.InfoCtx(ctx, "Processing token chunk",
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
					err = w.executor.UpdateIndexingBlockRangeForAddress(ctx, address, chainID, *actualMinBlock, scannedMaxBlock)
					storedMaxBlock = scannedMaxBlock
				} else {
					// Subsequent chunks - progressively update min_block toward start
					err = w.executor.UpdateIndexingBlockRangeForAddress(ctx, address, chainID, *actualMinBlock, storedMaxBlock)
				}
				if err != nil {
					logger.ErrorCtx(ctx,
						fmt.Errorf("failed to update block range"),
						zap.Error(err),
						zap.String("address", address),
					)
					return err
				}

				logger.InfoCtx(ctx, "Updated block range after chunk",
					zap.Uint64("actualMinBlock", *actualMinBlock),
					zap.Uint64("actualMaxBlock", *actualMaxBlock),
				)
			}

			// Check if we should continue after updating block range
			if !shouldContinue {
				// Quota exhausted after this chunk - block range already updated above with actual indexed range
				logger.InfoCtx(ctx, "Quota exhausted after processing chunk, will continue-as-new",
					zap.Int("chunksCompleted", i+1),
					zap.Int("totalChunks", len(chunks)),
				)
				return w.returnQuotaReschedule(ctx, jobID, quotaResetAt)
			}

			// Add a brief delay to prevent exceeding third-party service rate limits
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(2 * time.Second):
			}
		}

		// Final update to ensure we mark the complete scanned range
		err = w.executor.UpdateIndexingBlockRangeForAddress(ctx, address, chainID, scannedMinBlock, scannedMaxBlock)
		if err != nil {
			logger.ErrorCtx(ctx,
				fmt.Errorf("failed to update final block range"),
				zap.Error(err),
				zap.String("address", address),
			)
			return err
		}

		logger.InfoCtx(ctx, "Completed first run sweep",
			zap.Uint64("scannedMinBlock", scannedMinBlock),
			zap.Uint64("scannedMaxBlock", scannedMaxBlock),
		)
	} else {
		// Subsequent run: Sweep backward first (historical), then forward (latest updates)
		logger.InfoCtx(ctx, "Subsequent run detected, sweeping backward then forward",
			zap.Uint64("storedMinBlock", storedMinBlock),
			zap.Uint64("storedMaxBlock", storedMaxBlock),
			zap.Uint64("latestBlock", latestBlock),
		)

		// Part A: Sweep backward (historical data) - FIRST
		if storedMinBlock > w.config.EthereumTokenSweepStartBlock {
			logger.InfoCtx(ctx, "Sweeping backward for historical data",
				zap.Uint64("startBlock", w.config.EthereumTokenSweepStartBlock),
				zap.Uint64("storedMinBlock", storedMinBlock),
			)

			var backwardTokens []domain.TokenWithBlock
			var backwardTokensResult domain.TokenWithBlockRangeResult
			backwardTokensResult, err = w.executor.GetEthereumTokenCIDsByOwnerWithinBlockRange(ctx, address, w.config.EthereumTokenSweepStartBlock, storedMinBlock-1, limit, domain.BlockScanOrderDesc)
			if err != nil {
				logger.ErrorCtx(ctx,
					fmt.Errorf("failed to fetch backward tokens"),
					zap.Error(err),
					zap.String("address", address),
				)
				return err
			}
			backwardTokens = backwardTokensResult.Tokens

			logger.InfoCtx(ctx, "Retrieved backward sweep tokens",
				zap.String("address", address),
				zap.Int("tokenCount", len(backwardTokens)),
			)

			// Sort by block (descending - newest first) and process in chunks
			sortTokensByBlock(backwardTokens, true)
			chunks := chunkTokensByTargetBlockAligned(backwardTokens, w.config.EthereumOwnerSubsequentBatchTarget, w.config.EthereumOwnerSubsequentBatchTarget)

			// Store the actual scanned block range (not token block range)
			scannedMinBlock := backwardTokensResult.EffectiveFromBlock
			scannedMaxBlock := backwardTokensResult.EffectiveToBlock

			for i, chunk := range chunks {
				logger.InfoCtx(ctx, "Processing backward chunk",
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
					err = w.executor.UpdateIndexingBlockRangeForAddress(ctx, address, chainID, *actualMinBlock, storedMaxBlock)
					if err != nil {
						logger.ErrorCtx(ctx,
							fmt.Errorf("failed to update min block"),
							zap.Error(err),
							zap.String("address", address),
						)
						return err
					}

					logger.InfoCtx(ctx, "Updated min block after chunk",
						zap.Uint64("actualMinBlock", *actualMinBlock))
				}

				// Check if we should continue after updating block range
				if !shouldContinue {
					// Quota exhausted after this chunk - block range already updated above
					logger.InfoCtx(ctx, "Quota exhausted during backward sweep, will continue-as-new",
						zap.Int("chunksCompleted", i+1),
						zap.Int("totalChunks", len(chunks)),
					)
					return w.returnQuotaReschedule(ctx, jobID, quotaResetAt)
				}

				// Add a brief delay to prevent exceeding third-party service rate limits
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(2 * time.Second):
				}
			}

			// Final update to ensure we mark the complete scanned range
			err = w.executor.UpdateIndexingBlockRangeForAddress(ctx, address, chainID, scannedMinBlock, storedMaxBlock)
			if err != nil {
				logger.ErrorCtx(ctx,
					fmt.Errorf("failed to update final min block"),
					zap.Error(err),
					zap.String("address", address),
				)
				return err
			}

			storedMinBlock = scannedMinBlock
			logger.InfoCtx(ctx, "Completed backward sweep",
				zap.Uint64("scannedMinBlock", scannedMinBlock),
				zap.Uint64("scannedMaxBlock", scannedMaxBlock),
			)
		}

		// Part B: Sweep forward (latest updates) - SECOND
		if latestBlock > storedMaxBlock {
			logger.InfoCtx(ctx, "Sweeping forward for latest updates",
				zap.Uint64("storedMaxBlock", storedMaxBlock),
				zap.Uint64("latestBlock", latestBlock),
			)

			var forwardTokens []domain.TokenWithBlock
			var forwardTokensResult domain.TokenWithBlockRangeResult
			forwardTokensResult, err = w.executor.GetEthereumTokenCIDsByOwnerWithinBlockRange(ctx, address, storedMaxBlock+1, latestBlock, limit, domain.BlockScanOrderAsc)
			if err != nil {
				logger.ErrorCtx(ctx,
					fmt.Errorf("failed to fetch forward tokens"),
					zap.Error(err),
					zap.String("address", address),
				)
				return err
			}
			forwardTokens = forwardTokensResult.Tokens

			logger.InfoCtx(ctx, "Retrieved forward sweep tokens",
				zap.String("address", address),
				zap.Int("tokenCount", len(forwardTokens)),
			)

			// Sort by block (ascending - oldest first) and process in chunks
			sortTokensByBlock(forwardTokens, false)
			chunks := chunkTokensByTargetBlockAligned(forwardTokens, w.config.EthereumOwnerSubsequentBatchTarget, w.config.EthereumOwnerSubsequentBatchTarget)

			// Store the actual scanned block range (not token block range)
			scannedMinBlock := forwardTokensResult.EffectiveFromBlock
			scannedMaxBlock := forwardTokensResult.EffectiveToBlock

			for i, chunk := range chunks {
				logger.InfoCtx(ctx, "Processing forward chunk",
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
					err = w.executor.UpdateIndexingBlockRangeForAddress(ctx, address, chainID, storedMinBlock, *actualMaxBlock)
					if err != nil {
						logger.ErrorCtx(ctx,
							fmt.Errorf("failed to update max block"),
							zap.Error(err),
							zap.String("address", address),
						)
						return err
					}

					logger.InfoCtx(ctx, "Updated max block after chunk",
						zap.Uint64("actualMaxBlock", *actualMaxBlock))
				}

				// Check if we should continue after updating block range
				if !shouldContinue {
					// Quota exhausted after this chunk - block range already updated above
					logger.InfoCtx(ctx, "Quota exhausted during forward sweep, will continue-as-new",
						zap.Int("chunksCompleted", i+1),
						zap.Int("totalChunks", len(chunks)),
					)
					return w.returnQuotaReschedule(ctx, jobID, quotaResetAt)
				}

				// Add a brief delay to prevent exceeding third-party service rate limits
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(2 * time.Second):
				}
			}

			// Final update to ensure we mark the complete scanned range
			err = w.executor.UpdateIndexingBlockRangeForAddress(ctx, address, chainID, storedMinBlock, scannedMaxBlock)
			if err != nil {
				logger.ErrorCtx(ctx,
					fmt.Errorf("failed to update final max block"),
					zap.Error(err),
					zap.String("address", address),
				)
				return err
			}

			logger.InfoCtx(ctx, "Completed forward sweep",
				zap.Uint64("scannedMinBlock", scannedMinBlock),
				zap.Uint64("scannedMaxBlock", scannedMaxBlock),
			)
		}
	}

	logger.InfoCtx(ctx, "Ethereum token owner indexing completed", zap.String("address", address))

	return nil
}

// indexTokenChunk indexes a chunk of tokens using the IndexTokens workflow
// For owner-specific indexing, pass the address to enable efficient ERC1155 indexing
func (w *coreWorkflows) indexTokenChunk(ctx context.Context, tokenCIDs []domain.TokenCID, address *string) error {
	if len(tokenCIDs) == 0 {
		return nil
	}

	if err := w.IndexTokens(ctx, tokenCIDs, address); err != nil {
		logger.ErrorCtx(ctx,
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
func (w *coreWorkflows) processChunkWithQuota(
	ctx context.Context,
	address string,
	chainID domain.Chain,
	tokens []domain.TokenWithBlock,
	chunkInfo string, // e.g., "forward chunk 1/5" for logging
	jobID *int64, // optional jobs.id for progress tracking
) (bool, *uint64, *uint64, time.Time, error) {

	requestedCount := len(tokens)
	allowedCount := requestedCount
	var quotaResetAt time.Time
	if w.config.BudgetedIndexingModeEnabled {
		// Check quota
		quotaStatus, err := w.executor.GetQuotaInfo(ctx, address, chainID)
		if err != nil {
			return false, nil, nil, time.Time{}, fmt.Errorf("failed to check quota: %w", err)
		}

		quotaResetAt = quotaStatus.QuotaResetAt

		if quotaStatus.QuotaExhausted {
			logger.InfoCtx(ctx, "Quota exhausted, will reschedule at quota reset and continue",
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
		// TODO: Keep this for tezos for now, as we don't support truncating the tezos tokens yet
		completeBlockIndex := findLastCompleteBlockIndexByQuota(tokens, allowedCount)

		if completeBlockIndex <= 0 {
			// Edge case: even the first block exceeds quota
			// We still process it to ensure forward progress, but log a warning
			logger.WarnCtx(ctx, "First block exceeds quota, will process it anyway to ensure forward progress",
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
			logger.InfoCtx(ctx, "Limiting chunk to complete blocks within quota",
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
		err := w.executor.IncrementTokensIndexed(ctx, address, chainID, count)
		if err != nil {
			logger.ErrorCtx(ctx, fmt.Errorf("failed to increment token usage"),
				zap.Error(err),
				zap.String("address", address),
				zap.Int("count", count),
			)
			return false, nil, nil, time.Time{}, err
		}

		logger.InfoCtx(ctx, "Incremented token usage for budgeted indexing mode",
			zap.String("address", address),
			zap.Int("count", count),
		)
	}

	// Update job progress after successful chunk processing
	// Note: The store layer accumulates tokens_processed, so we pass the incremental count (chunk size)
	if jobID != nil {
		chunkSize := len(actualInfo.tokenCIDs)
		logger.InfoCtx(ctx, "Updating job progress (incremental)",
			zap.Int64("job_id", *jobID),
			zap.Int("chunkTokens", chunkSize),
			zap.Uint64("minBlock", actualInfo.minBlock),
			zap.Uint64("maxBlock", actualInfo.maxBlock),
		)

		err := w.executor.UpdateIndexingJobProgress(ctx,
			*jobID, chunkSize, actualInfo.minBlock, actualInfo.maxBlock)
		if err != nil {
			logger.ErrorCtx(ctx, fmt.Errorf("failed to update job progress"),
				zap.Error(err),
				zap.Int64("job_id", *jobID),
				zap.Int("chunkTokens", chunkSize))
			// Don't fail workflow if progress tracking fails
		}
	}

	// If we had to limit due to quota, return the actual range indexed and signal quota exhaustion
	if wasLimited {
		logger.InfoCtx(ctx, "Quota exhausted after partial chunk",
			zap.Int("indexed", len(actualInfo.tokenCIDs)),
			zap.Int("total", len(tokens)),
			zap.Uint64("actualMinBlock", actualInfo.minBlock),
			zap.Uint64("actualMaxBlock", actualInfo.maxBlock),
		)
		return false, &actualInfo.minBlock, &actualInfo.maxBlock, quotaResetAt, nil // Quota exhausted, return actual range
	}

	return true, &actualInfo.minBlock, &actualInfo.maxBlock, time.Time{}, nil // shouldContinue=true, success
}

// returnQuotaReschedule records paused status when a job id is available and returns ErrReschedule
// so the worker re-queues this job after the quota reset time (no in-process sleep in v1).
func (w *coreWorkflows) returnQuotaReschedule(ctx context.Context, jobID *int64, quotaResetAt time.Time) error {
	if jobID != nil {
		_ = w.executor.UpdateIndexingJobStatus(context.WithoutCancel(ctx), *jobID, schema.IndexingJobStatusPaused, time.Now().UTC())
	}
	return jobs.ErrReschedule(quotaResetAt)
}

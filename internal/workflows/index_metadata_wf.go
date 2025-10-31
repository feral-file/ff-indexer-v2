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
	"github.com/feral-file/ff-indexer-v2/internal/metadata"
	"github.com/feral-file/ff-indexer-v2/internal/types"
)

// IndexTokenMetadata indexes token metadata
func (w *workerCore) IndexTokenMetadata(ctx workflow.Context, tokenCID domain.TokenCID) error {
	logger.Info("Indexing token metadata", zap.String("tokenCID", tokenCID.String()))

	// Configure activity options with longer timeout for metadata fetching
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute, // Longer timeout for fetching from IPFS/Arweave
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	// Step 1: Fetch the token normalizedMetadata from the blockchain
	// This activity handles:
	// - ERC721: Call tokenURI() contract method
	// - ERC1155: Call uri() contract method
	// - FA2: Use TzKT API to get normalizedMetadata
	// It also processes the URI (IPFS, Arweave, HTTP, data URIs)
	var normalizedMetadata *metadata.NormalizedMetadata
	err := workflow.ExecuteActivity(ctx, w.executor.FetchTokenMetadata, tokenCID).Get(ctx, &normalizedMetadata)
	if err != nil {
		logger.Error(fmt.Errorf("failed to fetch token metadata: %w", err),
			zap.String("tokenCID", tokenCID.String()),
		)
		return err
	}

	// Collect media URLs from the normalized metadata
	mediaURLs := make(map[string]bool)
	if normalizedMetadata != nil {
		if normalizedMetadata.Image != "" {
			mediaURLs[normalizedMetadata.Image] = true
		}
		if normalizedMetadata.Animation != "" {
			mediaURLs[normalizedMetadata.Animation] = true
		}
	}

	// Step 2: Store or update the metadata in the database
	if normalizedMetadata != nil {
		err = workflow.ExecuteActivity(ctx, w.executor.UpsertTokenMetadata, tokenCID, normalizedMetadata).Get(ctx, nil)
		if err != nil {
			logger.Error(fmt.Errorf("failed to upsert token metadata: %w", err),
				zap.String("tokenCID", tokenCID.String()),
			)
			return err
		}
	}

	// Step 3: Enhance metadata from vendor APIs (ArtBlocks, fxhash, etc.)
	// This activity will:
	// - Detect if the token is from a known vendor (ArtBlocks, fxhash, etc.)
	// - Fetch additional metadata from vendor APIs
	// - Store enrichment data in enrichment_sources table
	// - Update token_metadata with enriched data and set enrichment_level to 'vendor'
	var enhancedMetadata *metadata.EnhancedMetadata
	if normalizedMetadata != nil {
		err = workflow.ExecuteActivity(ctx, w.executor.EnhanceTokenMetadata, tokenCID, normalizedMetadata).Get(ctx, &enhancedMetadata)
		if err != nil {
			// Log the error but don't fail the workflow
			// Enrichment is optional and should not block the main indexing flow
			logger.Warn("Failed to enhance token metadata (non-fatal)",
				zap.String("tokenCID", tokenCID.String()),
				zap.Error(err),
			)
		}

		// Collect media URLs from enhanced metadata
		if enhancedMetadata != nil {
			if enhancedMetadata.ImageURL != nil && *enhancedMetadata.ImageURL != "" {
				mediaURLs[*enhancedMetadata.ImageURL] = true
			}
			if enhancedMetadata.AnimationURL != nil && *enhancedMetadata.AnimationURL != "" {
				mediaURLs[*enhancedMetadata.AnimationURL] = true
			}
		}
	}

	// Step 4: Trigger media indexing workflow (fire and forget)
	// This should not fail the parent workflow
	if len(mediaURLs) > 0 {
		// Convert map to slice
		var urls []string
		for url := range mediaURLs {
			if types.IsValidURL(url) {
				urls = append(urls, url)
			}
		}

		logger.Info("Triggering media indexing workflow",
			zap.String("tokenCID", tokenCID.String()),
			zap.Int("mediaCount", len(urls)),
		)

		// Configure child workflow options for fire-and-forget
		childWorkflowOptions := workflow.ChildWorkflowOptions{
			WorkflowID:            fmt.Sprintf("index-media-token-%s", tokenCID.String()),
			WorkflowRunTimeout:    time.Hour,
			TaskQueue:             w.config.MediaTaskQueue,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
			ParentClosePolicy:     enums.PARENT_CLOSE_POLICY_ABANDON, // Don't wait for completion
		}
		childCtx := workflow.WithChildOptions(ctx, childWorkflowOptions)

		// Start the child workflow without waiting for result
		var wm workerMedia
		childWorkflow := workflow.ExecuteChildWorkflow(childCtx, wm.IndexMultipleMediaWorkflow, urls)

		// Only check if workflow started successfully, don't wait for completion
		var childExecution workflow.Execution
		if err := childWorkflow.GetChildWorkflowExecution().Get(childCtx, &childExecution); err != nil {
			// Log but don't fail the parent workflow
			logger.Warn("Failed to start media indexing workflow (non-fatal)",
				zap.String("tokenCID", tokenCID.String()),
				zap.Error(err),
			)
		} else {
			logger.Info("Media indexing workflow started",
				zap.String("tokenCID", tokenCID.String()),
				zap.String("workflowID", childExecution.ID),
			)
		}
	}

	logger.Info("Token metadata indexed successfully", zap.String("tokenCID", tokenCID.String()))

	return nil
}

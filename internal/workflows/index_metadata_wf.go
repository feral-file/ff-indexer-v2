package workflows

import (
	"fmt"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/metadata"
	"github.com/feral-file/ff-indexer-v2/internal/types"
	"github.com/feral-file/ff-indexer-v2/internal/webhook"
)

// IndexMetadataUpdate processes a metadata update event
func (w *workerCore) IndexMetadataUpdate(ctx workflow.Context, event *domain.BlockchainEvent) error {
	logger.InfoWf(ctx, "Processing metadata update event",
		zap.String("tokenCID", event.TokenCID().String()),
		zap.String("chain", string(event.Chain)),
		zap.String("txHash", event.TxHash),
	)

	// Configure activity options
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 10 * time.Second,
			MaximumAttempts: 2,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	// Step 1: Create the metadata update record in the database
	err := workflow.ExecuteActivity(ctx, w.executor.CreateMetadataUpdate, event).Get(ctx, nil)
	if err != nil {
		logger.ErrorWf(ctx,
			fmt.Errorf("failed to create metadata update record"),
			zap.String("tokenCID", event.TokenCID().String()),
			zap.Error(err),
		)
		return err
	}

	// Step 2: Start child workflow to index token metadata
	childWorkflowOptions := workflow.ChildWorkflowOptions{
		WorkflowID:               "index-metadata-" + event.TokenCID().String(),
		WorkflowExecutionTimeout: 30 * time.Minute,
		WorkflowIDReusePolicy:    enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		ParentClosePolicy:        enums.PARENT_CLOSE_POLICY_ABANDON,
	}
	childCtx := workflow.WithChildOptions(ctx, childWorkflowOptions)

	// Execute the child workflow without waiting for the result
	childWorkflowExec := workflow.ExecuteChildWorkflow(childCtx, w.IndexTokenMetadata, event.TokenCID(), nil).GetChildWorkflowExecution()
	if err := childWorkflowExec.Get(ctx, nil); err != nil {
		logger.ErrorWf(ctx,
			fmt.Errorf("failed to execute child workflow IndexTokenMetadata"),
			zap.String("tokenCID", event.TokenCID().String()),
			zap.Error(err),
		)
		return err
	}

	logger.InfoWf(ctx, "Metadata update event recorded and metadata indexing started",
		zap.String("tokenCID", event.TokenCID().String()),
	)

	return nil
}

// IndexTokenMetadata indexes token metadata
func (w *workerCore) IndexTokenMetadata(ctx workflow.Context, tokenCID domain.TokenCID, address *string) error {
	logger.InfoWf(ctx, "Indexing token metadata", zap.String("tokenCID", tokenCID.String()))

	// Configure activity options with longer timeout for metadata fetching
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute, // Longer timeout for fetching from IPFS/Arweave
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 10 * time.Second,
			MaximumAttempts: 2,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	// Step 1: Fetch the token normalizedMetadata from the blockchain
	// This activity handles:
	// - ERC721: Call tokenURI() contract method
	// - ERC1155: Call uri() contract method
	// - FA2: Use TzKT API to get normalizedMetadata
	// It also processes the URI (IPFS, Arweave, HTTP, data URIs)
	// and stores the metadata in the database
	var normalizedMetadata *metadata.NormalizedMetadata
	err := workflow.ExecuteActivity(ctx, w.executor.ResolveTokenMetadata, tokenCID).Get(ctx, &normalizedMetadata)
	if err != nil {
		logger.ErrorWf(ctx,
			fmt.Errorf("failed to fetch token metadata"),
			zap.String("tokenCID", tokenCID.String()),
			zap.Error(err),
		)
		// Log the error but don't fail the workflow
	}

	// Collect media URLs from the normalized metadata
	mediaURLs := make(map[string]interface{})
	if normalizedMetadata != nil {
		if normalizedMetadata.Image != "" {
			mediaURLs[normalizedMetadata.Image] = struct{}{}
		}
		if normalizedMetadata.Animation != "" {
			mediaURLs[normalizedMetadata.Animation] = struct{}{}
		}
	}

	// Step 2: Enhance metadata from vendor APIs (ArtBlocks, fxhash, OpenSea, etc.)
	// This activity will:
	// - Detect if the token is from a known vendor (ArtBlocks, fxhash, etc.)
	// - Fetch additional metadata from vendor APIs
	// - Store enrichment data in enrichment_sources table
	// - Update token_metadata with enriched data and set enrichment_level to 'vendor'
	var enhancedMetadata *metadata.EnhancedMetadata
	err = workflow.ExecuteActivity(ctx, w.executor.EnhanceTokenMetadata, tokenCID, normalizedMetadata).Get(ctx, &enhancedMetadata)
	if err != nil {
		// Log the error but don't fail the workflow
		// Enrichment is optional and should not block the main indexing flow
		logger.WarnWf(ctx, "Failed to enhance token metadata (non-fatal)",
			zap.String("tokenCID", tokenCID.String()),
			zap.Error(err),
		)
	}

	// Collect media URLs from enhanced metadata
	if enhancedMetadata != nil {
		if enhancedMetadata.ImageURL != nil && *enhancedMetadata.ImageURL != "" {
			mediaURLs[*enhancedMetadata.ImageURL] = struct{}{}
		}
		if enhancedMetadata.AnimationURL != nil && *enhancedMetadata.AnimationURL != "" {
			mediaURLs[*enhancedMetadata.AnimationURL] = struct{}{}
		}
	}

	// Convert map to slice of unique URLs
	var uniqueURLs []string
	for url := range mediaURLs {
		uniqueURLs = append(uniqueURLs, url)
	}

	// Step 3: Check media health and update viewability, then fire the webhook
	// This activity checks all URLs in parallel and updates the is_viewable column
	// It returns the viewability status and list of healthy URLs
	var result *MediaHealthCheckResult
	err = workflow.ExecuteActivity(ctx, w.executor.CheckMediaURLsHealthAndUpdateViewability,
		tokenCID.String(), uniqueURLs).Get(ctx, &result)
	if err != nil {
		logger.WarnWf(ctx, "Failed to check media health and update viewability (non-fatal)",
			zap.String("tokenCID", tokenCID.String()),
			zap.Error(err),
		)
		result = &MediaHealthCheckResult{
			IsViewable:  false,
			HealthyURLs: nil,
		}
	}

	logger.InfoWf(ctx, "Token viewability updated",
		zap.String("tokenCID", tokenCID.String()),
		zap.Bool("is_viewable", result.IsViewable),
		zap.Int("healthy_urls_count", len(result.HealthyURLs)),
	)

	// WEBHOOK: Trigger viewability changed event
	if result.IsViewable {
		w.triggerWebhookTokenIndexingNotification(ctx, tokenCID, webhook.EventTypeTokenIndexingViewable, address)
	} else {
		w.triggerWebhookTokenIndexingNotification(ctx, tokenCID, webhook.EventTypeTokenIndexingUnviewable, address)
	}

	// Step 4: Trigger media indexing workflow (fire and forget) - only for healthy URLs
	// This should not fail the parent workflow
	if len(result.HealthyURLs) > 0 {
		logger.InfoWf(ctx, "Triggering media indexing workflow",
			zap.String("tokenCID", tokenCID.String()),
			zap.Int("mediaCount", len(result.HealthyURLs)),
		)

		// Only index valid URLs from the healthy URLs list
		validURLs := make([]string, 0, len(result.HealthyURLs))
		for _, url := range result.HealthyURLs {
			if types.IsValidURL(url) {
				validURLs = append(validURLs, url)
			}
		}

		// Configure child workflow options for fire-and-forget
		childWorkflowOptions := workflow.ChildWorkflowOptions{
			WorkflowID:            fmt.Sprintf("index-media-token-%s", tokenCID.String()),
			WorkflowRunTimeout:    30 * time.Minute,
			TaskQueue:             w.config.MediaTaskQueue,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
			ParentClosePolicy:     enums.PARENT_CLOSE_POLICY_ABANDON, // Don't wait for completion
		}
		childCtx := workflow.WithChildOptions(ctx, childWorkflowOptions)

		// Start the child workflow without waiting for result
		// Use workflow name directly to avoid dependency on media package
		childWorkflow := workflow.ExecuteChildWorkflow(childCtx, "IndexMultipleMediaWorkflow", validURLs)

		// Only check if workflow started successfully, don't wait for completion
		var childExecution workflow.Execution
		if err := childWorkflow.GetChildWorkflowExecution().Get(childCtx, &childExecution); err != nil {
			// Log but don't fail the parent workflow
			logger.WarnWf(ctx, "Failed to start media indexing workflow (non-fatal)",
				zap.String("tokenCID", tokenCID.String()),
				zap.Error(err),
			)
		} else {
			logger.InfoWf(ctx, "Media indexing workflow started",
				zap.String("tokenCID", tokenCID.String()),
				zap.String("workflowID", childExecution.ID),
			)
		}
	}

	logger.InfoWf(ctx, "Token metadata indexed successfully", zap.String("tokenCID", tokenCID.String()))

	return nil
}

// IndexMultipleTokensMetadata indexes metadata for multiple tokens by triggering child workflows
func (w *workerCore) IndexMultipleTokensMetadata(ctx workflow.Context, tokenCIDs []domain.TokenCID) error {
	logger.InfoWf(ctx, "Indexing multiple tokens metadata", zap.Int("count", len(tokenCIDs)))

	if len(tokenCIDs) == 0 {
		logger.WarnWf(ctx, "No token CIDs provided for batch metadata indexing")
		return nil
	}

	// Trigger child workflows for each token
	// Each child workflow runs independently and in parallel
	var childFutures []workflow.ChildWorkflowFuture
	for _, tokenCID := range tokenCIDs {
		childWorkflowOptions := workflow.ChildWorkflowOptions{
			WorkflowID:               fmt.Sprintf("index-metadata-%s", tokenCID.String()),
			WorkflowExecutionTimeout: 30 * time.Minute,
			WorkflowIDReusePolicy:    enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
			ParentClosePolicy:        enums.PARENT_CLOSE_POLICY_ABANDON, // Don't wait for children to complete
		}
		childCtx := workflow.WithChildOptions(ctx, childWorkflowOptions)

		// Start the child workflow without waiting for result (fire and forget)
		childWorkflowFuture := workflow.ExecuteChildWorkflow(childCtx, w.IndexTokenMetadata, tokenCID, nil)
		childFutures = append(childFutures, childWorkflowFuture)

	}

	for _, childFuture := range childFutures {
		if err := childFuture.GetChildWorkflowExecution().Get(ctx, nil); err != nil {
			return err
		}
	}

	logger.InfoWf(ctx, "Multiple tokens metadata indexing workflows triggered",
		zap.Int("count", len(tokenCIDs)),
	)

	return nil
}

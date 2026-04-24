package workflows

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/providers/jobs"
	"github.com/feral-file/ff-indexer-v2/internal/types"
	"github.com/feral-file/ff-indexer-v2/internal/webhook"
)

// mediaIndexJobQueue is the jobs.queue for media work (cgo media worker).
const mediaIndexJobQueue = "media_index"

// IndexMetadataUpdate processes a metadata update event
func (w *coreWorkflows) IndexMetadataUpdate(ctx context.Context, event *domain.BlockchainEvent) error {
	logger.InfoCtx(ctx, "Processing metadata update event",
		zap.String("tokenCID", event.TokenCID().String()),
		zap.String("chain", string(event.Chain)),
		zap.String("txHash", event.TxHash),
	)

	// Step 1: Create the metadata update record in the database
	err := w.executor.CreateMetadataUpdate(ctx, event)
	if err != nil {
		logger.ErrorCtx(ctx,
			fmt.Errorf("failed to create metadata update record"),
			zap.String("tokenCID", event.TokenCID().String()),
			zap.Error(err),
		)
		return err
	}

	// Step 2: Enqueue token metadata indexing job
	_, _, err = w.jobQueue.Enqueue(ctx, jobs.EnqueueOptions{
		Queue:     tokenIndexJobQueue,
		Kind:      "IndexTokenMetadata",
		Args:      []any{event.TokenCID(), nil},
		UniqueKey: types.StringPtr(fmt.Sprintf("index-metadata-%s", event.TokenCID().String())),
	})
	if err != nil {
		logger.ErrorCtx(ctx,
			fmt.Errorf("failed to execute child workflow IndexTokenMetadata"),
			zap.String("tokenCID", event.TokenCID().String()),
			zap.Error(err),
		)
		return err
	}

	logger.InfoCtx(ctx, "Metadata update event recorded and metadata indexing started",
		zap.String("tokenCID", event.TokenCID().String()),
	)

	return nil
}

// IndexTokenMetadata indexes token metadata
func (w *coreWorkflows) IndexTokenMetadata(ctx context.Context, tokenCID domain.TokenCID, address *string) error {
	logger.InfoCtx(ctx, "Indexing token metadata", zap.String("tokenCID", tokenCID.String()))

	// Step 1: Fetch the token normalizedMetadata from the blockchain
	// This activity handles:
	// - ERC721: Call tokenURI() contract method
	// - ERC1155: Call uri() contract method
	// - FA2: Use TzKT API to get normalizedMetadata
	// It also processes the URI (IPFS, Arweave, HTTP, data URIs)
	// and stores the metadata in the database
	normalizedMetadata, err := w.executor.ResolveTokenMetadata(ctx, tokenCID)
	if err != nil {
		logger.ErrorCtx(ctx,
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
	enhancedMetadata, err := w.executor.EnhanceTokenMetadata(ctx, tokenCID, normalizedMetadata)
	if err != nil {
		// Log the error but don't fail the workflow
		// Enrichment is optional and should not block the main indexing flow
		logger.WarnCtx(ctx, "Failed to enhance token metadata (non-fatal)",
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
	result, err := w.executor.CheckMediaURLsHealthAndUpdateViewability(ctx,
		tokenCID.String(), uniqueURLs)
	if err != nil {
		logger.WarnCtx(ctx, "Failed to check media health and update viewability (non-fatal)",
			zap.String("tokenCID", tokenCID.String()),
			zap.Error(err),
		)
		result = &MediaHealthCheckResult{
			IsViewable:  false,
			HealthyURLs: nil,
		}
	}

	logger.InfoCtx(ctx, "Token viewability updated",
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
	// This should not fail the parent workflow.
	// When media indexing is disabled for the deployment, skip creating child workflows
	// so token-indexing does not enqueue work onto an unserved task queue.
	if !w.config.MediaEnabled {
		logger.InfoCtx(ctx, "Skipping media indexing workflow because media is disabled",
			zap.String("tokenCID", tokenCID.String()),
		)
	} else if len(result.HealthyURLs) > 0 {
		logger.InfoCtx(ctx, "Triggering media indexing workflow",
			zap.String("tokenCID", tokenCID.String()),
			zap.Int("mediaCount", len(result.HealthyURLs)),
		)

		// Only index valid URLs from the healthy URLs list
		validURLs := make([]string, 0, len(result.HealthyURLs))
		for _, u := range result.HealthyURLs {
			if types.IsValidURL(u) || types.IsDataURI(u) {
				validURLs = append(validURLs, u)
			}
		}

		// Start one IndexMediaWorkflow per URL (fire-and-forget) so that a single
		// oversized data URI cannot cause the entire batch to fail, and failures
		// are isolated to individual URLs.
		// FIXME: This is a temporary solution to avoid the entire batch failing due to a single oversized data URI.
		// The better approach for data URI could be Claim Check Pattern to avoid oversized job payloads.
		for _, u := range validURLs {
			uk := types.StringPtr("media-" + types.MD5Hash(u))
			_, _, err := w.jobQueue.Enqueue(ctx, jobs.EnqueueOptions{
				Queue:     mediaIndexJobQueue,
				Kind:      "IndexMediaWorkflow",
				Args:      []any{u},
				UniqueKey: uk,
			})
			if err != nil {
				logger.WarnCtx(ctx, "Failed to start media indexing workflow for URL (non-fatal)",
					zap.String("tokenCID", tokenCID.String()),
					zap.String("url", u),
					zap.Error(err),
				)
			} else {
				logger.InfoCtx(ctx, "Media indexing job enqueued",
					zap.String("tokenCID", tokenCID.String()),
					zap.String("uniqueKey", *uk),
				)
			}
		}
	}

	logger.InfoCtx(ctx, "Token metadata indexed successfully", zap.String("tokenCID", tokenCID.String()))

	return nil
}

// IndexMultipleTokensMetadata indexes metadata for multiple tokens by triggering child workflows
func (w *coreWorkflows) IndexMultipleTokensMetadata(ctx context.Context, tokenCIDs []domain.TokenCID) error {
	logger.InfoCtx(ctx, "Indexing multiple tokens metadata", zap.Int("count", len(tokenCIDs)))

	if len(tokenCIDs) == 0 {
		logger.WarnCtx(ctx, "No token CIDs provided for batch metadata indexing")
		return nil
	}

	// Trigger child workflows for each token
	// Each child workflow runs independently and in parallel
	for _, tokenCID := range tokenCIDs {
		_, _, err := w.jobQueue.Enqueue(ctx, jobs.EnqueueOptions{
			Queue:     tokenIndexJobQueue,
			Kind:      "IndexTokenMetadata",
			Args:      []any{tokenCID, nil},
			UniqueKey: types.StringPtr(fmt.Sprintf("index-metadata-%s", tokenCID.String())),
		})
		if err != nil {
			return err
		}
	}

	logger.InfoCtx(ctx, "Multiple tokens metadata indexing workflows triggered",
		zap.Int("count", len(tokenCIDs)),
	)

	return nil
}

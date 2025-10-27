package workflows

import (
	"fmt"
	"time"

	logger "github.com/bitmark-inc/autonomy-logger"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/metadata"
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

	// Step 1: Fetch the token metadata from the blockchain
	// This activity handles:
	// - ERC721: Call tokenURI() contract method
	// - ERC1155: Call uri() contract method
	// - FA2: Use TzKT API to get metadata
	// It also processes the URI (IPFS, Arweave, HTTP, data URIs)
	var metadata *metadata.NormalizedMetadata
	err := workflow.ExecuteActivity(ctx, w.executor.FetchTokenMetadata, tokenCID).Get(ctx, &metadata)
	if err != nil {
		logger.Error(fmt.Errorf("failed to fetch token metadata: %w", err),
			zap.String("tokenCID", tokenCID.String()),
		)
		return err
	}

	// Step 3: Store or update the metadata in the database
	if metadata != nil {
		err = workflow.ExecuteActivity(ctx, w.executor.UpsertTokenMetadata, tokenCID, metadata).Get(ctx, nil)
		if err != nil {
			logger.Error(fmt.Errorf("failed to upsert token metadata: %w", err),
				zap.String("tokenCID", tokenCID.String()),
			)
			return err
		}
	}

	// Step 4: Enhance metadata from vendor APIs (ArtBlocks, fxhash, etc.)
	// This activity will:
	// - Detect if the token is from a known vendor (ArtBlocks, fxhash, etc.)
	// - Fetch additional metadata from vendor APIs
	// - Store enrichment data in enrichment_sources table
	// - Update token_metadata with enriched data and set enrichment_level to 'vendor'
	if metadata != nil {
		err = workflow.ExecuteActivity(ctx, w.executor.EnhanceTokenMetadata, tokenCID, metadata).Get(ctx, nil)
		if err != nil {
			// Log the error but don't fail the workflow
			// Enrichment is optional and should not block the main indexing flow
			logger.Warn("Failed to enhance token metadata (non-fatal)",
				zap.String("tokenCID", tokenCID.String()),
				zap.Error(err),
			)
		}
	}

	logger.Info("Token metadata indexed successfully", zap.String("tokenCID", tokenCID.String()))

	return nil
}

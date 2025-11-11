package workflows

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
)

// IndexTokenProvenances indexes all provenances (balances and events) for a token
func (w *workerCore) IndexTokenProvenances(ctx workflow.Context, tokenCID domain.TokenCID) error {
	logger.InfoWf(ctx, "Starting token provenances indexing",
		zap.String("tokenCID", tokenCID.String()),
	)

	// Configure activity options with longer timeout for full provenance fetching
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 20 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 1,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	// Index token with full provenances
	err := workflow.ExecuteActivity(ctx, w.executor.IndexTokenWithFullProvenancesByTokenCID, tokenCID).Get(ctx, nil)
	if err != nil {
		logger.ErrorWf(ctx,
			fmt.Errorf("failed to index token provenances"),
			zap.Error(err),
			zap.String("tokenCID", tokenCID.String()),
		)
		return err
	}

	logger.InfoWf(ctx, "Token provenances indexed successfully",
		zap.String("tokenCID", tokenCID.String()),
	)

	return nil
}

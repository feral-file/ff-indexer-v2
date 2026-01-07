package workflows

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/types"
	"github.com/feral-file/ff-indexer-v2/internal/webhook"
)

// IndexTokenProvenances indexes all provenances (balances and events) for a token
// address is the address that triggered the indexing operation
// If nil, the indexing operation was not triggered by a specific address
func (w *workerCore) IndexTokenProvenances(ctx workflow.Context, tokenCID domain.TokenCID, address *string) error {
	logger.InfoWf(ctx, "Starting token provenances indexing",
		zap.String("tokenCID", tokenCID.String()),
		zap.String("address", types.SafeString(address)),
	)

	// Configure activity options with longer timeout for full provenance fetching
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Minute,
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

	// WEBHOOK: Trigger provenance complete event
	w.triggerWebhookTokenIndexingNotification(ctx, tokenCID, webhook.EventTypeTokenIndexingProvenanceCompleted, address)

	return nil
}

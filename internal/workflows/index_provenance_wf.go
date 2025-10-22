package workflows

import (
	"fmt"
	"time"

	logger "github.com/bitmark-inc/autonomy-logger"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
)

// IndexTokenProvenances indexes all provenances (balances and events) for a token
func (w *workerCore) IndexTokenProvenances(ctx workflow.Context, event *domain.BlockchainEvent) error {
	logger.Info("Starting token provenances indexing",
		zap.String("tokenCID", event.TokenCID().String()),
	)

	// Configure activity options with longer timeout for full provenance fetching
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 20 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	// Index token with full provenances
	err := workflow.ExecuteActivity(ctx, w.executor.IndexTokenWithFullProvenancesActivity, event).Get(ctx, nil)
	if err != nil {
		logger.Error(fmt.Errorf("failed to index token provenances: %w", err),
			zap.String("tokenCID", event.TokenCID().String()),
		)
		return err
	}

	logger.Info("Token provenances indexed successfully",
		zap.String("tokenCID", event.TokenCID().String()),
	)

	return nil
}

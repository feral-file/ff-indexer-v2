package workflows

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/types"
	"github.com/feral-file/ff-indexer-v2/internal/webhook"
)

// IndexTokenProvenances indexes all provenances (balances and events) for a token
// address is the address that triggered the indexing operation
// If nil, the indexing operation was not triggered by a specific address
func (w *coreWorkflows) IndexTokenProvenances(ctx context.Context, tokenCID domain.TokenCID, address *string) error {
	logger.InfoCtx(ctx, "Starting token provenances indexing",
		zap.String("tokenCID", tokenCID.String()),
		zap.String("address", types.SafeString(address)),
	)

	// Index token with full provenances
	err := w.executor.IndexTokenWithFullProvenancesByTokenCID(ctx, tokenCID)
	if err != nil {
		logger.ErrorCtx(ctx,
			fmt.Errorf("failed to index token provenances"),
			zap.Error(err),
			zap.String("tokenCID", tokenCID.String()),
		)
		return err
	}

	logger.InfoCtx(ctx, "Token provenances indexed successfully",
		zap.String("tokenCID", tokenCID.String()),
	)

	// WEBHOOK: Trigger provenance complete event
	w.triggerWebhookTokenIndexingNotification(ctx, tokenCID, webhook.EventTypeTokenIndexingProvenanceCompleted, address)

	return nil
}

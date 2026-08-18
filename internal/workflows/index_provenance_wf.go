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
	// Credit guard: full provenance for an EVM token replays its entire event history
	// from genesis (~0.9M Infura credits per token at the 10k block-span cap). Gating
	// here — the single entry point — covers every trigger: synchronous IndexToken
	// steps, enqueued jobs, and API-initiated refreshes. Skipped tokens keep their
	// minimal provenance and no completion webhook fires; they backfill when the
	// guard lifts. Tezos provenance is TzKT-backed and free, so it is not gated.
	if chain, _, _, _ := tokenCID.Parse(); w.config.EthereumFullProvenanceDisabled && chain.IsEVM() {
		logger.InfoCtx(ctx, "Full provenance indexing is disabled for EVM tokens (credit guard); skipping",
			zap.String("tokenCID", tokenCID.String()),
		)
		return nil
	}

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

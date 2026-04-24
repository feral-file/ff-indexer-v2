package workflows

import (
	"context"
	"fmt"
	"strings"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/providers/jobs"
	"github.com/feral-file/ff-indexer-v2/internal/types"
	"github.com/feral-file/ff-indexer-v2/internal/webhook"
)

const tokenIndexJobQueue = "token_index"

// IndexTokenMint processes a token mint event
func (w *coreWorkflows) IndexTokenMint(ctx context.Context, event *domain.BlockchainEvent) error {
	logger.InfoCtx(ctx, "Processing token mint event",
		zap.String("tokenCID", event.TokenCID().String()),
		zap.String("chain", string(event.Chain)),
		zap.String("txHash", event.TxHash),
	)

	// Check if contract is blacklisted
	if w.blacklist != nil && w.blacklist.IsTokenCIDBlacklisted(event.TokenCID()) {
		logger.InfoCtx(ctx, "Skipping blacklisted contract",
			zap.String("tokenCID", event.TokenCID().String()),
		)
		return nil
	}

	// Step 1: Create the token mint in the database
	err := w.executor.CreateTokenMint(ctx, event)
	if err != nil {
		logger.ErrorCtx(ctx,
			fmt.Errorf("failed to create token mint"),
			zap.Error(err),
			zap.String("tokenCID", event.TokenCID().String()),
		)
		return err
	}

	// Step 2: Start child workflow to index token metadata
	// This runs asynchronously without waiting for the result
	// FIXME: This should be optional of the token already minted
	w.startIndexTokenMetadataAsync(ctx, event.TokenCID(), nil)

	// WEBHOOK: Trigger ownership minted event notification (fire-and-forget)
	w.triggerWebhookTokenOwnershipNotification(ctx, event.TokenCID(), webhook.EventTypeTokenOwnershipMinted, event.FromAddress, event.ToAddress, event.Quantity)

	logger.InfoCtx(ctx, "Token mint processed successfully and metadata indexing started",
		zap.String("tokenCID", event.TokenCID().String()),
	)

	return nil
}

// IndexTokenTransfer processes a token transfer event
func (w *coreWorkflows) IndexTokenTransfer(ctx context.Context, event *domain.BlockchainEvent) error {
	logger.InfoCtx(ctx, "Processing token transfer event",
		zap.String("tokenCID", event.TokenCID().String()),
		zap.String("chain", string(event.Chain)),
		zap.String("from", types.SafeString(event.FromAddress)),
		zap.String("to", types.SafeString(event.ToAddress)),
		zap.String("txHash", event.TxHash),
	)

	// Check if contract is blacklisted
	if w.blacklist != nil && w.blacklist.IsTokenCIDBlacklisted(event.TokenCID()) {
		logger.InfoCtx(ctx, "Skipping blacklisted contract",
			zap.String("tokenCID", event.TokenCID().String()),
		)
		return nil
	}

	// Step 1: Check if token exists
	tokenExists, err := w.executor.CheckTokenExists(ctx, event.TokenCID())
	if err != nil {
		logger.ErrorCtx(ctx,
			fmt.Errorf("failed to check if token exists"),
			zap.Error(err),
			zap.String("tokenCID", event.TokenCID().String()),
		)
		return err
	}

	// Step 2: If token doesn't exist, trigger full token indexing workflow
	if !tokenExists {
		logger.InfoCtx(ctx, "Token doesn't exist, starting full token indexing",
			zap.String("tokenCID", event.TokenCID().String()),
		)

		// Execute the child workflow and waiting for the result
		if err := w.IndexTokenFromEvent(ctx, event); err != nil {
			logger.ErrorCtx(ctx,
				fmt.Errorf("failed to execute child workflow IndexFullToken"),
				zap.Error(err),
				zap.String("tokenCID", event.TokenCID().String()),
			)
			return err
		}

		logger.InfoCtx(ctx, "Full token indexing workflow started",
			zap.String("tokenCID", event.TokenCID().String()),
		)

		return nil
	}
	// Step 3: Token exists, update the token transfer
	logger.InfoCtx(ctx, "Token exists, updating token transfer",
		zap.String("tokenCID", event.TokenCID().String()),
	)

	err = w.executor.UpdateTokenTransfer(ctx, event)
	if err != nil {
		logger.ErrorCtx(ctx,
			fmt.Errorf("failed to update token transfer"),
			zap.Error(err),
			zap.String("tokenCID", event.TokenCID().String()),
		)
		return err
	}

	// WEBHOOK: Trigger ownership transferred event notification (fire-and-forget)
	w.triggerWebhookTokenOwnershipNotification(ctx, event.TokenCID(), webhook.EventTypeTokenOwnershipTransferred, event.FromAddress, event.ToAddress, event.Quantity)

	logger.InfoCtx(ctx, "Token transfer processed successfully",
		zap.String("tokenCID", event.TokenCID().String()),
	)

	return nil
}

// IndexTokenBurn processes a token burn event
func (w *coreWorkflows) IndexTokenBurn(ctx context.Context, event *domain.BlockchainEvent) error {
	logger.InfoCtx(ctx, "Processing token burn event",
		zap.String("tokenCID", event.TokenCID().String()),
		zap.String("chain", string(event.Chain)),
		zap.String("from", types.SafeString(event.FromAddress)),
		zap.String("txHash", event.TxHash),
	)

	// Check if contract is blacklisted
	if w.blacklist != nil && w.blacklist.IsTokenCIDBlacklisted(event.TokenCID()) {
		logger.InfoCtx(ctx, "Skipping blacklisted contract",
			zap.String("tokenCID", event.TokenCID().String()),
		)
		return nil
	}

	// Step 1: Check if token exists
	tokenExists, err := w.executor.CheckTokenExists(ctx, event.TokenCID())
	if err != nil {
		logger.ErrorCtx(ctx,
			fmt.Errorf("failed to check if token exists"),
			zap.Error(err),
			zap.String("tokenCID", event.TokenCID().String()),
		)
		return err
	}

	// Step 2: Token doesn't exist, return error
	if !tokenExists {
		return fmt.Errorf("token doesn't exist")
	}

	// Step 3: Token exists, update the token burn
	err = w.executor.UpdateTokenBurn(ctx, event)
	if err != nil {
		logger.ErrorCtx(ctx,
			fmt.Errorf("failed to update token burn"),
			zap.Error(err),
			zap.String("tokenCID", event.TokenCID().String()),
		)
		return err
	}

	// WEBHOOK: Trigger ownership burned event notification (fire-and-forget)
	w.triggerWebhookTokenOwnershipNotification(ctx, event.TokenCID(), webhook.EventTypeTokenOwnershipBurned, event.FromAddress, event.ToAddress, event.Quantity)

	logger.InfoCtx(ctx, "Token burn processed successfully",
		zap.String("tokenCID", event.TokenCID().String()),
	)

	return nil
}

// IndexTokenFromEvent indexes metadata and full provenances (provenance events and balances) for a token
func (w *coreWorkflows) IndexTokenFromEvent(ctx context.Context, event *domain.BlockchainEvent) error {
	logger.InfoCtx(ctx, "Starting full token indexing",
		zap.String("tokenCID", event.TokenCID().String()),
		zap.String("chain", string(event.Chain)),
	)

	// Check if contract is blacklisted
	if w.blacklist != nil && w.blacklist.IsTokenCIDBlacklisted(event.TokenCID()) {
		logger.InfoCtx(ctx, "Skipping blacklisted contract",
			zap.String("tokenCID", event.TokenCID().String()),
		)
		return nil
	}

	// Step 1: Create token with minimal provenance (from/to balances only) for speed
	err := w.executor.IndexTokenWithMinimalProvenancesByBlockchainEvent(ctx, event)
	if err != nil {
		logger.ErrorCtx(ctx,
			fmt.Errorf("failed to create token with minimal provenances"),
			zap.Error(err),
			zap.String("tokenCID", event.TokenCID().String()),
		)
		return err
	}

	logger.InfoCtx(ctx, "Token indexed with minimal provenances",
		zap.String("tokenCID", event.TokenCID().String()),
	)

	// WEBHOOK: Trigger queryable event notification (fire-and-forget)
	w.triggerWebhookTokenIndexingNotification(ctx, event.TokenCID(), webhook.EventTypeTokenIndexingQueryable, nil)

	// Step 2: Start child workflow to index token metadata (fire and forget)
	// FIXME: This should be optional of the token already minted
	w.startIndexTokenMetadataAsync(ctx, event.TokenCID(), nil)

	// Step 3: Start child workflow to index full provenance (fire and forget)
	// If the token is an ERC1155 token, skip the full provenance indexing workflow
	if event.Standard != domain.StandardERC1155 {
		w.startIndexTokenProvenancesAsync(ctx, event.TokenCID(), nil)
	}

	logger.InfoCtx(ctx, "Token indexing completed, metadata and provenances workflows started",
		zap.String("tokenCID", event.TokenCID().String()),
	)

	return nil
}

// IndexTokens indexes multiple tokens in chunks
// If address is provided, uses address-specific indexing for ERC1155 tokens
func (w *coreWorkflows) IndexTokens(ctx context.Context, tokenCIDs []domain.TokenCID, address *string) error {
	logger.InfoCtx(ctx, "Starting batch token indexing",
		zap.Int("count", len(tokenCIDs)),
		zap.String("address", types.SafeString(address)),
	)

	if len(tokenCIDs) == 0 {
		return nil
	}

	// Hard cap to prevent flooding the task queue when a chunk contains a very large number of tokens
	// (e.g., thousands of tokens minted in the same block).
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(5)

	for _, tc := range tokenCIDs {
		tid := tc
		g.Go(func() error {
			logger.InfoCtx(gctx, "Triggered token indexing workflow",
				zap.String("tokenCID", tid.String()),
			)
			return w.IndexToken(gctx, tid, address)
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	logger.InfoCtx(ctx, "Batch token indexing completed successfully",
		zap.Int("count", len(tokenCIDs)),
	)

	return nil
}

// IndexToken indexes a single token (metadata and provenances)
// address is the address that triggered the indexing operation
// If nil, the indexing operation was not triggered by a specific address
func (w *coreWorkflows) IndexToken(ctx context.Context, tokenCID domain.TokenCID, address *string) error {
	logger.InfoCtx(ctx, "Starting token indexing",
		zap.String("tokenCID", tokenCID.String()),
		zap.String("address", types.SafeString(address)),
	)

	// Check if contract is blacklisted
	if w.blacklist != nil && w.blacklist.IsTokenCIDBlacklisted(tokenCID) {
		logger.InfoCtx(ctx, "Skipping blacklisted contract",
			zap.String("tokenCID", tokenCID.String()),
		)
		return nil
	}

	// Step 1: Index token with minimal provenances (from blockchain query)
	err := w.executor.IndexTokenWithMinimalProvenancesByTokenCID(ctx, tokenCID, address)
	if err != nil {
		// Check if this is a "token not found on chain" error (or execution reverted)
		// In this case, we want to skip this token and continue the workflow
		if strings.Contains(err.Error(), "execution reverted") || strings.Contains(err.Error(), domain.ErrTokenNotFoundOnChain.Error()) {
			logger.ErrorCtx(ctx, fmt.Errorf("failed to index token with minimal provenances due to token not found on chain"),
				zap.String("tokenCID", tokenCID.String()),
				zap.Error(err),
			)
			return nil
		}

		// Check if this is a "contract is unreachable" error
		if strings.Contains(err.Error(), domain.ErrContractUnreachable.Error()) {
			logger.ErrorCtx(ctx, fmt.Errorf("failed to index token with minimal provenances due to contract is unreachable"),
				zap.String("tokenCID", tokenCID.String()),
				zap.Error(err),
			)
			return nil
		}

		// Check if this is a "balance is not a positive numeric value" error
		if strings.Contains(err.Error(), domain.ErrBalanceIsNotAPositiveNumericValue.Error()) {
			logger.ErrorCtx(ctx, fmt.Errorf("failed to index token with minimal provenances due to balance is not a positive numeric value, skipping token"),
				zap.String("tokenCID", tokenCID.String()),
				zap.Error(err),
			)
			return nil
		}

		logger.ErrorCtx(ctx,
			fmt.Errorf("failed to index token with minimal provenances"),
			zap.Error(err),
			zap.String("tokenCID", tokenCID.String()),
		)
		return err
	}

	logger.InfoCtx(ctx, "Token indexed with minimal provenances",
		zap.String("tokenCID", tokenCID.String()),
	)

	// WEBHOOK: Trigger queryable event notification (fire-and-forget)
	w.triggerWebhookTokenIndexingNotification(ctx, tokenCID, webhook.EventTypeTokenIndexingQueryable, address)

	// Step 2: Index token metadata (wait for completion).
	//
	// NOTE: This workflow is used by owner-indexing, which can enqueue a large number of tokens.
	// Waiting here provides backpressure so we don't flood the system with metadata/provenance
	// workflows across chunks (and across many owners).
	if err := w.IndexTokenMetadata(ctx, tokenCID, address); err != nil {
		logger.WarnCtx(ctx, "Metadata indexing workflow failed",
			zap.String("tokenCID", tokenCID.String()),
			zap.Error(err),
		)
	}

	// Step 3: Index full provenance (wait for completion).
	// If the token is an ERC1155 token and address is provided, skip the full provenance indexing workflow
	_, standard, _, _ := tokenCID.Parse()
	if standard == domain.StandardERC1155 && address != nil {
		logger.InfoCtx(ctx, "Skipping full provenance indexing for owner-specific ERC1155 token", zap.String("tokenCID", tokenCID.String()))
	} else {
		if err := w.IndexTokenProvenances(ctx, tokenCID, address); err != nil {
			logger.WarnCtx(ctx, "Full provenance indexing workflow failed",
				zap.String("tokenCID", tokenCID.String()),
				zap.Error(err),
			)
		}
	}

	logger.InfoCtx(ctx, "Token indexing completed, metadata and provenances workflows started",
		zap.String("tokenCID", tokenCID.String()),
	)

	return nil
}

func (w *coreWorkflows) startIndexTokenMetadataAsync(ctx context.Context, tokenCID domain.TokenCID, address *string) {
	_, _, err := w.jobQueue.Enqueue(ctx, jobs.EnqueueOptions{
		Queue:     tokenIndexJobQueue,
		Kind:      "IndexTokenMetadata",
		Args:      []any{tokenCID, address},
		UniqueKey: types.StringPtr(fmt.Sprintf("index-metadata-%s", tokenCID.String())),
	})
	if err != nil {
		logger.WarnCtx(ctx, "Failed to enqueue IndexTokenMetadata",
			zap.String("tokenCID", tokenCID.String()),
			zap.Error(err),
		)
	}
}

func (w *coreWorkflows) startIndexTokenProvenancesAsync(ctx context.Context, tokenCID domain.TokenCID, address *string) {
	_, _, err := w.jobQueue.Enqueue(ctx, jobs.EnqueueOptions{
		Queue:     tokenIndexJobQueue,
		Kind:      "IndexTokenProvenances",
		Args:      []any{tokenCID, address},
		UniqueKey: types.StringPtr(fmt.Sprintf("index-provenance-%s", tokenCID.String())),
	})
	if err != nil {
		logger.WarnCtx(ctx, "Failed to enqueue IndexTokenProvenances",
			zap.String("tokenCID", tokenCID.String()),
			zap.Error(err),
		)
	}
}

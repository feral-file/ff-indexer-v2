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
	"github.com/feral-file/ff-indexer-v2/internal/types"
)

// IndexTokenMint processes a token mint event
func (w *workerCore) IndexTokenMint(ctx workflow.Context, event *domain.BlockchainEvent) error {
	logger.InfoWf(ctx, "Processing token mint event",
		zap.String("tokenCID", event.TokenCID().String()),
		zap.String("chain", string(event.Chain)),
		zap.String("txHash", event.TxHash),
	)

	// Check if contract is blacklisted
	if w.blacklist != nil && w.blacklist.IsTokenCIDBlacklisted(event.TokenCID()) {
		logger.InfoWf(ctx, "Skipping blacklisted contract",
			zap.String("tokenCID", event.TokenCID().String()),
		)
		return nil
	}

	// Configure activity options
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 1,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	// Step 1: Create the token mint in the database
	err := workflow.ExecuteActivity(ctx, w.executor.CreateTokenMint, event).Get(ctx, nil)
	if err != nil {
		logger.ErrorWf(ctx,
			fmt.Errorf("failed to create token mint"),
			zap.Error(err),
			zap.String("tokenCID", event.TokenCID().String()),
		)
		return err
	}

	// Step 2: Start child workflow to index token metadata
	// This runs asynchronously without waiting for the result
	childWorkflowOptions := workflow.ChildWorkflowOptions{
		WorkflowID:               "index-metadata-" + event.TokenCID().String(),
		WorkflowExecutionTimeout: 15 * time.Minute,
		WorkflowIDReusePolicy:    enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		ParentClosePolicy:        enums.PARENT_CLOSE_POLICY_ABANDON,
	}
	childCtx := workflow.WithChildOptions(ctx, childWorkflowOptions)

	// Execute the child workflow without waiting for the result
	childWorkflowExec := workflow.ExecuteChildWorkflow(childCtx, w.IndexTokenMetadata, event.TokenCID()).GetChildWorkflowExecution()
	if err := childWorkflowExec.Get(ctx, nil); err != nil {
		logger.ErrorWf(ctx,
			fmt.Errorf("failed to execute child workflow IndexTokenMetadata"),
			zap.Error(err),
			zap.String("tokenCID", event.TokenCID().String()),
		)
		return err
	}

	logger.InfoWf(ctx, "Token mint processed successfully and metadata indexing started",
		zap.String("tokenCID", event.TokenCID().String()),
	)

	return nil
}

// IndexTokenTransfer processes a token transfer event
func (w *workerCore) IndexTokenTransfer(ctx workflow.Context, event *domain.BlockchainEvent) error {
	logger.InfoWf(ctx, "Processing token transfer event",
		zap.String("tokenCID", event.TokenCID().String()),
		zap.String("chain", string(event.Chain)),
		zap.String("from", types.SafeString(event.FromAddress)),
		zap.String("to", types.SafeString(event.ToAddress)),
		zap.String("txHash", event.TxHash),
	)

	// Check if contract is blacklisted
	if w.blacklist != nil && w.blacklist.IsTokenCIDBlacklisted(event.TokenCID()) {
		logger.InfoWf(ctx, "Skipping blacklisted contract",
			zap.String("tokenCID", event.TokenCID().String()),
		)
		return nil
	}

	// Configure activity options
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	// Step 1: Check if token exists
	var tokenExists bool
	err := workflow.ExecuteActivity(ctx, w.executor.CheckTokenExists, event.TokenCID()).Get(ctx, &tokenExists)
	if err != nil {
		logger.ErrorWf(ctx,
			fmt.Errorf("failed to check if token exists"),
			zap.Error(err),
			zap.String("tokenCID", event.TokenCID().String()),
		)
		return err
	}

	// Step 2: If token doesn't exist, trigger full token indexing workflow
	if !tokenExists {
		logger.InfoWf(ctx, "Token doesn't exist, starting full token indexing",
			zap.String("tokenCID", event.TokenCID().String()),
		)

		childWorkflowOptions := workflow.ChildWorkflowOptions{
			WorkflowID:               "index-full-token-" + event.TokenCID().String(),
			WorkflowExecutionTimeout: 30 * time.Minute,
			WorkflowIDReusePolicy:    enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
			ParentClosePolicy:        enums.PARENT_CLOSE_POLICY_ABANDON,
		}
		childCtx := workflow.WithChildOptions(ctx, childWorkflowOptions)

		// Execute the child workflow and waiting for the result
		childWorkflowExec := workflow.ExecuteChildWorkflow(childCtx, w.IndexTokenFromEvent, event)
		if err := childWorkflowExec.Get(ctx, nil); err != nil {
			logger.ErrorWf(ctx,
				fmt.Errorf("failed to execute child workflow IndexFullToken"),
				zap.Error(err),
				zap.String("tokenCID", event.TokenCID().String()),
			)
			return err
		}

		logger.InfoWf(ctx, "Full token indexing workflow started",
			zap.String("tokenCID", event.TokenCID().String()),
		)

		return nil
	}

	// Step 3: Token exists, update the token transfer
	err = workflow.ExecuteActivity(ctx, w.executor.UpdateTokenTransfer, event).Get(ctx, nil)
	if err != nil {
		logger.ErrorWf(ctx,
			fmt.Errorf("failed to update token transfer"),
			zap.Error(err),
			zap.String("tokenCID", event.TokenCID().String()),
		)
		return err
	}

	logger.InfoWf(ctx, "Token transfer processed successfully",
		zap.String("tokenCID", event.TokenCID().String()),
	)

	return nil
}

// IndexTokenBurn processes a token burn event
func (w *workerCore) IndexTokenBurn(ctx workflow.Context, event *domain.BlockchainEvent) error {
	logger.InfoWf(ctx, "Processing token burn event",
		zap.String("tokenCID", event.TokenCID().String()),
		zap.String("chain", string(event.Chain)),
		zap.String("from", types.SafeString(event.FromAddress)),
		zap.String("txHash", event.TxHash),
	)

	// Check if contract is blacklisted
	if w.blacklist != nil && w.blacklist.IsTokenCIDBlacklisted(event.TokenCID()) {
		logger.InfoWf(ctx, "Skipping blacklisted contract",
			zap.String("tokenCID", event.TokenCID().String()),
		)
		return nil
	}

	// Configure activity options
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 1,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	// Step 1: Check if token exists
	var tokenExists bool
	err := workflow.ExecuteActivity(ctx, w.executor.CheckTokenExists, event.TokenCID()).Get(ctx, &tokenExists)
	if err != nil {
		logger.ErrorWf(ctx,
			fmt.Errorf("failed to check if token exists"),
			zap.Error(err),
			zap.String("tokenCID", event.TokenCID().String()),
		)
		return err
	}

	// Step 2: If token doesn't exist, trigger full token indexing workflow
	if !tokenExists {
		logger.InfoWf(ctx, "Token doesn't exist, starting full token indexing",
			zap.String("tokenCID", event.TokenCID().String()),
		)

		childWorkflowOptions := workflow.ChildWorkflowOptions{
			WorkflowID:               "index-full-token-" + event.TokenCID().String(),
			WorkflowExecutionTimeout: 30 * time.Minute,
			WorkflowIDReusePolicy:    enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
			ParentClosePolicy:        enums.PARENT_CLOSE_POLICY_ABANDON,
		}
		childCtx := workflow.WithChildOptions(ctx, childWorkflowOptions)

		// Execute the child workflow and waiting for the result
		childWorkflowExec := workflow.ExecuteChildWorkflow(childCtx, w.IndexTokenFromEvent, event)
		if err := childWorkflowExec.Get(ctx, nil); err != nil {
			logger.ErrorWf(ctx,
				fmt.Errorf("failed to execute child workflow IndexFullToken"),
				zap.Error(err),
				zap.String("tokenCID", event.TokenCID().String()),
			)
			return err
		}

		logger.InfoWf(ctx, "Full token indexing workflow started",
			zap.String("tokenCID", event.TokenCID().String()),
		)

		return nil
	}

	// Step 3: Token exists, update the token burn
	err = workflow.ExecuteActivity(ctx, w.executor.UpdateTokenBurn, event).Get(ctx, nil)
	if err != nil {
		logger.ErrorWf(ctx,
			fmt.Errorf("failed to update token burn"),
			zap.Error(err),
			zap.String("tokenCID", event.TokenCID().String()),
		)
		return err
	}

	logger.InfoWf(ctx, "Token burn processed successfully",
		zap.String("tokenCID", event.TokenCID().String()),
	)

	return nil
}

// IndexTokenFromEvent indexes metadata and full provenances (provenance events and balances) for a token
func (w *workerCore) IndexTokenFromEvent(ctx workflow.Context, event *domain.BlockchainEvent) error {
	logger.InfoWf(ctx, "Starting full token indexing",
		zap.String("tokenCID", event.TokenCID().String()),
		zap.String("chain", string(event.Chain)),
	)

	// Check if contract is blacklisted
	if w.blacklist != nil && w.blacklist.IsTokenCIDBlacklisted(event.TokenCID()) {
		logger.InfoWf(ctx, "Skipping blacklisted contract",
			zap.String("tokenCID", event.TokenCID().String()),
		)
		return nil
	}

	// Configure activity options
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 1,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	// Step 1: Create token with minimal provenance (from/to balances only) for speed
	err := workflow.ExecuteActivity(ctx, w.executor.IndexTokenWithMinimalProvenancesByBlockchainEvent, event).Get(ctx, nil)
	if err != nil {
		logger.ErrorWf(ctx,
			fmt.Errorf("failed to create token with minimal provenances"),
			zap.Error(err),
			zap.String("tokenCID", event.TokenCID().String()),
		)
		return err
	}

	logger.InfoWf(ctx, "Token indexed with minimal provenances",
		zap.String("tokenCID", event.TokenCID().String()),
	)

	// Step 2: Start child workflow to index token metadata (fire and forget)
	metadataWorkflowOptions := workflow.ChildWorkflowOptions{
		WorkflowID:               "index-metadata-" + event.TokenCID().String(),
		WorkflowExecutionTimeout: 15 * time.Minute,
		WorkflowIDReusePolicy:    enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		ParentClosePolicy:        enums.PARENT_CLOSE_POLICY_ABANDON,
	}
	metadataCtx := workflow.WithChildOptions(ctx, metadataWorkflowOptions)

	// Execute the metadata workflow without waiting for the result
	metadataWorkflowExec := workflow.ExecuteChildWorkflow(metadataCtx, w.IndexTokenMetadata, event.TokenCID()).GetChildWorkflowExecution()
	if err := metadataWorkflowExec.Get(ctx, nil); err != nil {
		logger.WarnWf(ctx, "Failed to start metadata indexing workflow",
			zap.String("tokenCID", event.TokenCID().String()),
			zap.Error(err),
		)
		// Don't fail the whole workflow, just log the warning
	}

	// Step 3: Start child workflow to index full provenance (fire and forget)
	provenanceWorkflowOptions := workflow.ChildWorkflowOptions{
		WorkflowID:               "index-full-provenance-" + event.TokenCID().String(),
		WorkflowExecutionTimeout: 30 * time.Minute,
		WorkflowIDReusePolicy:    enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		ParentClosePolicy:        enums.PARENT_CLOSE_POLICY_ABANDON,
	}
	provenanceCtx := workflow.WithChildOptions(ctx, provenanceWorkflowOptions)

	// Execute the full provenance workflow without waiting for the result
	provenanceWorkflowExec := workflow.ExecuteChildWorkflow(provenanceCtx, w.IndexTokenProvenances, event.TokenCID()).GetChildWorkflowExecution()
	if err := provenanceWorkflowExec.Get(ctx, nil); err != nil {
		logger.WarnWf(ctx, "Failed to start full provenance indexing workflow",
			zap.String("tokenCID", event.TokenCID().String()),
			zap.Error(err),
		)
		// Don't fail the whole workflow, just log the warning
	}

	logger.InfoWf(ctx, "Token indexing completed, metadata and provenances workflows started",
		zap.String("tokenCID", event.TokenCID().String()),
	)

	return nil
}

// IndexTokens indexes multiple tokens in chunks
func (w *workerCore) IndexTokens(ctx workflow.Context, tokenCIDs []domain.TokenCID) error {
	logger.InfoWf(ctx, "Starting batch token indexing",
		zap.Int("count", len(tokenCIDs)),
	)

	// Configure child workflow options
	childWorkflowOptions := workflow.ChildWorkflowOptions{
		WorkflowExecutionTimeout: 15 * time.Minute,
		WorkflowIDReusePolicy:    enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		ParentClosePolicy:        enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
	}

	// Start child workflows for tokens
	var childFutures []workflow.ChildWorkflowFuture
	for _, tokenCID := range tokenCIDs {
		workflowID := fmt.Sprintf("index-token-%s", tokenCID.String())
		childWorkflowOptions.WorkflowID = workflowID
		childCtx := workflow.WithChildOptions(ctx, childWorkflowOptions)

		// Execute child workflow
		childFuture := workflow.ExecuteChildWorkflow(childCtx, w.IndexToken, tokenCID)
		childFutures = append(childFutures, childFuture)

		logger.InfoWf(ctx, "Triggered token indexing workflow",
			zap.String("tokenCID", tokenCID.String()),
			zap.String("workflowID", workflowID),
		)
	}

	// Wait for all workflows to complete
	for i, childFuture := range childFutures {
		if err := childFuture.Get(ctx, nil); err != nil {
			logger.WarnWf(ctx, "Child workflow failed",
				zap.String("tokenCID", tokenCIDs[i].String()),
				zap.Error(err),
			)
			// Continue with other workflows even if one fails
		}
	}

	logger.InfoWf(ctx, "Batch token indexing completed successfully",
		zap.Int("count", len(tokenCIDs)),
	)

	return nil
}

// IndexToken indexes a single token (metadata and provenances)
func (w *workerCore) IndexToken(ctx workflow.Context, tokenCID domain.TokenCID) error {
	logger.InfoWf(ctx, "Starting token indexing",
		zap.String("tokenCID", tokenCID.String()),
	)

	// Check if contract is blacklisted
	if w.blacklist != nil && w.blacklist.IsTokenCIDBlacklisted(tokenCID) {
		logger.InfoWf(ctx, "Skipping blacklisted contract",
			zap.String("tokenCID", tokenCID.String()),
		)
		return nil
	}

	// Configure activity options
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 1,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	// Step 1: Index token with minimal provenances (from blockchain query)
	err := workflow.ExecuteActivity(ctx, w.executor.IndexTokenWithMinimalProvenancesByTokenCID, tokenCID).Get(ctx, nil)
	if err != nil {
		logger.ErrorWf(ctx,
			fmt.Errorf("failed to index token with minimal provenances"),
			zap.Error(err),
			zap.String("tokenCID", tokenCID.String()),
		)
		return err
	}

	logger.InfoWf(ctx, "Token indexed with minimal provenances",
		zap.String("tokenCID", tokenCID.String()),
	)

	// Step 2: Start child workflow to index token metadata (fire and forget)
	metadataWorkflowOptions := workflow.ChildWorkflowOptions{
		WorkflowID:               "index-metadata-" + tokenCID.String(),
		WorkflowExecutionTimeout: 15 * time.Minute,
		WorkflowIDReusePolicy:    enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		ParentClosePolicy:        enums.PARENT_CLOSE_POLICY_ABANDON,
	}
	metadataCtx := workflow.WithChildOptions(ctx, metadataWorkflowOptions)

	metadataWorkflowExec := workflow.ExecuteChildWorkflow(metadataCtx, w.IndexTokenMetadata, tokenCID).GetChildWorkflowExecution()
	if err := metadataWorkflowExec.Get(ctx, nil); err != nil {
		logger.WarnWf(ctx, "Failed to start metadata indexing workflow",
			zap.String("tokenCID", tokenCID.String()),
			zap.Error(err),
		)
	}

	// Step 3: Start child workflow to index full provenance (fire and forget)
	provenanceWorkflowOptions := workflow.ChildWorkflowOptions{
		WorkflowID:               "index-full-provenance-" + tokenCID.String(),
		WorkflowExecutionTimeout: 30 * time.Minute,
		WorkflowIDReusePolicy:    enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		ParentClosePolicy:        enums.PARENT_CLOSE_POLICY_ABANDON,
	}
	provenanceCtx := workflow.WithChildOptions(ctx, provenanceWorkflowOptions)

	provenanceWorkflowExec := workflow.ExecuteChildWorkflow(provenanceCtx, w.IndexTokenProvenances, tokenCID).GetChildWorkflowExecution()
	if err := provenanceWorkflowExec.Get(ctx, nil); err != nil {
		logger.WarnWf(ctx, "Failed to start full provenance indexing workflow",
			zap.String("tokenCID", tokenCID.String()),
			zap.Error(err),
		)
	}

	logger.InfoWf(ctx, "Token indexing completed, metadata and provenances workflows started",
		zap.String("tokenCID", tokenCID.String()),
	)

	return nil
}

package workflows

import (
	"fmt"
	"time"

	logger "github.com/bitmark-inc/autonomy-logger"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/types"
)

// IndexTokenMint processes a token mint event
func (w *workerCore) IndexTokenMint(ctx workflow.Context, event *domain.BlockchainEvent) error {
	logger.Info("Processing token mint event",
		zap.String("tokenCID", event.TokenCID().String()),
		zap.String("chain", string(event.Chain)),
		zap.String("txHash", event.TxHash),
	)

	// Configure activity options
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	// Step 1: Create the token mint in the database
	err := workflow.ExecuteActivity(ctx, w.executor.CreateTokenMintActivity, event).Get(ctx, nil)
	if err != nil {
		logger.Error(fmt.Errorf("failed to create token mint: %w", err),
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
		logger.Error(fmt.Errorf("failed to execute child workflow IndexTokenMetadata: %w", err),
			zap.String("tokenCID", event.TokenCID().String()),
		)
		return err
	}

	logger.Info("Token mint processed successfully and metadata indexing started",
		zap.String("tokenCID", event.TokenCID().String()),
	)

	return nil
}

// IndexTokenTransfer processes a token transfer event
func (w *workerCore) IndexTokenTransfer(ctx workflow.Context, event *domain.BlockchainEvent) error {
	logger.Info("Processing token transfer event",
		zap.String("tokenCID", event.TokenCID().String()),
		zap.String("chain", string(event.Chain)),
		zap.String("from", types.SafeString(event.FromAddress)),
		zap.String("to", types.SafeString(event.ToAddress)),
		zap.String("txHash", event.TxHash),
	)

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
	err := workflow.ExecuteActivity(ctx, w.executor.CheckTokenExistsActivity, event.TokenCID()).Get(ctx, &tokenExists)
	if err != nil {
		logger.Error(fmt.Errorf("failed to check if token exists: %w", err),
			zap.String("tokenCID", event.TokenCID().String()),
		)
		return err
	}

	// Step 2: If token doesn't exist, trigger full token indexing workflow
	if !tokenExists {
		logger.Info("Token doesn't exist, starting full token indexing",
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
		childWorkflowExec := workflow.ExecuteChildWorkflow(childCtx, w.IndexToken, event)
		if err := childWorkflowExec.Get(ctx, nil); err != nil {
			logger.Error(fmt.Errorf("failed to execute child workflow IndexFullToken: %w", err),
				zap.String("tokenCID", event.TokenCID().String()),
			)
			return err
		}

		logger.Info("Full token indexing workflow started",
			zap.String("tokenCID", event.TokenCID().String()),
		)

		return nil
	}

	// Step 3: Token exists, update the token transfer
	err = workflow.ExecuteActivity(ctx, w.executor.UpdateTokenTransferActivity, event).Get(ctx, nil)
	if err != nil {
		logger.Error(fmt.Errorf("failed to update token transfer: %w", err),
			zap.String("tokenCID", event.TokenCID().String()),
		)
		return err
	}

	logger.Info("Token transfer processed successfully",
		zap.String("tokenCID", event.TokenCID().String()),
	)

	return nil
}

// IndexTokenBurn processes a token burn event
func (w *workerCore) IndexTokenBurn(ctx workflow.Context, event *domain.BlockchainEvent) error {
	logger.Info("Processing token burn event",
		zap.String("tokenCID", event.TokenCID().String()),
		zap.String("chain", string(event.Chain)),
		zap.String("from", types.SafeString(event.FromAddress)),
		zap.String("txHash", event.TxHash),
	)

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
	err := workflow.ExecuteActivity(ctx, w.executor.CheckTokenExistsActivity, event.TokenCID()).Get(ctx, &tokenExists)
	if err != nil {
		logger.Error(fmt.Errorf("failed to check if token exists: %w", err),
			zap.String("tokenCID", event.TokenCID().String()),
		)
		return err
	}

	// Step 2: If token doesn't exist, trigger full token indexing workflow
	if !tokenExists {
		logger.Info("Token doesn't exist, starting full token indexing",
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
		childWorkflowExec := workflow.ExecuteChildWorkflow(childCtx, w.IndexToken, event)
		if err := childWorkflowExec.Get(ctx, nil); err != nil {
			logger.Error(fmt.Errorf("failed to execute child workflow IndexFullToken: %w", err),
				zap.String("tokenCID", event.TokenCID().String()),
			)
			return err
		}

		logger.Info("Full token indexing workflow started",
			zap.String("tokenCID", event.TokenCID().String()),
		)

		return nil
	}

	// Step 3: Token exists, update the token burn
	err = workflow.ExecuteActivity(ctx, w.executor.UpdateTokenBurnActivity, event).Get(ctx, nil)
	if err != nil {
		logger.Error(fmt.Errorf("failed to update token burn: %w", err),
			zap.String("tokenCID", event.TokenCID().String()),
		)
		return err
	}

	logger.Info("Token burn processed successfully",
		zap.String("tokenCID", event.TokenCID().String()),
	)

	return nil
}

// IndexMetadataUpdate processes a metadata update event
func (w *workerCore) IndexMetadataUpdate(ctx workflow.Context, event *domain.BlockchainEvent) error {
	logger.Info("Processing metadata update event",
		zap.String("tokenCID", event.TokenCID().String()),
		zap.String("chain", string(event.Chain)),
		zap.String("txHash", event.TxHash),
	)

	// Configure activity options
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	// Step 1: Create the metadata update record in the database
	err := workflow.ExecuteActivity(ctx, w.executor.CreateMetadataUpdateActivity, event).Get(ctx, nil)
	if err != nil {
		logger.Error(fmt.Errorf("failed to create metadata update record: %w", err),
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
		logger.Error(fmt.Errorf("failed to execute child workflow IndexTokenMetadata: %w", err),
			zap.String("tokenCID", event.TokenCID().String()),
		)
		return err
	}

	logger.Info("Metadata update event recorded and metadata indexing started",
		zap.String("tokenCID", event.TokenCID().String()),
	)

	return nil
}

// IndexToken indexes metadata and full provenances (provenance events and balances) for a token
func (w *workerCore) IndexToken(ctx workflow.Context, event *domain.BlockchainEvent) error {
	logger.Info("Starting full token indexing",
		zap.String("tokenCID", event.TokenCID().String()),
		zap.String("chain", string(event.Chain)),
	)

	// Configure activity options
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	// Step 1: Create token with minimal provenance (from/to balances only) for speed
	err := workflow.ExecuteActivity(ctx, w.executor.IndexTokenWithMinimalProvenancesActivity, event).Get(ctx, nil)
	if err != nil {
		logger.Error(fmt.Errorf("failed to create token with minimal provenances: %w", err),
			zap.String("tokenCID", event.TokenCID().String()),
		)
		return err
	}

	logger.Info("Token indexed with minimal provenances",
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
		logger.Warn("Failed to start metadata indexing workflow",
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
	provenanceWorkflowExec := workflow.ExecuteChildWorkflow(provenanceCtx, w.IndexTokenProvenances, event).GetChildWorkflowExecution()
	if err := provenanceWorkflowExec.Get(ctx, nil); err != nil {
		logger.Warn("Failed to start full provenance indexing workflow",
			zap.String("tokenCID", event.TokenCID().String()),
			zap.Error(err),
		)
		// Don't fail the whole workflow, just log the warning
	}

	logger.Info("Token indexing completed, metadata and provenances workflows started",
		zap.String("tokenCID", event.TokenCID().String()),
	)

	return nil
}

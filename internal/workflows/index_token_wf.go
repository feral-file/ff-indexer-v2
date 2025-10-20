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

	// Step 1: Create or update the token transfer in the database
	// It returns whether the token was newly created
	var wasNewlyCreated bool
	err := workflow.ExecuteActivity(ctx, w.executor.CreateOrUpdateTokenTransferActivity, event).Get(ctx, &wasNewlyCreated)
	if err != nil {
		logger.Error(fmt.Errorf("failed to create or update token transfer: %w", err),
			zap.String("tokenCID", event.TokenCID().String()),
		)
		return err
	}

	// Step 2: If token was newly created, start metadata indexing
	if wasNewlyCreated {
		logger.Info("Token was newly created from transfer event, starting metadata indexing",
			zap.String("tokenCID", event.TokenCID().String()),
		)

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
	}

	logger.Info("Token transfer processed successfully",
		zap.String("tokenCID", event.TokenCID().String()),
		zap.Bool("wasNewlyCreated", wasNewlyCreated),
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

	// Step 1: Update the token burn in the database
	err := workflow.ExecuteActivity(ctx, w.executor.UpdateTokenBurnActivity, event).Get(ctx, nil)
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

//go:build cgo

package workflowsmedia

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/types"
)

// IndexMultipleMediaWorkflow handles the media processing for a list of URLs
// It triggers child workflows concurrently (fire and forget) for each URL
func (w *worker) IndexMultipleMediaWorkflow(ctx workflow.Context, urls []string) error {
	logger.InfoWf(ctx, "Starting multiple media indexing", zap.Int("count", len(urls)))

	// Remove duplicate URLs
	uniqueURLs := make(map[string]bool)
	for _, url := range urls {
		if types.IsValidURL(url) {
			uniqueURLs[url] = true
		}
	}

	if len(uniqueURLs) == 0 {
		logger.InfoWf(ctx, "No media URLs to process")
		return nil
	}

	logger.InfoWf(ctx, "Processing unique media URLs", zap.Int("count", len(uniqueURLs)))

	// Configure child workflow options
	childWorkflowOptions := workflow.ChildWorkflowOptions{
		WorkflowRunTimeout: 30 * time.Minute, // Longer timeout for media processing
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    30 * time.Second,
			BackoffCoefficient: 2.0,
			MaximumAttempts:    2,
		},
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		ParentClosePolicy:     enums.PARENT_CLOSE_POLICY_ABANDON, // Fire and forget
	}

	// Start all child workflows concurrently (fire and forget)
	for url := range uniqueURLs {
		// Generate a deterministic, fixed-length workflow ID from URL
		// Using SHA-256 hash ensures it's always within Temporal's 255 char limit
		// and is safe for use in workflow IDs
		hash := sha256.Sum256([]byte(url))
		workflowID := fmt.Sprintf("index-media-%s", base64.RawURLEncoding.EncodeToString(hash[:]))
		childWorkflowOptions.WorkflowID = workflowID
		childCtx := workflow.WithChildOptions(ctx, childWorkflowOptions)

		// Start child workflow without waiting for result
		childWorkflow := workflow.ExecuteChildWorkflow(childCtx, w.IndexMediaWorkflow, url)

		// We don't call Get() on the future, making it fire-and-forget
		// Just check if the workflow started successfully
		var childExecution workflow.Execution
		if err := childWorkflow.GetChildWorkflowExecution().Get(childCtx, &childExecution); err != nil {
			logger.WarnWf(ctx, "Failed to start child workflow for media URL",
				zap.String("url", url),
				zap.Error(err),
			)
			// Continue with other URLs even if one fails to start
			continue
		}

		logger.InfoWf(ctx, "Started child workflow for media URL",
			zap.String("url", url),
			zap.String("workflowID", childExecution.ID),
		)
	}

	logger.InfoWf(ctx, "All media child workflows triggered", zap.Int("count", len(uniqueURLs)))
	return nil
}

// IndexMediaWorkflow handles the media processing for a single URL
// This workflow uses a separate task queue with higher execution time
func (w *worker) IndexMediaWorkflow(ctx workflow.Context, url string) error {
	logger.InfoWf(ctx, "Starting media indexing", zap.String("url", url))

	// Configure activity options with longer timeout for media processing
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Minute, // Much longer timeout for downloading and uploading media
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    30 * time.Second,
			BackoffCoefficient: 2.0,
			MaximumAttempts:    2,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	// Execute the media indexing activity
	err := workflow.ExecuteActivity(ctx, w.executor.IndexMediaFile, url).Get(ctx, nil)
	if err != nil {
		logger.ErrorWf(ctx,
			fmt.Errorf("failed to index media file"),
			zap.Error(err),
			zap.String("url", url),
		)
		return err
	}

	logger.InfoWf(ctx, "Media indexed successfully", zap.String("url", url))
	return nil
}

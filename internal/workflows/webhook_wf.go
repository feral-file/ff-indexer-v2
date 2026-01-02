package workflows

import (
	"fmt"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/zap"

	"github.com/oklog/ulid/v2"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
	"github.com/feral-file/ff-indexer-v2/internal/webhook"
)

// NotifyWebhookClients is the orchestration workflow that:
// 1. Queries active webhook clients matching the event type
// 2. Triggers a delivery workflow for each client (fire-and-forget)
func (w *workerCore) NotifyWebhookClients(ctx workflow.Context, event webhook.WebhookEvent) error {
	logger.InfoWf(ctx, "Starting webhook notification orchestration",
		zap.String("eventID", event.EventID),
		zap.String("eventType", event.EventType))

	// Configure activity options
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 2,
			InitialInterval: 5 * time.Second,
		},
	}
	activityCtx := workflow.WithActivityOptions(ctx, activityOptions)

	// Get active webhook clients matching the event type
	var clients []*schema.WebhookClient
	err := workflow.ExecuteActivity(activityCtx, w.executor.GetActiveWebhookClientsByEventType, event.EventType).Get(activityCtx, &clients)
	if err != nil {
		return err
	}

	if len(clients) == 0 {
		logger.InfoWf(ctx, "No active webhook clients found for event type",
			zap.String("eventType", event.EventType))
		return nil
	}

	logger.InfoWf(ctx, "Found active webhook clients",
		zap.Int("count", len(clients)),
		zap.String("eventType", event.EventType))

	// Trigger delivery workflow for each client (fire-and-forget)
	for _, client := range clients {
		deliveryWorkflowOptions := workflow.ChildWorkflowOptions{
			WorkflowID:            fmt.Sprintf("webhook-delivery-%s-%s", client.ClientID, event.EventID),
			WorkflowRunTimeout:    1 * time.Hour, // Allow time for all retries
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
			ParentClosePolicy:     enums.PARENT_CLOSE_POLICY_ABANDON, // Don't wait
		}
		deliveryCtx := workflow.WithChildOptions(ctx, deliveryWorkflowOptions)

		// Start delivery workflow (don't wait for result)
		deliveryWorkflow := workflow.ExecuteChildWorkflow(deliveryCtx, w.DeliverWebhook, client.ClientID, event)

		// Only verify it started successfully
		var deliveryExecution workflow.Execution
		if err := deliveryWorkflow.GetChildWorkflowExecution().Get(ctx, &deliveryExecution); err != nil {
			logger.WarnWf(ctx, "Failed to start webhook delivery workflow",
				zap.String("clientID", client.ClientID),
				zap.String("eventID", event.EventID),
				zap.Error(err))
			continue
		}

		logger.InfoWf(ctx, "Webhook delivery workflow started",
			zap.String("clientID", client.ClientID),
			zap.String("workflowID", deliveryExecution.ID))
	}

	logger.InfoWf(ctx, "Webhook notification orchestration completed",
		zap.Int("clientsNotified", len(clients)))

	return nil
}

// DeliverWebhook handles webhook delivery to a single client
// Uses Temporal's retry policy for automatic retry with exponential backoff
func (w *workerCore) DeliverWebhook(ctx workflow.Context, clientID string, event webhook.WebhookEvent) error {
	logger.InfoWf(ctx, "Starting webhook delivery",
		zap.String("clientID", clientID),
		zap.String("eventID", event.EventID),
		zap.String("eventType", event.EventType))

	// Configure activity options for client lookup
	lookupActivityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 2,
			InitialInterval: 5 * time.Second,
		},
	}
	lookupCtx := workflow.WithActivityOptions(ctx, lookupActivityOptions)

	// Get client details
	var client *schema.WebhookClient
	err := workflow.ExecuteActivity(lookupCtx, w.executor.GetWebhookClientByID, clientID).Get(lookupCtx, &client)
	if err != nil {
		return err
	}
	if client == nil {
		logger.InfoWf(ctx, "Client not found, skipping delivery",
			zap.String("clientID", clientID))
		return nil
	}

	if !client.IsActive {
		logger.InfoWf(ctx, "Client is not active, skipping delivery",
			zap.String("clientID", clientID))
		return nil
	}

	// Create delivery record
	workflowInfo := workflow.GetInfo(ctx)
	var deliveryID uint64
	delivery := &schema.WebhookDelivery{
		ClientID:      client.ClientID,
		EventID:       event.EventID,
		EventType:     event.EventType,
		WorkflowID:    workflowInfo.WorkflowExecution.ID,
		WorkflowRunID: workflowInfo.WorkflowExecution.RunID,
		// Payload will be marshaled by the store when creating the record
	}

	err = workflow.ExecuteActivity(lookupCtx, w.executor.CreateWebhookDeliveryRecord, delivery, event).Get(lookupCtx, &deliveryID)
	if err != nil {
		return err
	}

	logger.InfoWf(ctx, "Webhook delivery record created",
		zap.Uint64("deliveryID", deliveryID))

	// Configure delivery activity with exponential backoff retry policy
	// Temporal will automatically retry with exponential backoff: 5s, 10s, 20s, 40s, 80s
	deliveryActivityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    5 * time.Second,                // First retry after 5s
			BackoffCoefficient: 2.0,                            // Double each time: 5s -> 10s -> 20s -> 40s -> 80s
			MaximumAttempts:    int32(client.RetryMaxAttempts), //nolint:gosec,G115
		},
	}
	deliveryCtx := workflow.WithActivityOptions(ctx, deliveryActivityOptions)

	// Execute delivery activity - Temporal handles retries automatically
	var deliveryResult webhook.DeliveryResult
	err = workflow.ExecuteActivity(deliveryCtx, w.executor.DeliverWebhookHTTP, client, event, deliveryID).Get(deliveryCtx, &deliveryResult)
	if err != nil {
		return err
	}

	logger.InfoWf(ctx, "Webhook delivered successfully",
		zap.String("clientID", clientID),
		zap.String("eventID", event.EventID),
		zap.Int("statusCode", deliveryResult.StatusCode))

	return nil
}

// triggerWebhookNotification triggers a webhook notification workflow (fire-and-forget)
// This helper function is called after successful token indexing operations to notify webhook clients
func (w *workerCore) triggerWebhookNotification(ctx workflow.Context, tokenCID domain.TokenCID, eventType string) {
	// Parse token CID to get components
	chain, standard, contract, tokenNumber := tokenCID.Parse()

	// Create webhook event with ULID for unique, time-sortable event ID
	ulid := ulid.MustNewDefault(workflow.Now(ctx))
	webhookEvent := webhook.WebhookEvent{
		EventID:   ulid.String(),
		EventType: eventType,
		Timestamp: workflow.Now(ctx),
		Data: webhook.EventData{
			TokenCID:    tokenCID.String(),
			Chain:       string(chain),
			Standard:    string(standard),
			Contract:    contract,
			TokenNumber: tokenNumber,
		},
	}

	// Configure child workflow options (fire-and-forget)
	childWorkflowOptions := workflow.ChildWorkflowOptions{
		WorkflowID:            fmt.Sprintf("webhook-notify-%s-%s", eventType, webhookEvent.EventID),
		WorkflowRunTimeout:    1 * time.Hour, // Allow time for all client deliveries and retries
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		ParentClosePolicy:     enums.PARENT_CLOSE_POLICY_ABANDON, // Don't wait for completion
	}
	childCtx := workflow.WithChildOptions(ctx, childWorkflowOptions)

	// Start notification workflow (don't wait for result)
	childWorkflow := workflow.ExecuteChildWorkflow(childCtx, w.NotifyWebhookClients, webhookEvent)

	// Only verify it started successfully
	var childExecution workflow.Execution
	if err := childWorkflow.GetChildWorkflowExecution().Get(ctx, &childExecution); err != nil {
		logger.WarnWf(ctx, "Failed to start webhook notification workflow",
			zap.String("eventType", eventType),
			zap.String("tokenCID", tokenCID.String()),
			zap.Error(err))
		return
	}

	logger.InfoWf(ctx, "Webhook notification workflow started",
		zap.String("eventType", eventType),
		zap.String("workflowID", childExecution.ID))
}

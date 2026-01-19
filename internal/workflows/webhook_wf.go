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
			WorkflowRunTimeout:    24 * time.Hour, // Allow time for all retries
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
	var deliveryID uint64
	workflowExecutionID := w.temporalWorkflow.GetExecutionID(ctx)
	workflowRunID := w.temporalWorkflow.GetRunID(ctx)
	delivery := &schema.WebhookDelivery{
		ClientID:      client.ClientID,
		EventID:       event.EventID,
		EventType:     event.EventType,
		WorkflowID:    workflowExecutionID,
		WorkflowRunID: workflowRunID,
		// Payload will be marshaled by the store when creating the record
	}

	err = workflow.ExecuteActivity(lookupCtx, w.executor.CreateWebhookDeliveryRecord, delivery, event).Get(lookupCtx, &deliveryID)
	if err != nil {
		return err
	}

	logger.InfoWf(ctx, "Webhook delivery record created",
		zap.Uint64("deliveryID", deliveryID))

	// Configure delivery activity with exponential backoff retry policy
	// Temporal will automatically retry with exponential backoff: 15m, 30m, 1h, 2h, 4h
	deliveryActivityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    15 * time.Minute,                   // First retry after 15 minutes
			BackoffCoefficient: 2.0,                                // Double each time: 15m -> 30m -> 1h -> 2h -> 4h
			MaximumAttempts:    int32(client.RetryMaxAttempts) + 1, //nolint:gosec,G115
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

// triggerWebhookTokenOwnershipNotification triggers a webhook notification workflow for a token ownership event
// eventType is the type of event (e.g., "token.ownership.minted", "token.ownership.transferred", "token.ownership.burned")
// from is the address of the sender
// to is the address of the receiver
// quantity is the number of tokens transferred
func (w *workerCore) triggerWebhookTokenOwnershipNotification(ctx workflow.Context, tokenCID domain.TokenCID, eventType string, from *string, to *string, quantity string) {
	// Parse token CID to get components
	chain, standard, contract, tokenNumber := tokenCID.Parse()

	// Create webhook event with ULID for unique, time-sortable event ID
	var eventID string
	err := workflow.SideEffect(ctx, func(ctx workflow.Context) interface{} {
		return ulid.MustNewDefault(time.Now()).String()
	}).Get(&eventID)
	if err != nil {
		logger.ErrorWf(ctx, err, zap.String("tokenCID", tokenCID.String()), zap.String("eventType", eventType))
		return
	}

	webhookEvent := webhook.WebhookEvent{
		EventID:   eventID,
		EventType: eventType,
		Timestamp: workflow.Now(ctx),
		Data: webhook.OwnershipEventData{
			EventData: webhook.EventData{
				TokenCID:    tokenCID.String(),
				Chain:       string(chain),
				Standard:    string(standard),
				Contract:    contract,
				TokenNumber: tokenNumber,
			},
			FromAddress: from,
			ToAddress:   to,
			Quantity:    quantity,
		},
	}

	w.triggerWebhookNotification(ctx, webhookEvent)
}

// triggerWebhookTokenIndexingNotification triggers a webhook notification workflow for a token indexing event
// eventType is the type of event (e.g., "token.indexing.queryable", "token.indexing.viewable", "token.indexing.provenance_completed")
// address is the address that triggered the indexing operation
// If nil, the indexing operation was not triggered by a specific address
func (w *workerCore) triggerWebhookTokenIndexingNotification(ctx workflow.Context, tokenCID domain.TokenCID, eventType string, address *string) {
	// Parse token CID to get components
	chain, standard, contract, tokenNumber := tokenCID.Parse()

	// Create webhook event with ULID for unique, time-sortable event ID
	var eventID string
	err := workflow.SideEffect(ctx, func(ctx workflow.Context) interface{} {
		return ulid.MustNewDefault(time.Now()).String()
	}).Get(&eventID)
	if err != nil {
		logger.ErrorWf(ctx, err, zap.String("tokenCID", tokenCID.String()), zap.String("eventType", eventType))
		return
	}

	webhookEvent := webhook.WebhookEvent{
		EventID:   eventID,
		EventType: eventType,
		Timestamp: workflow.Now(ctx),
		Data: webhook.IndexingEventData{
			EventData: webhook.EventData{
				TokenCID:    tokenCID.String(),
				Chain:       string(chain),
				Standard:    string(standard),
				Contract:    contract,
				TokenNumber: tokenNumber,
			},
			Address: address,
		},
	}

	w.triggerWebhookNotification(ctx, webhookEvent)
}

// triggerWebhookNotification triggers a webhook notification workflow (fire-and-forget)
func (w *workerCore) triggerWebhookNotification(ctx workflow.Context, event webhook.WebhookEvent) {
	// Configure child workflow options (fire-and-forget)
	childWorkflowOptions := workflow.ChildWorkflowOptions{
		WorkflowID:            fmt.Sprintf("webhook-notify-%s-%s", event.EventType, event.EventID),
		WorkflowRunTimeout:    30 * time.Minute,
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		ParentClosePolicy:     enums.PARENT_CLOSE_POLICY_ABANDON, // Don't wait for completion
	}
	childCtx := workflow.WithChildOptions(ctx, childWorkflowOptions)

	// Start notification workflow (don't wait for result)
	childWorkflow := workflow.ExecuteChildWorkflow(childCtx, w.NotifyWebhookClients, event)

	// Only verify it started successfully
	var childExecution workflow.Execution
	if err := childWorkflow.GetChildWorkflowExecution().Get(ctx, &childExecution); err != nil {
		logger.WarnWf(ctx, "Failed to start webhook notification workflow",
			zap.String("eventType", event.EventType),
			zap.String("eventID", event.EventID),
			zap.Error(err))
	}

	logger.InfoWf(ctx, "Webhook notification workflow started",
		zap.String("eventType", event.EventType),
		zap.String("workflowID", childExecution.ID))
}

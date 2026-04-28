package workflows

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/oklog/ulid/v2"
	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/providers/jobs"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
	"github.com/feral-file/ff-indexer-v2/internal/webhook"
)

// NotifyWebhookClients is the orchestration workflow that:
// 1. Queries active webhook clients matching the event type
// 2. Triggers a delivery workflow for each client (fire-and-forget)
func (w *coreWorkflows) NotifyWebhookClients(ctx context.Context, event webhook.WebhookEvent) error {
	logger.InfoCtx(ctx, "Starting webhook notification orchestration",
		zap.String("eventID", event.EventID),
		zap.String("eventType", event.EventType))

	// Get active webhook clients matching the event type
	clients, err := w.executor.GetActiveWebhookClientsByEventType(ctx, event.EventType)
	if err != nil {
		return err
	}

	if len(clients) == 0 {
		logger.InfoCtx(ctx, "No active webhook clients found for event type",
			zap.String("eventType", event.EventType))
		return nil
	}

	logger.InfoCtx(ctx, "Found active webhook clients",
		zap.Int("count", len(clients)),
		zap.String("eventType", event.EventType))

	// Trigger delivery job for each client (fire-and-forget)
	for _, c := range clients {
		uk := fmt.Sprintf("webhook-deliver-%s-%s", c.ClientID, event.EventID)
		_, _, err := w.jobQueue.Enqueue(ctx, jobs.EnqueueOptions{
			Queue:     w.config.TokenTaskQueue,
			Kind:      "DeliverWebhook",
			Args:      []any{c.ClientID, event},
			UniqueKey: &uk,
		})
		if err != nil {
			logger.WarnCtx(ctx, "Failed to enqueue webhook delivery",
				zap.String("clientID", c.ClientID),
				zap.String("eventID", event.EventID),
				zap.Error(err))
			continue
		}

		logger.InfoCtx(ctx, "Webhook delivery job enqueued",
			zap.String("clientID", c.ClientID),
		)
	}

	logger.InfoCtx(ctx, "Webhook notification orchestration completed",
		zap.Int("clientsNotified", len(clients)))

	return nil
}

// DeliverWebhook handles webhook delivery to a single client
func (w *coreWorkflows) DeliverWebhook(ctx context.Context, clientID string, event webhook.WebhookEvent) error {
	logger.InfoCtx(ctx, "Starting webhook delivery",
		zap.String("clientID", clientID),
		zap.String("eventID", event.EventID),
		zap.String("eventType", event.EventType))

	// Get client details
	client, err := w.executor.GetWebhookClientByID(ctx, clientID)
	if err != nil {
		return err
	}
	if client == nil {
		logger.InfoCtx(ctx, "Client not found, skipping delivery",
			zap.String("clientID", clientID))
		return nil
	}

	if !client.IsActive {
		logger.InfoCtx(ctx, "Client is not active, skipping delivery",
			zap.String("clientID", clientID))
		return nil
	}

	// Create delivery record
	workflowID := "job:local"
	workflowRunID := ""
	if id, ok := jobs.JobIDFromContext(ctx); ok {
		workflowID = "job:" + strconv.FormatInt(id, 10)
	}
	delivery := &schema.WebhookDelivery{
		ClientID:      client.ClientID,
		EventID:       event.EventID,
		EventType:     event.EventType,
		WorkflowID:    workflowID,
		WorkflowRunID: workflowRunID,
		// Payload will be marshaled by the store when creating the record
	}

	deliveryID, err := w.executor.CreateWebhookDeliveryRecord(ctx, delivery, event)
	if err != nil {
		return err
	}

	logger.InfoCtx(ctx, "Webhook delivery record created",
		zap.Uint64("deliveryID", deliveryID))

	// Execute delivery
	deliveryResult, err := w.executor.DeliverWebhookHTTP(ctx, client, event, deliveryID)
	if err != nil {
		return err
	}

	logger.InfoCtx(ctx, "Webhook delivered successfully",
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
func (w *coreWorkflows) triggerWebhookTokenOwnershipNotification(ctx context.Context, tokenCID domain.TokenCID, eventType string, from *string, to *string, quantity string) {
	// Parse token CID to get components
	chain, standard, contract, tokenNumber := tokenCID.Parse()

	// Create webhook event with ULID for unique, time-sortable event ID
	eventID := ulid.MustNewDefault(time.Now()).String()

	webhookEvent := webhook.WebhookEvent{
		EventID:   eventID,
		EventType: eventType,
		Timestamp: time.Now().UTC(),
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
func (w *coreWorkflows) triggerWebhookTokenIndexingNotification(ctx context.Context, tokenCID domain.TokenCID, eventType string, address *string) {
	// Parse token CID to get components
	chain, standard, contract, tokenNumber := tokenCID.Parse()

	// Create webhook event with ULID for unique, time-sortable event ID
	eventID := ulid.MustNewDefault(time.Now()).String()

	webhookEvent := webhook.WebhookEvent{
		EventID:   eventID,
		EventType: eventType,
		Timestamp: time.Now().UTC(),
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
func (w *coreWorkflows) triggerWebhookNotification(ctx context.Context, event webhook.WebhookEvent) {
	uk := "webhook-notify-" + event.EventID
	_, _, err := w.jobQueue.Enqueue(ctx, jobs.EnqueueOptions{
		Queue:     w.config.TokenTaskQueue,
		Kind:      "NotifyWebhookClients",
		Args:      []any{event},
		UniqueKey: &uk,
	})
	if err != nil {
		logger.WarnCtx(ctx, "Failed to start webhook notification workflow",
			zap.String("eventType", event.EventType),
			zap.String("eventID", event.EventID),
			zap.Error(err))
		return
	}

	logger.InfoCtx(ctx, "Webhook notification job enqueued",
		zap.String("eventType", event.EventType),
		zap.String("eventID", event.EventID),
	)
}

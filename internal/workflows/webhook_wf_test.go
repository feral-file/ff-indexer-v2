// Package workflows_test contains plain Go tests for webhook workflows (v1: no Temporal testsuite).
//
// Temporal-only cases (e.g. ErrMockStartChildWorkflowFailed) are not applicable; v1 DeliverWebhook
// performs a single HTTP delivery attempt (no activity retry loop).
package workflows_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"gorm.io/datatypes"

	"github.com/feral-file/ff-indexer-v2/internal/providers/jobs"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
	"github.com/feral-file/ff-indexer-v2/internal/webhook"
)

func newWebhookWf(t *testing.T) *coreWfDeps {
	t.Helper()
	return newCoreWfDeps(t, defaultCompactCoreWfConfig(), nil)
}

// webhookEventMatcher returns a gomock Matcher for webhook event payload shape.
func webhookEventMatcher(expected webhook.WebhookEvent) gomock.Matcher {
	return gomock.Cond(func(actual any) bool {
		ev, ok := actual.(webhook.WebhookEvent)
		if !ok {
			return false
		}
		if ev.EventID != expected.EventID ||
			ev.EventType != expected.EventType ||
			!ev.Timestamp.Equal(expected.Timestamp) {
			return false
		}
		expectedData, expectedOK := expected.Data.(webhook.EventData)
		if !expectedOK {
			return reflect.DeepEqual(ev.Data, expected.Data)
		}
		if actualMap, isMap := ev.Data.(map[string]interface{}); isMap {
			return actualMap["token_cid"] == expectedData.TokenCID &&
				actualMap["chain"] == expectedData.Chain &&
				actualMap["standard"] == expectedData.Standard &&
				actualMap["contract"] == expectedData.Contract &&
				actualMap["token_number"] == expectedData.TokenNumber
		}
		if actualData, isStruct := ev.Data.(webhook.EventData); isStruct {
			return actualData == expectedData
		}
		return false
	})
}

// ====================================================================================
// NotifyWebhookClients Tests
// ====================================================================================

func TestNotifyWebhookClients_NoClients(t *testing.T) {
	t.Parallel()
	d := newWebhookWf(t)
	defer d.Ctrl.Finish()
	ctx, exec, _, wf := d.Ctx, d.Exec, d.MockJQ, d.Wf
	_ = d.BlMock

	event := webhook.WebhookEvent{
		EventID:   "01JG8XAMPLE1234567890123456",
		EventType: webhook.EventTypeTokenIndexingQueryable,
		Timestamp: time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC),
		Data: webhook.EventData{
			TokenCID:    "eip155:1:erc721:0xABC:1",
			Chain:       "eip155:1",
			Standard:    "erc721",
			Contract:    "0xABC",
			TokenNumber: "1",
		},
	}

	exec.EXPECT().GetActiveWebhookClientsByEventType(gomock.Any(), event.EventType).Return([]*schema.WebhookClient{}, nil)

	err := wf.NotifyWebhookClients(ctx, event)
	require.NoError(t, err)
}

func TestNotifyWebhookClients_GetClientsError(t *testing.T) {
	t.Parallel()
	d := newWebhookWf(t)
	defer d.Ctrl.Finish()
	ctx, exec, _, wf := d.Ctx, d.Exec, d.MockJQ, d.Wf
	_ = d.BlMock

	event := webhook.WebhookEvent{
		EventID:   "01JG8XAMPLE1234567890123456",
		EventType: webhook.EventTypeTokenIndexingQueryable,
		Timestamp: time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC),
		Data: webhook.EventData{
			TokenCID:    "eip155:1:erc721:0xABC:1",
			Chain:       "eip155:1",
			Standard:    "erc721",
			Contract:    "0xABC",
			TokenNumber: "1",
		},
	}

	exec.EXPECT().GetActiveWebhookClientsByEventType(gomock.Any(), event.EventType).Return(nil, errors.New("database error"))

	err := wf.NotifyWebhookClients(ctx, event)
	require.Error(t, err)
}

func TestNotifyWebhookClients_SingleClient(t *testing.T) {
	t.Parallel()
	d := newWebhookWf(t)
	defer d.Ctrl.Finish()
	ctx, exec, jq, wf := d.Ctx, d.Exec, d.MockJQ, d.Wf
	_ = d.BlMock

	event := webhook.WebhookEvent{
		EventID:   "01JG8XAMPLE1234567890123456",
		EventType: webhook.EventTypeTokenIndexingQueryable,
		Timestamp: time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC),
		Data: webhook.EventData{
			TokenCID:    "eip155:1:erc721:0xABC:1",
			Chain:       "eip155:1",
			Standard:    "erc721",
			Contract:    "0xABC",
			TokenNumber: "1",
		},
	}

	eventFilters, _ := json.Marshal([]string{"*"})
	clients := []*schema.WebhookClient{
		{
			ClientID:         "client-123",
			WebhookURL:       "https://webhook.example.com/endpoint",
			WebhookSecret:    "736563726574313233",
			EventFilters:     datatypes.JSON(eventFilters),
			IsActive:         true,
			RetryMaxAttempts: 5,
		},
	}

	exec.EXPECT().GetActiveWebhookClientsByEventType(gomock.Any(), event.EventType).Return(clients, nil)
	jq.EXPECT().Enqueue(gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, opts jobs.EnqueueOptions) {
			require.Equal(t, "DeliverWebhook", opts.Kind)
			require.Len(t, opts.Args, 2)
			require.Equal(t, "client-123", opts.Args[0])
		}).
		Return(nil, true, nil).Times(1)

	err := wf.NotifyWebhookClients(ctx, event)
	require.NoError(t, err)
}

func TestNotifyWebhookClients_MultipleClients(t *testing.T) {
	t.Parallel()
	d := newWebhookWf(t)
	defer d.Ctrl.Finish()
	ctx, exec, jq, wf := d.Ctx, d.Exec, d.MockJQ, d.Wf
	_ = d.BlMock

	event := webhook.WebhookEvent{
		EventID:   "01JG8XAMPLE1234567890123456",
		EventType: webhook.EventTypeTokenIndexingViewable,
		Timestamp: time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC),
		Data: webhook.EventData{
			TokenCID:    "eip155:1:erc721:0xABC:1",
			Chain:       "eip155:1",
			Standard:    "erc721",
			Contract:    "0xABC",
			TokenNumber: "1",
		},
	}

	eventFilters1, _ := json.Marshal([]string{"*"})
	eventFilters2, _ := json.Marshal([]string{"token.indexing.viewable"})
	clients := []*schema.WebhookClient{
		{
			ClientID:         "client-123",
			WebhookURL:       "https://webhook1.example.com/endpoint",
			WebhookSecret:    "736563726574313233",
			EventFilters:     datatypes.JSON(eventFilters1),
			IsActive:         true,
			RetryMaxAttempts: 5,
		},
		{
			ClientID:         "client-456",
			WebhookURL:       "https://webhook2.example.com/endpoint",
			WebhookSecret:    "736563726574343536",
			EventFilters:     datatypes.JSON(eventFilters2),
			IsActive:         true,
			RetryMaxAttempts: 3,
		},
	}

	exec.EXPECT().GetActiveWebhookClientsByEventType(gomock.Any(), event.EventType).Return(clients, nil)
	jq.EXPECT().Enqueue(gomock.Any(), gomock.Any()).
		Return(nil, true, nil).Times(2)

	err := wf.NotifyWebhookClients(ctx, event)
	require.NoError(t, err)
}

// ====================================================================================
// DeliverWebhook Tests
// ====================================================================================

func TestDeliverWebhook_Success(t *testing.T) {
	t.Parallel()
	d := newWebhookWf(t)
	defer d.Ctrl.Finish()
	ctx, exec, _, wf := d.Ctx, d.Exec, d.MockJQ, d.Wf
	_ = d.BlMock

	clientID := "client-123"
	event := webhook.WebhookEvent{
		EventID:   "01JG8XAMPLE1234567890123456",
		EventType: webhook.EventTypeTokenIndexingQueryable,
		Timestamp: time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC),
		Data: webhook.EventData{
			TokenCID:    "eip155:1:erc721:0xABC:1",
			Chain:       "eip155:1",
			Standard:    "erc721",
			Contract:    "0xABC",
			TokenNumber: "1",
		},
	}

	eventFilters, _ := json.Marshal([]string{"*"})
	client := &schema.WebhookClient{
		ClientID:         clientID,
		WebhookURL:       "https://webhook.example.com/endpoint",
		WebhookSecret:    "736563726574313233",
		EventFilters:     datatypes.JSON(eventFilters),
		IsActive:         true,
		RetryMaxAttempts: 5,
	}

	exec.EXPECT().GetWebhookClientByID(gomock.Any(), clientID).Return(client, nil)
	exec.EXPECT().CreateWebhookDeliveryRecord(gomock.Any(), gomock.Any(), webhookEventMatcher(event)).Return(uint64(1), nil)
	exec.EXPECT().DeliverWebhookHTTP(gomock.Any(), client, webhookEventMatcher(event), uint64(1)).
		Return(webhook.DeliveryResult{Success: true, StatusCode: 200, Body: `{"status":"received"}`}, nil)

	err := wf.DeliverWebhook(ctx, clientID, event)
	require.NoError(t, err)
}

func TestDeliverWebhook_ClientNotFound(t *testing.T) {
	t.Parallel()
	d := newWebhookWf(t)
	defer d.Ctrl.Finish()
	ctx, exec, _, wf := d.Ctx, d.Exec, d.MockJQ, d.Wf
	_ = d.BlMock

	clientID := "non-existent-client"
	event := webhook.WebhookEvent{
		EventID:   "01JG8XAMPLE1234567890123456",
		EventType: webhook.EventTypeTokenIndexingQueryable,
		Timestamp: time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC),
		Data: webhook.EventData{
			TokenCID:    "eip155:1:erc721:0xABC:1",
			Chain:       "eip155:1",
			Standard:    "erc721",
			Contract:    "0xABC",
			TokenNumber: "1",
		},
	}

	exec.EXPECT().GetWebhookClientByID(gomock.Any(), clientID).Return(nil, nil)

	err := wf.DeliverWebhook(ctx, clientID, event)
	require.NoError(t, err)
}

func TestDeliverWebhook_ClientNotActive(t *testing.T) {
	t.Parallel()
	d := newWebhookWf(t)
	defer d.Ctrl.Finish()
	ctx, exec, _, wf := d.Ctx, d.Exec, d.MockJQ, d.Wf
	_ = d.BlMock

	clientID := "client-123"
	event := webhook.WebhookEvent{
		EventID:   "01JG8XAMPLE1234567890123456",
		EventType: webhook.EventTypeTokenIndexingQueryable,
		Timestamp: time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC),
		Data: webhook.EventData{
			TokenCID:    "eip155:1:erc721:0xABC:1",
			Chain:       "eip155:1",
			Standard:    "erc721",
			Contract:    "0xABC",
			TokenNumber: "1",
		},
	}

	eventFilters, _ := json.Marshal([]string{"*"})
	client := &schema.WebhookClient{
		ClientID:         clientID,
		WebhookURL:       "https://webhook.example.com/endpoint",
		WebhookSecret:    "736563726574313233",
		EventFilters:     datatypes.JSON(eventFilters),
		IsActive:         false,
		RetryMaxAttempts: 5,
	}

	exec.EXPECT().GetWebhookClientByID(gomock.Any(), clientID).Return(client, nil)

	err := wf.DeliverWebhook(ctx, clientID, event)
	require.NoError(t, err)
}

func TestDeliverWebhook_GetClientError(t *testing.T) {
	t.Parallel()
	d := newWebhookWf(t)
	defer d.Ctrl.Finish()
	ctx, exec, _, wf := d.Ctx, d.Exec, d.MockJQ, d.Wf
	_ = d.BlMock

	clientID := "client-123"
	event := webhook.WebhookEvent{
		EventID:   "01JG8XAMPLE1234567890123456",
		EventType: webhook.EventTypeTokenIndexingQueryable,
		Timestamp: time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC),
		Data: webhook.EventData{
			TokenCID:    "eip155:1:erc721:0xABC:1",
			Chain:       "eip155:1",
			Standard:    "erc721",
			Contract:    "0xABC",
			TokenNumber: "1",
		},
	}

	exec.EXPECT().GetWebhookClientByID(gomock.Any(), clientID).Return(nil, errors.New("database error"))

	err := wf.DeliverWebhook(ctx, clientID, event)
	require.Error(t, err)
}

func TestDeliverWebhook_CreateDeliveryRecordError(t *testing.T) {
	t.Parallel()
	d := newWebhookWf(t)
	defer d.Ctrl.Finish()
	ctx, exec, _, wf := d.Ctx, d.Exec, d.MockJQ, d.Wf
	_ = d.BlMock

	clientID := "client-123"
	event := webhook.WebhookEvent{
		EventID:   "01JG8XAMPLE1234567890123456",
		EventType: webhook.EventTypeTokenIndexingQueryable,
		Timestamp: time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC),
		Data: webhook.EventData{
			TokenCID:    "eip155:1:erc721:0xABC:1",
			Chain:       "eip155:1",
			Standard:    "erc721",
			Contract:    "0xABC",
			TokenNumber: "1",
		},
	}

	eventFilters, _ := json.Marshal([]string{"*"})
	client := &schema.WebhookClient{
		ClientID:         clientID,
		WebhookURL:       "https://webhook.example.com/endpoint",
		WebhookSecret:    "736563726574313233",
		EventFilters:     datatypes.JSON(eventFilters),
		IsActive:         true,
		RetryMaxAttempts: 5,
	}

	exec.EXPECT().GetWebhookClientByID(gomock.Any(), clientID).Return(client, nil)
	exec.EXPECT().CreateWebhookDeliveryRecord(gomock.Any(), gomock.Any(), gomock.Any()).Return(uint64(0), errors.New("database error")).Times(1)

	err := wf.DeliverWebhook(ctx, clientID, event)
	require.Error(t, err)
}

func TestDeliverWebhook_DeliveryFailed(t *testing.T) {
	t.Parallel()
	d := newWebhookWf(t)
	defer d.Ctrl.Finish()
	ctx, exec, _, wf := d.Ctx, d.Exec, d.MockJQ, d.Wf
	_ = d.BlMock

	clientID := "client-123"
	event := webhook.WebhookEvent{
		EventID:   "01JG8XAMPLE1234567890123456",
		EventType: webhook.EventTypeTokenIndexingQueryable,
		Timestamp: time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC),
		Data: webhook.EventData{
			TokenCID:    "eip155:1:erc721:0xABC:1",
			Chain:       "eip155:1",
			Standard:    "erc721",
			Contract:    "0xABC",
			TokenNumber: "1",
		},
	}

	eventFilters, _ := json.Marshal([]string{"*"})
	client := &schema.WebhookClient{
		ClientID:         clientID,
		WebhookURL:       "https://webhook.example.com/endpoint",
		WebhookSecret:    "736563726574313233",
		EventFilters:     datatypes.JSON(eventFilters),
		IsActive:         true,
		RetryMaxAttempts: 3,
	}

	exec.EXPECT().GetWebhookClientByID(gomock.Any(), clientID).Return(client, nil)
	exec.EXPECT().CreateWebhookDeliveryRecord(gomock.Any(), gomock.Any(), gomock.Any()).Return(uint64(1), nil)
	exec.EXPECT().DeliverWebhookHTTP(gomock.Any(), client, gomock.Any(), uint64(1)).
		Return(webhook.DeliveryResult{Success: false, StatusCode: 500, Body: `{"error":"internal server error"}`}, errors.New("HTTP 500")).
		Times(1)

	err := wf.DeliverWebhook(ctx, clientID, event)
	require.Error(t, err)
}

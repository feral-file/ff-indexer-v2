package workflows_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
	"gorm.io/datatypes"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
	"github.com/feral-file/ff-indexer-v2/internal/webhook"
	"github.com/feral-file/ff-indexer-v2/internal/workflows"
)

// WebhookWorkflowTestSuite is the test suite for webhook workflow tests
type WebhookWorkflowTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env        *testsuite.TestWorkflowEnvironment
	ctrl       *gomock.Controller
	executor   *mocks.MockCoreExecutor
	blacklist  *mocks.MockBlacklistRegistry
	workerCore workflows.WorkerCore
}

// webhookEventMatcher returns a function that matches webhook events
// accounting for the fact that Data can be either a struct or map[string]interface{}
// after JSON serialization/deserialization
func webhookEventMatcher(expected webhook.WebhookEvent) func(webhook.WebhookEvent) bool {
	return func(actual webhook.WebhookEvent) bool {
		// Compare non-Data fields
		if actual.EventID != expected.EventID ||
			actual.EventType != expected.EventType ||
			!actual.Timestamp.Equal(expected.Timestamp) {
			return false
		}

		// Handle Data field - can be struct or map
		expectedData, expectedOK := expected.Data.(webhook.EventData)
		if !expectedOK {
			// If expected is not EventData, do regular comparison
			return reflect.DeepEqual(actual.Data, expected.Data)
		}

		// Check if actual.Data is a map (after deserialization)
		actualMap, isMap := actual.Data.(map[string]interface{})
		if isMap {
			// Compare map fields with struct fields
			return actualMap["token_cid"] == expectedData.TokenCID &&
				actualMap["chain"] == expectedData.Chain &&
				actualMap["standard"] == expectedData.Standard &&
				actualMap["contract"] == expectedData.Contract &&
				actualMap["token_number"] == expectedData.TokenNumber
		}

		// Check if actual.Data is EventData struct
		actualData, isStruct := actual.Data.(webhook.EventData)
		if isStruct {
			return actualData == expectedData
		}

		return false
	}
}

// SetupTest is called before each test
func (s *WebhookWorkflowTestSuite) SetupTest() {
	// Initialize logger for tests
	_ = logger.Initialize(logger.Config{
		Debug: true,
	})

	s.env = s.NewTestWorkflowEnvironment()
	s.ctrl = gomock.NewController(s.T())
	s.executor = mocks.NewMockCoreExecutor(s.ctrl)
	s.blacklist = mocks.NewMockBlacklistRegistry(s.ctrl)
	s.workerCore = workflows.NewWorkerCore(s.executor, workflows.WorkerCoreConfig{
		TezosChainID:                 domain.ChainTezosMainnet,
		EthereumChainID:              domain.ChainEthereumMainnet,
		EthereumTokenSweepStartBlock: 0,
		TezosTokenSweepStartBlock:    0,
		MediaTaskQueue:               "media-task-queue",
	}, s.blacklist)
}

// TearDownTest is called after each test
func (s *WebhookWorkflowTestSuite) TearDownTest() {
	s.env.AssertExpectations(s.T())
	s.ctrl.Finish()
}

// TestWebhookWorkflowTestSuite runs the test suite
func TestWebhookWorkflowTestSuite(t *testing.T) {
	suite.Run(t, new(WebhookWorkflowTestSuite))
}

// ====================================================================================
// NotifyWebhookClients Tests
// ====================================================================================

func (s *WebhookWorkflowTestSuite) TestNotifyWebhookClients_NoClients() {
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

	// Mock GetActiveWebhookClientsByEventType activity - no clients
	s.env.OnActivity(s.executor.GetActiveWebhookClientsByEventType, mock.Anything, event.EventType).
		Return([]*schema.WebhookClient{}, nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.NotifyWebhookClients, event)

	// Verify workflow completed successfully (even with no clients)
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *WebhookWorkflowTestSuite) TestNotifyWebhookClients_GetClientsError() {
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

	// Mock GetActiveWebhookClientsByEventType activity - database error
	s.env.OnActivity(s.executor.GetActiveWebhookClientsByEventType, mock.Anything, event.EventType).
		Return(nil, errors.New("database error"))

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.NotifyWebhookClients, event)

	// Verify workflow failed
	s.True(s.env.IsWorkflowCompleted())
	s.Error(s.env.GetWorkflowError())
}

func (s *WebhookWorkflowTestSuite) TestNotifyWebhookClients_SingleClient() {
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
			WebhookSecret:    "secret123",
			EventFilters:     datatypes.JSON(eventFilters),
			IsActive:         true,
			RetryMaxAttempts: 5,
		},
	}

	// Mock GetActiveWebhookClientsByEventType activity
	s.env.OnActivity(s.executor.GetActiveWebhookClientsByEventType, mock.Anything, event.EventType).
		Return(clients, nil)

	// Mock DeliverWebhook child workflow
	s.env.OnWorkflow(s.workerCore.DeliverWebhook, mock.Anything, "client-123", mock.MatchedBy(webhookEventMatcher(event))).Return(nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.NotifyWebhookClients, event)

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *WebhookWorkflowTestSuite) TestNotifyWebhookClients_MultipleClients() {
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
			WebhookSecret:    "secret123",
			EventFilters:     datatypes.JSON(eventFilters1),
			IsActive:         true,
			RetryMaxAttempts: 5,
		},
		{
			ClientID:         "client-456",
			WebhookURL:       "https://webhook2.example.com/endpoint",
			WebhookSecret:    "secret456",
			EventFilters:     datatypes.JSON(eventFilters2),
			IsActive:         true,
			RetryMaxAttempts: 3,
		},
	}

	// Mock GetActiveWebhookClientsByEventType activity
	s.env.OnActivity(s.executor.GetActiveWebhookClientsByEventType, mock.Anything, event.EventType).
		Return(clients, nil)

	// Mock DeliverWebhook child workflows for both clients
	s.env.OnWorkflow(s.workerCore.DeliverWebhook, mock.Anything, "client-123", mock.MatchedBy(webhookEventMatcher(event))).Return(nil)
	s.env.OnWorkflow(s.workerCore.DeliverWebhook, mock.Anything, "client-456", mock.MatchedBy(webhookEventMatcher(event))).Return(nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.NotifyWebhookClients, event)

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

// ====================================================================================
// DeliverWebhook Tests
// ====================================================================================

func (s *WebhookWorkflowTestSuite) TestDeliverWebhook_Success() {
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
		WebhookSecret:    "secret123",
		EventFilters:     datatypes.JSON(eventFilters),
		IsActive:         true,
		RetryMaxAttempts: 5,
	}

	// Mock GetWebhookClientByID activity
	s.env.OnActivity(s.executor.GetWebhookClientByID, mock.Anything, clientID).
		Return(client, nil)

	// Mock CreateWebhookDeliveryRecord activity
	s.env.OnActivity(s.executor.CreateWebhookDeliveryRecord, mock.Anything, mock.AnythingOfType("*schema.WebhookDelivery"), mock.MatchedBy(webhookEventMatcher(event))).
		Return(uint64(1), nil)

	// Mock DeliverWebhookHTTP activity - successful delivery
	s.env.OnActivity(s.executor.DeliverWebhookHTTP, mock.Anything, client, mock.MatchedBy(webhookEventMatcher(event)), uint64(1)).
		Return(webhook.DeliveryResult{Success: true, StatusCode: 200, Body: `{"status":"received"}`}, nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.DeliverWebhook, clientID, event)

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *WebhookWorkflowTestSuite) TestDeliverWebhook_ClientNotFound() {
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

	// Mock GetWebhookClientByID activity - client not found
	s.env.OnActivity(s.executor.GetWebhookClientByID, mock.Anything, clientID).
		Return(nil, nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.DeliverWebhook, clientID, event)

	// Verify workflow completed successfully (even with no client)
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *WebhookWorkflowTestSuite) TestDeliverWebhook_ClientNotActive() {
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
		WebhookSecret:    "secret123",
		EventFilters:     datatypes.JSON(eventFilters),
		IsActive:         false,
		RetryMaxAttempts: 5,
	}

	// Mock GetWebhookClientByID activity
	s.env.OnActivity(s.executor.GetWebhookClientByID, mock.Anything, clientID).
		Return(client, nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.DeliverWebhook, clientID, event)

	// Verify workflow completed successfully (even with inactive client)
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *WebhookWorkflowTestSuite) TestDeliverWebhook_GetClientError() {
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

	// Mock GetWebhookClientByID activity - database error
	s.env.OnActivity(s.executor.GetWebhookClientByID, mock.Anything, clientID).
		Return(nil, errors.New("database error"))

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.DeliverWebhook, clientID, event)

	// Verify workflow failed
	s.True(s.env.IsWorkflowCompleted())
	s.Error(s.env.GetWorkflowError())
}

func (s *WebhookWorkflowTestSuite) TestDeliverWebhook_CreateDeliveryRecordError() {
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
		WebhookSecret:    "secret123",
		EventFilters:     datatypes.JSON(eventFilters),
		IsActive:         true,
		RetryMaxAttempts: 5,
	}

	// Mock GetWebhookClientByID activity
	s.env.OnActivity(s.executor.GetWebhookClientByID, mock.Anything, clientID).
		Return(client, nil)

	// Mock CreateWebhookDeliveryRecord activity - database error
	s.env.OnActivity(s.executor.CreateWebhookDeliveryRecord, mock.Anything, mock.AnythingOfType("*schema.WebhookDelivery"), mock.MatchedBy(webhookEventMatcher(event))).
		Return(uint64(0), errors.New("database error"))

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.DeliverWebhook, clientID, event)

	// Verify workflow failed
	s.True(s.env.IsWorkflowCompleted())
	s.Error(s.env.GetWorkflowError())
}

func (s *WebhookWorkflowTestSuite) TestDeliverWebhook_DeliveryFailed() {
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
	maxAttempts := 3
	client := &schema.WebhookClient{
		ClientID:         clientID,
		WebhookURL:       "https://webhook.example.com/endpoint",
		WebhookSecret:    "secret123",
		EventFilters:     datatypes.JSON(eventFilters),
		IsActive:         true,
		RetryMaxAttempts: maxAttempts,
	}

	// Mock GetWebhookClientByID activity
	s.env.OnActivity(s.executor.GetWebhookClientByID, mock.Anything, clientID).
		Return(client, nil)

	// Mock CreateWebhookDeliveryRecord activity
	s.env.OnActivity(s.executor.CreateWebhookDeliveryRecord, mock.Anything, mock.AnythingOfType("*schema.WebhookDelivery"), mock.MatchedBy(webhookEventMatcher(event))).
		Return(uint64(1), nil)

	// Mock DeliverWebhookHTTP activity - delivery failed (will retry with Temporal's retry policy)
	var activityCallCount int
	s.env.OnActivity(s.executor.DeliverWebhookHTTP, mock.Anything, client, mock.MatchedBy(webhookEventMatcher(event)), uint64(1)).
		Return(func(ctx context.Context, client *schema.WebhookClient, event webhook.WebhookEvent, deliveryID uint64) (webhook.DeliveryResult, error) {
			activityCallCount++
			return webhook.DeliveryResult{Success: false, StatusCode: 500, Body: `{"error":"internal server error"}`}, errors.New("HTTP 500")
		}, nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.DeliverWebhook, clientID, event)

	// Verify workflow failed (after retries)
	s.True(s.env.IsWorkflowCompleted())
	s.Error(s.env.GetWorkflowError())
	s.Equal(maxAttempts, activityCallCount, "Activity should be attempted the expected number of times")
}

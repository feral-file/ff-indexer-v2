package workflows_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/metadata"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
	"github.com/feral-file/ff-indexer-v2/internal/webhook"
	"github.com/feral-file/ff-indexer-v2/internal/workflows"
	workflowsmedia "github.com/feral-file/ff-indexer-v2/internal/workflows/media"
)

// IndexMetadataWorkflowTestSuite is the test suite for metadata workflow tests
type IndexMetadataWorkflowTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env              *testsuite.TestWorkflowEnvironment
	ctrl             *gomock.Controller
	executor         *mocks.MockCoreExecutor
	blacklist        *mocks.MockBlacklistRegistry
	temporalWorkflow *mocks.MockWorkflow
	workerCore       workflows.WorkerCore
	workerMedia      workflowsmedia.Worker
}

// SetupTest is called before each test
func (s *IndexMetadataWorkflowTestSuite) SetupTest() {
	// Initialize logger for tests
	_ = logger.Initialize(logger.Config{
		Debug: true,
	})

	s.env = s.NewTestWorkflowEnvironment()
	s.ctrl = gomock.NewController(s.T())
	s.executor = mocks.NewMockCoreExecutor(s.ctrl)
	s.blacklist = mocks.NewMockBlacklistRegistry(s.ctrl)
	s.temporalWorkflow = mocks.NewMockWorkflow(s.ctrl)
	s.workerCore = workflows.NewWorkerCore(s.executor, workflows.WorkerCoreConfig{
		TezosChainID:                 domain.ChainTezosMainnet,
		EthereumChainID:              domain.ChainEthereumMainnet,
		EthereumTokenSweepStartBlock: 0,
		TezosTokenSweepStartBlock:    0,
		MediaTaskQueue:               "media-task-queue",
	}, s.blacklist, s.temporalWorkflow)
	s.workerMedia = workflowsmedia.NewWorker(mocks.NewMockMediaExecutor(s.ctrl))
}

// TearDownTest is called after each test
func (s *IndexMetadataWorkflowTestSuite) TearDownTest() {
	s.env.AssertExpectations(s.T())
	s.ctrl.Finish()
}

// TestIndexMetadataWorkflowTestSuite runs the test suite
func TestIndexMetadataWorkflowTestSuite(t *testing.T) {
	suite.Run(t, new(IndexMetadataWorkflowTestSuite))
}

// ====================================================================================
// IndexMetadataUpdate Tests
// ====================================================================================

func (s *IndexMetadataWorkflowTestSuite) TestIndexMetadataUpdate_Success() {
	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeMetadataUpdate,
		TxHash:          "0xabcd",
		BlockNumber:     100,
	}
	tokenCID := event.TokenCID()

	// Mock CreateMetadataUpdate activity
	s.env.OnActivity(s.executor.CreateMetadataUpdate, mock.Anything, event).Return(nil)

	// Mock child workflow IndexTokenMetadata
	s.env.OnWorkflow(s.workerCore.IndexTokenMetadata, mock.Anything, tokenCID, (*string)(nil)).Return(nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexMetadataUpdate, event)

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *IndexMetadataWorkflowTestSuite) TestIndexMetadataUpdate_CreateMetadataUpdateError() {
	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeMetadataUpdate,
		TxHash:          "0xabcd",
		BlockNumber:     100,
	}
	expectedError := errors.New("database error")

	// Track retry attempts - MaximumAttempts: 2 (1 retry)
	var activityCallCount int
	s.env.OnActivity(s.executor.CreateMetadataUpdate, mock.Anything, event).Return(
		func(ctx context.Context, event *domain.BlockchainEvent) error {
			activityCallCount++
			return expectedError
		},
	)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexMetadataUpdate, event)

	// Verify workflow completed with error
	s.True(s.env.IsWorkflowCompleted())
	s.Error(s.env.GetWorkflowError())

	// Verify no retries (MaximumAttempts: 2)
	s.Equal(2, activityCallCount, "Activity should be attempted 2 times (initial + 1 retry)")
}

func (s *IndexMetadataWorkflowTestSuite) TestIndexMetadataUpdate_ChildWorkflowStartFailure() {
	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeMetadataUpdate,
		TxHash:          "0xabcd",
		BlockNumber:     100,
	}
	tokenCID := event.TokenCID()
	var childWorkflowCallCount int

	// Mock CreateMetadataUpdate activity
	s.env.OnActivity(s.executor.CreateMetadataUpdate, mock.Anything, event).Return(nil)

	// Mock child workflow to fail at start
	s.env.OnWorkflow(s.workerCore.IndexTokenMetadata, mock.Anything, tokenCID, (*string)(nil)).Return(
		func(ctx workflow.Context, tokenCID domain.TokenCID, address *string) error {
			childWorkflowCallCount++
			return testsuite.ErrMockStartChildWorkflowFailed
		},
	)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexMetadataUpdate, event)

	// Verify workflow completed with error (it returns the error)
	s.True(s.env.IsWorkflowCompleted())
	s.Error(s.env.GetWorkflowError())

	s.Equal(1, childWorkflowCallCount, "Child workflow should be attempted 1 time (initial + 0 retries)")
}

// ====================================================================================
// IndexTokenMetadata Tests
// ====================================================================================

func (s *IndexMetadataWorkflowTestSuite) TestIndexTokenMetadata_Success_WithoutEnhancement() {
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")
	normalizedMetadata := &metadata.NormalizedMetadata{
		Name:        "Test Token",
		Description: "Test Description",
		Image:       "https://example.com/image.jpg",
	}

	// Mock FetchTokenMetadata activity
	s.env.OnActivity(s.executor.FetchTokenMetadata, mock.Anything, tokenCID).Return(normalizedMetadata, nil)

	// Mock UpsertTokenMetadata activity
	s.env.OnActivity(s.executor.UpsertTokenMetadata, mock.Anything, tokenCID, normalizedMetadata).Return(nil)

	// Mock EnhanceTokenMetadata activity - returns nil (no enhancement)
	s.env.OnActivity(s.executor.EnhanceTokenMetadata, mock.Anything, tokenCID, normalizedMetadata).Return(nil, nil)

	// Mock CheckMediaURLsHealth activity - return healthy status
	s.env.OnActivity(s.executor.CheckMediaURLsHealth, mock.Anything, mock.MatchedBy(func(urls []string) bool {
		return len(urls) == 1 && urls[0] == normalizedMetadata.Image
	})).Return(map[string]schema.MediaHealthStatus{
		normalizedMetadata.Image: schema.MediaHealthStatusHealthy,
	}, nil)

	// Mock webhook notification workflow - should be triggered for token.indexing.viewable event
	s.env.OnWorkflow(s.workerCore.NotifyWebhookClients, mock.Anything, mock.MatchedBy(func(event interface{}) bool {
		// Verify it's a webhook event with correct event type
		if webhookEvent, ok := event.(webhook.WebhookEvent); ok {
			return webhookEvent.EventType == webhook.EventTypeTokenIndexingViewable
		}
		return false
	})).Return(nil)

	// Mock media indexing child workflow - match the actual workflow type
	s.env.OnWorkflow(s.workerMedia.IndexMultipleMediaWorkflow, mock.Anything, mock.MatchedBy(func(urls []string) bool {
		return len(urls) == 1 && urls[0] == "https://example.com/image.jpg"
	})).Return(nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenMetadata, tokenCID, (*string)(nil))

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *IndexMetadataWorkflowTestSuite) TestIndexTokenMetadata_Success_WithEnhancement() {
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")
	metadataImageURL := "https://example.com/image.jpg"
	normalizedMetadata := &metadata.NormalizedMetadata{
		Name:        "Test Token",
		Description: "Test Description",
		Image:       metadataImageURL,
	}
	enhancedImageURL := "https://example.com/enhanced-image.jpg"
	enhancedMetadata := &metadata.EnhancedMetadata{
		ImageURL: &enhancedImageURL,
	}

	// Mock FetchTokenMetadata activity
	s.env.OnActivity(s.executor.FetchTokenMetadata, mock.Anything, tokenCID).Return(normalizedMetadata, nil)

	// Mock UpsertTokenMetadata activity
	s.env.OnActivity(s.executor.UpsertTokenMetadata, mock.Anything, tokenCID, normalizedMetadata).Return(nil)

	// Mock EnhanceTokenMetadata activity - returns enhanced metadata
	s.env.OnActivity(s.executor.EnhanceTokenMetadata, mock.Anything, tokenCID, normalizedMetadata).Return(enhancedMetadata, nil)

	// Mock CheckMediaURLsHealth activity - return healthy status for both URLs
	s.env.OnActivity(s.executor.CheckMediaURLsHealth, mock.Anything, mock.MatchedBy(func(urls []string) bool {
		return len(urls) == 2
	})).Return(map[string]schema.MediaHealthStatus{
		normalizedMetadata.Image: schema.MediaHealthStatusHealthy,
		enhancedImageURL:         schema.MediaHealthStatusHealthy,
	}, nil)

	// Mock webhook notification workflow - should be triggered for token.indexing.viewable event
	s.env.OnWorkflow(s.workerCore.NotifyWebhookClients, mock.Anything, mock.MatchedBy(func(event interface{}) bool {
		if webhookEvent, ok := event.(webhook.WebhookEvent); ok {
			return webhookEvent.EventType == webhook.EventTypeTokenIndexingViewable
		}
		return false
	})).Return(nil)

	// Mock media indexing child workflow - match the actual workflow type
	s.env.OnWorkflow(s.workerMedia.IndexMultipleMediaWorkflow, mock.Anything, mock.MatchedBy(func(urls []string) bool {
		if len(urls) != 2 {
			return false
		}

		for _, url := range urls {
			if url != normalizedMetadata.Image && url != enhancedImageURL {
				return false
			}
		}

		return true
	})).Return(nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenMetadata, tokenCID, (*string)(nil))

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *IndexMetadataWorkflowTestSuite) TestIndexTokenMetadata_FetchMetadataError() {
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")
	expectedError := errors.New("failed to fetch metadata")

	// Track retry attempts - MaximumAttempts: 2 (1 retry)
	var activityCallCount int
	s.env.OnActivity(s.executor.FetchTokenMetadata, mock.Anything, tokenCID).Return(
		func(ctx context.Context, tokenCID domain.TokenCID) (*metadata.NormalizedMetadata, error) {
			activityCallCount++
			return nil, expectedError
		},
	)

	// Mock enhancement activity - returns nil (no enhancement)
	s.env.OnActivity(s.executor.EnhanceTokenMetadata, mock.Anything, tokenCID, mock.Anything).Return(nil, nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenMetadata, tokenCID, (*string)(nil))

	// Verify workflow completed without error
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())

	// Verify retries (MaximumAttempts: 2)
	s.Equal(2, activityCallCount, "Activity should be attempted 2 times (initial + 1 retry)")
}

func (s *IndexMetadataWorkflowTestSuite) TestIndexTokenMetadata_UpsertMetadataError() {
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")
	normalizedMetadata := &metadata.NormalizedMetadata{
		Name:        "Test Token",
		Description: "Test Description",
	}
	expectedError := errors.New("database error")

	// Mock FetchTokenMetadata activity
	s.env.OnActivity(s.executor.FetchTokenMetadata, mock.Anything, tokenCID).Return(normalizedMetadata, nil)

	// Mock UpsertTokenMetadata activity to fail
	s.env.OnActivity(s.executor.UpsertTokenMetadata, mock.Anything, tokenCID, normalizedMetadata).Return(expectedError)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenMetadata, tokenCID, (*string)(nil))

	// Verify workflow completed with error
	s.True(s.env.IsWorkflowCompleted())
	s.Error(s.env.GetWorkflowError())
}

func (s *IndexMetadataWorkflowTestSuite) TestIndexTokenMetadata_EnhancementError_NonFatal() {
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")
	normalizedMetadata := &metadata.NormalizedMetadata{
		Name:        "Test Token",
		Description: "Test Description",
		Image:       "https://example.com/image.jpg",
	}
	enhancementError := errors.New("enhancement failed")

	// Mock FetchTokenMetadata activity
	s.env.OnActivity(s.executor.FetchTokenMetadata, mock.Anything, tokenCID).Return(normalizedMetadata, nil)

	// Mock UpsertTokenMetadata activity
	s.env.OnActivity(s.executor.UpsertTokenMetadata, mock.Anything, tokenCID, normalizedMetadata).Return(nil)

	// Mock EnhanceTokenMetadata activity to fail - should not fail workflow
	s.env.OnActivity(s.executor.EnhanceTokenMetadata, mock.Anything, tokenCID, normalizedMetadata).Return(nil, enhancementError)

	// Mock CheckMediaURLsHealth activity - return healthy status for the image from normalized metadata
	s.env.OnActivity(s.executor.CheckMediaURLsHealth, mock.Anything, mock.MatchedBy(func(urls []string) bool {
		return len(urls) == 1 && urls[0] == normalizedMetadata.Image
	})).Return(map[string]schema.MediaHealthStatus{
		normalizedMetadata.Image: schema.MediaHealthStatusHealthy,
	}, nil)

	// Mock webhook notification workflow
	s.env.OnWorkflow(s.workerCore.NotifyWebhookClients, mock.Anything, mock.Anything).Return(nil)

	// Mock media indexing child workflow - match the actual workflow type
	s.env.OnWorkflow(s.workerMedia.IndexMultipleMediaWorkflow, mock.Anything, mock.Anything).Return(nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenMetadata, tokenCID, (*string)(nil))

	// Verify workflow completed successfully despite enhancement error (non-fatal)
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *IndexMetadataWorkflowTestSuite) TestIndexTokenMetadata_MediaWorkflowStartFailure_NonFatal() {
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")
	normalizedMetadata := &metadata.NormalizedMetadata{
		Name:        "Test Token",
		Description: "Test Description",
		Image:       "https://example.com/image.jpg",
	}

	// Mock FetchTokenMetadata activity
	s.env.OnActivity(s.executor.FetchTokenMetadata, mock.Anything, tokenCID).Return(normalizedMetadata, nil)

	// Mock UpsertTokenMetadata activity
	s.env.OnActivity(s.executor.UpsertTokenMetadata, mock.Anything, tokenCID, normalizedMetadata).Return(nil)

	// Mock EnhanceTokenMetadata activity
	s.env.OnActivity(s.executor.EnhanceTokenMetadata, mock.Anything, tokenCID, normalizedMetadata).Return(nil, nil)

	// Mock CheckMediaURLsHealth activity - return healthy status
	s.env.OnActivity(s.executor.CheckMediaURLsHealth, mock.Anything, mock.MatchedBy(func(urls []string) bool {
		return len(urls) == 1 && urls[0] == normalizedMetadata.Image
	})).Return(map[string]schema.MediaHealthStatus{
		normalizedMetadata.Image: schema.MediaHealthStatusHealthy,
	}, nil)

	// Mock webhook notification workflow
	s.env.OnWorkflow(s.workerCore.NotifyWebhookClients, mock.Anything, mock.Anything).Return(nil)

	// Mock media indexing child workflow to fail at start - should not fail parent workflow
	s.env.OnWorkflow(s.workerMedia.IndexMultipleMediaWorkflow, mock.Anything, mock.Anything).Return(testsuite.ErrMockStartChildWorkflowFailed)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenMetadata, tokenCID, (*string)(nil))

	// Verify workflow completed successfully despite media workflow start failure (non-fatal)
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *IndexMetadataWorkflowTestSuite) TestIndexTokenMetadata_MediaURLsUnhealthy_NoWebhook() {
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")
	normalizedMetadata := &metadata.NormalizedMetadata{
		Name:        "Test Token",
		Description: "Test Description",
		Image:       "https://example.com/broken-image.jpg",
	}

	// Mock FetchTokenMetadata activity
	s.env.OnActivity(s.executor.FetchTokenMetadata, mock.Anything, tokenCID).Return(normalizedMetadata, nil)

	// Mock UpsertTokenMetadata activity
	s.env.OnActivity(s.executor.UpsertTokenMetadata, mock.Anything, tokenCID, normalizedMetadata).Return(nil)

	// Mock EnhanceTokenMetadata activity - returns nil (no enhancement)
	s.env.OnActivity(s.executor.EnhanceTokenMetadata, mock.Anything, tokenCID, normalizedMetadata).Return(nil, nil)

	// Mock CheckMediaURLsHealth activity - return broken status for the image
	s.env.OnActivity(s.executor.CheckMediaURLsHealth, mock.Anything, mock.MatchedBy(func(urls []string) bool {
		return len(urls) == 1 && urls[0] == normalizedMetadata.Image
	})).Return(map[string]schema.MediaHealthStatus{
		normalizedMetadata.Image: schema.MediaHealthStatusBroken,
	}, nil)

	// No webhook should be triggered because media is not healthy

	// Mock media indexing child workflow - should still be triggered even if URL is broken
	s.env.OnWorkflow(s.workerMedia.IndexMultipleMediaWorkflow, mock.Anything, mock.MatchedBy(func(urls []string) bool {
		return len(urls) == 1 && urls[0] == normalizedMetadata.Image
	})).Return(nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenMetadata, tokenCID, (*string)(nil))

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

// ====================================================================================
// Media File Type Priority Tests (Animation vs Image)
// ====================================================================================

func (s *IndexMetadataWorkflowTestSuite) TestIndexTokenMetadata_AnimationHealthy_TriggersWebhook() {
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")
	animationURL := "https://example.com/animation.mp4"
	normalizedMetadata := &metadata.NormalizedMetadata{
		Name:        "Test Token",
		Description: "Test Description",
		Animation:   animationURL,
	}

	// Mock FetchTokenMetadata activity
	s.env.OnActivity(s.executor.FetchTokenMetadata, mock.Anything, tokenCID).Return(normalizedMetadata, nil)

	// Mock UpsertTokenMetadata activity
	s.env.OnActivity(s.executor.UpsertTokenMetadata, mock.Anything, tokenCID, normalizedMetadata).Return(nil)

	// Mock EnhanceTokenMetadata activity
	s.env.OnActivity(s.executor.EnhanceTokenMetadata, mock.Anything, tokenCID, normalizedMetadata).Return(nil, nil)

	// Mock CheckMediaURLsHealth activity - animation is healthy
	s.env.OnActivity(s.executor.CheckMediaURLsHealth, mock.Anything, mock.MatchedBy(func(urls []string) bool {
		return len(urls) == 1 && urls[0] == animationURL
	})).Return(map[string]schema.MediaHealthStatus{
		animationURL: schema.MediaHealthStatusHealthy,
	}, nil)

	// Mock webhook notification workflow - should be triggered because animation is healthy
	s.env.OnWorkflow(s.workerCore.NotifyWebhookClients, mock.Anything, mock.MatchedBy(func(event interface{}) bool {
		if webhookEvent, ok := event.(webhook.WebhookEvent); ok {
			return webhookEvent.EventType == webhook.EventTypeTokenIndexingViewable
		}
		return false
	})).Return(nil)

	// Mock media indexing child workflow
	s.env.OnWorkflow(s.workerMedia.IndexMultipleMediaWorkflow, mock.Anything, mock.MatchedBy(func(urls []string) bool {
		return len(urls) == 1 && urls[0] == animationURL
	})).Return(nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenMetadata, tokenCID, (*string)(nil))

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *IndexMetadataWorkflowTestSuite) TestIndexTokenMetadata_AnimationBroken_ImageHealthy_NoWebhook() {
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")
	imageURL := "https://example.com/image.jpg"
	animationURL := "https://example.com/animation.mp4"
	normalizedMetadata := &metadata.NormalizedMetadata{
		Name:        "Test Token",
		Description: "Test Description",
		Image:       imageURL,
		Animation:   animationURL,
	}

	// Mock FetchTokenMetadata activity
	s.env.OnActivity(s.executor.FetchTokenMetadata, mock.Anything, tokenCID).Return(normalizedMetadata, nil)

	// Mock UpsertTokenMetadata activity
	s.env.OnActivity(s.executor.UpsertTokenMetadata, mock.Anything, tokenCID, normalizedMetadata).Return(nil)

	// Mock EnhanceTokenMetadata activity
	s.env.OnActivity(s.executor.EnhanceTokenMetadata, mock.Anything, tokenCID, normalizedMetadata).Return(nil, nil)

	// Mock CheckMediaURLsHealth activity - animation is broken but image is healthy
	// Per the logic: if animation URL exists, only animation health matters
	s.env.OnActivity(s.executor.CheckMediaURLsHealth, mock.Anything, mock.MatchedBy(func(urls []string) bool {
		return len(urls) == 2
	})).Return(map[string]schema.MediaHealthStatus{
		animationURL: schema.MediaHealthStatusBroken,
		imageURL:     schema.MediaHealthStatusHealthy,
	}, nil)

	// No webhook should be triggered because animation takes priority and it's broken

	// Mock media indexing child workflow - should still be triggered
	s.env.OnWorkflow(s.workerMedia.IndexMultipleMediaWorkflow, mock.Anything, mock.MatchedBy(func(urls []string) bool {
		return len(urls) == 2
	})).Return(nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenMetadata, tokenCID, (*string)(nil))

	// Verify workflow completed successfully but no webhook triggered
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *IndexMetadataWorkflowTestSuite) TestIndexTokenMetadata_BothAnimationAndImageHealthy_TriggersWebhook() {
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")
	imageURL := "https://example.com/image.jpg"
	animationURL := "https://example.com/animation.mp4"
	normalizedMetadata := &metadata.NormalizedMetadata{
		Name:        "Test Token",
		Description: "Test Description",
		Image:       imageURL,
		Animation:   animationURL,
	}

	// Mock FetchTokenMetadata activity
	s.env.OnActivity(s.executor.FetchTokenMetadata, mock.Anything, tokenCID).Return(normalizedMetadata, nil)

	// Mock UpsertTokenMetadata activity
	s.env.OnActivity(s.executor.UpsertTokenMetadata, mock.Anything, tokenCID, normalizedMetadata).Return(nil)

	// Mock EnhanceTokenMetadata activity
	s.env.OnActivity(s.executor.EnhanceTokenMetadata, mock.Anything, tokenCID, normalizedMetadata).Return(nil, nil)

	// Mock CheckMediaURLsHealth activity - both are healthy
	s.env.OnActivity(s.executor.CheckMediaURLsHealth, mock.Anything, mock.MatchedBy(func(urls []string) bool {
		return len(urls) == 2
	})).Return(map[string]schema.MediaHealthStatus{
		animationURL: schema.MediaHealthStatusHealthy,
		imageURL:     schema.MediaHealthStatusHealthy,
	}, nil)

	// Mock webhook notification workflow - should be triggered because animation is healthy
	s.env.OnWorkflow(s.workerCore.NotifyWebhookClients, mock.Anything, mock.MatchedBy(func(event interface{}) bool {
		if webhookEvent, ok := event.(webhook.WebhookEvent); ok {
			return webhookEvent.EventType == webhook.EventTypeTokenIndexingViewable
		}
		return false
	})).Return(nil)

	// Mock media indexing child workflow
	s.env.OnWorkflow(s.workerMedia.IndexMultipleMediaWorkflow, mock.Anything, mock.MatchedBy(func(urls []string) bool {
		return len(urls) == 2
	})).Return(nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenMetadata, tokenCID, (*string)(nil))

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *IndexMetadataWorkflowTestSuite) TestIndexTokenMetadata_ImageHealthy_NoAnimation_TriggersWebhook() {
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")
	imageURL := "https://example.com/image.jpg"
	normalizedMetadata := &metadata.NormalizedMetadata{
		Name:        "Test Token",
		Description: "Test Description",
		Image:       imageURL,
		// No animation URL
	}

	// Mock FetchTokenMetadata activity
	s.env.OnActivity(s.executor.FetchTokenMetadata, mock.Anything, tokenCID).Return(normalizedMetadata, nil)

	// Mock UpsertTokenMetadata activity
	s.env.OnActivity(s.executor.UpsertTokenMetadata, mock.Anything, tokenCID, normalizedMetadata).Return(nil)

	// Mock EnhanceTokenMetadata activity
	s.env.OnActivity(s.executor.EnhanceTokenMetadata, mock.Anything, tokenCID, normalizedMetadata).Return(nil, nil)

	// Mock CheckMediaURLsHealth activity - image is healthy
	s.env.OnActivity(s.executor.CheckMediaURLsHealth, mock.Anything, mock.MatchedBy(func(urls []string) bool {
		return len(urls) == 1 && urls[0] == imageURL
	})).Return(map[string]schema.MediaHealthStatus{
		imageURL: schema.MediaHealthStatusHealthy,
	}, nil)

	// Mock webhook notification workflow - should be triggered because image is healthy and no animation exists
	s.env.OnWorkflow(s.workerCore.NotifyWebhookClients, mock.Anything, mock.MatchedBy(func(event interface{}) bool {
		if webhookEvent, ok := event.(webhook.WebhookEvent); ok {
			return webhookEvent.EventType == webhook.EventTypeTokenIndexingViewable
		}
		return false
	})).Return(nil)

	// Mock media indexing child workflow
	s.env.OnWorkflow(s.workerMedia.IndexMultipleMediaWorkflow, mock.Anything, mock.MatchedBy(func(urls []string) bool {
		return len(urls) == 1 && urls[0] == imageURL
	})).Return(nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenMetadata, tokenCID, (*string)(nil))

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *IndexMetadataWorkflowTestSuite) TestIndexTokenMetadata_BothBroken_NoWebhook() {
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")
	imageURL := "https://example.com/broken-image.jpg"
	animationURL := "https://example.com/broken-animation.mp4"
	normalizedMetadata := &metadata.NormalizedMetadata{
		Name:        "Test Token",
		Description: "Test Description",
		Image:       imageURL,
		Animation:   animationURL,
	}

	// Mock FetchTokenMetadata activity
	s.env.OnActivity(s.executor.FetchTokenMetadata, mock.Anything, tokenCID).Return(normalizedMetadata, nil)

	// Mock UpsertTokenMetadata activity
	s.env.OnActivity(s.executor.UpsertTokenMetadata, mock.Anything, tokenCID, normalizedMetadata).Return(nil)

	// Mock EnhanceTokenMetadata activity
	s.env.OnActivity(s.executor.EnhanceTokenMetadata, mock.Anything, tokenCID, normalizedMetadata).Return(nil, nil)

	// Mock CheckMediaURLsHealth activity - both are broken
	s.env.OnActivity(s.executor.CheckMediaURLsHealth, mock.Anything, mock.MatchedBy(func(urls []string) bool {
		return len(urls) == 2
	})).Return(map[string]schema.MediaHealthStatus{
		animationURL: schema.MediaHealthStatusBroken,
		imageURL:     schema.MediaHealthStatusBroken,
	}, nil)

	// No webhook should be triggered because both URLs are broken

	// Mock media indexing child workflow - should still be triggered
	s.env.OnWorkflow(s.workerMedia.IndexMultipleMediaWorkflow, mock.Anything, mock.MatchedBy(func(urls []string) bool {
		return len(urls) == 2
	})).Return(nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenMetadata, tokenCID, (*string)(nil))

	// Verify workflow completed successfully but no webhook triggered
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *IndexMetadataWorkflowTestSuite) TestIndexTokenMetadata_NoMediaURLs() {
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")
	normalizedMetadata := &metadata.NormalizedMetadata{
		Name:        "Test Token",
		Description: "Test Description",
		// No Image or Animation URLs
	}

	// Mock FetchTokenMetadata activity
	s.env.OnActivity(s.executor.FetchTokenMetadata, mock.Anything, tokenCID).Return(normalizedMetadata, nil)

	// Mock UpsertTokenMetadata activity
	s.env.OnActivity(s.executor.UpsertTokenMetadata, mock.Anything, tokenCID, normalizedMetadata).Return(nil)

	// Mock EnhanceTokenMetadata activity
	s.env.OnActivity(s.executor.EnhanceTokenMetadata, mock.Anything, tokenCID, normalizedMetadata).Return(nil, nil)

	// Mock CheckMediaURLsHealth activity - return empty map since no URLs
	s.env.OnActivity(s.executor.CheckMediaURLsHealth, mock.Anything, mock.MatchedBy(func(urls []string) bool {
		return len(urls) == 0
	})).Return(map[string]schema.MediaHealthStatus{}, nil)

	// No webhook should be triggered since media is not healthy (no URLs)
	// No media indexing workflow should be triggered

	// No media workflow should be triggered

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenMetadata, tokenCID, (*string)(nil))

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *IndexMetadataWorkflowTestSuite) TestIndexTokenMetadata_NilMetadata() {
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")

	// Mock FetchTokenMetadata activity to return nil metadata
	s.env.OnActivity(s.executor.FetchTokenMetadata, mock.Anything, tokenCID).Return(nil, nil)

	// Mock EnhanceTokenMetadata activity - should still be called even with nil metadata
	s.env.OnActivity(s.executor.EnhanceTokenMetadata, mock.Anything, tokenCID, (*metadata.NormalizedMetadata)(nil)).Return(nil, nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenMetadata, tokenCID, (*string)(nil))

	// Verify workflow completed successfully (gracefully handles nil metadata)
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

// ====================================================================================
// IndexMultipleTokensMetadata Tests
// ====================================================================================

func (s *IndexMetadataWorkflowTestSuite) TestIndexMultipleTokensMetadata_Success() {
	tokenCIDs := []domain.TokenCID{
		domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1"),
		domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "2"),
		domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "3"),
	}

	// Mock child workflows for each token
	for _, tokenCID := range tokenCIDs {
		s.env.OnWorkflow(s.workerCore.IndexTokenMetadata, mock.Anything, tokenCID, (*string)(nil)).Return(nil)
	}

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexMultipleTokensMetadata, tokenCIDs)

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *IndexMetadataWorkflowTestSuite) TestIndexMultipleTokensMetadata_EmptyList() {
	tokenCIDs := []domain.TokenCID{}

	// Execute the workflow with empty list
	s.env.ExecuteWorkflow(s.workerCore.IndexMultipleTokensMetadata, tokenCIDs)

	// Verify workflow completed successfully (no-op)
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *IndexMetadataWorkflowTestSuite) TestIndexMultipleTokensMetadata_ChildWorkflowStartFailure() {
	tokenCIDs := []domain.TokenCID{
		domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1"),
		domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "2"),
		domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "3"),
	}

	// Mock child workflows - first one fails to start
	s.env.OnWorkflow(s.workerCore.IndexTokenMetadata, mock.Anything, tokenCIDs[0], (*string)(nil)).Return(testsuite.ErrMockStartChildWorkflowFailed)
	s.env.OnWorkflow(s.workerCore.IndexTokenMetadata, mock.Anything, tokenCIDs[1], (*string)(nil)).Return(nil)
	s.env.OnWorkflow(s.workerCore.IndexTokenMetadata, mock.Anything, tokenCIDs[2], (*string)(nil)).Return(nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexMultipleTokensMetadata, tokenCIDs)

	// Verify workflow completed with error
	// The current implementation returns error when child workflow fails to start
	s.True(s.env.IsWorkflowCompleted())
	s.Error(s.env.GetWorkflowError())
}

func (s *IndexMetadataWorkflowTestSuite) TestIndexMultipleTokensMetadata_SingleToken() {
	tokenCIDs := []domain.TokenCID{
		domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1"),
	}

	// Mock child workflow
	s.env.OnWorkflow(s.workerCore.IndexTokenMetadata, mock.Anything, tokenCIDs[0], (*string)(nil)).Return(nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexMultipleTokensMetadata, tokenCIDs)

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *IndexMetadataWorkflowTestSuite) TestIndexMultipleTokensMetadata_LargeNumberOfTokens() {
	// Test with a larger number of tokens to ensure concurrent processing works
	tokenCIDs := make([]domain.TokenCID, 20)
	for i := range 20 {
		tokenCIDs[i] = domain.NewTokenCID(
			domain.ChainEthereumMainnet,
			domain.StandardERC721,
			"0x1234567890123456789012345678901234567890",
			fmt.Sprintf("%d", i),
		)
	}

	// Mock child workflows for each token
	for _, tokenCID := range tokenCIDs {
		s.env.OnWorkflow(s.workerCore.IndexTokenMetadata, mock.Anything, tokenCID, (*string)(nil)).Return(nil)
	}

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexMultipleTokensMetadata, tokenCIDs)

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

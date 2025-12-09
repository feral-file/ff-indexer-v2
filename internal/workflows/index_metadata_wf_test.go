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
	"github.com/feral-file/ff-indexer-v2/internal/workflows"
	workflowsmedia "github.com/feral-file/ff-indexer-v2/internal/workflows/media"
)

// IndexMetadataWorkflowTestSuite is the test suite for metadata workflow tests
type IndexMetadataWorkflowTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env         *testsuite.TestWorkflowEnvironment
	ctrl        *gomock.Controller
	executor    *mocks.MockCoreExecutor
	blacklist   *mocks.MockBlacklistRegistry
	workerCore  workflows.WorkerCore
	workerMedia workflowsmedia.Worker
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
	s.workerCore = workflows.NewWorkerCore(s.executor, workflows.WorkerCoreConfig{
		TezosChainID:                 domain.ChainTezosMainnet,
		EthereumChainID:              domain.ChainEthereumMainnet,
		EthereumTokenSweepStartBlock: 0,
		TezosTokenSweepStartBlock:    0,
		MediaTaskQueue:               "media-task-queue",
	}, s.blacklist)
	s.workerMedia = workflowsmedia.NewWorker(mocks.NewMockMediaExecutor(s.ctrl))

	// Register activities with the test environment
	s.env.RegisterActivity(s.executor.CreateMetadataUpdate)
	s.env.RegisterActivity(s.executor.FetchTokenMetadata)
	s.env.RegisterActivity(s.executor.UpsertTokenMetadata)
	s.env.RegisterActivity(s.executor.EnhanceTokenMetadata)
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
	s.env.OnWorkflow(s.workerCore.IndexTokenMetadata, mock.Anything, tokenCID).Return(nil)

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
	s.env.OnWorkflow(s.workerCore.IndexTokenMetadata, mock.Anything, tokenCID).Return(
		func(ctx workflow.Context, tokenCID domain.TokenCID) error {
			childWorkflowCallCount++
			return testsuite.ErrMockStartChildWorkflowFailed
		},
	)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexMetadataUpdate, event)

	// Verify workflow completed with error (it returns the error)
	s.True(s.env.IsWorkflowCompleted())
	s.Error(s.env.GetWorkflowError())

	s.Equal(2, childWorkflowCallCount, "Child workflow should be attempted 2 times (initial + 1 retry)")
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

	// Mock media indexing child workflow - match the actual workflow type
	s.env.OnWorkflow(s.workerMedia.IndexMultipleMediaWorkflow, mock.Anything, mock.Anything).Return(nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenMetadata, tokenCID)

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *IndexMetadataWorkflowTestSuite) TestIndexTokenMetadata_Success_WithEnhancement() {
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")
	normalizedMetadata := &metadata.NormalizedMetadata{
		Name:        "Test Token",
		Description: "Test Description",
		Image:       "https://example.com/image.jpg",
	}
	imageURL := "https://example.com/enhanced-image.jpg"
	enhancedMetadata := &metadata.EnhancedMetadata{
		ImageURL: &imageURL,
	}

	// Mock FetchTokenMetadata activity
	s.env.OnActivity(s.executor.FetchTokenMetadata, mock.Anything, tokenCID).Return(normalizedMetadata, nil)

	// Mock UpsertTokenMetadata activity
	s.env.OnActivity(s.executor.UpsertTokenMetadata, mock.Anything, tokenCID, normalizedMetadata).Return(nil)

	// Mock EnhanceTokenMetadata activity - returns enhanced metadata
	s.env.OnActivity(s.executor.EnhanceTokenMetadata, mock.Anything, tokenCID, normalizedMetadata).Return(enhancedMetadata, nil)

	// Mock media indexing child workflow - match the actual workflow type
	s.env.OnWorkflow(s.workerMedia.IndexMultipleMediaWorkflow, mock.Anything, mock.Anything).Return(nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenMetadata, tokenCID)

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

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenMetadata, tokenCID)

	// Verify workflow completed with error
	s.True(s.env.IsWorkflowCompleted())
	s.Error(s.env.GetWorkflowError())

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
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenMetadata, tokenCID)

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

	// Mock media indexing child workflow - match the actual workflow type
	s.env.OnWorkflow(s.workerMedia.IndexMultipleMediaWorkflow, mock.Anything, mock.Anything).Return(nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenMetadata, tokenCID)

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

	// Mock media indexing child workflow to fail at start - should not fail parent workflow
	s.env.OnWorkflow(s.workerMedia.IndexMultipleMediaWorkflow, mock.Anything, mock.Anything).Return(testsuite.ErrMockStartChildWorkflowFailed)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenMetadata, tokenCID)

	// Verify workflow completed successfully despite media workflow start failure (non-fatal)
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

	// No media workflow should be triggered

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenMetadata, tokenCID)

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *IndexMetadataWorkflowTestSuite) TestIndexTokenMetadata_NilMetadata() {
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")

	// Mock FetchTokenMetadata activity to return nil metadata
	s.env.OnActivity(s.executor.FetchTokenMetadata, mock.Anything, tokenCID).Return(nil, nil)

	// No other activities should be called

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenMetadata, tokenCID)

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
		s.env.OnWorkflow(s.workerCore.IndexTokenMetadata, mock.Anything, tokenCID).Return(nil)
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
	s.env.OnWorkflow(s.workerCore.IndexTokenMetadata, mock.Anything, tokenCIDs[0]).Return(testsuite.ErrMockStartChildWorkflowFailed)
	s.env.OnWorkflow(s.workerCore.IndexTokenMetadata, mock.Anything, tokenCIDs[1]).Return(nil)
	s.env.OnWorkflow(s.workerCore.IndexTokenMetadata, mock.Anything, tokenCIDs[2]).Return(nil)

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
	s.env.OnWorkflow(s.workerCore.IndexTokenMetadata, mock.Anything, tokenCIDs[0]).Return(nil)

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
		s.env.OnWorkflow(s.workerCore.IndexTokenMetadata, mock.Anything, tokenCID).Return(nil)
	}

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexMultipleTokensMetadata, tokenCIDs)

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

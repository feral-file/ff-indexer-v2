package workflows_test

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/webhook"
	"github.com/feral-file/ff-indexer-v2/internal/workflows"
)

// IndexProvenanceWorkflowTestSuite is the test suite for provenance workflow tests
type IndexProvenanceWorkflowTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env              *testsuite.TestWorkflowEnvironment
	ctrl             *gomock.Controller
	executor         *mocks.MockCoreExecutor
	blacklist        *mocks.MockBlacklistRegistry
	temporalWorkflow *mocks.MockWorkflow
	workerCore       workflows.WorkerCore
}

// SetupTest is called before each test
func (s *IndexProvenanceWorkflowTestSuite) SetupTest() {
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
	}, s.blacklist, s.temporalWorkflow)
}

// TearDownTest is called after each test
func (s *IndexProvenanceWorkflowTestSuite) TearDownTest() {
	s.env.AssertExpectations(s.T())
	s.ctrl.Finish()
}

// TestIndexProvenanceWorkflowTestSuite runs the test suite
func TestIndexProvenanceWorkflowTestSuite(t *testing.T) {
	suite.Run(t, new(IndexProvenanceWorkflowTestSuite))
}

// ====================================================================================
// IndexTokenProvenances Tests
// ====================================================================================

func (s *IndexProvenanceWorkflowTestSuite) TestIndexTokenProvenances_Success() {
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")

	// Mock IndexTokenWithFullProvenancesByTokenCID activity
	s.env.OnActivity(s.executor.IndexTokenWithFullProvenancesByTokenCID, mock.Anything, tokenCID).Return(nil)

	// Mock webhook notification workflow - should be triggered for token.indexing.provenance_completed event
	s.env.OnWorkflow(s.workerCore.NotifyWebhookClients, mock.Anything, mock.MatchedBy(func(event interface{}) bool {
		if webhookEvent, ok := event.(webhook.WebhookEvent); ok {
			return webhookEvent.EventType == webhook.EventTypeTokenIndexingProvenanceCompleted
		}
		return false
	})).Return(nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenProvenances, tokenCID, nil)

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *IndexProvenanceWorkflowTestSuite) TestIndexTokenProvenances_ActivityError() {
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")
	expectedError := errors.New("failed to fetch provenances")

	// Track retry attempts - MaximumAttempts: 1 (no retries)
	var activityCallCount int
	s.env.OnActivity(s.executor.IndexTokenWithFullProvenancesByTokenCID, mock.Anything, tokenCID).Return(
		func(ctx context.Context, tokenCID domain.TokenCID) error {
			activityCallCount++
			return expectedError
		},
	)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenProvenances, tokenCID, nil)

	// Verify workflow completed with error
	s.True(s.env.IsWorkflowCompleted())
	s.Error(s.env.GetWorkflowError())

	// Verify no retries (MaximumAttempts: 1)
	s.Equal(1, activityCallCount, "Activity should be attempted 1 time (no retries)")
}

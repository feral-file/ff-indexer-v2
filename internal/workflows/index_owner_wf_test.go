package workflows_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/store"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
	"github.com/feral-file/ff-indexer-v2/internal/workflows"
)

// IndexOwnerWorkflowTestSuite is the test suite for owner workflow tests
type IndexOwnerWorkflowTestSuite struct {
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
func (s *IndexOwnerWorkflowTestSuite) SetupTest() {
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
		EthereumTokenSweepStartBlock: 1000,
		TezosTokenSweepStartBlock:    1000,
		MediaTaskQueue:               "media-task-queue",
	}, s.blacklist, s.temporalWorkflow)
}

// TearDownTest is called after each test
func (s *IndexOwnerWorkflowTestSuite) TearDownTest() {
	s.env.AssertExpectations(s.T())
	s.ctrl.Finish()
}

// TestIndexOwnerWorkflowTestSuite runs the test suite
func TestIndexOwnerWorkflowTestSuite(t *testing.T) {
	suite.Run(t, new(IndexOwnerWorkflowTestSuite))
}

// ====================================================================================
// IndexTokenOwners Tests
// ====================================================================================

func (s *IndexOwnerWorkflowTestSuite) TestIndexTokenOwners_Success() {
	addresses := []string{
		"0x1234567890123456789012345678901234567890",
		"0xabcdef1234567890123456789012345678901234",
	}

	// Mock child workflows for each address
	for _, address := range addresses {
		s.env.OnWorkflow(s.workerCore.IndexTokenOwner, mock.Anything, address).Return(nil)
	}

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenOwners, addresses)

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *IndexOwnerWorkflowTestSuite) TestIndexTokenOwners_OneAddressFails() {
	addresses := []string{
		"0x1234567890123456789012345678901234567890",
		"0xabcdef1234567890123456789012345678901234",
		"0xfedcba0987654321098765432109876543210987",
	}

	// Mock child workflows - second one fails
	s.env.OnWorkflow(s.workerCore.IndexTokenOwner, mock.Anything, addresses[0]).Return(nil)
	s.env.OnWorkflow(s.workerCore.IndexTokenOwner, mock.Anything, addresses[1]).Return(errors.New("indexing failed"))
	// Third one should not be called due to sequential processing

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenOwners, addresses)

	// Verify workflow completed with error (stops at first failure)
	s.True(s.env.IsWorkflowCompleted())
	s.Error(s.env.GetWorkflowError())
}

// ====================================================================================
// IndexTokenOwner Tests
// ====================================================================================

func (s *IndexOwnerWorkflowTestSuite) TestIndexTokenOwner_EthereumAddress() {
	address := "0x1234567890123456789012345678901234567890"

	// Mock GetExecutionID activity
	s.temporalWorkflow.EXPECT().GetExecutionID(gomock.Any()).Return("workflow-123")

	// Mock GetCurrentHistoryLength activity
	s.temporalWorkflow.EXPECT().GetCurrentHistoryLength(gomock.Any()).Return(1).AnyTimes()

	// Mock job tracking activities
	s.env.OnActivity(s.executor.CreateIndexingJob, mock.Anything, address, domain.ChainEthereumMainnet, mock.Anything, mock.Anything).Return(nil).Once()
	s.env.OnActivity(s.executor.UpdateIndexingJobStatus, mock.Anything, mock.Anything, schema.IndexingJobStatusRunning, mock.Anything).Return(nil).Once()
	s.env.OnActivity(s.executor.UpdateIndexingJobStatus, mock.Anything, mock.Anything, schema.IndexingJobStatusCompleted, mock.Anything).Return(nil).Once()

	// Mock Ethereum child workflow
	s.env.OnWorkflow(s.workerCore.IndexEthereumTokenOwner, mock.Anything, address, mock.Anything).Return(nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenOwner, address)

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *IndexOwnerWorkflowTestSuite) TestIndexTokenOwner_TezosAddress() {
	address := "tz1VSUr8wwNhLAzempoch5d6hLRiTh8Cjcjb"

	// Mock GetExecutionID activity
	s.temporalWorkflow.EXPECT().GetExecutionID(gomock.Any()).Return("workflow-123")

	// Mock GetCurrentHistoryLength activity
	s.temporalWorkflow.EXPECT().GetCurrentHistoryLength(gomock.Any()).Return(1).AnyTimes()

	// Mock job tracking activities
	s.env.OnActivity(s.executor.CreateIndexingJob, mock.Anything, address, domain.ChainTezosMainnet, mock.Anything, mock.Anything).Return(nil).Once()
	s.env.OnActivity(s.executor.UpdateIndexingJobStatus, mock.Anything, mock.Anything, schema.IndexingJobStatusRunning, mock.Anything).Return(nil).Once()
	s.env.OnActivity(s.executor.UpdateIndexingJobStatus, mock.Anything, mock.Anything, schema.IndexingJobStatusCompleted, mock.Anything).Return(nil).Once()

	// Mock Tezos child workflow
	s.env.OnWorkflow(s.workerCore.IndexTezosTokenOwner, mock.Anything, address, mock.Anything).Return(nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenOwner, address)

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *IndexOwnerWorkflowTestSuite) TestIndexTokenOwner_UnknownAddress() {
	address := "someaddress"

	// Mock GetExecutionID activity
	s.temporalWorkflow.EXPECT().GetExecutionID(gomock.Any()).Return("workflow-123")

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenOwner, address)

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.Error(s.env.GetWorkflowError())
	s.Contains(s.env.GetWorkflowError().Error(), "unsupported blockchain for address: someaddress")
}

func (s *IndexOwnerWorkflowTestSuite) TestIndexTokenOwner_JobCreationFails_WorkflowContinues() {
	address := "0x1234567890123456789012345678901234567890"

	// Mock GetExecutionID activity
	s.temporalWorkflow.EXPECT().GetExecutionID(gomock.Any()).Return("workflow-123")

	// Mock GetCurrentHistoryLength activity
	s.temporalWorkflow.EXPECT().GetCurrentHistoryLength(gomock.Any()).Return(1).AnyTimes()

	// Mock job tracking activities - CreateIndexingJob fails but workflow should continue
	// Activity will retry twice due to MaximumAttempts: 2
	s.env.OnActivity(s.executor.CreateIndexingJob, mock.Anything, address, domain.ChainEthereumMainnet, mock.Anything, mock.Anything).Return(errors.New("database error")).Twice()
	s.env.OnActivity(s.executor.UpdateIndexingJobStatus, mock.Anything, mock.Anything, schema.IndexingJobStatusRunning, mock.Anything).Return(nil).Once()
	s.env.OnActivity(s.executor.UpdateIndexingJobStatus, mock.Anything, mock.Anything, schema.IndexingJobStatusCompleted, mock.Anything).Return(nil).Once()

	// Mock Ethereum child workflow to succeed
	s.env.OnWorkflow(s.workerCore.IndexEthereumTokenOwner, mock.Anything, address, mock.Anything).Return(nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenOwner, address)

	// Verify workflow completed successfully despite job creation failure
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *IndexOwnerWorkflowTestSuite) TestIndexTokenOwner_JobStatusUpdateFails_WorkflowContinues() {
	address := "tz1VSUr8wwNhLAzempoch5d6hLRiTh8Cjcjb"

	// Mock GetExecutionID activity
	s.temporalWorkflow.EXPECT().GetExecutionID(gomock.Any()).Return("workflow-123")

	// Mock GetCurrentHistoryLength activity
	s.temporalWorkflow.EXPECT().GetCurrentHistoryLength(gomock.Any()).Return(1).AnyTimes()

	// Mock job tracking activities - status updates fail but workflow should continue
	// Activities will retry twice due to MaximumAttempts: 2
	s.env.OnActivity(s.executor.CreateIndexingJob, mock.Anything, address, domain.ChainTezosMainnet, mock.Anything, mock.Anything).Return(nil).Once()
	s.env.OnActivity(s.executor.UpdateIndexingJobStatus, mock.Anything, mock.Anything, schema.IndexingJobStatusRunning, mock.Anything).Return(errors.New("status update error")).Twice()
	s.env.OnActivity(s.executor.UpdateIndexingJobStatus, mock.Anything, mock.Anything, schema.IndexingJobStatusCompleted, mock.Anything).Return(errors.New("status update error")).Twice()

	// Mock Tezos child workflow to succeed
	s.env.OnWorkflow(s.workerCore.IndexTezosTokenOwner, mock.Anything, address, mock.Anything).Return(nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenOwner, address)

	// Verify workflow completed successfully despite status update failures
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *IndexOwnerWorkflowTestSuite) TestIndexTokenOwner_ChildWorkflowFails_JobMarkedAsFailed() {
	address := "0x1234567890123456789012345678901234567890"
	expectedError := errors.New("child workflow failed")

	// Mock GetExecutionID activity
	s.temporalWorkflow.EXPECT().GetExecutionID(gomock.Any()).Return("workflow-123")

	// Mock GetCurrentHistoryLength activity
	s.temporalWorkflow.EXPECT().GetCurrentHistoryLength(gomock.Any()).Return(1).AnyTimes()

	// Mock job tracking activities
	s.env.OnActivity(s.executor.CreateIndexingJob, mock.Anything, address, domain.ChainEthereumMainnet, mock.Anything, mock.Anything).Return(nil).Once()
	s.env.OnActivity(s.executor.UpdateIndexingJobStatus, mock.Anything, mock.Anything, schema.IndexingJobStatusRunning, mock.Anything).Return(nil).Once()
	s.env.OnActivity(s.executor.UpdateIndexingJobStatus, mock.Anything, mock.Anything, schema.IndexingJobStatusFailed, mock.Anything).Return(nil).Once()

	// Mock Ethereum child workflow to fail
	s.env.OnWorkflow(s.workerCore.IndexEthereumTokenOwner, mock.Anything, address, mock.Anything).Return(expectedError)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenOwner, address)

	// Verify workflow completed with error and job status was updated to failed
	s.True(s.env.IsWorkflowCompleted())
	s.Error(s.env.GetWorkflowError())
	s.Contains(s.env.GetWorkflowError().Error(), expectedError.Error())
}

func (s *IndexOwnerWorkflowTestSuite) TestIndexTokenOwner_Cancellation_JobMarkedAsCanceled() {
	address := "0x1234567890123456789012345678901234567890"

	// Mock GetExecutionID activity
	s.temporalWorkflow.EXPECT().GetExecutionID(gomock.Any()).Return("workflow-123")

	// Mock GetCurrentHistoryLength activity - return > 0 to enable defer logic
	s.temporalWorkflow.EXPECT().GetCurrentHistoryLength(gomock.Any()).Return(5).AnyTimes()

	// Mock job tracking activities
	s.env.OnActivity(s.executor.CreateIndexingJob, mock.Anything, address, domain.ChainEthereumMainnet, mock.Anything, mock.Anything).Return(nil).Once()
	s.env.OnActivity(s.executor.UpdateIndexingJobStatus, mock.Anything, mock.Anything, schema.IndexingJobStatusRunning, mock.Anything).Return(nil).Once()
	s.env.OnActivity(s.executor.UpdateIndexingJobStatus, mock.Anything, mock.Anything, schema.IndexingJobStatusCanceled, mock.Anything).Return(nil).Once()

	// Mock Ethereum child workflow to be canceled
	s.env.OnWorkflow(s.workerCore.IndexEthereumTokenOwner, mock.Anything, address, mock.Anything).Return(workflow.ErrCanceled)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenOwner, address)

	// Verify workflow was canceled
	s.True(s.env.IsWorkflowCompleted())
	s.Error(s.env.GetWorkflowError())
	s.Contains(s.env.GetWorkflowError().Error(), "canceled")
}

func (s *IndexOwnerWorkflowTestSuite) TestIndexTokenOwner_FailedStatusUpdateAfterChildFailure_WorkflowStillFails() {
	address := "tz1VSUr8wwNhLAzempoch5d6hLRiTh8Cjcjb"
	childError := errors.New("child workflow failed")

	// Mock GetExecutionID activity
	s.temporalWorkflow.EXPECT().GetExecutionID(gomock.Any()).Return("workflow-123")

	// Mock GetCurrentHistoryLength activity
	s.temporalWorkflow.EXPECT().GetCurrentHistoryLength(gomock.Any()).Return(1).AnyTimes()

	// Mock job tracking activities - status update to 'failed' also fails (retries twice)
	s.env.OnActivity(s.executor.CreateIndexingJob, mock.Anything, address, domain.ChainTezosMainnet, mock.Anything, mock.Anything).Return(nil).Once()
	s.env.OnActivity(s.executor.UpdateIndexingJobStatus, mock.Anything, mock.Anything, schema.IndexingJobStatusRunning, mock.Anything).Return(nil).Once()
	s.env.OnActivity(s.executor.UpdateIndexingJobStatus, mock.Anything, mock.Anything, schema.IndexingJobStatusFailed, mock.Anything).Return(errors.New("status update error")).Twice()

	// Mock Tezos child workflow to fail
	s.env.OnWorkflow(s.workerCore.IndexTezosTokenOwner, mock.Anything, address, mock.Anything).Return(childError)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenOwner, address)

	// Verify workflow completed with child error, even though status update failed
	s.True(s.env.IsWorkflowCompleted())
	s.Error(s.env.GetWorkflowError())
	s.Contains(s.env.GetWorkflowError().Error(), childError.Error())
}

// ====================================================================================
// IndexTezosTokenOwner Tests
// ====================================================================================

func (s *IndexOwnerWorkflowTestSuite) TestIndexTezosTokenOwner_FirstRun_WithTokens() {
	address := "tz1VSUr8wwNhLAzempoch5d6hLRiTh8Cjcjb"
	chainID := domain.ChainTezosMainnet
	latestBlock := uint64(5000)

	// Create test tokens (2 chunks of 50 tokens each for chunk size 50)
	tokens := make([]domain.TokenWithBlock, 55)
	for i := range tokens {
		tokens[i] = domain.TokenWithBlock{
			TokenCID:    domain.NewTokenCID(chainID, domain.StandardFA2, "KT1Contract", fmt.Sprintf("%d", i)),
			BlockNumber: uint64(2000 + i*10), //nolint:gosec,G115
		}
	}

	startBlock := uint64(1000) // TezosTokenSweepStartBlock from config

	// Verify EnsureWatchedAddressExists is called with correct params
	s.env.OnActivity(s.executor.EnsureWatchedAddressExists, mock.Anything, address, chainID, mock.Anything).Return(nil)

	// Verify GetIndexingBlockRangeForAddress is called with correct params
	s.env.OnActivity(s.executor.GetIndexingBlockRangeForAddress, mock.Anything, address, chainID).Return(&workflows.BlockRangeResult{MinBlock: 0, MaxBlock: 0}, nil)

	// Verify GetLatestTezosBlock is called
	s.env.OnActivity(s.executor.GetLatestTezosBlock, mock.Anything).Return(latestBlock, nil)

	// Verify GetTezosTokenCIDsByAccountWithinBlockRange is called with correct block range
	s.env.OnActivity(s.executor.GetTezosTokenCIDsByAccountWithinBlockRange, mock.Anything, address, startBlock, latestBlock).Return(tokens, nil)

	// Track block range updates to verify resumability logic
	var blockRangeUpdates []struct {
		minBlock uint64
		maxBlock uint64
	}
	s.env.OnActivity(s.executor.UpdateIndexingBlockRangeForAddress, mock.Anything, address, chainID, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, addr string, chain domain.Chain, minBlock uint64, maxBlock uint64) error {
			blockRangeUpdates = append(blockRangeUpdates, struct {
				minBlock uint64
				maxBlock uint64
			}{minBlock, maxBlock})
			return nil
		},
	)

	// Track IndexTokens child workflow calls to verify chunking
	var indexTokensCalls [][]domain.TokenCID
	s.env.OnWorkflow(s.workerCore.IndexTokens, mock.Anything, mock.Anything, mock.Anything).Return(
		func(ctx workflow.Context, tokenCIDs []domain.TokenCID, address *string) error {
			indexTokensCalls = append(indexTokensCalls, tokenCIDs)
			return nil
		},
	)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTezosTokenOwner, address, (*string)(nil))

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())

	// Verify chunking: 55 tokens -> 2 chunks (50 + 5)
	s.Equal(2, len(indexTokensCalls), "Should have 2 chunks")
	s.Equal(50, len(indexTokensCalls[0]), "First chunk should have 50 tokens")
	s.Equal(5, len(indexTokensCalls[1]), "Second chunk should have 5 tokens")

	// Verify block range updates for resumability
	// First run descending: set max on first chunk, then progressively update min
	// Plus final update at the end
	s.GreaterOrEqual(len(blockRangeUpdates), 3, "Should have at least 3 block range updates")

	// All updates should maintain the latest block (5000)
	for _, update := range blockRangeUpdates {
		s.Equal(latestBlock, update.maxBlock, "All updates should maintain stored max_block")
	}

	// Second chunk update: should update min_block progressively
	s.LessOrEqual(startBlock, blockRangeUpdates[1].minBlock, "Second update should set min_block to min block of chunk")

	// Final update: should set min block to start block
	s.Equal(startBlock, blockRangeUpdates[len(blockRangeUpdates)-1].minBlock, "Final update should set min_block to start block")
}

func (s *IndexOwnerWorkflowTestSuite) TestIndexTezosTokenOwner_JobProgressTracking() {
	address := "tz1VSUr8wwNhLAzempoch5d6hLRiTh8Cjcjb"
	chainID := domain.ChainTezosMainnet
	latestBlock := uint64(10000)
	startBlock := uint64(1000)
	jobID := "test-job-123" // Provide a job ID for progress tracking

	// Create 100 tokens to test progress tracking across multiple chunks
	tokens := make([]domain.TokenWithBlock, 100)
	for i := range tokens {
		tokens[i] = domain.TokenWithBlock{
			TokenCID:    domain.NewTokenCID(chainID, domain.StandardFA2, "KT1Contract", fmt.Sprintf("%d", i)),
			BlockNumber: uint64(5000 + i*10), //nolint:gosec,G115
		}
	}

	// Other activities succeed
	s.env.OnActivity(s.executor.EnsureWatchedAddressExists, mock.Anything, address, chainID, mock.Anything).Return(nil)
	s.env.OnActivity(s.executor.GetIndexingBlockRangeForAddress, mock.Anything, address, chainID).Return(&workflows.BlockRangeResult{MinBlock: 0, MaxBlock: 0}, nil)
	s.env.OnActivity(s.executor.GetLatestTezosBlock, mock.Anything).Return(latestBlock, nil)
	s.env.OnActivity(s.executor.GetTezosTokenCIDsByAccountWithinBlockRange, mock.Anything, address, startBlock, latestBlock).Return(tokens, nil)
	s.env.OnActivity(s.executor.UpdateIndexingBlockRangeForAddress, mock.Anything, address, chainID, mock.Anything, mock.Anything).Return(nil)
	s.env.OnWorkflow(s.workerCore.IndexTokens, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Track progress updates
	var progressUpdates []int
	s.env.OnActivity(s.executor.UpdateIndexingJobProgress, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, workflowID string, tokensProcessed int, minBlock, maxBlock uint64) error {
			progressUpdates = append(progressUpdates, tokensProcessed)
			return nil
		},
	)

	// Execute the workflow with a jobID to enable progress tracking
	s.env.ExecuteWorkflow(s.workerCore.IndexTezosTokenOwner, address, &jobID)

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())

	// Verify progress tracking: 100 tokens with chunk size 50 = 2 chunks
	s.Equal(2, len(progressUpdates), "Should have 2 progress updates")
	s.Equal(50, progressUpdates[0], "First chunk should process 50 tokens")
	s.Equal(50, progressUpdates[1], "Second chunk should process 50 tokens")
}

func (s *IndexOwnerWorkflowTestSuite) TestIndexTezosTokenOwner_JobTrackingGracefulFailure() {
	address := "tz1VSUr8wwNhLAzempoch5d6hLRiTh8Cjcjb"
	chainID := domain.ChainTezosMainnet
	latestBlock := uint64(5000)
	startBlock := uint64(1000)
	jobID := "test-job-123" // Provide a job ID for progress tracking

	tokens := make([]domain.TokenWithBlock, 10)
	for i := range tokens {
		tokens[i] = domain.TokenWithBlock{
			TokenCID:    domain.NewTokenCID(chainID, domain.StandardFA2, "KT1Contract", fmt.Sprintf("%d", i)),
			BlockNumber: uint64(2000 + i*10), //nolint:gosec,G115
		}
	}

	// Job tracking fails, but workflow should continue
	s.env.OnActivity(s.executor.UpdateIndexingJobProgress, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("job tracking error"))

	// Other activities succeed
	s.env.OnActivity(s.executor.EnsureWatchedAddressExists, mock.Anything, address, chainID, mock.Anything).Return(nil)
	s.env.OnActivity(s.executor.GetIndexingBlockRangeForAddress, mock.Anything, address, chainID).Return(&workflows.BlockRangeResult{MinBlock: 0, MaxBlock: 0}, nil)
	s.env.OnActivity(s.executor.GetLatestTezosBlock, mock.Anything).Return(latestBlock, nil)
	s.env.OnActivity(s.executor.GetTezosTokenCIDsByAccountWithinBlockRange, mock.Anything, address, startBlock, latestBlock).Return(tokens, nil)
	s.env.OnActivity(s.executor.UpdateIndexingBlockRangeForAddress, mock.Anything, address, chainID, mock.Anything, mock.Anything).Return(nil)
	s.env.OnWorkflow(s.workerCore.IndexTokens, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute the workflow with a jobID to test job tracking failure handling
	s.env.ExecuteWorkflow(s.workerCore.IndexTezosTokenOwner, address, &jobID)

	// Verify workflow completed successfully despite job tracking failures
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError(), "Workflow should succeed even if job tracking fails")
}

func (s *IndexOwnerWorkflowTestSuite) TestIndexTezosTokenOwner_FirstRun_NoTokens() {
	address := "tz1VSUr8wwNhLAzempoch5d6hLRiTh8Cjcjb"
	chainID := domain.ChainTezosMainnet
	latestBlock := uint64(5000)

	// Other activities succeed
	s.env.OnActivity(s.executor.EnsureWatchedAddressExists, mock.Anything, address, chainID, mock.Anything).Return(nil)
	s.env.OnActivity(s.executor.GetIndexingBlockRangeForAddress, mock.Anything, address, chainID).Return(&workflows.BlockRangeResult{MinBlock: 0, MaxBlock: 0}, nil)
	s.env.OnActivity(s.executor.GetLatestTezosBlock, mock.Anything).Return(latestBlock, nil)

	// Mock GetTezosTokenCIDsByAccountWithinBlockRange returns no tokens
	s.env.OnActivity(s.executor.GetTezosTokenCIDsByAccountWithinBlockRange, mock.Anything, address, uint64(1000), latestBlock).Return([]domain.TokenWithBlock{}, nil)

	// Mock final update
	s.env.OnActivity(s.executor.UpdateIndexingBlockRangeForAddress, mock.Anything, address, chainID, mock.Anything, mock.Anything).Return(nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTezosTokenOwner, address, (*string)(nil))

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *IndexOwnerWorkflowTestSuite) TestIndexTezosTokenOwner_SubsequentRun_BackwardSweepOnly() {
	address := "tz1VSUr8wwNhLAzempoch5d6hLRiTh8Cjcjb"
	chainID := domain.ChainTezosMainnet
	storedMinBlock := uint64(3000)
	storedMaxBlock := uint64(5000)
	latestBlock := uint64(5000) // No new blocks since last run
	startBlock := uint64(1000)  // TezosTokenSweepStartBlock from config

	// Create backward tokens (historical)
	backwardTokens := make([]domain.TokenWithBlock, 10)
	for i := range backwardTokens {
		backwardTokens[i] = domain.TokenWithBlock{
			TokenCID:    domain.NewTokenCID(chainID, domain.StandardFA2, "KT1Contract", fmt.Sprintf("%d", i)),
			BlockNumber: uint64(1500 + i*10), //nolint:gosec,G115
		}
	}

	// Other activities succeed
	s.env.OnActivity(s.executor.EnsureWatchedAddressExists, mock.Anything, address, chainID, mock.Anything).Return(nil)
	s.env.OnActivity(s.executor.GetIndexingBlockRangeForAddress, mock.Anything, address, chainID).Return(&workflows.BlockRangeResult{MinBlock: storedMinBlock, MaxBlock: storedMaxBlock}, nil)
	s.env.OnActivity(s.executor.GetLatestTezosBlock, mock.Anything).Return(latestBlock, nil)

	// Verify backward sweep: from 1000 (startBlock) to 2999 (storedMinBlock - 1)
	s.env.OnActivity(s.executor.GetTezosTokenCIDsByAccountWithinBlockRange, mock.Anything, address, startBlock, storedMinBlock-1).Return(backwardTokens, nil)

	// Track block range updates to verify min_block is updated progressively
	var blockRangeUpdates []struct {
		minBlock uint64
		maxBlock uint64
	}
	s.env.OnActivity(s.executor.UpdateIndexingBlockRangeForAddress, mock.Anything, address, chainID, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, addr string, chain domain.Chain, minBlock uint64, maxBlock uint64) error {
			blockRangeUpdates = append(blockRangeUpdates, struct {
				minBlock uint64
				maxBlock uint64
			}{minBlock, maxBlock})
			return nil
		},
	)

	// Mock IndexTokens child workflow
	s.env.OnWorkflow(s.workerCore.IndexTokens, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTezosTokenOwner, address, (*string)(nil))

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())

	// Verify block range updates - backward sweep updates min_block progressively
	s.GreaterOrEqual(len(blockRangeUpdates), 2, "Should have 2 block range updates")

	// All updates should maintain the stored max_block (5000)
	for _, update := range blockRangeUpdates {
		s.Equal(storedMaxBlock, update.maxBlock, "All updates should maintain stored max_block")
	}

	// Final update should have min_block = startBlock (1000)
	finalUpdate := blockRangeUpdates[len(blockRangeUpdates)-1]
	s.Equal(startBlock, finalUpdate.minBlock, "Final update should set min_block to start block")
}

func (s *IndexOwnerWorkflowTestSuite) TestIndexTezosTokenOwner_SubsequentRun_ForwardSweepOnly() {
	address := "tz1VSUr8wwNhLAzempoch5d6hLRiTh8Cjcjb"
	chainID := domain.ChainTezosMainnet
	storedMinBlock := uint64(1000) // Already at start block
	storedMaxBlock := uint64(3000)
	latestBlock := uint64(5000)

	// Create forward tokens (new updates)
	forwardTokens := make([]domain.TokenWithBlock, 10)
	for i := range forwardTokens {
		forwardTokens[i] = domain.TokenWithBlock{
			TokenCID:    domain.NewTokenCID(chainID, domain.StandardFA2, "KT1Contract", fmt.Sprintf("%d", i)),
			BlockNumber: uint64(3500 + i*10), //nolint:gosec,G115
		}
	}

	// Other activities succeed
	s.env.OnActivity(s.executor.EnsureWatchedAddressExists, mock.Anything, address, chainID, mock.Anything).Return(nil)
	s.env.OnActivity(s.executor.GetIndexingBlockRangeForAddress, mock.Anything, address, chainID).Return(&workflows.BlockRangeResult{MinBlock: storedMinBlock, MaxBlock: storedMaxBlock}, nil)
	s.env.OnActivity(s.executor.GetLatestTezosBlock, mock.Anything).Return(latestBlock, nil)

	// Verify forward sweep: from 3001 (storedMaxBlock + 1) to 5000 (latestBlock)
	s.env.OnActivity(s.executor.GetTezosTokenCIDsByAccountWithinBlockRange, mock.Anything, address, storedMaxBlock+1, latestBlock).Return(forwardTokens, nil)

	// Track block range updates
	var blockRangeUpdates []struct {
		minBlock uint64
		maxBlock uint64
	}
	s.env.OnActivity(s.executor.UpdateIndexingBlockRangeForAddress, mock.Anything, address, chainID, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, addr string, chain domain.Chain, minBlock uint64, maxBlock uint64) error {
			blockRangeUpdates = append(blockRangeUpdates, struct {
				minBlock uint64
				maxBlock uint64
			}{minBlock, maxBlock})
			return nil
		},
	)

	// Mock IndexTokens child workflow
	s.env.OnWorkflow(s.workerCore.IndexTokens, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTezosTokenOwner, address, (*string)(nil))

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())

	// Verify block range updates - forward sweep updates max_block progressively
	s.GreaterOrEqual(len(blockRangeUpdates), 2, "Should have at least 2 block range updates")

	// All updates should maintain the stored min_block (1000)
	for _, update := range blockRangeUpdates {
		s.Equal(storedMinBlock, update.minBlock, "All updates should maintain stored min_block")
	}

	// Final update should have max_block = latestBlock (5000)
	finalUpdate := blockRangeUpdates[len(blockRangeUpdates)-1]
	s.Equal(latestBlock, finalUpdate.maxBlock, "Final update should set max_block to latest block")
}

func (s *IndexOwnerWorkflowTestSuite) TestIndexTezosTokenOwner_SubsequentRun_BothSweeps() {
	address := "tz1VSUr8wwNhLAzempoch5d6hLRiTh8Cjcjb"
	chainID := domain.ChainTezosMainnet
	storedMinBlock := uint64(3000)
	storedMaxBlock := uint64(4000)
	latestBlock := uint64(6000)
	startBlock := uint64(1000) // TezosTokenSweepStartBlock from config

	// Create backward tokens (historical)
	backwardTokens := make([]domain.TokenWithBlock, 5)
	for i := range backwardTokens {
		backwardTokens[i] = domain.TokenWithBlock{
			TokenCID:    domain.NewTokenCID(chainID, domain.StandardFA2, "KT1Contract", fmt.Sprintf("back_%d", i)),
			BlockNumber: uint64(1500 + i*10), //nolint:gosec,G115
		}
	}

	// Create forward tokens (new updates)
	forwardTokens := make([]domain.TokenWithBlock, 5)
	for i := range forwardTokens {
		forwardTokens[i] = domain.TokenWithBlock{
			TokenCID:    domain.NewTokenCID(chainID, domain.StandardFA2, "KT1Contract", fmt.Sprintf("forward_%d", i)),
			BlockNumber: uint64(5000 + i*10), //nolint:gosec,G115
		}
	}

	// Other activities succeed
	s.env.OnActivity(s.executor.EnsureWatchedAddressExists, mock.Anything, address, chainID, mock.Anything).Return(nil)
	s.env.OnActivity(s.executor.GetIndexingBlockRangeForAddress, mock.Anything, address, chainID).Return(&workflows.BlockRangeResult{MinBlock: storedMinBlock, MaxBlock: storedMaxBlock}, nil)
	s.env.OnActivity(s.executor.GetLatestTezosBlock, mock.Anything).Return(latestBlock, nil)

	// Verify backward sweep: from 1000 to 2999 (storedMinBlock - 1)
	s.env.OnActivity(s.executor.GetTezosTokenCIDsByAccountWithinBlockRange, mock.Anything, address, startBlock, storedMinBlock-1).Return(backwardTokens, nil)

	// Verify forward sweep: from 4001 (storedMaxBlock + 1) to 6000
	s.env.OnActivity(s.executor.GetTezosTokenCIDsByAccountWithinBlockRange, mock.Anything, address, storedMaxBlock+1, latestBlock).Return(forwardTokens, nil)

	// Track block range updates to verify sweep order and resumability
	var blockRangeUpdates []struct {
		minBlock uint64
		maxBlock uint64
	}
	s.env.OnActivity(s.executor.UpdateIndexingBlockRangeForAddress, mock.Anything, address, chainID, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, addr string, chain domain.Chain, minBlock uint64, maxBlock uint64) error {
			blockRangeUpdates = append(blockRangeUpdates, struct {
				minBlock uint64
				maxBlock uint64
			}{minBlock, maxBlock})
			return nil
		},
	)

	// Track IndexTokens child workflow calls - should be 2 (backward + forward)
	var indexTokensCalls int
	s.env.OnWorkflow(s.workerCore.IndexTokens, mock.Anything, mock.Anything, mock.Anything).Return(
		func(ctx workflow.Context, tokenCIDs []domain.TokenCID, address *string) error {
			indexTokensCalls++
			return nil
		},
	)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTezosTokenOwner, address, (*string)(nil))

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())

	// Verify both sweeps were executed
	s.Equal(2, indexTokensCalls, "Should have 2 IndexTokens calls (backward + forward)")

	// Verify block range updates
	// Should have updates for: backward chunk, backward final, forward chunk, forward final
	s.GreaterOrEqual(len(blockRangeUpdates), 4, "Should have at least 4 block range updates")

	// Backward sweep updates min_block progressively to startBlock (1000)
	// Forward sweep updates max_block progressively to latestBlock (6000)
	// Final updates should cover the complete range [1000, 6000]

	finalUpdate := blockRangeUpdates[len(blockRangeUpdates)-1]
	s.Equal(startBlock, finalUpdate.minBlock, "Final update should set min_block to start block (after backward sweep)")
	s.Equal(latestBlock, finalUpdate.maxBlock, "Final update should set max_block to latest block (after forward sweep)")
}

func (s *IndexOwnerWorkflowTestSuite) TestIndexTezosTokenOwner_EnsureWatchedAddressError() {
	address := "tz1VSUr8wwNhLAzempoch5d6hLRiTh8Cjcjb"
	chainID := domain.ChainTezosMainnet
	expectedError := errors.New("database error")

	// Mock activity to return error
	s.env.OnActivity(s.executor.EnsureWatchedAddressExists, mock.Anything, address, chainID, mock.Anything).Return(expectedError)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTezosTokenOwner, address, (*string)(nil))

	// Verify workflow completed with error
	s.True(s.env.IsWorkflowCompleted())
	s.Error(s.env.GetWorkflowError())
	s.Contains(s.env.GetWorkflowError().Error(), "database error")
}

func (s *IndexOwnerWorkflowTestSuite) TestIndexTezosTokenOwner_GetLatestBlockError() {
	address := "tz1VSUr8wwNhLAzempoch5d6hLRiTh8Cjcjb"
	chainID := domain.ChainTezosMainnet
	expectedError := errors.New("API error")

	// Mock activities
	s.env.OnActivity(s.executor.EnsureWatchedAddressExists, mock.Anything, address, chainID, mock.Anything).Return(nil)
	s.env.OnActivity(s.executor.GetIndexingBlockRangeForAddress, mock.Anything, address, chainID).Return(&workflows.BlockRangeResult{MinBlock: 0, MaxBlock: 0}, nil)

	// Track retry attempts - MaximumAttempts: 2 in the workflow
	var activityCallCount int
	s.env.OnActivity(s.executor.GetLatestTezosBlock, mock.Anything).Return(
		func(ctx context.Context) (uint64, error) {
			activityCallCount++
			return 0, expectedError
		},
	)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTezosTokenOwner, address, (*string)(nil))

	// Verify workflow completed with error
	s.True(s.env.IsWorkflowCompleted())
	s.Error(s.env.GetWorkflowError())

	// Verify retries (MaximumAttempts: 2 means initial + 1 retry)
	s.Equal(2, activityCallCount, "Activity should be attempted 2 times (initial + 1 retry)")
}

// ====================================================================================
// IndexTezosTokenOwner Tests - Budgeted Indexing Mode
// ====================================================================================

func (s *IndexOwnerWorkflowTestSuite) TestIndexTezosTokenOwner_BudgetedMode_QuotaExhausted() {
	// Setup worker core with budgeted mode enabled and low quota
	workerCore := workflows.NewWorkerCore(s.executor, workflows.WorkerCoreConfig{
		TezosChainID:                      domain.ChainTezosMainnet,
		EthereumChainID:                   domain.ChainEthereumMainnet,
		EthereumTokenSweepStartBlock:      1000,
		TezosTokenSweepStartBlock:         1000,
		MediaTaskQueue:                    "media-task-queue",
		BudgetedIndexingModeEnabled:       true, // Enabled
		BudgetedIndexingDefaultDailyQuota: 50,   // Low quota
	}, s.blacklist, s.temporalWorkflow)

	address := "tz1abc123def456"
	chainID := domain.ChainTezosMainnet
	latestBlock := uint64(5000)

	// Create 100 tokens that would exceed quota
	tokens := make([]domain.TokenWithBlock, 100)
	for i := range 100 {
		tokens[i] = domain.TokenWithBlock{
			TokenCID:    domain.TokenCID(fmt.Sprintf("tezos:mainnet:fa2:KT1abc:%d", i)),
			BlockNumber: uint64(1000 + i), //nolint:gosec,G115
		}
	}

	// Other activities succeed
	s.env.OnActivity(s.executor.EnsureWatchedAddressExists, mock.Anything, address, chainID, mock.Anything).Return(nil)
	s.env.OnActivity(s.executor.GetIndexingBlockRangeForAddress, mock.Anything, address, chainID).
		Return(&workflows.BlockRangeResult{MinBlock: 0, MaxBlock: 0}, nil)
	s.env.OnActivity(s.executor.GetLatestTezosBlock, mock.Anything).Return(latestBlock, nil)
	s.env.OnActivity(s.executor.GetTezosTokenCIDsByAccountWithinBlockRange, mock.Anything, address, uint64(1000), latestBlock).
		Return(tokens, nil)

	// First quota check - 50 tokens available
	s.env.OnActivity(s.executor.GetQuotaInfo, mock.Anything, address, chainID).Return(&store.QuotaInfo{
		RemainingQuota:     50,
		TotalQuota:         50,
		TokensIndexedToday: 0,
		QuotaExhausted:     false,
	}, nil).Once()

	// Mock IndexTokens for first chunk (50 tokens)
	s.env.OnWorkflow(workerCore.IndexTokens, mock.Anything, mock.MatchedBy(func(cids []domain.TokenCID) bool {
		return len(cids) == 50 // First chunk limited by quota
	}), mock.Anything).Return(nil).Once()

	// Mock IncrementTokensIndexed for first chunk
	s.env.OnActivity(s.executor.IncrementTokensIndexed, mock.Anything, address, chainID, 50).Return(nil).Once()

	// Mock UpdateIndexingBlockRangeForAddress - called once (after first chunk)
	s.env.OnActivity(s.executor.UpdateIndexingBlockRangeForAddress, mock.Anything, address, chainID, mock.Anything, mock.Anything).Return(nil).Once()

	// Second quota check after processing first chunk - exhausted
	s.env.OnActivity(s.executor.GetQuotaInfo, mock.Anything, address, chainID).Return(&store.QuotaInfo{
		RemainingQuota:     0,
		TotalQuota:         50,
		TokensIndexedToday: 50,
		QuotaExhausted:     true,
		QuotaResetAt:       s.env.Now().Add(24 * time.Hour),
	}, nil).Once()

	// Execute the workflow
	s.env.ExecuteWorkflow(workerCore.IndexTezosTokenOwner, address, (*string)(nil))

	// Should complete with continue-as-new error (special case - not a real failure)
	s.True(s.env.IsWorkflowCompleted())
	workflowErr := s.env.GetWorkflowError()
	s.Error(workflowErr, "Workflow should return continue-as-new error")
	s.Contains(workflowErr.Error(), "continue as new", "Error should be a continue-as-new error")
}

func (s *IndexOwnerWorkflowTestSuite) TestIndexTezosTokenOwner_BudgetedMode_Normal() {
	// Setup worker core with budgeted mode enabled and sufficient quota
	workerCore := workflows.NewWorkerCore(s.executor, workflows.WorkerCoreConfig{
		TezosChainID:                      domain.ChainTezosMainnet,
		EthereumChainID:                   domain.ChainEthereumMainnet,
		EthereumTokenSweepStartBlock:      1000,
		TezosTokenSweepStartBlock:         1000,
		MediaTaskQueue:                    "media-task-queue",
		BudgetedIndexingModeEnabled:       true,
		BudgetedIndexingDefaultDailyQuota: 1000, // High quota
	}, s.blacklist, s.temporalWorkflow)

	address := "tz1xyz789"
	chainID := domain.ChainTezosMainnet
	latestBlock := uint64(3000)

	// Create 15 tokens (less than quota)
	tokens := make([]domain.TokenWithBlock, 15)
	for i := range 15 {
		tokens[i] = domain.TokenWithBlock{
			TokenCID:    domain.TokenCID(fmt.Sprintf("tezos:mainnet:fa2:KT1xyz:%d", i)),
			BlockNumber: uint64(2000 + i*10), //nolint:gosec,G115
		}
	}

	// Other activities succeed
	s.env.OnActivity(s.executor.EnsureWatchedAddressExists, mock.Anything, address, chainID, mock.Anything).Return(nil)
	s.env.OnActivity(s.executor.GetIndexingBlockRangeForAddress, mock.Anything, address, chainID).
		Return(&workflows.BlockRangeResult{MinBlock: 0, MaxBlock: 0}, nil)
	s.env.OnActivity(s.executor.GetLatestTezosBlock, mock.Anything).Return(latestBlock, nil)
	s.env.OnActivity(s.executor.GetTezosTokenCIDsByAccountWithinBlockRange, mock.Anything, address, uint64(1000), latestBlock).
		Return(tokens, nil)

	// Quota check - plenty available
	s.env.OnActivity(s.executor.GetQuotaInfo, mock.Anything, address, chainID).Return(&store.QuotaInfo{
		RemainingQuota:     1000,
		TotalQuota:         1000,
		TokensIndexedToday: 0,
		QuotaExhausted:     false,
	}, nil)

	// Mock IndexTokens for single chunk (15 tokens)
	s.env.OnWorkflow(workerCore.IndexTokens, mock.Anything, mock.MatchedBy(func(cids []domain.TokenCID) bool {
		return len(cids) == 15
	}), mock.Anything).Return(nil)

	// Mock IncrementTokensIndexed
	s.env.OnActivity(s.executor.IncrementTokensIndexed, mock.Anything, address, chainID, 15).Return(nil)

	// Mock UpdateIndexingBlockRangeForAddress
	s.env.OnActivity(s.executor.UpdateIndexingBlockRangeForAddress, mock.Anything, address, chainID, mock.Anything, mock.Anything).Return(nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(workerCore.IndexTezosTokenOwner, address, (*string)(nil))

	// Should complete successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *IndexOwnerWorkflowTestSuite) TestIndexTezosTokenOwner_BudgetedMode_PartialQuota() {
	// Setup worker core with budgeted mode enabled
	workerCore := workflows.NewWorkerCore(s.executor, workflows.WorkerCoreConfig{
		TezosChainID:                      domain.ChainTezosMainnet,
		EthereumChainID:                   domain.ChainEthereumMainnet,
		EthereumTokenSweepStartBlock:      1000,
		TezosTokenSweepStartBlock:         1000,
		MediaTaskQueue:                    "media-task-queue",
		BudgetedIndexingModeEnabled:       true,
		BudgetedIndexingDefaultDailyQuota: 1000,
	}, s.blacklist, s.temporalWorkflow)

	address := "tz1partial"
	chainID := domain.ChainTezosMainnet
	latestBlock := uint64(3000)

	// Create 60 tokens in one chunk (more than remaining quota)
	tokens := make([]domain.TokenWithBlock, 60)
	for i := range 60 {
		tokens[i] = domain.TokenWithBlock{
			TokenCID:    domain.TokenCID(fmt.Sprintf("tezos:mainnet:fa2:KT1partial:%d", i)),
			BlockNumber: uint64(2000 + i*10), //nolint:gosec,G115
		}
	}

	// Other activities succeed
	s.env.OnActivity(s.executor.EnsureWatchedAddressExists, mock.Anything, address, chainID, mock.Anything).Return(nil)
	s.env.OnActivity(s.executor.GetIndexingBlockRangeForAddress, mock.Anything, address, chainID).
		Return(&workflows.BlockRangeResult{MinBlock: 0, MaxBlock: 0}, nil)
	s.env.OnActivity(s.executor.GetLatestTezosBlock, mock.Anything).Return(latestBlock, nil)
	s.env.OnActivity(s.executor.GetTezosTokenCIDsByAccountWithinBlockRange, mock.Anything, address, uint64(1000), latestBlock).
		Return(tokens, nil)

	// Quota check - only 30 remaining (less than chunk size of 50)
	s.env.OnActivity(s.executor.GetQuotaInfo, mock.Anything, address, chainID).Return(&store.QuotaInfo{
		RemainingQuota:     30,
		TotalQuota:         1000,
		TokensIndexedToday: 970,
		QuotaExhausted:     false,
	}, nil).Once()

	// Mock IndexTokens - should only process 30 tokens (limited by remaining quota)
	s.env.OnWorkflow(workerCore.IndexTokens, mock.Anything, mock.MatchedBy(func(cids []domain.TokenCID) bool {
		return len(cids) == 30 // Limited to remaining quota
	}), mock.Anything).Return(nil).Once()

	// Mock IncrementTokensIndexed for 30 tokens
	s.env.OnActivity(s.executor.IncrementTokensIndexed, mock.Anything, address, chainID, 30).Return(nil).Once()

	// Mock UpdateIndexingBlockRangeForAddress
	s.env.OnActivity(s.executor.UpdateIndexingBlockRangeForAddress, mock.Anything, address, chainID, mock.Anything, mock.Anything).Return(nil).Once()

	// Second quota check - now exhausted
	s.env.OnActivity(s.executor.GetQuotaInfo, mock.Anything, address, chainID).Return(&store.QuotaInfo{
		RemainingQuota:     0,
		TotalQuota:         1000,
		TokensIndexedToday: 1000,
		QuotaExhausted:     true,
		QuotaResetAt:       s.env.Now().Add(24 * time.Hour),
	}, nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(workerCore.IndexTezosTokenOwner, address, (*string)(nil))

	// Should complete
	s.True(s.env.IsWorkflowCompleted())
}

// ====================================================================================
// IndexEthereumTokenOwner Tests
// ====================================================================================

func (s *IndexOwnerWorkflowTestSuite) TestIndexEthereumTokenOwner_FirstRun_WithTokens() {
	address := "0x1234567890123456789012345678901234567890"
	chainID := domain.ChainEthereumMainnet
	latestBlock := uint64(5000)

	startBlock := uint64(1000) // EthereumTokenSweepStartBlock from config

	// Create test tokens
	tokens := make([]domain.TokenWithBlock, 15)
	for i := range tokens {
		tokens[i] = domain.TokenWithBlock{
			TokenCID:    domain.NewTokenCID(chainID, domain.StandardERC721, "0xContract123456789012345678901234567890", fmt.Sprintf("%d", i)),
			BlockNumber: uint64(2000 + i*10), //nolint:gosec,G115
		}
	}

	// Other activities succeed
	s.env.OnActivity(s.executor.EnsureWatchedAddressExists, mock.Anything, address, chainID, mock.Anything).Return(nil)
	s.env.OnActivity(s.executor.GetIndexingBlockRangeForAddress, mock.Anything, address, chainID).Return(&workflows.BlockRangeResult{MinBlock: 0, MaxBlock: 0}, nil)
	s.env.OnActivity(s.executor.GetLatestEthereumBlock, mock.Anything).Return(latestBlock, nil)

	// Verify GetEthereumTokenCIDsByOwnerWithinBlockRange called with correct range
	s.env.OnActivity(s.executor.GetEthereumTokenCIDsByOwnerWithinBlockRange, mock.Anything, address, startBlock, latestBlock).Return(tokens, nil)

	// Track block range updates
	var blockRangeUpdates []struct {
		minBlock uint64
		maxBlock uint64
	}
	s.env.OnActivity(s.executor.UpdateIndexingBlockRangeForAddress, mock.Anything, address, chainID, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, addr string, chain domain.Chain, minBlock uint64, maxBlock uint64) error {
			blockRangeUpdates = append(blockRangeUpdates, struct {
				minBlock uint64
				maxBlock uint64
			}{minBlock, maxBlock})
			return nil
		},
	)

	// Mock IndexTokens child workflow
	s.env.OnWorkflow(s.workerCore.IndexTokens, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexEthereumTokenOwner, address, (*string)(nil))

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())

	// Verify block range updates
	s.GreaterOrEqual(len(blockRangeUpdates), 1, "Should have at least 1 block range update")

	// All updates should maintain the latest block (5000)
	for _, update := range blockRangeUpdates {
		s.Equal(latestBlock, update.maxBlock, "All updates should maintain stored max_block")
	}

	// Final update: should set min block to start block
	s.Equal(startBlock, blockRangeUpdates[len(blockRangeUpdates)-1].minBlock, "Final update should set min_block to start block")
}

func (s *IndexOwnerWorkflowTestSuite) TestIndexEthereumTokenOwner_JobProgressTracking() {
	address := "0x1234567890123456789012345678901234567890"
	chainID := domain.ChainEthereumMainnet
	latestBlock := uint64(10000)
	startBlock := uint64(1000)
	jobID := "test-job-123" // Provide a job ID for progress tracking

	// Create 100 tokens to test progress tracking across multiple chunks
	tokens := make([]domain.TokenWithBlock, 100)
	for i := range tokens {
		tokens[i] = domain.TokenWithBlock{
			TokenCID:    domain.NewTokenCID(chainID, domain.StandardERC721, "0xContractAddress", fmt.Sprintf("%d", i)),
			BlockNumber: uint64(5000 + i*10), //nolint:gosec,G115
		}
	}

	// Other activities succeed
	s.env.OnActivity(s.executor.EnsureWatchedAddressExists, mock.Anything, address, chainID, mock.Anything).Return(nil)
	s.env.OnActivity(s.executor.GetIndexingBlockRangeForAddress, mock.Anything, address, chainID).Return(&workflows.BlockRangeResult{MinBlock: 0, MaxBlock: 0}, nil)
	s.env.OnActivity(s.executor.GetLatestEthereumBlock, mock.Anything).Return(latestBlock, nil)
	s.env.OnActivity(s.executor.GetEthereumTokenCIDsByOwnerWithinBlockRange, mock.Anything, address, startBlock, latestBlock).Return(tokens, nil)
	s.env.OnActivity(s.executor.UpdateIndexingBlockRangeForAddress, mock.Anything, address, chainID, mock.Anything, mock.Anything).Return(nil)
	s.env.OnWorkflow(s.workerCore.IndexTokens, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Track progress updates
	var progressUpdates []int
	s.env.OnActivity(s.executor.UpdateIndexingJobProgress, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, workflowID string, tokensProcessed int, minBlock, maxBlock uint64) error {
			progressUpdates = append(progressUpdates, tokensProcessed)
			return nil
		},
	)

	// Execute the workflow with a jobID to enable progress tracking
	s.env.ExecuteWorkflow(s.workerCore.IndexEthereumTokenOwner, address, &jobID)

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())

	// Verify progress tracking: 100 tokens with chunk size 50 = 2 chunks
	s.Equal(2, len(progressUpdates), "Should have 2 progress updates")
	s.Equal(50, progressUpdates[0], "First chunk should process 50 tokens")
	s.Equal(50, progressUpdates[1], "Second chunk should process 50 tokens")
}

func (s *IndexOwnerWorkflowTestSuite) TestIndexEthereumTokenOwner_JobTrackingGracefulFailure() {
	address := "0x1234567890123456789012345678901234567890"
	chainID := domain.ChainEthereumMainnet
	latestBlock := uint64(5000)
	startBlock := uint64(1000)
	jobID := "test-job-123" // Provide a job ID for progress tracking

	tokens := make([]domain.TokenWithBlock, 10)
	for i := range tokens {
		tokens[i] = domain.TokenWithBlock{
			TokenCID:    domain.NewTokenCID(chainID, domain.StandardERC721, "0xContract123456789012345678901234567890", fmt.Sprintf("%d", i)),
			BlockNumber: uint64(2000 + i*10), //nolint:gosec,G115
		}
	}

	// Job tracking fails, but workflow should continue
	s.env.OnActivity(s.executor.UpdateIndexingJobProgress, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("job tracking error"))

	// Other activities succeed
	s.env.OnActivity(s.executor.EnsureWatchedAddressExists, mock.Anything, address, chainID, mock.Anything).Return(nil)
	s.env.OnActivity(s.executor.GetIndexingBlockRangeForAddress, mock.Anything, address, chainID).Return(&workflows.BlockRangeResult{MinBlock: 0, MaxBlock: 0}, nil)
	s.env.OnActivity(s.executor.GetLatestEthereumBlock, mock.Anything).Return(latestBlock, nil)
	s.env.OnActivity(s.executor.GetEthereumTokenCIDsByOwnerWithinBlockRange, mock.Anything, address, startBlock, latestBlock).Return(tokens, nil)
	s.env.OnActivity(s.executor.UpdateIndexingBlockRangeForAddress, mock.Anything, address, chainID, mock.Anything, mock.Anything).Return(nil)
	s.env.OnWorkflow(s.workerCore.IndexTokens, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute the workflow with a jobID to test job tracking failure handling
	s.env.ExecuteWorkflow(s.workerCore.IndexEthereumTokenOwner, address, &jobID)

	// Verify workflow completed successfully despite job tracking failures
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError(), "Workflow should succeed even if job tracking fails")
}

func (s *IndexOwnerWorkflowTestSuite) TestIndexEthereumTokenOwner_SubsequentRun_ForwardSweep() {
	address := "0x1234567890123456789012345678901234567890"
	chainID := domain.ChainEthereumMainnet
	storedMinBlock := uint64(1000) // Already at start block

	storedMaxBlock := uint64(3000)
	latestBlock := uint64(5000)

	// Create forward tokens - 51 tokens to test chunking (50 + 1)
	forwardTokens := make([]domain.TokenWithBlock, 51)
	for i := range forwardTokens {
		forwardTokens[i] = domain.TokenWithBlock{
			TokenCID:    domain.NewTokenCID(chainID, domain.StandardERC721, "0xContract123456789012345678901234567890", fmt.Sprintf("%d", i)),
			BlockNumber: uint64(3500 + i*10), //nolint:gosec,G115
		}
	}

	// Other activities succeed
	s.env.OnActivity(s.executor.EnsureWatchedAddressExists, mock.Anything, address, chainID, mock.Anything).Return(nil)
	s.env.OnActivity(s.executor.GetIndexingBlockRangeForAddress, mock.Anything, address, chainID).Return(&workflows.BlockRangeResult{MinBlock: storedMinBlock, MaxBlock: storedMaxBlock}, nil)
	s.env.OnActivity(s.executor.GetLatestEthereumBlock, mock.Anything).Return(latestBlock, nil)

	// Verify forward sweep: from 3001 (storedMaxBlock + 1) to 5000 (latestBlock)
	s.env.OnActivity(s.executor.GetEthereumTokenCIDsByOwnerWithinBlockRange, mock.Anything, address, storedMaxBlock+1, latestBlock).Return(forwardTokens, nil)

	// Track block range updates to verify progressive max_block updates
	var blockRangeUpdates []struct {
		minBlock uint64
		maxBlock uint64
	}
	s.env.OnActivity(s.executor.UpdateIndexingBlockRangeForAddress, mock.Anything, address, chainID, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, addr string, chain domain.Chain, minBlock uint64, maxBlock uint64) error {
			blockRangeUpdates = append(blockRangeUpdates, struct {
				minBlock uint64
				maxBlock uint64
			}{minBlock, maxBlock})
			return nil
		},
	)

	// Track IndexTokens calls to verify chunking (21 tokens -> 2 chunks)
	var indexTokensCalls int
	s.env.OnWorkflow(s.workerCore.IndexTokens, mock.Anything, mock.Anything, mock.Anything).Return(
		func(ctx workflow.Context, tokenCIDs []domain.TokenCID, address *string) error {
			indexTokensCalls++
			return nil
		},
	)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexEthereumTokenOwner, address, (*string)(nil))

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())

	// Verify chunking: 51 tokens -> 2 chunks (50 + 1)
	s.Equal(2, indexTokensCalls, "Should have 2 chunks for 51 tokens")

	// Verify block range updates - forward sweep updates max_block progressively
	// Should have: chunk1 update, chunk2 update, final update (at least 3)
	s.GreaterOrEqual(len(blockRangeUpdates), 3, "Should have at least 3 block range updates for 2 chunks")

	// All updates should maintain stored min_block
	for _, update := range blockRangeUpdates {
		s.Equal(storedMinBlock, update.minBlock, "All updates should maintain stored min_block")
	}

	// Final update should have max_block = latestBlock
	finalUpdate := blockRangeUpdates[len(blockRangeUpdates)-1]
	s.Equal(latestBlock, finalUpdate.maxBlock, "Final update should set max_block to latest block")
}

func (s *IndexOwnerWorkflowTestSuite) TestIndexEthereumTokenOwner_IndexTokenChunkError() {
	address := "0x1234567890123456789012345678901234567890"
	latestBlock := uint64(5000)

	// Create test tokens
	tokens := make([]domain.TokenWithBlock, 5)
	for i := range tokens {
		tokens[i] = domain.TokenWithBlock{
			TokenCID:    domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0xContract123456789012345678901234567890", fmt.Sprintf("%d", i)),
			BlockNumber: uint64(2000 + i*10), //nolint:gosec,G115
		}
	}

	// Other activities succeed
	s.env.OnActivity(s.executor.EnsureWatchedAddressExists, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.executor.GetIndexingBlockRangeForAddress, mock.Anything, mock.Anything, mock.Anything).Return(&workflows.BlockRangeResult{MinBlock: 0, MaxBlock: 0}, nil)
	s.env.OnActivity(s.executor.GetLatestEthereumBlock, mock.Anything).Return(latestBlock, nil)
	s.env.OnActivity(s.executor.GetEthereumTokenCIDsByOwnerWithinBlockRange, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(tokens, nil)

	// Mock IndexTokens child workflow to fail
	s.env.OnWorkflow(s.workerCore.IndexTokens, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("indexing failed"))

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexEthereumTokenOwner, address, (*string)(nil))

	// Verify workflow completed with error
	s.True(s.env.IsWorkflowCompleted())
	s.Error(s.env.GetWorkflowError())
	s.Contains(s.env.GetWorkflowError().Error(), "indexing failed")
}

func (s *IndexOwnerWorkflowTestSuite) TestIndexEthereumTokenOwner_UpdateBlockRangeError() {
	address := "0x1234567890123456789012345678901234567890"
	chainID := domain.ChainEthereumMainnet
	latestBlock := uint64(5000)

	// Create test tokens
	tokens := make([]domain.TokenWithBlock, 5)
	for i := range tokens {
		tokens[i] = domain.TokenWithBlock{
			TokenCID:    domain.NewTokenCID(chainID, domain.StandardERC721, "0xContract123456789012345678901234567890", fmt.Sprintf("%d", i)),
			BlockNumber: uint64(2000 + i*10), //nolint:gosec,G115
		}
	}

	// Other activities succeed
	s.env.OnActivity(s.executor.EnsureWatchedAddressExists, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.executor.GetIndexingBlockRangeForAddress, mock.Anything, mock.Anything, mock.Anything).Return(&workflows.BlockRangeResult{MinBlock: 0, MaxBlock: 0}, nil)
	s.env.OnActivity(s.executor.GetLatestEthereumBlock, mock.Anything).Return(latestBlock, nil)
	s.env.OnActivity(s.executor.GetEthereumTokenCIDsByOwnerWithinBlockRange, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(tokens, nil)

	// Mock IndexTokens child workflow
	s.env.OnWorkflow(s.workerCore.IndexTokens, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Mock UpdateIndexingBlockRangeForAddress to fail
	s.env.OnActivity(s.executor.UpdateIndexingBlockRangeForAddress, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("database error"))

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexEthereumTokenOwner, address, (*string)(nil))

	// Verify workflow completed with error
	s.True(s.env.IsWorkflowCompleted())
	s.Error(s.env.GetWorkflowError())
	s.Contains(s.env.GetWorkflowError().Error(), "database error")
}

func (s *IndexOwnerWorkflowTestSuite) TestIndexEthereumTokenOwner_SubsequentRun_AlreadyUpToDate() {
	address := "0x1234567890123456789012345678901234567890"
	chainID := domain.ChainEthereumMainnet
	storedMinBlock := uint64(1000) // Already at start block
	storedMaxBlock := uint64(5000) // Already at latest block
	latestBlock := uint64(5000)    // No new blocks

	// Other activities succeed
	s.env.OnActivity(s.executor.EnsureWatchedAddressExists, mock.Anything, address, chainID, mock.Anything).Return(nil)
	s.env.OnActivity(s.executor.GetIndexingBlockRangeForAddress, mock.Anything, address, chainID).Return(&workflows.BlockRangeResult{MinBlock: storedMinBlock, MaxBlock: storedMaxBlock}, nil)
	s.env.OnActivity(s.executor.GetLatestEthereumBlock, mock.Anything).Return(latestBlock, nil)

	// No fetching or indexing should occur

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexEthereumTokenOwner, address, (*string)(nil))

	// Verify workflow completed successfully (no-op)
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

// ====================================================================================
// IndexEthereumTokenOwner Tests - Budgeted Indexing Mode
// ====================================================================================

func (s *IndexOwnerWorkflowTestSuite) TestIndexEthereumTokenOwner_BudgetedMode_QuotaExhausted() {
	// Setup worker core with budgeted mode enabled and low quota
	workerCore := workflows.NewWorkerCore(s.executor, workflows.WorkerCoreConfig{
		TezosChainID:                      domain.ChainTezosMainnet,
		EthereumChainID:                   domain.ChainEthereumMainnet,
		EthereumTokenSweepStartBlock:      1000,
		TezosTokenSweepStartBlock:         1000,
		MediaTaskQueue:                    "media-task-queue",
		BudgetedIndexingModeEnabled:       true,
		BudgetedIndexingDefaultDailyQuota: 50,
	}, s.blacklist, s.temporalWorkflow)

	address := "0x1234567890123456789012345678901234567890"
	chainID := domain.ChainEthereumMainnet
	latestBlock := uint64(5000)

	// Create 100 tokens that would exceed quota
	tokens := make([]domain.TokenWithBlock, 100)
	for i := range 100 {
		tokens[i] = domain.TokenWithBlock{
			TokenCID:    domain.TokenCID(fmt.Sprintf("eip155:1:erc721:0xabc:%d", i)),
			BlockNumber: uint64(1000 + i), //nolint:gosec,G115
		}
	}

	// Other activities succeed
	s.env.OnActivity(s.executor.EnsureWatchedAddressExists, mock.Anything, address, chainID, mock.Anything).Return(nil)
	s.env.OnActivity(s.executor.GetIndexingBlockRangeForAddress, mock.Anything, address, chainID).
		Return(&workflows.BlockRangeResult{MinBlock: 0, MaxBlock: 0}, nil)
	s.env.OnActivity(s.executor.GetLatestEthereumBlock, mock.Anything).Return(latestBlock, nil)
	s.env.OnActivity(s.executor.GetEthereumTokenCIDsByOwnerWithinBlockRange, mock.Anything, address, uint64(1000), latestBlock).
		Return(tokens, nil)

	// First quota check - 50 tokens available
	s.env.OnActivity(s.executor.GetQuotaInfo, mock.Anything, address, chainID).Return(&store.QuotaInfo{
		RemainingQuota:     50,
		TotalQuota:         50,
		TokensIndexedToday: 0,
		QuotaExhausted:     false,
	}, nil).Once()

	// Mock IndexTokens for first chunk (50 tokens)
	s.env.OnWorkflow(workerCore.IndexTokens, mock.Anything, mock.MatchedBy(func(cids []domain.TokenCID) bool {
		return len(cids) == 50
	}), mock.Anything).Return(nil).Once()

	// Mock IncrementTokensIndexed
	s.env.OnActivity(s.executor.IncrementTokensIndexed, mock.Anything, address, chainID, 50).Return(nil).Once()

	// Mock UpdateIndexingBlockRangeForAddress - called once (after first chunk)
	s.env.OnActivity(s.executor.UpdateIndexingBlockRangeForAddress, mock.Anything, address, chainID, mock.Anything, mock.Anything).Return(nil).Once()

	// Second quota check - exhausted
	s.env.OnActivity(s.executor.GetQuotaInfo, mock.Anything, address, chainID).Return(&store.QuotaInfo{
		RemainingQuota:     0,
		TotalQuota:         50,
		TokensIndexedToday: 50,
		QuotaExhausted:     true,
		QuotaResetAt:       s.env.Now().Add(24 * time.Hour),
	}, nil).Once()

	// Execute the workflow
	s.env.ExecuteWorkflow(workerCore.IndexEthereumTokenOwner, address, (*string)(nil))

	// Should complete with continue-as-new error (special case - not a real failure)
	s.True(s.env.IsWorkflowCompleted())
	workflowErr := s.env.GetWorkflowError()
	s.Error(workflowErr, "Workflow should return continue-as-new error")
	s.Contains(workflowErr.Error(), "continue as new", "Error should be a continue-as-new error")
}

func (s *IndexOwnerWorkflowTestSuite) TestIndexEthereumTokenOwner_BudgetedMode_Normal() {
	// Setup worker core with budgeted mode enabled and sufficient quota
	workerCore := workflows.NewWorkerCore(s.executor, workflows.WorkerCoreConfig{
		TezosChainID:                      domain.ChainTezosMainnet,
		EthereumChainID:                   domain.ChainEthereumMainnet,
		EthereumTokenSweepStartBlock:      1000,
		TezosTokenSweepStartBlock:         1000,
		MediaTaskQueue:                    "media-task-queue",
		BudgetedIndexingModeEnabled:       true,
		BudgetedIndexingDefaultDailyQuota: 1000,
	}, s.blacklist, s.temporalWorkflow)

	address := "0xabcdef1234567890123456789012345678901234"
	chainID := domain.ChainEthereumMainnet
	latestBlock := uint64(3000)

	// Create 15 tokens (less than quota)
	tokens := make([]domain.TokenWithBlock, 15)
	for i := range 15 {
		tokens[i] = domain.TokenWithBlock{
			TokenCID:    domain.TokenCID(fmt.Sprintf("eip155:1:erc721:0xdef:%d", i)),
			BlockNumber: uint64(2000 + i*10), //nolint:gosec,G115
		}
	}

	// Other activities succeed
	s.env.OnActivity(s.executor.EnsureWatchedAddressExists, mock.Anything, address, chainID, mock.Anything).Return(nil)
	s.env.OnActivity(s.executor.GetIndexingBlockRangeForAddress, mock.Anything, address, chainID).
		Return(&workflows.BlockRangeResult{MinBlock: 0, MaxBlock: 0}, nil)
	s.env.OnActivity(s.executor.GetLatestEthereumBlock, mock.Anything).Return(latestBlock, nil)
	s.env.OnActivity(s.executor.GetEthereumTokenCIDsByOwnerWithinBlockRange, mock.Anything, address, uint64(1000), latestBlock).
		Return(tokens, nil)

	// Quota check - plenty available
	s.env.OnActivity(s.executor.GetQuotaInfo, mock.Anything, address, chainID).Return(&store.QuotaInfo{
		RemainingQuota:     1000,
		TotalQuota:         1000,
		TokensIndexedToday: 0,
		QuotaExhausted:     false,
	}, nil)

	// Mock IndexTokens
	s.env.OnWorkflow(workerCore.IndexTokens, mock.Anything, mock.MatchedBy(func(cids []domain.TokenCID) bool {
		return len(cids) == 15
	}), mock.Anything).Return(nil)

	// Mock IncrementTokensIndexed
	s.env.OnActivity(s.executor.IncrementTokensIndexed, mock.Anything, address, chainID, 15).Return(nil)

	// Mock UpdateIndexingBlockRangeForAddress
	s.env.OnActivity(s.executor.UpdateIndexingBlockRangeForAddress, mock.Anything, address, chainID, mock.Anything, mock.Anything).Return(nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(workerCore.IndexEthereumTokenOwner, address, (*string)(nil))

	// Should complete successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *IndexOwnerWorkflowTestSuite) TestIndexEthereumTokenOwner_BudgetedMode_PartialQuota() {
	// Setup worker core with budgeted mode enabled
	workerCore := workflows.NewWorkerCore(s.executor, workflows.WorkerCoreConfig{
		TezosChainID:                      domain.ChainTezosMainnet,
		EthereumChainID:                   domain.ChainEthereumMainnet,
		EthereumTokenSweepStartBlock:      1000,
		TezosTokenSweepStartBlock:         1000,
		MediaTaskQueue:                    "media-task-queue",
		BudgetedIndexingModeEnabled:       true,
		BudgetedIndexingDefaultDailyQuota: 1000,
	}, s.blacklist, s.temporalWorkflow)

	address := "0xfedcba9876543210987654321098765432109876"
	chainID := domain.ChainEthereumMainnet
	latestBlock := uint64(3000)

	// Create 60 tokens
	tokens := make([]domain.TokenWithBlock, 60)
	for i := range 60 {
		tokens[i] = domain.TokenWithBlock{
			TokenCID:    domain.TokenCID(fmt.Sprintf("eip155:1:erc721:0xfed:%d", i)),
			BlockNumber: uint64(2000 + i*10), //nolint:gosec,G115
		}
	}

	// Other activities succeed
	s.env.OnActivity(s.executor.EnsureWatchedAddressExists, mock.Anything, address, chainID, mock.Anything).Return(nil)
	s.env.OnActivity(s.executor.GetIndexingBlockRangeForAddress, mock.Anything, address, chainID).
		Return(&workflows.BlockRangeResult{MinBlock: 0, MaxBlock: 0}, nil)
	s.env.OnActivity(s.executor.GetLatestEthereumBlock, mock.Anything).Return(latestBlock, nil)
	s.env.OnActivity(s.executor.GetEthereumTokenCIDsByOwnerWithinBlockRange, mock.Anything, address, uint64(1000), latestBlock).
		Return(tokens, nil)

	// Quota check - only 30 remaining
	s.env.OnActivity(s.executor.GetQuotaInfo, mock.Anything, address, chainID).Return(&store.QuotaInfo{
		RemainingQuota:     30,
		TotalQuota:         1000,
		TokensIndexedToday: 970,
		QuotaExhausted:     false,
	}, nil).Once()

	// Mock IndexTokens - should only process 30 tokens
	s.env.OnWorkflow(workerCore.IndexTokens, mock.Anything, mock.MatchedBy(func(cids []domain.TokenCID) bool {
		return len(cids) == 30
	}), mock.Anything).Return(nil).Once()

	// Mock IncrementTokensIndexed
	s.env.OnActivity(s.executor.IncrementTokensIndexed, mock.Anything, address, chainID, 30).Return(nil).Once()

	// Mock UpdateIndexingBlockRangeForAddress
	s.env.OnActivity(s.executor.UpdateIndexingBlockRangeForAddress, mock.Anything, address, chainID, mock.Anything, mock.Anything).Return(nil).Once()

	// Second quota check - exhausted
	s.env.OnActivity(s.executor.GetQuotaInfo, mock.Anything, address, chainID).Return(&store.QuotaInfo{
		RemainingQuota:     0,
		TotalQuota:         1000,
		TokensIndexedToday: 1000,
		QuotaExhausted:     true,
		QuotaResetAt:       s.env.Now().Add(24 * time.Hour),
	}, nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(workerCore.IndexEthereumTokenOwner, address, (*string)(nil))

	// Should complete
	s.True(s.env.IsWorkflowCompleted())
}

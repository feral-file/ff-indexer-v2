package workflows_test

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/webhook"
	"github.com/feral-file/ff-indexer-v2/internal/workflows"
)

// IndexTokenWorkflowTestSuite is the test suite for token workflow tests
type IndexTokenWorkflowTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env        *testsuite.TestWorkflowEnvironment
	ctrl       *gomock.Controller
	executor   *mocks.MockCoreExecutor
	blacklist  *mocks.MockBlacklistRegistry
	workerCore workflows.WorkerCore
}

// SetupTest is called before each test
func (s *IndexTokenWorkflowTestSuite) SetupTest() {
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
func (s *IndexTokenWorkflowTestSuite) TearDownTest() {
	s.env.AssertExpectations(s.T())
	s.ctrl.Finish()
}

// TestIndexTokenWorkflowTestSuite runs the test suite
func TestIndexTokenWorkflowTestSuite(t *testing.T) {
	suite.Run(t, new(IndexTokenWorkflowTestSuite))
}

// ====================================================================================
// IndexTokenMint Tests
// ====================================================================================

func (s *IndexTokenWorkflowTestSuite) TestIndexTokenMint_Success() {
	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeMint,
		ToAddress:       stringPtr("0xtoaddr"),
		TxHash:          "0xabcd",
		BlockNumber:     100,
		Quantity:        "1",
	}
	tokenCID := event.TokenCID()

	// Mock blacklist check
	s.blacklist.EXPECT().IsTokenCIDBlacklisted(tokenCID).Return(false)

	// Mock CreateTokenMint activity
	s.env.OnActivity(s.executor.CreateTokenMint, mock.Anything, event).Return(nil)

	// Mock child workflow IndexTokenMetadata
	s.env.OnWorkflow(s.workerCore.IndexTokenMetadata, mock.Anything, tokenCID, (*string)(nil)).Return(nil)

	// Mock webhook notification workflow
	s.env.OnWorkflow(s.workerCore.NotifyWebhookClients, mock.Anything, mock.Anything).Return(func(ctx workflow.Context, evt webhook.WebhookEvent) error {
		s.Equal(webhook.EventTypeTokenOwnershipMinted, evt.EventType)
		s.IsType(map[string]interface{}{}, evt.Data)
		data := evt.Data.(map[string]interface{})
		// For mint events, from_address is typically nil (minted from nothing)
		s.Equal("1", data["quantity"])
		s.Equal("0xtoaddr", data["to_address"])
		return nil
	}, nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenMint, event)

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *IndexTokenWorkflowTestSuite) TestIndexTokenMint_Blacklisted() {
	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeMint,
		TxHash:          "0xabcd",
		BlockNumber:     100,
	}
	tokenCID := event.TokenCID()

	// Mock blacklist check - return true (blacklisted)
	s.blacklist.EXPECT().IsTokenCIDBlacklisted(tokenCID).Return(true)

	// No other activities should be called

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenMint, event)

	// Verify workflow completed successfully (skipped due to blacklist)
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *IndexTokenWorkflowTestSuite) TestIndexTokenMint_CreateTokenMintError() {
	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeMint,
		TxHash:          "0xabcd",
		BlockNumber:     100,
	}
	tokenCID := event.TokenCID()
	expectedError := errors.New("database error")

	// Mock blacklist check
	s.blacklist.EXPECT().IsTokenCIDBlacklisted(tokenCID).Return(false)

	// Track retry attempts - MaximumAttempts: 2 (1 retry)
	var activityCallCount int
	s.env.OnActivity(s.executor.CreateTokenMint, mock.Anything, event).Return(
		func(ctx context.Context, event *domain.BlockchainEvent) error {
			activityCallCount++
			return expectedError
		},
	)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenMint, event)

	// Verify workflow completed with error
	s.True(s.env.IsWorkflowCompleted())
	s.Error(s.env.GetWorkflowError())

	// Verify retries (MaximumAttempts: 2)
	s.Equal(2, activityCallCount, "Activity should be attempted 2 times (initial + 1 retry)")
}

func (s *IndexTokenWorkflowTestSuite) TestIndexTokenMint_ChildWorkflowStartFailure() {
	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeMint,
		TxHash:          "0xabcd",
		BlockNumber:     100,
	}
	tokenCID := event.TokenCID()

	// Mock blacklist check
	s.blacklist.EXPECT().IsTokenCIDBlacklisted(tokenCID).Return(false)

	// Mock CreateTokenMint activity
	s.env.OnActivity(s.executor.CreateTokenMint, mock.Anything, event).Return(nil)

	// Mock child workflow to fail at start
	// Note: ErrMockStartChildWorkflowFailed is special and prevents workflow invocation
	var childWorkflowCallCount int
	s.env.OnWorkflow(s.workerCore.IndexTokenMetadata, mock.Anything, tokenCID, (*string)(nil)).Return(
		func(ctx workflow.Context, tokenCID domain.TokenCID, address *string) error {
			childWorkflowCallCount++
			return testsuite.ErrMockStartChildWorkflowFailed
		},
	)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenMint, event)

	// Verify workflow completed with error
	s.True(s.env.IsWorkflowCompleted())
	s.Error(s.env.GetWorkflowError())
	s.Contains(s.env.GetWorkflowError().Error(), "start child workflow failed")

	s.Equal(1, childWorkflowCallCount, "Child workflow should be attempted 1 time (initial + 0 retries)")
}

// ====================================================================================
// IndexTokenTransfer Tests
// ====================================================================================

func (s *IndexTokenWorkflowTestSuite) TestIndexTokenTransfer_TokenExists() {
	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeTransfer,
		FromAddress:     stringPtr("0xfrom"),
		ToAddress:       stringPtr("0xto"),
		TxHash:          "0xabcd",
		BlockNumber:     100,
		Quantity:        "1",
	}
	tokenCID := event.TokenCID()

	// Mock blacklist check
	s.blacklist.EXPECT().IsTokenCIDBlacklisted(tokenCID).Return(false)

	// Mock CheckTokenExists activity - returns true
	s.env.OnActivity(s.executor.CheckTokenExists, mock.Anything, tokenCID).Return(true, nil)

	// Mock UpdateTokenTransfer activity
	s.env.OnActivity(s.executor.UpdateTokenTransfer, mock.Anything, event).Return(nil)

	// Mock webhook notification workflow
	s.env.OnWorkflow(s.workerCore.NotifyWebhookClients, mock.Anything, mock.Anything).Return(func(ctx workflow.Context, evt webhook.WebhookEvent) error {
		s.Equal(webhook.EventTypeTokenOwnershipTransferred, evt.EventType)
		s.IsType(map[string]interface{}{}, evt.Data)
		data := evt.Data.(map[string]interface{})
		s.Equal("0xfrom", data["from_address"])
		s.Equal("0xto", data["to_address"])
		s.Equal("1", data["quantity"])
		return nil
	}, nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenTransfer, event)

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *IndexTokenWorkflowTestSuite) TestIndexTokenTransfer_TokenDoesNotExist() {
	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeTransfer,
		FromAddress:     stringPtr("0xfrom"),
		ToAddress:       stringPtr("0xto"),
		TxHash:          "0xabcd",
		BlockNumber:     100,
	}
	tokenCID := event.TokenCID()

	// Mock blacklist check
	s.blacklist.EXPECT().IsTokenCIDBlacklisted(tokenCID).Return(false)

	// Mock CheckTokenExists activity - returns false
	s.env.OnActivity(s.executor.CheckTokenExists, mock.Anything, tokenCID).Return(false, nil)

	// Mock child workflow IndexTokenFromEvent
	s.env.OnWorkflow(s.workerCore.IndexTokenFromEvent, mock.Anything, event).Return(nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenTransfer, event)

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *IndexTokenWorkflowTestSuite) TestIndexTokenTransfer_Blacklisted() {
	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeTransfer,
		FromAddress:     stringPtr("0xfrom"),
		ToAddress:       stringPtr("0xto"),
		TxHash:          "0xabcd",
		BlockNumber:     100,
	}
	tokenCID := event.TokenCID()

	// Mock blacklist check - return true (blacklisted)
	s.blacklist.EXPECT().IsTokenCIDBlacklisted(tokenCID).Return(true)

	// No other activities should be called

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenTransfer, event)

	// Verify workflow completed successfully (skipped due to blacklist)
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *IndexTokenWorkflowTestSuite) TestIndexTokenTransfer_CheckTokenExistsError() {
	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeTransfer,
		FromAddress:     stringPtr("0xfrom"),
		ToAddress:       stringPtr("0xto"),
		TxHash:          "0xabcd",
		BlockNumber:     100,
	}
	tokenCID := event.TokenCID()
	expectedError := errors.New("database error")

	// Mock blacklist check
	s.blacklist.EXPECT().IsTokenCIDBlacklisted(tokenCID).Return(false)

	// Track retry attempts
	var activityCallCount int
	s.env.OnActivity(s.executor.CheckTokenExists, mock.Anything, tokenCID).Return(
		func(ctx context.Context, tokenCID domain.TokenCID) (bool, error) {
			activityCallCount++
			return false, expectedError
		},
	)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenTransfer, event)

	// Verify workflow completed with error
	s.True(s.env.IsWorkflowCompleted())
	s.Error(s.env.GetWorkflowError())

	// Verify retries
	s.Equal(2, activityCallCount, "Activity should be attempted 2 times (initial + 1 retry)")
}

func (s *IndexTokenWorkflowTestSuite) TestIndexTokenTransfer_IndexTokenFromEventError() {
	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeTransfer,
	}
	tokenCID := event.TokenCID()
	expectedError := errors.New("indexing error")

	// Mock blacklist check
	s.blacklist.EXPECT().IsTokenCIDBlacklisted(tokenCID).Return(false)

	// Mock CheckTokenExists activity - returns false
	s.env.OnActivity(s.executor.CheckTokenExists, mock.Anything, tokenCID).Return(false, nil)

	// Mock IndexTokenFromEvent activity - returns error
	var workflowCallCount int
	s.env.OnWorkflow(s.workerCore.IndexTokenFromEvent, mock.Anything, event).Return(
		func(ctx workflow.Context, event *domain.BlockchainEvent) error {
			workflowCallCount++
			return expectedError
		},
	)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenTransfer, event)

	// Verify workflow completed with error
	s.True(s.env.IsWorkflowCompleted())
	s.Error(s.env.GetWorkflowError())
	s.Contains(s.env.GetWorkflowError().Error(), "indexing error")

	// Verify retries
	s.Equal(1, workflowCallCount, "Workflow should be attempted 1 time (initial)")
}

// ====================================================================================
// IndexTokenBurn Tests
// ====================================================================================

func (s *IndexTokenWorkflowTestSuite) TestIndexTokenBurn_Success() {
	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeBurn,
		FromAddress:     stringPtr("0xfrom"),
		TxHash:          "0xabcd",
		BlockNumber:     100,
		Quantity:        "1",
	}
	tokenCID := event.TokenCID()

	// Mock blacklist check
	s.blacklist.EXPECT().IsTokenCIDBlacklisted(tokenCID).Return(false)

	// Mock CheckTokenExists activity - returns true
	s.env.OnActivity(s.executor.CheckTokenExists, mock.Anything, tokenCID).Return(true, nil)

	// Mock UpdateTokenBurn activity
	s.env.OnActivity(s.executor.UpdateTokenBurn, mock.Anything, event).Return(nil)

	// Mock webhook notification workflow
	s.env.OnWorkflow(s.workerCore.NotifyWebhookClients, mock.Anything, mock.Anything).Return(func(ctx workflow.Context, event webhook.WebhookEvent) error {
		s.Equal(webhook.EventTypeTokenOwnershipBurned, event.EventType)
		s.IsType(map[string]interface{}{}, event.Data)
		data := event.Data.(map[string]interface{})
		s.Equal("0xfrom", data["from_address"])
		s.Nil(data["to_address"])
		s.Equal("1", data["quantity"])
		return nil
	}, nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenBurn, event)

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *IndexTokenWorkflowTestSuite) TestIndexTokenBurn_TokenDoesNotExist() {
	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeBurn,
		FromAddress:     stringPtr("0xfrom"),
		TxHash:          "0xabcd",
		BlockNumber:     100,
	}
	tokenCID := event.TokenCID()

	// Mock blacklist check
	s.blacklist.EXPECT().IsTokenCIDBlacklisted(tokenCID).Return(false)

	// Mock CheckTokenExists activity - returns false
	s.env.OnActivity(s.executor.CheckTokenExists, mock.Anything, tokenCID).Return(false, nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenBurn, event)

	// Verify workflow completed with error (token doesn't exist)
	s.True(s.env.IsWorkflowCompleted())
	s.Error(s.env.GetWorkflowError())
	s.Contains(s.env.GetWorkflowError().Error(), "token doesn't exist")
}

func (s *IndexTokenWorkflowTestSuite) TestIndexTokenBurn_Blacklisted() {
	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeBurn,
		FromAddress:     stringPtr("0xfrom"),
		TxHash:          "0xabcd",
		BlockNumber:     100,
	}
	tokenCID := event.TokenCID()

	// Mock blacklist check - return true (blacklisted)
	s.blacklist.EXPECT().IsTokenCIDBlacklisted(tokenCID).Return(true)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenBurn, event)

	// Verify workflow completed successfully (skipped due to blacklist)
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *IndexTokenWorkflowTestSuite) TestIndexTokenBurn_CheckTokenExistsError() {
	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeBurn,
	}

	tokenCID := event.TokenCID()
	expectedError := errors.New("database error")

	// Mock blacklist check
	s.blacklist.EXPECT().IsTokenCIDBlacklisted(tokenCID).Return(false)

	// Mock CheckTokenExists activity - returns error
	var activityCallCount int
	s.env.OnActivity(s.executor.CheckTokenExists, mock.Anything, tokenCID).Return(
		func(ctx context.Context, tokenCID domain.TokenCID) (bool, error) {
			activityCallCount++
			return false, expectedError
		},
	)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenBurn, event)

	// Verify workflow completed with error
	s.True(s.env.IsWorkflowCompleted())
	s.Error(s.env.GetWorkflowError())
	s.Contains(s.env.GetWorkflowError().Error(), "database error")

	// Verify retries
	s.Equal(2, activityCallCount, "Activity should be attempted 2 times (initial + 1 retry)")
}

func (s *IndexTokenWorkflowTestSuite) TestIndexTokenBurn_UpdateTokenBurnError() {
	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeBurn,
	}
	tokenCID := event.TokenCID()
	expectedError := errors.New("database error")

	// Mock blacklist check
	s.blacklist.EXPECT().IsTokenCIDBlacklisted(tokenCID).Return(false)

	// Mock CheckTokenExists activity - returns true
	s.env.OnActivity(s.executor.CheckTokenExists, mock.Anything, tokenCID).Return(true, nil)

	// Mock UpdateTokenBurn activity - returns error
	var activityCallCount int
	s.env.OnActivity(s.executor.UpdateTokenBurn, mock.Anything, event).Return(
		func(ctx context.Context, event *domain.BlockchainEvent) error {
			activityCallCount++
			return expectedError
		},
	)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenBurn, event)

	// Verify workflow completed with error
	s.True(s.env.IsWorkflowCompleted())
	s.Error(s.env.GetWorkflowError())
	s.Contains(s.env.GetWorkflowError().Error(), "database error")

	// Verify retries
	s.Equal(2, activityCallCount, "Activity should be attempted 2 times (initial + 1 retry)")
}

// ====================================================================================
// IndexTokenFromEvent Tests
// ====================================================================================

func (s *IndexTokenWorkflowTestSuite) TestIndexTokenFromEvent_Success() {
	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeMint,
		TxHash:          "0xabcd",
		BlockNumber:     100,
	}
	tokenCID := event.TokenCID()

	// Mock blacklist check
	s.blacklist.EXPECT().IsTokenCIDBlacklisted(tokenCID).Return(false)

	// Mock IndexTokenWithMinimalProvenancesByBlockchainEvent activity
	s.env.OnActivity(s.executor.IndexTokenWithMinimalProvenancesByBlockchainEvent, mock.Anything, event).Return(nil)

	// Mock metadata child workflow
	s.env.OnWorkflow(s.workerCore.IndexTokenMetadata, mock.Anything, tokenCID, (*string)(nil)).Return(nil)

	// Mock provenance child workflow
	s.env.OnWorkflow(s.workerCore.IndexTokenProvenances, mock.Anything, tokenCID, (*string)(nil)).Return(nil)

	// Mock webhook notification workflow
	s.env.OnWorkflow(s.workerCore.NotifyWebhookClients, mock.Anything, mock.Anything).Return(nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenFromEvent, event)

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *IndexTokenWorkflowTestSuite) TestIndexTokenFromEvent_Blacklisted() {
	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeMint,
		TxHash:          "0xabcd",
		BlockNumber:     100,
	}
	tokenCID := event.TokenCID()

	// Mock blacklist check - return true (blacklisted)
	s.blacklist.EXPECT().IsTokenCIDBlacklisted(tokenCID).Return(true)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenFromEvent, event)

	// Verify workflow completed successfully (skipped due to blacklist)
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *IndexTokenWorkflowTestSuite) TestIndexTokenFromEvent_ActivityError() {
	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeMint,
		TxHash:          "0xabcd",
		BlockNumber:     100,
	}
	tokenCID := event.TokenCID()
	expectedError := errors.New("indexing error")

	// Mock blacklist check
	s.blacklist.EXPECT().IsTokenCIDBlacklisted(tokenCID).Return(false)

	// Track retry attempts
	var activityCallCount int
	s.env.OnActivity(s.executor.IndexTokenWithMinimalProvenancesByBlockchainEvent, mock.Anything, event).Return(
		func(ctx context.Context, event *domain.BlockchainEvent) error {
			activityCallCount++
			return expectedError
		},
	)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenFromEvent, event)

	// Verify workflow completed with error
	s.True(s.env.IsWorkflowCompleted())
	s.Error(s.env.GetWorkflowError())

	// Verify retries
	s.Equal(2, activityCallCount, "Activity should be attempted 2 times (initial + 1 retry)")
}

func (s *IndexTokenWorkflowTestSuite) TestIndexTokenFromEvent_MetadataWorkflowStartFailure_NonFatal() {
	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeMint,
		TxHash:          "0xabcd",
		BlockNumber:     100,
	}
	tokenCID := event.TokenCID()

	// Mock blacklist check
	s.blacklist.EXPECT().IsTokenCIDBlacklisted(tokenCID).Return(false)

	// Mock IndexTokenWithMinimalProvenancesByBlockchainEvent activity
	s.env.OnActivity(s.executor.IndexTokenWithMinimalProvenancesByBlockchainEvent, mock.Anything, event).Return(nil)

	// Mock metadata child workflow to fail - should not fail parent workflow
	var workflowCallCount int
	s.env.OnWorkflow(s.workerCore.IndexTokenMetadata, mock.Anything, tokenCID, (*string)(nil)).Return(
		func(ctx workflow.Context, tokenCID domain.TokenCID, address *string) error {
			workflowCallCount++
			return testsuite.ErrMockStartChildWorkflowFailed
		},
	)

	// Mock provenance child workflow
	s.env.OnWorkflow(s.workerCore.IndexTokenProvenances, mock.Anything, tokenCID, (*string)(nil)).Return(nil)

	// Mock webhook notification workflow
	s.env.OnWorkflow(s.workerCore.NotifyWebhookClients, mock.Anything, mock.Anything).Return(nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenFromEvent, event)

	// Verify workflow completed successfully (metadata failure is non-fatal)
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())

	// Verify retries
	s.Equal(1, workflowCallCount, "Workflow should be attempted 1 time (initial + 0 retries)")
}

func (s *IndexTokenWorkflowTestSuite) TestIndexTokenFromEvent_ProvenanceWorkflowStartFailure_NonFatal() {
	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeMint,
	}
	tokenCID := event.TokenCID()

	// Mock blacklist check
	s.blacklist.EXPECT().IsTokenCIDBlacklisted(tokenCID).Return(false)

	// Mock IndexTokenWithMinimalProvenancesByBlockchainEvent activity
	s.env.OnActivity(s.executor.IndexTokenWithMinimalProvenancesByBlockchainEvent, mock.Anything, event).Return(nil)

	// Mock metadata child workflow
	s.env.OnWorkflow(s.workerCore.IndexTokenMetadata, mock.Anything, tokenCID, (*string)(nil)).Return(nil)

	// Mock provenance child workflow to fail - should not fail parent workflow
	var workflowCallCount int
	s.env.OnWorkflow(s.workerCore.IndexTokenProvenances, mock.Anything, tokenCID, (*string)(nil)).Return(
		func(ctx workflow.Context, tokenCID domain.TokenCID, address *string) error {
			workflowCallCount++
			return testsuite.ErrMockStartChildWorkflowFailed
		},
	)

	// Mock webhook notification workflow
	s.env.OnWorkflow(s.workerCore.NotifyWebhookClients, mock.Anything, mock.Anything).Return(nil)
	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenFromEvent, event)

	// Verify workflow completed successfully (provenance failure is non-fatal)
	s.True(s.env.IsWorkflowCompleted())

	// Verify retries
	s.Equal(1, workflowCallCount, "Workflow should be attempted 2 times (initial + 1 retry)")
}

func (s *IndexTokenWorkflowTestSuite) TestIndexTokenFromEvent_ERC1155_WithOwner_SkipsFullProvenance() {
	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC1155,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeMint,
	}

	tokenCID := event.TokenCID()

	// Mock blacklist check
	s.blacklist.EXPECT().IsTokenCIDBlacklisted(tokenCID).Return(false)

	// Mock IndexTokenWithMinimalProvenancesByBlockchainEvent activity
	s.env.OnActivity(s.executor.IndexTokenWithMinimalProvenancesByBlockchainEvent, mock.Anything, event).Return(nil)

	// Mock metadata child workflow
	s.env.OnWorkflow(s.workerCore.IndexTokenMetadata, mock.Anything, tokenCID, (*string)(nil)).Return(nil)

	// Mock webhook notification workflow
	s.env.OnWorkflow(s.workerCore.NotifyWebhookClients, mock.Anything, mock.Anything).Return(nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenFromEvent, event)

	// There is NO IndexTokenProvenances workflow call for ERC1155

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

// ====================================================================================
// IndexTokens Tests
// ====================================================================================

func (s *IndexTokenWorkflowTestSuite) TestIndexTokens_Success() {
	tokenCIDs := []domain.TokenCID{
		domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1"),
		domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "2"),
		domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "3"),
	}

	// Mock child workflows for each token
	for _, tokenCID := range tokenCIDs {
		s.env.OnWorkflow(s.workerCore.IndexToken, mock.Anything, tokenCID, mock.Anything).Return(nil)
	}

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokens, tokenCIDs, nil)

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *IndexTokenWorkflowTestSuite) TestIndexTokens_OneChildWorkflowFails() {
	tokenCIDs := []domain.TokenCID{
		domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1"),
		domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "2"),
		domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "3"),
	}

	// Mock child workflows - one fails
	s.env.OnWorkflow(s.workerCore.IndexToken, mock.Anything, tokenCIDs[0], mock.Anything).Return(nil)
	s.env.OnWorkflow(s.workerCore.IndexToken, mock.Anything, tokenCIDs[1], mock.Anything).Return(errors.New("child workflow error"))
	s.env.OnWorkflow(s.workerCore.IndexToken, mock.Anything, tokenCIDs[2], mock.Anything).Return(nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokens, tokenCIDs, nil)

	// Verify workflow completed with error (fails when any child workflow fails)
	s.True(s.env.IsWorkflowCompleted())
	s.Error(s.env.GetWorkflowError())
	s.Contains(s.env.GetWorkflowError().Error(), "child workflow error")
}

// ====================================================================================
// IndexToken Tests
// ====================================================================================

func (s *IndexTokenWorkflowTestSuite) TestIndexToken_Success() {
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")

	// Mock blacklist check
	s.blacklist.EXPECT().IsTokenCIDBlacklisted(tokenCID).Return(false)

	// Mock IndexTokenWithMinimalProvenancesByTokenCID activity
	s.env.OnActivity(s.executor.IndexTokenWithMinimalProvenancesByTokenCID, mock.Anything, tokenCID, mock.Anything).Return(nil)

	// Mock webhook notification workflow - should be triggered for token.indexing.queryable event
	s.env.OnWorkflow(s.workerCore.NotifyWebhookClients, mock.Anything, mock.MatchedBy(func(event interface{}) bool {
		// Verify it's a webhook event with correct event type
		if webhookEvent, ok := event.(webhook.WebhookEvent); ok {
			return webhookEvent.EventType == webhook.EventTypeTokenIndexingQueryable
		}
		return false
	})).Return(nil)

	// Mock metadata child workflow
	s.env.OnWorkflow(s.workerCore.IndexTokenMetadata, mock.Anything, tokenCID, (*string)(nil)).Return(nil)

	// Mock provenance child workflow
	s.env.OnWorkflow(s.workerCore.IndexTokenProvenances, mock.Anything, tokenCID, (*string)(nil)).Return(nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexToken, tokenCID, nil)

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *IndexTokenWorkflowTestSuite) TestIndexToken_Blacklisted() {
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")

	// Mock blacklist check - return true (blacklisted)
	s.blacklist.EXPECT().IsTokenCIDBlacklisted(tokenCID).Return(true)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexToken, tokenCID, nil)

	// Verify workflow completed successfully (skipped due to blacklist)
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *IndexTokenWorkflowTestSuite) TestIndexToken_ActivityError() {
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")
	expectedError := errors.New("indexing error")

	// Mock blacklist check
	s.blacklist.EXPECT().IsTokenCIDBlacklisted(tokenCID).Return(false)

	// Track retry attempts
	var activityCallCount int
	s.env.OnActivity(s.executor.IndexTokenWithMinimalProvenancesByTokenCID, mock.Anything, tokenCID, mock.Anything).Return(
		func(ctx context.Context, tokenCID domain.TokenCID, address *string) error {
			activityCallCount++
			return expectedError
		},
	)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexToken, tokenCID, nil)

	// Verify workflow completed with error
	s.True(s.env.IsWorkflowCompleted())
	s.Error(s.env.GetWorkflowError())

	// Verify retries
	s.Equal(2, activityCallCount, "Activity should be attempted 2 times (initial + 1 retry)")
}

func (s *IndexTokenWorkflowTestSuite) TestIndexToken_ChildWorkflowStartFailure_NonFatal() {
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")

	// Mock blacklist check
	s.blacklist.EXPECT().IsTokenCIDBlacklisted(tokenCID).Return(false)

	// Mock IndexTokenWithMinimalProvenancesByTokenCID activity
	s.env.OnActivity(s.executor.IndexTokenWithMinimalProvenancesByTokenCID, mock.Anything, tokenCID, mock.Anything).Return(nil)

	// Mock metadata child workflow to fail - should not fail parent workflow
	var workflowCallCount int
	s.env.OnWorkflow(s.workerCore.IndexTokenMetadata, mock.Anything, tokenCID, (*string)(nil)).Return(
		func(ctx workflow.Context, tokenCID domain.TokenCID, address *string) error {
			workflowCallCount++
			return testsuite.ErrMockStartChildWorkflowFailed
		},
	)

	// Mock provenance child workflow to fail - should not fail parent workflow
	s.env.OnWorkflow(s.workerCore.IndexTokenProvenances, mock.Anything, tokenCID, (*string)(nil)).Return(testsuite.ErrMockStartChildWorkflowFailed)

	// Mock webhook notification workflow
	s.env.OnWorkflow(s.workerCore.NotifyWebhookClients, mock.Anything, mock.Anything).Return(nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexToken, tokenCID, nil)

	// Verify workflow completed successfully (child workflow failures are non-fatal)
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())

	// Verify retries
	s.Equal(1, workflowCallCount, "Workflow should be attempted 1 time (initial + 0 retries)")
}

func (s *IndexTokenWorkflowTestSuite) TestIndexToken_ProvenanceWorkflowStartFailure_NonFatal() {
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")

	// Mock blacklist check
	s.blacklist.EXPECT().IsTokenCIDBlacklisted(tokenCID).Return(false)

	// Mock IndexTokenWithMinimalProvenancesByTokenCID activity
	s.env.OnActivity(s.executor.IndexTokenWithMinimalProvenancesByTokenCID, mock.Anything, tokenCID, mock.Anything).Return(nil)

	// Mock webhook notification workflow - should be triggered for token.indexing.viewable event
	s.env.OnWorkflow(s.workerCore.NotifyWebhookClients, mock.Anything, mock.MatchedBy(func(event interface{}) bool {
		// Verify it's a webhook event with correct event type
		if webhookEvent, ok := event.(webhook.WebhookEvent); ok {
			return webhookEvent.EventType == webhook.EventTypeTokenIndexingQueryable
		}
		return false
	})).Return(nil)

	// Mock metadata child workflow
	s.env.OnWorkflow(s.workerCore.IndexTokenMetadata, mock.Anything, tokenCID, (*string)(nil)).Return(nil)

	// Mock provenance child workflow to fail - should not fail parent workflow
	var workflowCallCount int
	s.env.OnWorkflow(s.workerCore.IndexTokenProvenances, mock.Anything, tokenCID, (*string)(nil)).Return(
		func(ctx workflow.Context, tokenCID domain.TokenCID, address *string) error {
			workflowCallCount++
			return testsuite.ErrMockStartChildWorkflowFailed
		},
	)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexToken, tokenCID, nil)

	// Verify workflow completed successfully (provenance failure is non-fatal)
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())

	// Verify retries
	s.Equal(1, workflowCallCount, "Workflow should be attempted 2 times (initial + 1 retry)")
}

func (s *IndexTokenWorkflowTestSuite) TestIndexToken_ERC1155_WithOwner_SkipsFullProvenance() {
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC1155, "0x1234567890123456789012345678901234567890", "1")
	address := "0xowner123"

	// Mock blacklist check
	s.blacklist.EXPECT().IsTokenCIDBlacklisted(tokenCID).Return(false)

	// Mock IndexTokenWithMinimalProvenancesByTokenCID activity
	s.env.OnActivity(s.executor.IndexTokenWithMinimalProvenancesByTokenCID, mock.Anything, tokenCID, &address).Return(nil)

	// Mock metadata child workflow
	s.env.OnWorkflow(s.workerCore.IndexTokenMetadata, mock.Anything, tokenCID, &address).Return(nil)

	// Mock webhook notification workflow
	s.env.OnWorkflow(s.workerCore.NotifyWebhookClients, mock.Anything, mock.Anything).Return(nil)

	// DO NOT mock IndexTokenProvenances - it should not be called for ERC1155 with owner

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexToken, tokenCID, &address)

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *IndexTokenWorkflowTestSuite) TestIndexToken_ERC1155_WithoutOwner_RunsFullProvenance() {
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC1155, "0x1234567890123456789012345678901234567890", "1")

	// Mock blacklist check
	s.blacklist.EXPECT().IsTokenCIDBlacklisted(tokenCID).Return(false)

	// Mock IndexTokenWithMinimalProvenancesByTokenCID activity
	s.env.OnActivity(s.executor.IndexTokenWithMinimalProvenancesByTokenCID, mock.Anything, tokenCID, mock.Anything).Return(nil)

	// Mock metadata child workflow
	s.env.OnWorkflow(s.workerCore.IndexTokenMetadata, mock.Anything, tokenCID, (*string)(nil)).Return(nil)

	// Mock provenance child workflow - should be called for ERC1155 without owner
	s.env.OnWorkflow(s.workerCore.IndexTokenProvenances, mock.Anything, tokenCID, (*string)(nil)).Return(nil)

	// Mock webhook notification workflow
	s.env.OnWorkflow(s.workerCore.NotifyWebhookClients, mock.Anything, mock.Anything).Return(nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexToken, tokenCID, nil)

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *IndexTokenWorkflowTestSuite) TestIndexToken_TokenNotFoundOnChain_GracefullySkips() {
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")
	address := "0xowner123"

	// Mock blacklist check
	s.blacklist.EXPECT().IsTokenCIDBlacklisted(tokenCID).Return(false)

	// Mock IndexTokenWithMinimalProvenancesByTokenCID activity to return "token not found on chain" error
	s.env.OnActivity(s.executor.IndexTokenWithMinimalProvenancesByTokenCID, mock.Anything, tokenCID, &address).Return(domain.ErrTokenNotFoundOnChain)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexToken, tokenCID, &address)

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *IndexTokenWorkflowTestSuite) TestIndexToken_ERC1155_ContractUnreachable_GracefullySkips() {
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC1155, "0x1234567890123456789012345678901234567890", "1")
	address := "0xowner123"

	// Mock blacklist check
	s.blacklist.EXPECT().IsTokenCIDBlacklisted(tokenCID).Return(false)

	// Mock IndexTokenWithMinimalProvenancesByTokenCID activity to return "contract is unreachable" error
	s.env.OnActivity(s.executor.IndexTokenWithMinimalProvenancesByTokenCID, mock.Anything, tokenCID, &address).Return(domain.ErrContractUnreachable)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexToken, tokenCID, &address)

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

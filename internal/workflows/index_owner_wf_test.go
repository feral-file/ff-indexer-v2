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
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/workflows"
)

// IndexOwnerWorkflowTestSuite is the test suite for owner workflow tests
type IndexOwnerWorkflowTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env        *testsuite.TestWorkflowEnvironment
	ctrl       *gomock.Controller
	executor   *mocks.MockCoreExecutor
	blacklist  *mocks.MockBlacklistRegistry
	workerCore workflows.WorkerCore
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
	s.workerCore = workflows.NewWorkerCore(s.executor, workflows.WorkerCoreConfig{
		TezosChainID:                 domain.ChainTezosMainnet,
		EthereumChainID:              domain.ChainEthereumMainnet,
		EthereumTokenSweepStartBlock: 1000,
		TezosTokenSweepStartBlock:    1000,
		MediaTaskQueue:               "media-task-queue",
	}, s.blacklist)

	// Register activities with the test environment
	s.env.RegisterActivity(s.executor.EnsureWatchedAddressExists)
	s.env.RegisterActivity(s.executor.GetIndexingBlockRangeForAddress)
	s.env.RegisterActivity(s.executor.GetLatestTezosBlock)
	s.env.RegisterActivity(s.executor.GetLatestEthereumBlock)
	s.env.RegisterActivity(s.executor.GetTezosTokenCIDsByAccountWithinBlockRange)
	s.env.RegisterActivity(s.executor.GetEthereumTokenCIDsByOwnerWithinBlockRange)
	s.env.RegisterActivity(s.executor.UpdateIndexingBlockRangeForAddress)

	// Register child workflows
	s.env.RegisterWorkflow(s.workerCore.IndexTokenOwner)
	s.env.RegisterWorkflow(s.workerCore.IndexTezosTokenOwner)
	s.env.RegisterWorkflow(s.workerCore.IndexEthereumTokenOwner)
	s.env.RegisterWorkflow(s.workerCore.IndexTokens)
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

	// Mock Ethereum child workflow
	s.env.OnWorkflow(s.workerCore.IndexEthereumTokenOwner, mock.Anything, address).Return(nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenOwner, address)

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *IndexOwnerWorkflowTestSuite) TestIndexTokenOwner_TezosAddress() {
	address := "tz1VSUr8wwNhLAzempoch5d6hLRiTh8Cjcjb"

	// Mock Tezos child workflow
	s.env.OnWorkflow(s.workerCore.IndexTezosTokenOwner, mock.Anything, address).Return(nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenOwner, address)

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *IndexOwnerWorkflowTestSuite) TestIndexTokenOwner_UnknownAddress() {
	address := "someaddress"

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTokenOwner, address)

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.Error(s.env.GetWorkflowError())
	s.Contains(s.env.GetWorkflowError().Error(), "unsupported blockchain for address: someaddress")
}

// ====================================================================================
// IndexTezosTokenOwner Tests
// ====================================================================================

func (s *IndexOwnerWorkflowTestSuite) TestIndexTezosTokenOwner_FirstRun_WithTokens() {
	address := "tz1VSUr8wwNhLAzempoch5d6hLRiTh8Cjcjb"
	chainID := domain.ChainTezosMainnet
	latestBlock := uint64(5000)

	// Create test tokens (2 chunks of 20 tokens each for chunk size 20)
	tokens := make([]domain.TokenWithBlock, 25)
	for i := range tokens {
		tokens[i] = domain.TokenWithBlock{
			TokenCID:    domain.NewTokenCID(chainID, domain.StandardFA2, "KT1Contract", fmt.Sprintf("%d", i)),
			BlockNumber: uint64(2000 + i*10), //nolint:gosec,G115
		}
	}

	startBlock := uint64(1000) // TezosTokenSweepStartBlock from config

	// Verify EnsureWatchedAddressExists is called with correct params
	s.env.OnActivity(s.executor.EnsureWatchedAddressExists, mock.Anything, address, chainID).Return(nil)

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
		func(ctx workflow.Context, tokenCIDs []domain.TokenCID, ownerAddress *string) error {
			indexTokensCalls = append(indexTokensCalls, tokenCIDs)
			return nil
		},
	)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTezosTokenOwner, address)

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())

	// Verify chunking: 25 tokens -> 2 chunks (20 + 5)
	s.Equal(2, len(indexTokensCalls), "Should have 2 chunks")
	s.Equal(20, len(indexTokensCalls[0]), "First chunk should have 20 tokens")
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

func (s *IndexOwnerWorkflowTestSuite) TestIndexTezosTokenOwner_FirstRun_NoTokens() {
	address := "tz1VSUr8wwNhLAzempoch5d6hLRiTh8Cjcjb"
	latestBlock := uint64(5000)

	// Mock activities
	s.env.OnActivity(s.executor.EnsureWatchedAddressExists, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.executor.GetIndexingBlockRangeForAddress, mock.Anything, mock.Anything, mock.Anything).Return(&workflows.BlockRangeResult{MinBlock: 0, MaxBlock: 0}, nil)
	s.env.OnActivity(s.executor.GetLatestTezosBlock, mock.Anything).Return(latestBlock, nil)
	s.env.OnActivity(s.executor.GetTezosTokenCIDsByAccountWithinBlockRange, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]domain.TokenWithBlock{}, nil)

	// Mock final update
	s.env.OnActivity(s.executor.UpdateIndexingBlockRangeForAddress, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTezosTokenOwner, address)

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

	// Verify activities called with correct parameters
	s.env.OnActivity(s.executor.EnsureWatchedAddressExists, mock.Anything, address, chainID).Return(nil)
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
	s.env.ExecuteWorkflow(s.workerCore.IndexTezosTokenOwner, address)

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

	// Verify activities called with correct parameters
	s.env.OnActivity(s.executor.EnsureWatchedAddressExists, mock.Anything, address, chainID).Return(nil)
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
	s.env.ExecuteWorkflow(s.workerCore.IndexTezosTokenOwner, address)

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

	// Verify activities are called with correct parameters
	s.env.OnActivity(s.executor.EnsureWatchedAddressExists, mock.Anything, address, chainID).Return(nil)
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
		func(ctx workflow.Context, tokenCIDs []domain.TokenCID, ownerAddress *string) error {
			indexTokensCalls++
			return nil
		},
	)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTezosTokenOwner, address)

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
	s.env.OnActivity(s.executor.EnsureWatchedAddressExists, mock.Anything, address, chainID).Return(expectedError)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexTezosTokenOwner, address)

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
	s.env.OnActivity(s.executor.EnsureWatchedAddressExists, mock.Anything, address, chainID).Return(nil)
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
	s.env.ExecuteWorkflow(s.workerCore.IndexTezosTokenOwner, address)

	// Verify workflow completed with error
	s.True(s.env.IsWorkflowCompleted())
	s.Error(s.env.GetWorkflowError())

	// Verify retries (MaximumAttempts: 2 means initial + 1 retry)
	s.Equal(2, activityCallCount, "Activity should be attempted 2 times (initial + 1 retry)")
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

	// Verify activities called with correct parameters
	s.env.OnActivity(s.executor.EnsureWatchedAddressExists, mock.Anything, address, chainID).Return(nil)
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
	s.env.ExecuteWorkflow(s.workerCore.IndexEthereumTokenOwner, address)

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

func (s *IndexOwnerWorkflowTestSuite) TestIndexEthereumTokenOwner_SubsequentRun_ForwardSweep() {
	address := "0x1234567890123456789012345678901234567890"
	chainID := domain.ChainEthereumMainnet
	storedMinBlock := uint64(1000) // Already at start block
	storedMaxBlock := uint64(3000)
	latestBlock := uint64(5000)

	// Create forward tokens - 21 tokens to test chunking (20 + 1)
	forwardTokens := make([]domain.TokenWithBlock, 21)
	for i := range forwardTokens {
		forwardTokens[i] = domain.TokenWithBlock{
			TokenCID:    domain.NewTokenCID(chainID, domain.StandardERC721, "0xContract123456789012345678901234567890", fmt.Sprintf("%d", i)),
			BlockNumber: uint64(3500 + i*10), //nolint:gosec,G115
		}
	}

	// Verify activities called with correct parameters
	s.env.OnActivity(s.executor.EnsureWatchedAddressExists, mock.Anything, address, chainID).Return(nil)
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
		func(ctx workflow.Context, tokenCIDs []domain.TokenCID, ownerAddress *string) error {
			indexTokensCalls++
			return nil
		},
	)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexEthereumTokenOwner, address)

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())

	// Verify chunking: 21 tokens -> 2 chunks (20 + 1)
	s.Equal(2, indexTokensCalls, "Should have 2 chunks for 21 tokens")

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

	// Mock activities
	s.env.OnActivity(s.executor.EnsureWatchedAddressExists, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.executor.GetIndexingBlockRangeForAddress, mock.Anything, mock.Anything, mock.Anything).Return(&workflows.BlockRangeResult{MinBlock: 0, MaxBlock: 0}, nil)
	s.env.OnActivity(s.executor.GetLatestEthereumBlock, mock.Anything).Return(latestBlock, nil)
	s.env.OnActivity(s.executor.GetEthereumTokenCIDsByOwnerWithinBlockRange, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(tokens, nil)

	// Mock IndexTokens child workflow to fail
	s.env.OnWorkflow(s.workerCore.IndexTokens, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("indexing failed"))

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexEthereumTokenOwner, address)

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

	// Mock activities
	s.env.OnActivity(s.executor.EnsureWatchedAddressExists, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.executor.GetIndexingBlockRangeForAddress, mock.Anything, mock.Anything, mock.Anything).Return(&workflows.BlockRangeResult{MinBlock: 0, MaxBlock: 0}, nil)
	s.env.OnActivity(s.executor.GetLatestEthereumBlock, mock.Anything).Return(latestBlock, nil)
	s.env.OnActivity(s.executor.GetEthereumTokenCIDsByOwnerWithinBlockRange, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(tokens, nil)

	// Mock IndexTokens child workflow
	s.env.OnWorkflow(s.workerCore.IndexTokens, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Mock UpdateIndexingBlockRangeForAddress to fail
	s.env.OnActivity(s.executor.UpdateIndexingBlockRangeForAddress, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("database error"))

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexEthereumTokenOwner, address)

	// Verify workflow completed with error
	s.True(s.env.IsWorkflowCompleted())
	s.Error(s.env.GetWorkflowError())
	s.Contains(s.env.GetWorkflowError().Error(), "database error")
}

func (s *IndexOwnerWorkflowTestSuite) TestIndexEthereumTokenOwner_SubsequentRun_AlreadyUpToDate() {
	address := "0x1234567890123456789012345678901234567890"
	storedMinBlock := uint64(1000) // Already at start block
	storedMaxBlock := uint64(5000) // Already at latest block
	latestBlock := uint64(5000)    // No new blocks

	// Mock activities
	s.env.OnActivity(s.executor.EnsureWatchedAddressExists, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.executor.GetIndexingBlockRangeForAddress, mock.Anything, mock.Anything, mock.Anything).Return(&workflows.BlockRangeResult{MinBlock: storedMinBlock, MaxBlock: storedMaxBlock}, nil)
	s.env.OnActivity(s.executor.GetLatestEthereumBlock, mock.Anything).Return(latestBlock, nil)

	// No fetching or indexing should occur

	// Execute the workflow
	s.env.ExecuteWorkflow(s.workerCore.IndexEthereumTokenOwner, address)

	// Verify workflow completed successfully (no-op)
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

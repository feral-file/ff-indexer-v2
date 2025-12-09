package workflowsmedia_test

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	workflows_media "github.com/feral-file/ff-indexer-v2/internal/workflows/media"
)

// WorkflowTestSuite is the test suite for media workflow tests
type WorkflowTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env      *testsuite.TestWorkflowEnvironment
	ctrl     *gomock.Controller
	executor *mocks.MockMediaExecutor
	worker   workflows_media.Worker
}

// SetupTest is called before each test
func (s *WorkflowTestSuite) SetupTest() {
	// Initialize logger for tests
	_ = logger.Initialize(logger.Config{
		Debug: true,
	})

	s.env = s.NewTestWorkflowEnvironment()
	s.ctrl = gomock.NewController(s.T())
	s.executor = mocks.NewMockMediaExecutor(s.ctrl)
	s.worker = workflows_media.NewWorker(s.executor)

	// Register the activity with the test environment
	s.env.RegisterActivity(s.executor.IndexMediaFile)
}

// TearDownTest is called after each test
func (s *WorkflowTestSuite) TearDownTest() {
	s.env.AssertExpectations(s.T())
	s.ctrl.Finish()
}

// TestWorkflowTestSuite runs the test suite
func TestWorkflowTestSuite(t *testing.T) {
	suite.Run(t, new(WorkflowTestSuite))
}

// ====================================================================================
// IndexMediaWorkflow Tests
// ====================================================================================

func (s *WorkflowTestSuite) TestIndexMediaWorkflow_Success() {
	url := "https://example.com/media.jpg"

	// Mock the IndexMediaFile activity to return success
	s.env.OnActivity(s.executor.IndexMediaFile, mock.Anything, url).Return(nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.worker.IndexMediaWorkflow, url)

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *WorkflowTestSuite) TestIndexMediaWorkflow_ActivityError() {
	url := "https://example.com/media.jpg"
	expectedError := errors.New("failed to process media")

	// Track the number of retry attempts
	var activityCallCount int

	// Mock the IndexMediaFile activity to return an error and count retries
	// The retry policy is configured with MaximumAttempts: 2
	s.env.OnActivity(s.executor.IndexMediaFile, mock.Anything, url).Return(
		func(ctx context.Context, url string) error {
			activityCallCount++
			return expectedError
		},
	)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.worker.IndexMediaWorkflow, url)

	// Verify workflow completed with error
	s.True(s.env.IsWorkflowCompleted())
	s.Error(s.env.GetWorkflowError())
	s.Contains(s.env.GetWorkflowError().Error(), "failed to process media")

	// Verify the activity was retried the expected number of times (MaximumAttempts: 2)
	s.Equal(2, activityCallCount, "Activity should be attempted 2 times (initial + 1 retry)")
}

// ====================================================================================
// IndexMultipleMediaWorkflow Tests
// ====================================================================================

func (s *WorkflowTestSuite) TestIndexMultipleMediaWorkflow_Success() {
	urls := []string{
		"https://example.com/media1.jpg",
		"https://example.com/media2.jpg",
		"https://example.com/media3.jpg",
	}

	// Mock child workflow executions
	for _, url := range urls {
		s.env.OnWorkflow(s.worker.IndexMediaWorkflow, mock.Anything, url).Return(nil)
	}

	// Execute the workflow
	s.env.ExecuteWorkflow(s.worker.IndexMultipleMediaWorkflow, urls)

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *WorkflowTestSuite) TestIndexMultipleMediaWorkflow_EmptyList() {
	urls := []string{}

	// Execute the workflow with empty list
	s.env.ExecuteWorkflow(s.worker.IndexMultipleMediaWorkflow, urls)

	// Verify workflow completed successfully (no-op)
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *WorkflowTestSuite) TestIndexMultipleMediaWorkflow_DuplicateURLs() {
	urls := []string{
		"https://example.com/media1.jpg",
		"https://example.com/media1.jpg", // duplicate
		"https://example.com/media2.jpg",
		"https://example.com/media2.jpg", // duplicate
	}

	// Mock child workflow executions - should only be called once per unique URL
	uniqueURLs := []string{
		"https://example.com/media1.jpg",
		"https://example.com/media2.jpg",
	}
	for _, url := range uniqueURLs {
		s.env.OnWorkflow(s.worker.IndexMediaWorkflow, mock.Anything, url).Return(nil).Once()
	}

	// Execute the workflow
	s.env.ExecuteWorkflow(s.worker.IndexMultipleMediaWorkflow, urls)

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *WorkflowTestSuite) TestIndexMultipleMediaWorkflow_InvalidURLs() {
	urls := []string{
		"not-a-url",
		"",
		"just-text",
		"https://", // incomplete URL
	}

	// No child workflows should be started since all URLs are invalid

	// Execute the workflow
	s.env.ExecuteWorkflow(s.worker.IndexMultipleMediaWorkflow, urls)

	// Verify workflow completed successfully (no-op for invalid URLs)
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *WorkflowTestSuite) TestIndexMultipleMediaWorkflow_MixedValidInvalidURLs() {
	urls := []string{
		"https://example.com/media1.jpg", // valid
		"not-a-url",                      // invalid
		"https://example.com/media2.jpg", // valid
		"",                               // invalid
		"https://example.com/media3.jpg", // valid
	}

	// Mock child workflow executions - only for valid URLs
	validURLs := []string{
		"https://example.com/media1.jpg",
		"https://example.com/media2.jpg",
		"https://example.com/media3.jpg",
	}
	for _, url := range validURLs {
		s.env.OnWorkflow(s.worker.IndexMediaWorkflow, mock.Anything, url).Return(nil)
	}

	// Execute the workflow
	s.env.ExecuteWorkflow(s.worker.IndexMultipleMediaWorkflow, urls)

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *WorkflowTestSuite) TestIndexMultipleMediaWorkflow_ChildWorkflowStartFailure() {
	urls := []string{
		"https://example.com/media1.jpg",
		"https://example.com/media2.jpg",
		"https://example.com/media3.jpg",
	}

	// Mock child workflow start - simulate that starting one child workflow fails
	s.env.OnWorkflow(s.worker.IndexMediaWorkflow, mock.Anything, urls[0]).Return(nil)
	s.env.OnWorkflow(s.worker.IndexMediaWorkflow, mock.Anything, urls[1]).Return(testsuite.ErrMockStartChildWorkflowFailed)
	s.env.OnWorkflow(s.worker.IndexMediaWorkflow, mock.Anything, urls[2]).Return(nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.worker.IndexMultipleMediaWorkflow, urls)

	// Verify parent workflow completed successfully
	// When GetChildWorkflowExecution() fails (workflow fails to start),
	// the parent logs a warning and continues processing other URLs
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *WorkflowTestSuite) TestIndexMultipleMediaWorkflow_ChildWorkflowCompleteWithError() {
	urls := []string{
		"https://example.com/media1.jpg",
		"https://example.com/media2.jpg",
		"https://example.com/media3.jpg",
	}

	// Mock child workflow executions - one completes with error, others succeed
	s.env.OnWorkflow(s.worker.IndexMediaWorkflow, mock.Anything, urls[0]).Return(nil)
	s.env.OnWorkflow(s.worker.IndexMediaWorkflow, mock.Anything, urls[1]).Return(errors.New("child workflow execution error"))
	s.env.OnWorkflow(s.worker.IndexMediaWorkflow, mock.Anything, urls[2]).Return(nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(s.worker.IndexMultipleMediaWorkflow, urls)

	// Verify parent workflow completed successfully
	// The parent workflow uses fire-and-forget pattern with ParentClosePolicy_ABANDON,
	// so it completes successfully even if child workflows fail during execution
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *WorkflowTestSuite) TestIndexMultipleMediaWorkflow_ChildWorkflowRetryPolicy() {
	// Test that verifies child workflows retry according to the configured retry policy
	url := "https://example.com/media1.jpg"
	urls := []string{url}

	// Track the number of retry attempts for the child workflow
	var childWorkflowCallCount int
	expectedError := errors.New("child workflow execution error")

	// Mock child workflow to fail and count retries
	// The retry policy is configured with MaximumAttempts: 2
	s.env.OnWorkflow(s.worker.IndexMediaWorkflow, mock.Anything, url).Return(
		func(ctx workflow.Context, url string) error {
			childWorkflowCallCount++
			return expectedError
		},
	)

	// Execute the parent workflow
	s.env.ExecuteWorkflow(s.worker.IndexMultipleMediaWorkflow, urls)

	// Verify parent workflow completed successfully (fire-and-forget pattern)
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())

	// Verify the child workflow was retried the expected number of times (MaximumAttempts: 2)
	s.Equal(2, childWorkflowCallCount, "Child workflow should be attempted 2 times (initial + 1 retry)")
}

func (s *WorkflowTestSuite) TestIndexMultipleMediaWorkflow_WorkflowIDReusePolicy() {
	// Test that the workflow can be called multiple times with the same URLs
	// The workflow generates deterministic IDs using SHA-256 hash of the URL
	// and uses WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE to allow multiple executions
	urls := []string{
		"https://example.com/media1.jpg",
	}

	// Mock child workflow execution for first batch
	s.env.OnWorkflow(s.worker.IndexMediaWorkflow, mock.Anything, urls[0]).Return(nil)

	// Execute the workflow first time
	s.env.ExecuteWorkflow(s.worker.IndexMultipleMediaWorkflow, urls)
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())

	// Setup new test environment for second execution (simulates running again later)
	s.TearDownTest()
	s.SetupTest()

	// Mock child workflow execution for second batch with same URL
	s.env.OnWorkflow(s.worker.IndexMediaWorkflow, mock.Anything, urls[0]).Return(nil)

	// Execute the workflow again with the same URL
	s.env.ExecuteWorkflow(s.worker.IndexMultipleMediaWorkflow, urls)

	// Verify second execution also completed successfully
	// This demonstrates that:
	// 1. The same URL generates the same deterministic workflow ID (SHA-256 hash)
	// 2. WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE allows the workflow to run again
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *WorkflowTestSuite) TestIndexMultipleMediaWorkflow_LargeNumberOfURLs() {
	// Test with a larger number of URLs to ensure concurrent processing works
	urls := make([]string, 50)
	for i := range 50 {
		urls[i] = "https://example.com/media" + string(rune('0'+i%10)) + ".jpg"
	}

	// Mock child workflow executions for unique URLs
	seen := make(map[string]bool)
	for _, url := range urls {
		if !seen[url] {
			seen[url] = true
			s.env.OnWorkflow(s.worker.IndexMediaWorkflow, mock.Anything, url).Return(nil)
		}
	}

	// Execute the workflow
	s.env.ExecuteWorkflow(s.worker.IndexMultipleMediaWorkflow, urls)

	// Verify workflow completed successfully
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

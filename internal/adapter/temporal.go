package adapter

import (
	"context"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"
)

// Workflow defines an interface for workflow operations
//
//go:generate mockgen -source=temporal.go -destination=../mocks/temporal.go -package=mocks -mock_names=Workflow=MockWorkflow
type Workflow interface {
	// GetExecutionID returns the workflow execution ID
	GetExecutionID(ctx workflow.Context) string

	// GetRunID returns the workflow run ID
	GetRunID(ctx workflow.Context) string

	// GetCurrentHistoryLength returns the current length of history
	GetCurrentHistoryLength(ctx workflow.Context) int

	// GetParentWorkflowID returns the parent workflow ID
	GetParentWorkflowID(ctx workflow.Context) *string
}

// RealWorkflow implements Workflow using the standard workflow package
type RealWorkflow struct{}

// NewWorkflow creates a new real workflow implementation
func NewWorkflow() Workflow {
	return &RealWorkflow{}
}

// GetInfo returns the workflow info
func (w *RealWorkflow) GetInfo(ctx workflow.Context) *workflow.Info {
	return workflow.GetInfo(ctx)
}

// GetExecutionID returns the workflow execution ID
func (w *RealWorkflow) GetExecutionID(ctx workflow.Context) string {
	return workflow.GetInfo(ctx).WorkflowExecution.ID
}

// GetRunID returns the workflow run ID
func (w *RealWorkflow) GetRunID(ctx workflow.Context) string {
	return workflow.GetInfo(ctx).WorkflowExecution.RunID
}

// GetCurrentHistoryLength returns the current length of history
func (w *RealWorkflow) GetCurrentHistoryLength(ctx workflow.Context) int {
	return workflow.GetInfo(ctx).GetCurrentHistoryLength()
}

// GetParentWorkflowID returns the parent workflow ID
func (w *RealWorkflow) GetParentWorkflowID(ctx workflow.Context) *string {
	if info := workflow.GetInfo(ctx); info != nil && info.ParentWorkflowExecution != nil {
		return &info.ParentWorkflowExecution.ID
	}
	return nil
}

//go:generate mockgen -source=temporal.go -destination=../mocks/temporal.go -package=mocks -mock_names=Activity=MockActivity
type Activity interface {
	// GetInfo returns the activity info
	GetInfo(ctx context.Context) activity.Info
}

// RealActivity implements Activity using the standard activity package
type RealActivity struct{}

// NewActivity creates a new real activity implementation
func NewActivity() Activity {
	return &RealActivity{}
}

// GetInfo returns the activity info
func (a *RealActivity) GetInfo(ctx context.Context) activity.Info {
	return activity.GetInfo(ctx)
}

package logger

import (
	"go.temporal.io/sdk/workflow"
	"go.uber.org/zap"
)

// GetWorkflowInfo extracts workflow information from workflow.Context for Sentry tracking
// This is a helper function that workflows can use to get workflow info
// Returns nil if workflow info is not available
func GetWorkflowInfo(ctx workflow.Context) *WorkflowInfo {
	info := workflow.GetInfo(ctx)
	if info == nil {
		return nil
	}

	// Get workflow type name from WorkflowType struct
	workflowTypeName := info.WorkflowType.Name
	if workflowTypeName == "" {
		// Fallback: use "unknown" if name is not available
		workflowTypeName = "unknown"
	}

	return &WorkflowInfo{
		WorkflowType: workflowTypeName,
		WorkflowID:   info.WorkflowExecution.ID,
		RunID:        info.WorkflowExecution.RunID,
		Namespace:    info.Namespace,
		TaskQueue:    info.TaskQueueName,
	}
}

// FromWorkflow returns a logger with Sentry scope from workflow context
// This should be used in Temporal workflows to enable Sentry tracking
// Usage:
//
//	workflowInfo := logger.GetWorkflowInfo(ctx)
//	logger.FromWorkflow(ctx, workflowInfo).Info("Processing event", ...)
func FromWorkflow(ctx workflow.Context, info *WorkflowInfo) *zap.Logger {
	if info == nil {
		// Try to get info from context
		info = GetWorkflowInfo(ctx)
	}

	if info == nil {
		// No workflow info available, return default logger
		return log
	}

	// Use WithWorkflowInfo to create logger with Sentry context
	return WithWorkflowInfo(*info)
}

// InfoWf logs an info message with workflow context (shortcut for workflows)
func InfoWf(ctx workflow.Context, msg string, fields ...zap.Field) {
	info := GetWorkflowInfo(ctx)
	if info != nil {
		InfoWorkflow(*info, msg, fields...)
	} else {
		Info(msg, fields...)
	}
}

// ErrorWf logs an error message with workflow context (shortcut for workflows)
func ErrorWf(ctx workflow.Context, err error, fields ...zap.Field) {
	info := GetWorkflowInfo(ctx)
	if info != nil {
		ErrorWorkflow(*info, err, fields...)
	} else {
		Error(err, fields...)
	}
}

// WarnWf logs a warning message with workflow context (shortcut for workflows)
func WarnWf(ctx workflow.Context, msg string, fields ...zap.Field) {
	info := GetWorkflowInfo(ctx)
	if info != nil {
		WarnWorkflow(*info, msg, fields...)
	} else {
		Warn(msg, fields...)
	}
}

// DebugWf logs a debug message with workflow context (shortcut for workflows)
func DebugWf(ctx workflow.Context, msg string, fields ...zap.Field) {
	info := GetWorkflowInfo(ctx)
	if info != nil {
		DebugWorkflow(*info, msg, fields...)
	} else {
		Debug(msg, fields...)
	}
}

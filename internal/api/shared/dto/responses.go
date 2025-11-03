package dto

import "time"

// TriggerIndexingResponse represents the response for triggering indexing
type TriggerIndexingResponse struct {
	WorkflowID string `json:"workflow_id"`
	RunID      string `json:"run_id"`
}

// WorkflowStatusResponse represents the status of a Temporal workflow execution
type WorkflowStatusResponse struct {
	WorkflowID    string     `json:"workflow_id"`
	RunID         string     `json:"run_id"`
	Status        string     `json:"status"`
	StartTime     *time.Time `json:"start_time,omitempty"`
	CloseTime     *time.Time `json:"close_time,omitempty"`
	ExecutionTime *uint64    `json:"execution_time_ms,omitempty"` // Execution time in milliseconds
}

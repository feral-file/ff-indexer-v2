package dto

// TriggerIndexingResponse represents the response for triggering indexing
type TriggerIndexingResponse struct {
	WorkflowID string `json:"workflow_id"`
	RunID      string `json:"run_id"`
}

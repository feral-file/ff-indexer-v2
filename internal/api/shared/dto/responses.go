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

// CreateWebhookClientResponse represents the response for creating a webhook client
type CreateWebhookClientResponse struct {
	ClientID         string    `json:"client_id"`
	WebhookURL       string    `json:"webhook_url"`
	WebhookSecret    string    `json:"webhook_secret"`
	EventFilters     []string  `json:"event_filters"`
	IsActive         bool      `json:"is_active"`
	RetryMaxAttempts int       `json:"retry_max_attempts"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

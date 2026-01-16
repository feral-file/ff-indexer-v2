package dto

import "time"

// TriggerIndexingResponse represents the response for triggering indexing
type TriggerIndexingResponse struct {
	WorkflowID string `json:"workflow_id"`
	RunID      string `json:"run_id"`
}

// AddressIndexingJobInfo represents job information for a single address
type AddressIndexingJobInfo struct {
	Address    string `json:"address"`
	WorkflowID string `json:"workflow_id"`
}

// TriggerAddressIndexingResponse represents the response for triggering address indexing
type TriggerAddressIndexingResponse struct {
	Jobs []AddressIndexingJobInfo `json:"jobs"`
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

// AddressIndexingJobResponse represents an address indexing job
type AddressIndexingJobResponse struct {
	WorkflowID          string     `json:"workflow_id"`
	Address             string     `json:"address"`
	Chain               string     `json:"chain"`
	Status              string     `json:"status"`
	TokensProcessed     int        `json:"tokens_processed"`
	TotalTokensIndexed  *int       `json:"total_tokens_indexed,omitempty"`  // Optional: total tokens owned by address (all tokens in DB)
	TotalTokensViewable *int       `json:"total_tokens_viewable,omitempty"` // Optional: tokens with metadata or enrichment (ready for display)
	CurrentMinBlock     *uint64    `json:"current_min_block,omitempty"`
	CurrentMaxBlock     *uint64    `json:"current_max_block,omitempty"`
	StartedAt           time.Time  `json:"started_at"`
	PausedAt            *time.Time `json:"paused_at,omitempty"`
	CompletedAt         *time.Time `json:"completed_at,omitempty"`
	FailedAt            *time.Time `json:"failed_at,omitempty"`
	CanceledAt          *time.Time `json:"canceled_at,omitempty"`
}

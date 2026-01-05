package schema

import (
	"time"

	"gorm.io/datatypes"
)

// WebhookDeliveryStatus is the status of a webhook delivery
type WebhookDeliveryStatus string

const (
	// WebhookDeliveryStatusPending is the status of a webhook delivery that is pending
	WebhookDeliveryStatusPending WebhookDeliveryStatus = "pending"
	// WebhookDeliveryStatusSuccess is the status of a webhook delivery that was successful
	WebhookDeliveryStatusSuccess WebhookDeliveryStatus = "success"
	// WebhookDeliveryStatusFailed is the status of a webhook delivery that failed
	WebhookDeliveryStatusFailed WebhookDeliveryStatus = "failed"
)

// WebhookDelivery represents the webhook_deliveries table - audit log of webhook delivery attempts
type WebhookDelivery struct {
	// ID is an auto-incrementing sequence number
	ID uint64 `gorm:"column:id;primaryKey;autoIncrement"`
	// ClientID is the webhook client this delivery is for
	ClientID string `gorm:"column:client_id;not null;type:varchar(36)"`
	// EventID is a unique identifier for this event (ULID for time-sortable uniqueness)
	EventID string `gorm:"column:event_id;not null;type:varchar(255)"`
	// EventType is the type of event being delivered (e.g., "token.indexing.queryable")
	EventType string `gorm:"column:event_type;not null;type:varchar(50)"`
	// Payload is the complete webhook event payload as JSON
	Payload datatypes.JSON `gorm:"column:payload;not null;type:jsonb"`
	// WorkflowID is the Temporal workflow ID handling this delivery
	WorkflowID string `gorm:"column:workflow_id;not null;type:varchar(255)"`
	// WorkflowRunID is the Temporal run ID for this workflow execution
	WorkflowRunID string `gorm:"column:workflow_run_id;type:varchar(255)"`
	// DeliveryStatus indicates the current status: pending, success, failed
	DeliveryStatus WebhookDeliveryStatus `gorm:"column:delivery_status;not null;default:pending"`
	// Attempts is the number of delivery attempts made
	Attempts int `gorm:"column:attempts;not null;default:0"`
	// LastAttemptAt is the timestamp of the most recent delivery attempt
	LastAttemptAt *time.Time `gorm:"column:last_attempt_at;type:timestamptz"`
	// ResponseStatus is the HTTP status code from the webhook endpoint
	ResponseStatus *int `gorm:"column:response_status"`
	// ResponseBody is the response body from the webhook endpoint (limited to 4KB)
	ResponseBody string `gorm:"column:response_body;type:text"`
	// ErrorMessage contains error details if delivery failed
	ErrorMessage string `gorm:"column:error_message;type:text"`
	// CreatedAt is the timestamp when this delivery record was created
	CreatedAt time.Time `gorm:"column:created_at;not null;default:now();type:timestamptz"`
	// UpdatedAt is the timestamp when this delivery record was last updated
	UpdatedAt time.Time `gorm:"column:updated_at;not null;default:now();type:timestamptz"`
}

// TableName specifies the table name for the WebhookDelivery model
func (WebhookDelivery) TableName() string {
	return "webhook_deliveries"
}

package schema

import (
	"time"

	"gorm.io/datatypes"
)

// WebhookClient represents the webhook_clients table - registered webhook clients
type WebhookClient struct {
	// ID is an auto-incrementing sequence number
	ID uint64 `gorm:"column:id;primaryKey;autoIncrement"`
	// ClientID is a unique identifier for the webhook client (UUID)
	ClientID string `gorm:"column:client_id;not null;unique;type:varchar(36)"`
	// WebhookURL is the HTTPS endpoint where webhooks will be delivered
	WebhookURL string `gorm:"column:webhook_url;not null;type:text"`
	// WebhookSecret is the secret key used for HMAC-SHA256 signature generation
	WebhookSecret string `gorm:"column:webhook_secret;not null;type:text"`
	// EventFilters is a JSON array of event types this client wants to receive
	// Examples: ["token.queryable", "token.viewable"] or ["*"] for all events
	EventFilters datatypes.JSON `gorm:"column:event_filters;not null;type:jsonb"`
	// IsActive indicates whether this client should receive webhooks
	IsActive bool `gorm:"column:is_active;not null;default:true"`
	// RetryMaxAttempts is the maximum number of delivery attempts before giving up
	RetryMaxAttempts int `gorm:"column:retry_max_attempts;not null;default:5"`
	// CreatedAt is the timestamp when this client was registered
	CreatedAt time.Time `gorm:"column:created_at;not null;default:now();type:timestamptz"`
	// UpdatedAt is the timestamp when this client was last updated
	UpdatedAt time.Time `gorm:"column:updated_at;not null;default:now();type:timestamptz"`
}

// TableName specifies the table name for the WebhookClient model
func (WebhookClient) TableName() string {
	return "webhook_clients"
}

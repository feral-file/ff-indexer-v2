package webhook

import "time"

// Event type constants
const (
	// EventTypeTokenQueryable is fired when a token becomes queryable
	// (minimal ownership has been indexed, token can be queried by ID)
	EventTypeTokenQueryable = "token.queryable"

	// EventTypeTokenViewable is fired when a token becomes viewable
	// (metadata and enrichment have been completed, full token info available)
	EventTypeTokenViewable = "token.viewable"

	// EventTypeTokenProvenanceComplete is fired when full provenance has been indexed
	// (all historical transfers and events have been indexed)
	EventTypeTokenProvenanceComplete = "token.provenance.complete"

	// EventTypeWildcard is a special filter that matches all event types
	EventTypeWildcard = "*"
)

// WebhookEvent represents a webhook event to be delivered to clients
type WebhookEvent struct {
	// EventID is a unique identifier for this event (ULID for time-sortable uniqueness)
	EventID string `json:"event_id"`
	// EventType is the type of event (e.g., "token.queryable")
	EventType string `json:"event_type"`
	// Timestamp is when the event was generated
	Timestamp time.Time `json:"timestamp"`
	// Data contains the event-specific payload
	Data EventData `json:"data"`
}

// EventData contains the webhook event payload
type EventData struct {
	// TokenCID is the canonical token identifier (e.g., "eip155:1:erc721:0xabc...:1234")
	TokenCID string `json:"token_cid"`
	// Chain is the blockchain network (e.g., "eip155:1")
	Chain string `json:"chain"`
	// Standard is the token standard (e.g., "erc721", "erc1155", "fa2")
	Standard string `json:"standard"`
	// Contract is the contract address
	Contract string `json:"contract"`
	// TokenNumber is the token ID within the contract
	TokenNumber string `json:"token_number"`
}

// DeliveryResult represents the result of a webhook delivery attempt
type DeliveryResult struct {
	// Success indicates whether the delivery was successful
	Success bool
	// StatusCode is the HTTP status code returned by the webhook endpoint
	StatusCode int
	// Body is the response body (limited to 4KB)
	Body string
	// Error contains error details if delivery failed
	Error string
}

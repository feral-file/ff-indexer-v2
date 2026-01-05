package webhook

import "time"

// Event type constants
const (
	// EventTypeTokenIndexingQueryable is fired when a token becomes queryable
	// (minimal ownership has been indexed, token can be queried by ID)
	EventTypeTokenIndexingQueryable = "token.indexing.queryable" //nolint:gosec,G101

	// EventTypeTokenIndexingViewable is fired when a token becomes viewable
	// (metadata and enrichment have been completed, full token info available)
	EventTypeTokenIndexingViewable = "token.indexing.viewable" //nolint:gosec,G101

	// EventTypeTokenIndexingProvenanceCompleted is fired when full provenance has been indexed
	// (all historical transfers and events have been indexed)
	EventTypeTokenIndexingProvenanceCompleted = "token.indexing.provenance_completed" //nolint:gosec,G101

	// EventTypeTokenOwnershipMinted is fired when a token is minted
	// (token has been minted)
	EventTypeTokenOwnershipMinted = "token.ownership.minted"

	// EventTypeTokenOwnershipTransferred is fired when a token is transferred
	// (token has been transferred to a new owner)
	EventTypeTokenOwnershipTransferred = "token.ownership.transferred"

	// EventTypeTokenOwnershipBurned is fired when a token is burned
	// (token has been burned)
	EventTypeTokenOwnershipBurned = "token.ownership.burned"

	// EventTypeTokenIndexingMediaCompleted is fired when media has been indexed
	// EventTypeWildcard is a special filter that matches all event types
	EventTypeWildcard = "*"
)

// WebhookEvent represents a webhook event to be delivered to clients
type WebhookEvent struct {
	// EventID is a unique identifier for this event (ULID for time-sortable uniqueness)
	EventID string `json:"event_id"`
	// EventType is the type of event (e.g., "token.indexing.queryable")
	EventType string `json:"event_type"`
	// Timestamp is when the event was generated
	Timestamp time.Time `json:"timestamp"`
	// Data contains the event-specific payload
	Data interface{} `json:"data"`
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

// IndexingEventData contains the data for an indexing event
type IndexingEventData struct {
	// EventData is the common event data
	EventData

	// Address is the address associated with the event
	Address *string `json:"address,omitempty"`
}

// OwnershipEventData contains the data for an ownership event
type OwnershipEventData struct {
	// EventData is the common event data
	EventData

	// FromAddress is the address of the sender
	FromAddress *string `json:"from_address,omitempty"`
	// ToAddress is the address of the receiver
	ToAddress *string `json:"to_address,omitempty"`
	// Quantity is the number of tokens transferred
	Quantity string `json:"quantity"`
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

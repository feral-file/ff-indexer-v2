package dto

// TriggerIndexingRequest represents the request body for triggering indexing
type TriggerIndexingRequest struct {
	TokenCIDs []string `json:"token_cids,omitempty"`
	Addresses []string `json:"addresses,omitempty"`
}

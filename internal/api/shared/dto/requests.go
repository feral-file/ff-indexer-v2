package dto

import (
	"fmt"

	"github.com/feral-file/ff-indexer-v2/internal/api/shared/constants"
	apierrors "github.com/feral-file/ff-indexer-v2/internal/api/shared/errors"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	internalTypes "github.com/feral-file/ff-indexer-v2/internal/types"
	"github.com/feral-file/ff-indexer-v2/internal/webhook"
)

// TriggerTokenIndexingRequest represents the request body for triggering token indexing by CIDs
type TriggerTokenIndexingRequest struct {
	TokenCIDs []domain.TokenCID `json:"token_cids"`
}

// Validate validates the request body
func (r *TriggerTokenIndexingRequest) Validate() error {
	// Validate: token CIDs must be provided
	if len(r.TokenCIDs) == 0 {
		return apierrors.NewValidationError("token_cids is required")
	}

	// Validate: maximum number of token CIDs allowed
	if len(r.TokenCIDs) > constants.MAX_TOKEN_CIDS_PER_REQUEST {
		return apierrors.NewValidationError(fmt.Sprintf("maximum %d token CIDs allowed", constants.MAX_TOKEN_CIDS_PER_REQUEST))
	}

	// Validate: token CIDs must be valid
	for _, cid := range r.TokenCIDs {
		if !domain.TokenCID(cid).Valid() {
			return apierrors.NewValidationError(fmt.Sprintf("invalid token CID: %s", cid))
		}
	}

	return nil
}

// TriggerOwnerIndexingRequest represents the request body for triggering token indexing by owner addresses
type TriggerOwnerIndexingRequest struct {
	Addresses []string `json:"addresses"`
}

// Validate validates the request body
func (r *TriggerOwnerIndexingRequest) Validate() error {
	// Validate: addresses must be provided
	if len(r.Addresses) == 0 {
		return apierrors.NewValidationError("addresses is required")
	}

	// Validate: maximum number of addresses allowed
	if len(r.Addresses) > constants.MAX_ADDRESSES_PER_REQUEST {
		return apierrors.NewValidationError(fmt.Sprintf("maximum %d addresses allowed", constants.MAX_ADDRESSES_PER_REQUEST))
	}

	// Validate: addresses must be valid
	for _, address := range r.Addresses {
		if !internalTypes.IsTezosAddress(address) && !internalTypes.IsEthereumAddress(address) {
			return apierrors.NewValidationError(fmt.Sprintf("invalid address: %s. Must be a valid Tezos or Ethereum address", address))
		}
	}

	return nil
}

// TriggerMetadataIndexingRequest represents the request body for triggering token metadata refresh by IDs or CIDs
type TriggerMetadataIndexingRequest struct {
	TokenIDs  []uint64          `json:"token_ids,omitempty"`
	TokenCIDs []domain.TokenCID `json:"token_cids,omitempty"`
}

// Validate validates the request body
func (r *TriggerMetadataIndexingRequest) Validate() error {
	// Validate: at least one of token IDs or token CIDs must be provided
	if len(r.TokenIDs) == 0 && len(r.TokenCIDs) == 0 {
		return apierrors.NewValidationError("at least one of token_ids or token_cids is required")
	}

	// Validate: maximum number of token IDs allowed
	if len(r.TokenIDs) > constants.MAX_TOKEN_CIDS_PER_REQUEST {
		return apierrors.NewValidationError(fmt.Sprintf("maximum %d token IDs allowed", constants.MAX_TOKEN_CIDS_PER_REQUEST))
	}

	// Validate: maximum number of token CIDs allowed
	if len(r.TokenCIDs) > constants.MAX_TOKEN_CIDS_PER_REQUEST {
		return apierrors.NewValidationError(fmt.Sprintf("maximum %d token CIDs allowed", constants.MAX_TOKEN_CIDS_PER_REQUEST))
	}

	// Validate: token CIDs must be valid
	for _, cid := range r.TokenCIDs {
		if !domain.TokenCID(cid).Valid() {
			return apierrors.NewValidationError(fmt.Sprintf("invalid token CID: %s", cid))
		}
	}

	return nil
}

// CreateWebhookClientRequest represents the request body for creating a webhook client
type CreateWebhookClientRequest struct {
	WebhookURL       string   `json:"webhook_url"`
	EventFilters     []string `json:"event_filters"`
	RetryMaxAttempts *int     `json:"retry_max_attempts,omitempty"`
}

// Validate validates the request body
func (r *CreateWebhookClientRequest) Validate(debug bool) error {
	// Validate: webhook URL must be provided
	if r.WebhookURL == "" {
		return apierrors.NewValidationError("webhook_url is required")
	}

	// Validate: webhook URL must be valid
	if debug {
		if !internalTypes.IsValidURL(r.WebhookURL) {
			return apierrors.NewValidationError("webhook_url must be a valid URL")
		}
	} else {
		if !internalTypes.IsHTTPSURL(r.WebhookURL) {
			return apierrors.NewValidationError("webhook_url must be a valid HTTPS URL")
		}
	}

	// Validate: event filters must be provided
	if len(r.EventFilters) == 0 {
		return apierrors.NewValidationError("event_filters is required and must not be empty")
	}

	// Validate: each event filter must be supported
	for _, eventType := range r.EventFilters {
		if !webhook.IsValidEventType(eventType) {
			return apierrors.NewValidationError(fmt.Sprintf("unsupported event type: %s. Supported types: %v", eventType, webhook.SupportedEventTypes))
		}
	}

	// Validate: retry_max_attempts must be valid if provided
	if r.RetryMaxAttempts != nil {
		if *r.RetryMaxAttempts < 0 || *r.RetryMaxAttempts > constants.MAX_RETRY_MAX_ATTEMPTS {
			return apierrors.NewValidationError("retry_max_attempts must be between 0 and 10")
		}
	}

	return nil
}

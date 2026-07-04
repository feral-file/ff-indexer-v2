package dto

import (
	"fmt"
	"strings"

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

// TriggerAddressIndexingRequest represents the request body for POST /api/v1/tokens/addresses/index
type TriggerAddressIndexingRequest struct {
	Addresses []string `json:"addresses"`
}

// Validate validates the request body
func (r *TriggerAddressIndexingRequest) Validate() error {
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

// TriggerReleaseIndexingRequest represents the request body for POST /api/v1/releases/index.
// It triggers asynchronous indexing of tokens within a mint range for a given vendor release.
// Exactly one of VendorReleaseID or VendorReleaseSlug must be provided.
type TriggerReleaseIndexingRequest struct {
	// Vendor identifies the source platform. Must be one of: artblocks, feralfile, fxhash, objkt.
	// opensea is not supported for release indexing — OpenSea tokens are indexed via
	// on-chain events (IndexTokens path) where mint_number is derived from the actual
	// token identifier during enrichment.
	Vendor string `json:"vendor"`
	// VendorReleaseID is the platform-specific release key:
	//   artblocks: "{chainID}-{contract}-{projectID}" (e.g. "1-0xa7d...-78")
	//   feralfile: series UUID (e.g. "abc-123-...")
	//   fxhash:    generative token ID (e.g. "9997")
	//   objkt:     KT1 contract address (e.g. "KT1abc...") — must be a "custom" collection
	// Mutually exclusive with VendorReleaseSlug.
	VendorReleaseID string `json:"vendor_release_id"`
	// VendorReleaseSlug is the human-readable URL slug from the vendor's website.
	//   artblocks: "fidenza-by-tyler-hobbs"
	//   feralfile: "data-pilgrims-01-769"
	//   fxhash:    "industrial-park"
	//   objkt:     KT1 contract address (same as vendor_release_id)
	// Mutually exclusive with VendorReleaseID. The indexer resolves the slug to a
	// vendor_release_id before enqueuing the job.
	VendorReleaseSlug string `json:"vendor_release_slug"`
	// MintNumbers is the explicit list of 1-based mint positions to index (required, non-empty,
	// max 50, no duplicates). Using an explicit list instead of a range lets callers target only
	// the mints that are still missing without re-indexing already-completed positions.
	MintNumbers []int64 `json:"mint_numbers"`
}

// Validate validates the TriggerReleaseIndexingRequest.
// Exactly one of vendor_release_id or vendor_release_slug must be provided.
// mint_numbers must be non-empty, each >= 1, no duplicates, and at most MAX_RELEASE_MINT_NUMBERS entries.
func (r *TriggerReleaseIndexingRequest) Validate() error {
	if strings.TrimSpace(r.Vendor) == "" {
		return apierrors.NewValidationError("vendor is required")
	}
	switch r.Vendor {
	case "artblocks", "feralfile", "fxhash", "objkt":
		// valid
	default:
		return apierrors.NewValidationError(fmt.Sprintf("unsupported vendor: %s. Must be one of: artblocks, feralfile, fxhash, objkt", r.Vendor))
	}

	hasID := strings.TrimSpace(r.VendorReleaseID) != ""
	hasSlug := strings.TrimSpace(r.VendorReleaseSlug) != ""
	if !hasID && !hasSlug {
		return apierrors.NewValidationError("exactly one of vendor_release_id or vendor_release_slug is required")
	}
	if hasID && hasSlug {
		return apierrors.NewValidationError("vendor_release_id and vendor_release_slug are mutually exclusive; provide exactly one")
	}

	if len(r.MintNumbers) == 0 {
		return apierrors.NewValidationError("mint_numbers is required and must not be empty")
	}
	if int64(len(r.MintNumbers)) > constants.MAX_RELEASE_MINT_NUMBERS {
		return apierrors.NewValidationError(fmt.Sprintf("too many mint_numbers: max %d per request", constants.MAX_RELEASE_MINT_NUMBERS))
	}
	seen := make(map[int64]struct{}, len(r.MintNumbers))
	for _, n := range r.MintNumbers {
		if n < 1 {
			return apierrors.NewValidationError("each mint_number must be >= 1")
		}
		if _, dup := seen[n]; dup {
			return apierrors.NewValidationError(fmt.Sprintf("duplicate mint_number: %d", n))
		}
		seen[n] = struct{}{}
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

package dto

import (
	"fmt"

	"github.com/feral-file/ff-indexer-v2/internal/api/shared/constants"
	apierrors "github.com/feral-file/ff-indexer-v2/internal/api/shared/errors"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	internalTypes "github.com/feral-file/ff-indexer-v2/internal/types"
)

// TriggerIndexingRequest represents the request body for triggering indexing
type TriggerIndexingRequest struct {
	TokenCIDs []domain.TokenCID `json:"token_cids,omitempty"`
	Addresses []string          `json:"addresses,omitempty"`
}

// Validate validates the request body
func (r *TriggerIndexingRequest) Validate() error {
	hasTokenCIDs := len(r.TokenCIDs) > 0
	hasAddresses := len(r.Addresses) > 0

	// Validate: only one type of input should be provided
	if hasTokenCIDs && hasAddresses {
		return apierrors.NewValidationError("cannot provide both token_cids and addresses")
	}

	// Validate: maximum number of token CIDs allowed
	if hasTokenCIDs && len(r.TokenCIDs) > constants.MAX_TOKEN_CIDS_PER_REQUEST {
		return apierrors.NewValidationError(fmt.Sprintf("maximum %d token CIDs allowed", constants.MAX_TOKEN_CIDS_PER_REQUEST))
	}

	// Validate: maximum number of addresses allowed
	if hasAddresses && len(r.Addresses) > constants.MAX_ADDRESSES_PER_REQUEST {
		return apierrors.NewValidationError(fmt.Sprintf("maximum %d addresses allowed", constants.MAX_ADDRESSES_PER_REQUEST))
	}

	// Validate: token CIDs must be valid
	for _, cid := range r.TokenCIDs {
		if !domain.TokenCID(cid).Valid() {
			return apierrors.NewValidationError(fmt.Sprintf("invalid token CID: %s", cid))
		}
	}

	// Validate: addresses must be valid
	for _, address := range r.Addresses {
		if !internalTypes.IsTezosAddress(address) && !internalTypes.IsEthereumAddress(address) {
			return apierrors.NewValidationError(fmt.Sprintf("invalid address: %s. Must be a valid Tezos or Ethereum address", address))
		}
	}

	return nil
}

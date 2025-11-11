package dto

import (
	"fmt"

	"github.com/feral-file/ff-indexer-v2/internal/api/shared/constants"
	apierrors "github.com/feral-file/ff-indexer-v2/internal/api/shared/errors"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	internalTypes "github.com/feral-file/ff-indexer-v2/internal/types"
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

package rest

import (
	"fmt"

	"github.com/gin-gonic/gin"

	"github.com/feral-file/ff-indexer-v2/internal/api/shared/constants"
	apierrors "github.com/feral-file/ff-indexer-v2/internal/api/shared/errors"
	"github.com/feral-file/ff-indexer-v2/internal/api/shared/types"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	internalTypes "github.com/feral-file/ff-indexer-v2/internal/types"
)

// GetTokenQueryParams holds query parameters for GET /tokens/:cid
type GetTokenQueryParams struct {
	Expand []types.Expansion `form:"expand"`

	// Owners expansion parameters
	OwnerLimit  uint8  `form:"owners.limit,default=10"`
	OwnerOffset uint64 `form:"owners.offset,default=0"`

	// Provenance events expansion parameters
	ProvenanceEventLimit  uint8       `form:"provenance_events.limit,default=10"`
	ProvenanceEventOffset uint64      `form:"provenance_events.offset,default=0"`
	ProvenanceEventOrder  types.Order `form:"provenance_events.order,default=desc"`
}

// Validate validates the query parameters for GET /tokens/:cid
func (p *GetTokenQueryParams) Validate() error {
	// Validate expansions
	for _, expansion := range p.Expand {
		if expansion != types.ExpansionOwners &&
			expansion != types.ExpansionProvenanceEvents &&
			expansion != types.ExpansionMetadata &&
			expansion != types.ExpansionEnrichmentSource &&
			expansion != types.ExpansionMediaAsset &&
			expansion != types.ExpansionDisplay {
			return apierrors.NewValidationError(fmt.Sprintf("Invalid expansion: %s. Must be a valid expansion", expansion))
		}
	}

	// Validate provenance event order
	if !p.ProvenanceEventOrder.Valid() {
		return apierrors.NewValidationError(fmt.Sprintf("Invalid provenance event order: %s. Must be a valid order", p.ProvenanceEventOrder))
	}

	return nil
}

// ListTokensQueryParams holds query parameters for GET /tokens
type ListTokensQueryParams struct {
	// Filters
	Owners            []string       `form:"owner"`
	Chains            []domain.Chain `form:"chain"`
	ContractAddresses []string       `form:"contract_address"`
	TokenNumbers      []string       `form:"token_number"`
	TokenIDs          []uint64       `form:"token_id"`
	TokenCIDs         []string       `form:"token_cid"`
	IncludeUnviewable bool           `form:"include_unviewable,default=false"` // Include tokens with is_viewable=false

	// Pagination
	Limit  uint8  `form:"limit,default=20"`
	Offset uint64 `form:"offset,default=0"`

	// Sorting
	SortBy    types.TokenSortBy `form:"sort_by,default=latest_provenance"`
	SortOrder types.Order       `form:"sort_order,default=desc"`

	// Expansion
	Expansions []types.Expansion `form:"expand"`
}

// Validate validates the query parameters for GET /tokens
func (p *ListTokensQueryParams) Validate() error {
	// Validate owners
	for _, owner := range p.Owners {
		if !internalTypes.IsTezosAddress(owner) && !internalTypes.IsEthereumAddress(owner) {
			return apierrors.NewValidationError(fmt.Sprintf("Invalid owner: %s. Must be a valid Tezos or Ethereum address", owner))
		}
	}

	// Validate chains
	for _, chain := range p.Chains {
		if !domain.IsValidChain(chain) {
			return apierrors.NewValidationError(fmt.Sprintf("Invalid chain: %s. Must be a valid Tezos or Ethereum chain", chain))
		}
	}

	// Validate contract addresses
	for _, contractAddress := range p.ContractAddresses {
		if !internalTypes.IsTezosContractAddress(contractAddress) && !internalTypes.IsEthereumAddress(contractAddress) {
			return apierrors.NewValidationError(fmt.Sprintf("Invalid contract address: %s. Must be a valid Tezos or Ethereum address", contractAddress))
		}
	}

	// Validate token CIDs
	for _, tokenCID := range p.TokenCIDs {
		if !domain.TokenCID(tokenCID).Valid() {
			return apierrors.NewValidationError(fmt.Sprintf("Invalid token CID: %s. Must be a valid token CID", tokenCID))
		}
	}

	// Validate token numbers
	for _, tokenNumber := range p.TokenNumbers {
		if !internalTypes.IsNumeric(tokenNumber) {
			return apierrors.NewValidationError(fmt.Sprintf("Invalid token number: %s. Must be a valid positive numeric value", tokenNumber))
		}
	}

	// Validate expansions
	for _, expansion := range p.Expansions {
		if expansion != types.ExpansionOwners &&
			expansion != types.ExpansionOwnerProvenances &&
			expansion != types.ExpansionProvenanceEvents &&
			expansion != types.ExpansionMetadata &&
			expansion != types.ExpansionEnrichmentSource &&
			expansion != types.ExpansionMediaAsset &&
			expansion != types.ExpansionDisplay {
			return apierrors.NewValidationError(fmt.Sprintf("Invalid expansion: %s. Must be a valid expansion", expansion))
		}
	}

	// Validate sort_by
	if !p.SortBy.Valid() {
		return apierrors.NewValidationError(fmt.Sprintf("Invalid sort_by: %s. Must be a valid sort field", p.SortBy))
	}

	// Validate sort_order
	if !p.SortOrder.Valid() {
		return apierrors.NewValidationError(fmt.Sprintf("Invalid sort_order: %s. Must be a valid order", p.SortOrder))
	}

	return nil
}

// ParseGetTokenQuery parses query parameters for GET /tokens/:cid
func ParseGetTokenQuery(c *gin.Context) (*GetTokenQueryParams, error) {
	var params GetTokenQueryParams
	if err := c.ShouldBindQuery(&params); err != nil {
		return nil, err
	}

	// Cap limits
	if params.OwnerLimit > constants.MAX_PAGE_SIZE {
		params.OwnerLimit = constants.MAX_PAGE_SIZE
	}
	if params.ProvenanceEventLimit > constants.MAX_PAGE_SIZE {
		params.ProvenanceEventLimit = constants.MAX_PAGE_SIZE
	}

	// Validate order
	if !params.ProvenanceEventOrder.Asc() && !params.ProvenanceEventOrder.Desc() {
		params.ProvenanceEventOrder = types.OrderDesc
	}

	return &params, nil
}

// ParseListTokensQuery parses query parameters for GET /tokens
func ParseListTokensQuery(c *gin.Context) (*ListTokensQueryParams, error) {
	var params ListTokensQueryParams
	if err := c.ShouldBindQuery(&params); err != nil {
		return nil, err
	}

	// Normalize addresses
	params.Owners = domain.NormalizeAddresses(params.Owners)
	params.ContractAddresses = domain.NormalizeAddresses(params.ContractAddresses)

	// Validate sort order
	if !params.SortOrder.Asc() && !params.SortOrder.Desc() {
		params.SortOrder = types.OrderDesc
	}

	// Validate sort by
	if !params.SortBy.Valid() {
		params.SortBy = types.TokenLatestProvenance
	}

	return &params, nil
}

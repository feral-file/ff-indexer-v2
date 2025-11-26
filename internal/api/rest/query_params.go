package rest

import (
	"fmt"
	"slices"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/feral-file/ff-indexer-v2/internal/api/shared/constants"
	apierrors "github.com/feral-file/ff-indexer-v2/internal/api/shared/errors"
	"github.com/feral-file/ff-indexer-v2/internal/api/shared/types"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
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
			expansion != types.ExpansionEnrichmentSource &&
			expansion != types.ExpansionMetadataMediaAsset &&
			expansion != types.ExpansionEnrichmentSourceMediaAsset {
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

	// Pagination
	Limit  uint8  `form:"limit,default=20"`
	Offset uint64 `form:"offset,default=0"`

	// Expansion
	Expand []types.Expansion `form:"expand"`

	// Owners expansion parameters
	OwnerLimit  uint8  `form:"owners.limit,default=10"`
	OwnerOffset uint64 `form:"owners.offset,default=0"`

	// Provenance events expansion parameters
	ProvenanceEventLimit  uint8       `form:"provenance_events.limit,default=10"`
	ProvenanceEventOffset uint64      `form:"provenance_events.offset,default=0"`
	ProvenanceEventOrder  types.Order `form:"provenance_events.order,default=desc"`
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
	for _, expansion := range p.Expand {
		if expansion != types.ExpansionOwners &&
			expansion != types.ExpansionProvenanceEvents &&
			expansion != types.ExpansionEnrichmentSource &&
			expansion != types.ExpansionMetadataMediaAsset &&
			expansion != types.ExpansionEnrichmentSourceMediaAsset {
			return apierrors.NewValidationError(fmt.Sprintf("Invalid expansion: %s. Must be a valid expansion", expansion))
		}
	}

	// Validate provenance event order
	if !p.ProvenanceEventOrder.Valid() {
		return apierrors.NewValidationError(fmt.Sprintf("Invalid provenance event order: %s. Must be a valid order", p.ProvenanceEventOrder))
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

	// Cap limits
	if params.Limit > constants.MAX_PAGE_SIZE {
		params.Limit = constants.MAX_PAGE_SIZE
	}
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

// GetChangesQueryParams holds query parameters for GET /changes
type GetChangesQueryParams struct {
	// Filters
	TokenIDs     []uint64             `form:"token_id"`
	TokenCIDs    []string             `form:"token_cid"`
	Addresses    []string             `form:"address"`
	SubjectTypes []schema.SubjectType `form:"subject_type"`
	SubjectIDs   []string             `form:"subject_id"`

	// Cursor-based pagination
	Anchor *uint64 `form:"anchor"` // ID-based cursor - show changes after this ID

	// Deprecated: Use anchor instead for reliable pagination
	// Timestamp filter - only show changes after this time
	// Note: Different subject types use different timestamp semantics which may cause inconsistent results
	Since *time.Time `form:"since" time_format:"2006-01-02T15:04:05.999999999Z07:00"`

	// Pagination
	Limit uint8 `form:"limit,default=20"`

	// Deprecated: Use anchor for cursor-based pagination instead
	// Offset only applies when using 'since' parameter - not used with 'anchor'
	Offset uint64 `form:"offset,default=0"`

	// Deprecated: Only applies when using 'since' parameter - always ascending with 'anchor' for sequential audit log
	Order types.Order `form:"order,default=asc"`

	Expand []types.Expansion `form:"expand"` // Expansion options: subject
}

// Validate validates the query parameters for GET /changes
func (p *GetChangesQueryParams) Validate() error {
	// Validate token CIDs
	for _, tokenCID := range p.TokenCIDs {
		if !domain.TokenCID(tokenCID).Valid() {
			return apierrors.NewValidationError(fmt.Sprintf("Invalid token CID: %s. Must be a valid token CID", tokenCID))
		}
	}

	// Validate addresses
	for _, address := range p.Addresses {
		if !internalTypes.IsTezosAddress(address) && !internalTypes.IsEthereumAddress(address) {
			return apierrors.NewValidationError(fmt.Sprintf("Invalid address: %s. Must be a valid Tezos or Ethereum address", address))
		}
	}

	// Validate expansions
	for _, expansion := range p.Expand {
		if expansion != types.ExpansionSubject {
			return apierrors.NewValidationError(fmt.Sprintf("Invalid expansion: %s. Must be a valid expansion", expansion))
		}
	}

	// Validate order
	if !p.Order.Valid() {
		return apierrors.NewValidationError(fmt.Sprintf("Invalid order: %s. Must be a valid order", p.Order))
	}

	return nil
}

// ParseGetChangesQuery parses query parameters for GET /changes
func ParseGetChangesQuery(c *gin.Context) (*GetChangesQueryParams, error) {
	var params GetChangesQueryParams
	if err := c.ShouldBindQuery(&params); err != nil {
		return nil, err
	}

	// Cap limit
	if params.Limit > constants.MAX_PAGE_SIZE {
		params.Limit = constants.MAX_PAGE_SIZE
	}

	// Validate order
	if !params.Order.Asc() && !params.Order.Desc() {
		params.Order = types.OrderAsc
	}

	return &params, nil
}

// ShouldExpandSubject returns true if subject expansion is requested
func (p *GetChangesQueryParams) ShouldExpandSubject() bool {
	return slices.Contains(p.Expand, types.ExpansionSubject)
}

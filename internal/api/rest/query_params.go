package rest

import (
	"slices"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/feral-file/ff-indexer-v2/internal/api/shared/constants"
	"github.com/feral-file/ff-indexer-v2/internal/api/shared/types"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
)

// GetTokenQueryParams holds query parameters for GET /tokens/:cid
type GetTokenQueryParams struct {
	Expand []types.Expansion `form:"expand"`

	// Owners expansion parameters
	OwnerLimit  int    `form:"owners.limit,default=10"`
	OwnerOffset uint64 `form:"owners.offset,default=0"`

	// Provenance events expansion parameters
	ProvenanceEventLimit  int         `form:"provenance_events.limit,default=10"`
	ProvenanceEventOffset uint64      `form:"provenance_events.offset,default=0"`
	ProvenanceEventOrder  types.Order `form:"provenance_events.order,default=desc"`
}

// ListTokensQueryParams holds query parameters for GET /tokens
type ListTokensQueryParams struct {
	// Filters
	Owners            []string       `form:"owner"`
	Chains            []domain.Chain `form:"chain"`
	ContractAddresses []string       `form:"contract_address"`
	TokenIDs          []string       `form:"token_id"`

	// Pagination
	Limit  int    `form:"limit,default=20"`
	Offset uint64 `form:"offset,default=0"`

	// Expansion
	Expand []types.Expansion `form:"expand"`

	// Owners expansion parameters
	OwnerLimit  int    `form:"owners.limit,default=10"`
	OwnerOffset uint64 `form:"owners.offset,default=0"`

	// Provenance events expansion parameters
	ProvenanceEventLimit  int         `form:"provenance_events.limit,default=10"`
	ProvenanceEventOffset uint64      `form:"provenance_events.offset,default=0"`
	ProvenanceEventOrder  types.Order `form:"provenance_events.order,default=desc"`
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
	TokenCIDs []string   `form:"token_cid"`
	Addresses []string   `form:"address"`
	Since     *time.Time `form:"since" time_format:"2006-01-02T15:04:05Z07:00"` // Timestamp filter - only show changes after this time

	// Pagination
	Limit  int               `form:"limit,default=20"`
	Offset uint64            `form:"offset,default=0"`
	Order  types.Order       `form:"order,default=asc"` // asc or desc (based on changed_at)
	Expand []types.Expansion `form:"expand"`            // Expansion options: subject
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

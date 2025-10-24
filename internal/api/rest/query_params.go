package rest

import (
	"time"

	"github.com/gin-gonic/gin"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
)

const MAX_PAGE_SIZE = 100

type Order string

const (
	OrderAsc  Order = "asc"
	OrderDesc Order = "desc"
)

func (o Order) Desc() bool {
	return o == OrderDesc
}

func (o Order) Asc() bool {
	return o == OrderAsc
}

// GetTokenQueryParams holds query parameters for GET /tokens/:cid
type GetTokenQueryParams struct {
	Expand []string `form:"expand"`

	// Owners expansion parameters
	OwnerLimit  int `form:"owners.limit,default=10"`
	OwnerOffset int `form:"owners.offset,default=0"`

	// Provenance events expansion parameters
	ProvenanceEventLimit  int   `form:"provenance_events.limit,default=10"`
	ProvenanceEventOffset int   `form:"provenance_events.offset,default=0"`
	ProvenanceEventOrder  Order `form:"provenance_events.order,default=desc"`
}

// ListTokensQueryParams holds query parameters for GET /tokens
type ListTokensQueryParams struct {
	// Filters
	Owners            []string `form:"owner"`
	Chains            []string `form:"chain"`
	ContractAddresses []string `form:"contract_address"`
	TokenIDs          []string `form:"token_id"`

	// Pagination
	Limit  int `form:"limit,default=20"`
	Offset int `form:"offset,default=0"`

	// Expansion
	Expand []string `form:"expand"`

	// Owners expansion parameters
	OwnerLimit  int `form:"owners.limit,default=10"`
	OwnerOffset int `form:"owners.offset,default=0"`

	// Provenance events expansion parameters
	ProvenanceEventLimit  int   `form:"provenance_events.limit,default=10"`
	ProvenanceEventOffset int   `form:"provenance_events.offset,default=0"`
	ProvenanceEventOrder  Order `form:"provenance_events.order,default=desc"`
}

// ExpansionParams holds parsed expansion flags
type ExpansionParams struct {
	Owners           bool
	ProvenanceEvents bool
}

// ParseGetTokenQuery parses query parameters for GET /tokens/:cid
func ParseGetTokenQuery(c *gin.Context) (*GetTokenQueryParams, error) {
	var params GetTokenQueryParams
	if err := c.ShouldBindQuery(&params); err != nil {
		return nil, err
	}

	// Cap limits
	if params.OwnerLimit > MAX_PAGE_SIZE {
		params.OwnerLimit = MAX_PAGE_SIZE
	}
	if params.ProvenanceEventLimit > MAX_PAGE_SIZE {
		params.ProvenanceEventLimit = MAX_PAGE_SIZE
	}

	// Validate order
	if !params.ProvenanceEventOrder.Asc() && !params.ProvenanceEventOrder.Desc() {
		params.ProvenanceEventOrder = OrderDesc
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
	if params.Limit > MAX_PAGE_SIZE {
		params.Limit = MAX_PAGE_SIZE
	}
	if params.OwnerLimit > MAX_PAGE_SIZE {
		params.OwnerLimit = MAX_PAGE_SIZE
	}
	if params.ProvenanceEventLimit > MAX_PAGE_SIZE {
		params.ProvenanceEventLimit = MAX_PAGE_SIZE
	}

	// Validate order
	if !params.ProvenanceEventOrder.Asc() && !params.ProvenanceEventOrder.Desc() {
		params.ProvenanceEventOrder = OrderDesc
	}

	return &params, nil
}

// GetExpansions returns expansion flags
func (p *GetTokenQueryParams) GetExpansions() ExpansionParams {
	var result ExpansionParams
	for _, item := range p.Expand {
		switch item {
		case "owners":
			result.Owners = true
		case "provenance_events":
			result.ProvenanceEvents = true
		}
	}
	return result
}

// GetExpansions returns expansion flags
func (p *ListTokensQueryParams) GetExpansions() ExpansionParams {
	var result ExpansionParams
	for _, item := range p.Expand {
		switch item {
		case "owners":
			result.Owners = true
		case "provenance_events":
			result.ProvenanceEvents = true
		}
	}
	return result
}

// GetChangesQueryParams holds query parameters for GET /changes
type GetChangesQueryParams struct {
	// Filters
	TokenCIDs []string   `form:"token_cid"`
	Addresses []string   `form:"address"`
	Since     *time.Time `form:"since" time_format:"2006-01-02T15:04:05Z07:00"` // Timestamp filter - only show changes after this time

	// Pagination
	Limit  int      `form:"limit,default=20"`
	Offset int      `form:"offset,default=0"`
	Order  Order    `form:"order,default=asc"` // asc or desc (based on changed_at)
	Expand []string `form:"expand"`            // Expansion options: subject
}

// ParseGetChangesQuery parses query parameters for GET /changes
func ParseGetChangesQuery(c *gin.Context) (*GetChangesQueryParams, error) {
	var params GetChangesQueryParams
	if err := c.ShouldBindQuery(&params); err != nil {
		return nil, err
	}

	// Cap limit
	if params.Limit > MAX_PAGE_SIZE {
		params.Limit = MAX_PAGE_SIZE
	}

	// Validate order
	if !params.Order.Asc() && !params.Order.Desc() {
		params.Order = OrderAsc
	}

	return &params, nil
}

// ShouldExpandSubject returns true if subject expansion is requested
func (p *GetChangesQueryParams) ShouldExpandSubject() bool {
	for _, item := range p.Expand {
		if item == "subject" {
			return true
		}
	}
	return false
}

package rest

import (
	"github.com/gin-gonic/gin"
)

// GetTokenQueryParams holds query parameters for GET /tokens/:cid
type GetTokenQueryParams struct {
	Expand []string `form:"expand"`

	// Owners expansion parameters
	OwnerLimit  int `form:"owners.limit,default=10"`
	OwnerOffset int `form:"owners.offset,default=0"`

	// Provenance events expansion parameters
	ProvenanceEventLimit  int    `form:"provenance_events.limit,default=10"`
	ProvenanceEventOffset int    `form:"provenance_events.offset,default=0"`
	ProvenanceEventOrder  string `form:"provenance_events.order,default=desc"`
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
	ProvenanceEventLimit  int    `form:"provenance_events.limit,default=10"`
	ProvenanceEventOffset int    `form:"provenance_events.offset,default=0"`
	ProvenanceEventOrder  string `form:"provenance_events.order,default=desc"`
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
	if params.OwnerLimit > 100 {
		params.OwnerLimit = 100
	}
	if params.ProvenanceEventLimit > 100 {
		params.ProvenanceEventLimit = 100
	}

	// Validate order
	if params.ProvenanceEventOrder != "asc" && params.ProvenanceEventOrder != "desc" {
		params.ProvenanceEventOrder = "desc"
	}

	return &params, nil
}

// ParseListTokensQuery parses query parameters for GET /tokens
func ParseListTokensQuery(c *gin.Context) (*ListTokensQueryParams, error) {
	var params ListTokensQueryParams
	if err := c.ShouldBindQuery(&params); err != nil {
		return nil, err
	}

	// Cap limits
	if params.Limit > 100 {
		params.Limit = 100
	}
	if params.OwnerLimit > 100 {
		params.OwnerLimit = 100
	}
	if params.ProvenanceEventLimit > 100 {
		params.ProvenanceEventLimit = 100
	}

	// Validate order
	if params.ProvenanceEventOrder != "asc" && params.ProvenanceEventOrder != "desc" {
		params.ProvenanceEventOrder = "desc"
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

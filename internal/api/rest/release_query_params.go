package rest

import (
	"fmt"

	"github.com/gin-gonic/gin"

	"github.com/feral-file/ff-indexer-v2/internal/api/shared/constants"
	apierrors "github.com/feral-file/ff-indexer-v2/internal/api/shared/errors"
	"github.com/feral-file/ff-indexer-v2/internal/api/shared/types"
)

// GetReleaseQueryParams holds query parameters for GET /releases/:id
type GetReleaseQueryParams struct {
	Limit      uint8             `form:"limit,default=20"`
	Offset     uint64            `form:"offset,default=0"`
	SortOrder  types.Order       `form:"sort_order,default=asc"`
	Expansions []types.Expansion `form:"expand"`
}

// Validate validates the query parameters for GET /releases/:id
func (p *GetReleaseQueryParams) Validate() error {
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

	if !p.SortOrder.Valid() {
		return apierrors.NewValidationError(fmt.Sprintf("Invalid sort_order: %s. Must be a valid order", p.SortOrder))
	}

	return nil
}

// ParseGetReleaseQuery parses query parameters for GET /releases/:id
func ParseGetReleaseQuery(c *gin.Context) (*GetReleaseQueryParams, error) {
	var params GetReleaseQueryParams
	if err := c.ShouldBindQuery(&params); err != nil {
		return nil, err
	}

	if params.Limit > constants.MAX_PAGE_SIZE {
		params.Limit = constants.MAX_PAGE_SIZE
	}
	if !params.SortOrder.Asc() && !params.SortOrder.Desc() {
		params.SortOrder = types.OrderAsc
	}

	return &params, nil
}

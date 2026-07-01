package rest

import (
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apierrors "github.com/feral-file/ff-indexer-v2/internal/api/shared/errors"
	"github.com/feral-file/ff-indexer-v2/internal/api/shared/types"
)

func TestGetReleaseQueryParamsValidateInvalidExpansion(t *testing.T) {
	t.Parallel()

	params := GetReleaseQueryParams{
		Limit:      1,
		Expansions: []types.Expansion{"invalid"},
		SortOrder:  types.OrderAsc,
	}

	err := params.Validate()
	require.Error(t, err)
	var apiErr *apierrors.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Contains(t, apiErr.Details, "Invalid expansion")
}

func TestGetReleaseQueryParamsValidateInvalidSortOrder(t *testing.T) {
	t.Parallel()

	params := GetReleaseQueryParams{
		Limit:     1,
		SortOrder: types.Order("invalid"),
	}

	err := params.Validate()
	require.Error(t, err)
	var apiErr *apierrors.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Contains(t, apiErr.Details, "Invalid sort_order")
}

func TestParseGetReleaseQueryInvalidSortOrderPassesThroughToValidate(t *testing.T) {
	// ParseGetReleaseQuery does not silently rewrite unknown sort_order values;
	// Validate() is responsible for rejecting them with a clear error.
	gin.SetMode(gin.TestMode)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/releases/1?sort_order=invalid", nil)

	params, err := ParseGetReleaseQuery(c)
	require.NoError(t, err)
	require.NotNil(t, params)

	// The parse step must preserve the raw (invalid) value so Validate can reject it.
	assert.Equal(t, types.Order("invalid"), params.SortOrder)

	// Validate must reject the unrecognized value.
	validateErr := params.Validate()
	require.Error(t, validateErr)
	var apiErr *apierrors.APIError
	require.ErrorAs(t, validateErr, &apiErr)
	assert.Contains(t, apiErr.Details, "Invalid sort_order")
}

func TestGetReleaseQueryParamsValidateRejectsZeroLimit(t *testing.T) {
	t.Parallel()

	params := GetReleaseQueryParams{
		Limit:     0,
		SortOrder: types.OrderAsc,
	}

	err := params.Validate()
	require.Error(t, err)
	var apiErr *apierrors.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Contains(t, apiErr.Details, "Invalid limit")
}

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

func TestListTokensQueryParamsValidateReleaseID(t *testing.T) {
	t.Parallel()

	releaseID := uint64(0)
	params := ListTokensQueryParams{
		ReleaseID: &releaseID,
		SortBy:    types.TokenLatestProvenance,
		SortOrder: types.OrderDesc,
	}

	err := params.Validate()
	require.Error(t, err)
	var apiErr *apierrors.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "Invalid release_id: must be a positive integer", apiErr.Details)
}

func TestListTokensQueryParamsValidateMintNumberRequiresReleaseID(t *testing.T) {
	t.Parallel()

	params := ListTokensQueryParams{
		SortBy:    types.TokenSortByMintNumber,
		SortOrder: types.OrderAsc,
	}

	err := params.Validate()
	require.Error(t, err)
	var apiErr *apierrors.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "sort_by=mint_number requires release_id", apiErr.Details)
}

func TestListTokensQueryParamsValidateMintNumberWithReleaseID(t *testing.T) {
	t.Parallel()

	releaseID := uint64(7)
	params := ListTokensQueryParams{
		ReleaseID: &releaseID,
		SortBy:    types.TokenSortByMintNumber,
		SortOrder: types.OrderAsc,
	}

	assert.NoError(t, params.Validate())
}

// TestParseListTokensQueryInvalidSortByPassesThroughToValidate confirms that
// ParseListTokensQuery no longer silently normalizes an unrecognized sort_by
// value to the default. The raw value must reach Validate() so that the caller
// gets an actionable error (e.g. a typo like "mint_nubmer" with release_id set
// returns a clear rejection instead of silently sorting by latest_provenance).
func TestParseListTokensQueryInvalidSortByPassesThroughToValidate(t *testing.T) {
	gin.SetMode(gin.TestMode)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/tokens?sort_by=mint_nubmer&sort_order=asc", nil)

	params, err := ParseListTokensQuery(c)
	require.NoError(t, err)
	require.NotNil(t, params)

	// The parser must preserve the raw value so Validate() can reject it.
	assert.Equal(t, types.TokenSortBy("mint_nubmer"), params.SortBy)

	validateErr := params.Validate()
	require.Error(t, validateErr)
	var apiErr *apierrors.APIError
	require.ErrorAs(t, validateErr, &apiErr)
	assert.Contains(t, apiErr.Details, "Invalid sort_by")
}

// TestParseListTokensQueryInvalidSortOrderPassesThroughToValidate mirrors the
// release endpoint test: an unrecognized sort_order value must not be silently
// rewritten to "desc" before Validate() is called.
func TestParseListTokensQueryInvalidSortOrderPassesThroughToValidate(t *testing.T) {
	gin.SetMode(gin.TestMode)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/tokens?sort_order=ascending&sort_by=created_at", nil)

	params, err := ParseListTokensQuery(c)
	require.NoError(t, err)
	require.NotNil(t, params)

	// The parser must preserve the raw (invalid) value.
	assert.Equal(t, types.Order("ascending"), params.SortOrder)

	validateErr := params.Validate()
	require.Error(t, validateErr)
	var apiErr *apierrors.APIError
	require.ErrorAs(t, validateErr, &apiErr)
	assert.Contains(t, apiErr.Details, "Invalid sort_order")
}

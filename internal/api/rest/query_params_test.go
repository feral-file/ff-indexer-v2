package rest

import (
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apierrors "github.com/feral-file/ff-indexer-v2/internal/api/shared/errors"
	"github.com/feral-file/ff-indexer-v2/internal/api/shared/types"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

// --- GET /releases/:id ---

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

	assert.Equal(t, types.Order("invalid"), params.SortOrder)

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

// --- GET /releases ---

func TestListReleasesQueryParamsValidateRequiresFilter(t *testing.T) {
	t.Parallel()

	params := ListReleasesQueryParams{Limit: 20}
	err := params.Validate()
	require.Error(t, err)
	var apiErr *apierrors.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Contains(t, apiErr.Details, "at least one of ids, vendor, or vendor_release_id is required")
}

func TestListReleasesQueryParamsValidateIDsOnly(t *testing.T) {
	t.Parallel()

	params := ListReleasesQueryParams{
		IDs:   []uint64{1, 2, 3},
		Limit: 20,
	}
	require.NoError(t, params.Validate())
	assert.Equal(t, []uint64{1, 2, 3}, params.ParsedIDs)
	assert.Nil(t, params.ParsedVendor)
	assert.Nil(t, params.ParsedVendorReleaseID)
}

func TestListReleasesQueryParamsValidateIDsWithVendor(t *testing.T) {
	t.Parallel()

	params := ListReleasesQueryParams{
		IDs:    []uint64{5},
		Vendor: "feralfile",
		Limit:  20,
	}
	require.NoError(t, params.Validate())
	assert.Equal(t, []uint64{5}, params.ParsedIDs)
	require.NotNil(t, params.ParsedVendor)
	assert.Equal(t, schema.VendorFeralFile, *params.ParsedVendor)
}

func TestListReleasesQueryParamsValidateVendorOnly(t *testing.T) {
	t.Parallel()

	params := ListReleasesQueryParams{
		Vendor: "artblocks",
		Limit:  20,
	}
	require.NoError(t, params.Validate())
	require.NotNil(t, params.ParsedVendor)
	assert.Equal(t, schema.VendorArtBlocks, *params.ParsedVendor)
	assert.Nil(t, params.ParsedVendorReleaseID)
}

func TestListReleasesQueryParamsValidateVendorReleaseIDOnly(t *testing.T) {
	t.Parallel()

	params := ListReleasesQueryParams{
		VendorReleaseID: "series-uuid",
		Limit:           20,
	}
	require.NoError(t, params.Validate())
	assert.Nil(t, params.ParsedVendor)
	require.NotNil(t, params.ParsedVendorReleaseID)
	assert.Equal(t, "series-uuid", *params.ParsedVendorReleaseID)
}

func TestListReleasesQueryParamsValidateInvalidVendor(t *testing.T) {
	t.Parallel()

	params := ListReleasesQueryParams{
		Vendor: "superrare",
		Limit:  20,
	}
	err := params.Validate()
	require.Error(t, err)
	var apiErr *apierrors.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Contains(t, apiErr.Details, "invalid vendor")
}

func TestListReleasesQueryParamsValidateFxhashVendor(t *testing.T) {
	t.Parallel()

	params := ListReleasesQueryParams{
		Vendor: "fxhash",
		Limit:  20,
	}
	err := params.Validate()
	require.NoError(t, err)
	require.NotNil(t, params.ParsedVendor)
	assert.Equal(t, schema.VendorFXHash, *params.ParsedVendor)
}

func TestListReleasesQueryParamsValidateObjktVendor(t *testing.T) {
	t.Parallel()

	params := ListReleasesQueryParams{
		Vendor: "objkt",
		Limit:  20,
	}
	err := params.Validate()
	require.NoError(t, err)
	require.NotNil(t, params.ParsedVendor)
	assert.Equal(t, schema.VendorObjkt, *params.ParsedVendor)
}

func TestListReleasesQueryParamsValidateRejectsZeroLimit(t *testing.T) {
	t.Parallel()

	params := ListReleasesQueryParams{
		Vendor: "feralfile",
		Limit:  0,
	}
	err := params.Validate()
	require.Error(t, err)
	var apiErr *apierrors.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Contains(t, apiErr.Details, "Invalid limit")
}

func TestParseListReleasesQueryCapsLimit(t *testing.T) {
	gin.SetMode(gin.TestMode)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/releases?vendor=artblocks&limit=255", nil)

	params, err := ParseListReleasesQuery(c)
	require.NoError(t, err)
	assert.Equal(t, uint8(255), params.Limit)
}

// --- GET /tokens ---

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

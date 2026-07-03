package rest

import (
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/api/shared/dto"
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
	assert.Contains(t, apiErr.Details, "at least one of ids, vendor, vendor_release_id, or vendor_release_slug is required")
}

func TestListReleasesQueryParamsValidateIDsRejectsZero(t *testing.T) {
	t.Parallel()

	params := ListReleasesQueryParams{
		IDs:   []uint64{1, 0, 3},
		Limit: 20,
	}
	err := params.Validate()
	require.Error(t, err)
	var apiErr *apierrors.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Contains(t, apiErr.Details, "must be a positive integer")
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

func TestListReleasesQueryParamsValidateVendorReleaseSlugOnly(t *testing.T) {
	t.Parallel()

	params := ListReleasesQueryParams{
		VendorReleaseSlug: "industrial-park",
		Limit:             20,
	}
	require.NoError(t, params.Validate())
	assert.Nil(t, params.ParsedVendor)
	assert.Nil(t, params.ParsedVendorReleaseID)
	require.NotNil(t, params.ParsedVendorReleaseSlug)
	assert.Equal(t, "industrial-park", *params.ParsedVendorReleaseSlug)
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
	assert.Contains(t, apiErr.Details, "sort_by=mint_number requires at least one of")
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

// ─── Mint range filter on ListTokensQueryParams ───────────────────────────────

func TestListTokensQueryParamsMintRangeRequiresReleaseID(t *testing.T) {
	t.Parallel()

	from := int64(1)
	to := int64(10)
	params := ListTokensQueryParams{
		MintFrom:  &from,
		MintTo:    &to,
		SortOrder: types.OrderAsc,
		SortBy:    types.TokenLatestProvenance,
	}

	err := params.Validate()
	require.Error(t, err)
	var apiErr *apierrors.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Contains(t, apiErr.Details, "release_id")
}

func TestListTokensQueryParamsMintRangeValidWithReleaseID(t *testing.T) {
	t.Parallel()

	releaseID := uint64(5)
	from := int64(1)
	to := int64(100)
	params := ListTokensQueryParams{
		ReleaseID: &releaseID,
		MintFrom:  &from,
		MintTo:    &to,
		SortBy:    types.TokenSortByMintNumber,
		SortOrder: types.OrderAsc,
	}

	assert.NoError(t, params.Validate())
}

func TestListTokensQueryParamsMintFromMustBePositive(t *testing.T) {
	t.Parallel()

	releaseID := uint64(5)
	zero := int64(0)
	to := int64(10)
	params := ListTokensQueryParams{
		ReleaseID: &releaseID,
		MintFrom:  &zero,
		MintTo:    &to,
		SortBy:    types.TokenSortByMintNumber,
		SortOrder: types.OrderAsc,
	}

	err := params.Validate()
	require.Error(t, err)
	var apiErr *apierrors.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Contains(t, apiErr.Details, "mint_from must be >= 1")
}

func TestListTokensQueryParamsMintToMustBeGEMintFrom(t *testing.T) {
	t.Parallel()

	releaseID := uint64(5)
	from := int64(10)
	to := int64(5)
	params := ListTokensQueryParams{
		ReleaseID: &releaseID,
		MintFrom:  &from,
		MintTo:    &to,
		SortBy:    types.TokenSortByMintNumber,
		SortOrder: types.OrderAsc,
	}

	err := params.Validate()
	require.Error(t, err)
	var apiErr *apierrors.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Contains(t, apiErr.Details, "mint_to must be >= mint_from")
}

func TestParseListTokensQueryMintRange(t *testing.T) {
	gin.SetMode(gin.TestMode)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/tokens?release_id=7&mint_from=3&mint_to=25&sort_by=mint_number&sort_order=asc", nil)

	params, err := ParseListTokensQuery(c)
	require.NoError(t, err)
	require.NotNil(t, params)

	require.NotNil(t, params.MintFrom)
	require.NotNil(t, params.MintTo)
	assert.Equal(t, int64(3), *params.MintFrom)
	assert.Equal(t, int64(25), *params.MintTo)

	assert.NoError(t, params.Validate())
}

// ─── TriggerReleaseIndexingRequest DTO ───────────────────────────────────────

func TestTriggerReleaseIndexingRequestValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		vendor  string
		id      string
		slug    string
		from    *int64
		to      int64
		wantErr string // matched against APIError.Details (empty = expect no error)
	}{
		{"valid artblocks with id", "artblocks", "1-0xabc-78", "", nil, 100, ""},
		{"valid feralfile with id", "feralfile", "series-uuid", "", nil, 50, ""},
		{"valid fxhash with id", "fxhash", "9997", "", nil, 50, ""},
		{"valid objkt with id", "objkt", "KT1abc", "", nil, 100, ""},
		{"valid fxhash with slug", "fxhash", "", "industrial-park", nil, 50, ""},
		{"valid feralfile with slug", "feralfile", "", "data-pilgrims-01-769", nil, 50, ""},
		{"missing vendor", "", "abc", "", nil, 10, "vendor is required"},
		{"invalid vendor", "superrare", "abc", "", nil, 10, "unsupported vendor"},
		{"missing release id", "artblocks", "", "", nil, 10, "exactly one of vendor_release_id or vendor_release_slug is required"},
		{"both id and slug", "artblocks", "1-0xabc-78", "fidenza-by-tyler-hobbs", nil, 10, "mutually exclusive"},
		{"mint_to < mint_from", "artblocks", "abc", "", int64Ptr(5), 3, "mint_to must be"},
		{"mint_from < 1", "artblocks", "abc", "", int64Ptr(0), 10, "mint_from must be"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			req := dto.TriggerReleaseIndexingRequest{
				Vendor:            tt.vendor,
				VendorReleaseID:   tt.id,
				VendorReleaseSlug: tt.slug,
				MintFrom:          tt.from,
				MintTo:            tt.to,
			}
			err := req.Validate()
			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				var apiErr *apierrors.APIError
				require.ErrorAs(t, err, &apiErr)
				assert.Contains(t, apiErr.Details, tt.wantErr)
			}
		})
	}
}

func int64Ptr(v int64) *int64 { return &v }

// ─── release_vendor and release_vendor_slug on ListTokensQueryParams ──────────

// TestListTokensQueryParamsReleaseVendorValid checks that a valid vendor is parsed.
func TestListTokensQueryParamsReleaseVendorValid(t *testing.T) {
	t.Parallel()

	params := ListTokensQueryParams{
		ReleaseVendor: "artblocks",
		SortBy:        types.TokenLatestProvenance,
		SortOrder:     types.OrderDesc,
	}

	assert.NoError(t, params.Validate())
	require.NotNil(t, params.ParsedReleaseVendor)
	assert.Equal(t, schema.VendorArtBlocks, *params.ParsedReleaseVendor)
}

// TestListTokensQueryParamsReleaseVendorInvalid checks that an unrecognized vendor is rejected.
func TestListTokensQueryParamsReleaseVendorInvalid(t *testing.T) {
	t.Parallel()

	params := ListTokensQueryParams{
		ReleaseVendor: "superrare",
		SortBy:        types.TokenLatestProvenance,
		SortOrder:     types.OrderDesc,
	}

	err := params.Validate()
	require.Error(t, err)
	var apiErr *apierrors.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Contains(t, apiErr.Details, "Invalid release_vendor")
}

// TestListTokensQueryParamsMintNumberWithReleaseVendor verifies that release_vendor alone
// is sufficient context to enable sort_by=mint_number.
func TestListTokensQueryParamsMintNumberWithReleaseVendor(t *testing.T) {
	t.Parallel()

	params := ListTokensQueryParams{
		ReleaseVendor: "feralfile",
		SortBy:        types.TokenSortByMintNumber,
		SortOrder:     types.OrderAsc,
	}

	assert.NoError(t, params.Validate())
}

// TestListTokensQueryParamsMintRangeWithVendorSlug verifies that release_vendor_slug alone
// is sufficient context to enable mint_from / mint_to.
func TestListTokensQueryParamsMintRangeWithVendorSlug(t *testing.T) {
	t.Parallel()

	from := int64(1)
	to := int64(50)
	params := ListTokensQueryParams{
		ReleaseVendorSlug: "fidenza-by-tyler-hobbs",
		MintFrom:          &from,
		MintTo:            &to,
		SortBy:            types.TokenSortByMintNumber,
		SortOrder:         types.OrderAsc,
	}

	assert.NoError(t, params.Validate())
}

// TestParseListTokensQueryReleaseVendorSlug verifies that release_vendor_slug and
// release_vendor are parsed from the query string.
func TestParseListTokensQueryReleaseVendorSlug(t *testing.T) {
	gin.SetMode(gin.TestMode)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET",
		"/tokens?release_vendor=artblocks&release_vendor_slug=fidenza-by-tyler-hobbs&sort_by=mint_number&sort_order=asc", nil)

	params, err := ParseListTokensQuery(c)
	require.NoError(t, err)
	require.NotNil(t, params)
	assert.Equal(t, "artblocks", params.ReleaseVendor)
	assert.Equal(t, "fidenza-by-tyler-hobbs", params.ReleaseVendorSlug)
	assert.NoError(t, params.Validate())
	require.NotNil(t, params.ParsedReleaseVendor)
	assert.Equal(t, schema.VendorArtBlocks, *params.ParsedReleaseVendor)
}

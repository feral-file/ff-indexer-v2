package graphql

// Unit tests for query resolver fields that require custom conversion or
// validation logic. Tests exercise the resolver functions directly without
// a running GraphQL server so type-conversion bugs and validation gaps are
// caught before integration.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/api/shared/dto"
	"github.com/feral-file/ff-indexer-v2/internal/api/shared/types"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
)

// --- release(id) ---

func TestQueryResolverReleaseReturnsNameAndTotalMints(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockExec := mocks.NewMockAPIExecutor(ctrl)
	resolver := NewResolver(false, mockExec)

	name := "1DE94"
	totalMints := int64(75)
	mockExec.EXPECT().
		GetRelease(gomock.Any(), uint64(7)).
		Return(&dto.ReleaseResponse{
			ID:              7,
			Vendor:          "feralfile",
			VendorReleaseID: "1f060e42-0000-0000-0000-000000000001",
			Name:            &name,
			TotalMints:      &totalMints,
		}, nil)

	result, err := resolver.Query().Release(context.Background(), Uint64(7))
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotNil(t, result.Name)
	assert.Equal(t, name, *result.Name)
	require.NotNil(t, result.TotalMints)
	assert.Equal(t, totalMints, *result.TotalMints)
}

// --- releases(vendor, vendor_release_id) ---

func TestQueryResolverReleasesReturnsList(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockExec := mocks.NewMockAPIExecutor(ctrl)
	resolver := NewResolver(false, mockExec)

	vendor := "artblocks"
	nextOffset := uint64(20)
	mockExec.EXPECT().
		ListReleases(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&dto.ReleaseListResponse{
			Items: []dto.ReleaseResponse{{
				ID:              3,
				Vendor:          "artblocks",
				VendorReleaseID: "1-0xabc-1",
			}},
			Offset: &nextOffset,
		}, nil)

	result, err := resolver.Query().Releases(context.Background(), nil, &vendor, nil, nil, nil, nil)
	require.NoError(t, err)
	require.Len(t, result.Items, 1)
	assert.Equal(t, uint64(3), result.Items[0].ID)
	require.NotNil(t, result.Offset)
	assert.Equal(t, uint64(20), *result.Offset)
}

func TestQueryResolverReleasesRequiresFilter(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockExec := mocks.NewMockAPIExecutor(ctrl)
	resolver := NewResolver(false, mockExec)

	_, err := resolver.Query().Releases(context.Background(), nil, nil, nil, nil, nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at least one of ids, vendor, vendor_release_id, or vendor_release_slug is required")
}

func TestQueryResolverReleasesRejectsZeroID(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockExec := mocks.NewMockAPIExecutor(ctrl)
	resolver := NewResolver(false, mockExec)

	ids := []Uint64{1, 0, 3}
	_, err := resolver.Query().Releases(context.Background(), ids, nil, nil, nil, nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be a positive integer")
}

func TestQueryResolverReleasesReturnsByIDs(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockExec := mocks.NewMockAPIExecutor(ctrl)
	resolver := NewResolver(false, mockExec)

	ids := []Uint64{3, 7}
	mockExec.EXPECT().
		ListReleases(gomock.Any(), []uint64{3, 7}, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&dto.ReleaseListResponse{
			Items: []dto.ReleaseResponse{
				{ID: 3, Vendor: "feralfile", VendorReleaseID: "uuid-3"},
				{ID: 7, Vendor: "artblocks", VendorReleaseID: "0xabc-1"},
			},
		}, nil)

	result, err := resolver.Query().Releases(context.Background(), ids, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	require.Len(t, result.Items, 2)
	assert.Equal(t, uint64(3), result.Items[0].ID)
	assert.Equal(t, uint64(7), result.Items[1].ID)
}

// --- token.release_id field resolver ---

// TestTokenResolverReleaseID_Nil verifies that a token with no release
// membership returns nil rather than panicking or returning a zero value.
func TestTokenResolverReleaseID_Nil(t *testing.T) {
	t.Parallel()

	r := &tokenResolver{}
	obj := &dto.TokenResponse{ReleaseID: nil}

	result, err := r.ReleaseID(context.Background(), obj)
	require.NoError(t, err)
	assert.Nil(t, result, "release_id should be nil when token has no release membership")
}

// TestTokenResolverReleaseID_Set verifies that a token with release membership
// returns the correct Uint64 scalar value.
func TestTokenResolverReleaseID_Set(t *testing.T) {
	t.Parallel()

	r := &tokenResolver{}
	id := uint64(42)
	obj := &dto.TokenResponse{ReleaseID: &id}

	result, err := r.ReleaseID(context.Background(), obj)
	require.NoError(t, err)
	require.NotNil(t, result, "release_id should not be nil when token has release membership")
	assert.Equal(t, Uint64(42), *result)
}

// TestTokenResolverReleaseID_LargeID verifies the full uint64 range is preserved
// without truncation to int32 (the previous Int! type would overflow).
func TestTokenResolverReleaseID_LargeID(t *testing.T) {
	t.Parallel()

	r := &tokenResolver{}
	id := uint64(9_999_999_999)
	obj := &dto.TokenResponse{ReleaseID: &id}

	result, err := r.ReleaseID(context.Background(), obj)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, Uint64(9_999_999_999), *result)
}

// --- tokens(release_vendor, release_vendor_slug) ---

// TestQueryResolverTokensRejectsInvalidReleaseVendor ensures an unrecognized
// release_vendor value returns a validation error.
func TestQueryResolverTokensRejectsInvalidReleaseVendor(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockExec := mocks.NewMockAPIExecutor(ctrl)
	resolver := NewResolver(false, mockExec)

	vendor := "superrare"
	_, err := resolver.Query().Tokens(
		context.Background(),
		nil, nil, nil, nil, nil, nil, nil,
		&vendor, nil,
		nil, nil, nil, nil, nil, nil, nil,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Invalid release_vendor")
}

// TestQueryResolverTokensMintNumberRequiresReleaseContext verifies that
// sort_by=mint_number without any release context is rejected.
func TestQueryResolverTokensMintNumberRequiresReleaseContext(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockExec := mocks.NewMockAPIExecutor(ctrl)
	resolver := NewResolver(false, mockExec)

	sortBy := types.TokenSortByMintNumber
	_, err := resolver.Query().Tokens(
		context.Background(),
		nil, nil, nil, nil, nil, nil, nil,
		nil, nil,
		nil, nil, nil, nil, nil, &sortBy, nil,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "sort_by=mint_number requires release_id")
}

// TestQueryResolverTokensVendorAloneRejectsForMintNumber verifies that release_vendor alone
// (without release_vendor_slug) is rejected for sort_by=mint_number. A vendor covers many
// releases, so mint-number ordering is ambiguous without a specific release identifier.
func TestQueryResolverTokensVendorAloneRejectsForMintNumber(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	vendor := "artblocks"
	sortBy := types.TokenSortByMintNumber

	mockExec := mocks.NewMockAPIExecutor(ctrl)
	resolver := NewResolver(false, mockExec)

	_, err := resolver.Query().Tokens(
		context.Background(),
		nil, nil, nil, nil, nil, nil, nil,
		&vendor, nil,
		nil, nil, nil, nil, nil, &sortBy, nil,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "sort_by=mint_number requires release_id")
}

// TestQueryResolverTokensMintNumbersWithVendorSlug verifies that release_vendor_slug
// combined with release_vendor is accepted and enables mint_numbers.
func TestQueryResolverTokensMintNumbersWithVendorSlug(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	vendor := "artblocks"
	slug := "fidenza-by-tyler-hobbs"
	mintNums := []int{1, 25, 50}

	mockExec := mocks.NewMockAPIExecutor(ctrl)
	mockExec.EXPECT().
		GetTokens(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
			gomock.Any(), gomock.Any(), gomock.Any(),
			gomock.Any(), gomock.Any(), // release vendor + slug
			gomock.Any(), // mint_numbers
			gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&dto.TokenListResponse{}, nil)

	resolver := NewResolver(false, mockExec)
	_, err := resolver.Query().Tokens(
		context.Background(),
		nil, nil, nil, nil, nil, nil, nil,
		&vendor, &slug,
		mintNums, nil, nil, nil, nil, nil, nil,
	)
	require.NoError(t, err)
}

// TestQueryResolverTokensSlugAloneRejected verifies that release_vendor_slug without
// release_vendor is rejected. Slug uniqueness is scoped per vendor.
func TestQueryResolverTokensSlugAloneRejected(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	slug := "fidenza-by-tyler-hobbs"

	mockExec := mocks.NewMockAPIExecutor(ctrl)
	resolver := NewResolver(false, mockExec)
	_, err := resolver.Query().Tokens(
		context.Background(),
		nil, nil, nil, nil, nil, nil, nil,
		nil, &slug,
		nil, nil, nil, nil, nil, nil, nil,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "release_vendor_slug requires release_vendor or release_id")
}

// --- token(cid, include_moderated) ---

// TestQueryResolverTokenDefaultsToExcludingSpam pins the GraphQL half of the
// detail-lookup wiring. gqlgen applies the schema default (false) when the caller
// omits the argument, but the resolver still has to forward it; if it does not,
// the endpoint renders flagged tokens regardless of what the executor decides.
func TestQueryResolverTokenDefaultsToExcludingSpam(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockExec := mocks.NewMockAPIExecutor(ctrl)
	resolver := NewResolver(false, mockExec)

	const cid = "eip155:1:erc721:0x1234567890abcdef1234567890abcdef12345678:1" //nolint:gosec
	includeModerated := false
	mockExec.EXPECT().
		GetToken(gomock.Any(), cid, gomock.Any(), gomock.Any(), gomock.Any(),
			gomock.Any(), gomock.Any(), gomock.Any(), false).
		Return(&dto.TokenResponse{TokenCID: cid}, nil)

	result, err := resolver.Query().Token(context.Background(), cid, nil, nil, nil, nil, nil, &includeModerated)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, cid, result.TokenCID)
}

// TestQueryResolverTokenNilIncludeModeratedIsFalse covers the argument being absent
// entirely, which reaches the resolver as a nil pointer rather than false.
func TestQueryResolverTokenNilIncludeModeratedIsFalse(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockExec := mocks.NewMockAPIExecutor(ctrl)
	resolver := NewResolver(false, mockExec)

	const cid = "eip155:1:erc721:0x1234567890abcdef1234567890abcdef12345678:1" //nolint:gosec
	mockExec.EXPECT().
		GetToken(gomock.Any(), cid, gomock.Any(), gomock.Any(), gomock.Any(),
			gomock.Any(), gomock.Any(), gomock.Any(), false).
		Return(&dto.TokenResponse{TokenCID: cid}, nil)

	result, err := resolver.Query().Token(context.Background(), cid, nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestQueryResolverTokenForwardsIncludeModerated(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockExec := mocks.NewMockAPIExecutor(ctrl)
	resolver := NewResolver(false, mockExec)

	const cid = "eip155:1:erc721:0x1234567890abcdef1234567890abcdef12345678:1" //nolint:gosec
	includeModerated := true
	mockExec.EXPECT().
		GetToken(gomock.Any(), cid, gomock.Any(), gomock.Any(), gomock.Any(),
			gomock.Any(), gomock.Any(), gomock.Any(), true).
		Return(&dto.TokenResponse{TokenCID: cid}, nil)

	result, err := resolver.Query().Token(context.Background(), cid, nil, nil, nil, nil, nil, &includeModerated)
	require.NoError(t, err)
	require.NotNil(t, result)
}

// TestQueryResolverTokenFilteredSpamIsNotFound pins that a token the executor
// suppresses surfaces as a not-found error rather than a nil token with no error,
// so clients get a consistent shape whether the CID is unknown or flagged.
func TestQueryResolverTokenFilteredSpamIsNotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockExec := mocks.NewMockAPIExecutor(ctrl)
	resolver := NewResolver(false, mockExec)

	const cid = "eip155:1:erc721:0x1234567890abcdef1234567890abcdef12345678:1" //nolint:gosec
	mockExec.EXPECT().
		GetToken(gomock.Any(), cid, gomock.Any(), gomock.Any(), gomock.Any(),
			gomock.Any(), gomock.Any(), gomock.Any(), false).
		Return(nil, nil)

	result, err := resolver.Query().Token(context.Background(), cid, nil, nil, nil, nil, nil, nil)
	require.Error(t, err)
	assert.Nil(t, result)
}

// --- Release.members(include_moderated) ---

// TestReleaseResolverMembersDefaultsToExcludingSpam mirrors the REST contract:
// members is a renderable TokenList, so it is filtered unless the caller opts in.
// include_unviewable stays true — that one is a transient pipeline state, not a
// content decision.
func TestReleaseResolverMembersDefaultsToExcludingSpam(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockExec := mocks.NewMockAPIExecutor(ctrl)
	resolver := NewResolver(false, mockExec)

	releaseID := uint64(42)
	includeModeratedFalse := false
	includeUnviewableTrue := true
	mockExec.EXPECT().
		GetTokens(gomock.Any(), gomock.Nil(), gomock.Nil(), gomock.Nil(),
			gomock.Nil(), gomock.Nil(), gomock.Nil(), &releaseID, gomock.Nil(),
			gomock.Nil(), gomock.Nil(), gomock.Any(), gomock.Any(),
			&includeUnviewableTrue, &includeModeratedFalse, gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&dto.TokenListResponse{Tokens: []dto.TokenResponse{}}, nil)

	obj := &dto.ReleaseResponse{ID: releaseID, Vendor: "artblocks"}
	_, err := resolver.Release().Members(context.Background(), obj, nil, nil, nil, nil)
	require.NoError(t, err)
}

func TestReleaseResolverMembersForwardsIncludeModerated(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockExec := mocks.NewMockAPIExecutor(ctrl)
	resolver := NewResolver(false, mockExec)

	releaseID := uint64(42)
	includeModeratedTrue := true
	mockExec.EXPECT().
		GetTokens(gomock.Any(), gomock.Nil(), gomock.Nil(), gomock.Nil(),
			gomock.Nil(), gomock.Nil(), gomock.Nil(), &releaseID, gomock.Nil(),
			gomock.Nil(), gomock.Nil(), gomock.Any(), gomock.Any(),
			gomock.Any(), &includeModeratedTrue, gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&dto.TokenListResponse{Tokens: []dto.TokenResponse{}}, nil)

	obj := &dto.ReleaseResponse{ID: releaseID, Vendor: "artblocks"}
	optIn := true
	_, err := resolver.Release().Members(context.Background(), obj, nil, nil, nil, &optIn)
	require.NoError(t, err)
}

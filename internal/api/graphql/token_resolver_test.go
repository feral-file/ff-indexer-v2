package graphql

// Unit tests for token field resolvers that require custom conversion logic.
// These tests are intentionally narrow: they exercise the resolver functions
// directly without a running GraphQL server so that type-conversion bugs
// (e.g. panic stubs left by gqlgen) are caught before integration.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/api/shared/dto"
)

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

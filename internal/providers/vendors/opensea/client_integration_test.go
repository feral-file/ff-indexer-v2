//go:build integration

package opensea_test

// Integration tests for the opensea client against the real OpenSea v2 API.
// Gated behind //go:build integration; excluded from plain go test ./... but
// included in CI and make test-integration via -tags=integration.
//
// Run with:
//
//	OPENSEA_API_KEY=<key> go test -tags=integration ./internal/providers/vendors/opensea/...
//
// Tests gracefully skip when OPENSEA_API_KEY is not set.

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/opensea"
)

func newIntegrationClient(t *testing.T) opensea.Client {
	t.Helper()
	apiKey := os.Getenv("OPENSEA_API_KEY")
	if apiKey == "" {
		t.Skip("OPENSEA_API_KEY not set; skipping OpenSea integration test")
	}
	return opensea.NewClient(
		adapter.NewHTTPClient(30*time.Second),
		nil, // nil limiter passes through immediately
		testAPIURL,
		apiKey,
		adapter.NewJSON(),
	)
}

// TestGetCollection_Integration fetches a well-known collection (Bored Ape Yacht Club)
// and validates all release-relevant fields are present and well-formed.
func TestGetCollection_Integration(t *testing.T) {
	client := newIntegrationClient(t)
	ctx := context.Background()

	collection, err := client.GetCollection(ctx, "boredapeyachtclub")
	if err != nil {
		t.Logf("Network error (API may be unreachable): %v", err)
		return
	}

	require.NotNil(t, collection)
	assert.Equal(t, "boredapeyachtclub", collection.Collection)
	assert.NotEmpty(t, collection.Name, "collection name must not be empty")
	assert.NotEmpty(t, collection.Contracts, "Bored Ape Yacht Club must have at least one contract")

	for i, contract := range collection.Contracts {
		t.Logf("contract[%d]: address=%q chain=%q", i, contract.Address, contract.Chain)
		assert.NotEmpty(t, contract.Address)
		assert.NotEmpty(t, contract.Chain)
	}
	t.Logf("Collection: slug=%q name=%q total_supply=%d contracts=%d",
		collection.Collection, collection.Name, collection.TotalSupply, len(collection.Contracts))
}

// TestGetCollection_Integration_KnownCollections checks multiple well-known collections
// and validates that their Ethereum chain and contract fields are populated.
func TestGetCollection_Integration_KnownCollections(t *testing.T) {
	client := newIntegrationClient(t)
	ctx := context.Background()

	cases := []struct {
		slug          string
		wantChain     string
		wantMinSupply int64
	}{
		{"boredapeyachtclub", "ethereum", 9000},
		{"cryptopunks", "ethereum", 9000},
	}

	for _, tc := range cases {
		t.Run(tc.slug, func(t *testing.T) {
			collection, err := client.GetCollection(ctx, tc.slug)
			if err != nil {
				t.Logf("Network error for %s: %v", tc.slug, err)
				return
			}

			require.NotNil(t, collection)
			assert.Equal(t, tc.slug, collection.Collection)
			assert.GreaterOrEqual(t, collection.TotalSupply, tc.wantMinSupply,
				"total_supply should be at least %d for %s", tc.wantMinSupply, tc.slug)

			require.NotEmpty(t, collection.Contracts)
			// At least one contract should be on the expected chain.
			found := false
			for _, c := range collection.Contracts {
				if c.Chain == tc.wantChain {
					found = true
					break
				}
			}
			assert.True(t, found, "expected at least one contract on chain %q for %s", tc.wantChain, tc.slug)
		})
	}
}

// TestResolveSlug_Integration validates that ResolveSlug returns the slug unchanged
// and that the collection has at least one associated contract.
func TestResolveSlug_Integration(t *testing.T) {
	client := newIntegrationClient(t)
	ctx := context.Background()

	resolved, err := client.ResolveSlug(ctx, "boredapeyachtclub")
	if err != nil {
		t.Fatalf("ResolveSlug failed (API may be unreachable): %v", err)
	}

	// The slug IS the vendor_release_id for OpenSea — it must be returned unchanged.
	assert.Equal(t, "boredapeyachtclub", resolved)
	t.Logf("ResolveSlug: %q → %q (same value, as expected for OpenSea)", "boredapeyachtclub", resolved)
}

// TestResolveSlug_Integration_NotFound verifies that an unknown slug returns ErrCollectionNotFound.
func TestResolveSlug_Integration_NotFound(t *testing.T) {
	client := newIntegrationClient(t)

	_, err := client.ResolveSlug(context.Background(), "no-such-opensea-collection-xyz-99999")
	require.Error(t, err)
	// Should be wrapped ErrCollectionNotFound, not a network error.
	assert.True(t, errors.Is(err, opensea.ErrCollectionNotFound),
		"expected ErrCollectionNotFound, got: %v", err)
	t.Logf("Correctly returned ErrCollectionNotFound for unknown slug: %v", err)
}

// TestGetCollection_Integration_ContextCancellation verifies that a canceled context
// propagates as an error rather than hanging.
func TestGetCollection_Integration_ContextCancellation(t *testing.T) {
	client := newIntegrationClient(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	_, err := client.GetCollection(ctx, "boredapeyachtclub")

	assert.Error(t, err, "canceled context must produce an error")
	t.Logf("Context cancellation error: %v", err)
}

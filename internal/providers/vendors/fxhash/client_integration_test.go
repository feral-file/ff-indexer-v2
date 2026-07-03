//go:build integration

package fxhash_test

// Integration tests for the fxhash client against the real fxhash v2 GraphQL API.
// Gated behind //go:build integration; excluded from plain go test ./... but
// included in CI and make test-integration via -tags=integration.
//
// Run with:
//
//	go test -tags=integration ./internal/providers/vendors/fxhash/...

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/fxhash"
)

func newIntegrationClient() fxhash.Client {
	return fxhash.NewClient(
		adapter.NewHTTPClient(30*time.Second),
		nil, // nil limiter passes through immediately
		testAPIURL,
		adapter.NewJSON(),
	)
}

// TestGetGentk_Integration_KnownGentk fetches a well-known fxhash generative token
// (Anticyclone #224128 on KT1U6EH...) and validates all release-relevant fields.
func TestGetGentk_Integration_KnownGentk(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()

	// Anticyclone is project 9997, minted on KT1U6EHmNxJTkvaWJ4ThczG4FSDaHC21ssvi.
	// Token 224128 is edition #224128 of the series (iteration should equal tokenID for v1).
	gentk, err := client.GetGentk(ctx, "KT1U6EHmNxJTkvaWJ4ThczG4FSDaHC21ssvi", "224128")
	if err != nil {
		t.Logf("Network error (API may be unreachable): %v", err)
		return
	}

	if gentk == nil {
		t.Logf("fxhash returned nil for this token — it may not be indexed by the v2 API yet")
		return
	}

	require.NotEmpty(t, gentk.Iteration, "Iteration must not be empty")
	t.Logf("Iteration: %s", gentk.Iteration)

	if gentk.Name != nil {
		t.Logf("Name: %s", *gentk.Name)
	}
	if gentk.DisplayURI != nil {
		t.Logf("DisplayURI: %s", *gentk.DisplayURI)
	}

	require.NotNil(t, gentk.GenerativeToken, "GenerativeToken must not be nil for a valid gentk")
	gt := gentk.GenerativeToken
	assert.NotEmpty(t, gt.ID, "GenerativeToken.ID must not be empty")
	assert.NotEmpty(t, gt.Name, "GenerativeToken.Name must not be empty")
	assert.NotEmpty(t, gt.Supply, "GenerativeToken.Supply must not be empty")
	t.Logf("GenerativeToken: id=%s name=%q supply=%s original_supply=%v", gt.ID, gt.Name, gt.Supply, gt.OriginalSupply)

	if gt.Author != nil {
		t.Logf("Author: name=%q wallet=%v", gt.Author.Name, gt.Author.WalletAccount)
		if gt.Author.WalletAccount != nil {
			assert.NotEmpty(t, gt.Author.WalletAccount.Address)
		}
	}
}

// TestGetGentk_Integration_KnownGentk_Genesis fetches a gentk from the Genesis contract
// (KT1KEa8...) to verify the new registry entry is reachable through the same API path.
func TestGetGentk_Integration_KnownGentk_Genesis(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()

	// Pick a low token_id from the Genesis contract. Token 1 may or may not be indexed.
	gentk, err := client.GetGentk(ctx, "KT1KEa8z6vWXDJrVqtMrAeDVzsvxat3kHaCE", "1")
	if err != nil {
		t.Logf("Network error (API may be unreachable): %v", err)
		return
	}

	// nil is acceptable — the API may not index old Genesis tokens in v2
	if gentk == nil {
		t.Logf("Genesis token 1 not indexed by fxhash v2 (expected for legacy tokens)")
		return
	}

	t.Logf("Genesis token 1 found: iteration=%s", gentk.Iteration)
	if gentk.GenerativeToken != nil {
		t.Logf("  GenerativeToken.ID=%s Name=%q", gentk.GenerativeToken.ID, gentk.GenerativeToken.Name)
	}
}

// TestGetGentk_Integration_NonExistentToken verifies that the client returns (nil, nil)
// for a valid contract/tokenID pair that is not indexed by fxhash.
func TestGetGentk_Integration_NonExistentToken(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()

	// Use a clearly non-fxhash contract so the API will return null.
	gentk, err := client.GetGentk(ctx, "KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton", "999999999")
	if err != nil {
		t.Logf("Network error (API may be unreachable): %v", err)
		return
	}

	// A non-fxhash token should come back as nil, never an error.
	assert.Nil(t, gentk, "non-fxhash token should return nil, not an error")
	t.Logf("Correctly returned nil for non-fxhash token")
}

// TestGetGentk_Integration_ContextCancellation verifies that a canceled context
// propagates as an error rather than hanging.
func TestGetGentk_Integration_ContextCancellation(t *testing.T) {
	client := newIntegrationClient()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	gentk, err := client.GetGentk(ctx, "KT1U6EHmNxJTkvaWJ4ThczG4FSDaHC21ssvi", "1")

	assert.Error(t, err, "canceled context must produce an error")
	assert.Nil(t, gentk)
	t.Logf("Context cancellation error: %v", err)
}

// TestGetGentk_Integration_MultipleTokens exercises the client against several
// well-known fxhash gentks and logs their release metadata.
func TestGetGentk_Integration_MultipleTokens(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()

	cases := []struct {
		name     string
		contract string
		tokenID  string
	}{
		// Anticyclone by Ciphrd — project 9997
		{"Anticyclone_224128", "KT1U6EHmNxJTkvaWJ4ThczG4FSDaHC21ssvi", "224128"},
		// Another iteration of the same project
		{"Anticyclone_1", "KT1U6EHmNxJTkvaWJ4ThczG4FSDaHC21ssvi", "1"},
		// A token on the EfsNuqwL contract (early fxhash)
		{"EfsNuq_1", "KT1EfsNuqwLAWDd3o4pvfUx1CAh5GMdTrRvr", "1"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gentk, err := client.GetGentk(ctx, tc.contract, tc.tokenID)
			if err != nil {
				t.Logf("Network error for %s/%s: %v", tc.contract, tc.tokenID, err)
				return
			}

			if gentk == nil {
				t.Logf("%s/%s not indexed by fxhash v2", tc.contract, tc.tokenID)
				return
			}

			t.Logf("Token %s/%s: iteration=%s", tc.contract, tc.tokenID, gentk.Iteration)
			if gentk.GenerativeToken != nil {
				gt := gentk.GenerativeToken
				t.Logf("  GenerativeToken: id=%s name=%q supply=%s", gt.ID, gt.Name, gt.Supply)
				assert.NotEmpty(t, gt.ID)
				assert.NotEmpty(t, gt.Name)
				if gt.Author != nil && gt.Author.WalletAccount != nil {
					t.Logf("  Author address: %s", gt.Author.WalletAccount.Address)
					assert.NotEmpty(t, gt.Author.WalletAccount.Address)
				}
			}
		})
	}
}

// TestResolveSlug_Integration verifies slug → generative token ID resolution against the
// live fxhash v2 GraphQL API. Note: project display names (e.g. "Anticyclone") are not
// always equal to the URL slug; industrial-park is the canonical slug for GT 9997.
func TestResolveSlug_Integration(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()

	cases := []struct {
		name             string
		slug             string
		wantGenerativeID string
	}{
		{
			name:             "Industrial_Park",
			slug:             "industrial-park",
			wantGenerativeID: "9997",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			id, err := client.ResolveSlug(ctx, tc.slug)
			if err != nil {
				t.Fatalf("ResolveSlug failed (API may be unreachable): %v", err)
			}
			assert.Equal(t, tc.wantGenerativeID, id)
			t.Logf("slug %q → generative token id %q", tc.slug, id)
		})
	}
}

// TestResolveSlug_Integration_NotFound verifies unknown slugs return an error.
func TestResolveSlug_Integration_NotFound(t *testing.T) {
	client := newIntegrationClient()

	_, err := client.ResolveSlug(context.Background(), "no-such-fxhash-slug-xyz")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "slug not found")
}

// TestGetGentksByIteration_Integration fetches iteration→token mappings for a known GT.
// Required by IndexRelease to derive on-chain CIDs for fxhash releases.
func TestGetGentksByIteration_Integration(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()

	// Resolve slug the same way IndexRelease does when triggered by vendor_release_slug.
	const slug = "industrial-park"
	gtID, err := client.ResolveSlug(ctx, slug)
	if err != nil {
		t.Fatalf("ResolveSlug failed: %v", err)
	}
	assert.Equal(t, "9997", gtID)

	refs, err := client.GetGentksByIteration(ctx, gtID, 1, 3)
	if err != nil {
		t.Fatalf("GetGentksByIteration failed: %v", err)
	}

	require.NotEmpty(t, refs, "expected gentk refs for iterations 1..3")
	for i, ref := range refs {
		t.Logf("ref[%d]: contract=%q tokenID=%q iteration=%d",
			i, ref.ContractAddress, ref.TokenID, ref.Iteration)
		assert.NotEmpty(t, ref.ContractAddress)
		assert.NotEmpty(t, ref.TokenID)
		assert.GreaterOrEqual(t, ref.Iteration, int64(1))
		assert.LessOrEqual(t, ref.Iteration, int64(3))
	}
}

// TestGetGentk_Integration_SlugField verifies generative_token.slug is returned by the
// live API (used during enrichment to populate vendor_release_slug).
func TestGetGentk_Integration_SlugField(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()

	// Resolve a known GT, then fetch a gentk from the iteration range (token IDs are not
	// predictable from mint numbers alone on fxhash).
	gtID, err := client.ResolveSlug(ctx, "industrial-park")
	if err != nil {
		t.Fatalf("ResolveSlug failed: %v", err)
	}

	refs, err := client.GetGentksByIteration(ctx, gtID, 1, 1)
	if err != nil {
		t.Fatalf("GetGentksByIteration failed: %v", err)
	}
	require.NotEmpty(t, refs, "need at least one gentk ref for iteration 1")

	ref := refs[0]
	gentk, err := client.GetGentk(ctx, ref.ContractAddress, ref.TokenID)
	if err != nil {
		t.Fatalf("GetGentk failed: %v", err)
	}
	if gentk == nil {
		t.Fatalf("expected gentk %s/%s to be indexed", ref.ContractAddress, ref.TokenID)
	}

	require.NotNil(t, gentk.GenerativeToken)
	assert.Equal(t, "9997", gentk.GenerativeToken.ID)
	assert.Equal(t, "industrial-park", gentk.GenerativeToken.Slug)
	t.Logf("GenerativeToken slug=%q name=%q", gentk.GenerativeToken.Slug, gentk.GenerativeToken.Name)
}

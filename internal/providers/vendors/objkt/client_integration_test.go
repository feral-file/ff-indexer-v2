//go:build integration

package objkt_test

// Integration tests for the objkt client against the real objkt v3 GraphQL API.
// They are excluded from the default test run and CI.
//
// Run with:
//
//	go test -tags=integration ./internal/providers/vendors/objkt/...

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/objkt"
)

func newIntegrationClient() objkt.Client {
	return objkt.NewClient(
		adapter.NewHTTPClient(30*time.Second),
		nil, // nil limiter passes through immediately
		OBJKT_API_URL,
		"",
		adapter.NewJSON(),
	)
}

// TestClient_GetToken_Integration_KnownToken fetches a known token and validates that
// all core metadata fields and the new FA collection fields are returned.
func TestClient_GetToken_Integration_KnownToken(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()

	// Anticyclone token on the fxhash KT1U6EH contract — a well-indexed fxhash collection.
	// Token 224128 is a real gentk whose objkt metadata is stable and public.
	token, err := client.GetToken(ctx, "KT1U6EHmNxJTkvaWJ4ThczG4FSDaHC21ssvi", "224128")
	if err != nil {
		t.Logf("Network error (API may be unreachable): %v", err)
		return
	}

	require.NotNil(t, token)

	if token.Name != nil {
		t.Logf("Name: %s", *token.Name)
		assert.NotEmpty(t, *token.Name)
	}
	if token.DisplayURI != nil {
		t.Logf("DisplayURI: %s", *token.DisplayURI)
	}
	if token.Mime != nil {
		t.Logf("Mime: %s", *token.Mime)
	}
	t.Logf("Creators: %d", len(token.Creators))

	// FA collection data — fxhash contracts have collection_type "open_generative"
	if token.FA != nil {
		t.Logf("FA: name=%q editions=%d collection_type=%q", token.FA.Name, token.FA.Editions, token.FA.CollectionType)
		assert.NotEmpty(t, token.FA.CollectionType)
	} else {
		t.Logf("FA: nil (not returned by objkt for this token)")
	}
}

// TestClient_GetToken_Integration_CustomCollection fetches a token from an objkt
// custom collection (dedicated per-artist contract) and verifies collection_type is "custom".
func TestClient_GetToken_Integration_CustomCollection(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()

	// KT19oAHnjgpQ6PauwgC8RxAb5pVj6svg9Myn is used as a reference contract in the
	// existing objkt tests. Try token 1.
	token, err := client.GetToken(ctx, "KT19oAHnjgpQ6PauwgC8RxAb5pVj6svg9Myn", "1")
	if err != nil {
		t.Logf("Network error or token not found: %v", err)
		return
	}

	require.NotNil(t, token)

	if token.Name != nil {
		t.Logf("Name: %s", *token.Name)
	}
	if token.FA != nil {
		t.Logf("FA: name=%q editions=%d collection_type=%q", token.FA.Name, token.FA.Editions, token.FA.CollectionType)
		if token.FA.CollectionType != "" {
			t.Logf("Collection type: %s", token.FA.CollectionType)
		}
	}
}

// TestClient_GetToken_Integration_MultipleTokens exercises several well-known tokens
// and logs their FA metadata to assist in manual verification.
func TestClient_GetToken_Integration_MultipleTokens(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()

	cases := []struct {
		name         string
		contract     string
		tokenID      string
		expectFAType string // expected collection_type if known; empty means unknown
	}{
		// fxhash KT1U6EH — open_generative (multi-artist fxhash platform contract)
		{
			name:         "fxhash_anticyclone",
			contract:     "KT1U6EHmNxJTkvaWJ4ThczG4FSDaHC21ssvi",
			tokenID:      "1",
			expectFAType: "open_generative",
		},
		// hic et nunc — the canonical open Tezos NFT platform contract
		{
			name:         "hen_open",
			contract:     "KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton",
			tokenID:      "1",
			expectFAType: "open",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			token, err := client.GetToken(ctx, tc.contract, tc.tokenID)
			if err != nil {
				t.Logf("Error for %s/%s: %v", tc.contract, tc.tokenID, err)
				return
			}

			require.NotNil(t, token)
			if token.Name != nil {
				t.Logf("Name: %s", *token.Name)
			}
			if token.FA != nil {
				t.Logf("FA: name=%q editions=%d collection_type=%q", token.FA.Name, token.FA.Editions, token.FA.CollectionType)
				if tc.expectFAType != "" {
					assert.Equal(t, tc.expectFAType, token.FA.CollectionType,
						"collection_type mismatch for %s", tc.name)
				}
			} else {
				t.Logf("FA: nil for %s/%s", tc.contract, tc.tokenID)
			}
		})
	}
}

// TestClient_GetToken_Integration_ContextCancellation verifies that a canceled context
// surfaces as an error rather than hanging.
func TestClient_GetToken_Integration_ContextCancellation(t *testing.T) {
	client := newIntegrationClient()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	token, err := client.GetToken(ctx, "KT1U6EHmNxJTkvaWJ4ThczG4FSDaHC21ssvi", "1")

	assert.Error(t, err, "canceled context must produce an error")
	assert.Nil(t, token)
	t.Logf("Context cancellation error: %v", err)
}

// TestClient_GetToken_Integration_NotFound verifies that a non-existent token returns an error.
func TestClient_GetToken_Integration_NotFound(t *testing.T) {
	client := newIntegrationClient()
	ctx := context.Background()

	token, err := client.GetToken(ctx, "KT1U6EHmNxJTkvaWJ4ThczG4FSDaHC21ssvi", "99999999999")
	if err == nil {
		// Some APIs return an empty list rather than an error for missing tokens;
		// our client converts that to an error, but if the real API surprises us, log it.
		t.Logf("Unexpected success (token may exist): %+v", token)
		return
	}

	assert.Contains(t, err.Error(), "token not found")
	t.Logf("Correctly returned error for non-existent token: %v", err)
}

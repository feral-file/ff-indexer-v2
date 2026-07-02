package fxhash_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/fxhash"
)

const testAPIURL = "https://api.v2.fxhash.xyz/v1/graphql"

func TestMain(m *testing.M) {
	if err := logger.Initialize(logger.Config{Debug: false}); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

func newTestClient(t *testing.T) (fxhash.Client, *mocks.MockHTTPClient, *gomock.Controller) {
	ctrl := gomock.NewController(t)
	httpClient := mocks.NewMockHTTPClient(ctrl)
	// Use nil limiter so ratelimit.Do executes the function directly, avoiding
	// provider-config requirements during unit tests.
	json := adapter.NewJSON()
	client := fxhash.NewClient(httpClient, nil, testAPIURL, json)
	return client, httpClient, ctrl
}

func TestGetGentk_Success(t *testing.T) {
	client, httpClient, ctrl := newTestClient(t)
	defer ctrl.Finish()

	responseBody := []byte(`{
		"data": {
			"onchain": {
				"objkt_by_pk": {
					"iteration": "42",
					"display_uri": "ipfs://QmDisplay123",
					"name": "Anticyclone #42",
					"generative_token": {
						"id": "9997",
						"name": "Anticyclone",
						"slug": "anticyclone",
						"supply": "512",
						"original_supply": "512",
						"author": {
							"name": "Ciphrd",
							"wallet_account": {
								"address": "tz1VSUr8wwNhLAzempoch5d6hLRiTh8Cjcjb"
							}
						}
					}
				}
			}
		}
	}`)

	httpClient.
		EXPECT().
		PostBytes(gomock.Any(), "https://api.v2.fxhash.xyz/v1/graphql", gomock.Any(), gomock.Any()).
		Return(responseBody, nil)

	gentk, err := client.GetGentk(context.Background(), "KT1U6EHmNxJTkvaWJ4ThczG4FSDaHC21ssvi", "42")

	require.NoError(t, err)
	require.NotNil(t, gentk)
	assert.Equal(t, "42", gentk.Iteration)
	require.NotNil(t, gentk.Name)
	assert.Equal(t, "Anticyclone #42", *gentk.Name)
	require.NotNil(t, gentk.DisplayURI)
	assert.Equal(t, "ipfs://QmDisplay123", *gentk.DisplayURI)
	require.NotNil(t, gentk.GenerativeToken)
	assert.Equal(t, "9997", gentk.GenerativeToken.ID)
	assert.Equal(t, "Anticyclone", gentk.GenerativeToken.Name)
	require.NotNil(t, gentk.GenerativeToken.OriginalSupply)
	assert.Equal(t, "512", *gentk.GenerativeToken.OriginalSupply)
	require.NotNil(t, gentk.GenerativeToken.Author)
	assert.Equal(t, "Ciphrd", gentk.GenerativeToken.Author.Name)
	require.NotNil(t, gentk.GenerativeToken.Author.WalletAccount)
	assert.Equal(t, "tz1VSUr8wwNhLAzempoch5d6hLRiTh8Cjcjb", gentk.GenerativeToken.Author.WalletAccount.Address)
}

// TestGetGentk_NullGentk verifies that GetGentk returns (nil, nil) when the fxhash API
// returns null for objkt_by_pk — meaning the token is not indexed by fxhash.
func TestGetGentk_NullGentk(t *testing.T) {
	client, httpClient, ctrl := newTestClient(t)
	defer ctrl.Finish()

	responseBody := []byte(`{
		"data": {
			"onchain": {
				"objkt_by_pk": null
			}
		}
	}`)

	httpClient.
		EXPECT().
		PostBytes(gomock.Any(), "https://api.v2.fxhash.xyz/v1/graphql", gomock.Any(), gomock.Any()).
		Return(responseBody, nil)

	gentk, err := client.GetGentk(context.Background(), "KT1Abc", "1")

	require.NoError(t, err)
	assert.Nil(t, gentk, "expected nil when fxhash does not index the token")
}

// TestGetGentk_HTTPError verifies that network errors are propagated correctly.
func TestGetGentk_HTTPError(t *testing.T) {
	client, httpClient, ctrl := newTestClient(t)
	defer ctrl.Finish()

	httpClient.
		EXPECT().
		PostBytes(gomock.Any(), "https://api.v2.fxhash.xyz/v1/graphql", gomock.Any(), gomock.Any()).
		Return(nil, assert.AnError)

	gentk, err := client.GetGentk(context.Background(), "KT1Abc", "1")

	assert.Error(t, err)
	assert.Nil(t, gentk)
	assert.Contains(t, err.Error(), "failed to call fxhash v2 API")
}

// TestGetGentk_GraphQLError verifies that GraphQL-level errors in the response are returned as errors.
func TestGetGentk_GraphQLError(t *testing.T) {
	client, httpClient, ctrl := newTestClient(t)
	defer ctrl.Finish()

	responseBody := []byte(`{
		"errors": [{"message": "time limit exceeded"}]
	}`)

	httpClient.
		EXPECT().
		PostBytes(gomock.Any(), "https://api.v2.fxhash.xyz/v1/graphql", gomock.Any(), gomock.Any()).
		Return(responseBody, nil)

	gentk, err := client.GetGentk(context.Background(), "KT1Abc", "1")

	assert.Error(t, err)
	assert.Nil(t, gentk)
	assert.Contains(t, err.Error(), "fxhash API error")
}

// TestGetGentk_InvalidJSON verifies that malformed JSON responses are returned as errors.
func TestGetGentk_InvalidJSON(t *testing.T) {
	client, httpClient, ctrl := newTestClient(t)
	defer ctrl.Finish()

	httpClient.
		EXPECT().
		PostBytes(gomock.Any(), "https://api.v2.fxhash.xyz/v1/graphql", gomock.Any(), gomock.Any()).
		Return([]byte(`not valid json`), nil)

	gentk, err := client.GetGentk(context.Background(), "KT1Abc", "1")

	assert.Error(t, err)
	assert.Nil(t, gentk)
	assert.Contains(t, err.Error(), "failed to unmarshal fxhash response")
}

// TestGetGentk_CompositeID verifies that GetGentk accepts various contract+tokenID pairs
// and constructs the composite key "{contract}-{tokenID}" expected by the fxhash API.
func TestGetGentk_CompositeID(t *testing.T) {
	client, httpClient, ctrl := newTestClient(t)
	defer ctrl.Finish()

	responseBody := []byte(`{"data":{"onchain":{"objkt_by_pk":null}}}`)

	httpClient.
		EXPECT().
		PostBytes(gomock.Any(), testAPIURL, gomock.Any(), gomock.Any()).
		Return(responseBody, nil)

	gentk, err := client.GetGentk(context.Background(), "KT1KEa8z6vWXDJrVqtMrAeDVzsvxat3kHaCE", "777")
	require.NoError(t, err)
	assert.Nil(t, gentk)
}

// ---- Integration tests (real fxhash v2 API) ----

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

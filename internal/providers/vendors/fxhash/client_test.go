package fxhash_test

import (
	"context"
	"os"
	"testing"

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

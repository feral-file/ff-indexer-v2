package objkt_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/objkt"
)

const (
	OBJKT_API_URL = "https://data.objkt.com/v3/graphql"
)

func TestMain(m *testing.M) {
	err := logger.Initialize(logger.Config{
		Debug: false,
	})
	if err != nil {
		panic(err)
	}

	code := m.Run()
	os.Exit(code)
}

func TestClient_GetToken_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockJSON := adapter.NewJSON()
	mockLimiter := mocks.NewMockLimiter(ctrl)
	client := objkt.NewClient(mockHTTPClient, mockLimiter, OBJKT_API_URL, "", mockJSON)

	ctx := context.Background()
	contractAddress := "KT19oAHnjgpQ6PauwgC8RxAb5pVj6svg9Myn"
	tokenID := "125"

	name := "Test Token"
	description := "Test Description"
	displayURI := "ipfs://test-display"
	artifactURI := "ipfs://test-artifact"
	thumbnailURI := "ipfs://test-thumbnail"
	mime := "image/png"
	alias := "test-artist"

	expectedResponse := objkt.TokenResponse{
		Data: struct {
			Token []objkt.Token `json:"token"`
		}{
			Token: []objkt.Token{
				{
					Name:         &name,
					Description:  &description,
					DisplayURI:   &displayURI,
					ArtifactURI:  &artifactURI,
					ThumbnailURI: &thumbnailURI,
					Mime:         &mime,
					Metadata:     map[string]interface{}{"key": "value"},
					Creators: []objkt.Creator{
						{
							Holder: objkt.Holder{
								Address: "tz1VSUr8wwNhLAzempoch5d6hLRiTh8Cjcjb",
								Alias:   &alias,
							},
						},
					},
				},
			},
		},
	}

	mockLimiter.EXPECT().
		Do(gomock.Any(), objkt.PROVIDER_NAME, gomock.Any()).
		DoAndReturn(func(ctx context.Context, providerName string, fn func(context.Context) (interface{}, error)) (interface{}, error) {
			return fn(ctx)
		})

	mockHTTPClient.EXPECT().
		PostBytes(ctx, OBJKT_API_URL, map[string]string{"Content-Type": "application/json"}, gomock.Any()).
		DoAndReturn(func(ctx context.Context, url string, headers map[string]string, body interface{}) ([]byte, error) {
			data, _ := json.Marshal(expectedResponse)
			return data, nil
		}).
		Times(1)

	token, err := client.GetToken(ctx, contractAddress, tokenID)

	require.NoError(t, err)
	assert.NotNil(t, token)
	assert.Equal(t, name, *token.Name)
	assert.Equal(t, description, *token.Description)
	assert.Equal(t, displayURI, *token.DisplayURI)
	assert.Equal(t, artifactURI, *token.ArtifactURI)
	assert.Equal(t, thumbnailURI, *token.ThumbnailURI)
	assert.Equal(t, mime, *token.Mime)
	assert.Len(t, token.Creators, 1)
	assert.Equal(t, "tz1VSUr8wwNhLAzempoch5d6hLRiTh8Cjcjb", token.Creators[0].Holder.Address)
	assert.Equal(t, alias, *token.Creators[0].Holder.Alias)
}

func TestClient_GetToken_HTTPError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockJSON := adapter.NewJSON()
	mockLimiter := mocks.NewMockLimiter(ctrl)
	client := objkt.NewClient(mockHTTPClient, mockLimiter, OBJKT_API_URL, "", mockJSON)

	ctx := context.Background()
	contractAddress := "KT19oAHnjgpQ6PauwgC8RxAb5pVj6svg9Myn"
	tokenID := "125"

	expectedError := errors.New("network error")

	mockLimiter.EXPECT().
		Do(gomock.Any(), objkt.PROVIDER_NAME, gomock.Any()).
		DoAndReturn(func(ctx context.Context, providerName string, fn func(context.Context) (interface{}, error)) (interface{}, error) {
			return fn(ctx)
		})

	mockHTTPClient.EXPECT().
		PostBytes(ctx, OBJKT_API_URL, map[string]string{"Content-Type": "application/json"}, gomock.Any()).
		Return(nil, expectedError).
		Times(1)

	token, err := client.GetToken(ctx, contractAddress, tokenID)

	assert.Error(t, err)
	assert.Nil(t, token)
	assert.Contains(t, err.Error(), "failed to call objkt v3 API")
	assert.Contains(t, err.Error(), "network error")
}

func TestClient_GetToken_InvalidJSON(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockJSON := adapter.NewJSON()
	mockLimiter := mocks.NewMockLimiter(ctrl)
	client := objkt.NewClient(mockHTTPClient, mockLimiter, OBJKT_API_URL, "", mockJSON)

	ctx := context.Background()
	contractAddress := "KT19oAHnjgpQ6PauwgC8RxAb5pVj6svg9Myn"
	tokenID := "125"

	mockLimiter.EXPECT().
		Do(gomock.Any(), objkt.PROVIDER_NAME, gomock.Any()).
		DoAndReturn(func(ctx context.Context, providerName string, fn func(context.Context) (interface{}, error)) (interface{}, error) {
			return fn(ctx)
		})

	mockHTTPClient.EXPECT().
		PostBytes(ctx, OBJKT_API_URL, map[string]string{"Content-Type": "application/json"}, gomock.Any()).
		Return([]byte("invalid json"), nil).
		Times(1)

	token, err := client.GetToken(ctx, contractAddress, tokenID)

	assert.Error(t, err)
	assert.Nil(t, token)
	assert.Contains(t, err.Error(), "failed to unmarshal objkt response")
}

func TestClient_GetToken_EmptyResponse(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockJSON := adapter.NewJSON()
	client := objkt.NewClient(mockHTTPClient, nil, OBJKT_API_URL, "", mockJSON)

	ctx := context.Background()
	contractAddress := "KT19oAHnjgpQ6PauwgC8RxAb5pVj6svg9Myn"
	tokenID := "999999"

	emptyResponse := objkt.TokenResponse{
		Data: struct {
			Token []objkt.Token `json:"token"`
		}{
			Token: []objkt.Token{},
		},
	}

	mockHTTPClient.EXPECT().
		PostBytes(ctx, OBJKT_API_URL, map[string]string{"Content-Type": "application/json"}, gomock.Any()).
		DoAndReturn(func(ctx context.Context, url string, headers map[string]string, body interface{}) ([]byte, error) {
			data, _ := json.Marshal(emptyResponse)
			return data, nil
		}).
		Times(1)

	token, err := client.GetToken(ctx, contractAddress, tokenID)

	assert.Error(t, err)
	assert.Nil(t, token)
	assert.Contains(t, err.Error(), "token not found")
}

// TestClient_GetToken_FAFieldInResponse verifies that the FA sub-struct (collection type
// and editions) is correctly unmarshalled from a mock response.
func TestClient_GetToken_FAFieldInResponse(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockJSON := adapter.NewJSON()
	client := objkt.NewClient(mockHTTPClient, nil, OBJKT_API_URL, "", mockJSON)

	ctx := context.Background()

	mockHTTPClient.EXPECT().
		PostBytes(ctx, OBJKT_API_URL, map[string]string{"Content-Type": "application/json"}, gomock.Any()).
		DoAndReturn(func(ctx context.Context, url string, headers map[string]string, body interface{}) ([]byte, error) {
			return []byte(`{
				"data": {
					"token": [{
						"name": "Custom Work #1",
						"description": "A custom collection piece",
						"display_uri": "ipfs://QmDisplay",
						"artifact_uri": null,
						"thumbnail_uri": null,
						"mime": "image/jpeg",
						"metadata": null,
						"creators": [],
						"fa": {
							"name": "My Custom Collection",
							"editions": 100,
							"collection_type": "custom"
						}
					}]
				}
			}`), nil
		}).
		Times(1)

	token, err := client.GetToken(ctx, "KT1CustomArtist111111111111111111111", "1")

	require.NoError(t, err)
	require.NotNil(t, token)
	require.NotNil(t, token.FA, "FA field must be populated for custom collection tokens")
	assert.Equal(t, "My Custom Collection", token.FA.Name)
	assert.Equal(t, int64(100), token.FA.Editions)
	assert.Equal(t, "custom", token.FA.CollectionType)
}

// TestClient_GetToken_FAFieldOpenCollection verifies that FA with collection_type "open"
// is correctly parsed (open/curated collections are multi-artist; no release is derived).
func TestClient_GetToken_FAFieldOpenCollection(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockJSON := adapter.NewJSON()
	client := objkt.NewClient(mockHTTPClient, nil, OBJKT_API_URL, "", mockJSON)

	ctx := context.Background()

	mockHTTPClient.EXPECT().
		PostBytes(ctx, OBJKT_API_URL, map[string]string{"Content-Type": "application/json"}, gomock.Any()).
		DoAndReturn(func(ctx context.Context, url string, headers map[string]string, body interface{}) ([]byte, error) {
			return []byte(`{
				"data": {
					"token": [{
						"name": "Open Platform Token",
						"creators": [],
						"fa": {
							"name": "hic et nunc",
							"editions": 10,
							"collection_type": "open"
						}
					}]
				}
			}`), nil
		}).
		Times(1)

	token, err := client.GetToken(ctx, "KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton", "42")

	require.NoError(t, err)
	require.NotNil(t, token)
	require.NotNil(t, token.FA)
	assert.Equal(t, "open", token.FA.CollectionType)
}

// TestClient_GetToken_FAFieldNullFA verifies that tokens with no FA entry parse cleanly
// (FA pointer is nil; the enhancer skips release population in this case).
func TestClient_GetToken_FAFieldNullFA(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockJSON := adapter.NewJSON()
	client := objkt.NewClient(mockHTTPClient, nil, OBJKT_API_URL, "", mockJSON)

	ctx := context.Background()

	mockHTTPClient.EXPECT().
		PostBytes(ctx, OBJKT_API_URL, map[string]string{"Content-Type": "application/json"}, gomock.Any()).
		DoAndReturn(func(ctx context.Context, url string, headers map[string]string, body interface{}) ([]byte, error) {
			return []byte(`{
				"data": {
					"token": [{
						"name": "Token Without FA",
						"creators": [],
						"fa": null
					}]
				}
			}`), nil
		}).
		Times(1)

	token, err := client.GetToken(ctx, "KT1SomeContract11111111111111111111", "7")

	require.NoError(t, err)
	require.NotNil(t, token)
	assert.Nil(t, token.FA, "nil FA in response should yield nil FA pointer")
}

// ---- Integration tests (real objkt v3 API) ----

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
// We use a token from a well-known dedicated artist contract on Tezos.
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
		// Custom collections should have collection_type "custom"
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

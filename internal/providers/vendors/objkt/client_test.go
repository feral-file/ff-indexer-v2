package objkt_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"

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

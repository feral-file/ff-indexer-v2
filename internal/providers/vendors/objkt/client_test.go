package objkt_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/objkt"
)

const (
	OBJKT_API_URL = "https://data.objkt.com/v3/graphql"
)

func TestMain(m *testing.M) {
	// Initialize logger for tests
	err := logger.Initialize(logger.Config{
		Debug: false,
	})
	if err != nil {
		panic(err)
	}

	code := m.Run()
	os.Exit(code)
}

// TestClient_GetToken_Success tests successful token retrieval with mock
func TestClient_GetToken_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockJSON := adapter.NewJSON()
	mockRateLimitProxy := mocks.NewMockRateLimitProxy(ctrl)
	client := objkt.NewClient(mockHTTPClient, mockRateLimitProxy, OBJKT_API_URL, "", mockJSON)

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

	mockRateLimitProxy.EXPECT().
		Request(gomock.Any(), objkt.PROVIDER_NAME, gomock.Any()).
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

// TestClient_GetToken_HTTPError tests error handling when HTTP client returns an error
func TestClient_GetToken_HTTPError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockJSON := adapter.NewJSON()
	mockRateLimitProxy := mocks.NewMockRateLimitProxy(ctrl)
	client := objkt.NewClient(mockHTTPClient, mockRateLimitProxy, OBJKT_API_URL, "", mockJSON)

	ctx := context.Background()
	contractAddress := "KT19oAHnjgpQ6PauwgC8RxAb5pVj6svg9Myn"
	tokenID := "125"

	expectedError := errors.New("network error")

	mockRateLimitProxy.EXPECT().
		Request(gomock.Any(), objkt.PROVIDER_NAME, gomock.Any()).
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

// TestClient_GetToken_InvalidJSON tests error handling when API returns invalid JSON
func TestClient_GetToken_InvalidJSON(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockJSON := adapter.NewJSON()
	mockRateLimitProxy := mocks.NewMockRateLimitProxy(ctrl)
	client := objkt.NewClient(mockHTTPClient, mockRateLimitProxy, OBJKT_API_URL, "", mockJSON)

	ctx := context.Background()
	contractAddress := "KT19oAHnjgpQ6PauwgC8RxAb5pVj6svg9Myn"
	tokenID := "125"

	mockRateLimitProxy.EXPECT().
		Request(gomock.Any(), objkt.PROVIDER_NAME, gomock.Any()).
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

// TestClient_GetToken_EmptyResponse tests handling when no tokens are found
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

// Integration tests - these test against the real objkt v3 API

func TestClient_GetToken_Integration(t *testing.T) {
	t.Skip("Temporarily skipping Objkt integration test (CI blocked by upstream)")

	httpClient := adapter.NewHTTPClient(30 * time.Second)
	jsonAdapter := adapter.NewJSON()
	client := objkt.NewClient(httpClient, nil, OBJKT_API_URL, "", jsonAdapter)

	ctx := context.Background()

	// Test cases with actual contract addresses and token IDs
	testCases := []struct {
		name            string
		contractAddress string
		tokenID         string
	}{
		{
			name:            "token_KT19oAHnjgpQ6PauwgC8RxAb5pVj6svg9Myn_125",
			contractAddress: "KT19oAHnjgpQ6PauwgC8RxAb5pVj6svg9Myn",
			tokenID:         "125",
		},
		{
			name:            "token_KT1EfsNuqwLAWDd3o4pvfUx1CAh5GMdTrRvr_224758",
			contractAddress: "KT1EfsNuqwLAWDd3o4pvfUx1CAh5GMdTrRvr",
			tokenID:         "224758",
		},
		{
			name:            "token_KT1EfsNuqwLAWDd3o4pvfUx1CAh5GMdTrRvr_224212",
			contractAddress: "KT1EfsNuqwLAWDd3o4pvfUx1CAh5GMdTrRvr",
			tokenID:         "224212",
		},
		{
			name:            "token_KT1U6EHmNxJTkvaWJ4ThczG4FSDaHC21ssvi_1223343",
			contractAddress: "KT1U6EHmNxJTkvaWJ4ThczG4FSDaHC21ssvi",
			tokenID:         "1223343",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			token, err := client.GetToken(ctx, tc.contractAddress, tc.tokenID)

			require.NoError(t, err, "Failed to fetch token for contract: %s, tokenID: %s", tc.contractAddress, tc.tokenID)
			require.NotNil(t, token, "Token should not be nil for contract: %s, tokenID: %s", tc.contractAddress, tc.tokenID)

			// Validate response structure
			// Note: Some fields might be nil based on the actual token data
			if token.Name != nil {
				assert.NotEmpty(t, *token.Name, "Token name should not be empty if present")
				t.Logf("Token Name: %s", *token.Name)
			}

			if token.Description != nil {
				t.Logf("Token Description: %s", *token.Description)
			}

			if token.DisplayURI != nil {
				assert.NotEmpty(t, *token.DisplayURI, "Display URI should not be empty if present")
				t.Logf("Display URI: %s", *token.DisplayURI)
			}

			if token.ArtifactURI != nil {
				assert.NotEmpty(t, *token.ArtifactURI, "Artifact URI should not be empty if present")
				t.Logf("Artifact URI: %s", *token.ArtifactURI)
			}

			if token.ThumbnailURI != nil {
				t.Logf("Thumbnail URI: %s", *token.ThumbnailURI)
			}

			if token.Mime != nil {
				assert.NotEmpty(t, *token.Mime, "Mime type should not be empty if present")
				t.Logf("Mime Type: %s", *token.Mime)
			}

			// Validate creators
			if len(token.Creators) > 0 {
				assert.NotEmpty(t, token.Creators[0].Holder.Address, "Creator address should not be empty")
				t.Logf("Creator Address: %s", token.Creators[0].Holder.Address)

				if token.Creators[0].Holder.Alias != nil {
					t.Logf("Creator Alias: %s", *token.Creators[0].Holder.Alias)
				}
			}

			// Log metadata if present
			if token.Metadata != nil {
				t.Logf("Metadata: %+v", token.Metadata)
			}
		})
	}
}

// TestClient_GetToken_Integration_InvalidToken tests error handling with invalid token
func TestClient_GetToken_Integration_InvalidToken(t *testing.T) {
	t.Skip("Temporarily skipping Objkt integration test (CI blocked by upstream)")

	httpClient := adapter.NewHTTPClient(30 * time.Second)
	jsonAdapter := adapter.NewJSON()
	client := objkt.NewClient(httpClient, nil, OBJKT_API_URL, "", jsonAdapter)

	ctx := context.Background()
	invalidContract := "KT1InvalidContractAddress"
	invalidTokenID := "999999999"

	token, err := client.GetToken(ctx, invalidContract, invalidTokenID)

	assert.Error(t, err)
	assert.Nil(t, token)
	assert.Contains(t, err.Error(), "token not found")
	t.Logf("Expected error: %v", err)
}

// TestClient_GetToken_Integration_ContextCancellation tests context cancellation
func TestClient_GetToken_Integration_ContextCancellation(t *testing.T) {
	httpClient := adapter.NewHTTPClient(30 * time.Second)
	jsonAdapter := adapter.NewJSON()
	client := objkt.NewClient(httpClient, nil, OBJKT_API_URL, "", jsonAdapter)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	contractAddress := "KT19oAHnjgpQ6PauwgC8RxAb5pVj6svg9Myn"
	tokenID := "125"

	token, err := client.GetToken(ctx, contractAddress, tokenID)

	// Should return an error due to context cancellation
	assert.Error(t, err)
	assert.Nil(t, token)
	t.Logf("Context cancellation error: %v", err)
}

// TestClient_GetToken_Integration_EdgeCases tests various edge cases
func TestClient_GetToken_Integration_EdgeCases(t *testing.T) {
	t.Skip("Temporarily skipping Objkt integration test (CI blocked by upstream)")

	httpClient := adapter.NewHTTPClient(30 * time.Second)
	jsonAdapter := adapter.NewJSON()
	client := objkt.NewClient(httpClient, nil, OBJKT_API_URL, "", jsonAdapter)

	ctx := context.Background()

	t.Run("EmptyTokenID", func(t *testing.T) {
		contractAddress := "KT19oAHnjgpQ6PauwgC8RxAb5pVj6svg9Myn"
		tokenID := ""

		token, err := client.GetToken(ctx, contractAddress, tokenID)

		// This should return an error (token not found or API error)
		assert.Error(t, err)
		assert.Nil(t, token)
		t.Logf("Empty token ID error: %v", err)
	})

	t.Run("EmptyContractAddress", func(t *testing.T) {
		contractAddress := ""
		tokenID := "125"

		token, err := client.GetToken(ctx, contractAddress, tokenID)

		// This should return an error (token not found or API error)
		assert.Error(t, err)
		assert.Nil(t, token)
		t.Logf("Empty contract address error: %v", err)
	})

	t.Run("VeryLargeTokenID", func(t *testing.T) {
		contractAddress := "KT19oAHnjgpQ6PauwgC8RxAb5pVj6svg9Myn"
		tokenID := "99999999999999999999999999999999999999999999999"

		token, err := client.GetToken(ctx, contractAddress, tokenID)

		// This should return token not found error
		assert.Error(t, err)
		assert.Nil(t, token)
		assert.Contains(t, err.Error(), "token not found")
		t.Logf("Very large token ID error: %v", err)
	})
}

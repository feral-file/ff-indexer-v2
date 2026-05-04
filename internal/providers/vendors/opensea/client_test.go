package opensea_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/opensea"
)

func TestOpenSeaClient_GetNFT(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockJSON := mocks.NewMockJSON(ctrl)
	mockLimiter := mocks.NewMockLimiter(ctrl)

	client := opensea.NewClient(mockHTTPClient, mockLimiter, "https://api.opensea.io/api/v2", "test-api-key", mockJSON)

	ctx := context.Background()
	contractAddress := "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D"
	tokenID := "1"

	responseJSON := []byte(`{
		"nft": {
			"identifier": "1",
			"collection": "bored-ape-yacht-club",
			"contract": "0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d",
			"name": "Bored Ape #1",
			"description": "A Bored Ape from the Yacht Club",
			"image_url": "https://i.seadn.io/gae/1.png",
			"display_animation_url": "https://i.seadn.io/gae/1.mp4",
			"metadata_url": "ipfs://QmTest1",
			"traits": [
				{
					"trait_type": "Background",
					"value": "Blue"
				},
				{
					"trait_type": "Artist",
					"value": "Yuga Labs"
				}
			]
		}
	}`)

	expectedURL := "https://api.opensea.io/api/v2/chain/ethereum/contract/0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d/nfts/1"
	expectedHeaders := map[string]string{
		"X-API-KEY": "test-api-key",
	}

	mockLimiter.EXPECT().
		Do(gomock.Any(), opensea.PROVIDER_NAME, gomock.Any()).
		DoAndReturn(func(ctx context.Context, providerName string, fn func(context.Context) (interface{}, error)) (interface{}, error) {
			return fn(ctx)
		})

	mockHTTPClient.EXPECT().
		GetBytes(ctx, expectedURL, expectedHeaders).
		Return(responseJSON, nil)

	identifier := "1"
	collection := "bored-ape-yacht-club"
	contract := "0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d"
	name := "Bored Ape #1"
	description := "A Bored Ape from the Yacht Club"
	imageURL := "https://i.seadn.io/gae/1.png"
	displayAnimationURL := "https://i.seadn.io/gae/1.mp4"
	metadataURL := "ipfs://QmTest1"

	expectedNFT := &opensea.NFTMetadata{
		Identifier:          identifier,
		Collection:          collection,
		Contract:            contract,
		Name:                &name,
		Description:         &description,
		ImageURL:            &imageURL,
		DisplayAnimationURL: &displayAnimationURL,
		MetadataURL:         &metadataURL,
		Traits: []opensea.Trait{
			{
				TraitType: "Background",
				Value:     "Blue",
			},
			{
				TraitType: "Artist",
				Value:     "Yuga Labs",
			},
		},
	}

	mockJSON.EXPECT().
		Unmarshal(responseJSON, gomock.Any()).
		DoAndReturn(func(data []byte, v interface{}) error {
			resp := v.(*opensea.NFTResponse)
			resp.NFT = *expectedNFT
			return nil
		})

	result, err := client.GetNFT(ctx, contractAddress, tokenID)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, identifier, result.Identifier)
	assert.Equal(t, collection, result.Collection)
	assert.Equal(t, contract, result.Contract)
	assert.Equal(t, name, *result.Name)
	assert.Equal(t, description, *result.Description)
	assert.Equal(t, imageURL, *result.ImageURL)
	assert.Equal(t, displayAnimationURL, *result.DisplayAnimationURL)
	assert.Equal(t, metadataURL, *result.MetadataURL)
	assert.Len(t, result.Traits, 2)
}

func TestOpenSeaClient_GetNFT_NoAPIKey(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockJSON := mocks.NewMockJSON(ctrl)
	mockLimiter := mocks.NewMockLimiter(ctrl)

	client := opensea.NewClient(mockHTTPClient, mockLimiter, "https://api.opensea.io/api/v2", "", mockJSON)

	ctx := context.Background()
	contractAddress := "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D"
	tokenID := "1"

	result, err := client.GetNFT(ctx, contractAddress, tokenID)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, opensea.ErrNoAPIKey)
}

func TestOpenSeaClient_GetNFT_APIError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockJSON := mocks.NewMockJSON(ctrl)
	mockLimiter := mocks.NewMockLimiter(ctrl)

	client := opensea.NewClient(mockHTTPClient, mockLimiter, "https://api.opensea.io/api/v2", "test-api-key", mockJSON)

	ctx := context.Background()
	contractAddress := "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D"
	tokenID := "1"

	mockLimiter.EXPECT().
		Do(gomock.Any(), opensea.PROVIDER_NAME, gomock.Any()).
		DoAndReturn(func(ctx context.Context, providerName string, fn func(context.Context) (interface{}, error)) (interface{}, error) {
			return fn(ctx)
		})

	mockHTTPClient.EXPECT().
		GetBytes(ctx, gomock.Any(), gomock.Any()).
		Return(nil, assert.AnError)

	result, err := client.GetNFT(ctx, contractAddress, tokenID)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to call OpenSea API")
}

func TestOpenSeaClient_GetNFT_UnmarshalError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockJSON := mocks.NewMockJSON(ctrl)
	mockLimiter := mocks.NewMockLimiter(ctrl)

	client := opensea.NewClient(mockHTTPClient, mockLimiter, "https://api.opensea.io/api/v2", "test-api-key", mockJSON)

	ctx := context.Background()
	contractAddress := "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D"
	tokenID := "1"

	responseJSON := []byte(`invalid json`)

	mockLimiter.EXPECT().
		Do(gomock.Any(), opensea.PROVIDER_NAME, gomock.Any()).
		DoAndReturn(func(ctx context.Context, providerName string, fn func(context.Context) (interface{}, error)) (interface{}, error) {
			return fn(ctx)
		})

	mockHTTPClient.EXPECT().
		GetBytes(ctx, gomock.Any(), gomock.Any()).
		Return(responseJSON, nil)

	mockJSON.EXPECT().
		Unmarshal(responseJSON, gomock.Any()).
		Return(assert.AnError)

	result, err := client.GetNFT(ctx, contractAddress, tokenID)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to unmarshal OpenSea response")
}

func TestOpenSeaClient_GetNFT_ResponseErrors(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockJSON := mocks.NewMockJSON(ctrl)
	mockLimiter := mocks.NewMockLimiter(ctrl)

	client := opensea.NewClient(mockHTTPClient, mockLimiter, "https://api.opensea.io/api/v2", "test-api-key", mockJSON)

	ctx := context.Background()
	contractAddress := "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D"
	tokenID := "1"

	responseJSON := []byte(`{"errors": ["NFT not found"]}`)

	mockLimiter.EXPECT().
		Do(gomock.Any(), opensea.PROVIDER_NAME, gomock.Any()).
		DoAndReturn(func(ctx context.Context, providerName string, fn func(context.Context) (interface{}, error)) (interface{}, error) {
			return fn(ctx)
		})

	mockHTTPClient.EXPECT().
		GetBytes(ctx, gomock.Any(), gomock.Any()).
		Return(responseJSON, nil)

	mockJSON.EXPECT().
		Unmarshal(responseJSON, gomock.Any()).
		DoAndReturn(func(data []byte, v interface{}) error {
			resp := v.(*opensea.NFTResponse)
			resp.Errors = []string{"NFT not found"}
			return nil
		})

	result, err := client.GetNFT(ctx, contractAddress, tokenID)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "OpenSea API errors")
}

func TestExtractArtistFromTraits(t *testing.T) {
	tests := []struct {
		name     string
		traits   []opensea.Trait
		expected string
	}{
		{
			name: "single artist trait",
			traits: []opensea.Trait{
				{TraitType: "Artist", Value: "Alice"},
			},
			expected: "Alice",
		},
		{
			name: "multiple artist trait aliases",
			traits: []opensea.Trait{
				{TraitType: "Artist", Value: "Alice"},
				{TraitType: "creator", Value: "Bob"},
			},
			expected: "Alice, Bob",
		},
		{
			name: "deduplicates artists",
			traits: []opensea.Trait{
				{TraitType: "artist", Value: "Alice"},
				{TraitType: "Artist", Value: "Alice"},
			},
			expected: "Alice",
		},
		{
			name: "ignores non-string values",
			traits: []opensea.Trait{
				{TraitType: "Artist", Value: 123},
			},
			expected: "",
		},
		{
			name: "no matching traits",
			traits: []opensea.Trait{
				{TraitType: "Background", Value: "Blue"},
			},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, opensea.ExtractArtistFromTraits(tt.traits))
		})
	}
}

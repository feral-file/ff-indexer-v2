package opensea_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/opensea"
)

func TestOpenSeaClient_GetNFT(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockJSON := mocks.NewMockJSON(ctrl)

	client := opensea.NewClient(mockHTTPClient, "https://api.opensea.io/api/v2", "test-api-key", mockJSON)

	ctx := context.Background()
	contractAddress := "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D"
	tokenID := "1"

	// Mock response
	responseJSON := []byte(`{
		"nft": {
			"identifier": "1",
			"collection": "bored-ape-yacht-club",
			"contract": "0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d",
			"name": "Bored Ape #1",
			"description": "A Bored Ape from the Yacht Club",
			"image_url": "https://i.seadn.io/gae/1.png",
			"animation_url": "https://i.seadn.io/gae/1.mp4",
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

	mockHTTPClient.EXPECT().
		GetBytes(ctx, expectedURL, expectedHeaders).
		Return(responseJSON, nil)

	identifier := "1"
	collection := "bored-ape-yacht-club"
	contract := "0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d"
	name := "Bored Ape #1"
	description := "A Bored Ape from the Yacht Club"
	imageURL := "https://i.seadn.io/gae/1.png"
	animationURL := "https://i.seadn.io/gae/1.mp4"
	metadataURL := "ipfs://QmTest1"

	expectedNFT := &opensea.NFTMetadata{
		Identifier:   identifier,
		Collection:   collection,
		Contract:     contract,
		Name:         &name,
		Description:  &description,
		ImageURL:     &imageURL,
		AnimationURL: &animationURL,
		MetadataURL:  &metadataURL,
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
	assert.Equal(t, animationURL, *result.AnimationURL)
	assert.Equal(t, metadataURL, *result.MetadataURL)
	assert.Len(t, result.Traits, 2)
}

func TestOpenSeaClient_GetNFT_NoAPIKey(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockJSON := mocks.NewMockJSON(ctrl)

	// Create client without API key (empty string)
	client := opensea.NewClient(mockHTTPClient, "https://api.opensea.io/api/v2", "", mockJSON)

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

	client := opensea.NewClient(mockHTTPClient, "https://api.opensea.io/api/v2", "test-api-key", mockJSON)

	ctx := context.Background()
	contractAddress := "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D"
	tokenID := "1"

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

	client := opensea.NewClient(mockHTTPClient, "https://api.opensea.io/api/v2", "test-api-key", mockJSON)

	ctx := context.Background()
	contractAddress := "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D"
	tokenID := "1"

	responseJSON := []byte(`invalid json`)

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

	client := opensea.NewClient(mockHTTPClient, "https://api.opensea.io/api/v2", "test-api-key", mockJSON)

	ctx := context.Background()
	contractAddress := "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D"
	tokenID := "1"

	responseJSON := []byte(`{"errors": ["NFT not found"]}`)

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
			name: "Artist trait",
			traits: []opensea.Trait{
				{TraitType: "Background", Value: "Blue"},
				{TraitType: "Artist", Value: "Tyler Hobbs"},
			},
			expected: "Tyler Hobbs",
		},
		{
			name: "Artists trait",
			traits: []opensea.Trait{
				{TraitType: "Background", Value: "Blue"},
				{TraitType: "Artists", Value: "Tyler Hobbs, John Doe"},
			},
			expected: "Tyler Hobbs, John Doe",
		},
		{
			name: "Creator trait",
			traits: []opensea.Trait{
				{TraitType: "Background", Value: "Blue"},
				{TraitType: "Creator", Value: "John Doe"},
			},
			expected: "John Doe",
		},
		{
			name: "Artist Name trait",
			traits: []opensea.Trait{
				{TraitType: "Artist Name", Value: "Jane Smith"},
			},
			expected: "Jane Smith",
		},
		{
			name: "No artist trait",
			traits: []opensea.Trait{
				{TraitType: "Background", Value: "Blue"},
				{TraitType: "Eyes", Value: "Laser"},
			},
			expected: "",
		},
		{
			name:     "Empty traits",
			traits:   []opensea.Trait{},
			expected: "",
		},
		{
			name: "Case insensitive matching",
			traits: []opensea.Trait{
				{TraitType: "ARTIST", Value: "Test Artist"},
			},
			expected: "Test Artist",
		},
		{
			name: "Non-string value",
			traits: []opensea.Trait{
				{TraitType: "Artist", Value: 123},
			},
			expected: "",
		},
		{
			name: "Multiple artist traits",
			traits: []opensea.Trait{
				{TraitType: "Background", Value: "Blue"},
				{TraitType: "Artist", Value: "Tyler Hobbs"},
				{TraitType: "Artist", Value: "John Doe"},
			},
			expected: "Tyler Hobbs, John Doe",
		},
		{
			name: "Multiple different artist traits",
			traits: []opensea.Trait{
				{TraitType: "Background", Value: "Blue"},
				{TraitType: "Artist", Value: "Tyler Hobbs"},
				{TraitType: "Artist Name", Value: "John Doe"},
				{TraitType: "Creator", Value: "Jane Smith"},
				{TraitType: "Creator Name", Value: "Jane Smith"},
				{TraitType: "Made By", Value: "Jane Smith"},
				{TraitType: "Created By", Value: "Jane Smith"},
			},
			expected: "Tyler Hobbs, John Doe, Jane Smith",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := opensea.ExtractArtistFromTraits(tt.traits)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestOpenSeaClient_Integration is an integration test that calls the real OpenSea API
// It will only run if OPENSEA_API_KEY environment variable is set
func TestOpenSeaClient_Integration(t *testing.T) {
	apiKey := os.Getenv("OPENSEA_API_KEY")
	if apiKey == "" {
		t.Skip("Skipping integration test: OPENSEA_API_KEY not set")
	}

	// Use real HTTP client and JSON adapter
	httpClient := adapter.NewHTTPClient(30 * time.Second)
	jsonAdapter := adapter.NewJSON()

	client := opensea.NewClient(httpClient, "https://api.opensea.io/api/v2", apiKey, jsonAdapter)

	ctx := context.Background()
	contractAddress := "0x513AC47320798fB6D74543242a9c0F686682998D" // BAYC
	tokenID := "60555235769946072280257368134421579233129900805931125862653074656717210657350"

	result, err := client.GetNFT(ctx, contractAddress, tokenID)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "60555235769946072280257368134421579233129900805931125862653074656717210657350", result.Identifier)
	assert.NotEmpty(t, result.Name)
	assert.NotEmpty(t, result.Description)
	assert.NotEmpty(t, result.ImageURL)
	assert.NotEmpty(t, result.AnimationURL)
	assert.NotEmpty(t, result.MetadataURL)
}

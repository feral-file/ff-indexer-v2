package opensea_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/opensea"
)

const testAPIURL = "https://api.opensea.io/api/v2"

func TestOpenSeaClient_GetNFT(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockJSON := mocks.NewMockJSON(ctrl)
	mockLimiter := mocks.NewMockLimiter(ctrl)

	client := opensea.NewClient(mockHTTPClient, mockLimiter, testAPIURL, "test-api-key", mockJSON)

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

	expectedURL := testAPIURL + "/chain/ethereum/contract/0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d/nfts/1"
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

	client := opensea.NewClient(mockHTTPClient, mockLimiter, testAPIURL, "", mockJSON)

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

	client := opensea.NewClient(mockHTTPClient, mockLimiter, testAPIURL, "test-api-key", mockJSON)

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

	client := opensea.NewClient(mockHTTPClient, mockLimiter, testAPIURL, "test-api-key", mockJSON)

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

// TestOpenSeaClient_GetNFT_ResponseErrors_NotFound verifies that a "not found" error
// in the JSON errors array is mapped to ErrNFTNotFound (not a generic API error).
func TestOpenSeaClient_GetNFT_ResponseErrors_NotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockJSON := mocks.NewMockJSON(ctrl)
	mockLimiter := mocks.NewMockLimiter(ctrl)

	client := opensea.NewClient(mockHTTPClient, mockLimiter, testAPIURL, "test-api-key", mockJSON)

	ctx := context.Background()
	contractAddress := "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D"
	tokenID := "0"

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

	require.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, opensea.ErrNFTNotFound)
}

// TestOpenSeaClient_GetNFT_ResponseErrors_Other verifies that non-"not found" error
// strings in the JSON errors array are returned as generic API errors.
func TestOpenSeaClient_GetNFT_ResponseErrors_Other(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockJSON := mocks.NewMockJSON(ctrl)
	mockLimiter := mocks.NewMockLimiter(ctrl)

	client := opensea.NewClient(mockHTTPClient, mockLimiter, testAPIURL, "test-api-key", mockJSON)

	ctx := context.Background()
	contractAddress := "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D"
	tokenID := "1"

	responseJSON := []byte(`{"errors": ["Internal server error"]}`)

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
			resp.Errors = []string{"Internal server error"}
			return nil
		})

	result, err := client.GetNFT(ctx, contractAddress, tokenID)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "OpenSea API errors")
}

// TestOpenSeaClient_GetNFT_HTTP404_ReturnsErrNFTNotFound verifies that an HTTP 404
// response from the NFT endpoint is mapped to ErrNFTNotFound.
func TestOpenSeaClient_GetNFT_HTTP404_ReturnsErrNFTNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockJSON := mocks.NewMockJSON(ctrl)
	mockLimiter := mocks.NewMockLimiter(ctrl)

	client := opensea.NewClient(mockHTTPClient, mockLimiter, testAPIURL, "test-api-key", mockJSON)

	ctx := context.Background()
	contractAddress := "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D"
	tokenID := "0"

	mockLimiter.EXPECT().
		Do(gomock.Any(), opensea.PROVIDER_NAME, gomock.Any()).
		DoAndReturn(func(ctx context.Context, providerName string, fn func(context.Context) (interface{}, error)) (interface{}, error) {
			return fn(ctx)
		})

	// Simulate the HTTP client returning "unexpected status code 404" (how GetBytes surfaces 404s).
	mockHTTPClient.EXPECT().
		GetBytes(ctx, gomock.Any(), gomock.Any()).
		Return(nil, fmt.Errorf("unexpected status code 404"))

	result, err := client.GetNFT(ctx, contractAddress, tokenID)

	require.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, opensea.ErrNFTNotFound)
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

func TestOpenSeaClient_GetCollection(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockJSON := mocks.NewMockJSON(ctrl)
	mockLimiter := mocks.NewMockLimiter(ctrl)

	client := opensea.NewClient(mockHTTPClient, mockLimiter, testAPIURL, "test-api-key", mockJSON)

	ctx := context.Background()
	slug := "boredapeyachtclub"

	responseJSON := []byte(`{"collection":"boredapeyachtclub","name":"Bored Ape Yacht Club","total_supply":10000,"contracts":[{"address":"0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d","chain":"ethereum"}]}`)

	expectedURL := testAPIURL + "/collections/" + slug
	expectedHeaders := map[string]string{"X-API-KEY": "test-api-key"}

	mockLimiter.EXPECT().
		Do(gomock.Any(), opensea.PROVIDER_NAME, gomock.Any()).
		DoAndReturn(func(ctx context.Context, providerName string, fn func(context.Context) (interface{}, error)) (interface{}, error) {
			return fn(ctx)
		})

	mockHTTPClient.EXPECT().
		GetBytes(ctx, expectedURL, expectedHeaders).
		Return(responseJSON, nil)

	mockJSON.EXPECT().
		Unmarshal(responseJSON, gomock.Any()).
		DoAndReturn(func(data []byte, v interface{}) error {
			resp := v.(*opensea.CollectionResponse)
			resp.Collection = "boredapeyachtclub"
			resp.Name = "Bored Ape Yacht Club"
			resp.TotalSupply = 10000
			resp.Contracts = []opensea.CollectionContract{
				{Address: "0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d", Chain: "ethereum"},
			}
			return nil
		})

	result, err := client.GetCollection(ctx, slug)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "boredapeyachtclub", result.Collection)
	assert.Equal(t, "Bored Ape Yacht Club", result.Name)
	assert.Equal(t, int64(10000), result.TotalSupply)
	assert.Len(t, result.Contracts, 1)
	assert.Equal(t, "0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d", result.Contracts[0].Address)
	assert.Equal(t, "ethereum", result.Contracts[0].Chain)
}

func TestOpenSeaClient_GetCollection_NoAPIKey(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	client := opensea.NewClient(mocks.NewMockHTTPClient(ctrl), mocks.NewMockLimiter(ctrl), testAPIURL, "", mocks.NewMockJSON(ctrl))

	result, err := client.GetCollection(context.Background(), "boredapeyachtclub")

	assert.ErrorIs(t, err, opensea.ErrNoAPIKey)
	assert.Nil(t, result)
}

func TestOpenSeaClient_GetCollection_APIError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockJSON := mocks.NewMockJSON(ctrl)
	mockLimiter := mocks.NewMockLimiter(ctrl)

	client := opensea.NewClient(mockHTTPClient, mockLimiter, testAPIURL, "test-api-key", mockJSON)

	mockLimiter.EXPECT().
		Do(gomock.Any(), opensea.PROVIDER_NAME, gomock.Any()).
		DoAndReturn(func(ctx context.Context, providerName string, fn func(context.Context) (interface{}, error)) (interface{}, error) {
			return fn(ctx)
		})

	mockHTTPClient.EXPECT().
		GetBytes(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, assert.AnError)

	result, err := client.GetCollection(context.Background(), "testslug")

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to call OpenSea collections API")
}

func TestOpenSeaClient_GetCollection_NotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockJSON := mocks.NewMockJSON(ctrl)
	mockLimiter := mocks.NewMockLimiter(ctrl)

	client := opensea.NewClient(mockHTTPClient, mockLimiter, testAPIURL, "test-api-key", mockJSON)

	responseJSON := []byte(`{"errors": ["Resource not found"]}`)

	mockLimiter.EXPECT().
		Do(gomock.Any(), opensea.PROVIDER_NAME, gomock.Any()).
		DoAndReturn(func(ctx context.Context, providerName string, fn func(context.Context) (interface{}, error)) (interface{}, error) {
			return fn(ctx)
		})

	mockHTTPClient.EXPECT().
		GetBytes(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(responseJSON, nil)

	mockJSON.EXPECT().
		Unmarshal(responseJSON, gomock.Any()).
		DoAndReturn(func(data []byte, v interface{}) error {
			resp := v.(*opensea.CollectionResponse)
			resp.Errors = []string{"Resource not found"}
			return nil
		})

	result, err := client.GetCollection(context.Background(), "unknown-slug")

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, opensea.ErrCollectionNotFound)
}

func TestOpenSeaClient_ResolveSlug(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockJSON := mocks.NewMockJSON(ctrl)
	mockLimiter := mocks.NewMockLimiter(ctrl)

	client := opensea.NewClient(mockHTTPClient, mockLimiter, testAPIURL, "test-api-key", mockJSON)

	responseJSON := []byte(`{"collection":"boredapeyachtclub","name":"Bored Ape Yacht Club","contracts":[{"address":"0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d","chain":"ethereum"}]}`)

	mockLimiter.EXPECT().
		Do(gomock.Any(), opensea.PROVIDER_NAME, gomock.Any()).
		DoAndReturn(func(ctx context.Context, providerName string, fn func(context.Context) (interface{}, error)) (interface{}, error) {
			return fn(ctx)
		})

	mockHTTPClient.EXPECT().
		GetBytes(gomock.Any(), testAPIURL+"/collections/boredapeyachtclub", gomock.Any()).
		Return(responseJSON, nil)

	mockJSON.EXPECT().
		Unmarshal(responseJSON, gomock.Any()).
		DoAndReturn(func(data []byte, v interface{}) error {
			resp := v.(*opensea.CollectionResponse)
			resp.Collection = "boredapeyachtclub"
			resp.Name = "Bored Ape Yacht Club"
			resp.Contracts = []opensea.CollectionContract{
				{Address: "0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d", Chain: "ethereum"},
			}
			return nil
		})

	slug, err := client.ResolveSlug(context.Background(), "boredapeyachtclub")

	assert.NoError(t, err)
	// ResolveSlug returns the slug unchanged — the slug IS the vendor_release_id.
	assert.Equal(t, "boredapeyachtclub", slug)
}

func TestOpenSeaClient_ResolveSlug_NotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockJSON := mocks.NewMockJSON(ctrl)
	mockLimiter := mocks.NewMockLimiter(ctrl)

	client := opensea.NewClient(mockHTTPClient, mockLimiter, testAPIURL, "test-api-key", mockJSON)

	responseJSON := []byte(`{"errors":["Resource not found"]}`)

	mockLimiter.EXPECT().
		Do(gomock.Any(), opensea.PROVIDER_NAME, gomock.Any()).
		DoAndReturn(func(ctx context.Context, providerName string, fn func(context.Context) (interface{}, error)) (interface{}, error) {
			return fn(ctx)
		})

	mockHTTPClient.EXPECT().
		GetBytes(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(responseJSON, nil)

	mockJSON.EXPECT().
		Unmarshal(responseJSON, gomock.Any()).
		DoAndReturn(func(data []byte, v interface{}) error {
			resp := v.(*opensea.CollectionResponse)
			resp.Errors = []string{"Resource not found"}
			return nil
		})

	slug, err := client.ResolveSlug(context.Background(), "nonexistent-slug")

	assert.Error(t, err)
	assert.Empty(t, slug)
	assert.ErrorIs(t, err, opensea.ErrCollectionNotFound)
}

func TestOpenSeaClient_ResolveSlug_NoContracts(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := mocks.NewMockHTTPClient(ctrl)
	mockJSON := mocks.NewMockJSON(ctrl)
	mockLimiter := mocks.NewMockLimiter(ctrl)

	client := opensea.NewClient(mockHTTPClient, mockLimiter, testAPIURL, "test-api-key", mockJSON)

	responseJSON := []byte(`{"collection":"empty-collection","name":"Empty"}`)

	mockLimiter.EXPECT().
		Do(gomock.Any(), opensea.PROVIDER_NAME, gomock.Any()).
		DoAndReturn(func(ctx context.Context, providerName string, fn func(context.Context) (interface{}, error)) (interface{}, error) {
			return fn(ctx)
		})

	mockHTTPClient.EXPECT().
		GetBytes(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(responseJSON, nil)

	mockJSON.EXPECT().
		Unmarshal(responseJSON, gomock.Any()).
		DoAndReturn(func(data []byte, v interface{}) error {
			resp := v.(*opensea.CollectionResponse)
			resp.Collection = "empty-collection"
			resp.Name = "Empty"
			// No contracts
			return nil
		})

	slug, err := client.ResolveSlug(context.Background(), "empty-collection")

	assert.Error(t, err)
	assert.Empty(t, slug)
	assert.Contains(t, err.Error(), "no associated contracts")
}

func TestReleaseMetadataFromCollection(t *testing.T) {
	t.Parallel()

	name, totalMints := opensea.ReleaseMetadataFromCollection(&opensea.CollectionMetadata{
		Name:        "  Bored Ape Yacht Club  ",
		TotalSupply: 10000,
	})
	assert.NotNil(t, name)
	assert.Equal(t, "Bored Ape Yacht Club", *name)
	assert.NotNil(t, totalMints)
	assert.Equal(t, int64(10000), *totalMints)

	name, totalMints = opensea.ReleaseMetadataFromCollection(&opensea.CollectionMetadata{
		Name:        "   ",
		TotalSupply: 0,
	})
	assert.Nil(t, name)
	assert.Nil(t, totalMints)

	name, totalMints = opensea.ReleaseMetadataFromCollection(nil)
	assert.Nil(t, name)
	assert.Nil(t, totalMints)
}

func TestExtractMintNumber(t *testing.T) {
	tests := []struct {
		name           string
		nftName        string
		identifier     string
		expectedNumber int64
		expectedOK     bool
	}{
		{
			name:           "hash pattern in name",
			nftName:        "Fidenza #78",
			identifier:     "100",
			expectedNumber: 78,
			expectedOK:     true,
		},
		{
			name:           "hash pattern with large number",
			nftName:        "Chromie Squiggle #9999",
			identifier:     "0",
			expectedNumber: 9999,
			expectedOK:     true,
		},
		{
			name:           "no hash pattern, falls back to identifier",
			nftName:        "Bored Ape Yacht Club",
			identifier:     "42",
			expectedNumber: 42,
			expectedOK:     true,
		},
		{
			name:           "empty name falls back to identifier",
			nftName:        "",
			identifier:     "7",
			expectedNumber: 7,
			expectedOK:     true,
		},
		{
			name:           "both empty returns false",
			nftName:        "",
			identifier:     "",
			expectedNumber: 0,
			expectedOK:     false,
		},
		{
			name:           "non-numeric identifier and no hash pattern returns false",
			nftName:        "My NFT",
			identifier:     "abc",
			expectedNumber: 0,
			expectedOK:     false,
		},
		{
			name:           "hash pattern takes priority over identifier",
			nftName:        "Artwork #5",
			identifier:     "100",
			expectedNumber: 5,
			expectedOK:     true,
		},
		{
			name:           "hash pattern with zero",
			nftName:        "Token #0",
			identifier:     "99",
			expectedNumber: 0,
			expectedOK:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := opensea.ExtractMintNumber(tt.nftName, tt.identifier)
			assert.Equal(t, tt.expectedOK, ok)
			assert.Equal(t, tt.expectedNumber, got)
		})
	}
}

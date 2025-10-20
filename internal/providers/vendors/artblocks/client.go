package artblocks

import "github.com/feral-file/ff-indexer-v2/internal/adapter"

// Client defines the interface for ArtBlocks client operations to enable mocking
//
//go:generate mockgen -source=client.go -destination=../../mocks/artblocks_client.go -package=mocks -mock_names=Client=MockArtBlocksClient
type Client interface{}

// ArtBlocksClient implements ArtBlocks client
type ArtBlocksClient struct {
	httpClient adapter.HTTPClient
}

// NewClient creates a new ArtBlocks client
func NewClient(httpClient adapter.HTTPClient) Client {
	return &ArtBlocksClient{httpClient: httpClient}
}

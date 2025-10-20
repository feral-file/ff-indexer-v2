package fxhash

import "github.com/feral-file/ff-indexer-v2/internal/adapter"

// Client defines the interface for Objkt client operations to enable mocking
//
//go:generate mockgen -source=client.go -destination=../../mocks/fxhash_client.go -package=mocks -mock_names=Client=MockFxhashClient
type Client interface{}

// FxhashClient implements Fxhash client
type FxhashClient struct {
	httpClient adapter.HTTPClient
}

// NewClient creates a new Fxhash client
func NewClient(httpClient adapter.HTTPClient) Client {
	return &FxhashClient{httpClient: httpClient}
}

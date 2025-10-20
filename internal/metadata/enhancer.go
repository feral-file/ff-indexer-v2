package metadata

import (
	"context"

	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/artblocks"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/fxhash"
)

// Enhancer defines the interface for enhancing metadata from vendors
//
//go:generate mockgen -source=enhancer.go -destination=../mocks/metadata_enhancer.go -package=mocks -mock_names=MetadataEnhancer=MockMetadataEnhancer
type Enhancer interface {
	Enhance(ctx context.Context, meta *NormalizedMetadata) (*NormalizedMetadata, error)
}

type enhancer struct {
	artblocksClient artblocks.Client
	fxhashClient    fxhash.Client
}

func NewEnhancer(artblocksClient artblocks.Client, fxhashClient fxhash.Client) Enhancer {
	return &enhancer{artblocksClient: artblocksClient, fxhashClient: fxhashClient}
}

func (e *enhancer) Enhance(ctx context.Context, meta *NormalizedMetadata) (*NormalizedMetadata, error) {
	return meta, nil
}

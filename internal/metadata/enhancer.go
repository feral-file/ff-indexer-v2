package metadata

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"

	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/artblocks"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/fxhash"
	"github.com/feral-file/ff-indexer-v2/internal/registry"
	"github.com/feral-file/ff-indexer-v2/internal/uri"
)

// EnhancedMetadata represents metadata enhanced from vendor APIs
type EnhancedMetadata struct {
	Vendor       registry.PublisherName
	VendorJSON   []byte
	Name         *string
	Description  *string
	ImageURL     *string
	AnimationURL *string
	Artists      []Artist
	MimeType     *string
}

// Enhancer defines the interface for enhancing metadata from vendors
//
//go:generate mockgen -source=enhancer.go -destination=../mocks/metadata_enhancer.go -package=mocks -mock_names=Enhancer=MockMetadataEnhancer
type Enhancer interface {
	// Enhance enhances metadata from vendor APIs based on the token CID and returns enriched data
	Enhance(ctx context.Context, tokenCID domain.TokenCID, meta *NormalizedMetadata) (*EnhancedMetadata, error)

	// VendorJsonHash returns the hash of the canonicalized vendor JSON and the vendor JSON itself
	VendorJsonHash(metadata *EnhancedMetadata) ([]byte, error)
}

type enhancer struct {
	httpClient      adapter.HTTPClient
	uriResolver     uri.Resolver
	artblocksClient artblocks.Client
	fxhashClient    fxhash.Client
	json            adapter.JSON
	jcs             adapter.JCS
}

func NewEnhancer(httpClient adapter.HTTPClient, uriResolver uri.Resolver, artblocksClient artblocks.Client, fxhashClient fxhash.Client, json adapter.JSON, jcs adapter.JCS) Enhancer {
	return &enhancer{httpClient: httpClient, uriResolver: uriResolver, artblocksClient: artblocksClient, fxhashClient: fxhashClient, json: json, jcs: jcs}
}

// VendorJsonHash returns the hash of the canonicalized vendor JSON and the vendor JSON itself
func (e *enhancer) VendorJsonHash(metadata *EnhancedMetadata) ([]byte, error) {
	canonicalizedVendorJSON, err := e.jcs.Transform(metadata.VendorJSON)
	if err != nil {
		return nil, fmt.Errorf("failed to canonicalize vendor JSON: %w", err)
	}
	hash := sha256.Sum256(canonicalizedVendorJSON)
	return hash[:], nil
}

// Enhance enhances metadata from vendor APIs based on the token CID
func (e *enhancer) Enhance(ctx context.Context, tokenCID domain.TokenCID, meta *NormalizedMetadata) (*EnhancedMetadata, error) {
	// Skip if no publisher information is available
	if meta.Publisher == nil || meta.Publisher.Name == nil {
		return nil, nil
	}

	chain, _, contractAddress, tokenNumber := tokenCID.Parse()

	// Check publisher name and route to appropriate enhancer
	publisherName := registry.PublisherName(*meta.Publisher.Name)

	var enhancedMetadata *EnhancedMetadata
	var err error
	switch publisherName {
	case registry.PublisherNameArtBlocks:
		// Only enhance Ethereum mainnet tokens
		if chain == domain.ChainEthereumMainnet {
			enhancedMetadata, err = e.enhanceArtBlocks(ctx, tokenCID, contractAddress, tokenNumber, meta.Raw)
			if err != nil {
				return nil, fmt.Errorf("failed to enhance ArtBlocks metadata: %w", err)
			}
		}

	// TODO: Add support for other vendors
	// case "fxhash":
	//     return e.enhanceFxhash(ctx, tokenCID, contractAddress, tokenNumber, meta)
	// case "Feral File":
	//     return e.enhanceFeralFile(ctx, tokenCID, contractAddress, tokenNumber, meta)

	default:
		// No enhancement available for this publisher
		return nil, nil
	}

	if enhancedMetadata != nil {
		enhancedMetadata.MimeType = detectMimeType(ctx, e.httpClient, e.uriResolver, enhancedMetadata.AnimationURL, enhancedMetadata.ImageURL)
	}

	return enhancedMetadata, nil
}

// enhanceArtBlocks enhances metadata from ArtBlocks API
func (e *enhancer) enhanceArtBlocks(ctx context.Context, tokenCID domain.TokenCID, contractAddress, tokenNumber string, rawMetadata map[string]interface{}) (*EnhancedMetadata, error) {
	logger.InfoCtx(ctx, "Enhancing ArtBlocks metadata", zap.String("tokenCID", tokenCID.String()))

	// Parse the token ID to get project ID and mint number
	projectID, mintNumber, err := artblocks.ParseArtBlocksTokenID(tokenNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to parse ArtBlocks token ID: %w", err)
	}

	// Build the project ID string in the format: contractAddress-projectID
	projectIDStr := fmt.Sprintf("%s-%d", strings.ToLower(contractAddress), projectID)

	// Fetch project metadata from ArtBlocks API
	project, err := e.artblocksClient.GetProjectMetadata(ctx, projectIDStr)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch ArtBlocks project metadata: %w", err)
	}

	logger.InfoCtx(ctx, "Fetched ArtBlocks project metadata",
		zap.String("tokenCID", tokenCID.String()),
		zap.String("projectID", projectIDStr),
		zap.String("projectName", project.Name),
		zap.Int64("mintNumber", mintNumber))

	vendorJSON, err := e.json.Marshal(project)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal ArtBlocks project metadata: %w", err)
	}

	// Build enhanced metadata
	enhanced := &EnhancedMetadata{
		Vendor:     registry.PublisherNameArtBlocks,
		VendorJSON: vendorJSON,
	}

	// Format the name as "{project.name} #{mintNumber}"
	name := fmt.Sprintf("%s #%d", project.Name, mintNumber)
	enhanced.Name = &name

	// Use project description if available
	if project.Description != nil && *project.Description != "" {
		enhanced.Description = project.Description
	}

	// Build artist information using DID
	if project.ArtistAddress != "" {
		artistDID := domain.NewDID(project.ArtistAddress, domain.ChainEthereumMainnet)
		enhanced.Artists = []Artist{
			{
				DID:  artistDID,
				Name: project.ArtistName,
			},
		}
	}

	// Animation URL is the generator URL
	if g, ok := rawMetadata["generator_url"].(string); ok {
		enhanced.AnimationURL = &g
	}

	// Image URL is the raw metadata image URL
	if i, ok := rawMetadata["image"].(string); ok {
		enhanced.ImageURL = &i
	}

	return enhanced, nil
}

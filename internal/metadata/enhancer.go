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
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/feralfile"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/objkt"
	"github.com/feral-file/ff-indexer-v2/internal/registry"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
	"github.com/feral-file/ff-indexer-v2/internal/types"
	"github.com/feral-file/ff-indexer-v2/internal/uri"
)

// EnhancedMetadata represents metadata enhanced from vendor APIs
type EnhancedMetadata struct {
	Vendor       schema.Vendor
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
	feralfileClient feralfile.Client
	objktClient     objkt.Client
	json            adapter.JSON
	jcs             adapter.JCS
}

func NewEnhancer(httpClient adapter.HTTPClient, uriResolver uri.Resolver, artblocksClient artblocks.Client, feralfileClient feralfile.Client, objktClient objkt.Client, json adapter.JSON, jcs adapter.JCS) Enhancer {
	return &enhancer{httpClient: httpClient, uriResolver: uriResolver, artblocksClient: artblocksClient, feralfileClient: feralfileClient, objktClient: objktClient, json: json, jcs: jcs}
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
	chain, _, contractAddress, tokenNumber := tokenCID.Parse()

	// Check publisher name and route to appropriate enhancer
	var publisherName registry.PublisherName
	if meta.Publisher != nil && meta.Publisher.Name != nil {
		publisherName = registry.PublisherName(*meta.Publisher.Name)
	}

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

	case registry.PublisherNameFeralFile:
		// Enhance Feral File tokens for both Ethereum and Tezos
		if chain == domain.ChainEthereumMainnet || chain == domain.ChainTezosMainnet {
			enhancedMetadata, err = e.enhanceFeralFile(ctx, tokenCID, chain, tokenNumber, meta.Raw)
			if err != nil {
				return nil, fmt.Errorf("failed to enhance Feral File metadata: %w", err)
			}
		}

	default:
		// For Tezos tokens that are not Feral File, use objkt
		if chain == domain.ChainTezosMainnet {
			enhancedMetadata, err = e.enhanceObjkt(ctx, tokenCID, contractAddress, tokenNumber)
			if err != nil {
				return nil, fmt.Errorf("failed to enhance objkt metadata: %w", err)
			}
		} else {
			// No enhancement available
			return nil, nil
		}
	}

	if enhancedMetadata != nil && types.StringNilOrEmpty(enhancedMetadata.MimeType) {
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
		Vendor:     schema.VendorArtBlocks,
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

// enhanceFeralFile enhances metadata from Feral File API
func (e *enhancer) enhanceFeralFile(ctx context.Context, tokenCID domain.TokenCID, chain domain.Chain, tokenNumber string, rawMetadata map[string]interface{}) (*EnhancedMetadata, error) {
	logger.InfoCtx(ctx, "Enhancing Feral File metadata", zap.String("tokenCID", tokenCID.String()))

	// Fetch artwork data from Feral File API
	artwork, err := e.feralfileClient.GetArtwork(ctx, tokenNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch Feral File artwork: %w", err)
	}

	vendorJSON, err := e.json.Marshal(artwork)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal Feral File artwork: %w", err)
	}

	// Build enhanced metadata
	enhanced := &EnhancedMetadata{
		Vendor:     schema.VendorFeralFile,
		VendorJSON: vendorJSON,
	}

	// Set the artwork name
	artworkName := artwork.CanonicalName()
	enhanced.Name = &artworkName

	// Set the series description
	if artwork.Series.Description != "" {
		enhanced.Description = &artwork.Series.Description
	}

	// Determine which URI to use for image and animation based on the medium
	medium := strings.ToLower(artwork.Series.Medium)

	if medium == "image" {
		// For image medium, use previewURI as image
		if artwork.PreviewURI != "" {
			imageURL := feralfile.URL(artwork.PreviewURI)
			enhanced.ImageURL = &imageURL
		}
	} else {
		// For non-image mediums, use thumbnailURI as image and previewURI as animation
		if artwork.ThumbnailURI != "" {
			imageURL := feralfile.URL(artwork.ThumbnailURI)
			enhanced.ImageURL = &imageURL
		}
		if artwork.PreviewURI != "" {
			animationURL := feralfile.URL(artwork.PreviewURI)
			enhanced.AnimationURL = &animationURL
		}
	}

	// Build artist information from alumni account
	// Try to get the address for the specific blockchain first
	var artistAddress string
	chainKey := ""
	switch chain {
	case domain.ChainEthereumMainnet:
		chainKey = "ethereum"
	case domain.ChainTezosMainnet:
		chainKey = "tezos"
	}

	if chainKey != "" {
		if addr, ok := artwork.Series.Artist.AlumniAccount.Addresses[chainKey]; ok {
			artistAddress = addr
		}
	}

	// If we found an artist address, create the DID and add artist info
	if artistAddress != "" {
		artistDID := domain.NewDID(artistAddress, chain)
		enhanced.Artists = []Artist{
			{
				DID:  artistDID,
				Name: artwork.Series.Artist.AlumniAccount.Alias,
			},
		}
	}

	return enhanced, nil
}

// enhanceObjkt enhances metadata from objkt v3 API for Tezos tokens
func (e *enhancer) enhanceObjkt(ctx context.Context, tokenCID domain.TokenCID, contractAddress, tokenNumber string) (*EnhancedMetadata, error) {
	logger.InfoCtx(ctx, "Enhancing objkt metadata", zap.String("tokenCID", tokenCID.String()))

	// Fetch token data from objkt v3 API
	token, err := e.objktClient.GetToken(ctx, contractAddress, tokenNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch objkt token: %w", err)
	}

	vendorJSON, err := e.json.Marshal(token)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal objkt token: %w", err)
	}

	// Build enhanced metadata
	enhanced := &EnhancedMetadata{
		Vendor:     schema.VendorObjkt,
		VendorJSON: vendorJSON,
	}

	// Set the token name
	if token.Name != nil {
		enhanced.Name = token.Name
	}

	// Set the token description
	if token.Description != nil {
		enhanced.Description = token.Description
	}

	// Set display_uri as image (this is the main display image)
	if !types.StringNilOrEmpty(token.DisplayURI) {
		url := domain.UriToGateway(*token.DisplayURI)
		enhanced.ImageURL = &url
	}

	// Set artifact_uri as animation_url (this is the actual artwork/animation)
	if !types.StringNilOrEmpty(token.ArtifactURI) {
		url := domain.UriToGateway(*token.ArtifactURI)
		enhanced.AnimationURL = &url
	}

	// Set mime type (objkt provides this, so no need to detect)
	if !types.StringNilOrEmpty(token.Mime) {
		enhanced.MimeType = token.Mime
	}

	// Build artist information from creators
	if len(token.Creators) > 0 {
		artists := make([]Artist, 0, len(token.Creators))
		for _, creator := range token.Creators {
			if types.IsTezosAddress(creator.Holder.Address) {
				artistDID := domain.NewDID(creator.Holder.Address, domain.ChainTezosMainnet)
				artistName := ""
				if creator.Holder.Alias != nil {
					artistName = *creator.Holder.Alias
				}
				artists = append(artists, Artist{
					DID:  artistDID,
					Name: artistName,
				})
			}
		}
		if len(artists) > 0 {
			enhanced.Artists = artists
		}
	}

	return enhanced, nil
}

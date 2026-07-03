package metadata

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/artblocks"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/feralfile"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/fxhash"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/objkt"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/opensea"
	"github.com/feral-file/ff-indexer-v2/internal/registry"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
	"github.com/feral-file/ff-indexer-v2/internal/types"
	"github.com/feral-file/ff-indexer-v2/internal/uri"
)

// ReleaseInfo carries release membership data extracted during vendor enrichment.
type ReleaseInfo struct {
	VendorReleaseID string
	// Slug is the URL slug for this release on the vendor's website.
	// For objkt, this equals VendorReleaseID (KT1 address). Nil when not available.
	Slug       *string
	MintNumber int64
	Name       *string
	TotalMints *int64
}

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
	Release      *ReleaseInfo
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
	fxhashClient    fxhash.Client
	objktClient     objkt.Client
	openseaClient   opensea.Client
	json            adapter.JSON
	jcs             adapter.JCS
	// openSeaCollectionCache avoids repeated GetCollection calls when indexing many
	// tokens from the same OpenSea release in one worker process.
	openSeaCollectionCache sync.Map
}

// NewEnhancer creates a new metadata enhancer that routes vendor-specific enrichment
// to ArtBlocks, Feral File, fxhash, objkt, or OpenSea depending on the publisher registry
// entry for the token's contract.
func NewEnhancer(httpClient adapter.HTTPClient, uriResolver uri.Resolver, artblocksClient artblocks.Client, feralfileClient feralfile.Client, fxhashClient fxhash.Client, objktClient objkt.Client, openseaClient opensea.Client, json adapter.JSON, jcs adapter.JCS) Enhancer {
	return &enhancer{
		httpClient:      httpClient,
		uriResolver:     uriResolver,
		artblocksClient: artblocksClient,
		feralfileClient: feralfileClient,
		fxhashClient:    fxhashClient,
		objktClient:     objktClient,
		openseaClient:   openseaClient,
		json:            json,
		jcs:             jcs,
	}
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
	if meta != nil && meta.Publisher != nil && meta.Publisher.Name != nil {
		publisherName = registry.PublisherName(*meta.Publisher.Name)
	}

	var enhancedMetadata *EnhancedMetadata
	var err error
	switch publisherName {
	case registry.PublisherNameArtBlocks:
		// Art Blocks Hasura resolves projects by (chain_id, id); only EVM chains are supported here.
		if chain.IsEVM() {
			enhancedMetadata, err = e.enhanceArtBlocks(ctx, chain, contractAddress, tokenNumber, meta.Raw)
			if err != nil {
				return nil, fmt.Errorf("failed to enhance ArtBlocks metadata: %w", err)
			}
		}

	case registry.PublisherNameFeralFile:
		// Enhance Feral File tokens for both Ethereum and Tezos
		if chain == domain.ChainEthereumMainnet || chain == domain.ChainTezosMainnet {
			enhancedMetadata, err = e.enhanceFeralFile(ctx, chain, contractAddress, tokenNumber)
			if err != nil {
				return nil, fmt.Errorf("failed to enhance Feral File metadata: %w", err)
			}
		}

	case registry.PublisherNameFXHash:
		// fxhash generative tokens live on Tezos mainnet only.
		// If the fxhash API does not recognize the gentk (returns nil), we fall through
		// to objkt so basic metadata is still fetched rather than leaving the record empty.
		if chain == domain.ChainTezosMainnet {
			enhancedMetadata, err = e.enhanceFxhash(ctx, contractAddress, tokenNumber)
			if err != nil {
				return nil, fmt.Errorf("failed to enhance fxhash metadata: %w", err)
			}
		}

	default:
		switch chain {
		case domain.ChainTezosMainnet:
			// For Tezos tokens that are not Feral File, use objkt
			enhancedMetadata, err = e.enhanceObjkt(ctx, contractAddress, tokenNumber)
			if err != nil {
				return nil, fmt.Errorf("failed to enhance objkt metadata: %w", err)
			}
		case domain.ChainEthereumMainnet:
			// For Ethereum tokens without a known publisher, try OpenSea
			enhancedMetadata, err = e.enhanceOpenSea(ctx, contractAddress, tokenNumber)
			if err != nil {
				return nil, fmt.Errorf("failed to enhance OpenSea metadata: %w", err)
			}
		default:
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
func (e *enhancer) enhanceArtBlocks(ctx context.Context, chain domain.Chain, contractAddress, tokenNumber string, rawMetadata map[string]interface{}) (*EnhancedMetadata, error) {
	logger.InfoCtx(ctx, "Enhancing ArtBlocks metadata", zap.String("contractAddress", contractAddress), zap.String("tokenNumber", tokenNumber))

	evmChainID, ok := chain.EIP155NumericID()
	if !ok {
		return nil, fmt.Errorf("art blocks enhancement requires an eip155 chain, got %q", chain)
	}

	// Parse the token ID to get project ID and mint number.
	// ParseArtBlocksTokenID returns a 0-based mint index (tokenID % 1_000_000).
	// rawMintIndex is used for the canonical Art Blocks display name (0-based: "Fidenza #0" is
	// the first minted piece). releaseMintNumber is the 1-based index stored in release_members
	// to match the FF convention and the schema's mint_number > 0 constraint.
	projectID, rawMintIndex, err := artblocks.ParseArtBlocksTokenID(tokenNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to parse ArtBlocks token ID: %w", err)
	}
	releaseMintNumber := rawMintIndex + 1

	// Build the vendor_release_id as "{chainID}-{contractAddress}-{projectID}" so that
	// the same contract/project on different EVM chains (e.g. mainnet vs L2) maps to
	// distinct release rows and does not collide on the UNIQUE (vendor, vendor_release_id)
	// constraint.
	projectIDStr := fmt.Sprintf("%d-%s-%d", evmChainID, strings.ToLower(contractAddress), projectID)

	// Fetch project metadata from ArtBlocks API (composite key: chain_id + id)
	project, err := e.artblocksClient.GetProjectMetadata(ctx, evmChainID, fmt.Sprintf("%s-%d", strings.ToLower(contractAddress), projectID))
	if err != nil {
		return nil, fmt.Errorf("failed to fetch ArtBlocks project metadata: %w", err)
	}

	logger.InfoCtx(ctx, "Fetched ArtBlocks project metadata",
		zap.String("contractAddress", contractAddress),
		zap.String("tokenNumber", tokenNumber),
		zap.Int("chainID", evmChainID),
		zap.String("projectID", projectIDStr),
		zap.String("projectName", project.Name),
		zap.Int64("rawMintIndex", rawMintIndex),
		zap.Int64("releaseMintNumber", releaseMintNumber))

	vendorJSON, err := e.json.Marshal(project)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal ArtBlocks project metadata: %w", err)
	}

	// Build enhanced metadata
	enhanced := &EnhancedMetadata{
		Vendor:     schema.VendorArtBlocks,
		VendorJSON: vendorJSON,
	}

	// Format the canonical Art Blocks display name as "{project.name} #{rawMintIndex}".
	// AB uses 0-based display numbering: the first minted token is #0.
	// releaseMintNumber (1-based) is only used for release_members ordering below.
	name := fmt.Sprintf("%s #%d", project.Name, rawMintIndex)
	enhanced.Name = &name

	// Use project description if available
	if project.Description != nil && *project.Description != "" {
		enhanced.Description = project.Description
	}

	// Build artist information using DID on the same chain as the token
	if project.ArtistAddress != "" {
		artistDID := domain.NewDID(project.ArtistAddress, chain)
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

	enhanced.Release = &ReleaseInfo{
		VendorReleaseID: projectIDStr,
		MintNumber:      releaseMintNumber,
	}
	if title := strings.TrimSpace(project.Name); title != "" {
		enhanced.Release.Name = &title
	}
	if project.MaxInvocations > 0 {
		total := int64(project.MaxInvocations)
		enhanced.Release.TotalMints = &total
	}
	if slug := strings.TrimSpace(project.Slug); slug != "" {
		enhanced.Release.Slug = &slug
	}

	return enhanced, nil
}

// enhanceFeralFile enhances metadata from Feral File API
func (e *enhancer) enhanceFeralFile(ctx context.Context, chain domain.Chain, contractAddress string, tokenNumber string) (*EnhancedMetadata, error) {
	logger.InfoCtx(ctx, "Enhancing Feral File metadata", zap.String("contractAddress", contractAddress), zap.String("tokenNumber", tokenNumber))

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

			// If the contract address is Maya Man StarQuest, add the mode=episode parameter to the animation URL
			if contractAddress == feralfile.MAYA_MAN_STARQUEST_CONTRACT {
				animationURL = fmt.Sprintf("%s&mode=episode", animationURL)
			}

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

	// Require both seriesID and Index to be present before recording release membership.
	// Index is a pointer; when the FF API omits "index", it is nil and mint_number cannot
	// be determined. Skipping here rather than writing mint_number=1 prevents a false
	// UNIQUE (release_id, mint_number) conflict for the legitimate first token of the release.
	if seriesID := artwork.SeriesIDOrFallback(); seriesID != "" && artwork.Index != nil {
		enhanced.Release = &ReleaseInfo{
			VendorReleaseID: seriesID,
			MintNumber:      *artwork.Index + 1,
		}
		if title := strings.TrimSpace(artwork.Series.Title); title != "" {
			enhanced.Release.Name = &title
		}
		if artwork.Series.Settings.MaxArtwork > 0 {
			enhanced.Release.TotalMints = &artwork.Series.Settings.MaxArtwork
		}
		if slug := strings.TrimSpace(artwork.Series.Slug); slug != "" {
			enhanced.Release.Slug = &slug
		}
	}

	return enhanced, nil
}

// enhanceFxhash enhances metadata for a fxhash generative token (gentk) using the
// fxhash v2 GraphQL API. It returns (nil, nil) when the fxhash API does not index
// the gentk, allowing the caller to fall through to enhanceObjkt for basic metadata.
//
// Release mapping:
//   - VendorReleaseID = generative_token.id (stable numeric project id, e.g. "9997")
//   - MintNumber      = gentk.Iteration (1-based within the project)
//   - TotalMints      = generative_token.original_supply if set, else supply
func (e *enhancer) enhanceFxhash(ctx context.Context, contractAddress, tokenNumber string) (*EnhancedMetadata, error) {
	logger.InfoCtx(ctx, "Enhancing fxhash metadata", zap.String("contractAddress", contractAddress), zap.String("tokenNumber", tokenNumber))

	gentk, err := e.fxhashClient.GetGentk(ctx, contractAddress, tokenNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch fxhash gentk: %w", err)
	}

	// fxhash does not index this token (e.g. legacy h=n contract, or not yet synced).
	// Fall through to objkt so at least basic metadata is available.
	if gentk == nil {
		logger.InfoCtx(ctx, "fxhash gentk not found, falling back to objkt",
			zap.String("contractAddress", contractAddress),
			zap.String("tokenNumber", tokenNumber))
		return e.enhanceObjkt(ctx, contractAddress, tokenNumber)
	}

	vendorJSON, err := e.json.Marshal(gentk)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal fxhash gentk: %w", err)
	}

	enhanced := &EnhancedMetadata{
		Vendor:     schema.VendorFXHash,
		VendorJSON: vendorJSON,
	}

	if gentk.Name != nil {
		enhanced.Name = gentk.Name
	}

	// fxhash generative tokens do not produce a static artifact URI; display_uri
	// is the preview image generated at mint time.
	if !types.StringNilOrEmpty(gentk.DisplayURI) {
		resolved, err := e.uriResolver.Resolve(ctx, *gentk.DisplayURI)
		if err != nil {
			logger.WarnCtx(ctx, "failed to resolve fxhash display URI, fallback to default gateway", zap.Error(err), zap.String("displayURI", *gentk.DisplayURI))
			url := domain.UriToGateway(*gentk.DisplayURI)
			enhanced.ImageURL = &url
		} else {
			enhanced.ImageURL = &resolved
		}
	}

	// Build artist DID from the generative token author's wallet address.
	if gentk.GenerativeToken != nil && gentk.GenerativeToken.Author != nil {
		gt := gentk.GenerativeToken
		author := gt.Author
		if author.WalletAccount != nil && author.WalletAccount.Address != "" {
			artistDID := domain.NewDID(author.WalletAccount.Address, domain.ChainTezosMainnet)
			enhanced.Artists = []Artist{
				{
					DID:  artistDID,
					Name: author.Name,
				},
			}
		}

		// Populate release membership.
		iteration, parseErr := strconv.ParseInt(gentk.Iteration, 10, 64)
		if parseErr == nil && iteration > 0 {
			var totalMints *int64
			supplyStr := gt.OriginalSupply
			if supplyStr == nil {
				supplyStr = &gt.Supply
			}
			if supplyStr != nil {
				// Only set TotalMints when the vendor-reported supply is positive.
				// A supply of "0" (or any non-positive value) would violate the
				// CHECK (total_mints IS NULL OR total_mints > 0) DB constraint and
				// abort enrichment. Treat non-positive as unknown (nil).
				if parsed, err := strconv.ParseInt(*supplyStr, 10, 64); err == nil && parsed > 0 {
					totalMints = &parsed
				}
			}

			releaseName := gt.Name
			release := &ReleaseInfo{
				VendorReleaseID: gt.ID,
				MintNumber:      iteration,
				Name:            &releaseName,
				TotalMints:      totalMints,
			}
			if slug := strings.TrimSpace(gt.Slug); slug != "" {
				release.Slug = &slug
			}
			enhanced.Release = release
		}
	}

	return enhanced, nil
}

// enhanceObjkt enhances metadata from objkt v3 API for Tezos tokens
func (e *enhancer) enhanceObjkt(ctx context.Context, contractAddress, tokenNumber string) (*EnhancedMetadata, error) {
	logger.InfoCtx(ctx, "Enhancing objkt metadata", zap.String("contractAddress", contractAddress), zap.String("tokenNumber", tokenNumber))

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
		resolved, err := e.uriResolver.Resolve(ctx, *token.DisplayURI)
		if nil != err {
			logger.WarnCtx(ctx, "failed to resolve display URI, fallback to default gateway", zap.Error(err), zap.String("displayURI", *token.DisplayURI))
			url := domain.UriToGateway(*token.DisplayURI)
			enhanced.ImageURL = &url
		} else {
			enhanced.ImageURL = &resolved
		}
	}

	// Set artifact_uri as animation_url (this is the actual artwork/animation)
	if !types.StringNilOrEmpty(token.ArtifactURI) {
		resolved, err := e.uriResolver.Resolve(ctx, *token.ArtifactURI)
		if nil != err {
			logger.WarnCtx(ctx, "failed to resolve artifact URI, fallback to default gateway", zap.Error(err), zap.String("artifactURI", *token.ArtifactURI))
			url := domain.UriToGateway(*token.ArtifactURI)
			enhanced.AnimationURL = &url
		} else {
			enhanced.AnimationURL = &resolved
		}
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

	// Populate release for objkt custom collections.
	// collection_type "custom" means a dedicated per-artist contract where the KT1
	// address is the stable release id and token_id is the 1-based mint number.
	// Open/curated collections are multi-artist and have no meaningful single-release
	// semantics, so we skip them here.
	if token.FA != nil && token.FA.CollectionType == "custom" {
		mintNum, parseErr := strconv.ParseInt(tokenNumber, 10, 64)
		if parseErr == nil && mintNum > 0 {
			faName := token.FA.Name
			// For objkt, the KT1 contract address is also the URL identifier
			// (objkt.com/collections/KT1...) — there is no separate human slug.
			// We set Slug = VendorReleaseID so clients can use vendor_release_slug
			// with the same value they see in the URL.
			contractAddressCopy := contractAddress
			release := &ReleaseInfo{
				VendorReleaseID: contractAddress, // KT1 address is chain-unique; no chain prefix needed
				Slug:            &contractAddressCopy,
				MintNumber:      mintNum,
				Name:            &faName,
			}
			// Only set TotalMints when the vendor-reported edition count is positive.
			// Editions == 0 violates CHECK (total_mints IS NULL OR total_mints > 0) and
			// would abort enrichment. Treat non-positive as unknown (nil).
			if token.FA.Editions > 0 {
				totalMints := token.FA.Editions
				release.TotalMints = &totalMints
			}
			enhanced.Release = release
		}
	}

	return enhanced, nil
}

// enhanceOpenSea enhances metadata from OpenSea API for Ethereum tokens.
//
// Returns (nil, nil) — skip, no error — for two expected non-failure cases:
//  1. No API key configured (ErrNoAPIKey): degraded mode, token created without enrichment.
//  2. Token not found on OpenSea (ErrNFTNotFound): happens when a derived token ID does not
//     correspond to a real on-chain token, e.g. tokenID=0 for a 1-indexed ERC-721 contract.
//     This is an expected outcome of the mintNum-1 token-ID mapping used by IndexRelease for
//     OpenSea; treating it as a skip prevents infinite retries for phantom token IDs.
func (e *enhancer) enhanceOpenSea(ctx context.Context, contractAddress, tokenNumber string) (*EnhancedMetadata, error) {
	logger.InfoCtx(ctx, "Enhancing OpenSea metadata", zap.String("contractAddress", contractAddress), zap.String("tokenNumber", tokenNumber))

	nft, err := e.openseaClient.GetNFT(ctx, contractAddress, tokenNumber)
	if err != nil {
		if errors.Is(err, opensea.ErrNoAPIKey) {
			logger.WarnCtx(ctx, "OpenSea enrichment skipped: no API key configured",
				zap.String("contractAddress", contractAddress),
				zap.String("tokenNumber", tokenNumber))
			return nil, nil
		}
		if errors.Is(err, opensea.ErrNFTNotFound) {
			// Token ID does not exist on OpenSea. This is an expected skip when the
			// collection uses 1-based token IDs and the derived CID landed on token 0
			// via the mintNum-1 mapping. Log at info (not warn) because it is not a
			// transient failure and retrying would not help.
			logger.InfoCtx(ctx, "OpenSea enrichment skipped: token not found (may be outside collection token ID range)",
				zap.String("contractAddress", contractAddress),
				zap.String("tokenNumber", tokenNumber))
			return nil, nil
		}
		return nil, fmt.Errorf("failed to fetch OpenSea NFT: %w", err)
	}

	vendorJSON, err := e.json.Marshal(nft)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal OpenSea NFT: %w", err)
	}

	// Build enhanced metadata
	enhanced := &EnhancedMetadata{
		Vendor:     schema.VendorOpenSea,
		VendorJSON: vendorJSON,
	}

	// Populate release info from the single-NFT response.
	// nft.Collection is the collection slug — for OpenSea this IS the vendor_release_id.
	//
	// Release membership is only recorded when a positive (1-based) mint number can be
	// derived. ExtractMintNumber returns ok=false for tokens with non-numeric identifiers
	// and no "#N" pattern in their name; mintNum==0 would violate the release_members
	// DB constraint (mint_number > 0). Skipping here avoids a repeated persistence
	// failure for CryptoPunks V1 / tokens without parseable edition numbers.
	if nft.Collection != "" {
		slug := nft.Collection
		var nftName string
		if nft.Name != nil {
			nftName = *nft.Name
		}
		mintNum, ok := opensea.ExtractMintNumber(nftName, nft.Identifier)
		if ok && mintNum > 0 {
			release := &ReleaseInfo{
				VendorReleaseID: slug,
				Slug:            &slug,
				MintNumber:      mintNum,
			}
			if name, totalMints := e.openSeaCollectionReleaseMetadata(ctx, slug); name != nil || totalMints != nil {
				release.Name = name
				release.TotalMints = totalMints
			}
			enhanced.Release = release
		}
	}

	// Set name
	if nft.Name != nil && *nft.Name != "" {
		enhanced.Name = nft.Name
	}

	// Set description
	if nft.Description != nil && *nft.Description != "" {
		enhanced.Description = nft.Description
	}

	// Set image URL
	if nft.ImageURL != nil && *nft.ImageURL != "" {
		// OpenSea CDN uses image resizing on the fly, we pick the 4k version as higher quality
		imageURL := *nft.ImageURL
		if strings.Contains(imageURL, "?") {
			imageURL = fmt.Sprintf("%s&w=1000", imageURL)
		} else {
			imageURL = fmt.Sprintf("%s?w=1000", imageURL)
		}
		enhanced.ImageURL = &imageURL
	}

	// Set animation URL
	if nft.DisplayAnimationURL != nil && *nft.DisplayAnimationURL != "" {
		enhanced.AnimationURL = nft.DisplayAnimationURL
	}

	// Try to extract artist from traits
	if len(nft.Traits) > 0 {
		artistName := opensea.ExtractArtistFromTraits(nft.Traits)
		if artistName != "" {
			// We don't have the artist's address from OpenSea, so we can't create a DID
			// We'll just store the name
			enhanced.Artists = []Artist{
				{
					Name: artistName,
				},
			}
		}
	}

	return enhanced, nil
}

// openSeaCollectionReleaseMetadata fetches collection-level name and total_mints for a slug,
// using an in-process cache to avoid N+1 GetCollection calls during release indexing.
func (e *enhancer) openSeaCollectionReleaseMetadata(ctx context.Context, slug string) (name *string, totalMints *int64) {
	if slug == "" {
		return nil, nil
	}
	if cached, ok := e.openSeaCollectionCache.Load(slug); ok {
		return opensea.ReleaseMetadataFromCollection(cached.(*opensea.CollectionMetadata))
	}

	collection, err := e.openseaClient.GetCollection(ctx, slug)
	if err != nil {
		if !errors.Is(err, opensea.ErrNoAPIKey) {
			logger.WarnCtx(ctx, "Failed to fetch OpenSea collection for release metadata",
				zap.String("slug", slug),
				zap.Error(err),
			)
		}
		return nil, nil
	}

	e.openSeaCollectionCache.Store(slug, collection)
	return opensea.ReleaseMetadataFromCollection(collection)
}

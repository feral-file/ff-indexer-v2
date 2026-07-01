package executor

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/api/shared/constants"
	"github.com/feral-file/ff-indexer-v2/internal/api/shared/dto"
	apierrors "github.com/feral-file/ff-indexer-v2/internal/api/shared/errors"
	"github.com/feral-file/ff-indexer-v2/internal/api/shared/types"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/providers/jobs"
	"github.com/feral-file/ff-indexer-v2/internal/registry"
	"github.com/feral-file/ff-indexer-v2/internal/store"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
	internalTypes "github.com/feral-file/ff-indexer-v2/internal/types"
)

// Executor is the interface for the API executor
//
//go:generate mockgen -source=executor.go -destination=../../../mocks/api_executor.go -package=mocks -mock_names=Executor=MockAPIExecutor
type Executor interface {
	// GetToken retrieves a single token by its CID with optional expansions
	GetToken(ctx context.Context, tokenCID string, expansions []types.Expansion, ownersLimit *uint8, ownersOffset *uint64, provenanceEventsLimit *uint8, provenanceEventsOffset *uint64, provenanceEventsOrder *types.Order) (*dto.TokenResponse, error)

	// GetTokens retrieves tokens with optional filters and expansions (bulk: fixed sub-page sizes for owners/provenance per token).
	GetTokens(ctx context.Context, owners []string, chains []domain.Chain, contractAddresses []string, tokenNumbers []string, tokenIDs []uint64, tokenCIDs []string, releaseID *uint64, limit *uint8, offset *uint64, includeUnviewable *bool, sortBy *types.TokenSortBy, sortOrder *types.Order, expansions []types.Expansion) (*dto.TokenListResponse, error)

	// GetRelease retrieves a release by internal id without member tokens.
	GetRelease(ctx context.Context, releaseID uint64) (*dto.ReleaseResponse, error)

	// ListReleases retrieves releases filtered by vendor and/or vendor_release_id without member tokens.
	ListReleases(ctx context.Context, vendor *schema.Vendor, vendorReleaseID *string, limit *uint8, offset *uint64) (*dto.ReleaseListResponse, error)

	// TriggerTokenIndexing triggers indexing for one or more tokens by their CIDs.
	// Returns a queue job id for tracking.
	TriggerTokenIndexing(ctx context.Context, tokenCIDs []domain.TokenCID) (*dto.TriggerIndexingResponse, error)

	// TriggerAddressIndexing triggers indexing for tokens by owner addresses.
	// Creates per-address job records; returns a job id per address for tracking.
	TriggerAddressIndexing(ctx context.Context, addresses []string) (*dto.TriggerAddressIndexingResponse, error)

	// TriggerMetadataIndexing triggers metadata refresh for one or more tokens by IDs or CIDs
	TriggerMetadataIndexing(ctx context.Context, tokenIDs []uint64, tokenCIDs []domain.TokenCID) (*dto.TriggerIndexingResponse, error)

	// GetJobStatus returns status for a postgres job row
	GetJobStatus(ctx context.Context, jobID int64) (*dto.JobStatusResponse, error)

	// CreateWebhookClient creates a new webhook client
	CreateWebhookClient(ctx context.Context, webhookURL string, eventFilters []string, retryMaxAttempts int) (*dto.CreateWebhookClientResponse, error)

	// GetAddressIndexingJob retrieves an address indexing job by postgres jobs.id (queue job id)
	GetAddressIndexingJob(ctx context.Context, jobID int64, opts GetAddressIndexingJobOptions) (*dto.AddressIndexingJobResponse, error)

	// GetAddressIndexingJobByLegacyWorkflowID loads an address indexing job when the client passes deprecated
	// workflow_id: either the decimal string of jobs.id or the opaque value stored in address_indexing_jobs.workflow_id.
	GetAddressIndexingJobByLegacyWorkflowID(ctx context.Context, workflowID string, opts GetAddressIndexingJobOptions) (*dto.AddressIndexingJobResponse, error)

	// SyncCollection retrieves token events for an address since a checkpoint
	// checkpoint: optional, if nil returns events from beginning (initial sync)
	// Uses cursor-based pagination with (timestamp, event_id) for reliable pagination
	SyncCollection(ctx context.Context, address string, checkpoint *dto.SyncCheckpoint, limit uint8) (*dto.SyncCollectionResponse, error)
}

// GetAddressIndexingJobOptions configures optional data to include in indexing job response
type GetAddressIndexingJobOptions struct {
	IncludeTotalIndexed  bool // Include total tokens owned by address
	IncludeTotalViewable bool // Include total viewable tokens (with metadata or enrichment)
}

type executor struct {
	store           store.Store
	jobQueue        jobs.JobQueue
	tokenQueue      string
	blacklist       registry.BlacklistRegistry
	json            adapter.JSON
	clock           adapter.Clock
	tezosChainID    domain.Chain
	ethereumChainID domain.Chain
}

// NewExecutor builds the API executor. tokenQueue is jobs.token_queue (e.g. token_index).
func NewExecutor(store store.Store, jobQueue jobs.JobQueue, tokenQueue string, blacklist registry.BlacklistRegistry, json adapter.JSON, clock adapter.Clock, tezosChainID domain.Chain, ethereumChainID domain.Chain) Executor {
	return &executor{
		store:           store,
		jobQueue:        jobQueue,
		tokenQueue:      tokenQueue,
		blacklist:       blacklist,
		json:            json,
		clock:           clock,
		tezosChainID:    tezosChainID,
		ethereumChainID: ethereumChainID,
	}
}

func (e *executor) GetToken(ctx context.Context, tokenCID string, expansions []types.Expansion, ownersLimit *uint8, ownersOffset *uint64, provenanceEventsLimit *uint8, provenanceEventsOffset *uint64, provenanceEventsOrder *types.Order) (*dto.TokenResponse, error) {
	// Normalize token CID
	normalizedTokenCID := domain.TokenCID(tokenCID).Normalized().String()

	// Get token
	token, err := e.store.GetTokenByTokenCID(ctx, normalizedTokenCID)
	if err != nil {
		return nil, apierrors.NewDatabaseError(fmt.Sprintf("Failed to get token: %v", err))
	}

	if token == nil {
		return nil, nil
	}

	// Map to track which expansions were requested
	expansionMap := make(map[types.Expansion]bool)

	// Map to DTO
	tokenDTO := dto.MapTokenToDTO(token)
	if err := e.applyReleaseMembership(ctx, []*dto.TokenResponse{tokenDTO}); err != nil {
		return nil, err
	}

	// Handle expansions
	for _, exp := range expansions {
		switch exp {
		case types.ExpansionMetadata:
			if err := e.expandMetadata(ctx, tokenDTO, token.ID); err != nil {
				return nil, err
			}
		case types.ExpansionOwners:
			if err := e.expandOwners(ctx, tokenDTO, token.ID, ownersLimit, ownersOffset); err != nil {
				return nil, err
			}
		case types.ExpansionProvenanceEvents:
			if err := e.expandProvenanceEvents(ctx, tokenDTO, token.ID, provenanceEventsLimit, provenanceEventsOffset, provenanceEventsOrder); err != nil {
				return nil, err
			}
		case types.ExpansionEnrichmentSource:
			if err := e.expandEnrichmentSource(ctx, tokenDTO, token.ID); err != nil {
				return nil, err
			}
		case types.ExpansionDisplay:
			// Display expansion requires metadata and enrichment_source internally
			// Fetch them if not already expanded, but don't populate the response fields
			if tokenDTO.Metadata == nil {
				if err := e.expandMetadata(ctx, tokenDTO, token.ID); err != nil {
					return nil, err
				}
			}
			if tokenDTO.EnrichmentSource == nil {
				if err := e.expandEnrichmentSource(ctx, tokenDTO, token.ID); err != nil {
					return nil, err
				}
			}
		case types.ExpansionMediaAsset:
			// When display is also requested, pre-populate the health-filtered display before
			// expandMediaAssets runs. expandMediaAssets is called inside this loop while
			// tokenDTO.Display is only set in the post-loop block; without pre-population,
			// display-derived media assets would be silently omitted.
			// Ensure metadata and enrichment are available first (display needs both).
			if containsExpansion(expansions, types.ExpansionDisplay) {
				if tokenDTO.Metadata == nil {
					if err := e.expandMetadata(ctx, tokenDTO, token.ID); err != nil {
						return nil, err
					}
				}
				if tokenDTO.EnrichmentSource == nil {
					if err := e.expandEnrichmentSource(ctx, tokenDTO, token.ID); err != nil {
						return nil, err
					}
				}
				if err := e.populateHealthFilteredDisplay(ctx, tokenDTO, token.ID); err != nil {
					return nil, err
				}
			}
			if err := e.expandMediaAssets(ctx, tokenDTO, expansions); err != nil {
				return nil, err
			}
		}

		expansionMap[exp] = true
	}

	// Populate display field if requested and clean up internal data if not explicitly requested.
	// populateHealthFilteredDisplay is idempotent: if media_asset expansion already ran first and
	// pre-populated the field, this is a no-op — no extra DB round trip.
	if expansionMap[types.ExpansionDisplay] {
		if err := e.populateHealthFilteredDisplay(ctx, tokenDTO, token.ID); err != nil {
			return nil, err
		}

		// Replace any data URI image/animation URLs with the public variant from media_assets
		if err := e.resolveDisplayDataURIs(ctx, []dto.TokenResponse{*tokenDTO}); err != nil {
			return nil, err
		}

		// Clean up metadata and enrichment_source if they weren't explicitly requested
		if !expansionMap[types.ExpansionMetadata] {
			tokenDTO.Metadata = nil
		}
		if !expansionMap[types.ExpansionEnrichmentSource] {
			tokenDTO.EnrichmentSource = nil
		}
	}

	return tokenDTO, nil
}

func (e *executor) GetTokens(ctx context.Context, owners []string, chains []domain.Chain, contractAddresses []string, tokenNumbers []string, tokenIDs []uint64, tokenCIDs []string, releaseID *uint64, limit *uint8, offset *uint64, includeUnviewable *bool, sortBy *types.TokenSortBy, sortOrder *types.Order, expansions []types.Expansion) (*dto.TokenListResponse, error) {
	// Use defaults if not provided
	if limit == nil {
		defaultLimit := constants.DEFAULT_TOKENS_LIMIT
		limit = &defaultLimit
	}
	if offset == nil {
		defaultOffset := constants.DEFAULT_OFFSET
		offset = &defaultOffset
	}
	if includeUnviewable == nil {
		defaultIncludeUnviewable := false
		includeUnviewable = &defaultIncludeUnviewable
	}
	if sortBy == nil {
		defaultSortBy := types.TokenLatestProvenance
		sortBy = &defaultSortBy
	}
	if sortOrder == nil {
		defaultSortOrder := types.OrderDesc
		sortOrder = &defaultSortOrder
	}

	// Normalize token CIDs
	if len(tokenCIDs) > 0 {
		normalizedTokenCIDs := make([]string, len(tokenCIDs))
		for i, tokenCID := range tokenCIDs {
			normalizedTokenCIDs[i] = domain.TokenCID(tokenCID).Normalized().String()
		}
		tokenCIDs = normalizedTokenCIDs
	}

	// Normalize contract addresses
	if len(contractAddresses) > 0 {
		normalizedContractAddresses := domain.NormalizeAddresses(contractAddresses)
		contractAddresses = normalizedContractAddresses
	}

	// Normalize owners
	if len(owners) > 0 {
		normalizedOwners := domain.NormalizeAddresses(owners)
		owners = normalizedOwners
	}

	// Convert API types to store types
	storeSortBy := types.ToStoreTokenSortBy(*sortBy)
	storeSortOrder := types.ToStoreSortOrder(*sortOrder)

	// Build filter
	filter := store.TokenQueryFilter{
		Owners:            owners,
		ContractAddresses: contractAddresses,
		TokenNumbers:      tokenNumbers,
		TokenIDs:          tokenIDs,
		Chains:            chains,
		TokenCIDs:         tokenCIDs,
		ReleaseID:         releaseID,
		IncludeUnviewable: *includeUnviewable,
		SortBy:            storeSortBy,
		SortOrder:         storeSortOrder,
		Limit:             int(*limit) + 1, // limit+1 to detect whether there are more results
		Offset:            *offset,
	}

	// Get tokens
	tokens, err := e.store.GetTokensByFilter(ctx, filter)
	if err != nil {
		return nil, apierrors.NewDatabaseError(fmt.Sprintf("Failed to get tokens: %v", err))
	}

	limitInt := int(*limit)
	hasMore := len(tokens) > limitInt
	if hasMore {
		tokens = tokens[:limitInt]
	}

	// Collect token IDs for bulk queries
	tokenIDsForBulk := make([]uint64, len(tokens))
	for i, token := range tokens {
		tokenIDsForBulk[i] = token.ID
	}

	// Fetch bulk data for expansions
	var bulkOwners map[uint64][]schema.Balance
	var bulkOwnersTotals map[uint64]uint64
	var bulkProvenanceEvents map[uint64][]schema.ProvenanceEvent
	var bulkProvenanceTotals map[uint64]uint64
	var bulkOwnerProvenances map[uint64][]schema.TokenOwnershipProvenance
	var bulkOwnerProvenancesTotals map[uint64]uint64
	var bulkEnrichmentSources map[uint64]*schema.EnrichmentSource
	var bulkMetadata map[uint64]*schema.TokenMetadata
	var bulkMediaHealth map[uint64][]schema.TokenMediaHealth
	var allMediaSourceURLs []string

	// Map to track which expansions were requested
	expansionMap := make(map[types.Expansion]bool)

	for _, exp := range expansions {
		switch exp {
		case types.ExpansionOwners:
			// Default limit for bulk owner queries
			// TODO: If owner filter is set, only fetch for those specific owners
			// Otherwise cap at DEFAULT_OWNERS_LIMIT per token to prevent unbounded results
			bulkOwners, bulkOwnersTotals, err = e.store.GetTokenOwnersBulk(ctx, tokenIDsForBulk, int(constants.DEFAULT_OWNERS_LIMIT))
			if err != nil {
				return nil, apierrors.NewDatabaseError(fmt.Sprintf("Failed to get owners: %v", err))
			}
		case types.ExpansionProvenanceEvents:
			// Default limit for bulk provenance queries
			// TODO: If owner filter is set, only fetch for those specific owners
			// Otherwise cap at DEFAULT_PROVENANCE_EVENTS_LIMIT per token to prevent unbounded results
			bulkProvenanceEvents, bulkProvenanceTotals, err = e.store.GetTokenProvenanceEventsBulk(ctx, tokenIDsForBulk, int(constants.DEFAULT_PROVENANCE_EVENTS_LIMIT))
			if err != nil {
				return nil, apierrors.NewDatabaseError(fmt.Sprintf("Failed to get provenance events: %v", err))
			}
		case types.ExpansionOwnerProvenances:
			// Fetch latest provenance per owner for all requested tokens
			// If owners filter is set, only fetch for those specific owners
			// Otherwise cap at DEFAULT_PROVENANCE_EVENTS_LIMIT per token to prevent unbounded results
			bulkOwnerProvenances, bulkOwnerProvenancesTotals, err = e.store.GetTokenOwnerProvenancesBulk(ctx, tokenIDsForBulk, owners, int(constants.DEFAULT_PROVENANCE_EVENTS_LIMIT))
			if err != nil {
				return nil, apierrors.NewDatabaseError(fmt.Sprintf("Failed to get owner provenances: %v", err))
			}
		case types.ExpansionMetadata:
			if bulkMetadata == nil {
				bulkMetadata, err = e.store.GetTokenMetadataByTokenIDs(ctx, tokenIDsForBulk)
				if err != nil {
					return nil, apierrors.NewDatabaseError(fmt.Sprintf("Failed to get token metadata: %v", err))
				}
			}
		case types.ExpansionEnrichmentSource:
			if bulkEnrichmentSources == nil {
				bulkEnrichmentSources, err = e.store.GetEnrichmentSourcesByTokenIDs(ctx, tokenIDsForBulk)
				if err != nil {
					return nil, apierrors.NewDatabaseError(fmt.Sprintf("Failed to get enrichment sources: %v", err))
				}
			}
		case types.ExpansionDisplay:
			// Display expansion requires metadata, enrichment_source, and media health internally.
			// Fetch them all if not already fetched.
			if bulkMetadata == nil {
				bulkMetadata, err = e.store.GetTokenMetadataByTokenIDs(ctx, tokenIDsForBulk)
				if err != nil {
					return nil, apierrors.NewDatabaseError(fmt.Sprintf("Failed to get token metadata: %v", err))
				}
			}
			if bulkEnrichmentSources == nil {
				bulkEnrichmentSources, err = e.store.GetEnrichmentSourcesByTokenIDs(ctx, tokenIDsForBulk)
				if err != nil {
					return nil, apierrors.NewDatabaseError(fmt.Sprintf("Failed to get enrichment sources: %v", err))
				}
			}
			if bulkMediaHealth == nil {
				// Bulk-fetch health rows in one query to avoid N+1 when iterating tokens.
				bulkMediaHealth, err = e.store.GetTokenMediaHealthByTokenIDs(ctx, tokenIDsForBulk)
				if err != nil {
					return nil, apierrors.NewDatabaseError(fmt.Sprintf("Failed to get token media health: %v", err))
				}
			}
		}

		expansionMap[exp] = true
	}

	// Collect metadata media source URLs when media_asset is requested with metadata or display
	if expansionMap[types.ExpansionMediaAsset] && (expansionMap[types.ExpansionMetadata] || expansionMap[types.ExpansionDisplay]) {
		for _, token := range tokens {
			metadata, ok := bulkMetadata[token.ID]
			if !ok || metadata == nil {
				continue
			}
			allMediaSourceURLs = append(allMediaSourceURLs, metadata.MediaURLs()...)
		}
	}

	// Collect enrichment source media URLs when media_asset is requested with enrichment_source or display
	if expansionMap[types.ExpansionMediaAsset] && (expansionMap[types.ExpansionEnrichmentSource] || expansionMap[types.ExpansionDisplay]) {
		for _, token := range tokens {
			enrichment, ok := bulkEnrichmentSources[token.ID]
			if !ok || enrichment == nil {
				continue
			}
			allMediaSourceURLs = append(allMediaSourceURLs, enrichment.MediaURLs()...)
		}
	}

	// Pre-compute health-filtered display URLs and add them to the bulk source URL set.
	// ApplyHealthyMediaURLs reads the URL directly from token_media_health.media_url, which can
	// diverge from raw metadata/enrichment URLs during the narrow window between a propagation
	// transaction commit and the metadata bulk read (two separate queries, no shared snapshot).
	// Adding display URLs here ensures GetMediaAssetsBySourceURLs includes them so that
	// collectMediaAssetDTOsFromMap can satisfy the display+media_asset contract.
	// The per-token loop re-runs MergeTokenDisplay+ApplyHealthyMediaURLs cheaply (no extra DB calls).
	if expansionMap[types.ExpansionDisplay] && expansionMap[types.ExpansionMediaAsset] {
		for _, token := range tokens {
			metaDTO := dto.MapTokenMetadataToDTO(bulkMetadata[token.ID])
			enrichDTO := dto.MapEnrichmentSourceToDTO(bulkEnrichmentSources[token.ID])
			preDisplay := dto.MergeTokenDisplay(metaDTO, enrichDTO)
			preDisplay = dto.ApplyHealthyMediaURLs(preDisplay, bulkMediaHealth[token.ID])
			if preDisplay != nil {
				allMediaSourceURLs = append(allMediaSourceURLs, preDisplay.MediaURLs()...)
			}
		}
	}

	// Fetch media assets in bulk if needed
	var mediaAssetsMap map[string]schema.MediaAsset
	if len(allMediaSourceURLs) > 0 {
		mediaAssetsList, err := e.store.GetMediaAssetsBySourceURLs(ctx, allMediaSourceURLs)
		if err != nil {
			return nil, apierrors.NewDatabaseError(fmt.Sprintf("Failed to get media assets: %v", err))
		}
		// Create map for fast lookup
		mediaAssetsMap = make(map[string]schema.MediaAsset)
		for _, asset := range mediaAssetsList {
			mediaAssetsMap[asset.SourceURLHash] = asset
		}
	}

	// Map to DTOs and apply bulk data
	tokenDTOs := make([]dto.TokenResponse, len(tokens))
	for i := range tokens {
		token := tokens[i]
		tokenDTO := dto.MapTokenToDTO(&token)
		metadata := bulkMetadata[token.ID]
		enrichment := bulkEnrichmentSources[token.ID]

		// Map to track media URLs for this specific token
		mediaURLsMap := make(map[string]bool)

		// Apply bulk expansions
		for _, exp := range expansions {
			switch exp {
			case types.ExpansionMetadata:
				tokenDTO.Metadata = dto.MapTokenMetadataToDTO(metadata)
			case types.ExpansionOwners:
				owners := bulkOwners[token.ID]
				ownerDTOs := make([]dto.OwnerResponse, len(owners))
				for j := range owners {
					ownerDTOs[j] = *dto.MapOwnerToDTO(&owners[j])
				}
				total := bulkOwnersTotals[token.ID] // Get actual total from DB
				tokenDTO.Owners = &dto.PaginatedOwners{
					Owners: ownerDTOs,
					Offset: nil, // No pagination in bulk queries
					Total:  total,
				}
			case types.ExpansionProvenanceEvents:
				events := bulkProvenanceEvents[token.ID]
				eventDTOs := make([]dto.ProvenanceEventResponse, len(events))
				for j := range events {
					eventDTOs[j] = *dto.MapProvenanceEventToDTO(&events[j])
				}
				total := bulkProvenanceTotals[token.ID] // Get actual total from DB
				tokenDTO.ProvenanceEvents = &dto.PaginatedProvenanceEvents{
					Events: eventDTOs,
					Offset: nil, // No pagination in bulk queries
					Total:  total,
				}
			case types.ExpansionOwnerProvenances:
				ownerProvenances := bulkOwnerProvenances[token.ID]
				ownerProvenanceDTOs := make([]dto.OwnerProvenanceResponse, len(ownerProvenances))
				for j := range ownerProvenances {
					ownerProvenanceDTOs[j] = *dto.MapOwnerProvenanceToDTO(&ownerProvenances[j])
				}
				total := bulkOwnerProvenancesTotals[token.ID] // Get actual total from DB
				tokenDTO.OwnerProvenances = &dto.PaginatedOwnerProvenances{
					OwnerProvenances: ownerProvenanceDTOs,
					Offset:           nil, // No pagination in bulk queries
					Total:            total,
				}
			case types.ExpansionEnrichmentSource:
				tokenDTO.EnrichmentSource = dto.MapEnrichmentSourceToDTO(enrichment)
			case types.ExpansionMediaAsset:
				// New unified expansion: include media from metadata and enrichment source (if expanded)

				// Collect metadata media URLs if metadata was explicitly requested
				if expansionMap[types.ExpansionMetadata] && metadata != nil {
					urls := metadata.MediaURLs()
					for _, url := range urls {
						mediaURLsMap[url] = true
					}
				}

				// Collect enrichment source media URLs if enrichment source was explicitly requested
				if expansionMap[types.ExpansionEnrichmentSource] && enrichment != nil {
					urls := enrichment.MediaURLs()
					for _, url := range urls {
						mediaURLsMap[url] = true
					}
				}

			case types.ExpansionDisplay:
				// Display will be populated after all expansions
			}
		}

		// Populate display field if requested
		if expansionMap[types.ExpansionDisplay] {
			// Get metadata and enrichment for merging
			metadataForDisplay := dto.MapTokenMetadataToDTO(metadata)
			enrichmentForDisplay := dto.MapEnrichmentSourceToDTO(enrichment)

			// Merge into display field, then apply health-aware URL filtering so broken
			// gateway URLs are never served to FF1 clients.
			tokenDTO.Display = dto.MergeTokenDisplay(metadataForDisplay, enrichmentForDisplay)
			tokenDTO.Display = dto.ApplyHealthyMediaURLs(tokenDTO.Display, bulkMediaHealth[token.ID])

			// Collect media URLs from display field if media_asset was requested
			if expansionMap[types.ExpansionMediaAsset] && tokenDTO.Display != nil {
				urls := tokenDTO.Display.MediaURLs()
				for _, url := range urls {
					mediaURLsMap[url] = true
				}
			}

			// Clean up metadata and enrichment_source if they weren't explicitly requested
			if !expansionMap[types.ExpansionMetadata] {
				tokenDTO.Metadata = nil
			}
			if !expansionMap[types.ExpansionEnrichmentSource] {
				tokenDTO.EnrichmentSource = nil
			}
		}

		// Collect media assets from mediaURLsMap
		var mediaURLs []string
		for url := range mediaURLsMap {
			mediaURLs = append(mediaURLs, url)
		}
		tokenDTO.MediaAssets = collectMediaAssetDTOsFromMap(mediaURLs, mediaAssetsMap)

		tokenDTOs[i] = *tokenDTO
	}

	// Replace data URI image/animation URLs in display fields with public variant URLs.
	if expansionMap[types.ExpansionDisplay] {
		if err := e.resolveDisplayDataURIs(ctx, tokenDTOs); err != nil {
			return nil, err
		}
	}

	tokenPtrs := make([]*dto.TokenResponse, len(tokenDTOs))
	for i := range tokenDTOs {
		tokenPtrs[i] = &tokenDTOs[i]
	}
	if err := e.applyReleaseMembership(ctx, tokenPtrs); err != nil {
		return nil, err
	}

	// Build response with pagination
	var nextOffset *uint64
	if hasMore {
		offsetVal := *offset + uint64(*limit)
		nextOffset = &offsetVal
	}

	return &dto.TokenListResponse{
		Tokens: tokenDTOs,
		Offset: nextOffset,
		Total:  0, // Deprecated: use the offset as the indicator for next page
	}, nil
}

func (e *executor) TriggerTokenIndexing(ctx context.Context, tokenCIDs []domain.TokenCID) (*dto.TriggerIndexingResponse, error) {
	// Normalize token CIDs
	normalizedTokenCIDs := make([]domain.TokenCID, len(tokenCIDs))
	for i, tokenCID := range tokenCIDs {
		normalizedTokenCIDs[i] = tokenCID.Normalized()
	}

	// Check for blacklisted contracts
	if e.blacklist != nil {
		for _, tokenCID := range normalizedTokenCIDs {
			if e.blacklist.IsTokenCIDBlacklisted(tokenCID) {
				return nil, apierrors.NewValidationError(fmt.Sprintf("contract is blacklisted: %s", tokenCID.String()))
			}
		}
	}

	j, _, err := e.jobQueue.Enqueue(ctx, jobs.EnqueueOptions{
		Queue: e.tokenQueue,
		Kind:  "IndexTokens",
		Args:  []any{normalizedTokenCIDs, nil},
	})
	if err != nil {
		return nil, apierrors.NewServiceError(fmt.Sprintf("Failed to trigger indexing: %v", err))
	}
	if j == nil {
		return nil, apierrors.NewServiceError("Failed to trigger indexing: empty job")
	}

	return newTriggerIndexingResponse(j.ID), nil
}

// newTriggerIndexingResponse builds a trigger response with canonical job_id and deprecated workflow/run fields for legacy clients.
func newTriggerIndexingResponse(jobID int64) *dto.TriggerIndexingResponse {
	// Deprecated JSON workflow_id; token/metadata triggers use decimal str(job_id) only.
	wf := strconv.FormatInt(jobID, 10)
	return &dto.TriggerIndexingResponse{
		JobID:      jobID,
		WorkflowID: wf,
		RunID:      nil, // legacy Temporal run id; unused for Postgres queue jobs
	}
}

// TriggerAddressIndexing triggers indexing for tokens by owner addresses.
// Creates individual workflows for each address with job tracking
// If an active job already exists for an address, returns the existing job info instead of creating a new one
func (e *executor) TriggerAddressIndexing(ctx context.Context, addresses []string) (*dto.TriggerAddressIndexingResponse, error) {
	// Normalize and deduplicate addresses
	normalizedAddresses := domain.NormalizeAddresses(addresses)
	uniqueAddresses := make([]string, 0, len(normalizedAddresses))
	seen := make(map[string]bool)
	for _, addr := range normalizedAddresses {
		if !seen[addr] {
			seen[addr] = true
			uniqueAddresses = append(uniqueAddresses, addr)
		}
	}

	outJobs := make([]dto.AddressIndexingJobInfo, 0, len(uniqueAddresses))

	for _, address := range uniqueAddresses {
		// Determine chain from address
		var chainID domain.Chain
		blockchain := internalTypes.AddressToBlockchain(address)
		switch blockchain {
		case domain.BlockchainTezos:
			chainID = e.tezosChainID
		case domain.BlockchainEthereum:
			chainID = e.ethereumChainID
		default:
			return nil, apierrors.NewValidationError(fmt.Sprintf("unsupported blockchain for address: %s", address))
		}

		// Check for existing active job (running or paused)
		existingJob, err := e.store.GetActiveIndexingJobForAddress(ctx, address, chainID)
		if err != nil {
			return nil, apierrors.NewServiceError(fmt.Sprintf("Failed to check existing jobs for address %s: %v", address, err))
		}

		if existingJob != nil {
			if existingJob.JobID == 0 {
				return nil, apierrors.NewServiceError(
					fmt.Sprintf("active indexing job for %s is missing job_id; cannot report progress", address))
			}
			// Deprecated response field workflow_id (JSON); mirrors address_indexing_jobs.workflow_id.
			wf := strings.TrimSpace(existingJob.WorkflowID)
			if wf == "" {
				wf = strconv.FormatInt(existingJob.JobID, 10)
			}
			outJobs = append(outJobs, dto.AddressIndexingJobInfo{
				Address:    address,
				JobID:      existingJob.JobID,
				WorkflowID: wf,
			})

			logger.Info(fmt.Sprintf("Found existing %s job for address", existingJob.Status),
				zap.String("address", address),
				zap.Int64("job_id", existingJob.JobID),
				zap.String("status", string(existingJob.Status)),
			)
			continue
		}

		uk := jobs.IndexTokenOwnerUniqueKey(chainID, address)
		pj, _, err := e.jobQueue.Enqueue(ctx, jobs.EnqueueOptions{
			Queue:     e.tokenQueue,
			Kind:      "IndexTokenOwner",
			Args:      []any{address},
			UniqueKey: &uk,
		})
		if err != nil {
			return nil, apierrors.NewServiceError(fmt.Sprintf("Failed to trigger indexing for address %s: %v", address, err))
		}
		if pj == nil {
			return nil, apierrors.NewServiceError(fmt.Sprintf("Failed to trigger indexing for address %s: empty job", address))
		}
		err = e.store.CreateAddressIndexingJob(ctx, store.CreateAddressIndexingJobInput{
			Address: address,
			Chain:   chainID,
			Status:  schema.IndexingJobStatusRunning,
			JobID:   pj.ID,
		})
		if err != nil {
			logger.Warn(fmt.Sprintf("Failed to create indexing job: %v", err),
				zap.String("address", address),
				zap.Int64("job_id", pj.ID))
		}

		outJobs = append(outJobs, dto.AddressIndexingJobInfo{
			Address: address,
			JobID:   pj.ID,
			// Deprecated JSON workflow_id; new rows use str(job_id).
			WorkflowID: strconv.FormatInt(pj.ID, 10),
		})

		logger.Info("Started new indexing job for address",
			zap.String("address", address),
			zap.Int64("job_id", pj.ID),
		)
	}

	// Return job information for all addresses
	return &dto.TriggerAddressIndexingResponse{Jobs: outJobs}, nil
}

func (e *executor) TriggerMetadataIndexing(ctx context.Context, tokenIDs []uint64, tokenCIDs []domain.TokenCID) (*dto.TriggerIndexingResponse, error) {
	// Validate provided token CIDs exist in database
	if len(tokenCIDs) > 0 {
		// Convert to strings for database query
		cidStrings := make([]string, len(tokenCIDs))
		for i, cid := range tokenCIDs {
			cidStrings[i] = cid.Normalized().String()
		}

		// Batch fetch to validate all CIDs exist
		existingTokens, err := e.store.GetTokensByCIDs(ctx, cidStrings)
		if err != nil {
			return nil, apierrors.NewDatabaseError(fmt.Sprintf("Failed to validate token CIDs: %v", err))
		}

		// Check if all token CIDs were found
		if len(existingTokens) != len(tokenCIDs) {
			foundCIDs := make(map[string]bool)
			for _, token := range existingTokens {
				foundCIDs[token.TokenCID] = true
			}
			for _, tokenCID := range tokenCIDs {
				if !foundCIDs[string(tokenCID)] {
					return nil, apierrors.NewNotFoundError(fmt.Sprintf("Token with CID %s not found", tokenCID))
				}
			}
		}
	}

	// Collect all token CIDs (from both provided CIDs and fetched by IDs)
	allTokenCIDs := make([]domain.TokenCID, 0, len(tokenIDs)+len(tokenCIDs))

	// Add provided token CIDs
	allTokenCIDs = append(allTokenCIDs, tokenCIDs...)

	// Batch fetch token CIDs for provided token IDs
	if len(tokenIDs) > 0 {
		tokens, err := e.store.GetTokensByIDs(ctx, tokenIDs)
		if err != nil {
			return nil, apierrors.NewDatabaseError(fmt.Sprintf("Failed to get tokens by IDs: %v", err))
		}

		// Check if all token IDs were found
		if len(tokens) != len(tokenIDs) {
			foundIDs := make(map[uint64]bool)
			for _, token := range tokens {
				foundIDs[token.ID] = true
			}
			for _, tokenID := range tokenIDs {
				if !foundIDs[tokenID] {
					return nil, apierrors.NewNotFoundError(fmt.Sprintf("Token with ID %d not found", tokenID))
				}
			}
		}

		// Add token CIDs from fetched tokens
		for _, token := range tokens {
			allTokenCIDs = append(allTokenCIDs, domain.TokenCID(token.TokenCID))
		}
	}

	// Deduplicate token CIDs
	cidMap := make(map[domain.TokenCID]bool)
	uniqueTokenCIDs := make([]domain.TokenCID, 0, len(allTokenCIDs))
	for _, tokenCID := range allTokenCIDs {
		if !cidMap[tokenCID] {
			cidMap[tokenCID] = true
			uniqueTokenCIDs = append(uniqueTokenCIDs, tokenCID)
		}
	}

	j, _, err := e.jobQueue.Enqueue(ctx, jobs.EnqueueOptions{
		Queue: e.tokenQueue,
		Kind:  "IndexMultipleTokensMetadata",
		Args:  []any{uniqueTokenCIDs},
	})
	if err != nil {
		return nil, apierrors.NewServiceError(fmt.Sprintf("Failed to trigger metadata indexing: %v", err))
	}
	if j == nil {
		return nil, apierrors.NewServiceError("Failed to trigger metadata indexing: empty job")
	}
	return newTriggerIndexingResponse(j.ID), nil
}

// Helper methods for expanding token data

// expandOwners expands the owners of a token
func (e *executor) expandOwners(ctx context.Context, tokenDTO *dto.TokenResponse, tokenID uint64, limit *uint8, offset *uint64) error {
	// If owners are already expanded, return nil
	if tokenDTO.Owners != nil {
		return nil
	}

	// Use defaults if not provided
	if limit == nil {
		defaultLimit := constants.DEFAULT_OWNERS_LIMIT
		limit = &defaultLimit
	}
	if offset == nil {
		defaultOffset := constants.DEFAULT_OFFSET
		offset = &defaultOffset
	}

	// Get owners
	owners, total, err := e.store.GetTokenOwners(ctx, tokenID, int(*limit), *offset)
	if err != nil {
		return apierrors.NewDatabaseError(fmt.Sprintf("Failed to get token owners: %v", err))
	}

	// Map to DTOs
	ownerDTOs := make([]dto.OwnerResponse, len(owners))
	for i := range owners {
		ownerDTOs[i] = *dto.MapOwnerToDTO(&owners[i])
	}

	var nextOffset *uint64
	if *offset+uint64(len(owners)) < total { //nolint:gosec,G115
		offsetVal := *offset + uint64(len(owners))
		nextOffset = &offsetVal
	}

	tokenDTO.Owners = &dto.PaginatedOwners{
		Owners: ownerDTOs,
		Offset: nextOffset,
		Total:  total,
	}

	return nil
}

// expandProvenanceEvents expands the provenance events of a token
func (e *executor) expandProvenanceEvents(ctx context.Context, tokenDTO *dto.TokenResponse, tokenID uint64, limit *uint8, offset *uint64, order *types.Order) error {
	// If provenance events are already expanded, return nil
	if tokenDTO.ProvenanceEvents != nil {
		return nil
	}

	// Use defaults if not provided
	if limit == nil {
		defaultLimit := constants.DEFAULT_PROVENANCE_EVENTS_LIMIT
		limit = &defaultLimit
	}
	if offset == nil {
		defaultOffset := constants.DEFAULT_OFFSET
		offset = &defaultOffset
	}
	orderDesc := order == nil || order.Desc() // Default to DESC

	// Get provenance events
	events, total, err := e.store.GetTokenProvenanceEvents(ctx, tokenID, int(*limit), *offset, orderDesc)
	if err != nil {
		return apierrors.NewDatabaseError(fmt.Sprintf("Failed to get provenance events: %v", err))
	}

	// Map to DTOs
	eventDTOs := make([]dto.ProvenanceEventResponse, len(events))
	for i := range events {
		eventDTOs[i] = *dto.MapProvenanceEventToDTO(&events[i])
	}

	var nextOffset *uint64
	if *offset+uint64(len(events)) < total { //nolint:gosec,G115
		offsetVal := *offset + uint64(len(events))
		nextOffset = &offsetVal
	}

	tokenDTO.ProvenanceEvents = &dto.PaginatedProvenanceEvents{
		Events: eventDTOs,
		Offset: nextOffset,
		Total:  total,
	}

	return nil
}

// expandMetadata expands the metadata of a token
func (e *executor) expandMetadata(ctx context.Context, tokenDTO *dto.TokenResponse, tokenID uint64) error {
	// If metadata is already expanded, return nil
	if tokenDTO.Metadata != nil {
		return nil
	}

	// Get metadata
	metadata, err := e.store.GetTokenMetadataByTokenID(ctx, tokenID)
	if err != nil {
		return apierrors.NewDatabaseError(fmt.Sprintf("Failed to get token metadata: %v", err))
	}
	if metadata != nil {
		tokenDTO.Metadata = dto.MapTokenMetadataToDTO(metadata)
	}
	return nil
}

// expandEnrichmentSource expands the enrichment source of a token
func (e *executor) expandEnrichmentSource(ctx context.Context, tokenDTO *dto.TokenResponse, tokenID uint64) error {
	// If enrichment source is already expanded, return nil
	if tokenDTO.EnrichmentSource != nil {
		return nil
	}

	// Get enrichment source
	enrichment, err := e.store.GetEnrichmentSourceByTokenID(ctx, tokenID)
	if err != nil {
		return apierrors.NewDatabaseError(fmt.Sprintf("Failed to get enrichment source: %v", err))
	}
	if enrichment != nil {
		tokenDTO.EnrichmentSource = dto.MapEnrichmentSourceToDTO(enrichment)
	}
	return nil
}

// collectMediaAssetDTOsFromMap collects media asset DTOs by looking up URLs in the provided map
func collectMediaAssetDTOsFromMap(urls []string, mediaAssetsMap map[string]schema.MediaAsset) []dto.MediaAssetResponse {
	if len(urls) == 0 {
		return nil
	}
	var mediaDTOs []dto.MediaAssetResponse
	for _, url := range urls {
		lookupKey := mediaAssetLookupKey(url)
		if asset, ok := mediaAssetsMap[lookupKey]; ok {
			mediaDTOs = append(mediaDTOs, *dto.MapMediaAssetToDTO(&asset))
		}
	}
	return mediaDTOs
}

func mediaAssetLookupKey(url string) string {
	if url == "" {
		return ""
	}
	return internalTypes.MD5Hash(url)
}

// populateHealthFilteredDisplay merges metadata and enrichment source into tokenDTO.Display and
// applies health filtering against token_media_health so only confirmed-healthy URLs are served.
//
// It is idempotent: if tokenDTO.Display is already set the call is a no-op, which allows callers
// to call it from multiple code paths without incurring a second DB round trip.
//
// Health state is a correctness dependency of display: a failure here should propagate like any
// other expansion DB failure (metadata, enrichment) rather than silently fall back to serving
// stale or broken URLs. Callers must check the returned error.
func (e *executor) populateHealthFilteredDisplay(ctx context.Context, tokenDTO *dto.TokenResponse, tokenID uint64) error {
	if tokenDTO.Display != nil {
		return nil
	}
	tokenDTO.Display = dto.MergeTokenDisplay(tokenDTO.Metadata, tokenDTO.EnrichmentSource)
	healthByToken, err := e.store.GetTokenMediaHealthByTokenIDs(ctx, []uint64{tokenID})
	if err != nil {
		return apierrors.NewDatabaseError(fmt.Sprintf("Failed to get token media health: %v", err))
	}
	tokenDTO.Display = dto.ApplyHealthyMediaURLs(tokenDTO.Display, healthByToken[tokenID])
	return nil
}

// containsExpansion reports whether exp appears in the expansions slice.
func containsExpansion(expansions []types.Expansion, exp types.Expansion) bool {
	for _, e := range expansions {
		if e == exp {
			return true
		}
	}
	return false
}

// expandMediaAssets expands both metadata and enrichment source media assets (unified approach)
// It respects the original expansion request to determine which media sources to include
func (e *executor) expandMediaAssets(ctx context.Context, tokenDTO *dto.TokenResponse, expansions []types.Expansion) error {
	// Early return if media assets already expanded (prevents redundant calls)
	if tokenDTO.MediaAssets != nil {
		return nil
	}

	// Map to track which expansions were requested
	expansionMap := make(map[types.Expansion]bool)
	for _, exp := range expansions {
		expansionMap[exp] = true
	}

	// Collect source URLs used to fetch media assets
	var sourceURLs []string

	if expansionMap[types.ExpansionMediaAsset] && (expansionMap[types.ExpansionMetadata] || expansionMap[types.ExpansionDisplay]) {
		// Get metadata if not already present
		if tokenDTO.Metadata == nil {
			metadata, err := e.store.GetTokenMetadataByTokenID(ctx, tokenDTO.ID)
			if err != nil {
				return apierrors.NewDatabaseError(fmt.Sprintf("Failed to get token metadata: %v", err))
			}
			if metadata != nil {
				tokenDTO.Metadata = dto.MapTokenMetadataToDTO(metadata)
			}
		}
		// Collect source URLs from metadata
		if tokenDTO.Metadata != nil {
			sourceURLs = append(sourceURLs, tokenDTO.Metadata.MediaURLs()...)
		}
	}

	if expansionMap[types.ExpansionMediaAsset] && (expansionMap[types.ExpansionEnrichmentSource] || expansionMap[types.ExpansionDisplay]) {
		// Get enrichment source if not already present
		if tokenDTO.EnrichmentSource == nil {
			enrichment, err := e.store.GetEnrichmentSourceByTokenID(ctx, tokenDTO.ID)
			if err != nil {
				return apierrors.NewDatabaseError(fmt.Sprintf("Failed to get enrichment source: %v", err))
			}
			if enrichment != nil {
				tokenDTO.EnrichmentSource = dto.MapEnrichmentSourceToDTO(enrichment)
			}
		}
		// Collect source URLs from enrichment source
		if tokenDTO.EnrichmentSource != nil {
			sourceURLs = append(sourceURLs, tokenDTO.EnrichmentSource.MediaURLs()...)
		}
	}

	// Add health-filtered display URLs to the DB lookup so that media assets are always
	// fetchable for any URL that ApplyHealthyMediaURLs selected from token_media_health.media_url.
	// ApplyHealthyMediaURLs reads the URL directly from the health row, which can differ from the
	// raw metadata/enrichment URL during the narrow window between a propagation transaction commit
	// and the metadata bulk read. Without this, media assets for health-row URLs that are absent
	// from metadata/enrichment are never queried, so collectMediaAssetDTOsFromMap returns nothing
	// for them — contradicting the OpenAPI contract that display+media_asset surfaces assets for
	// the health-filtered display URLs.
	// populateHealthFilteredDisplay must be called before expandMediaAssets for this to be effective.
	if expansionMap[types.ExpansionDisplay] && tokenDTO.Display != nil {
		sourceURLs = append(sourceURLs, tokenDTO.Display.MediaURLs()...)
	}

	if len(sourceURLs) == 0 {
		return nil
	}

	lookupSourceURLs := make([]string, 0, len(sourceURLs))
	lookupSet := make(map[string]bool)
	for _, url := range sourceURLs {
		lookupKey := mediaAssetLookupKey(url)
		if lookupKey == "" || lookupSet[lookupKey] {
			continue
		}
		lookupSet[lookupKey] = true
		lookupSourceURLs = append(lookupSourceURLs, url)
	}
	if len(lookupSourceURLs) == 0 {
		return nil
	}

	// Query media assets
	mediaAssets, err := e.store.GetMediaAssetsBySourceURLs(ctx, lookupSourceURLs)
	if err != nil {
		return apierrors.NewDatabaseError(fmt.Sprintf("Failed to get media assets: %v", err))
	}

	// Turn media assets into a map for fast lookup
	mediaAssetsMap := make(map[string]schema.MediaAsset)
	for _, asset := range mediaAssets {
		mediaAssetsMap[asset.SourceURLHash] = asset
	}

	// Map to track media URLs should be included in the response
	mediaURLsMap := make(map[string]bool)

	// Apply expansions
	for _, exp := range expansions {
		switch exp {
		case types.ExpansionMediaAsset:
			// New unified expansion: only add URLs from explicitly requested sources
			// Collect metadata media URLs if metadata was explicitly requested
			if expansionMap[types.ExpansionMetadata] && tokenDTO.Metadata != nil {
				urls := tokenDTO.Metadata.MediaURLs()
				for _, url := range urls {
					mediaURLsMap[url] = true
				}
			}

			// Collect enrichment source media URLs if enrichment source was explicitly requested
			if expansionMap[types.ExpansionEnrichmentSource] && tokenDTO.EnrichmentSource != nil {
				urls := tokenDTO.EnrichmentSource.MediaURLs()
				for _, url := range urls {
					mediaURLsMap[url] = true
				}
			}

		}
	}

	// Collect media URLs from display if both display and media_asset are requested.
	// Use tokenDTO.Display (already health-filtered) rather than re-running MergeTokenDisplay,
	// so media assets are consistent with what is actually surfaced in the display field.
	if expansionMap[types.ExpansionDisplay] && expansionMap[types.ExpansionMediaAsset] {
		if tokenDTO.Display != nil {
			for _, url := range tokenDTO.Display.MediaURLs() {
				mediaURLsMap[url] = true
			}
		}
	}

	// Collect media assets from mediaURLsMap
	var mediaURLs []string
	for url := range mediaURLsMap {
		mediaURLs = append(mediaURLs, url)
	}
	tokenDTO.MediaAssets = collectMediaAssetDTOsFromMap(mediaURLs, mediaAssetsMap)

	return nil
}

// GetJobStatus retrieves the status of a job row
func (e *executor) GetJobStatus(ctx context.Context, jobID int64) (*dto.JobStatusResponse, error) {
	j, err := e.jobQueue.GetStatus(ctx, jobID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, apierrors.NewNotFoundError("Job not found")
		}
		return nil, apierrors.NewServiceError(fmt.Sprintf("Failed to get job: %v", err))
	}

	var startTime *time.Time
	if j.StartedAt != nil {
		t := *j.StartedAt
		startTime = &t
	}
	var closeTime *time.Time
	if j.FinishedAt != nil {
		t := *j.FinishedAt
		closeTime = &t
	}
	var executionTime *uint64
	if startTime != nil && closeTime != nil {
		duration := uint64(closeTime.Sub(*startTime).Milliseconds()) //nolint:gosec,G115
		executionTime = &duration
	}

	return &dto.JobStatusResponse{
		JobID:         j.ID,
		Status:        string(j.Status),
		LastError:     j.LastError,
		StartTime:     startTime,
		CloseTime:     closeTime,
		ExecutionTime: executionTime,
	}, nil
}

// CreateWebhookClient creates a new webhook client
func (e *executor) CreateWebhookClient(ctx context.Context, webhookURL string, eventFilters []string, retryMaxAttempts int) (*dto.CreateWebhookClientResponse, error) {
	// Generate client ID (UUID v4)
	clientID, err := internalTypes.GenerateUUID()
	if err != nil {
		return nil, apierrors.NewInternalError(fmt.Sprintf("Failed to generate client ID: %v", err))
	}

	// Generate webhook secret (secure random 64-character hex string)
	webhookSecret, err := internalTypes.GenerateSecureToken(constants.DEFAULT_WEBHOOK_CLIENT_SECRET_LENGTH) // 32 bytes = 64 hex characters
	if err != nil {
		return nil, apierrors.NewInternalError(fmt.Sprintf("Failed to generate webhook secret: %v", err))
	}

	// Marshal event filters to JSON
	eventFiltersJSON, err := e.json.Marshal(eventFilters)
	if err != nil {
		return nil, apierrors.NewInternalError(fmt.Sprintf("Failed to marshal event filters: %v", err))
	}

	// Create webhook client input
	input := store.CreateWebhookClientInput{
		ClientID:         clientID,
		WebhookURL:       webhookURL,
		WebhookSecret:    webhookSecret,
		EventFilters:     eventFiltersJSON,
		IsActive:         true,
		RetryMaxAttempts: retryMaxAttempts,
	}

	// Save to database
	webhookClient, err := e.store.CreateWebhookClient(ctx, input)
	if err != nil {
		return nil, apierrors.NewDatabaseError(fmt.Sprintf("Failed to create webhook client: %v", err))
	}

	logger.Info("Webhook client created",
		zap.String("client_id", clientID),
		zap.String("webhook_url", webhookURL),
		zap.Strings("event_filters", eventFilters),
	)

	return &dto.CreateWebhookClientResponse{
		ClientID:         webhookClient.ClientID,
		WebhookURL:       webhookClient.WebhookURL,
		WebhookSecret:    webhookClient.WebhookSecret,
		EventFilters:     eventFilters,
		IsActive:         webhookClient.IsActive,
		RetryMaxAttempts: webhookClient.RetryMaxAttempts,
		CreatedAt:        webhookClient.CreatedAt,
		UpdatedAt:        webhookClient.UpdatedAt,
	}, nil
}

// GetAddressIndexingJob retrieves an address indexing job by postgres jobs.id.
func (e *executor) GetAddressIndexingJob(ctx context.Context, jobID int64, opts GetAddressIndexingJobOptions) (*dto.AddressIndexingJobResponse, error) {
	job, err := e.store.GetAddressIndexingJobByJobID(ctx, jobID)
	if err != nil {
		return nil, apierrors.NewNotFoundError(fmt.Sprintf("Indexing job not found: %v", err))
	}
	return e.buildAddressIndexingJobResponse(ctx, job, opts)
}

// GetAddressIndexingJobByLegacyWorkflowID handles deprecated GraphQL/address-indexing arguments named workflow_id:
// unsigned decimal jobs.id, or exact match on address_indexing_jobs.workflow_id (opaque legacy string).
func (e *executor) GetAddressIndexingJobByLegacyWorkflowID(ctx context.Context, workflowID string, opts GetAddressIndexingJobOptions) (*dto.AddressIndexingJobResponse, error) {
	wf := strings.TrimSpace(workflowID) // client-supplied deprecated workflow_id
	if wf == "" {
		return nil, apierrors.NewValidationError("workflow_id is required")
	}
	var job *schema.AddressIndexingJob
	var err error
	if jobID, perr := internalTypes.Int64FromUnsignedDecimalString(wf); perr == nil && jobID >= 1 {
		job, err = e.store.GetAddressIndexingJobByJobID(ctx, jobID)
	} else {
		job, err = e.store.GetAddressIndexingJobByWorkflowID(ctx, wf)
	}
	if err != nil {
		return nil, apierrors.NewNotFoundError(fmt.Sprintf("Indexing job not found: %v", err))
	}
	return e.buildAddressIndexingJobResponse(ctx, job, opts)
}

func (e *executor) buildAddressIndexingJobResponse(ctx context.Context, job *schema.AddressIndexingJob, opts GetAddressIndexingJobOptions) (*dto.AddressIndexingJobResponse, error) {
	// Deprecated JSON field workflow_id: echo persisted address_indexing_jobs.workflow_id (fallback str(job_id)).
	wf := strings.TrimSpace(job.WorkflowID)
	if wf == "" {
		wf = strconv.FormatInt(job.JobID, 10)
	}
	response := &dto.AddressIndexingJobResponse{
		JobID:           job.JobID,
		WorkflowID:      wf,
		Address:         job.Address,
		Chain:           string(job.Chain),
		Status:          string(job.Status),
		TokensProcessed: job.TokensProcessed,
		CurrentMinBlock: job.CurrentMinBlock,
		CurrentMaxBlock: job.CurrentMaxBlock,
		StartedAt:       job.StartedAt,
		PausedAt:        job.PausedAt,
		CompletedAt:     job.CompletedAt,
		FailedAt:        job.FailedAt,
		CanceledAt:      job.CanceledAt,
	}

	if opts.IncludeTotalIndexed || opts.IncludeTotalViewable {
		counts, err := e.store.GetTokenCountsByAddress(ctx, job.Address, job.Chain)
		if err != nil {
			logger.WarnCtx(ctx, "Failed to get token counts",
				zap.Error(err),
				zap.String("address", job.Address),
				zap.String("chain", string(job.Chain)),
			)
		} else {
			if opts.IncludeTotalIndexed {
				response.TotalTokensIndexed = &counts.TotalIndexed
			}
			if opts.IncludeTotalViewable {
				response.TotalTokensViewable = &counts.TotalViewable
			}
		}
	}

	return response, nil
}

// resolveDisplayDataURIs replaces data URI image/animation URLs in the given token displays
// with the corresponding "public" variant URL from the media_assets table.
//
// To avoid redundant DB queries, it first builds a lookup from each token's already-fetched
// MediaAssets (populated when the media_asset expansion is also requested). Only data URIs
// that cannot be resolved from in-memory assets are then fetched from the DB in one bulk query.
func (e *executor) resolveDisplayDataURIs(ctx context.Context, tokens []dto.TokenResponse) error {
	// Seed the lookup from already-fetched media assets on each token DTO.
	publicURLs := make(map[string]string)
	for _, token := range tokens {
		for _, asset := range token.MediaAssets {
			if asset.VariantURLs == nil {
				continue
			}
			var variants map[string]string
			if err := e.json.Unmarshal(asset.VariantURLs, &variants); err != nil {
				continue
			}
			if publicURL, ok := variants["public"]; ok && publicURL != "" {
				publicURLs[asset.SourceURL] = publicURL
			}
		}
	}

	// Collect data URIs that still need a DB lookup (not already in memory).
	var needsDB []string
	seen := make(map[string]bool)
	for _, token := range tokens {
		if token.Display == nil {
			continue
		}
		for _, urlPtr := range []*string{token.Display.ImageURL, token.Display.AnimationURL} {
			if urlPtr == nil || !internalTypes.IsDataURI(*urlPtr) || seen[*urlPtr] {
				continue
			}
			seen[*urlPtr] = true
			if _, found := publicURLs[*urlPtr]; !found {
				needsDB = append(needsDB, *urlPtr)
			}
		}
	}

	if len(needsDB) > 0 {
		assets, err := e.store.GetMediaAssetsBySourceURLs(ctx, needsDB)
		if err != nil {
			return apierrors.NewDatabaseError(fmt.Sprintf("Failed to get media assets for display data URIs: %v", err))
		}
		for _, asset := range assets {
			if asset.VariantURLs == nil {
				continue
			}
			var variants map[string]string
			if err := e.json.Unmarshal([]byte(asset.VariantURLs), &variants); err != nil {
				continue
			}
			if publicURL, ok := variants["public"]; ok && publicURL != "" {
				publicURLs[asset.SourceURL] = publicURL
			}
		}
	}

	// If there is nothing to replace, return early.
	if len(publicURLs) == 0 {
		return nil
	}

	// Apply replacements in-place on each display and media asset.
	// Media asset source URLs are updated only when they match a data URI that appeared
	// in the display, keeping them consistent with the resolved display URLs.
	for i := range tokens {
		token := &tokens[i]

		// Collect the data URIs actually used in this token's display so that media
		// asset updates are scoped to those exact URLs.
		displayDataURIs := make(map[string]bool)
		if token.Display != nil {
			if token.Display.ImageURL != nil && internalTypes.IsDataURI(*token.Display.ImageURL) {
				displayDataURIs[*token.Display.ImageURL] = true
				if publicURL, ok := publicURLs[*token.Display.ImageURL]; ok {
					token.Display.ImageURL = &publicURL
				}
			}
			if token.Display.AnimationURL != nil && internalTypes.IsDataURI(*token.Display.AnimationURL) {
				displayDataURIs[*token.Display.AnimationURL] = true
				if publicURL, ok := publicURLs[*token.Display.AnimationURL]; ok {
					token.Display.AnimationURL = &publicURL
				}
			}
		}

		if len(displayDataURIs) == 0 {
			continue
		}

		for j := range token.MediaAssets {
			sourceURL := token.MediaAssets[j].SourceURL
			if displayDataURIs[sourceURL] {
				if publicURL, ok := publicURLs[sourceURL]; ok {
					token.MediaAssets[j].SourceURL = publicURL
				}
			}
		}
	}
	return nil
}

// SyncCollection retrieves token changes for an address since a checkpoint with cursor-based pagination
func (e *executor) SyncCollection(ctx context.Context, address string, checkpoint *dto.SyncCheckpoint, limit uint8) (*dto.SyncCollectionResponse, error) {
	normalizedAddress := domain.NormalizeAddress(address)

	// Extract checkpoint values (use zero values for initial sync)
	var sinceTimestamp time.Time
	var sinceEventID uint64
	if checkpoint != nil {
		sinceTimestamp = checkpoint.Timestamp
		sinceEventID = checkpoint.EventID
	}

	// Capture server time at query execution
	serverTime := time.Now()

	// Request limit+1 to detect if there are more results
	events, err := e.store.GetTokenChangesByOwnerSinceCheckpoint(ctx, normalizedAddress, sinceTimestamp, sinceEventID, int(limit)+1)
	if err != nil {
		return nil, fmt.Errorf("failed to get token changes: %w", err)
	}

	// Check if there are more results
	hasMore := len(events) > int(limit)
	if hasMore {
		// Return only the requested limit
		events = events[:limit]
	}

	// NextCheckpoint: use the last event (by created_at ordering from DB)
	// Since DB returns events sorted by created_at, the last event has the max created_at
	var nextCheckpoint *dto.SyncCheckpoint
	if len(events) > 0 {
		lastEvent := events[len(events)-1]
		nextCheckpoint = &dto.SyncCheckpoint{
			Timestamp: lastEvent.CreatedAt,
			EventID:   lastEvent.ID,
		}
	}

	// Sort events by occurred_at for chronological display to client
	// DB returns them sorted by created_at for pagination consistency
	sort.Slice(events, func(i, j int) bool {
		if events[i].OccurredAt.Equal(events[j].OccurredAt) {
			return events[i].ID < events[j].ID
		}
		return events[i].OccurredAt.Before(events[j].OccurredAt)
	})

	return &dto.SyncCollectionResponse{
		Events:         dto.MapTokenEventsToDTOs(events),
		NextCheckpoint: nextCheckpoint,
		ServerTime:     serverTime,
	}, nil
}

func (e *executor) GetRelease(ctx context.Context, releaseID uint64) (*dto.ReleaseResponse, error) {
	release, err := e.store.GetReleaseByID(ctx, releaseID)
	if err != nil {
		return nil, apierrors.NewDatabaseError(fmt.Sprintf("Failed to get release: %v", err))
	}
	if release == nil {
		return nil, nil
	}

	return dto.MapReleaseToDTO(release), nil
}

// ListReleases retrieves releases filtered by vendor and/or vendor_release_id without member tokens.
func (e *executor) ListReleases(ctx context.Context, vendor *schema.Vendor, vendorReleaseID *string, limit *uint8, offset *uint64) (*dto.ReleaseListResponse, error) {
	if limit == nil {
		defaultLimit := constants.DEFAULT_TOKENS_LIMIT
		limit = &defaultLimit
	}
	if offset == nil {
		defaultOffset := constants.DEFAULT_OFFSET
		offset = &defaultOffset
	}

	filter := store.ReleaseQueryFilter{
		Vendor:          vendor,
		VendorReleaseID: vendorReleaseID,
		Limit:           int(*limit) + 1,
		Offset:          *offset,
	}

	releases, err := e.store.ListReleases(ctx, filter)
	if err != nil {
		return nil, apierrors.NewDatabaseError(fmt.Sprintf("Failed to list releases: %v", err))
	}

	limitInt := int(*limit)
	hasMore := len(releases) > limitInt
	if hasMore {
		releases = releases[:limitInt]
	}

	items := make([]dto.ReleaseResponse, len(releases))
	for i := range releases {
		mapped := dto.MapReleaseToDTO(&releases[i])
		if mapped != nil {
			items[i] = *mapped
		}
	}

	var nextOffset *uint64
	if hasMore {
		offsetVal := *offset + uint64(*limit)
		nextOffset = &offsetVal
	}

	return &dto.ReleaseListResponse{
		Items:  items,
		Offset: nextOffset,
		Total:  0,
	}, nil
}

func (e *executor) applyReleaseMembership(ctx context.Context, tokens []*dto.TokenResponse) error {
	if len(tokens) == 0 {
		return nil
	}

	tokenIDs := make([]uint64, 0, len(tokens))
	for _, token := range tokens {
		if token != nil {
			tokenIDs = append(tokenIDs, token.ID)
		}
	}

	members, err := e.store.GetReleaseMembersByTokenIDs(ctx, tokenIDs)
	if err != nil {
		return apierrors.NewDatabaseError(fmt.Sprintf("Failed to get release members: %v", err))
	}

	for _, token := range tokens {
		if token == nil {
			continue
		}
		member, ok := members[token.ID]
		if !ok || member == nil {
			continue
		}
		releaseID := member.ReleaseID
		mintNumber := member.MintNumber
		token.ReleaseID = &releaseID
		token.MintNumber = &mintNumber
	}

	return nil
}

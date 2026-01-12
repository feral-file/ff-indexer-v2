package executor

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"go.temporal.io/sdk/client"
	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/api/shared/constants"
	"github.com/feral-file/ff-indexer-v2/internal/api/shared/dto"
	apierrors "github.com/feral-file/ff-indexer-v2/internal/api/shared/errors"
	"github.com/feral-file/ff-indexer-v2/internal/api/shared/types"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/providers/temporal"
	"github.com/feral-file/ff-indexer-v2/internal/registry"
	"github.com/feral-file/ff-indexer-v2/internal/store"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
	internalTypes "github.com/feral-file/ff-indexer-v2/internal/types"
	"github.com/feral-file/ff-indexer-v2/internal/workflows"
)

// Executor is the interface for the API executor
//
//go:generate mockgen -source=executor.go -destination=../../../mocks/api_executor.go -package=mocks -mock_names=Executor=MockAPIExecutor
type Executor interface {
	// GetToken retrieves a single token by its CID with optional expansions
	GetToken(ctx context.Context, tokenCID string, expand []types.Expansion, ownersLimit *uint8, ownersOffset *uint64, provenanceEventsLimit *uint8, provenanceEventsOffset *uint64, provenanceEventsOrder *types.Order) (*dto.TokenResponse, error)

	// GetTokens retrieves tokens with optional filters and expansions
	GetTokens(ctx context.Context, owners []string, chains []domain.Chain, contractAddresses []string, tokenNumbers []string, tokenIDs []uint64, tokenCIDs []string, limit *uint8, offset *uint64, includeBroken *bool, expand []types.Expansion, ownersLimit *uint8, ownersOffset *uint64, provenanceEventsLimit *uint8, provenanceEventsOffset *uint64, provenanceEventsOrder *types.Order) (*dto.TokenListResponse, error)

	// GetChanges retrieves changes with optional filters and expansions
	// Returns changes in ascending order by ID (sequential audit log)
	// Note: 'order' parameter only applies when using deprecated 'since' parameter
	// Deprecated parameter: since, offset, order (use anchor instead for reliable pagination)
	GetChanges(ctx context.Context, tokenIDs []uint64, tokenCIDs []string, addresses []string, subjectTypes []schema.SubjectType, subjectIDs []string, anchor *uint64, since *time.Time, limit *uint8, offset *uint64, order *types.Order, expand []types.Expansion) (*dto.ChangeListResponse, error)

	// TriggerTokenIndexing triggers indexing for one or more tokens and addresses
	TriggerTokenIndexing(ctx context.Context, tokenCIDs []domain.TokenCID, addresses []string) (*dto.TriggerIndexingResponse, error)

	// TriggerMetadataIndexing triggers metadata refresh for one or more tokens by IDs or CIDs
	TriggerMetadataIndexing(ctx context.Context, tokenIDs []uint64, tokenCIDs []domain.TokenCID) (*dto.TriggerIndexingResponse, error)

	// GetWorkflowStatus retrieves the status of a Temporal workflow execution
	GetWorkflowStatus(ctx context.Context, workflowID, runID string) (*dto.WorkflowStatusResponse, error)

	// CreateWebhookClient creates a new webhook client
	CreateWebhookClient(ctx context.Context, webhookURL string, eventFilters []string, retryMaxAttempts int) (*dto.CreateWebhookClientResponse, error)
}

type executor struct {
	store                 store.Store
	orchestrator          temporal.TemporalOrchestrator
	orchestratorTaskQueue string
	blacklist             registry.BlacklistRegistry
	json                  adapter.JSON
	clock                 adapter.Clock
}

func NewExecutor(store store.Store, orchestrator temporal.TemporalOrchestrator, orchestratorTaskQueue string, blacklist registry.BlacklistRegistry, json adapter.JSON, clock adapter.Clock) Executor {
	return &executor{
		store:                 store,
		orchestrator:          orchestrator,
		orchestratorTaskQueue: orchestratorTaskQueue,
		blacklist:             blacklist,
		json:                  json,
		clock:                 clock,
	}
}

func (e *executor) GetToken(ctx context.Context, tokenCID string, expand []types.Expansion, ownersLimit *uint8, ownersOffset *uint64, provenanceEventsLimit *uint8, provenanceEventsOffset *uint64, provenanceEventsOrder *types.Order) (*dto.TokenResponse, error) {
	// Normalize token CID
	normalizedTokenCID := domain.TokenCID(tokenCID).Normalized().String()

	// Get token with metadata
	result, err := e.store.GetTokenWithMetadataByTokenCID(ctx, normalizedTokenCID)
	if err != nil {
		return nil, apierrors.NewDatabaseError(fmt.Sprintf("Failed to get token: %v", err))
	}

	if result == nil {
		return nil, nil
	}

	// Map to DTO
	tokenDTO := dto.MapTokenToDTO(result.Token, result.Metadata)

	// Handle expansions
	for _, exp := range expand {
		switch exp {
		case types.ExpansionOwners:
			if err := e.expandOwners(ctx, tokenDTO, result.Token.ID, ownersLimit, ownersOffset); err != nil {
				return nil, err
			}
		case types.ExpansionProvenanceEvents:
			if err := e.expandProvenanceEvents(ctx, tokenDTO, result.Token.ID, provenanceEventsLimit, provenanceEventsOffset, provenanceEventsOrder); err != nil {
				return nil, err
			}
		case types.ExpansionEnrichmentSource:
			if err := e.expandEnrichmentSource(ctx, tokenDTO, result.Token.ID); err != nil {
				return nil, err
			}
		case types.ExpansionMetadataMediaAsset:
			if err := e.expandMetadataMediaAssets(ctx, tokenDTO); err != nil {
				return nil, err
			}
		case types.ExpansionEnrichmentSourceMediaAsset:
			if err := e.expandEnrichmentSourceMediaAssets(ctx, tokenDTO); err != nil {
				return nil, err
			}
		}
	}

	return tokenDTO, nil
}

func (e *executor) GetTokens(ctx context.Context, owners []string, chains []domain.Chain, contractAddresses []string, tokenNumbers []string, tokenIDs []uint64, tokenCIDs []string, limit *uint8, offset *uint64, includeBroken *bool, expand []types.Expansion, ownersLimit *uint8, ownersOffset *uint64, provenanceEventsLimit *uint8, provenanceEventsOffset *uint64, provenanceEventsOrder *types.Order) (*dto.TokenListResponse, error) {
	// Use defaults if not provided
	if limit == nil {
		defaultLimit := constants.DEFAULT_TOKENS_LIMIT
		limit = &defaultLimit
	}
	if offset == nil {
		defaultOffset := constants.DEFAULT_OFFSET
		offset = &defaultOffset
	}
	if includeBroken == nil {
		defaultIncludeBroken := false
		includeBroken = &defaultIncludeBroken
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

	// Build filter
	filter := store.TokenQueryFilter{
		Owners:            owners,
		ContractAddresses: contractAddresses,
		TokenNumbers:      tokenNumbers,
		TokenIDs:          tokenIDs,
		Chains:            chains,
		TokenCIDs:         tokenCIDs,
		IncludeBroken:     *includeBroken,
		Limit:             int(*limit),
		Offset:            *offset,
	}

	// Get tokens
	results, total, err := e.store.GetTokensByFilter(ctx, filter)
	if err != nil {
		return nil, apierrors.NewDatabaseError(fmt.Sprintf("Failed to get tokens: %v", err))
	}

	// Collect token IDs for bulk queries
	tokenIDsForBulk := make([]uint64, len(results))
	for i, result := range results {
		tokenIDsForBulk[i] = result.Token.ID
	}

	// Fetch bulk data for expansions
	var bulkOwners map[uint64][]schema.Balance
	var bulkOwnersTotals map[uint64]uint64
	var bulkProvenanceEvents map[uint64][]schema.ProvenanceEvent
	var bulkProvenanceTotals map[uint64]uint64
	var bulkEnrichmentSources map[uint64]*schema.EnrichmentSource
	var allMediaSourceURLs []string

	for _, exp := range expand {
		switch exp {
		case types.ExpansionOwners:
			// Default limit for bulk owner queries
			bulkOwners, bulkOwnersTotals, err = e.store.GetTokenOwnersBulk(ctx, tokenIDsForBulk, int(constants.DEFAULT_OWNERS_LIMIT))
			if err != nil {
				return nil, apierrors.NewDatabaseError(fmt.Sprintf("Failed to get owners: %v", err))
			}
		case types.ExpansionProvenanceEvents:
			// Default limit for bulk provenance queries
			bulkProvenanceEvents, bulkProvenanceTotals, err = e.store.GetTokenProvenanceEventsBulk(ctx, tokenIDsForBulk, int(constants.DEFAULT_PROVENANCE_EVENTS_LIMIT))
			if err != nil {
				return nil, apierrors.NewDatabaseError(fmt.Sprintf("Failed to get provenance events: %v", err))
			}
		case types.ExpansionEnrichmentSource, types.ExpansionEnrichmentSourceMediaAsset:
			if bulkEnrichmentSources == nil { // Avoid fetching twice if both expansions are requested
				bulkEnrichmentSources, err = e.store.GetEnrichmentSourcesByTokenIDs(ctx, tokenIDsForBulk)
				if err != nil {
					return nil, apierrors.NewDatabaseError(fmt.Sprintf("Failed to get enrichment sources: %v", err))
				}
			}
		}
	}

	// Collect all source URLs for media assets if needed
	for _, exp := range expand {
		if exp == types.ExpansionMetadataMediaAsset || exp == types.ExpansionEnrichmentSourceMediaAsset {
			for _, result := range results {
				if exp == types.ExpansionMetadataMediaAsset && result.Metadata != nil {
					if !internalTypes.StringNilOrEmpty(result.Metadata.ImageURL) {
						allMediaSourceURLs = append(allMediaSourceURLs, *result.Metadata.ImageURL)
					}
					if !internalTypes.StringNilOrEmpty(result.Metadata.AnimationURL) {
						allMediaSourceURLs = append(allMediaSourceURLs, *result.Metadata.AnimationURL)
					}
				}
				if exp == types.ExpansionEnrichmentSourceMediaAsset {
					if enrichment, ok := bulkEnrichmentSources[result.Token.ID]; ok && enrichment != nil {
						if !internalTypes.StringNilOrEmpty(enrichment.ImageURL) {
							allMediaSourceURLs = append(allMediaSourceURLs, *enrichment.ImageURL)
						}
						if !internalTypes.StringNilOrEmpty(enrichment.AnimationURL) {
							allMediaSourceURLs = append(allMediaSourceURLs, *enrichment.AnimationURL)
						}
					}
				}
			}
			break // Only need to collect once
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
			mediaAssetsMap[asset.SourceURL] = asset
		}
	}

	// Map to DTOs and apply bulk data
	tokenDTOs := make([]dto.TokenResponse, len(results))
	for i, result := range results {
		tokenDTO := dto.MapTokenToDTO(result.Token, result.Metadata)

		// Apply bulk expansions
		for _, exp := range expand {
			switch exp {
			case types.ExpansionOwners:
				if owners, ok := bulkOwners[result.Token.ID]; ok {
					ownerDTOs := make([]dto.OwnerResponse, len(owners))
					for j := range owners {
						ownerDTOs[j] = *dto.MapOwnerToDTO(&owners[j])
					}
					total := bulkOwnersTotals[result.Token.ID] // Get actual total from DB
					tokenDTO.Owners = &dto.PaginatedOwners{
						Owners: ownerDTOs,
						Offset: nil, // No pagination in bulk queries
						Total:  total,
					}
				}
			case types.ExpansionProvenanceEvents:
				if events, ok := bulkProvenanceEvents[result.Token.ID]; ok {
					eventDTOs := make([]dto.ProvenanceEventResponse, len(events))
					for j := range events {
						eventDTOs[j] = *dto.MapProvenanceEventToDTO(&events[j])
					}
					total := bulkProvenanceTotals[result.Token.ID] // Get actual total from DB
					tokenDTO.ProvenanceEvents = &dto.PaginatedProvenanceEvents{
						Events: eventDTOs,
						Offset: nil, // No pagination in bulk queries
						Total:  total,
					}
				}
			case types.ExpansionEnrichmentSource:
				if enrichment, ok := bulkEnrichmentSources[result.Token.ID]; ok && enrichment != nil {
					tokenDTO.EnrichmentSource = dto.MapEnrichmentSourceToDTO(enrichment)
				}
			case types.ExpansionMetadataMediaAsset:
				if result.Metadata != nil {
					var mediaDTOs []dto.MediaAssetResponse
					if !internalTypes.StringNilOrEmpty(result.Metadata.ImageURL) {
						if asset, ok := mediaAssetsMap[*result.Metadata.ImageURL]; ok {
							mediaDTOs = append(mediaDTOs, *dto.MapMediaAssetToDTO(&asset))
						}
					}
					if !internalTypes.StringNilOrEmpty(result.Metadata.AnimationURL) {
						if asset, ok := mediaAssetsMap[*result.Metadata.AnimationURL]; ok {
							mediaDTOs = append(mediaDTOs, *dto.MapMediaAssetToDTO(&asset))
						}
					}
					tokenDTO.MetadataMediaAssets = mediaDTOs
				}
			case types.ExpansionEnrichmentSourceMediaAsset:
				if enrichment, ok := bulkEnrichmentSources[result.Token.ID]; ok && enrichment != nil {
					tokenDTO.EnrichmentSource = dto.MapEnrichmentSourceToDTO(enrichment)
					var mediaDTOs []dto.MediaAssetResponse
					if !internalTypes.StringNilOrEmpty(enrichment.ImageURL) {
						if asset, ok := mediaAssetsMap[*enrichment.ImageURL]; ok {
							mediaDTOs = append(mediaDTOs, *dto.MapMediaAssetToDTO(&asset))
						}
					}
					if !internalTypes.StringNilOrEmpty(enrichment.AnimationURL) {
						if asset, ok := mediaAssetsMap[*enrichment.AnimationURL]; ok {
							mediaDTOs = append(mediaDTOs, *dto.MapMediaAssetToDTO(&asset))
						}
					}
					tokenDTO.EnrichmentSourceMediaAssets = mediaDTOs
				}
			}
		}

		tokenDTOs[i] = *tokenDTO
	}

	// Build response with pagination
	var nextOffset *uint64
	if *offset+uint64(len(results)) < total { //nolint:gosec,G115
		offsetVal := *offset + uint64(len(results))
		nextOffset = &offsetVal
	}

	return &dto.TokenListResponse{
		Tokens: tokenDTOs,
		Offset: nextOffset,
		Total:  total,
	}, nil
}

func (e *executor) GetChanges(ctx context.Context, tokenIDs []uint64, tokenCIDs []string, addresses []string, subjectTypes []schema.SubjectType, subjectIDs []string, anchor *uint64, since *time.Time, limit *uint8, offset *uint64, order *types.Order, expand []types.Expansion) (*dto.ChangeListResponse, error) {
	// Use defaults if not provided
	if limit == nil {
		defaultLimit := constants.DEFAULT_CHANGES_LIMIT
		limit = &defaultLimit
	}
	if offset == nil {
		defaultOffset := constants.DEFAULT_OFFSET
		offset = &defaultOffset
	}

	// Determine ordering: 'order' only applies when using deprecated 'since' parameter
	// When using 'anchor' or neither, always ascending by ID
	orderDesc := anchor == nil && since != nil && order != nil && order.Desc()

	// Determine offset: only applies when using deprecated 'since' parameter
	// When using 'anchor' for cursor-based pagination, offset is not used (cursor continues from anchor ID)
	var offsetValue uint64
	if anchor == nil && since != nil {
		offsetValue = *offset
	}

	// Normalize token CIDs
	if len(tokenCIDs) > 0 {
		normalizedTokenCIDs := make([]string, len(tokenCIDs))
		for i, tokenCID := range tokenCIDs {
			normalizedTokenCIDs[i] = domain.TokenCID(tokenCID).Normalized().String()
		}
		tokenCIDs = normalizedTokenCIDs
	}

	// Normalize addresses
	if len(addresses) > 0 {
		normalizedAddresses := domain.NormalizeAddresses(addresses)
		addresses = normalizedAddresses
	}

	// Build filter
	filter := store.ChangesQueryFilter{
		TokenIDs:     tokenIDs,
		TokenCIDs:    tokenCIDs,
		Addresses:    addresses,
		SubjectTypes: subjectTypes,
		SubjectIDs:   subjectIDs,
		Anchor:       anchor,
		Since:        since, // Deprecated: kept for backward compatibility
		Limit:        int(*limit),
		Offset:       offsetValue, // Deprecated: only applies when using 'since' parameter
		OrderDesc:    orderDesc,   // Deprecated: only applies when using 'since' parameter
	}

	// Get changes
	results, total, err := e.store.GetChanges(ctx, filter)
	if err != nil {
		return nil, apierrors.NewDatabaseError(fmt.Sprintf("Failed to get changes: %v", err))
	}

	// Map to DTOs
	changeDTOs := make([]dto.ChangeResponse, len(results))
	for i, change := range results {
		changeDTO := dto.MapChangeToDTO(change)

		// Handle subject expansion
		for _, exp := range expand {
			if exp == types.ExpansionSubject {
				subject, err := e.expandSubject(ctx, change)
				if err != nil {
					return nil, err
				}
				changeDTO.Subject = subject
			}
		}

		changeDTOs[i] = *changeDTO
	}

	// Build response with pagination
	var nextOffset *uint64
	var nextAnchor *uint64

	// Calculate next offset only when using deprecated 'since' parameter (offset-based pagination)
	// For cursor-based pagination with 'anchor', use next_anchor instead
	if anchor == nil && since != nil && offsetValue+uint64(len(results)) < total { //nolint:gosec,G115
		offsetVal := offsetValue + uint64(len(results))
		nextOffset = &offsetVal
	}

	// Set next anchor for cursor-based pagination
	// Always provide next_anchor when results are present for clients to continue from this point
	if (anchor != nil || since == nil) && len(results) > 0 {
		maxID := results[0].ID
		for _, change := range results {
			if change.ID > maxID {
				maxID = change.ID
			}
		}
		nextAnchor = &maxID
	}

	return &dto.ChangeListResponse{
		Changes:    changeDTOs,
		Offset:     nextOffset,
		NextAnchor: nextAnchor,
		Total:      total,
	}, nil
}

func (e *executor) TriggerTokenIndexing(ctx context.Context, tokenCIDs []domain.TokenCID, addresses []string) (*dto.TriggerIndexingResponse, error) {
	w := workflows.NewWorkerCore(nil, workflows.WorkerCoreConfig{}, nil)
	var workflowID string
	var runID string

	hasTokenCIDs := len(tokenCIDs) > 0
	hasAddresses := len(addresses) > 0

	if hasTokenCIDs {
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

		// Trigger IndexTokens workflow
		options := client.StartWorkflowOptions{
			TaskQueue:                e.orchestratorTaskQueue,
			WorkflowExecutionTimeout: 30 * time.Minute,
		}
		wfRun, err := e.orchestrator.ExecuteWorkflow(ctx, options, w.IndexTokens, normalizedTokenCIDs, nil)
		if err != nil {
			return nil, apierrors.NewServiceError(fmt.Sprintf("Failed to trigger indexing: %v", err))
		}
		workflowID = wfRun.GetID()
		runID = wfRun.GetRunID()
	}

	if hasAddresses {
		// Normalize addresses
		normalizedAddresses := domain.NormalizeAddresses(addresses)

		// Trigger IndexTokenOwners workflow
		options := client.StartWorkflowOptions{
			TaskQueue:                e.orchestratorTaskQueue,
			WorkflowExecutionTimeout: 24*time.Hour + 15*time.Minute, // 24 hours + 15 minutes to cover the child workflow timeout
		}
		wfRun, err := e.orchestrator.ExecuteWorkflow(ctx, options, w.IndexTokenOwners, normalizedAddresses)
		if err != nil {
			return nil, apierrors.NewServiceError(fmt.Sprintf("Failed to trigger indexing: %v", err))
		}
		workflowID = wfRun.GetID()
		runID = wfRun.GetRunID()
	}

	return &dto.TriggerIndexingResponse{
		WorkflowID: workflowID,
		RunID:      runID,
	}, nil
}

func (e *executor) TriggerMetadataIndexing(ctx context.Context, tokenIDs []uint64, tokenCIDs []domain.TokenCID) (*dto.TriggerIndexingResponse, error) {
	w := workflows.NewWorkerCore(nil, workflows.WorkerCoreConfig{}, nil)
	var workflowID string
	var runID string

	if len(tokenIDs) == 0 && len(tokenCIDs) == 0 {
		return nil, apierrors.NewValidationError("at least one of token_ids or token_cids is required")
	}

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

	// Trigger batch metadata indexing workflow
	// The workflow will handle child workflows for individual tokens
	options := client.StartWorkflowOptions{
		TaskQueue:                e.orchestratorTaskQueue,
		WorkflowExecutionTimeout: 30 * time.Minute,
	}
	wfRun, err := e.orchestrator.ExecuteWorkflow(ctx, options, w.IndexMultipleTokensMetadata, uniqueTokenCIDs)
	if err != nil {
		return nil, apierrors.NewServiceError(fmt.Sprintf("Failed to trigger metadata indexing: %v", err))
	}
	workflowID = wfRun.GetID()
	runID = wfRun.GetRunID()

	return &dto.TriggerIndexingResponse{
		WorkflowID: workflowID,
		RunID:      runID,
	}, nil
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

// expandSubject expands the subject of a change journal entry
func (e *executor) expandSubject(ctx context.Context, change *schema.ChangesJournal) (interface{}, error) {
	switch change.SubjectType {
	case schema.SubjectTypeToken, schema.SubjectTypeOwner, schema.SubjectTypeBalance:
		// For token and owner changes, the subject is a provenance event
		provenanceEventID, err := strconv.ParseUint(change.SubjectID, 10, 64)
		if err != nil {
			return nil, apierrors.NewInternalError(fmt.Sprintf("Invalid subject_id: %v", err))
		}
		event, err := e.store.GetProvenanceEventByID(ctx, provenanceEventID)
		if err != nil {
			return nil, apierrors.NewDatabaseError(fmt.Sprintf("Failed to get provenance event: %v", err))
		}
		if event == nil {
			return nil, nil
		}
		return dto.MapProvenanceEventToDTO(event), nil

	case schema.SubjectTypeMetadata, schema.SubjectTypeEnrichSource:
		// For metadata and enrichment source changes,
		// the subject ID is token_id for metadata and enrichment_source
		tokenID, err := strconv.ParseUint(change.SubjectID, 10, 64)
		if err != nil {
			return nil, apierrors.NewInternalError(fmt.Sprintf("Invalid subject_id for metadata: %v", err))
		}

		token, err := e.store.GetTokenByID(ctx, tokenID)
		if err != nil {
			return nil, apierrors.NewDatabaseError(fmt.Sprintf("Failed to get token: %v", err))
		}
		if token == nil {
			return nil, nil
		}

		return dto.MapTokenToDTO(token, nil), nil

	case schema.SubjectTypeMediaAsset:
		// For media asset changes, the subject ID is media_asset_id
		mediaAssetID, err := strconv.ParseInt(change.SubjectID, 10, 64)
		if err != nil {
			return nil, apierrors.NewInternalError(fmt.Sprintf("Invalid subject_id for media asset: %v", err))
		}

		mediaAsset, err := e.store.GetMediaAssetByID(ctx, mediaAssetID)
		if err != nil {
			return nil, apierrors.NewDatabaseError(fmt.Sprintf("Failed to get media asset: %v", err))
		}
		if mediaAsset == nil {
			return nil, nil
		}

		return dto.MapMediaAssetToDTO(mediaAsset), nil

	default:
		return nil, nil
	}
}

// expandMetadataMediaAssets expands the media assets of a token's metadata
func (e *executor) expandMetadataMediaAssets(ctx context.Context, tokenDTO *dto.TokenResponse) error {
	// If metadata is not available, return nil
	if tokenDTO.Metadata == nil {
		return nil
	}

	// Collect source URLs from metadata
	var sourceURLs []string
	if !internalTypes.StringNilOrEmpty(tokenDTO.Metadata.ImageURL) {
		sourceURLs = append(sourceURLs, *tokenDTO.Metadata.ImageURL)
	}
	if !internalTypes.StringNilOrEmpty(tokenDTO.Metadata.AnimationURL) {
		sourceURLs = append(sourceURLs, *tokenDTO.Metadata.AnimationURL)
	}

	if len(sourceURLs) == 0 {
		return nil
	}

	// Query media assets
	mediaAssets, err := e.store.GetMediaAssetsBySourceURLs(ctx, sourceURLs)
	if err != nil {
		return apierrors.NewDatabaseError(fmt.Sprintf("Failed to get media assets: %v", err))
	}

	// Map to DTOs
	mediaDTOs := make([]dto.MediaAssetResponse, len(mediaAssets))
	for i := range mediaAssets {
		mediaDTOs[i] = *dto.MapMediaAssetToDTO(&mediaAssets[i])
	}

	tokenDTO.MetadataMediaAssets = mediaDTOs
	return nil
}

// expandEnrichmentSourceMediaAssets expands the media assets of a token's enrichment source
func (e *executor) expandEnrichmentSourceMediaAssets(ctx context.Context, tokenDTO *dto.TokenResponse) error {
	if tokenDTO.EnrichmentSource == nil {
		// No enrichment source available
		// Query enrichment source by token ID
		enrichment, err := e.store.GetEnrichmentSourceByTokenID(ctx, tokenDTO.ID)
		if err != nil {
			return apierrors.NewDatabaseError(fmt.Sprintf("Failed to get enrichment source: %v", err))
		}
		if enrichment == nil {
			return nil
		}

		tokenDTO.EnrichmentSource = dto.MapEnrichmentSourceToDTO(enrichment)
	}

	// Collect source URLs from enrichment source
	var sourceURLs []string
	if !internalTypes.StringNilOrEmpty(tokenDTO.EnrichmentSource.ImageURL) {
		sourceURLs = append(sourceURLs, *tokenDTO.EnrichmentSource.ImageURL)
	}
	if !internalTypes.StringNilOrEmpty(tokenDTO.EnrichmentSource.AnimationURL) {
		sourceURLs = append(sourceURLs, *tokenDTO.EnrichmentSource.AnimationURL)
	}

	logger.Debug("sourceURLs", zap.Any("sourceURLs", sourceURLs))

	if len(sourceURLs) == 0 {
		return nil
	}

	// Query media assets
	mediaAssets, err := e.store.GetMediaAssetsBySourceURLs(ctx, sourceURLs)
	if err != nil {
		return apierrors.NewDatabaseError(fmt.Sprintf("Failed to get media assets: %v", err))
	}

	// Map to DTOs
	mediaDTOs := make([]dto.MediaAssetResponse, len(mediaAssets))
	for i := range mediaAssets {
		mediaDTOs[i] = *dto.MapMediaAssetToDTO(&mediaAssets[i])
	}

	tokenDTO.EnrichmentSourceMediaAssets = mediaDTOs
	return nil
}

// GetWorkflowStatus retrieves the status of a workflow execution
func (e *executor) GetWorkflowStatus(ctx context.Context, workflowID, runID string) (*dto.WorkflowStatusResponse, error) {
	// Validate inputs
	if workflowID == "" {
		return nil, apierrors.NewValidationError("workflowID is required")
	}
	if runID == "" {
		return nil, apierrors.NewValidationError("runID is required")
	}

	// Get workflow execution details from Temporal
	describeResp, err := e.orchestrator.DescribeWorkflowExecution(ctx, workflowID, runID)
	if err != nil {
		return nil, apierrors.NewServiceError(fmt.Sprintf("Failed to describe workflow execution: %v", err))
	}

	workflowInfo := describeResp.GetWorkflowExecutionInfo()
	if workflowInfo == nil {
		return nil, apierrors.NewNotFoundError("Workflow execution not found")
	}

	// Map status to string
	status := workflowInfo.GetStatus().String()

	// Get start time
	var startTime *time.Time
	if workflowInfo.GetStartTime() != nil {
		t := workflowInfo.GetStartTime().AsTime()
		startTime = &t
	}

	// Get close time
	var closeTime *time.Time
	if workflowInfo.GetCloseTime() != nil {
		t := workflowInfo.GetCloseTime().AsTime()
		closeTime = &t
	}

	// Calculate execution time in milliseconds
	var executionTime *uint64
	if startTime != nil && closeTime != nil {
		duration := uint64(closeTime.Sub(*startTime).Milliseconds()) //nolint:gosec,G115 // time is always positive
		executionTime = &duration
	}

	return &dto.WorkflowStatusResponse{
		WorkflowID:    workflowID,
		RunID:         runID,
		Status:        status,
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

package executor

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"go.temporal.io/sdk/client"

	"github.com/feral-file/ff-indexer-v2/internal/api/shared/constants"
	"github.com/feral-file/ff-indexer-v2/internal/api/shared/dto"
	apierrors "github.com/feral-file/ff-indexer-v2/internal/api/shared/errors"
	"github.com/feral-file/ff-indexer-v2/internal/api/shared/types"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/providers/temporal"
	"github.com/feral-file/ff-indexer-v2/internal/registry"
	"github.com/feral-file/ff-indexer-v2/internal/store"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
	internalTypes "github.com/feral-file/ff-indexer-v2/internal/types"
	"github.com/feral-file/ff-indexer-v2/internal/workflows"
)

// Executor is the interface for the API executor
//
//go:generate mockgen -source=executor.go -destination=../../../mocks/mock_api_executor.go -package=mocks -mock_names=Executor=MockAPIExecutor
type Executor interface {
	// GetToken retrieves a single token by its CID with optional expansions
	GetToken(ctx context.Context, tokenCID string, expand []types.Expansion, ownersLimit *uint8, ownersOffset *uint64, provenanceEventsLimit *uint8, provenanceEventsOffset *uint64, provenanceEventsOrder *types.Order) (*dto.TokenResponse, error)

	// GetTokens retrieves tokens with optional filters and expansions
	GetTokens(ctx context.Context, owners []string, chains []domain.Chain, contractAddresses []string, tokenNumbers []string, tokenIDs []uint64, tokenCIDs []string, limit *uint8, offset *uint64, expand []types.Expansion, ownersLimit *uint8, ownersOffset *uint64, provenanceEventsLimit *uint8, provenanceEventsOffset *uint64, provenanceEventsOrder *types.Order) (*dto.TokenListResponse, error)

	// GetChanges retrieves changes with optional filters and expansions
	GetChanges(ctx context.Context, tokenIDs []uint64, tokenCIDs []string, addresses []string, subjectTypes []schema.SubjectType, subjectIDs []string, since *time.Time, limit *uint8, offset *uint64, order *types.Order, expand []types.Expansion) (*dto.ChangeListResponse, error)

	// TriggerTokenIndexing triggers indexing for one or more tokens and addresses
	TriggerTokenIndexing(ctx context.Context, tokenCIDs []domain.TokenCID, addresses []string) (*dto.TriggerIndexingResponse, error)

	// GetWorkflowStatus retrieves the status of a Temporal workflow execution
	GetWorkflowStatus(ctx context.Context, workflowID, runID string) (*dto.WorkflowStatusResponse, error)
}

type executor struct {
	store                 store.Store
	orchestrator          temporal.TemporalOrchestrator
	orchestratorTaskQueue string
	blacklist             registry.BlacklistRegistry
}

func NewExecutor(store store.Store, orchestrator temporal.TemporalOrchestrator, orchestratorTaskQueue string, blacklist registry.BlacklistRegistry) Executor {
	return &executor{
		store:                 store,
		orchestrator:          orchestrator,
		orchestratorTaskQueue: orchestratorTaskQueue,
		blacklist:             blacklist,
	}
}

func (e *executor) GetToken(ctx context.Context, tokenCID string, expand []types.Expansion, ownersLimit *uint8, ownersOffset *uint64, provenanceEventsLimit *uint8, provenanceEventsOffset *uint64, provenanceEventsOrder *types.Order) (*dto.TokenResponse, error) {
	// Get token with metadata
	result, err := e.store.GetTokenWithMetadataByTokenCID(ctx, tokenCID)
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

func (e *executor) GetTokens(ctx context.Context, owners []string, chains []domain.Chain, contractAddresses []string, tokenNumbers []string, tokenIDs []uint64, tokenCIDs []string, limit *uint8, offset *uint64, expand []types.Expansion, ownersLimit *uint8, ownersOffset *uint64, provenanceEventsLimit *uint8, provenanceEventsOffset *uint64, provenanceEventsOrder *types.Order) (*dto.TokenListResponse, error) {
	// Use defaults if not provided
	if limit == nil {
		defaultLimit := constants.DEFAULT_TOKENS_LIMIT
		limit = &defaultLimit
	}
	if offset == nil {
		defaultOffset := constants.DEFAULT_OFFSET
		offset = &defaultOffset
	}

	// Build filter
	filter := store.TokenQueryFilter{
		Owners:            owners,
		ContractAddresses: contractAddresses,
		TokenNumbers:      tokenNumbers,
		TokenIDs:          tokenIDs,
		Chains:            chains,
		TokenCIDs:         tokenCIDs,
		Limit:             int(*limit),
		Offset:            *offset,
	}

	// Get tokens
	results, total, err := e.store.GetTokensByFilter(ctx, filter)
	if err != nil {
		return nil, apierrors.NewDatabaseError(fmt.Sprintf("Failed to get tokens: %v", err))
	}

	// Map to DTOs
	tokenDTOs := make([]dto.TokenResponse, len(results))
	for i, result := range results {
		tokenDTO := dto.MapTokenToDTO(result.Token, result.Metadata)

		// Handle expansions for each token
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

func (e *executor) GetChanges(ctx context.Context, tokenIDs []uint64, tokenCIDs []string, addresses []string, subjectTypes []schema.SubjectType, subjectIDs []string, since *time.Time, limit *uint8, offset *uint64, order *types.Order, expand []types.Expansion) (*dto.ChangeListResponse, error) {
	// Use defaults if not provided
	if limit == nil {
		defaultLimit := constants.DEFAULT_CHANGES_LIMIT
		limit = &defaultLimit
	}
	if offset == nil {
		defaultOffset := constants.DEFAULT_OFFSET
		offset = &defaultOffset
	}
	orderDesc := order == nil || order.Desc() // Default to DESC

	// Build filter
	filter := store.ChangesQueryFilter{
		TokenIDs:     tokenIDs,
		TokenCIDs:    tokenCIDs,
		Addresses:    addresses,
		SubjectTypes: subjectTypes,
		SubjectIDs:   subjectIDs,
		Since:        since,
		Limit:        int(*limit),
		Offset:       *offset,
		OrderDesc:    orderDesc,
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
	if *offset+uint64(len(results)) < total { //nolint:gosec,G115
		offsetVal := *offset + uint64(len(results))
		nextOffset = &offsetVal
	}

	return &dto.ChangeListResponse{
		Changes: changeDTOs,
		Offset:  nextOffset,
		Total:   total,
	}, nil
}

func (e *executor) TriggerTokenIndexing(ctx context.Context, tokenCIDs []domain.TokenCID, addresses []string) (*dto.TriggerIndexingResponse, error) {
	w := workflows.NewWorkerCore(nil, workflows.WorkerCoreConfig{}, nil)
	var workflowID string
	var runID string

	hasTokenCIDs := len(tokenCIDs) > 0
	hasAddresses := len(addresses) > 0

	if hasTokenCIDs {
		// Check for blacklisted contracts
		if e.blacklist != nil {
			for _, tokenCID := range tokenCIDs {
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
		wfRun, err := e.orchestrator.ExecuteWorkflow(ctx, options, w.IndexTokens, tokenCIDs)
		if err != nil {
			return nil, apierrors.NewServiceError(fmt.Sprintf("Failed to trigger indexing: %v", err))
		}
		workflowID = wfRun.GetID()
		runID = wfRun.GetRunID()
	}

	if hasAddresses {
		// Trigger IndexTokenOwners workflow
		options := client.StartWorkflowOptions{
			TaskQueue:                e.orchestratorTaskQueue,
			WorkflowExecutionTimeout: time.Hour,
		}
		wfRun, err := e.orchestrator.ExecuteWorkflow(ctx, options, w.IndexTokenOwners, addresses)
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
	if internalTypes.StringNilOrEmpty(tokenDTO.EnrichmentSource.ImageURL) {
		sourceURLs = append(sourceURLs, *tokenDTO.EnrichmentSource.ImageURL)
	}
	if internalTypes.StringNilOrEmpty(tokenDTO.EnrichmentSource.AnimationURL) {
		sourceURLs = append(sourceURLs, *tokenDTO.EnrichmentSource.AnimationURL)
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

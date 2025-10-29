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
	GetToken(ctx context.Context, tokenCID string, expand []types.Expansion, ownersLimit *int, ownersOffset *uint64, provenanceEventsLimit *int, provenanceEventsOffset *uint64, provenanceEventsOrder *types.Order) (*dto.TokenResponse, error)

	// GetTokens retrieves tokens with optional filters and expansions
	GetTokens(ctx context.Context, owners []string, chains []domain.Chain, contractAddresses []string, tokenIDs []string, limit *int, offset *uint64, expand []types.Expansion, ownersLimit *int, ownersOffset *uint64, provenanceEventsLimit *int, provenanceEventsOffset *uint64, provenanceEventsOrder *types.Order) (*dto.TokenListResponse, error)

	// GetChanges retrieves changes with optional filters and expansions
	GetChanges(ctx context.Context, tokenCIDs []string, addresses []string, since *time.Time, limit *int, offset *uint64, order *types.Order, expand []types.Expansion) (*dto.ChangeListResponse, error)

	// TriggerTokenIndexing triggers indexing for one or more tokens and addresses
	TriggerTokenIndexing(ctx context.Context, tokenCIDs []string, addresses []string) (*dto.TriggerIndexingResponse, error)
}

type executor struct {
	store                 store.Store
	orchestrator          temporal.TemporalOrchestrator
	orchestratorTaskQueue string
}

func NewExecutor(store store.Store, orchestrator temporal.TemporalOrchestrator, orchestratorTaskQueue string) Executor {
	return &executor{store: store, orchestrator: orchestrator, orchestratorTaskQueue: orchestratorTaskQueue}
}

func (e *executor) GetToken(ctx context.Context, tokenCID string, expand []types.Expansion, ownersLimit *int, ownersOffset *uint64, provenanceEventsLimit *int, provenanceEventsOffset *uint64, provenanceEventsOrder *types.Order) (*dto.TokenResponse, error) {
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
		}
	}

	return tokenDTO, nil
}

func (e *executor) GetTokens(ctx context.Context, owners []string, chains []domain.Chain, contractAddresses []string, tokenIDs []string, limit *int, offset *uint64, expand []types.Expansion, ownersLimit *int, ownersOffset *uint64, provenanceEventsLimit *int, provenanceEventsOffset *uint64, provenanceEventsOrder *types.Order) (*dto.TokenListResponse, error) {
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
		TokenNumbers:      tokenIDs,
		Chains:            chains,
		Limit:             *limit,
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

func (e *executor) GetChanges(ctx context.Context, tokenCIDs []string, addresses []string, since *time.Time, limit *int, offset *uint64, order *types.Order, expand []types.Expansion) (*dto.ChangeListResponse, error) {
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
		TokenCIDs: tokenCIDs,
		Addresses: addresses,
		Since:     since,
		Limit:     *limit,
		Offset:    *offset,
		OrderDesc: orderDesc,
	}

	// Get changes
	results, total, err := e.store.GetChanges(ctx, filter)
	if err != nil {
		return nil, apierrors.NewDatabaseError(fmt.Sprintf("Failed to get changes: %v", err))
	}

	// Map to DTOs
	changeDTOs := make([]dto.ChangeResponse, len(results))
	for i, result := range results {
		changeDTO := dto.MapChangeToDTO(result.Change, result.Token)

		// Handle subject expansion
		for _, exp := range expand {
			if exp == types.ExpansionSubject {
				subject, err := e.expandSubject(ctx, result.Change, result.Token)
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

func (e *executor) TriggerTokenIndexing(ctx context.Context, tokenCIDs []string, addresses []string) (*dto.TriggerIndexingResponse, error) {
	w := workflows.NewWorkerCore(nil, workflows.WorkerCoreConfig{})
	var workflowID string
	var runID string

	hasTokenCIDs := len(tokenCIDs) > 0
	hasAddresses := len(addresses) > 0

	if hasTokenCIDs {
		// Convert string CIDs to domain.TokenCID
		domainTokenCIDs := make([]domain.TokenCID, len(tokenCIDs))
		for i, cid := range tokenCIDs {
			domainTokenCIDs[i] = domain.TokenCID(cid)
		}

		// Validate token CIDs
		for _, cid := range domainTokenCIDs {
			if !cid.Valid() {
				return nil, apierrors.NewValidationError(fmt.Sprintf("Invalid token CID: %s", cid.String()))
			}
		}

		// Trigger IndexTokens workflow
		options := client.StartWorkflowOptions{
			TaskQueue:                e.orchestratorTaskQueue,
			WorkflowExecutionTimeout: 30 * time.Minute,
		}
		wfRun, err := e.orchestrator.ExecuteWorkflow(ctx, options, w.IndexTokens, domainTokenCIDs)
		if err != nil {
			return nil, apierrors.NewServiceError(fmt.Sprintf("Failed to trigger indexing: %v", err))
		}
		workflowID = wfRun.GetID()
		runID = wfRun.GetRunID()
	}

	if hasAddresses {
		// Validate addresses
		for _, address := range addresses {
			if !internalTypes.IsTezosAddress(address) && !internalTypes.IsEthereumAddress(address) {
				return nil, apierrors.NewValidationError(fmt.Sprintf("Invalid address: %s. Must be a valid Tezos or Ethereum address", address))
			}
		}

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

func (e *executor) expandOwners(ctx context.Context, tokenDTO *dto.TokenResponse, tokenID uint64, limit *int, offset *uint64) error {
	if limit == nil {
		defaultLimit := constants.DEFAULT_OWNERS_LIMIT
		limit = &defaultLimit
	}
	if offset == nil {
		defaultOffset := constants.DEFAULT_OFFSET
		offset = &defaultOffset
	}

	owners, total, err := e.store.GetTokenOwners(ctx, tokenID, *limit, *offset)
	if err != nil {
		return apierrors.NewDatabaseError(fmt.Sprintf("Failed to get token owners: %v", err))
	}

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

func (e *executor) expandProvenanceEvents(ctx context.Context, tokenDTO *dto.TokenResponse, tokenID uint64, limit *int, offset *uint64, order *types.Order) error {
	if limit == nil {
		defaultLimit := constants.DEFAULT_PROVENANCE_EVENTS_LIMIT
		limit = &defaultLimit
	}
	if offset == nil {
		defaultOffset := constants.DEFAULT_OFFSET
		offset = &defaultOffset
	}
	orderDesc := order == nil || order.Desc() // Default to DESC

	events, total, err := e.store.GetTokenProvenanceEvents(ctx, tokenID, *limit, *offset, orderDesc)
	if err != nil {
		return apierrors.NewDatabaseError(fmt.Sprintf("Failed to get provenance events: %v", err))
	}

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

func (e *executor) expandEnrichmentSource(ctx context.Context, tokenDTO *dto.TokenResponse, tokenID uint64) error {
	enrichment, err := e.store.GetEnrichmentSourceByTokenID(ctx, tokenID)
	if err != nil {
		return apierrors.NewDatabaseError(fmt.Sprintf("Failed to get enrichment source: %v", err))
	}
	if enrichment != nil {
		tokenDTO.EnrichmentSource = dto.MapEnrichmentSourceToDTO(enrichment)
	}
	return nil
}

func (e *executor) expandSubject(ctx context.Context, change *schema.ChangesJournal, token *schema.Token) (interface{}, error) {
	switch change.SubjectType {
	case schema.SubjectTypeToken, schema.SubjectTypeOwner:
		// For token and owner changes, the subject is a provenance event
		id, err := strconv.ParseUint(change.SubjectID, 10, 64)
		if err != nil {
			return nil, apierrors.NewInternalError(fmt.Sprintf("Invalid subject_id: %v", err))
		}
		event, err := e.store.GetProvenanceEventByID(ctx, id)
		if err != nil {
			return nil, apierrors.NewDatabaseError(fmt.Sprintf("Failed to get provenance event: %v", err))
		}
		if event == nil {
			return nil, nil
		}
		return dto.MapProvenanceEventToDTO(event), nil

	case schema.SubjectTypeBalance:
		// For balance changes, the subject is a balance record
		id, err := strconv.ParseUint(change.SubjectID, 10, 64)
		if err != nil {
			return nil, apierrors.NewInternalError(fmt.Sprintf("Invalid subject_id: %v", err))
		}
		balance, err := e.store.GetBalanceByID(ctx, id)
		if err != nil {
			return nil, apierrors.NewDatabaseError(fmt.Sprintf("Failed to get balance: %v", err))
		}
		if balance == nil {
			return nil, nil
		}
		return dto.MapOwnerToDTO(balance), nil

	case schema.SubjectTypeMetadata:
		// For metadata changes, the subject is token metadata
		metadata, err := e.store.GetTokenMetadataByTokenCID(ctx, token.TokenCID)
		if err != nil {
			return nil, apierrors.NewDatabaseError(fmt.Sprintf("Failed to get token metadata: %v", err))
		}
		if metadata == nil {
			return nil, nil
		}
		return dto.MapTokenMetadataToDTO(metadata), nil

	case schema.SubjectTypeMedia:
		// For media changes, the subject is a media asset
		id, err := strconv.ParseInt(change.SubjectID, 10, 64)
		if err != nil {
			return nil, apierrors.NewInternalError(fmt.Sprintf("Invalid subject_id: %v", err))
		}
		media, err := e.store.GetMediaAssetByID(ctx, id)
		if err != nil {
			return nil, apierrors.NewDatabaseError(fmt.Sprintf("Failed to get media asset: %v", err))
		}
		if media == nil {
			return nil, nil
		}
		return dto.MapMediaAssetToDTO(media), nil

	default:
		return nil, nil
	}
}

package rest

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"go.temporal.io/sdk/client"
	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/api/rest/dto"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/providers/temporal"
	"github.com/feral-file/ff-indexer-v2/internal/store"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
	"github.com/feral-file/ff-indexer-v2/internal/types"
	"github.com/feral-file/ff-indexer-v2/internal/workflows"
)

const (
	MAX_TOKEN_CIDS_PER_REQUEST = 20
	MAX_ADDRESSES_PER_REQUEST  = 5
)

// Handler defines the interface for REST API handlers
// This interface allows for easy mocking and testing
//
//go:generate mockgen -source=handler.go -destination=../../mocks/mock_api_handler.go -package=mocks -mock_names=Handler=MockAPIHandler
type Handler interface {
	// GetToken retrieves a single token by its CID
	// GET /api/v1/tokens/:cid?expand=owners,provenance_events,enrichment_source&owners.limit=<limit>&owners.offset=<offset>&provenance_events.limit=<limit>&provenance_events.offset=<offset>&provenance_events.order=<order>
	GetToken(c *gin.Context)

	// ListTokens retrieves tokens with optional filters
	// GET /api/v1/tokens?owners=<address1>,<address2>&chain=<chain1>,<chain2>&contract_address=<contract_address1>,<contract_address2>&token_id=<id1>,<id2>&limit=<limit>&offset=<offset>&expand=owners,provenance_events,enrichment_source&owners.limit=<limit>&owners.offset=<offset>&provenance_events.limit=<limit>&provenance_events.offset=<offset>&provenance_events.order=<order>
	ListTokens(c *gin.Context)

	// GetChanges retrieves changes with optional filters
	// GET /api/v1/changes?token_cid=<cid>&address=<address>&since=<timestamp>&limit=<limit>&offset=<offset>&order=<order>&expand=<expand>
	GetChanges(c *gin.Context)

	// TriggerTokenIndexing triggers indexing for one or more tokens
	// POST /api/v1/tokens/index
	TriggerTokenIndexing(c *gin.Context)

	// HealthCheck returns the health status of the API
	// GET /health
	HealthCheck(c *gin.Context)
}

// handler implements the Handler interface
type handler struct {
	store                 store.Store
	orchestrator          temporal.TemporalOrchestrator
	orchestratorTaskQueue string
}

// NewHandler creates a new REST API handler
func NewHandler(store store.Store, orchestrator temporal.TemporalOrchestrator, orchestratorTaskQueue string) Handler {
	return &handler{
		store:                 store,
		orchestrator:          orchestrator,
		orchestratorTaskQueue: orchestratorTaskQueue,
	}
}

// GetToken retrieves a single token by its ID (token_cid)
func (h *handler) GetToken(c *gin.Context) {
	tokenCID := c.Param("cid")
	if tokenCID == "" {
		respondBadRequest(c, "Token CID is required")
		return
	}

	// Parse query parameters
	queryParams, err := ParseGetTokenQuery(c)
	if err != nil {
		respondValidationError(c, err.Error())
		return
	}

	// Get token with metadata
	result, err := h.store.GetTokenWithMetadataByTokenCID(c.Request.Context(), tokenCID)
	if err != nil {
		respondInternalError(c, err, "get_token", zap.String("token_cid", tokenCID))
		return
	}

	if result == nil {
		respondNotFound(c, "Token not found")
		return
	}

	// Map to DTO
	tokenDTO := dto.MapTokenToDTO(result.Token, result.Metadata)

	// Handle expansions
	expansions := queryParams.GetExpansions()

	if expansions.Owners {
		owners, total, err := h.store.GetTokenOwners(
			c.Request.Context(),
			result.Token.ID,
			queryParams.OwnerLimit,
			queryParams.OwnerOffset,
		)
		if err != nil {
			respondInternalError(c, err, "get_token_owners", zap.Uint64("token_id", result.Token.ID))
			return
		} else {
			ownerDTOs := make([]dto.OwnerResponse, len(owners))
			for i := range owners {
				ownerDTOs[i] = *dto.MapOwnerToDTO(&owners[i])
			}

			var nextOffset *int
			if uint64(queryParams.OwnerOffset+len(owners)) < total { //nolint:gosec,G115
				offset := queryParams.OwnerOffset + len(owners)
				nextOffset = &offset
			}

			tokenDTO.Owners = &dto.PaginatedOwners{
				Owners: ownerDTOs,
				Offset: nextOffset,
				Total:  total,
			}
		}
	}

	if expansions.ProvenanceEvents {
		events, total, err := h.store.GetTokenProvenanceEvents(
			c.Request.Context(),
			result.Token.ID,
			queryParams.ProvenanceEventLimit,
			queryParams.ProvenanceEventOffset,
			queryParams.ProvenanceEventOrder.Desc(),
		)
		if err != nil {
			respondInternalError(c, err, "get_provenance_events", zap.String("token_cid", tokenCID))
			return
		} else {
			eventDTOs := make([]dto.ProvenanceEventResponse, len(events))
			for i := range events {
				eventDTOs[i] = *dto.MapProvenanceEventToDTO(&events[i])
			}

			var nextOffset *int
			if uint64(queryParams.ProvenanceEventOffset+len(events)) < total { //nolint:gosec,G115
				offset := queryParams.ProvenanceEventOffset + len(events)
				nextOffset = &offset
			}

			tokenDTO.ProvenanceEvents = &dto.PaginatedProvenanceEvents{
				Events: eventDTOs,
				Offset: nextOffset,
				Total:  total,
			}
		}
	}

	if expansions.EnrichmentSource {
		enrichment, err := h.store.GetEnrichmentSourceByTokenID(c.Request.Context(), result.Token.ID)
		if err != nil {
			respondInternalError(c, err, "get_enrichment_source", zap.Uint64("token_id", result.Token.ID))
			return
		}
		if enrichment != nil {
			tokenDTO.EnrichmentSource = dto.MapEnrichmentSourceToDTO(enrichment)
		}
	}

	c.JSON(http.StatusOK, tokenDTO)
}

// ListTokens retrieves tokens with optional filters
func (h *handler) ListTokens(c *gin.Context) {
	// Parse query parameters
	queryParams, err := ParseListTokensQuery(c)
	if err != nil {
		respondValidationError(c, err.Error())
		return
	}

	// Build filter
	filter := store.TokenQueryFilter{
		Owners:            queryParams.Owners,
		ContractAddresses: queryParams.ContractAddresses,
		TokenNumbers:      queryParams.TokenIDs,
		Limit:             queryParams.Limit,
		Offset:            queryParams.Offset,
	}

	// Convert chain strings to domain.Chain
	if len(queryParams.Chains) > 0 {
		chains := make([]domain.Chain, len(queryParams.Chains))
		for i, chain := range queryParams.Chains {
			chains[i] = domain.Chain(chain)
		}
		filter.Chains = chains
	}

	// Get tokens
	results, total, err := h.store.GetTokensByFilter(c.Request.Context(), filter)
	if err != nil {
		respondInternalError(c, err, "list_tokens")
		return
	}

	// Get expansion parameters
	expansions := queryParams.GetExpansions()

	// Map to DTOs
	tokenDTOs := make([]dto.TokenResponse, len(results))
	for i, result := range results {
		tokenDTO := dto.MapTokenToDTO(result.Token, result.Metadata)

		// Handle expansions for each token
		if expansions.Owners {
			owners, ownerTotal, err := h.store.GetTokenOwners(
				c.Request.Context(),
				result.Token.ID,
				queryParams.OwnerLimit,
				queryParams.OwnerOffset,
			)
			if err != nil {
				respondInternalError(c, err, "get_token_owners", zap.Uint64("token_id", result.Token.ID))
				return
			} else {
				ownerDTOs := make([]dto.OwnerResponse, len(owners))
				for j := range owners {
					ownerDTOs[j] = *dto.MapOwnerToDTO(&owners[j])
				}

				var nextOffset *int
				if uint64(queryParams.OwnerOffset+len(owners)) < ownerTotal { //nolint:gosec,G115
					offset := queryParams.OwnerOffset + len(owners)
					nextOffset = &offset
				}

				tokenDTO.Owners = &dto.PaginatedOwners{
					Owners: ownerDTOs,
					Offset: nextOffset,
					Total:  ownerTotal,
				}
			}
		}

		if expansions.ProvenanceEvents {
			events, eventTotal, err := h.store.GetTokenProvenanceEvents(
				c.Request.Context(),
				result.Token.ID,
				queryParams.ProvenanceEventLimit,
				queryParams.ProvenanceEventOffset,
				queryParams.ProvenanceEventOrder.Desc(),
			)
			if err != nil {
				respondInternalError(c, err, "get_provenance_events", zap.Uint64("token_id", result.Token.ID))
				return
			} else {
				eventDTOs := make([]dto.ProvenanceEventResponse, len(events))
				for j := range events {
					eventDTOs[j] = *dto.MapProvenanceEventToDTO(&events[j])
				}

				var nextOffset *int
				if uint64(queryParams.ProvenanceEventOffset+len(events)) < eventTotal { //nolint:gosec,G115
					offset := queryParams.ProvenanceEventOffset + len(events)
					nextOffset = &offset
				}

				tokenDTO.ProvenanceEvents = &dto.PaginatedProvenanceEvents{
					Events: eventDTOs,
					Offset: nextOffset,
					Total:  eventTotal,
				}
			}
		}

		if expansions.EnrichmentSource {
			enrichment, err := h.store.GetEnrichmentSourceByTokenID(c.Request.Context(), result.Token.ID)
			if err != nil {
				respondInternalError(c, err, "get_enrichment_source", zap.Uint64("token_id", result.Token.ID))
				return
			}
			if enrichment != nil {
				tokenDTO.EnrichmentSource = dto.MapEnrichmentSourceToDTO(enrichment)
			}
		}

		tokenDTOs[i] = *tokenDTO
	}

	// Build response
	var nextOffset *int
	if uint64(queryParams.Offset+len(results)) < total { //nolint:gosec,G115
		offset := queryParams.Offset + len(results)
		nextOffset = &offset
	}

	response := dto.TokenListResponse{
		Tokens: tokenDTOs,
		Offset: nextOffset,
		Total:  total,
	}

	c.JSON(http.StatusOK, response)
}

// GetChanges retrieves changes with filtering and pagination
func (h *handler) GetChanges(c *gin.Context) {
	// Parse query parameters
	queryParams, err := ParseGetChangesQuery(c)
	if err != nil {
		respondValidationError(c, err.Error())
		return
	}

	// Build filter
	filter := store.ChangesQueryFilter{
		TokenCIDs: queryParams.TokenCIDs,
		Addresses: queryParams.Addresses,
		Since:     queryParams.Since,
		Limit:     queryParams.Limit,
		Offset:    queryParams.Offset,
		OrderDesc: queryParams.Order.Desc(),
	}

	// Get changes
	results, total, err := h.store.GetChanges(c.Request.Context(), filter)
	if err != nil {
		respondInternalError(c, err, "get_changes")
		return
	}

	// Map to DTOs
	changeDTOs := make([]dto.ChangeResponse, len(results))
	for i, result := range results {
		changeDTO := dto.MapChangeToDTO(result.Change, result.Token)

		// Handle subject expansion
		if queryParams.ShouldExpandSubject() {
			subject, err := h.expandSubject(c.Request.Context(), result.Change, result.Token)
			if err != nil {
				respondInternalError(c, err, "expand_subject",
					zap.String("subject_type", string(result.Change.SubjectType)),
					zap.String("subject_id", result.Change.SubjectID))
				return
			}
			changeDTO.Subject = subject
		}

		changeDTOs[i] = *changeDTO
	}

	// Build response with offset-based pagination
	var nextOffset *int
	if uint64(queryParams.Offset+len(results)) < total { //nolint:gosec,G115
		offset := queryParams.Offset + len(results)
		nextOffset = &offset
	}

	response := dto.ChangeListResponse{
		Changes: changeDTOs,
		Offset:  nextOffset,
		Total:   total,
	}

	c.JSON(http.StatusOK, response)
}

// expandSubject expands the subject based on subject_type
func (h *handler) expandSubject(ctx context.Context, change *schema.ChangesJournal, token *schema.Token) (interface{}, error) {
	switch change.SubjectType {
	case schema.SubjectTypeToken, schema.SubjectTypeOwner:
		// For token and owner changes, the subject is a provenance event
		id, err := strconv.ParseUint(change.SubjectID, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid subject_id: %w", err)
		}
		event, err := h.store.GetProvenanceEventByID(ctx, id)
		if err != nil {
			return nil, err
		}
		if event == nil {
			return nil, nil
		}
		return dto.MapProvenanceEventToDTO(event), nil

	case schema.SubjectTypeBalance:
		// For balance changes, the subject is a balance record
		id, err := strconv.ParseUint(change.SubjectID, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid subject_id: %w", err)
		}
		balance, err := h.store.GetBalanceByID(ctx, id)
		if err != nil {
			return nil, err
		}
		if balance == nil {
			return nil, nil
		}
		return dto.MapOwnerToDTO(balance), nil

	case schema.SubjectTypeMetadata:
		// For metadata changes, the subject is token metadata
		metadata, err := h.store.GetTokenMetadataByTokenCID(ctx, token.TokenCID)
		if err != nil {
			return nil, err
		}
		if metadata == nil {
			return nil, nil
		}
		return dto.MapTokenMetadataToDTO(metadata), nil

	case schema.SubjectTypeMedia:
		// For media changes, the subject is a media asset
		id, err := strconv.ParseInt(change.SubjectID, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid subject_id: %w", err)
		}
		media, err := h.store.GetMediaAssetByID(ctx, id)
		if err != nil {
			return nil, err
		}
		if media == nil {
			return nil, nil
		}
		return dto.MapMediaAssetToDTO(media), nil

	default:
		return nil, nil
	}
}

// TriggerTokenIndexing triggers indexing for one or more tokens
func (h *handler) TriggerTokenIndexing(c *gin.Context) {
	var req dto.TriggerIndexingRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		respondValidationError(c, fmt.Sprintf("Invalid request body: %v", err))
		return
	}

	// Validate: only one type of input should be provided
	hasTokenCIDs := len(req.TokenCIDs) > 0
	hasAddresses := len(req.Addresses) > 0

	if !hasTokenCIDs && !hasAddresses {
		respondValidationError(c, "Either token_cids or addresses must be provided")
		return
	}

	if hasTokenCIDs && hasAddresses {
		respondValidationError(c, "Cannot provide both token_cids and addresses")
		return
	}

	// Validate limits
	if hasTokenCIDs && len(req.TokenCIDs) > MAX_TOKEN_CIDS_PER_REQUEST {
		respondValidationError(c, fmt.Sprintf("Maximum %d token CIDs allowed", MAX_TOKEN_CIDS_PER_REQUEST))
		return
	}

	if hasAddresses && len(req.Addresses) > MAX_ADDRESSES_PER_REQUEST {
		respondValidationError(c, fmt.Sprintf("Maximum %d addresses allowed", MAX_ADDRESSES_PER_REQUEST))
		return
	}

	ctx := c.Request.Context()
	w := workflows.NewWorkerCore(nil, workflows.WorkerCoreConfig{})
	var workflowID string
	var runID string

	if hasTokenCIDs {
		// Convert string CIDs to domain.TokenCID
		tokenCIDs := make([]domain.TokenCID, len(req.TokenCIDs))
		for i, cid := range req.TokenCIDs {
			tokenCIDs[i] = domain.TokenCID(cid)
		}

		// Validate token CIDs
		for _, cid := range tokenCIDs {
			if !cid.Valid() {
				respondValidationError(c, fmt.Sprintf("Invalid token CID: %s", cid.String()))
				return
			}
		}

		// Trigger IndexTokens workflow
		options := client.StartWorkflowOptions{
			TaskQueue:                h.orchestratorTaskQueue,
			WorkflowExecutionTimeout: 30 * time.Minute,
		}
		wfRun, err := h.orchestrator.ExecuteWorkflow(ctx, options, w.IndexTokens, tokenCIDs)
		if err != nil {
			respondInternalError(c, err, "trigger_index_tokens")
			return
		}
		workflowID = wfRun.GetID()
		runID = wfRun.GetRunID()
	}

	if hasAddresses {
		// Validate addresses
		for _, address := range req.Addresses {
			if !types.IsTezosAddress(address) && !types.IsEthereumAddress(address) {
				respondValidationError(c, fmt.Sprintf("Invalid address: %s. Must be a valid Tezos or Ethereum address", address))
				return
			}
		}

		// Build workflow ID from addresses hash
		options := client.StartWorkflowOptions{
			TaskQueue:                h.orchestratorTaskQueue,
			WorkflowExecutionTimeout: time.Hour,
		}

		// Trigger IndexTokenOwners workflow
		wfRun, err := h.orchestrator.ExecuteWorkflow(ctx, options, w.IndexTokenOwners, req.Addresses)
		if err != nil {
			respondInternalError(c, err, "trigger_index_token_owners")
			return
		}
		workflowID = wfRun.GetID()
		runID = wfRun.GetRunID()
	}

	response := dto.TriggerIndexingResponse{
		WorkflowID: workflowID,
		RunID:      runID,
	}

	c.JSON(http.StatusAccepted, response)
}

// HealthCheck returns the health status of the API
func (h *handler) HealthCheck(c *gin.Context) {
	c.JSON(200, gin.H{
		"status":  "ok",
		"service": "ff-indexer-api",
	})
}

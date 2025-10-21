package rest

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/api/rest/dto"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/store"
)

// Handler defines the interface for REST API handlers
// This interface allows for easy mocking and testing
//
//go:generate mockgen -source=handler.go -destination=../../mocks/mock_api_handler.go -package=mocks -mock_names=Handler=MockAPIHandler
type Handler interface {
	// GetToken retrieves a single token by its ID
	// GET /api/v1/tokens/:id
	GetToken(c *gin.Context)

	// ListTokens retrieves tokens with optional filters
	// GET /api/v1/tokens?owners=<address1>,<address2>
	ListTokens(c *gin.Context)

	// GetChanges retrieves changes since a specific cursor
	// GET /api/v1/changes?since=<cursor>
	GetChanges(c *gin.Context)

	// TriggerIndexing triggers indexing for one or more tokens
	// POST /api/v1/tokens
	TriggerIndexing(c *gin.Context)

	// HealthCheck returns the health status of the API
	// GET /health
	HealthCheck(c *gin.Context)
}

// handler implements the Handler interface
type handler struct {
	store store.Store
}

// NewHandler creates a new REST API handler
func NewHandler(store store.Store) Handler {
	return &handler{
		store: store,
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
		orderDesc := queryParams.ProvenanceEventOrder == "desc"
		events, total, err := h.store.GetTokenProvenanceEvents(
			c.Request.Context(),
			result.Token.ID,
			queryParams.ProvenanceEventLimit,
			queryParams.ProvenanceEventOffset,
			orderDesc,
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
			orderDesc := queryParams.ProvenanceEventOrder == "desc"
			events, eventTotal, err := h.store.GetTokenProvenanceEvents(
				c.Request.Context(),
				result.Token.ID,
				queryParams.ProvenanceEventLimit,
				queryParams.ProvenanceEventOffset,
				orderDesc,
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

// GetChanges retrieves changes since a specific cursor
func (h *handler) GetChanges(c *gin.Context) {
	// TODO: Implement changes retrieval logic
	since := c.Query("since")
	c.JSON(200, gin.H{
		"message": "GetChanges endpoint - to be implemented",
		"since":   since,
	})
}

// TriggerIndexing triggers indexing for one or more tokens
func (h *handler) TriggerIndexing(c *gin.Context) {
	// TODO: Implement indexing trigger logic
	c.JSON(202, gin.H{
		"message": "TriggerIndexing endpoint - to be implemented",
		"status":  "accepted",
	})
}

// HealthCheck returns the health status of the API
func (h *handler) HealthCheck(c *gin.Context) {
	c.JSON(200, gin.H{
		"status":  "ok",
		"service": "ff-indexer-api",
	})
}

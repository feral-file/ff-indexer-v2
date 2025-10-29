package rest

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/feral-file/ff-indexer-v2/internal/api/shared/constants"
	"github.com/feral-file/ff-indexer-v2/internal/api/shared/dto"
	"github.com/feral-file/ff-indexer-v2/internal/api/shared/executor"
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
	executor executor.Executor
}

// NewHandler creates a new REST API handler using the shared executor
func NewHandler(exec executor.Executor) Handler {
	return &handler{
		executor: exec,
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

	// Convert query parameters to executor parameters
	expansions := queryParams.Expand
	ownersLimit := &queryParams.OwnerLimit
	ownersOffset := &queryParams.OwnerOffset
	provenanceEventsLimit := &queryParams.ProvenanceEventLimit
	provenanceEventsOffset := &queryParams.ProvenanceEventOffset
	provenanceEventsOrder := &queryParams.ProvenanceEventOrder

	// Call executor's GetToken method
	tokenDTO, err := h.executor.GetToken(
		c.Request.Context(),
		tokenCID,
		expansions,
		ownersLimit,
		ownersOffset,
		provenanceEventsLimit,
		provenanceEventsOffset,
		provenanceEventsOrder,
	)

	if err != nil {
		// Handle errors from executor
		respondInternalError(c, err, "Failed to get token")
		return
	}

	if tokenDTO == nil {
		respondNotFound(c, "Token not found")
		return
	}

	// Return successful response
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

	// Convert query parameters to executor parameters
	limit := &queryParams.Limit
	offset := &queryParams.Offset
	expansions := queryParams.Expand
	ownersLimit := &queryParams.OwnerLimit
	ownersOffset := &queryParams.OwnerOffset
	provenanceEventsLimit := &queryParams.ProvenanceEventLimit
	provenanceEventsOffset := &queryParams.ProvenanceEventOffset
	provenanceEventsOrder := &queryParams.ProvenanceEventOrder

	// Call executor's GetTokens method
	response, err := h.executor.GetTokens(
		c.Request.Context(),
		queryParams.Owners,
		queryParams.Chains,
		queryParams.ContractAddresses,
		queryParams.TokenIDs,
		limit,
		offset,
		expansions,
		ownersLimit,
		ownersOffset,
		provenanceEventsLimit,
		provenanceEventsOffset,
		provenanceEventsOrder,
	)

	if err != nil {
		respondInternalError(c, err, "Failed to list tokens")
		return
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

	// Convert query parameters to executor parameters
	limit := &queryParams.Limit
	offset := &queryParams.Offset
	order := &queryParams.Order
	expansions := queryParams.Expand

	// Call executor's GetChanges method
	response, err := h.executor.GetChanges(
		c.Request.Context(),
		queryParams.TokenCIDs,
		queryParams.Addresses,
		queryParams.Since,
		limit,
		offset,
		order,
		expansions,
	)

	if err != nil {
		respondInternalError(c, err, "Failed to get changes")
		return
	}

	c.JSON(http.StatusOK, response)
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
	if hasTokenCIDs && len(req.TokenCIDs) > constants.MAX_TOKEN_CIDS_PER_REQUEST {
		respondValidationError(c, fmt.Sprintf("Maximum %d token CIDs allowed", constants.MAX_TOKEN_CIDS_PER_REQUEST))
		return
	}

	if hasAddresses && len(req.Addresses) > constants.MAX_ADDRESSES_PER_REQUEST {
		respondValidationError(c, fmt.Sprintf("Maximum %d addresses allowed", constants.MAX_ADDRESSES_PER_REQUEST))
		return
	}

	// Call executor's TriggerTokenIndexing method
	response, err := h.executor.TriggerTokenIndexing(
		c.Request.Context(),
		req.TokenCIDs,
		req.Addresses,
	)

	if err != nil {
		respondInternalError(c, err, "Failed to trigger indexing")
		return
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

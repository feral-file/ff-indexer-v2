package rest

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/feral-file/ff-indexer-v2/internal/api/shared/constants"
	"github.com/feral-file/ff-indexer-v2/internal/api/shared/dto"
	"github.com/feral-file/ff-indexer-v2/internal/api/shared/executor"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
)

// Handler defines the interface for REST API handlers
// This interface allows for easy mocking and testing
//
//go:generate mockgen -source=handler.go -destination=../../mocks/api_handler.go -package=mocks -mock_names=Handler=MockAPIHandler
type Handler interface {
	// GetToken retrieves a single token by its CID
	// GET /api/v1/tokens/:cid?expand=owners,provenance_events,enrichment_source&owners.limit=<limit>&owners.offset=<offset>&provenance_events.limit=<limit>&provenance_events.offset=<offset>&provenance_events.order=<order>
	GetToken(c *gin.Context)

	// ListTokens retrieves tokens with optional filters
	// GET /api/v1/tokens?owner=<address1>,<address2>&chain=<chain1>,<chain2>&contract_address=<contract_address1>,<contract_address2>&token_number=<number1>,<number2>&token_id=<id1>,<id2>&token_cid=<cid1>,<cid2>&limit=<limit>&offset=<offset>&expand=owners,provenance_events,enrichment_source&owners.limit=<limit>&owners.offset=<offset>&provenance_events.limit=<limit>&provenance_events.offset=<offset>&provenance_events.order=<order>
	ListTokens(c *gin.Context)

	// GetChanges retrieves changes with optional filters
	// GET /api/v1/changes?token_cid=<cid>&address=<address>&anchor=<id>&limit=<limit>&offset=<offset>&order=<order>&expand=<expand>
	// Returns changes in ascending order by ID (sequential audit log)
	// Note: 'order' parameter only applies when using deprecated 'since' parameter
	// Deprecated parameter: since=<timestamp> (use anchor instead for reliable pagination)
	GetChanges(c *gin.Context)

	// TriggerTokenIndexing triggers indexing for tokens by CIDs (open, no authentication required)
	// POST /api/v1/tokens/index
	TriggerTokenIndexing(c *gin.Context)

	// TriggerOwnerIndexing triggers indexing for tokens by owner addresses (requires authentication)
	// POST /api/v1/tokens/owners/index
	TriggerOwnerIndexing(c *gin.Context)

	// TriggerMetadataIndexing triggers metadata refresh for tokens by IDs or CIDs (open, no authentication required)
	// POST /api/v1/tokens/metadata/index
	TriggerMetadataIndexing(c *gin.Context)

	// GetWorkflowStatus retrieves the status of a Temporal workflow execution
	// GET /api/v1/workflows/:workflow_id/runs/:run_id
	GetWorkflowStatus(c *gin.Context)

	// CreateWebhookClient creates a new webhook client (requires authentication via API key)
	// POST /api/v1/webhooks/clients
	CreateWebhookClient(c *gin.Context)

	// HealthCheck returns the health status of the API
	// GET /health
	HealthCheck(c *gin.Context)
}

// handler implements the Handler interface
type handler struct {
	debug    bool
	executor executor.Executor
}

// NewHandler creates a new REST API handler using the shared executor
func NewHandler(debug bool, exec executor.Executor) Handler {
	return &handler{
		debug:    debug,
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

	// Validate token CID
	if !domain.TokenCID(tokenCID).Valid() {
		respondBadRequest(c, "Invalid token CID")
		return
	}

	// Parse query parameters
	queryParams, err := ParseGetTokenQuery(c)
	if err != nil {
		respondValidationError(c, err.Error())
		return
	}

	// Validate query parameters
	err = queryParams.Validate()
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

	// Validate query parameters
	err = queryParams.Validate()
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
	includeBroken := &queryParams.IncludeBroken

	// Call executor's GetTokens method
	response, err := h.executor.GetTokens(
		c.Request.Context(),
		queryParams.Owners,
		queryParams.Chains,
		queryParams.ContractAddresses,
		queryParams.TokenNumbers,
		queryParams.TokenIDs,
		queryParams.TokenCIDs,
		limit,
		offset,
		includeBroken,
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

	// Validate query parameters
	err = queryParams.Validate()
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
		queryParams.TokenIDs,
		queryParams.TokenCIDs,
		queryParams.Addresses,
		queryParams.SubjectTypes,
		queryParams.SubjectIDs,
		queryParams.Anchor,
		queryParams.Since, // Deprecated but supported for backward compatibility
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

// TriggerTokenIndexing triggers indexing for tokens by CIDs (open, no authentication required)
func (h *handler) TriggerTokenIndexing(c *gin.Context) {
	var req dto.TriggerTokenIndexingRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		respondValidationError(c, fmt.Sprintf("Invalid request body: %v", err))
		return
	}

	// Validate request body
	err := req.Validate()
	if err != nil {
		respondValidationError(c, err.Error())
		return
	}

	// Call executor's TriggerTokenIndexing method with only token CIDs
	response, err := h.executor.TriggerTokenIndexing(
		c.Request.Context(),
		req.TokenCIDs,
		nil, // No addresses for this endpoint
	)

	if err != nil {
		respondInternalError(c, err, "Failed to trigger indexing")
		return
	}

	c.JSON(http.StatusAccepted, response)
}

// TriggerOwnerIndexing triggers indexing for tokens by owner addresses (requires authentication)
func (h *handler) TriggerOwnerIndexing(c *gin.Context) {
	var req dto.TriggerOwnerIndexingRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		respondValidationError(c, fmt.Sprintf("Invalid request body: %v", err))
		return
	}

	// Validate request body
	err := req.Validate()
	if err != nil {
		respondValidationError(c, err.Error())
		return
	}

	// Call executor's TriggerTokenIndexing method with only addresses
	response, err := h.executor.TriggerTokenIndexing(
		c.Request.Context(),
		nil, // No token CIDs for this endpoint
		req.Addresses,
	)

	if err != nil {
		respondInternalError(c, err, "Failed to trigger indexing")
		return
	}

	c.JSON(http.StatusAccepted, response)
}

// TriggerMetadataIndexing triggers metadata refresh for tokens by IDs or CIDs (open, no authentication required)
func (h *handler) TriggerMetadataIndexing(c *gin.Context) {
	var req dto.TriggerMetadataIndexingRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		respondValidationError(c, fmt.Sprintf("Invalid request body: %v", err))
		return
	}

	// Validate request body
	err := req.Validate()
	if err != nil {
		respondValidationError(c, err.Error())
		return
	}

	// Call executor's TriggerMetadataIndexing method
	response, err := h.executor.TriggerMetadataIndexing(
		c.Request.Context(),
		req.TokenIDs,
		req.TokenCIDs,
	)

	if err != nil {
		respondInternalError(c, err, "Failed to trigger metadata indexing")
		return
	}

	c.JSON(http.StatusAccepted, response)
}

// GetWorkflowStatus retrieves the status of a Temporal workflow execution
func (h *handler) GetWorkflowStatus(c *gin.Context) {
	workflowID := c.Param("workflow_id")
	if workflowID == "" {
		respondBadRequest(c, "workflow_id is required")
		return
	}

	runID := c.Param("run_id")
	if runID == "" {
		respondBadRequest(c, "run_id is required")
		return
	}

	// Call executor's GetWorkflowStatus method
	status, err := h.executor.GetWorkflowStatus(c.Request.Context(), workflowID, runID)
	if err != nil {
		respondInternalError(c, err, "Failed to get workflow status")
		return
	}

	c.JSON(http.StatusOK, status)
}

// CreateWebhookClient creates a new webhook client (requires authentication via API key)
func (h *handler) CreateWebhookClient(c *gin.Context) {
	var req dto.CreateWebhookClientRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		respondValidationError(c, fmt.Sprintf("Invalid request body: %v", err))
		return
	}

	// Validate request body
	err := req.Validate(h.debug)
	if err != nil {
		respondValidationError(c, err.Error())
		return
	}

	// Set default retry_max_attempts if not provided
	retryMaxAttempts := constants.DEFAULT_RETRY_MAX_ATTEMPTS // Default value
	if req.RetryMaxAttempts != nil {
		retryMaxAttempts = *req.RetryMaxAttempts
	}

	// Call executor's CreateWebhookClient method
	response, err := h.executor.CreateWebhookClient(
		c.Request.Context(),
		req.WebhookURL,
		req.EventFilters,
		retryMaxAttempts,
	)

	if err != nil {
		respondInternalError(c, err, "Failed to create webhook client")
		return
	}

	c.JSON(http.StatusCreated, response)
}

// HealthCheck returns the health status of the API
func (h *handler) HealthCheck(c *gin.Context) {
	c.JSON(200, gin.H{
		"status":  "ok",
		"service": "ff-indexer-api",
	})
}

package rest

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/feral-file/ff-indexer-v2/internal/api/shared/constants"
	"github.com/feral-file/ff-indexer-v2/internal/api/shared/dto"
	"github.com/feral-file/ff-indexer-v2/internal/api/shared/executor"
	"github.com/feral-file/ff-indexer-v2/internal/api/shared/types"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	internalTypes "github.com/feral-file/ff-indexer-v2/internal/types"
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
	// GET /api/v1/tokens?owner=<address1>,<address2>&chain=<chain1>,<chain2>&contract_address=<contract_address1>,<contract_address2>&token_number=<number1>,<number2>&token_id=<id1>,<id2>&token_cid=<cid1>,<cid2>&release_id=<id>&limit=<limit>&offset=<offset>&expand=owners,provenance_events,enrichment_source&owners.limit=<limit>&owners.offset=<offset>&provenance_events.limit=<limit>&provenance_events.offset=<offset>&provenance_events.order=<order>
	ListTokens(c *gin.Context)

	// ListReleases retrieves releases filtered by vendor and/or vendor_release_id
	// GET /api/v1/releases?vendor=<vendor>&vendor_release_id=<id>&limit=<limit>&offset=<offset>
	ListReleases(c *gin.Context)

	// GetRelease retrieves a release by internal id with mint-ordered member tokens
	// GET /api/v1/releases/:id?limit=<limit>&offset=<offset>&sort_order=<order>&expand=...
	GetRelease(c *gin.Context)

	// TriggerTokenIndexing triggers indexing for tokens by CIDs (open, no authentication required)
	// POST /api/v1/tokens/index
	TriggerTokenIndexing(c *gin.Context)

	// TriggerReleaseIndexing triggers asynchronous indexing for all tokens in a vendor release within a mint range (open, no auth required)
	// POST /api/v1/releases/index
	TriggerReleaseIndexing(c *gin.Context)

	// TriggerAddressIndexing triggers indexing for tokens by owner addresses with job tracking (requires authentication)
	// POST /api/v1/tokens/addresses/index
	TriggerAddressIndexing(c *gin.Context)

	// TriggerMetadataIndexing triggers metadata refresh for tokens by IDs or CIDs (open, no authentication required)
	// POST /api/v1/tokens/metadata/index
	TriggerMetadataIndexing(c *gin.Context)

	// GetJobStatus retrieves the status of a postgres job
	// GET /api/v1/jobs/:job_id
	GetJobStatus(c *gin.Context)

	// GetWorkflowRun retrieves job status by deprecated legacy URL paths. Path param workflow_id is the decimal string of jobs.id only.
	// GET /api/v1/workflows/:workflow_id
	// GET /api/v1/workflows/:workflow_id/runs/:run_id
	GetWorkflowRun(c *gin.Context)

	// CreateWebhookClient creates a new webhook client (requires authentication via API key)
	// POST /api/v1/webhooks/clients
	CreateWebhookClient(c *gin.Context)

	// GetAddressIndexingJob retrieves an address indexing job by postgres jobs.id (canonical path).
	// JSON responses include deprecated `workflow_id` from address_indexing_jobs when present; legacy opaque ids require GraphQL `indexingJob(workflow_id)`.
	GetAddressIndexingJob(c *gin.Context)

	// SyncCollection retrieves token changes for an address using timestamp-based watermark mechanism
	// GET /api/v1/collection/:address/sync?last_sync_time=<RFC3339 timestamp>&limit=<int>
	SyncCollection(c *gin.Context)

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

	limit := &queryParams.Limit
	offset := &queryParams.Offset
	expansions := queryParams.Expansions
	includeUnviewable := &queryParams.IncludeUnviewable
	sortBy := &queryParams.SortBy
	sortOrder := &queryParams.SortOrder

	// Build optional release_vendor_slug pointer (empty string means not provided).
	var releaseVendorSlug *string
	if slug := strings.TrimSpace(queryParams.ReleaseVendorSlug); slug != "" {
		releaseVendorSlug = &slug
	}

	response, err := h.executor.GetTokens(
		c.Request.Context(),
		queryParams.Owners,
		queryParams.Chains,
		queryParams.ContractAddresses,
		queryParams.TokenNumbers,
		queryParams.TokenIDs,
		queryParams.TokenCIDs,
		queryParams.ReleaseID,
		queryParams.ParsedReleaseVendor,
		releaseVendorSlug,
		queryParams.MintNumbers,
		limit,
		offset,
		includeUnviewable,
		sortBy,
		sortOrder,
		expansions,
	)

	if err != nil {
		respondInternalError(c, err, "Failed to list tokens")
		return
	}

	c.JSON(http.StatusOK, response)
}

// ListReleases retrieves releases filtered by vendor and/or vendor_release_id.
func (h *handler) ListReleases(c *gin.Context) {
	queryParams, err := ParseListReleasesQuery(c)
	if err != nil {
		respondValidationError(c, err.Error())
		return
	}

	if err := queryParams.Validate(); err != nil {
		respondValidationError(c, err.Error())
		return
	}

	limit := &queryParams.Limit
	offset := &queryParams.Offset

	response, err := h.executor.ListReleases(
		c.Request.Context(),
		queryParams.ParsedIDs,
		queryParams.ParsedVendor,
		queryParams.ParsedVendorReleaseID,
		queryParams.ParsedVendorReleaseSlug,
		limit,
		offset,
	)
	if err != nil {
		respondInternalError(c, err, "Failed to list releases")
		return
	}

	c.JSON(http.StatusOK, response)
}

// GetRelease retrieves a release by internal id with mint-ordered member tokens
func (h *handler) GetRelease(c *gin.Context) {
	releaseIDStr := c.Param("id")
	releaseID, err := strconv.ParseUint(releaseIDStr, 10, 64)
	if err != nil || releaseID == 0 {
		respondValidationError(c, "Invalid release id")
		return
	}

	queryParams, err := ParseGetReleaseQuery(c)
	if err != nil {
		respondValidationError(c, err.Error())
		return
	}
	if err := queryParams.Validate(); err != nil {
		respondValidationError(c, err.Error())
		return
	}

	limit := &queryParams.Limit
	offset := &queryParams.Offset
	sortOrder := &queryParams.SortOrder

	response, err := h.executor.GetRelease(c.Request.Context(), releaseID)
	if err != nil {
		respondInternalError(c, err, "Failed to get release")
		return
	}
	if response == nil {
		respondNotFound(c, "Release not found")
		return
	}

	// Release membership is a data relationship, not a viewability gate.
	// All members are returned regardless of is_viewable so the list is stable
	// across viewability state changes (tokens may be unviewable temporarily during
	// media processing). Callers that need only publicly visible tokens should use
	// GET /api/v1/tokens?release_id=... with include_unviewable omitted (default false).
	sortBy := types.TokenSortByMintNumber
	includeUnviewable := true
	members, err := h.executor.GetTokens(
		c.Request.Context(),
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		&releaseID,
		nil, // no release vendor filter for member listing
		nil, // no release vendor slug filter for member listing
		nil, // no mint_numbers filter for member listing
		limit,
		offset,
		&includeUnviewable,
		&sortBy,
		sortOrder,
		queryParams.Expansions,
	)
	if err != nil {
		respondInternalError(c, err, "Failed to get release members")
		return
	}
	response.Members = members

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

	// Call executor's TriggerTokenIndexingByCIDs method
	response, err := h.executor.TriggerTokenIndexing(
		c.Request.Context(),
		req.TokenCIDs,
	)

	if err != nil {
		respondInternalError(c, err, "Failed to trigger indexing")
		return
	}

	c.JSON(http.StatusAccepted, response)
}

// TriggerReleaseIndexing triggers asynchronous indexing for all tokens in a vendor release within a mint range.
// POST /api/v1/releases/index
func (h *handler) TriggerReleaseIndexing(c *gin.Context) {
	var req dto.TriggerReleaseIndexingRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		respondValidationError(c, fmt.Sprintf("Invalid request body: %v", err))
		return
	}

	if err := req.Validate(); err != nil {
		respondValidationError(c, err.Error())
		return
	}

	response, err := h.executor.TriggerReleaseIndexing(
		c.Request.Context(),
		req.Vendor,
		req.VendorReleaseID,
		req.VendorReleaseSlug,
		req.MintNumbers,
	)
	if err != nil {
		respondInternalError(c, err, "Failed to trigger release indexing")
		return
	}

	c.JSON(http.StatusAccepted, response)
}

// TriggerAddressIndexing triggers indexing for tokens by owner addresses with job tracking
// POST /api/v1/tokens/addresses/index
func (h *handler) TriggerAddressIndexing(c *gin.Context) {
	var req dto.TriggerAddressIndexingRequest
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

	response, err := h.executor.TriggerAddressIndexing(
		c.Request.Context(),
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

// GetJobStatus retrieves the status of a job
func (h *handler) GetJobStatus(c *gin.Context) {
	idStr := c.Param("job_id")
	if idStr == "" {
		respondBadRequest(c, "job_id is required")
		return
	}
	jobID, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil || jobID < 1 {
		respondBadRequest(c, "job_id must be a positive integer")
		return
	}

	status, err := h.executor.GetJobStatus(c.Request.Context(), jobID)
	if err != nil {
		respondInternalError(c, err, "Failed to get job status")
		return
	}

	c.JSON(http.StatusOK, status)
}

// GetWorkflowRun returns the same payload as GetJobStatus. Deprecated path segment workflow_id must be the decimal jobs.id.
// An optional /runs/:run_id suffix is accepted for legacy URL shape only; it is not used.
func (h *handler) GetWorkflowRun(c *gin.Context) {
	wf := c.Param("workflow_id") // deprecated param name; value is jobs.id as decimal string
	if wf == "" {
		respondBadRequest(c, "workflow_id is required")
		return
	}
	jobID, err := internalTypes.Int64FromUnsignedDecimalString(wf)
	if err != nil || jobID < 1 {
		respondBadRequest(c, "workflow_id must be a positive decimal integer (jobs.id)")
		return
	}

	status, err := h.executor.GetJobStatus(c.Request.Context(), jobID)
	if err != nil {
		respondInternalError(c, err, "Failed to get job status")
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

// GetAddressIndexingJob retrieves an address indexing job by postgres jobs.id; response echoes deprecated workflow_id from the row.
func (h *handler) GetAddressIndexingJob(c *gin.Context) {
	p := c.Param("job_id")
	if p == "" {
		respondBadRequest(c, "job_id is required")
		return
	}
	jobID, err := strconv.ParseInt(p, 10, 64)
	if err != nil || jobID < 1 {
		respondBadRequest(c, "job_id must be a positive integer")
		return
	}

	// Parse optional query parameters for including token counts
	opts := executor.GetAddressIndexingJobOptions{
		IncludeTotalIndexed:  c.Query("include_total_indexed") == "true",
		IncludeTotalViewable: c.Query("include_total_viewable") == "true",
	}

	job, err := h.executor.GetAddressIndexingJob(c.Request.Context(), jobID, opts)
	if err != nil {
		respondInternalError(c, err, "Failed to get indexing job")
		return
	}

	c.JSON(http.StatusOK, job)
}

// SyncCollection retrieves token changes for an address using checkpoint-based pagination
// GET /api/v1/collection/:address/sync?checkpoint_timestamp=<RFC3339>&checkpoint_event_id=<uint64>&limit=<int>
func (h *handler) SyncCollection(c *gin.Context) {
	address := c.Param("address")
	if address == "" {
		respondBadRequest(c, "address is required")
		return
	}

	// Validate address format
	if !internalTypes.IsTezosAddress(address) && !internalTypes.IsEthereumAddress(address) {
		respondBadRequest(c, "invalid address format")
		return
	}

	// Parse checkpoint parameters (both must be provided or both omitted)
	var checkpoint *dto.SyncCheckpoint
	checkpointTimestampStr := c.Query("checkpoint_timestamp")
	checkpointEventIDStr := c.Query("checkpoint_event_id")

	if checkpointTimestampStr != "" && checkpointEventIDStr != "" {
		// Parse timestamp
		checkpointTimestamp, err := time.Parse(time.RFC3339, checkpointTimestampStr)
		if err != nil {
			respondBadRequest(c, "invalid checkpoint_timestamp format, expected RFC3339")
			return
		}

		// Parse event ID
		var checkpointEventID uint64
		if _, err := fmt.Sscanf(checkpointEventIDStr, "%d", &checkpointEventID); err != nil {
			respondBadRequest(c, "invalid checkpoint_event_id format")
			return
		}

		checkpoint = &dto.SyncCheckpoint{
			Timestamp: checkpointTimestamp,
			EventID:   checkpointEventID,
		}
	} else if checkpointTimestampStr != "" || checkpointEventIDStr != "" {
		// Both or neither must be provided
		respondBadRequest(c, "checkpoint_timestamp and checkpoint_event_id must both be provided or both omitted")
		return
	}

	// Parse limit (optional, default from constants)
	limit := constants.DEFAULT_SYNC_COLLECTION_LIMIT
	if limitStr := c.Query("limit"); limitStr != "" {
		if _, err := fmt.Sscanf(limitStr, "%d", &limit); err != nil {
			respondBadRequest(c, "invalid limit format")
			return
		}
	}

	// Call executor's SyncCollection method
	response, err := h.executor.SyncCollection(
		c.Request.Context(),
		address,
		checkpoint,
		limit,
	)

	if err != nil {
		respondInternalError(c, err, "Failed to sync collection")
		return
	}

	c.JSON(http.StatusOK, response)
}

// HealthCheck returns the health status of the API
func (h *handler) HealthCheck(c *gin.Context) {
	c.JSON(200, gin.H{
		"status":  "ok",
		"service": "ff-indexer-api",
	})
}

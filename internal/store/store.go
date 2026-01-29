package store

import (
	"context"
	"time"

	"gorm.io/datatypes"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

// TokenSortBy enumeration for token sorting
type TokenSortBy string

const (
	TokenSortByCreatedAt        TokenSortBy = "created_at"
	TokenSortByLatestProvenance TokenSortBy = "latest_provenance"
)

// Valid checks if a token sort by is valid
func (t TokenSortBy) Valid() bool {
	return t == TokenSortByCreatedAt || t == TokenSortByLatestProvenance
}

// SortOrder enumeration for sorting
type SortOrder string

const (
	SortOrderAsc  SortOrder = "asc"
	SortOrderDesc SortOrder = "desc"
)

// Valid checks if a sort order is valid
func (s SortOrder) Valid() bool {
	return s == SortOrderAsc || s == SortOrderDesc
}

// CreateTokenInput represents the input for creating a token
type CreateTokenInput struct {
	TokenCID        string
	Chain           domain.Chain
	Standard        domain.ChainStandard
	ContractAddress string
	TokenNumber     string
	CurrentOwner    *string
	Burned          bool
}

// CreateBalanceInput represents the input for creating a balance record
type CreateBalanceInput struct {
	OwnerAddress string
	Quantity     string
}

// CreateProvenanceEventInput represents the input for creating a provenance event
type CreateProvenanceEventInput struct {
	Chain       domain.Chain
	EventType   schema.ProvenanceEventType
	FromAddress *string
	ToAddress   *string
	Quantity    string
	TxHash      string
	BlockNumber uint64
	BlockHash   *string
	Raw         []byte
	Timestamp   time.Time
}

// CreateTokenMintInput represents the complete input for creating a token mint with all related data
type CreateTokenMintInput struct {
	Token           CreateTokenInput
	Balance         CreateBalanceInput
	ProvenanceEvent CreateProvenanceEventInput
}

// CreateTokenMetadataInput represents the input for creating or updating token metadata
type CreateTokenMetadataInput struct {
	TokenID         uint64
	OriginJSON      []byte
	LatestJSON      []byte
	LatestHash      *string
	EnrichmentLevel schema.EnrichmentLevel
	LastRefreshedAt time.Time
	ImageURL        *string
	AnimationURL    *string
	Name            *string
	Artists         schema.Artists
	Description     *string
	Publisher       *schema.Publisher
	MimeType        *string
}

// CreateEnrichmentSourceInput represents the input for creating or updating an enrichment source
type CreateEnrichmentSourceInput struct {
	TokenID      uint64
	Vendor       schema.Vendor
	VendorJSON   []byte
	VendorHash   *string
	ImageURL     *string
	AnimationURL *string
	Name         *string
	Description  *string
	Artists      schema.Artists
	MimeType     *string
}

// UpdateBalanceInput represents the input for updating a balance record
type UpdateBalanceInput struct {
	OwnerAddress string
	Delta        string // Delta to add/subtract (cannot be negative)
}

// CreateTokenBurnInput represents the complete input for updating a token burn with all related data
type CreateTokenBurnInput struct {
	TokenCID            string
	SenderBalanceUpdate *UpdateBalanceInput // Update sender balance (decrease by quantity)
	ProvenanceEvent     CreateProvenanceEventInput
}

// CreateMetadataUpdateInput represents the complete input for creating a metadata update record
type CreateMetadataUpdateInput struct {
	TokenCID        string
	ProvenanceEvent CreateProvenanceEventInput
}

// TokenViewabilityInfo represents a token's viewability status
type TokenViewabilityInfo struct {
	TokenID    uint64 `gorm:"column:id"`
	TokenCID   string `gorm:"column:token_cid"`
	IsViewable bool   `gorm:"column:is_viewable"`
}

// TokenViewabilityChange represents a change in token viewability
type TokenViewabilityChange struct {
	TokenID     uint64 `gorm:"column:token_id"`
	TokenCID    string `gorm:"column:token_cid"`
	OldViewable bool   `gorm:"column:old_viewable"`
	NewViewable bool   `gorm:"column:new_viewable"`
}

// UpdateTokenTransferInput represents the input for updating a token transfer (assumes token exists)
type UpdateTokenTransferInput struct {
	TokenCID              string
	CurrentOwner          *string
	SenderBalanceUpdate   *UpdateBalanceInput // nil if sender is zero address (mint)
	ReceiverBalanceUpdate *UpdateBalanceInput // nil if receiver is zero address (burn)
	ProvenanceEvent       CreateProvenanceEventInput
}

// CreateTokenWithProvenancesInput represents the input for creating/upserting a token with all its provenance data
type CreateTokenWithProvenancesInput struct {
	Token    CreateTokenInput             // Token to create or update
	Balances []CreateBalanceInput         // Balances to upsert
	Events   []CreateProvenanceEventInput // Provenance events to insert
}

// UpsertTokenBalanceForOwnerInput represents the input for upserting a token balance for a specific owner
// This is used for owner-specific indexing where we only update one owner's balance without affecting others
type UpsertTokenBalanceForOwnerInput struct {
	Token        CreateTokenInput             // Token to upsert
	OwnerAddress string                       // Specific owner address
	Quantity     string                       // Owner's balance
	Events       []CreateProvenanceEventInput // Owner-related provenance events only
}

// TokenQueryFilter represents filters for token queries
type TokenQueryFilter struct {
	Owners            []string
	Chains            []domain.Chain
	ContractAddresses []string
	TokenNumbers      []string
	TokenIDs          []uint64
	TokenCIDs         []string
	IncludeUnviewable bool        // If false (default), only return tokens with is_viewable=true
	SortBy            TokenSortBy // Sort field: created_at or last_owner_provenance_timestamp
	SortOrder         SortOrder   // Sort order: asc or desc
	Limit             int
	Offset            uint64 // Offset for pagination
}

// ChangesQueryFilter represents filters for changes queries
type ChangesQueryFilter struct {
	TokenIDs     []uint64             // Filter by token IDs
	TokenCIDs    []string             // Filter by token CIDs
	Addresses    []string             // Filter by addresses (matches from/to addresses in provenance events)
	SubjectTypes []schema.SubjectType // Filter by subject types
	SubjectIDs   []string             // Filter by subject IDs
	Anchor       *uint64              // ID-based cursor - only show changes after this ID (exclusive) - recommended
	Since        *time.Time           // Deprecated: Timestamp filter - only show changes after this time (use Anchor instead)
	Limit        int                  // Number of results to return
	Offset       uint64               // Deprecated: Offset for pagination (only applies when using 'since' parameter, not used with 'anchor')
	OrderDesc    bool                 // Deprecated: Order descending (only applies when using 'since' parameter)
}

// TokensWithMetadataResult represents a token with its metadata
type TokensWithMetadataResult struct {
	Token    *schema.Token
	Metadata *schema.TokenMetadata
}

// CreateMediaAssetInput represents the input for creating a media asset record
type CreateMediaAssetInput struct {
	SourceURL        string
	MimeType         *string
	FileSizeBytes    *int64
	Provider         schema.StorageProvider
	ProviderAssetID  *string
	ProviderMetadata datatypes.JSON
	VariantURLs      datatypes.JSON
}

// CreateWebhookClientInput represents the input for creating a webhook client
type CreateWebhookClientInput struct {
	ClientID         string
	WebhookURL       string
	WebhookSecret    string
	EventFilters     datatypes.JSON // JSONB array of event types
	IsActive         bool
	RetryMaxAttempts int
}

// CreateAddressIndexingJobInput represents input for creating an address indexing job
type CreateAddressIndexingJobInput struct {
	Address       string
	Chain         domain.Chain
	Status        schema.IndexingJobStatus
	WorkflowID    string
	WorkflowRunID *string // Optional, may be nil initially
}

// QuotaInfo represents the current quota status for an address
type QuotaInfo struct {
	RemainingQuota     int       // Tokens remaining in current quota period
	TotalQuota         int       // Total quota per 24-hour period
	TokensIndexedToday int       // Tokens already indexed in current period
	QuotaResetAt       time.Time // When the current quota period ends
	QuotaExhausted     bool      // Whether quota is exhausted (remaining == 0)
}

// TokenCountsByAddress represents token count statistics for an address
type TokenCountsByAddress struct {
	TotalIndexed  int // Total tokens owned by the address (present in database)
	TotalViewable int // Tokens with metadata OR enrichment source (ready for display)
}

// Store defines the interface for database operations
//
//go:generate mockgen -source=store.go -destination=../mocks/store.go -package=mocks -mock_names=Store=MockStore
type Store interface {
	// =============================================================================
	// Token Management & Queries
	// =============================================================================

	// GetTokenByTokenCID retrieves a token by its canonical ID
	GetTokenByTokenCID(ctx context.Context, tokenCID string) (*schema.Token, error)
	// GetTokensByCIDs retrieves multiple tokens by their canonical IDs
	GetTokensByCIDs(ctx context.Context, tokenCIDs []string) ([]*schema.Token, error)
	// GetTokenByID retrieves a token by its internal ID
	GetTokenByID(ctx context.Context, tokenID uint64) (*schema.Token, error)
	// GetTokensByIDs retrieves multiple tokens by their internal IDs
	GetTokensByIDs(ctx context.Context, tokenIDs []uint64) ([]*schema.Token, error)
	// GetTokenWithMetadataByTokenCID retrieves a token with its metadata by canonical ID
	GetTokenWithMetadataByTokenCID(ctx context.Context, tokenCID string) (*TokensWithMetadataResult, error)
	// GetTokensByFilter retrieves tokens based on filters
	GetTokensByFilter(ctx context.Context, filter TokenQueryFilter) ([]schema.Token, error)
	// CreateTokenMint creates a new token with associated balance, change journal, and provenance event in a single transaction
	// For multi-edition tokens (FA2/ERC1155), this also handles subsequent mints via conflict resolution
	CreateTokenMint(ctx context.Context, input CreateTokenMintInput) error
	// UpdateTokenTransfer updates a token transfer (assumes token exists)
	UpdateTokenTransfer(ctx context.Context, input UpdateTokenTransferInput) error
	// UpdateTokenBurn updates a token as burned with associated balance update, change journal, and provenance event in a single transaction
	UpdateTokenBurn(ctx context.Context, input CreateTokenBurnInput) error
	// CreateTokenWithProvenances creates or updates a token with all its provenance data (balances and events)
	CreateTokenWithProvenances(ctx context.Context, input CreateTokenWithProvenancesInput) error
	// UpsertTokenBalanceForOwner upserts a token balance for a specific owner with owner-related provenance events
	// This is used for owner-specific indexing and does not delete other owners' balances
	UpsertTokenBalanceForOwner(ctx context.Context, input UpsertTokenBalanceForOwnerInput) error

	// =============================================================================
	// Token Metadata Operations
	// =============================================================================

	// GetTokenMetadataByTokenCID retrieves token metadata by token CID
	GetTokenMetadataByTokenCID(ctx context.Context, tokenCID string) (*schema.TokenMetadata, error)
	// GetTokenMetadataByTokenID retrieves token metadata by token ID
	GetTokenMetadataByTokenID(ctx context.Context, tokenID uint64) (*schema.TokenMetadata, error)
	// GetTokenMetadataByTokenIDs retrieves token metadata for multiple tokens
	// Returns a map of tokenID -> metadata
	GetTokenMetadataByTokenIDs(ctx context.Context, tokenIDs []uint64) (map[uint64]*schema.TokenMetadata, error)
	// UpsertTokenMetadata creates or updates token metadata
	UpsertTokenMetadata(ctx context.Context, input CreateTokenMetadataInput) error
	// CreateMetadataUpdate creates a provenance event for a metadata update
	CreateMetadataUpdate(ctx context.Context, input CreateMetadataUpdateInput) error
	// GetMediaAssetByID retrieves a media asset by ID
	GetMediaAssetByID(ctx context.Context, id int64) (*schema.MediaAsset, error)
	// GetMediaAssetBySourceURL retrieves a media asset by source URL and provider
	GetMediaAssetBySourceURL(ctx context.Context, sourceURL string, provider schema.StorageProvider) (*schema.MediaAsset, error)
	// GetMediaAssetsBySourceURLs retrieves media assets by multiple source URLs
	GetMediaAssetsBySourceURLs(ctx context.Context, sourceURLs []string) ([]schema.MediaAsset, error)
	// CreateMediaAsset creates a new media asset record
	CreateMediaAsset(ctx context.Context, input CreateMediaAssetInput) (*schema.MediaAsset, error)

	// =============================================================================
	// Enrichment Source Operations
	// =============================================================================

	// GetEnrichmentSourceByTokenID retrieves an enrichment source by token ID
	GetEnrichmentSourceByTokenID(ctx context.Context, tokenID uint64) (*schema.EnrichmentSource, error)
	// GetEnrichmentSourcesByTokenIDs retrieves enrichment sources for multiple tokens
	// Returns a map of tokenID -> enrichment source
	GetEnrichmentSourcesByTokenIDs(ctx context.Context, tokenIDs []uint64) (map[uint64]*schema.EnrichmentSource, error)
	// GetEnrichmentSourceByTokenCID retrieves an enrichment source by token CID
	GetEnrichmentSourceByTokenCID(ctx context.Context, tokenCID string) (*schema.EnrichmentSource, error)
	// UpsertEnrichmentSource creates or updates an enrichment source
	UpsertEnrichmentSource(ctx context.Context, input CreateEnrichmentSourceInput) error

	// =============================================================================
	// Token Media Health Operations
	// =============================================================================

	// GetURLsForChecking returns URLs that need health checking based on last check time
	GetURLsForChecking(ctx context.Context, recheckAfter time.Duration, limit int) ([]string, error)
	// GetTokenIDsByMediaURL returns all token IDs that use a specific URL
	GetTokenIDsByMediaURL(ctx context.Context, url string) ([]uint64, error)
	// GetTokensViewabilityByIDs returns viewability status for a specific set of token IDs
	GetTokensViewabilityByIDs(ctx context.Context, tokenIDs []uint64) ([]TokenViewabilityInfo, error)
	// UpdateTokenMediaHealthByURL updates health status for all records with a specific URL
	UpdateTokenMediaHealthByURL(ctx context.Context, url string, status schema.MediaHealthStatus, lastError *string) error
	// UpdateMediaURLAndPropagate updates a URL across token_media_health and source tables (metadata/enrichment) in a transaction
	UpdateMediaURLAndPropagate(ctx context.Context, oldURL string, newURL string) error

	// =============================================================================
	// Token Viewability Operations
	// =============================================================================

	// BatchUpdateTokensViewability computes and updates is_viewable for multiple tokens in one query
	// Returns a list of tokens whose viewability actually changed
	BatchUpdateTokensViewability(ctx context.Context, tokenIDs []uint64) ([]TokenViewabilityChange, error)

	// =============================================================================
	// Token Ownership & Balances
	// =============================================================================

	// GetTokenOwners retrieves owners (balances) for a token
	GetTokenOwners(ctx context.Context, tokenID uint64, limit int, offset uint64) ([]schema.Balance, uint64, error)
	// GetTokenOwnersBulk retrieves owners (balances) for multiple tokens
	// Returns a map of tokenID -> balances and a map of tokenID -> total count. Limit is applied per token.
	GetTokenOwnersBulk(ctx context.Context, tokenIDs []uint64, limit int) (map[uint64][]schema.Balance, map[uint64]uint64, error)
	// GetTokenCIDsByOwner retrieves all token CIDs owned by an address (where balance > 0)
	GetTokenCIDsByOwner(ctx context.Context, ownerAddress string) ([]domain.TokenCID, error)
	// GetTokenOwnerProvenancesBulk retrieves latest provenance per owner for multiple tokens
	// If ownerAddresses is provided, only returns provenances for those specific owners
	// Otherwise returns all owners, limited to maxPerToken per token (to prevent unbounded results for ERC1155)
	// Results are sorted by last_timestamp DESC, last_tx_index DESC
	// Returns a map of tokenID -> provenances and a map of tokenID -> total count. Limit is applied per token.
	GetTokenOwnerProvenancesBulk(ctx context.Context, tokenIDs []uint64, ownerAddresses []string, maxPerToken int) (map[uint64][]schema.TokenOwnershipProvenance, map[uint64]uint64, error)

	// =============================================================================
	// Provenance & Event Tracking
	// =============================================================================

	// GetTokenProvenanceEvents retrieves provenance events for a token
	GetTokenProvenanceEvents(ctx context.Context, tokenID uint64, limit int, offset uint64, orderDesc bool) ([]schema.ProvenanceEvent, uint64, error)
	// GetTokenProvenanceEventsBulk retrieves provenance events for multiple tokens
	// Returns a map of tokenID -> events and a map of tokenID -> total count. Limit is applied per token. Results are ordered by timestamp DESC (most recent first).
	GetTokenProvenanceEventsBulk(ctx context.Context, tokenIDs []uint64, limit int) (map[uint64][]schema.ProvenanceEvent, map[uint64]uint64, error)
	// GetProvenanceEventByID retrieves a provenance event by ID
	GetProvenanceEventByID(ctx context.Context, id uint64) (*schema.ProvenanceEvent, error)

	// =============================================================================
	// Changes & Audit Log
	// =============================================================================

	// GetChanges retrieves changes with optional filters and pagination
	GetChanges(ctx context.Context, filter ChangesQueryFilter) ([]*schema.ChangesJournal, uint64, error)

	// =============================================================================
	// System Configuration & Monitoring
	// =============================================================================

	// IsAnyAddressWatched checks if any of the given addresses are being watched on a specific chain
	IsAnyAddressWatched(ctx context.Context, chain domain.Chain, addresses []string) (bool, error)
	// GetBlockCursor retrieves the last processed block number for a chain
	GetBlockCursor(ctx context.Context, chain string) (uint64, error)
	// SetBlockCursor stores the last processed block number for a chain
	SetBlockCursor(ctx context.Context, chain string, blockNumber uint64) error
	// GetIndexingBlockRangeForAddress retrieves the indexing block range for an address and chain
	// Returns min_block=0, max_block=0 if no range exists for the chain
	GetIndexingBlockRangeForAddress(ctx context.Context, address string, chainID domain.Chain) (minBlock uint64, maxBlock uint64, err error)
	// UpdateIndexingBlockRangeForAddress updates the indexing block range for an address and chain
	UpdateIndexingBlockRangeForAddress(ctx context.Context, address string, chainID domain.Chain, minBlock uint64, maxBlock uint64) error
	// EnsureWatchedAddressExists creates a watched address record if it doesn't exist
	EnsureWatchedAddressExists(ctx context.Context, address string, chain domain.Chain, dailyQuota int) error

	// =============================================================================
	// Budgeted Indexing Mode Quota Operations
	// =============================================================================

	// GetQuotaInfo retrieves quota information for an address
	// Auto-resets quota if the 24-hour window has expired
	GetQuotaInfo(ctx context.Context, address string, chain domain.Chain) (*QuotaInfo, error)
	// IncrementTokensIndexed increments the token counter after successful indexing
	IncrementTokensIndexed(ctx context.Context, address string, chain domain.Chain, count int) error

	// =============================================================================
	// Key-Value Store Operations
	// =============================================================================

	// SetKeyValue sets a key-value pair in the key-value store
	SetKeyValue(ctx context.Context, key string, value string) error
	// GetKeyValue retrieves a value by key from the key-value store
	GetKeyValue(ctx context.Context, key string) (string, error)
	// GetAllKeyValuesByPrefix retrieves all key-value pairs with a specific prefix
	GetAllKeyValuesByPrefix(ctx context.Context, prefix string) (map[string]string, error)

	// =============================================================================
	// Webhook Operations
	// =============================================================================

	// GetActiveWebhookClientsByEventType retrieves active webhook clients that match the given event type
	// Supports wildcard matching: clients with ["*"] in their event_filters will match all event types
	GetActiveWebhookClientsByEventType(ctx context.Context, eventType string) ([]*schema.WebhookClient, error)
	// GetWebhookClientByID retrieves a webhook client by client ID
	GetWebhookClientByID(ctx context.Context, clientID string) (*schema.WebhookClient, error)
	// CreateWebhookClient creates a new webhook client
	CreateWebhookClient(ctx context.Context, input CreateWebhookClientInput) (*schema.WebhookClient, error)
	// CreateWebhookDelivery creates a new webhook delivery record
	CreateWebhookDelivery(ctx context.Context, delivery *schema.WebhookDelivery) error
	// UpdateWebhookDeliveryStatus updates the status and result of a webhook delivery
	UpdateWebhookDeliveryStatus(ctx context.Context, deliveryID uint64, status schema.WebhookDeliveryStatus, attempts int, responseStatus *int, responseBody, errorMessage string) error

	// =============================================================================
	// Address Indexing Job Operations
	// =============================================================================

	// CreateAddressIndexingJob creates a new address indexing job record
	// This handles conflicts gracefully by doing nothing if the job already exists
	CreateAddressIndexingJob(ctx context.Context, input CreateAddressIndexingJobInput) error
	// GetAddressIndexingJobByWorkflowID retrieves a job by workflow ID
	GetAddressIndexingJobByWorkflowID(ctx context.Context, workflowID string) (*schema.AddressIndexingJob, error)
	// GetActiveIndexingJobForAddress retrieves an active (running or paused) job for a specific address and chain
	// Returns nil if no active job is found (not an error)
	GetActiveIndexingJobForAddress(ctx context.Context, address string, chainID domain.Chain) (*schema.AddressIndexingJob, error)
	// UpdateAddressIndexingJobStatus updates job status with timestamp
	UpdateAddressIndexingJobStatus(ctx context.Context, workflowID string, status schema.IndexingJobStatus, timestamp time.Time) error
	// UpdateAddressIndexingJobProgress updates job progress metrics
	UpdateAddressIndexingJobProgress(ctx context.Context, workflowID string, tokensProcessed int, minBlock, maxBlock uint64) error
	// GetTokenCountsByAddress retrieves total token counts for an address
	// Returns both total tokens indexed and total tokens viewable (with metadata or enrichment)
	GetTokenCountsByAddress(ctx context.Context, address string, chain domain.Chain) (*TokenCountsByAddress, error)
}

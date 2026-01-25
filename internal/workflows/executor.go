package workflows

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/metadata"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum"
	"github.com/feral-file/ff-indexer-v2/internal/providers/tezos"
	"github.com/feral-file/ff-indexer-v2/internal/registry"
	"github.com/feral-file/ff-indexer-v2/internal/store"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
	"github.com/feral-file/ff-indexer-v2/internal/types"
	"github.com/feral-file/ff-indexer-v2/internal/uri"
	"github.com/feral-file/ff-indexer-v2/internal/webhook"
)

// Executor defines the interface for executing activities
//
//go:generate mockgen -source=executor.go -destination=../mocks/executor_core.go -package=mocks -mock_names=Executor=MockCoreExecutor
type Executor interface {
	// CheckTokenExists checks if a token exists in the database
	CheckTokenExists(ctx context.Context, tokenCID domain.TokenCID) (bool, error)

	// CreateTokenMint creates a new token and related provenance data for mint event
	CreateTokenMint(ctx context.Context, event *domain.BlockchainEvent) error

	// UpdateTokenTransfer updates a token and related provenance data for a transfer event
	UpdateTokenTransfer(ctx context.Context, event *domain.BlockchainEvent) error

	// UpdateTokenBurn updates a token and related provenance data for burn event
	UpdateTokenBurn(ctx context.Context, event *domain.BlockchainEvent) error

	// CreateMetadataUpdate creates a metadata update provenance event
	CreateMetadataUpdate(ctx context.Context, event *domain.BlockchainEvent) error

	// ResolveTokenMetadata resolves token metadata from blockchain and store metadata in the database
	ResolveTokenMetadata(ctx context.Context, tokenCID domain.TokenCID) (*metadata.NormalizedMetadata, error)

	// EnhanceTokenMetadata enhances token metadata from vendor APIs and stores enrichment source
	EnhanceTokenMetadata(ctx context.Context, tokenCID domain.TokenCID, normalizedMetadata *metadata.NormalizedMetadata) (*metadata.EnhancedMetadata, error)

	// CheckMediaURLsHealthAndUpdateViewability checks media URLs in parallel and updates token viewability
	// Returns true if the token is viewable (has at least one healthy media URL, preferably an animation URL, fallback to image URL)
	CheckMediaURLsHealthAndUpdateViewability(ctx context.Context, tokenCID string, mediaURLs []string) (bool, error)

	// IndexTokenWithMinimalProvenancesByBlockchainEvent index token with minimal provenance data
	// Minimal provenance data includes balances for from/to addresses, provenance event and change journal related to the event
	IndexTokenWithMinimalProvenancesByBlockchainEvent(ctx context.Context, event *domain.BlockchainEvent) error

	// IndexTokenWithFullProvenancesByTokenCID indexes token with full provenances using token CID
	// Full provenance data includes balances for all addresses, provenance events and change journal related to the token
	IndexTokenWithFullProvenancesByTokenCID(ctx context.Context, tokenCID domain.TokenCID) error

	// IndexTokenWithMinimalProvenancesByTokenCID indexes token with minimal provenances using tokenCID
	// If address is provided, uses address-specific indexing for ERC1155 (efficient, partial balance + events)
	// For ERC721 and FA2, address parameter is ignored and full provenance is always indexed
	IndexTokenWithMinimalProvenancesByTokenCID(ctx context.Context, tokenCID domain.TokenCID, address *string) error

	// GetEthereumTokenCIDsByOwnerWithinBlockRange retrieves token CIDs with block numbers for an owner within a block range.
	// This is used to sweep tokens by block ranges for incremental indexing.
	// limit is always applied; pass a very large value if you want "no cap". order controls scan direction for limit selection.
	// Returns effectiveFromBlock/effectiveToBlock for the range actually used after limiting.
	GetEthereumTokenCIDsByOwnerWithinBlockRange(
		ctx context.Context,
		address string,
		requestedFromBlock uint64,
		requestedToBlock uint64,
		limit int,
		order domain.BlockScanOrder,
	) (domain.TokenWithBlockRangeResult, error)

	// GetTezosTokenCIDsByAccountWithinBlockRange retrieves token CIDs with block numbers for an account within a block range
	GetTezosTokenCIDsByAccountWithinBlockRange(ctx context.Context, address string, fromBlock, toBlock uint64) ([]domain.TokenWithBlock, error)

	// GetIndexingBlockRangeForAddress retrieves the indexing block range for an address and chain
	GetIndexingBlockRangeForAddress(ctx context.Context, address string, chainID domain.Chain) (*BlockRangeResult, error)

	// UpdateIndexingBlockRangeForAddress updates the indexing block range for an address and chain
	UpdateIndexingBlockRangeForAddress(ctx context.Context, address string, chainID domain.Chain, minBlock uint64, maxBlock uint64) error

	// EnsureWatchedAddressExists creates a watched address record if it doesn't exist
	EnsureWatchedAddressExists(ctx context.Context, address string, chain domain.Chain, dailyQuota int) error

	// GetLatestEthereumBlock retrieves the latest block number from the Ethereum blockchain
	GetLatestEthereumBlock(ctx context.Context) (uint64, error)

	// GetLatestTezosBlock retrieves the latest block number from the Tezos blockchain via TzKT
	GetLatestTezosBlock(ctx context.Context) (uint64, error)

	// =============================================================================
	// Budgeted Indexing Mode Quota Activities
	// =============================================================================

	// GetQuotaInfo retrieves quota information for an address (auto-resets if expired)
	GetQuotaInfo(ctx context.Context, address string, chain domain.Chain) (*store.QuotaInfo, error)

	// IncrementTokensIndexed increments the token counter after successful indexing
	IncrementTokensIndexed(ctx context.Context, address string, chain domain.Chain, count int) error

	// =============================================================================
	// Webhook Activities
	// =============================================================================

	// GetActiveWebhookClientsByEventType retrieves active webhook clients matching the event type
	GetActiveWebhookClientsByEventType(ctx context.Context, eventType string) ([]*schema.WebhookClient, error)

	// GetWebhookClientByID retrieves a webhook client by client ID
	GetWebhookClientByID(ctx context.Context, clientID string) (*schema.WebhookClient, error)

	// CreateWebhookDeliveryRecord creates a new webhook delivery record
	CreateWebhookDeliveryRecord(ctx context.Context, delivery *schema.WebhookDelivery, event webhook.WebhookEvent) (uint64, error)

	// DeliverWebhookHTTP performs the actual HTTP delivery of a webhook with signature
	DeliverWebhookHTTP(ctx context.Context, client *schema.WebhookClient, event webhook.WebhookEvent, deliveryID uint64) (webhook.DeliveryResult, error)

	// =============================================================================
	// Address Indexing Job Activities
	// =============================================================================

	// CreateIndexingJob creates a new indexing job record (handles race conditions)
	CreateIndexingJob(ctx context.Context, address string, chain domain.Chain, workflowID string, workflowRunID *string) error

	// UpdateIndexingJobStatus updates the job status with appropriate timestamp
	UpdateIndexingJobStatus(ctx context.Context, workflowID string, status schema.IndexingJobStatus, timestamp time.Time) error

	// UpdateIndexingJobProgress updates job progress metrics
	UpdateIndexingJobProgress(ctx context.Context, workflowID string, tokensProcessed int, minBlock, maxBlock uint64) error
}

// BlockRangeResult represents the result of getting an indexing block range
type BlockRangeResult struct {
	MinBlock uint64
	MaxBlock uint64
}

// MediaURLToCheck represents a media URL that needs health checking
type MediaURLToCheck struct {
	URL    string
	Source schema.MediaHealthSource
}

// executor is the concrete implementation of Executor
type executor struct {
	store            store.Store
	metadataResolver metadata.Resolver
	metadataEnhancer metadata.Enhancer
	ethClient        ethereum.EthereumClient
	tzktClient       tezos.TzKTClient
	json             adapter.JSON
	clock            adapter.Clock
	httpClient       adapter.HTTPClient
	io               adapter.IO
	temporalActivity adapter.Activity
	blacklist        registry.BlacklistRegistry
	urlChecker       uri.URLChecker
	dataURIChecker   uri.DataURIChecker
}

// NewExecutor creates a new executor instance
func NewExecutor(
	store store.Store,
	metadataResolver metadata.Resolver,
	metadataEnhancer metadata.Enhancer,
	ethClient ethereum.EthereumClient,
	tzktClient tezos.TzKTClient,
	jsonAdapter adapter.JSON,
	clock adapter.Clock,
	httpClient adapter.HTTPClient,
	io adapter.IO,
	temporalActivity adapter.Activity,
	blacklist registry.BlacklistRegistry,
	urlChecker uri.URLChecker,
	dataURIChecker uri.DataURIChecker,
) Executor {
	return &executor{
		store:            store,
		metadataResolver: metadataResolver,
		metadataEnhancer: metadataEnhancer,
		ethClient:        ethClient,
		tzktClient:       tzktClient,
		json:             jsonAdapter,
		clock:            clock,
		httpClient:       httpClient,
		io:               io,
		temporalActivity: temporalActivity,
		blacklist:        blacklist,
		urlChecker:       urlChecker,
		dataURIChecker:   dataURIChecker,
	}
}

// CheckTokenExists checks if a token exists in the database
func (e *executor) CheckTokenExists(ctx context.Context, tokenCID domain.TokenCID) (bool, error) {
	if !tokenCID.Valid() {
		return false, domain.ErrInvalidTokenCID
	}

	token, err := e.store.GetTokenByTokenCID(ctx, tokenCID.String())
	if err != nil {
		return false, fmt.Errorf("failed to check if token exists: %w", err)
	}

	return token != nil, nil
}

// verifyTokenExistsOnChain verifies if a token exists on the blockchain
// This is an internal method used to validate token existence before indexing
func (e *executor) verifyTokenExistsOnChain(ctx context.Context, tokenCID domain.TokenCID) (bool, error) {
	chain, standard, contractAddress, tokenNumber := tokenCID.Parse()

	switch chain {
	case domain.ChainEthereumMainnet, domain.ChainEthereumSepolia:
		// Use the Ethereum client's TokenExists method which properly handles both ERC721 and ERC1155
		exists, err := e.ethClient.TokenExists(ctx, contractAddress, tokenNumber, standard)
		if err != nil {
			logger.WarnCtx(ctx, "Failed to verify token existence on Ethereum",
				zap.String("tokenCID", tokenCID.String()),
				zap.Error(err))
			return false, err
		}
		return exists, nil

	case domain.ChainTezosMainnet, domain.ChainTezosGhostnet:
		// For Tezos FA2, check token metadata
		_, err := e.tzktClient.GetTokenMetadata(ctx, contractAddress, tokenNumber)
		if err != nil {
			// Check if it's a "token not found on chain" error (specific error from TzKT client)
			if errors.Is(err, domain.ErrTokenNotFoundOnChain) {
				return false, nil
			}
			logger.WarnCtx(ctx, "Failed to verify token existence on Tezos",
				zap.String("tokenCID", tokenCID.String()),
				zap.Error(err))
			return false, err
		}
		return true, nil

	default:
		return false, fmt.Errorf("unsupported chain: %s", chain)
	}
}

// CreateTokenMint creates a new token and related provenance data for mint event
func (e *executor) CreateTokenMint(ctx context.Context, event *domain.BlockchainEvent) error {
	// Validate event
	if !event.Valid() {
		return domain.ErrInvalidBlockchainEvent
	}

	// Marshal raw event
	rawEventData, err := e.json.Marshal(event)
	if err != nil {
		return err
	}

	// Transform domain event to store input
	input := store.CreateTokenMintInput{
		Token: store.CreateTokenInput{
			TokenCID:        event.TokenCID().String(),
			Chain:           event.Chain,
			Standard:        event.Standard,
			ContractAddress: event.ContractAddress,
			TokenNumber:     event.TokenNumber,
			CurrentOwner:    event.CurrentOwner(),
			Burned:          false,
		},
		ProvenanceEvent: store.CreateProvenanceEventInput{
			Chain:       event.Chain,
			EventType:   schema.ProvenanceEventTypeMint,
			FromAddress: event.FromAddress,
			ToAddress:   event.ToAddress,
			Quantity:    event.Quantity,
			TxHash:      event.TxHash,
			BlockNumber: event.BlockNumber,
			BlockHash:   event.BlockHash,
			Raw:         rawEventData,
			Timestamp:   event.Timestamp,
		},
	}

	// Add balance
	input.Balance = store.CreateBalanceInput{
		OwnerAddress: *event.ToAddress,
		Quantity:     event.Quantity,
	}
	// For erc1155 & fa2, the balance should be fetched again
	// to ensure the balance is correct.
	// erc1155 & fa2 can be minted multiple times for the same token.
	switch event.Standard {
	case domain.StandardFA2:
		balance, err := e.tzktClient.GetTokenOwnerBalance(
			ctx,
			event.ContractAddress,
			event.TokenNumber,
			*event.ToAddress)
		if err != nil {
			return fmt.Errorf("failed to get FA2 token balance: %w", err)
		}
		input.Balance.Quantity = balance
	case domain.StandardERC1155:
		balance, err := e.ethClient.ERC1155BalanceOf(
			ctx,
			event.ContractAddress,
			*event.ToAddress,
			event.TokenNumber)
		if err != nil {
			return fmt.Errorf("failed to get ERC1155 token balance: %w", err)
		}
		input.Balance.Quantity = balance
	}

	// Create the token atomically with balance, provenance event, and change journal
	if err := e.store.CreateTokenMint(ctx, input); err != nil {
		return fmt.Errorf("failed to create token mint: %w", err)
	}

	return nil
}

// UpdateTokenTransfer updates a token and related provenance data for a transfer event
func (e *executor) UpdateTokenTransfer(ctx context.Context, event *domain.BlockchainEvent) error {
	// Validate event
	if !event.Valid() {
		return domain.ErrInvalidBlockchainEvent
	}

	// Marshal raw event
	rawEventData, err := e.json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	// Prepare balance updates
	var senderBalanceUpdate *store.UpdateBalanceInput
	var receiverBalanceUpdate *store.UpdateBalanceInput

	// Sender balance update (decrease)
	if !types.StringNilOrEmpty(event.FromAddress) {
		senderBalanceUpdate = &store.UpdateBalanceInput{
			OwnerAddress: *event.FromAddress,
			Delta:        event.Quantity,
		}
	}

	// Receiver balance update (increase)
	if !types.StringNilOrEmpty(event.ToAddress) {
		receiverBalanceUpdate = &store.UpdateBalanceInput{
			OwnerAddress: *event.ToAddress,
			Delta:        event.Quantity,
		}
	}

	// Transform domain event to store input
	input := store.UpdateTokenTransferInput{
		TokenCID:              event.TokenCID().String(),
		CurrentOwner:          event.CurrentOwner(),
		SenderBalanceUpdate:   senderBalanceUpdate,
		ReceiverBalanceUpdate: receiverBalanceUpdate,
		ProvenanceEvent: store.CreateProvenanceEventInput{
			Chain:       event.Chain,
			EventType:   schema.ProvenanceEventTypeTransfer,
			FromAddress: event.FromAddress,
			ToAddress:   event.ToAddress,
			Quantity:    event.Quantity,
			TxHash:      event.TxHash,
			BlockNumber: event.BlockNumber,
			BlockHash:   event.BlockHash,
			Raw:         rawEventData,
			Timestamp:   event.Timestamp,
		},
	}

	// Update the token atomically with balance updates, provenance event, and change journal
	if err := e.store.UpdateTokenTransfer(ctx, input); err != nil {
		return fmt.Errorf("failed to update token transfer: %w", err)
	}

	return nil
}

// ResolveTokenMetadata resolves token metadata from blockchain and store metadata in the database
func (e *executor) ResolveTokenMetadata(ctx context.Context, tokenCID domain.TokenCID) (*metadata.NormalizedMetadata, error) {
	// Validate token CID
	if !tokenCID.Valid() {
		return nil, domain.ErrInvalidTokenCID
	}

	// Resolve the token metadata from blockchain
	normalizedMetadata, err := e.metadataResolver.Resolve(ctx, tokenCID)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve token metadata: %w", err)
	}

	// Get current token with metadata from database
	tokenWithMetadata, err := e.store.GetTokenWithMetadataByTokenCID(ctx, tokenCID.String())
	if err != nil {
		return nil, fmt.Errorf("failed to get token with metadata: %w", err)
	}

	if tokenWithMetadata == nil {
		return nil, domain.ErrTokenNotFound
	}

	// Hash the new metadata
	hash, metadataJSON, err := e.metadataResolver.RawHash(normalizedMetadata)
	if err != nil {
		return nil, fmt.Errorf("failed to get raw hash: %w", err)
	}
	hashString := hex.EncodeToString(hash)

	// Preserve the origin JSON if it exists
	var originJSON []byte
	if tokenWithMetadata.Metadata != nil {
		originJSON = tokenWithMetadata.Metadata.OriginJSON
	} else {
		originJSON = metadataJSON
	}

	// Convert artists to schema.Artist
	var artists schema.Artists
	for _, artist := range normalizedMetadata.Artists {
		artists = append(artists, schema.Artist{
			DID:  artist.DID,
			Name: artist.Name,
		})
	}

	// Convert publisher to schema.Publisher
	var publisher *schema.Publisher
	if normalizedMetadata.Publisher != nil {
		publisher = &schema.Publisher{
			Name: types.StringPtr(string(*normalizedMetadata.Publisher.Name)),
			URL:  normalizedMetadata.Publisher.URL,
		}
	}

	now := e.clock.Now()
	// Transform metadata to store input
	input := store.CreateTokenMetadataInput{
		TokenID:         tokenWithMetadata.Token.ID,
		OriginJSON:      originJSON,
		LatestJSON:      metadataJSON,
		LatestHash:      &hashString,
		EnrichmentLevel: schema.EnrichmentLevelNone,
		LastRefreshedAt: now,
		ImageURL:        &normalizedMetadata.Image,
		AnimationURL:    &normalizedMetadata.Animation,
		Name:            &normalizedMetadata.Name,
		Artists:         artists,
		Description:     &normalizedMetadata.Description,
		Publisher:       publisher,
		MimeType:        normalizedMetadata.MimeType,
	}

	// Upsert the metadata
	if err := e.store.UpsertTokenMetadata(ctx, input); err != nil {
		return nil, fmt.Errorf("failed to upsert token metadata: %w", err)
	}

	logger.InfoCtx(ctx, "Successfully resolved token metadata",
		zap.String("tokenCID", tokenCID.String()))

	return normalizedMetadata, nil
}

// EnhanceTokenMetadata enhances token metadata from vendor APIs and stores enrichment source
func (e *executor) EnhanceTokenMetadata(ctx context.Context, tokenCID domain.TokenCID, normalizedMetadata *metadata.NormalizedMetadata) (*metadata.EnhancedMetadata, error) {
	// Validate token CID
	if !tokenCID.Valid() {
		return nil, domain.ErrInvalidTokenCID
	}

	// Get token to retrieve its ID
	token, err := e.store.GetTokenByTokenCID(ctx, tokenCID.String())
	if err != nil {
		return nil, fmt.Errorf("failed to get token: %w", err)
	}

	if token == nil {
		return nil, domain.ErrTokenNotFound
	}

	// Enhance metadata from vendor APIs
	enhanced, err := e.metadataEnhancer.Enhance(ctx, tokenCID, normalizedMetadata)
	if err != nil {
		return nil, fmt.Errorf("failed to enhance metadata: %w", err)
	}

	// If no enhancement is available, skip
	if enhanced == nil {
		logger.InfoCtx(ctx, "No enhancement available for token", zap.String("tokenCID", tokenCID.String()))
		return nil, nil
	}

	// Hash the vendor JSON
	hash, err := e.metadataEnhancer.VendorJsonHash(enhanced)
	if err != nil {
		return nil, fmt.Errorf("failed to get vendor JSON hash: %w", err)
	}
	hashString := hex.EncodeToString(hash)

	// Convert artists to schema.Artists
	var artists schema.Artists
	for _, artist := range enhanced.Artists {
		artists = append(artists, schema.Artist{
			DID:  artist.DID,
			Name: artist.Name,
		})
	}

	// Upsert enrichment source (which also updates enrichment_level to 'vendor' in the same transaction)
	enrichmentInput := store.CreateEnrichmentSourceInput{
		TokenID:      token.ID,
		Vendor:       enhanced.Vendor,
		VendorJSON:   enhanced.VendorJSON,
		VendorHash:   &hashString,
		ImageURL:     enhanced.ImageURL,
		AnimationURL: enhanced.AnimationURL,
		Name:         enhanced.Name,
		Description:  enhanced.Description,
		Artists:      artists,
		MimeType:     enhanced.MimeType,
	}

	if err := e.store.UpsertEnrichmentSource(ctx, enrichmentInput); err != nil {
		return nil, fmt.Errorf("failed to upsert enrichment source: %w", err)
	}

	logger.InfoCtx(ctx, "Successfully enhanced token metadata",
		zap.String("tokenCID", tokenCID.String()),
		zap.String("vendor", string(enhanced.Vendor)))

	return enhanced, nil
}

// CheckMediaURLsHealthAndUpdateViewability checks media URLs in parallel and updates token viewability
// This combines URL health checking with viewability computation and DB update
func (e *executor) CheckMediaURLsHealthAndUpdateViewability(ctx context.Context, tokenCID string, mediaURLs []string) (bool, error) {
	logger.InfoCtx(ctx, "Checking media health and updating viewability",
		zap.String("token_cid", tokenCID),
		zap.Strings("urls", mediaURLs),
		zap.Int("url_count", len(mediaURLs)),
	)

	// If no URLs to check, return false
	if len(mediaURLs) == 0 {
		return false, nil
	}

	// Use goroutines to check URLs in parallel
	type urlResult struct {
		url          string
		healthStatus schema.MediaHealthStatus
		lastError    *string
	}

	resultsChan := make(chan urlResult, len(mediaURLs))
	var wg sync.WaitGroup

	// Launch goroutine for each URL
	for _, url := range mediaURLs {
		wg.Add(1)
		go func(u string) {
			defer wg.Done()

			var status schema.MediaHealthStatus
			var errMsg *string

			if types.IsDataURI(u) {
				result := e.dataURIChecker.Check(u)
				if result.Valid {
					status = schema.MediaHealthStatusHealthy
				} else {
					status = schema.MediaHealthStatusBroken
					errMsg = result.Error
				}
			} else {
				result := e.urlChecker.Check(ctx, u)
				switch result.Status {
				case uri.HealthStatusHealthy:
					status = schema.MediaHealthStatusHealthy
				case uri.HealthStatusBroken:
					status = schema.MediaHealthStatusBroken
				default:
					status = schema.MediaHealthStatusUnknown
				}
				errMsg = result.Error
			}

			resultsChan <- urlResult{
				url:          u,
				healthStatus: status,
				lastError:    errMsg,
			}
		}(url)
	}

	// Wait for all checks to complete
	wg.Wait()
	close(resultsChan)

	// Collect results
	healthStatuses := make(map[string]schema.MediaHealthStatus)
	for result := range resultsChan {
		healthStatuses[result.url] = result.healthStatus

		// Update media health in database
		if err := e.store.UpdateTokenMediaHealthByURL(ctx, result.url, result.healthStatus, result.lastError); err != nil {
			logger.ErrorCtx(ctx, err, zap.String("url", result.url))
		}
	}

	// Get token to update viewability
	token, err := e.store.GetTokenByTokenCID(ctx, tokenCID)
	if err != nil {
		return false, fmt.Errorf("failed to get token: %w", err)
	}

	if token == nil {
		return false, domain.ErrTokenNotFound
	}

	// Update viewability
	changes, err := e.store.BatchUpdateTokensViewability(ctx, []uint64{token.ID})
	if err != nil {
		return false, fmt.Errorf("failed to update token viewability: %w", err)
	}

	// If there are changes, return the new viewability status
	if len(changes) > 0 {
		return changes[0].NewViewable, nil
	}

	// If no changes, the viewability status didn't change
	// We need to query the current status to return the correct value
	viewabilityInfo, err := e.store.GetTokensViewabilityByIDs(ctx, []uint64{token.ID})
	if err != nil {
		return false, fmt.Errorf("failed to get token viewability: %w", err)
	}

	if len(viewabilityInfo) == 0 {
		return false, fmt.Errorf("token viewability info not found")
	}

	return viewabilityInfo[0].IsViewable, nil
}

// UpdateTokenBurn updates a token and related provenance data for burn event
func (e *executor) UpdateTokenBurn(ctx context.Context, event *domain.BlockchainEvent) error {
	// Validate event
	if !event.Valid() {
		return domain.ErrInvalidBlockchainEvent
	}

	// Marshal raw event
	rawEventData, err := e.json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	// Prepare balance update for sender (decrease)
	var senderBalanceUpdate *store.UpdateBalanceInput
	if !types.StringNilOrEmpty(event.FromAddress) {
		senderBalanceUpdate = &store.UpdateBalanceInput{
			OwnerAddress: *event.FromAddress,
			Delta:        event.Quantity,
		}
	}

	// Transform domain event to store input
	input := store.CreateTokenBurnInput{
		TokenCID:            event.TokenCID().String(),
		SenderBalanceUpdate: senderBalanceUpdate,
		ProvenanceEvent: store.CreateProvenanceEventInput{
			Chain:       event.Chain,
			EventType:   schema.ProvenanceEventTypeBurn,
			FromAddress: event.FromAddress,
			ToAddress:   event.ToAddress,
			Quantity:    event.Quantity,
			TxHash:      event.TxHash,
			BlockNumber: event.BlockNumber,
			BlockHash:   event.BlockHash,
			Raw:         rawEventData,
			Timestamp:   event.Timestamp,
		},
	}

	// Update the token burn atomically with balance update, provenance event, and change journal
	if err := e.store.UpdateTokenBurn(ctx, input); err != nil {
		return fmt.Errorf("failed to update token burn: %w", err)
	}

	return nil
}

// CreateMetadataUpdate creates a metadata update provenance event
func (e *executor) CreateMetadataUpdate(ctx context.Context, event *domain.BlockchainEvent) error {
	// Validate event
	if !event.Valid() {
		return domain.ErrInvalidBlockchainEvent
	}

	// Marshal raw event
	rawEventData, err := e.json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	// Transform domain event to store input
	input := store.CreateMetadataUpdateInput{
		TokenCID: event.TokenCID().String(),
		ProvenanceEvent: store.CreateProvenanceEventInput{
			Chain:       event.Chain,
			EventType:   schema.ProvenanceEventTypeMetadataUpdate,
			FromAddress: event.FromAddress,
			ToAddress:   event.ToAddress,
			Quantity:    event.Quantity,
			TxHash:      event.TxHash,
			BlockNumber: event.BlockNumber,
			BlockHash:   event.BlockHash,
			Raw:         rawEventData,
			Timestamp:   event.Timestamp,
		},
	}

	// Create metadata update provenance event
	if err := e.store.CreateMetadataUpdate(ctx, input); err != nil {
		return fmt.Errorf("failed to create metadata update: %w", err)
	}

	return nil
}

// IndexTokenWithMinimalProvenancesByBlockchainEvent index token with minimal provenance data
// Minimal provenance data includes balances for from/to addresses, provenance event and change journal related to the event
func (e *executor) IndexTokenWithMinimalProvenancesByBlockchainEvent(ctx context.Context, event *domain.BlockchainEvent) error {
	// Validate event
	if !event.Valid() {
		return domain.ErrInvalidBlockchainEvent
	}

	tokenCID := event.TokenCID()
	chain, standard, contractAddress, tokenNumber := tokenCID.Parse()

	// Marshal raw event
	rawEventData, err := e.json.Marshal(event)
	if err != nil {
		return err
	}

	// Determine provenance event type
	transferEventType := domain.TransferEventType(event.FromAddress, event.ToAddress)
	provenanceEventType := types.TransferEventTypeToProvenanceEventType(transferEventType)

	// Prepare token input
	input := store.CreateTokenWithProvenancesInput{
		Token: store.CreateTokenInput{
			TokenCID:        tokenCID.String(),
			Chain:           chain,
			Standard:        standard,
			ContractAddress: contractAddress,
			TokenNumber:     tokenNumber,
			CurrentOwner:    event.CurrentOwner(),
			Burned:          event.EventType == domain.EventTypeBurn,
		},
		Balances: []store.CreateBalanceInput{},
		Events: []store.CreateProvenanceEventInput{
			{
				Chain:       event.Chain,
				EventType:   provenanceEventType,
				FromAddress: event.FromAddress,
				ToAddress:   event.ToAddress,
				Quantity:    event.Quantity,
				TxHash:      event.TxHash,
				BlockNumber: event.BlockNumber,
				BlockHash:   event.BlockHash,
				Raw:         rawEventData,
				Timestamp:   event.Timestamp,
			},
		},
	}

	// Fetch and add balance for from address
	if err := e.addOwnerBalanceAndEvents(ctx, event, event.FromAddress, &input); err != nil {
		return fmt.Errorf("failed to add balance for from address: %w", err)
	}

	// Fetch and add balance for to address
	if err := e.addOwnerBalanceAndEvents(ctx, event, event.ToAddress, &input); err != nil {
		return fmt.Errorf("failed to add balance for to address: %w", err)
	}

	// Create/update the token with minimal provenance data
	if err := e.store.CreateTokenWithProvenances(ctx, input); err != nil {
		return fmt.Errorf("failed to create token with minimal provenances: %w", err)
	}

	return nil
}

// addOwnerBalanceAndEvents fetches and adds balance and events for a specific owner to the input
func (e *executor) addOwnerBalanceAndEvents(ctx context.Context, event *domain.BlockchainEvent, address *string, input *store.CreateTokenWithProvenancesInput) error {
	// Skip if address is nil or zero address
	if types.StringNilOrEmpty(address) || *address == domain.ETHEREUM_ZERO_ADDRESS {
		return nil
	}

	// Skip from address for ERC721 (sender doesn't have balance after transfer)
	if event.Standard == domain.StandardERC721 && address == event.FromAddress {
		return nil
	}

	// Fetch balance and events based on standard
	balance, events, err := e.fetchOwnerBalanceAndEvents(ctx, event, *address)
	if err != nil {
		return err
	}

	// Add balance if positive
	if types.IsPositiveNumeric(balance) {
		input.Balances = append(input.Balances, store.CreateBalanceInput{
			OwnerAddress: *address,
			Quantity:     balance,
		})
	}

	// Add provenance events
	for _, evt := range events {
		rawEventData, err := e.json.Marshal(evt)
		if err != nil {
			return fmt.Errorf("failed to marshal event: %w", err)
		}

		input.Events = append(input.Events, store.CreateProvenanceEventInput{
			Chain:       evt.Chain,
			EventType:   schema.ProvenanceEventType(evt.EventType),
			FromAddress: evt.FromAddress,
			ToAddress:   evt.ToAddress,
			Quantity:    evt.Quantity,
			TxHash:      evt.TxHash,
			BlockNumber: evt.BlockNumber,
			BlockHash:   evt.BlockHash,
			Raw:         rawEventData,
			Timestamp:   evt.Timestamp,
		})
	}

	return nil
}

// fetchOwnerBalanceAndEvents fetches balance and events for an owner based on token standard
func (e *executor) fetchOwnerBalanceAndEvents(ctx context.Context, event *domain.BlockchainEvent, address string) (string, []domain.BlockchainEvent, error) {
	switch event.Standard {
	case domain.StandardFA2:
		balance, err := e.tzktClient.GetTokenOwnerBalance(ctx, event.ContractAddress, event.TokenNumber, address)
		return balance, nil, err

	case domain.StandardERC1155:
		return e.ethClient.GetERC1155BalanceAndEventsForOwner(ctx, event.ContractAddress, event.TokenNumber, address)

	case domain.StandardERC721:
		// ERC721: to address always has balance of 1
		return "1", nil, nil

	default:
		return "", nil, nil
	}
}

// IndexTokenWithMinimalProvenancesByTokenCID indexes token with minimal provenances using tokenCID
// Minimal provenance data includes balances for all addresses (or specific owner if provided).
// The provenance events and change journal are not included for full indexing,
// but ARE included for owner-specific indexing.
// If address is provided, uses address-specific indexing for ERC1155 (efficient, partial balance + events)
func (e *executor) IndexTokenWithMinimalProvenancesByTokenCID(ctx context.Context, tokenCID domain.TokenCID, address *string) error {
	// If address is not provided, verify the token exists in the database and on-chain before indexing
	// Without address, the event logs would be huge and the indexing is inefficient for ERC1155 tokens.
	// Otherwise, we can skip the existence check and index the token with minimal provenances, the indexing with
	// address-specific indexing is more efficient for ERC1155 tokens.
	// We also can assume the workflow will gather tokens belongs to the address before calling this activity.
	if address == nil {
		// Check if token exists in database
		existsInDB, err := e.CheckTokenExists(ctx, tokenCID)
		if err != nil {
			return fmt.Errorf("failed to check token existence in DB: %w", err)
		}

		// If token doesn't exist in DB, verify it exists on-chain
		if !existsInDB {
			existsOnChain, err := e.verifyTokenExistsOnChain(ctx, tokenCID)
			if err != nil {
				return fmt.Errorf("failed to verify token existence on-chain: %w", err)
			}
			if !existsOnChain {
				return temporal.NewNonRetryableApplicationError(
					"token not found on chain",
					"TokenNotFoundOnChain",
					domain.ErrTokenNotFoundOnChain,
				)
			}
		}
	}

	chain, standard, contractAddress, tokenNumber := tokenCID.Parse()

	// Prepare input for creating token with minimal provenance
	input := store.CreateTokenWithProvenancesInput{
		Token: store.CreateTokenInput{
			TokenCID:        tokenCID.String(),
			Chain:           chain,
			Standard:        standard,
			ContractAddress: contractAddress,
			TokenNumber:     tokenNumber,
		},
		Balances: []store.CreateBalanceInput{},
		Events:   []store.CreateProvenanceEventInput{}, // No events for minimal provenance
	}

	// Fetch current balances based on chain and standard
	switch chain {
	case domain.ChainTezosMainnet, domain.ChainTezosGhostnet:
		// For Tezos FA2, TzKT API provides all current token balances directly
		balances, err := e.tzktClient.GetTokenBalances(ctx, contractAddress, tokenNumber)
		if err != nil {
			return fmt.Errorf("failed to get token balances from TzKT: %w", err)
		}

		for _, bal := range balances {
			if types.IsPositiveNumeric(bal.Balance) {
				input.Balances = append(input.Balances, store.CreateBalanceInput{
					OwnerAddress: bal.Account.Address,
					Quantity:     bal.Balance,
				})
			}
		}

	case domain.ChainEthereumMainnet, domain.ChainEthereumSepolia:
		switch standard {
		case domain.StandardERC721:
			// For ERC721, use ownerOf to get the current single owner
			owner, err := e.ethClient.ERC721OwnerOf(ctx, contractAddress, tokenNumber)
			if err != nil {
				if strings.Contains(err.Error(), ethereum.ErrExecutionReverted.Error()) ||
					strings.Contains(err.Error(), ethereum.ErrContractNotFound.Error()) {
					// It's likely that the token is not found on chain (burned or contract has been self-destructed),
					// so we return a non-retryable error
					return temporal.NewNonRetryableApplicationError(
						"token not found on chain",
						"TokenNotFoundOnChain",
						domain.ErrTokenNotFoundOnChain,
					)
				}
				if strings.Contains(err.Error(), ethereum.ErrOutOfGas.Error()) {
					// It's likely that the contract is unreachable (e.g. contract exists but function is not callable),
					// so we return a non-retryable error
					// FIXME: This could be strict so we may need a better approach to categorize contract if function is not callable.
					return temporal.NewNonRetryableApplicationError(
						"contract is unreachable",
						"ContractUnreachable",
						domain.ErrContractUnreachable,
					)
				}

				return fmt.Errorf("failed to get ERC721 owner: %w", err)
			} else if owner != "" && owner != domain.ETHEREUM_ZERO_ADDRESS {
				input.Token.CurrentOwner = &owner
				input.Balances = append(input.Balances, store.CreateBalanceInput{
					OwnerAddress: owner,
					Quantity:     "1",
				})
			} else {
				input.Token.Burned = true
			}

		case domain.StandardERC1155:
			// For ERC1155, choose between full indexing (all owners) or owner-specific indexing
			if address != nil {
				// OWNER-SPECIFIC PATH: Efficient indexing for one owner only
				// Get balance and events for this specific owner
				balance, events, err := e.ethClient.GetERC1155BalanceAndEventsForOwner(ctx, contractAddress, tokenNumber, *address)
				if err != nil {
					if strings.Contains(err.Error(), ethereum.ErrExecutionReverted.Error()) ||
						strings.Contains(err.Error(), ethereum.ErrContractNotFound.Error()) {
						// It's likely that the token is not found on chain (burned or contract has been self-destructed),
						// so we return a non-retryable error
						return temporal.NewNonRetryableApplicationError(
							"token not found on chain",
							"TokenNotFoundOnChain",
							domain.ErrTokenNotFoundOnChain,
						)
					}

					if strings.Contains(err.Error(), ethereum.ErrOutOfGas.Error()) {
						// It's likely that the contract is unreachable (e.g. contract exists but function is not callable),
						// so we return a non-retryable error
						// FIXME: This could be strict so we may need a better approach to categorize contract if function is not callable.
						return temporal.NewNonRetryableApplicationError(
							"contract is unreachable",
							"ContractUnreachable",
							domain.ErrContractUnreachable,
						)
					}

					return fmt.Errorf("failed to get ERC1155 balance and events for owner: %w", err)
				}

				// If balance is not a positive numeric value, return a non-retryable error
				if !types.IsPositiveNumeric(balance) {
					return temporal.NewNonRetryableApplicationError(
						"balance is not a positive numeric value",
						"InvalidBalance",
						domain.ErrBalanceIsNotAPositiveNumericValue,
					)
				}

				logger.InfoCtx(ctx, "Indexing ERC1155 token for specific owner",
					zap.String("tokenCID", tokenCID.String()),
					zap.String("owner", *address),
					zap.String("balance", balance),
					zap.Int("eventCount", len(events)),
				)

				// Convert blockchain events to store input
				storeEvents := make([]store.CreateProvenanceEventInput, 0, len(events))
				for _, evt := range events {
					blockHash := evt.BlockHash

					// Marshal event data for indexing
					rawEventData, err := e.json.Marshal(evt)
					if err != nil {
						return fmt.Errorf("failed to marshal event: %w", err)
					}

					storeEvents = append(storeEvents, store.CreateProvenanceEventInput{
						Chain:       evt.Chain,
						EventType:   schema.ProvenanceEventType(evt.EventType),
						FromAddress: evt.FromAddress,
						ToAddress:   evt.ToAddress,
						Quantity:    evt.Quantity,
						TxHash:      evt.TxHash,
						BlockNumber: evt.BlockNumber,
						BlockHash:   blockHash,
						Raw:         rawEventData,
						Timestamp:   evt.Timestamp,
					})
				}

				// Use UpsertTokenBalanceForOwner for partial update
				ownerInput := store.UpsertTokenBalanceForOwnerInput{
					Token: store.CreateTokenInput{
						TokenCID:        tokenCID.String(),
						Chain:           chain,
						Standard:        standard,
						ContractAddress: contractAddress,
						TokenNumber:     tokenNumber,
						CurrentOwner:    nil,   // ERC1155 doesn't have single owner
						Burned:          false, // Can't determine from one owner's perspective
					},
					OwnerAddress: *address,
					Quantity:     balance,
					Events:       storeEvents,
				}

				if err := e.store.UpsertTokenBalanceForOwner(ctx, ownerInput); err != nil {
					return fmt.Errorf("failed to upsert token balance for owner: %w", err)
				}

				return nil

			} else {
				// FULL INDEXING PATH: Get all owners' balances (existing logic)
				// For ERC1155, use ERC1155Balances to calculate balances from events
				// FIXME: ERC1155Balances has a 30-second timeout and 10M block limit to prevent indefinite blocking
				// This means we may get partial/incomplete balances for high-activity contracts
				balances, err := e.ethClient.ERC1155Balances(ctx, contractAddress, tokenNumber)
				if err != nil {
					// It's likely that the token is not found on chain (burned or contract has been self-destructed),
					// so we return a non-retryable error
					if strings.Contains(err.Error(), ethereum.ErrExecutionReverted.Error()) ||
						strings.Contains(err.Error(), ethereum.ErrContractNotFound.Error()) {
						return temporal.NewNonRetryableApplicationError(
							"token not found on chain",
							"TokenNotFoundOnChain",
							domain.ErrTokenNotFoundOnChain,
						)
					}

					if strings.Contains(err.Error(), ethereum.ErrOutOfGas.Error()) {
						// It's likely that the contract is unreachable (e.g. contract exists but function is not callable),
						// so we return a non-retryable error
						// FIXME: This could be strict so we may need a better approach to categorize contract if function is not callable.
						return temporal.NewNonRetryableApplicationError(
							"contract is unreachable",
							"ContractUnreachable",
							domain.ErrContractUnreachable,
						)
					}

					return fmt.Errorf("failed to get ERC1155 balances from Ethereum: %w", err)
				}

				// Convert balances map to CreateBalanceInput slice
				for addr, balance := range balances {
					if types.IsPositiveNumeric(balance) {
						input.Balances = append(input.Balances, store.CreateBalanceInput{
							OwnerAddress: addr,
							Quantity:     balance,
						})
					}
				}
			}

		default:
			return fmt.Errorf("unsupported standard: %s", standard)
		}

	default:
		return fmt.Errorf("unsupported chain: %s", chain)
	}

	// Determine burned status from balances (if no positive balances, token is burned)
	if len(input.Balances) == 0 {
		input.Token.Burned = true
	}

	// Create/update the token with minimal provenance data
	if err := e.store.CreateTokenWithProvenances(ctx, input); err != nil {
		return fmt.Errorf("failed to create token with minimal provenances: %w", err)
	}

	return nil
}

// GetTokenCIDsByOwner retrieves all token CIDs owned by an address
func (e *executor) GetTokenCIDsByOwner(ctx context.Context, address string) ([]domain.TokenCID, error) {
	return e.store.GetTokenCIDsByOwner(ctx, address)
}

// GetEthereumTokenCIDsByOwnerWithinBlockRange retrieves all token CIDs for an owner within a block range
// This is used to sweep tokens by block ranges for incremental indexing
// Filters out blacklisted tokens before returning
func (e *executor) GetEthereumTokenCIDsByOwnerWithinBlockRange(
	ctx context.Context,
	address string,
	requestedFromBlock uint64,
	requestedToBlock uint64,
	limit int,
	order domain.BlockScanOrder,
) (domain.TokenWithBlockRangeResult, error) {
	blockchain := types.AddressToBlockchain(address)
	if blockchain != domain.BlockchainEthereum {
		return domain.TokenWithBlockRangeResult{}, fmt.Errorf("unsupported blockchain for address: %s", address)
	}

	return e.ethClient.GetTokenCIDsByOwnerAndBlockRange(
		ctx,
		address,
		requestedFromBlock,
		requestedToBlock,
		limit,
		order,
		e.blacklist,
	)
}

// IndexTokenWithFullProvenancesByTokenCID indexes token with full provenances using token CID
// Full provenance data includes balances for all addresses, provenance events and change journal related to the token
func (e *executor) IndexTokenWithFullProvenancesByTokenCID(ctx context.Context, tokenCID domain.TokenCID) error {
	chain, standard, contractAddress, tokenNumber := tokenCID.Parse()

	// Fetch all events based on chain
	allBalances := make(map[string]string)
	var allEvents []domain.BlockchainEvent
	var err error

	switch chain {
	case domain.ChainTezosMainnet, domain.ChainTezosGhostnet:
		// Fetch all balances from TzKT
		tzktBalances, err := e.tzktClient.GetTokenBalances(ctx, contractAddress, tokenNumber)
		if err != nil {
			return fmt.Errorf("failed to fetch token balances from TzKT: %w", err)
		}

		for _, bal := range tzktBalances {
			if types.IsPositiveNumeric(bal.Balance) {
				allBalances[bal.Account.Address] = bal.Balance
			}
		}

		// Fetch all token events (transfers + metadata updates) from TzKT
		allEvents, err = e.tzktClient.GetTokenEvents(ctx, contractAddress, tokenNumber)
		if err != nil {
			return fmt.Errorf("failed to fetch token events from TzKT: %w", err)
		}

	case domain.ChainEthereumMainnet, domain.ChainEthereumSepolia:
		// Fetch all events (transfers + metadata updates) from Ethereum
		allEvents, err = e.ethClient.GetTokenEvents(ctx, contractAddress, tokenNumber, standard)
		if err != nil {
			return fmt.Errorf("failed to fetch token events from Ethereum: %w", err)
		}

		// Fetch current balances for all unique addresses in the events
		addressSet := make(map[string]bool)
		for _, evt := range allEvents {
			if evt.EventType == domain.EventTypeMetadataUpdate {
				continue
			}

			if !types.StringNilOrEmpty(evt.FromAddress) && *evt.FromAddress != domain.ETHEREUM_ZERO_ADDRESS {
				addressSet[*evt.FromAddress] = true
			}
			if !types.StringNilOrEmpty(evt.ToAddress) && *evt.ToAddress != domain.ETHEREUM_ZERO_ADDRESS {
				addressSet[*evt.ToAddress] = true
			}
		}

		switch standard {
		case domain.StandardERC1155:
			// Convert addressSet to slice for batch query
			addresses := make([]string, 0, len(addressSet))
			for addr := range addressSet {
				addresses = append(addresses, addr)
			}

			// Use batch balance query for efficiency
			allBalances, err = e.ethClient.ERC1155BalanceOfBatch(ctx, contractAddress, tokenNumber, addresses)
			if err != nil {
				return fmt.Errorf("failed to get ERC1155 balances from Ethereum: %w", err)
			}
		case domain.StandardERC721:
			// Skip fetching balance for ERC721 tokens
			// ERC721 ownership is determined from the latest transfer event
		}

	default:
		return fmt.Errorf("unsupported chain: %s", chain)
	}

	// Determine the current owner and burned status based on the latest transfer event
	var currentOwner *string
	var burned bool

	if len(allEvents) > 0 {
		// Since the events are in ascending order, check from the end
		for i := len(allEvents) - 1; i >= 0; i-- {
			evt := allEvents[i]
			if evt.EventType == domain.EventTypeTransfer ||
				evt.EventType == domain.EventTypeBurn ||
				evt.EventType == domain.EventTypeMint {
				currentOwner = evt.CurrentOwner()

				switch evt.Standard {
				case domain.StandardERC721:
					// For ERC721, the token is burned if the burn event is found
					if evt.EventType == domain.EventTypeBurn {
						burned = true
					}
				case domain.StandardFA2, domain.StandardERC1155:
					// For ERC1155 & FA2, the token is burned if there are no balances
					if len(allBalances) == 0 {
						burned = true
					}
				}

				break
			}
		}
	}

	// Prepare input for creating/updating token with all provenance data
	input := store.CreateTokenWithProvenancesInput{
		Token: store.CreateTokenInput{
			TokenCID:        tokenCID.String(),
			Chain:           chain,
			Standard:        standard,
			ContractAddress: contractAddress,
			TokenNumber:     tokenNumber,
			CurrentOwner:    currentOwner,
			Burned:          burned,
		},
		Balances: []store.CreateBalanceInput{},
		Events:   []store.CreateProvenanceEventInput{},
	}

	// For ERC721, the token is owned by a single address, so we can add the current owner with a balance of 1
	if standard == domain.StandardERC721 && currentOwner != nil && !burned {
		allBalances = map[string]string{
			*currentOwner: "1",
		}
	}

	for addr, quantity := range allBalances {
		input.Balances = append(input.Balances, store.CreateBalanceInput{
			OwnerAddress: addr,
			Quantity:     quantity,
		})
	}

	for _, evt := range allEvents {
		rawEventData, err := e.json.Marshal(evt)
		if err != nil {
			logger.WarnCtx(ctx, "Failed to marshal event", zap.Error(err))
			continue
		}

		eventType := types.TransferEventTypeToProvenanceEventType(evt.EventType)
		input.Events = append(input.Events, store.CreateProvenanceEventInput{
			Chain:       evt.Chain,
			EventType:   eventType,
			FromAddress: evt.FromAddress,
			ToAddress:   evt.ToAddress,
			Quantity:    evt.Quantity,
			TxHash:      evt.TxHash,
			BlockNumber: evt.BlockNumber,
			BlockHash:   evt.BlockHash,
			Raw:         rawEventData,
			Timestamp:   evt.Timestamp,
		})
	}

	if err := e.store.CreateTokenWithProvenances(ctx, input); err != nil {
		return fmt.Errorf("failed to create token with full provenances: %w", err)
	}

	return nil
}

// GetTezosTokenCIDsByAccountWithinBlockRange retrieves token CIDs with block numbers for an account within a block range
// Handles pagination automatically by fetching all results within the range
// Returns tokens with their last interaction block number (lastLevel from TzKT)
// Filters out blacklisted tokens before returning
func (e *executor) GetTezosTokenCIDsByAccountWithinBlockRange(ctx context.Context, address string, fromBlock, toBlock uint64) ([]domain.TokenWithBlock, error) {
	var allTokensWithBlocks []domain.TokenWithBlock
	limit := tezos.MAX_PAGE_SIZE
	offset := 0

	for {
		balances, err := e.tzktClient.GetTokenBalancesByAccountWithinBlockRange(ctx, address, fromBlock, toBlock, limit, offset)
		if err != nil {
			return nil, fmt.Errorf("failed to get token balances from TzKT: %w", err)
		}

		if len(balances) == 0 {
			break
		}

		for _, balance := range balances {
			tokenCID := domain.NewTokenCID(
				e.tzktClient.ChainID(),
				balance.Token.Standard,
				balance.Token.Contract.Address,
				balance.Token.TokenID,
			)

			// Filter out blacklisted tokens
			if !e.blacklist.IsTokenCIDBlacklisted(tokenCID) {
				allTokensWithBlocks = append(allTokensWithBlocks, domain.TokenWithBlock{
					TokenCID:    tokenCID,
					BlockNumber: balance.LastLevel,
				})
			}
		}

		// If we got fewer results than the limit, we've reached the end
		if len(balances) < limit {
			break
		}

		offset += limit
	}

	return allTokensWithBlocks, nil
}

// GetLatestEthereumBlock retrieves the latest block number from the Ethereum blockchain using caching
func (e *executor) GetLatestEthereumBlock(ctx context.Context) (uint64, error) {
	blockNum, err := e.ethClient.GetLatestBlock(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get latest block: %w", err)
	}
	return blockNum, nil
}

// GetLatestTezosBlock retrieves the latest block number from the Tezos blockchain via TzKT using caching
func (e *executor) GetLatestTezosBlock(ctx context.Context) (uint64, error) {
	latestBlock, err := e.tzktClient.GetLatestBlock(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get latest Tezos block: %w", err)
	}
	return latestBlock, nil
}

// =============================================================================
// Budgeted Indexing Mode Quota Activities
// =============================================================================

// GetQuotaInfo retrieves quota information for an address (auto-resets if expired)
func (e *executor) GetQuotaInfo(ctx context.Context, address string, chain domain.Chain) (*store.QuotaInfo, error) {
	return e.store.GetQuotaInfo(ctx, address, chain)
}

// IncrementTokensIndexed increments the token counter after successful indexing
func (e *executor) IncrementTokensIndexed(ctx context.Context, address string, chain domain.Chain, count int) error {
	return e.store.IncrementTokensIndexed(ctx, address, chain, count)
}

// =============================================================================
// Webhook Activities
// =============================================================================
func (e *executor) GetIndexingBlockRangeForAddress(ctx context.Context, address string, chainID domain.Chain) (*BlockRangeResult, error) {
	minBlock, maxBlock, err := e.store.GetIndexingBlockRangeForAddress(ctx, address, chainID)
	if err != nil {
		return nil, err
	}
	return &BlockRangeResult{
		MinBlock: minBlock,
		MaxBlock: maxBlock,
	}, nil
}

// UpdateIndexingBlockRangeForAddress updates the indexing block range for an address and chain
func (e *executor) UpdateIndexingBlockRangeForAddress(ctx context.Context, address string, chainID domain.Chain, minBlock uint64, maxBlock uint64) error {
	return e.store.UpdateIndexingBlockRangeForAddress(ctx, address, chainID, minBlock, maxBlock)
}

// EnsureWatchedAddressExists creates a watched address record if it doesn't exist
func (e *executor) EnsureWatchedAddressExists(ctx context.Context, address string, chain domain.Chain, dailyQuota int) error {
	return e.store.EnsureWatchedAddressExists(ctx, address, chain, dailyQuota)
}

// =============================================================================
// Webhook Activities
// =============================================================================

// GetActiveWebhookClientsByEventType retrieves active webhook clients matching the event type
func (e *executor) GetActiveWebhookClientsByEventType(ctx context.Context, eventType string) ([]*schema.WebhookClient, error) {
	return e.store.GetActiveWebhookClientsByEventType(ctx, eventType)
}

// GetWebhookClientByID retrieves a webhook client by client ID
func (e *executor) GetWebhookClientByID(ctx context.Context, clientID string) (*schema.WebhookClient, error) {
	return e.store.GetWebhookClientByID(ctx, clientID)
}

// CreateWebhookDeliveryRecord creates a new webhook delivery record
func (e *executor) CreateWebhookDeliveryRecord(ctx context.Context, delivery *schema.WebhookDelivery, event webhook.WebhookEvent) (uint64, error) {
	// Marshal event to JSON for the Payload field
	eventJSON, err := e.json.Marshal(event)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal webhook event: %w", err)
	}
	delivery.Payload = eventJSON

	if err := e.store.CreateWebhookDelivery(ctx, delivery); err != nil {
		return 0, err
	}
	return delivery.ID, nil
}

// DeliverWebhookHTTP performs the actual HTTP delivery of a webhook with HMAC signature
// This activity will be automatically retried by Temporal with exponential backoff
func (e *executor) DeliverWebhookHTTP(ctx context.Context, client *schema.WebhookClient, event webhook.WebhookEvent, deliveryID uint64) (webhook.DeliveryResult, error) {
	// Get attempt number from Temporal activity info
	attempt := e.temporalActivity.GetInfo(ctx).Attempt

	logger.InfoCtx(ctx, "Attempting webhook delivery",
		zap.String("clientID", client.ClientID),
		zap.String("eventID", event.EventID),
		zap.Int32("attempt", attempt))

	// Generate signed payload with HMAC-SHA256
	payload, signature, timestamp, err := webhook.GenerateSignedPayload(client.WebhookSecret, event)
	if err != nil {
		logger.ErrorCtx(ctx, errors.New("failed to generate signed payload"),
			zap.Error(err), zap.String("clientID", client.ClientID))

		if ierr := e.store.UpdateWebhookDeliveryStatus(ctx, deliveryID, schema.WebhookDeliveryStatusFailed, int(attempt), nil, "", err.Error()); ierr != nil {
			logger.ErrorCtx(ctx, errors.New("failed to update webhook delivery status"),
				zap.Error(ierr),
				zap.String("clientID", client.ClientID))
		}

		// Return non-retryable error to stop Temporal retry
		return webhook.DeliveryResult{Success: false, Error: err.Error()}, temporal.NewNonRetryableApplicationError(err.Error(), "failed to generate signed payload", err)
	}

	// Build headers for webhook delivery
	headers := map[string]string{
		"Content-Type":         "application/json",
		"X-Webhook-Signature":  signature,
		"X-Webhook-Event-ID":   event.EventID,
		"X-Webhook-Event-Type": event.EventType,
		"X-Webhook-Timestamp":  fmt.Sprintf("%d", timestamp),
		"User-Agent":           "FF-Indexer-Webhook/2.0",
	}

	// Send HTTP request
	resp, err := e.httpClient.PostWithHeadersNoRetry(ctx, client.WebhookURL, headers, bytes.NewReader(payload))
	if err != nil {
		logger.ErrorCtx(ctx, errors.New("failed to post webhook HTTP request"),
			zap.Error(err), zap.String("clientID", client.ClientID))

		if ierr := e.store.UpdateWebhookDeliveryStatus(ctx, deliveryID, schema.WebhookDeliveryStatusFailed, int(attempt), nil, "", err.Error()); ierr != nil {
			logger.ErrorCtx(ctx, errors.New("failed to update webhook delivery status"),
				zap.Error(ierr),
				zap.String("clientID", client.ClientID))
		}

		// Return error to trigger Temporal retry
		return webhook.DeliveryResult{Success: false, Error: err.Error()}, err
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			logger.WarnCtx(ctx, "failed to close response body", zap.Error(err), zap.String("url", client.WebhookURL))
		}
	}()

	// Read response body with a size limit to prevent memory exhaustion
	// We use LimitReader to ensure we never read more than 4KB
	limitedReader := io.LimitReader(resp.Body, 4*1024)

	respBody, err := e.io.ReadAll(limitedReader)
	if err != nil {
		logger.ErrorCtx(ctx, errors.New("failed to read response body for webhook delivery"),
			zap.Error(err), zap.String("clientID", client.ClientID))
		// Continue with empty body - don't fail the delivery
		respBody = []byte{}
	}

	// Check status code for non-2xx responses
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		logger.ErrorCtx(ctx, errors.New("failed to post webhook HTTP request"),
			zap.Int("statusCode", resp.StatusCode),
			zap.String("clientID", client.ClientID))

		err := fmt.Errorf("HTTP %d", resp.StatusCode)
		if ierr := e.store.UpdateWebhookDeliveryStatus(ctx, deliveryID, schema.WebhookDeliveryStatusFailed, int(attempt), &resp.StatusCode, string(respBody), err.Error()); ierr != nil {
			logger.ErrorCtx(ctx, errors.New("failed to update webhook delivery status"),
				zap.Error(ierr),
				zap.String("clientID", client.ClientID))
		}

		// Return error to trigger Temporal retry
		return webhook.DeliveryResult{Success: false, StatusCode: resp.StatusCode, Body: string(respBody)}, err
	}

	// Update webhook delivery status
	if err := e.store.UpdateWebhookDeliveryStatus(ctx, deliveryID, schema.WebhookDeliveryStatusSuccess, int(attempt), &resp.StatusCode, string(respBody), ""); err != nil {
		logger.ErrorCtx(ctx, errors.New("failed to update webhook delivery status"),
			zap.Error(err), zap.String("clientID", client.ClientID))
	}

	return webhook.DeliveryResult{Success: true, StatusCode: resp.StatusCode, Body: string(respBody)}, nil
}

// =============================================================================
// Address Indexing Job Activities
// =============================================================================

// CreateIndexingJob creates a new indexing job record
func (e *executor) CreateIndexingJob(ctx context.Context, address string, chain domain.Chain, workflowID string, workflowRunID *string) error {
	input := store.CreateAddressIndexingJobInput{
		Address:       address,
		Chain:         chain,
		Status:        schema.IndexingJobStatusRunning, // Workflow is starting, set to running
		WorkflowID:    workflowID,
		WorkflowRunID: workflowRunID,
	}

	return e.store.CreateAddressIndexingJob(ctx, input)
}

// UpdateIndexingJobStatus updates the job status with appropriate timestamp
func (e *executor) UpdateIndexingJobStatus(ctx context.Context, workflowID string, status schema.IndexingJobStatus, timestamp time.Time) error {
	return e.store.UpdateAddressIndexingJobStatus(ctx, workflowID, status, timestamp)
}

// UpdateIndexingJobProgress updates job progress metrics
func (e *executor) UpdateIndexingJobProgress(ctx context.Context, workflowID string, tokensProcessed int, minBlock, maxBlock uint64) error {
	return e.store.UpdateAddressIndexingJobProgress(ctx, workflowID, tokensProcessed, minBlock, maxBlock)
}

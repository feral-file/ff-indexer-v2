package workflows

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"

	"go.temporal.io/sdk/temporal"
	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	mediaProcessor "github.com/feral-file/ff-indexer-v2/internal/media/processor"
	"github.com/feral-file/ff-indexer-v2/internal/metadata"
	"github.com/feral-file/ff-indexer-v2/internal/providers/cloudflare"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum"
	"github.com/feral-file/ff-indexer-v2/internal/providers/tezos"
	"github.com/feral-file/ff-indexer-v2/internal/store"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
	"github.com/feral-file/ff-indexer-v2/internal/types"
)

// Executor defines the interface for executing activities
//
//go:generate mockgen -source=activities.go -destination=../mocks/activities.go -package=mocks -mock_names=Executor=MockExecutor
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

	// FetchTokenMetadata fetches token metadata from blockchain
	FetchTokenMetadata(ctx context.Context, tokenCID domain.TokenCID) (*metadata.NormalizedMetadata, error)

	// UpsertTokenMetadata stores or updates token metadata in the database
	UpsertTokenMetadata(ctx context.Context, tokenCID domain.TokenCID, normalizedMetadata *metadata.NormalizedMetadata) error

	// EnhanceTokenMetadata enhances token metadata from vendor APIs and stores enrichment source
	EnhanceTokenMetadata(ctx context.Context, tokenCID domain.TokenCID, normalizedMetadata *metadata.NormalizedMetadata) (*metadata.EnhancedMetadata, error)

	// IndexTokenWithMinimalProvenancesByBlockchainEvent index token with minimal provenance data
	// Minimal provenance data includes balances for from/to addresses, provenance event and change journal related to the event
	IndexTokenWithMinimalProvenancesByBlockchainEvent(ctx context.Context, event *domain.BlockchainEvent) error

	// IndexTokenWithFullProvenancesByTokenCID indexes token with full provenances using token CID
	// Full provenance data includes balances for all addresses, provenance events and change journal related to the token
	IndexTokenWithFullProvenancesByTokenCID(ctx context.Context, tokenCID domain.TokenCID) error

	// IndexTokenWithMinimalProvenancesByTokenCID indexes token with minimal provenances using tokenCID
	// Minimal provenance data includes balances for all addresses.
	// The provenance events and change journal are not included.
	IndexTokenWithMinimalProvenancesByTokenCID(ctx context.Context, tokenCID domain.TokenCID) error

	// GetEthereumTokenCIDsByOwnerWithinBlockRange retrieves all token CIDs with block numbers for an owner within a block range
	// This is used to sweep tokens by block ranges for incremental indexing
	GetEthereumTokenCIDsByOwnerWithinBlockRange(ctx context.Context, address string, fromBlock, toBlock uint64) ([]domain.TokenWithBlock, error)

	// GetTezosTokenCIDsByAccountWithinBlockRange retrieves token CIDs with block numbers for an account within a block range
	GetTezosTokenCIDsByAccountWithinBlockRange(ctx context.Context, address string, fromBlock, toBlock uint64) ([]domain.TokenWithBlock, error)

	// GetIndexingBlockRangeForAddress retrieves the indexing block range for an address and chain
	GetIndexingBlockRangeForAddress(ctx context.Context, address string, chainID domain.Chain) (*BlockRangeResult, error)

	// UpdateIndexingBlockRangeForAddress updates the indexing block range for an address and chain
	UpdateIndexingBlockRangeForAddress(ctx context.Context, address string, chainID domain.Chain, minBlock uint64, maxBlock uint64) error

	// EnsureWatchedAddressExists creates a watched address record if it doesn't exist
	EnsureWatchedAddressExists(ctx context.Context, address string, chain domain.Chain) error

	// GetLatestEthereumBlock retrieves the latest block number from the Ethereum blockchain
	GetLatestEthereumBlock(ctx context.Context) (uint64, error)

	// GetLatestTezosBlock retrieves the latest block number from the Tezos blockchain via TzKT
	GetLatestTezosBlock(ctx context.Context) (uint64, error)

	// IndexMediaFile processes a media file by downloading, uploading to storage, and storing metadata
	IndexMediaFile(ctx context.Context, url string) error
}

// BlockRangeResult represents the result of getting an indexing block range
type BlockRangeResult struct {
	MinBlock uint64
	MaxBlock uint64
}

// executor is the concrete implementation of Executor
type executor struct {
	store            store.Store
	metadataResolver metadata.Resolver
	metadataEnhancer metadata.Enhancer
	mediaProcessor   mediaProcessor.Processor
	ethClient        ethereum.EthereumClient
	tzktClient       tezos.TzKTClient
	json             adapter.JSON
	clock            adapter.Clock
}

// NewExecutor creates a new executor instance
func NewExecutor(
	store store.Store,
	metadataResolver metadata.Resolver,
	metadataEnhancer metadata.Enhancer,
	ethClient ethereum.EthereumClient,
	tzktClient tezos.TzKTClient,
	mediaProcessor mediaProcessor.Processor,
	jsonAdapter adapter.JSON,
	clock adapter.Clock,
) Executor {
	return &executor{
		store:            store,
		metadataResolver: metadataResolver,
		metadataEnhancer: metadataEnhancer,
		ethClient:        ethClient,
		tzktClient:       tzktClient,
		mediaProcessor:   mediaProcessor,
		json:             jsonAdapter,
		clock:            clock,
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

// FetchTokenMetadata fetches token metadata from blockchain
func (e *executor) FetchTokenMetadata(ctx context.Context, tokenCID domain.TokenCID) (*metadata.NormalizedMetadata, error) {
	// Validate token CID
	if !tokenCID.Valid() {
		return nil, domain.ErrInvalidTokenCID
	}

	return e.metadataResolver.Resolve(ctx, tokenCID)
}

// UpsertTokenMetadata stores or updates token metadata in the database
func (e *executor) UpsertTokenMetadata(ctx context.Context, tokenCID domain.TokenCID, normalizedMetadata *metadata.NormalizedMetadata) error {
	// Validate token CID
	if !tokenCID.Valid() {
		return domain.ErrInvalidTokenCID
	}

	// Get token with metadata
	result, err := e.store.GetTokenWithMetadataByTokenCID(ctx, tokenCID.String())
	if err != nil {
		return fmt.Errorf("failed to get token with metadata: %w", err)
	}

	if result == nil {
		return domain.ErrTokenNotFound
	}

	currentMetadata := result.Metadata

	// Hash the new metadata
	hash, metadataJSON, err := e.metadataResolver.RawHash(normalizedMetadata)
	if err != nil {
		return fmt.Errorf("failed to get raw hash: %w", err)
	}
	hashString := hex.EncodeToString(hash)

	// Preserve the origin JSON if it exists
	var originJSON []byte
	if currentMetadata != nil {
		originJSON = currentMetadata.OriginJSON
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
		TokenID:         result.Token.ID,
		OriginJSON:      originJSON,
		LatestJSON:      metadataJSON,
		LatestHash:      &hashString,
		EnrichmentLevel: schema.EnrichmentLevelNone,
		LastRefreshedAt: &now,
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
		return fmt.Errorf("failed to upsert token metadata: %w", err)
	}

	return nil
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

	chain, standard, contractAddress, tokenNumber := tokenCID.Parse()

	// Marshal raw event
	rawEventData, err := e.json.Marshal(event)
	if err != nil {
		return err
	}

	// Determine provenance event type
	transferEventType := domain.TransferEventType(event.FromAddress, event.ToAddress)
	provenanceEventType := types.TransferEventTypeToProvenanceEventType(transferEventType)
	burned := event.EventType == domain.EventTypeBurn

	// Prepare input for creating/updating token with minimal provenance data
	input := store.CreateTokenWithProvenancesInput{
		Token: store.CreateTokenInput{
			TokenCID:        tokenCID.String(),
			Chain:           chain,
			Standard:        standard,
			ContractAddress: contractAddress,
			TokenNumber:     tokenNumber,
			CurrentOwner:    event.CurrentOwner(),
			Burned:          burned,
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

	// Fetch balances for the from and to addresses (minimal provenance)
	if !types.StringNilOrEmpty(event.FromAddress) && *event.FromAddress != domain.ETHEREUM_ZERO_ADDRESS {
		var balance string
		switch event.Standard {
		case domain.StandardFA2:
			balance, err = e.tzktClient.GetTokenOwnerBalance(
				ctx,
				event.ContractAddress,
				event.TokenNumber,
				*event.FromAddress)
			if err != nil {
				return fmt.Errorf("failed to get FA2 token balance for from address: %w", err)
			}
		case domain.StandardERC1155:
			balance, err = e.ethClient.ERC1155BalanceOf(
				ctx,
				event.ContractAddress,
				*event.FromAddress,
				event.TokenNumber)
			if err != nil {
				return fmt.Errorf("failed to get ERC1155 token balance for from address: %w", err)
			}
		case domain.StandardERC721:
			// No need to insert sender balance for ERC721
		}

		if types.IsPositiveNumeric(balance) {
			input.Balances = append(input.Balances, store.CreateBalanceInput{
				OwnerAddress: *event.FromAddress,
				Quantity:     balance,
			})
		}
	}

	if !types.StringNilOrEmpty(event.ToAddress) && *event.ToAddress != domain.ETHEREUM_ZERO_ADDRESS {
		var balance string
		switch event.Standard {
		case domain.StandardFA2:
			balance, err = e.tzktClient.GetTokenOwnerBalance(
				ctx,
				event.ContractAddress,
				event.TokenNumber,
				*event.ToAddress)
			if err != nil {
				return fmt.Errorf("failed to get FA2 token balance for to address: %w", err)
			}
		case domain.StandardERC1155:
			balance, err = e.ethClient.ERC1155BalanceOf(
				ctx,
				event.ContractAddress,
				*event.ToAddress,
				event.TokenNumber)
			if err != nil {
				return fmt.Errorf("failed to get ERC1155 token balance for to address: %w", err)
			}
		case domain.StandardERC721:
			balance = "1"
		}

		if types.IsPositiveNumeric(balance) {
			input.Balances = append(input.Balances, store.CreateBalanceInput{
				OwnerAddress: *event.ToAddress,
				Quantity:     balance,
			})
		}
	}

	// Create/update the token with minimal provenance data
	if err := e.store.CreateTokenWithProvenances(ctx, input); err != nil {
		return fmt.Errorf("failed to create token with minimal provenances: %w", err)
	}

	return nil
}

// IndexTokenWithMinimalProvenancesByTokenCID indexes token with minimal provenances using tokenCID
// Minimal provenance data includes balances for all addresses.
// The provenance events and change journal are not included.
func (e *executor) IndexTokenWithMinimalProvenancesByTokenCID(ctx context.Context, tokenCID domain.TokenCID) error {
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
			// For ERC1155, use ERC1155Balances to calculate balances from events
			// FIXME: ERC1155Balances has a 30-second timeout and 10M block limit to prevent indefinite blocking
			// This means we may get partial/incomplete balances for high-activity contracts
			balances, err := e.ethClient.ERC1155Balances(ctx, contractAddress, tokenNumber)
			if err != nil {
				// Check if error is due to timeout - if so, continue with partial balances
				if errors.Is(err, context.DeadlineExceeded) {
					logger.WarnCtx(ctx, "ERC1155 balance fetch timed out, continuing with partial balances",
						zap.String("tokenCID", tokenCID.String()),
						zap.String("contract", contractAddress),
						zap.String("tokenNumber", tokenNumber),
					)
					// balances will be empty or partial, continue with whatever we got
				} else {
					return fmt.Errorf("failed to get ERC1155 balances from Ethereum: %w", err)
				}
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
func (e *executor) GetEthereumTokenCIDsByOwnerWithinBlockRange(ctx context.Context, address string, fromBlock, toBlock uint64) ([]domain.TokenWithBlock, error) {
	blockchain := domain.AddressToBlockchain(address)
	if blockchain != domain.BlockchainEthereum {
		return nil, fmt.Errorf("unsupported blockchain for address: %s", address)
	}

	return e.ethClient.GetTokenCIDsByOwnerAndBlockRange(ctx, address, fromBlock, toBlock)
}

// IndexTokenWithFullProvenancesByTokenCID indexes token with full provenances using token CID
// Full provenance data includes balances for all addresses, provenance events and change journal related to the token
func (e *executor) IndexTokenWithFullProvenancesByTokenCID(ctx context.Context, tokenCID domain.TokenCID) error {
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

	chain, standard, contractAddress, tokenNumber := tokenCID.Parse()

	// Fetch all events based on chain
	var allBalances map[string]string
	var allEvents []domain.BlockchainEvent

	switch chain {
	case domain.ChainTezosMainnet, domain.ChainTezosGhostnet:
		// Fetch all balances from TzKT
		tzktBalances, err := e.tzktClient.GetTokenBalances(ctx, contractAddress, tokenNumber)
		if err != nil {
			return fmt.Errorf("failed to fetch token balances from TzKT: %w", err)
		}

		allBalances = make(map[string]string)
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

		allBalances = make(map[string]string)
		for addr := range addressSet {
			var balance string
			switch standard {
			case domain.StandardERC1155:
				balance, err = e.ethClient.ERC1155BalanceOf(ctx, contractAddress, addr, tokenNumber)
				if err != nil {
					logger.WarnCtx(ctx, "Failed to get balance for address", zap.String("address", addr), zap.Error(err))
					continue
				}
			case domain.StandardERC721:
				// Skip fetching balance here
				continue
			}

			if types.IsPositiveNumeric(balance) {
				allBalances[addr] = balance
			}
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
			allTokensWithBlocks = append(allTokensWithBlocks, domain.TokenWithBlock{
				TokenCID:    tokenCID,
				BlockNumber: balance.LastLevel,
			})
		}

		// If we got fewer results than the limit, we've reached the end
		if len(balances) < limit {
			break
		}

		offset += limit
	}

	return allTokensWithBlocks, nil
}

// GetLatestEthereumBlock retrieves the latest block number from the Ethereum blockchain
func (e *executor) GetLatestEthereumBlock(ctx context.Context) (uint64, error) {
	header, err := e.ethClient.HeaderByNumber(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to get latest block: %w", err)
	}
	return header.Number.Uint64(), nil
}

// GetLatestTezosBlock retrieves the latest block number from the Tezos blockchain via TzKT
func (e *executor) GetLatestTezosBlock(ctx context.Context) (uint64, error) {
	latestBlock, err := e.tzktClient.GetLatestBlock(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get latest Tezos block: %w", err)
	}
	return latestBlock, nil
}

// GetIndexingBlockRangeForAddress retrieves the indexing block range for an address and chain
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
func (e *executor) EnsureWatchedAddressExists(ctx context.Context, address string, chain domain.Chain) error {
	return e.store.EnsureWatchedAddressExists(ctx, address, chain)
}

// IndexMediaFile processes a media file by downloading, uploading to storage, and storing metadata
func (e *executor) IndexMediaFile(ctx context.Context, url string) error {
	if url == "" {
		return fmt.Errorf("media URL is empty")
	}

	// Check if media asset already exists
	existingAsset, err := e.store.GetMediaAssetBySourceURL(ctx, url, e.toSchemaStorageProvider())
	if err != nil {
		return fmt.Errorf("failed to check existing media asset: %w", err)
	}

	if existingAsset != nil {
		logger.InfoCtx(ctx, "Media asset already exists, skipping", zap.String("url", url))
		return nil
	}

	// Process the media file
	if err := e.mediaProcessor.Process(ctx, url); err != nil {
		if errors.Is(err, domain.ErrUnsupportedMediaFile) ||
			errors.Is(err, domain.ErrUnsupportedSelfHostedMediaFile) ||
			errors.Is(err, domain.ErrExceededMaxFileSize) ||
			errors.Is(err, domain.ErrMissingContentLength) {
			// Skip known errors
			return nil
		}

		return fmt.Errorf("failed to process media file: %w", err)
	}

	logger.InfoCtx(ctx, "Successfully indexed media file", zap.String("url", url))
	return nil
}

// toSchemaStorageProvider converts the provider name to a schema storage provider
func (e *executor) toSchemaStorageProvider() schema.StorageProvider {
	switch e.mediaProcessor.Provider() {
	case cloudflare.CLOUDFLARE_PROVIDER_NAME:
		return schema.StorageProviderCloudflare
	default:
		return schema.StorageProviderSelfHosted
	}
}

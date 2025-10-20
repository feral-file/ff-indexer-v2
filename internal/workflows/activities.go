package workflows

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	logger "github.com/bitmark-inc/autonomy-logger"
	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/metadata"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum"
	"github.com/feral-file/ff-indexer-v2/internal/providers/tezos"
	"github.com/feral-file/ff-indexer-v2/internal/store"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

// Executor defines the interface for executing activities
//
//go:generate mockgen -source=activities.go -destination=../mocks/activities.go -package=mocks -mock_names=Executor=MockExecutor
type Executor interface {
	// CreateTokenMintActivity creates a new token in the database
	CreateTokenMintActivity(ctx context.Context, event *domain.BlockchainEvent) error

	// FetchTokenMetadataActivity fetches token metadata from blockchain/API
	FetchTokenMetadataActivity(ctx context.Context, tokenCID domain.TokenCID) (*metadata.NormalizedMetadata, error)

	// UpsertTokenMetadataActivity stores or updates token metadata in the database
	UpsertTokenMetadataActivity(ctx context.Context, tokenCID domain.TokenCID, metadata *metadata.NormalizedMetadata) error
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
) Executor {
	return &executor{
		store:            store,
		metadataResolver: metadataResolver,
		metadataEnhancer: metadataEnhancer,
		ethClient:        ethClient,
		tzktClient:       tzktClient,
		json:             jsonAdapter,
		clock:            clock,
	}
}

// CreateTokenMintActivity creates a new token in the database
func (e *executor) CreateTokenMintActivity(ctx context.Context, event *domain.BlockchainEvent) error {
	// Check if token already exists
	existingToken, err := e.store.GetTokenByTokenCID(ctx, event.TokenCID().String())
	if err != nil {
		return fmt.Errorf("failed to check if token exists: %w", err)
	}

	if existingToken != nil {
		return domain.ErrTokenAlreadyExists
	}

	// Determine current owner from event
	// For ERC1155 and FA2, the current owner is nil since it's a multi-owner token
	currentOwner := event.ToAddress
	if event.Standard == domain.StandardERC1155 || event.Standard == domain.StandardFA2 {
		currentOwner = nil
	}

	// Marshal raw event
	rawEventData, err := e.json.Marshal(event)
	if err != nil {
		return err
	}

	// Transform domain event to store input
	input := store.CreateTokenMintInput{
		Token: store.CreateTokenInput{
			TokenCID:         event.TokenCID().String(),
			Chain:            event.Chain,
			Standard:         event.Standard,
			ContractAddress:  event.ContractAddress,
			TokenNumber:      event.TokenNumber,
			CurrentOwner:     currentOwner,
			Burned:           false,
			LastActivityTime: event.Timestamp,
		},
		ProvenanceEvent: store.CreateProvenanceEventInput{
			Chain:       event.Chain,
			EventType:   schema.ProvenanceEventTypeMint,
			FromAddress: event.FromAddress,
			ToAddress:   event.ToAddress,
			Quantity:    &event.Quantity,
			TxHash:      &event.TxHash,
			BlockNumber: &event.BlockNumber,
			BlockHash:   event.BlockHash,
			Raw:         rawEventData,
			Timestamp:   event.Timestamp,
		},
		TokenCID:  event.TokenCID().String(),
		ChangedAt: event.Timestamp,
	}

	// Add balance
	input.Balance = &store.CreateBalanceInput{
		OwnerAddress: *event.ToAddress,
		Quantity:     event.Quantity,
	}
	// For erc1155 & fa2, the balance should be fetched again
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

	logger.Info("Token mint created successfully",
		zap.String("tokenCID", event.TokenCID().String()),
		zap.String("chain", string(event.Chain)),
		zap.String("standard", string(event.Standard)),
	)

	return nil
}

// FetchTokenMetadataActivity fetches token metadata from blockchain/API
func (e *executor) FetchTokenMetadataActivity(ctx context.Context, tokenCID domain.TokenCID) (*metadata.NormalizedMetadata, error) {
	return e.metadataResolver.Resolve(ctx, tokenCID)
}

// UpsertTokenMetadataActivity stores or updates token metadata in the database
func (e *executor) UpsertTokenMetadataActivity(ctx context.Context, tokenCID domain.TokenCID, metadata *metadata.NormalizedMetadata) error {
	// Get the token to obtain its ID
	token, err := e.store.GetTokenByTokenCID(ctx, tokenCID.String())
	if err != nil {
		return fmt.Errorf("failed to get token: %w", err)
	}

	if token == nil {
		return domain.ErrTokenNotFound
	}

	// Convert metadata to JSON
	metadataJSON, err := e.json.Marshal(metadata.Raw)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	// Hash the metadata
	hash := sha256.Sum256(metadataJSON)
	hashString := hex.EncodeToString(hash[:])

	now := e.clock.Now()

	// Transform metadata to store input
	input := store.CreateTokenMetadataInput{
		TokenID:         token.ID,
		OriginJSON:      metadataJSON,
		LatestJSON:      metadataJSON,
		LatestHash:      &hashString,
		EnrichmentLevel: schema.EnrichmentLevelNone,
		LastRefreshedAt: &now,
		ImageURL:        &metadata.Image,
		AnimationURL:    &metadata.Animation,
		Name:            &metadata.Name,
		Artists:         metadata.Artists,
	}

	// Upsert the metadata
	if err := e.store.UpsertTokenMetadata(ctx, input); err != nil {
		return fmt.Errorf("failed to upsert token metadata: %w", err)
	}

	logger.Info("Token metadata upserted successfully",
		zap.String("tokenCID", tokenCID.String()),
		zap.Int64("tokenID", token.ID),
	)

	// TODO: Trigger workflow to enrich token metadata from vendor APIs (OpenSea, ArtBlocks, etc.)

	return nil
}

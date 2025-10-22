package workflows

import (
	"context"
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
	"github.com/feral-file/ff-indexer-v2/internal/types"
)

// Executor defines the interface for executing activities
//
//go:generate mockgen -source=activities.go -destination=../mocks/activities.go -package=mocks -mock_names=Executor=MockExecutor
type Executor interface {
	// CheckTokenExistsActivity checks if a token exists in the database
	CheckTokenExistsActivity(ctx context.Context, tokenCID domain.TokenCID) (bool, error)

	// CreateTokenMintActivity creates a new token and related provenance data
	CreateTokenMintActivity(ctx context.Context, event *domain.BlockchainEvent) error

	// UpdateTokenTransferActivity updates a token and related provenance data for a transfer event
	UpdateTokenTransferActivity(ctx context.Context, event *domain.BlockchainEvent) error

	// UpdateTokenBurnActivity updates a token and related provenance data as burned
	UpdateTokenBurnActivity(ctx context.Context, event *domain.BlockchainEvent) error

	// CreateMetadataUpdateActivity creates a metadata update provenance event and change journal entry
	CreateMetadataUpdateActivity(ctx context.Context, event *domain.BlockchainEvent) error

	// FetchTokenMetadataActivity fetches token metadata from blockchain
	FetchTokenMetadataActivity(ctx context.Context, tokenCID domain.TokenCID) (*metadata.NormalizedMetadata, error)

	// UpsertTokenMetadataActivity stores or updates token metadata in the database
	UpsertTokenMetadataActivity(ctx context.Context, tokenCID domain.TokenCID, metadata *metadata.NormalizedMetadata) error

	// IndexTokenWithMinimalProvenancesActivity index token with minimal provenance data (balances for from/to addresses only)
	IndexTokenWithMinimalProvenancesActivity(ctx context.Context, event *domain.BlockchainEvent) error

	// IndexTokenWithFullProvenancesActivity index token with all its provenance data (all balances and events)
	IndexTokenWithFullProvenancesActivity(ctx context.Context, event *domain.BlockchainEvent) error
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

// CheckTokenExistsActivity check if a token exists
func (e *executor) CheckTokenExistsActivity(ctx context.Context, tokenCID domain.TokenCID) (bool, error) {
	token, err := e.store.GetTokenByTokenCID(ctx, tokenCID.String())
	if err != nil {
		return false, fmt.Errorf("failed to check if token exists: %w", err)
	}

	return token != nil, nil
}

// CreateTokenMintActivity creates a new token and related provenance data
func (e *executor) CreateTokenMintActivity(ctx context.Context, event *domain.BlockchainEvent) error {
	// Check if token already exists
	existingToken, err := e.store.GetTokenByTokenCID(ctx, event.TokenCID().String())
	if err != nil {
		return fmt.Errorf("failed to check if token exists: %w", err)
	}

	if existingToken != nil {
		return domain.ErrTokenAlreadyExists
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
			CurrentOwner:     event.CurrentOwner(),
			Burned:           false,
			LastActivityTime: event.Timestamp,
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

// UpdateTokenTransferActivity updates a token and related provenance data for a transfer event
func (e *executor) UpdateTokenTransferActivity(ctx context.Context, event *domain.BlockchainEvent) error {
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
		LastActivityTime:      event.Timestamp,
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
		ChangedAt: event.Timestamp,
	}

	// Update the token atomically with balance updates, provenance event, and change journal
	if err := e.store.UpdateTokenTransfer(ctx, input); err != nil {
		return fmt.Errorf("failed to update token transfer: %w", err)
	}

	return nil
}

// FetchTokenMetadataActivity fetches token metadata from blockchain
func (e *executor) FetchTokenMetadataActivity(ctx context.Context, tokenCID domain.TokenCID) (*metadata.NormalizedMetadata, error) {
	return e.metadataResolver.Resolve(ctx, tokenCID)
}

// UpsertTokenMetadataActivity stores or updates token metadata in the database
func (e *executor) UpsertTokenMetadataActivity(ctx context.Context, tokenCID domain.TokenCID, metadata *metadata.NormalizedMetadata) error {
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
	hash, metadataJSON, err := metadata.RawHash()
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

	now := e.clock.Now()
	// Transform metadata to store input
	input := store.CreateTokenMetadataInput{
		TokenID:         result.Token.ID,
		OriginJSON:      originJSON,
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

	return nil
}

// UpdateTokenBurnActivity updates a token and related provenance data as burned
func (e *executor) UpdateTokenBurnActivity(ctx context.Context, event *domain.BlockchainEvent) error {
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
		ChangedAt:        event.Timestamp,
		LastActivityTime: event.Timestamp,
	}

	// Update the token burn atomically with balance update, provenance event, and change journal
	if err := e.store.UpdateTokenBurn(ctx, input); err != nil {
		return fmt.Errorf("failed to update token burn: %w", err)
	}

	return nil
}

// CreateMetadataUpdateActivity creates a metadata update provenance event and change journal entry
func (e *executor) CreateMetadataUpdateActivity(ctx context.Context, event *domain.BlockchainEvent) error {
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
		ChangedAt: event.Timestamp,
	}

	// Create metadata update provenance event and change journal entry
	if err := e.store.CreateMetadataUpdate(ctx, input); err != nil {
		return fmt.Errorf("failed to create metadata update: %w", err)
	}

	return nil
}

// IndexTokenWithMinimalProvenancesActivity index token with minimal provenance data (balances for from/to addresses only)
func (e *executor) IndexTokenWithMinimalProvenancesActivity(ctx context.Context, event *domain.BlockchainEvent) error {
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
	burned := event.EventType == domain.EventTypeBurn

	// Prepare input for creating/updating token with minimal provenance data
	input := store.CreateTokenWithProvenancesInput{
		Token: store.CreateTokenInput{
			TokenCID:         tokenCID.String(),
			Chain:            chain,
			Standard:         standard,
			ContractAddress:  contractAddress,
			TokenNumber:      tokenNumber,
			CurrentOwner:     event.CurrentOwner(),
			Burned:           burned,
			LastActivityTime: event.Timestamp,
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

// IndexTokenWithFullProvenancesActivity index token with all its provenance data (all balances and events)
func (e *executor) IndexTokenWithFullProvenancesActivity(ctx context.Context, event *domain.BlockchainEvent) error {
	// Fetch all events based on chain
	var allBalances map[string]string
	var allEvents []domain.BlockchainEvent
	var err error

	switch event.Chain {
	case domain.ChainTezosMainnet, domain.ChainTezosGhostnet:
		// Fetch all balances from TzKT
		tzktBalances, err := e.tzktClient.GetTokenBalances(ctx, event.ContractAddress, event.TokenNumber)
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
		allEvents, err = e.tzktClient.GetTokenEvents(ctx, event.ContractAddress, event.TokenNumber)
		if err != nil {
			return fmt.Errorf("failed to fetch token events from TzKT: %w", err)
		}

	case domain.ChainEthereumMainnet, domain.ChainEthereumSepolia:
		// Fetch all events (transfers + metadata updates) from Ethereum
		allEvents, err = e.ethClient.GetTokenEvents(ctx, event.ContractAddress, event.TokenNumber, event.Standard)
		if err != nil {
			return fmt.Errorf("failed to fetch token events from Ethereum: %w", err)
		}

		// Fetch current balances for all unique addresses in the events (excluding metadata updates)
		addressSet := make(map[string]bool)
		for _, evt := range allEvents {
			// Skip metadata updates
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
			switch event.Standard {
			case domain.StandardERC1155:
				balance, err = e.ethClient.ERC1155BalanceOf(ctx, event.ContractAddress, addr, event.TokenNumber)
				if err != nil {
					logger.Warn("Failed to get balance for address", zap.String("address", addr), zap.Error(err))
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
		return fmt.Errorf("unsupported chain: %s", event.Chain)
	}

	// Determine the current owner and burned status based on the latest transfer event
	var currentOwner *string
	var burned bool
	var lastActivityTime = event.Timestamp

	// Find the latest transfer event to determine current state
	if len(allEvents) > 0 {
		// Since the events are in ascending order of block number (level),
		// the latest transfer event should be checked from the end of the list
		var latestTransferEvent domain.BlockchainEvent
		for i := len(allEvents) - 1; i >= 0; i-- {
			if allEvents[i].EventType == domain.EventTypeTransfer ||
				allEvents[i].EventType == domain.EventTypeBurn ||
				allEvents[i].EventType == domain.EventTypeMint {
				latestTransferEvent = allEvents[i]
				break
			}
		}

		currentOwner = latestTransferEvent.CurrentOwner()
		if latestTransferEvent.EventType == domain.EventTypeBurn {
			burned = true
		}
		lastActivityTime = latestTransferEvent.Timestamp
	} else {
		// If no events found, use the provided event
		currentOwner = event.CurrentOwner()
		if event.EventType == domain.EventTypeBurn {
			burned = true
		}
	}

	// Prepare input for creating/updating token with all provenance data
	input := store.CreateTokenWithProvenancesInput{
		Token: store.CreateTokenInput{
			TokenCID:         event.TokenCID().String(),
			Chain:            event.Chain,
			Standard:         event.Standard,
			ContractAddress:  event.ContractAddress,
			TokenNumber:      event.TokenNumber,
			CurrentOwner:     currentOwner,
			Burned:           burned,
			LastActivityTime: lastActivityTime,
		},
		Balances: []store.CreateBalanceInput{},
		Events:   []store.CreateProvenanceEventInput{},
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
			logger.Warn("Failed to marshal event", zap.Error(err))
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
		return fmt.Errorf("failed to create token with provenances: %w", err)
	}

	return nil
}

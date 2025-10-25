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
	// CheckTokenExists checks if a token exists in the database
	CheckTokenExists(ctx context.Context, tokenCID domain.TokenCID) (bool, error)

	// CreateTokenMint creates a new token and related provenance data for mint event
	CreateTokenMint(ctx context.Context, event *domain.BlockchainEvent) error

	// UpdateTokenTransfer updates a token and related provenance data for a transfer event
	UpdateTokenTransfer(ctx context.Context, event *domain.BlockchainEvent) error

	// UpdateTokenBurn updates a token and related provenance data for burn event
	UpdateTokenBurn(ctx context.Context, event *domain.BlockchainEvent) error

	// CreateMetadataUpdate creates a metadata update provenance event and change journal entry
	CreateMetadataUpdate(ctx context.Context, event *domain.BlockchainEvent) error

	// FetchTokenMetadata fetches token metadata from blockchain
	FetchTokenMetadata(ctx context.Context, tokenCID domain.TokenCID) (*metadata.NormalizedMetadata, error)

	// UpsertTokenMetadata stores or updates token metadata in the database
	UpsertTokenMetadata(ctx context.Context, tokenCID domain.TokenCID, metadata *metadata.NormalizedMetadata) error

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

	// GetEthereumTokenCIDsByOwnerWithinBlockRange retrieves all token CIDs for an owner within a block range
	// This is used to sweep tokens by block ranges for incremental indexing
	GetEthereumTokenCIDsByOwnerWithinBlockRange(ctx context.Context, address string, fromBlock, toBlock uint64) ([]domain.TokenCID, error)

	// GetTezosTokenCIDsByAccountWithinBlockRange retrieves token CIDs for an account within a block range
	GetTezosTokenCIDsByAccountWithinBlockRange(ctx context.Context, address string, fromBlock, toBlock uint64) ([]domain.TokenCID, error)

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

// CheckTokenExists checks if a token exists in the database
func (e *executor) CheckTokenExists(ctx context.Context, tokenCID domain.TokenCID) (bool, error) {
	token, err := e.store.GetTokenByTokenCID(ctx, tokenCID.String())
	if err != nil {
		return false, fmt.Errorf("failed to check if token exists: %w", err)
	}

	return token != nil, nil
}

// CreateTokenMint creates a new token and related provenance data for mint event
func (e *executor) CreateTokenMint(ctx context.Context, event *domain.BlockchainEvent) error {
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
		ChangedAt: event.Timestamp,
	}

	// Update the token atomically with balance updates, provenance event, and change journal
	if err := e.store.UpdateTokenTransfer(ctx, input); err != nil {
		return fmt.Errorf("failed to update token transfer: %w", err)
	}

	return nil
}

// FetchTokenMetadata fetches token metadata from blockchain
func (e *executor) FetchTokenMetadata(ctx context.Context, tokenCID domain.TokenCID) (*metadata.NormalizedMetadata, error) {
	return e.metadataResolver.Resolve(ctx, tokenCID)
}

// UpsertTokenMetadata stores or updates token metadata in the database
func (e *executor) UpsertTokenMetadata(ctx context.Context, tokenCID domain.TokenCID, metadata *metadata.NormalizedMetadata) error {
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
		Description:     &metadata.Description,
		Publisher:       metadata.Publisher,
	}

	// Upsert the metadata
	if err := e.store.UpsertTokenMetadata(ctx, input); err != nil {
		return fmt.Errorf("failed to upsert token metadata: %w", err)
	}

	return nil
}

// UpdateTokenBurn updates a token and related provenance data for burn event
func (e *executor) UpdateTokenBurn(ctx context.Context, event *domain.BlockchainEvent) error {
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
		ChangedAt: event.Timestamp,
	}

	// Update the token burn atomically with balance update, provenance event, and change journal
	if err := e.store.UpdateTokenBurn(ctx, input); err != nil {
		return fmt.Errorf("failed to update token burn: %w", err)
	}

	return nil
}

// CreateMetadataUpdate creates a metadata update provenance event and change journal entry
func (e *executor) CreateMetadataUpdate(ctx context.Context, event *domain.BlockchainEvent) error {
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

// IndexTokenWithMinimalProvenancesByBlockchainEvent index token with minimal provenance data
// Minimal provenance data includes balances for from/to addresses, provenance event and change journal related to the event
func (e *executor) IndexTokenWithMinimalProvenancesByBlockchainEvent(ctx context.Context, event *domain.BlockchainEvent) error {
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
			balances, err := e.ethClient.ERC1155Balances(ctx, contractAddress, tokenNumber)
			if err != nil {
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
func (e *executor) GetEthereumTokenCIDsByOwnerWithinBlockRange(ctx context.Context, address string, fromBlock, toBlock uint64) ([]domain.TokenCID, error) {
	blockchain := domain.AddressToBlockchain(address)
	if blockchain != domain.BlockchainEthereum {
		return nil, fmt.Errorf("unsupported blockchain for address: %s", address)
	}

	return e.ethClient.GetTokenCIDsByOwnerAndBlockRange(ctx, address, fromBlock, toBlock)
}

// IndexTokenWithFullProvenancesByTokenCID indexes token with full provenances using token CID
// Full provenance data includes balances for all addresses, provenance events and change journal related to the token
func (e *executor) IndexTokenWithFullProvenancesByTokenCID(ctx context.Context, tokenCID domain.TokenCID) error {
	chain, standard, contractAddress, tokenNumber := tokenCID.Parse()

	// Fetch all events based on chain
	var allBalances map[string]string
	var allEvents []domain.BlockchainEvent
	var err error

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
		return fmt.Errorf("unsupported chain: %s", chain)
	}

	// Determine the current owner and burned status based on the latest transfer event
	var currentOwner *string
	var burned bool

	if len(allEvents) > 0 {
		// Since the events are in ascending order, check from the end
		for i := len(allEvents) - 1; i >= 0; i-- {
			if allEvents[i].EventType == domain.EventTypeTransfer ||
				allEvents[i].EventType == domain.EventTypeBurn ||
				allEvents[i].EventType == domain.EventTypeMint {
				currentOwner = allEvents[i].CurrentOwner()
				if allEvents[i].EventType == domain.EventTypeBurn {
					burned = true
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
		return fmt.Errorf("failed to create token with full provenances: %w", err)
	}

	return nil
}

// GetTezosTokenCIDsByAccountWithinBlockRange retrieves token CIDs for an account within a block range
// Handles pagination automatically by fetching all results within the range
func (e *executor) GetTezosTokenCIDsByAccountWithinBlockRange(ctx context.Context, address string, fromBlock, toBlock uint64) ([]domain.TokenCID, error) {
	var allTokenCIDs []domain.TokenCID
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
			allTokenCIDs = append(allTokenCIDs, tokenCID)
		}

		// If we got fewer results than the limit, we've reached the end
		if len(balances) < limit {
			break
		}

		offset += limit
	}

	return allTokenCIDs, nil
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

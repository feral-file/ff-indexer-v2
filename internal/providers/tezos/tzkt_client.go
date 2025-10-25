package tezos

import (
	"context"
	"fmt"
	"time"

	logger "github.com/bitmark-inc/autonomy-logger"
	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
)

const MAX_PAGE_SIZE = 10_000

// TzKTTokenBalance represents a token balance from the TzKT API
type TzKTTokenBalance struct {
	Account   TzKTAccount   `json:"account"`
	Token     TzKTTokenInfo `json:"token"`
	Balance   string        `json:"balance"`
	TokenID   string        `json:"tokenId"`
	LastLevel uint64        `json:"lastLevel"`
	LastTime  string        `json:"lastTime"`
}

// TzKTAccount represents an account address
type TzKTAccount struct {
	Address string `json:"address"`
}

// TzKTToken represents token information from the TzKT API
type TzKTToken struct {
	Contract TzKTContract           `json:"contract"`
	TokenID  string                 `json:"tokenId"`
	Standard string                 `json:"standard"`
	Metadata map[string]interface{} `json:"metadata"`
}

// TzKTContract represents a contract address
type TzKTContract struct {
	Address string `json:"address"`
}

// TzKTTokenTransfer represents a token transfer from the TzKT API
type TzKTTokenTransfer struct {
	ID            uint64        `json:"id"`
	Level         uint64        `json:"level"`
	Timestamp     string        `json:"timestamp"`
	Token         TzKTTokenInfo `json:"token"`
	From          *TzKTAccount  `json:"from"`
	To            *TzKTAccount  `json:"to"`
	Amount        string        `json:"amount"`
	TransactionID *uint64       `json:"transactionId"`
	OriginationID *uint64       `json:"originationId"`
	MigrationID   *uint64       `json:"migrationId"`
}

// TzKTTransaction represents a transaction from the TzKT API
type TzKTTransaction struct {
	ID        uint64            `json:"id"`
	Level     uint64            `json:"level"`
	Timestamp string            `json:"timestamp"`
	Hash      string            `json:"hash"`
	Diffs     []TzKTStorageDiff `json:"diffs"`
}

// TzKTStorageDiff represents a storage diff in a transaction
type TzKTStorageDiff struct {
	BigMap  uint64      `json:"bigmap"`
	Path    string      `json:"path"`
	Action  string      `json:"action"`
	Content interface{} `json:"content"`
}

// TzKTBigMap represents a big map from the TzKT API
type TzKTBigMap struct {
	Ptr    uint64   `json:"ptr"`
	Tags   []string `json:"tags"`
	Active bool     `json:"active"`
}

// TzKTBigMapUpdate represents a big map update event from TzKT
type TzKTBigMapUpdate struct {
	ID        uint64 `json:"id"`        // Update ID
	Level     uint64 `json:"level"`     // Block level
	Timestamp string `json:"timestamp"` // ISO timestamp

	BigMap uint64 `json:"bigmap"` // BigMap ID

	Contract struct {
		Address string `json:"address"`
	} `json:"contract"`

	Path string `json:"path"` // Path in contract storage (e.g., "token_metadata")

	Action string `json:"action"` // "add_key", "update_key", "remove_key"

	Content struct {
		Hash  string      `json:"hash"`
		Key   interface{} `json:"key"`   // Token ID (can be int or string)
		Value interface{} `json:"value"` // Metadata value
	} `json:"content"`

	TransactionID *uint64 `json:"transactionId,omitempty"`
}

// TzKTTokenInfo represents token info in a transfer response
type TzKTTokenInfo struct {
	ID       uint64               `json:"id"`
	Contract TzKTContract         `json:"contract"`
	TokenID  string               `json:"tokenId"`
	Standard domain.ChainStandard `json:"standard"`
	Metadata interface{}          `json:"metadata"`
}

// TzKTClient defines an interface for TzKT API client operations to enable mocking
//
//go:generate mockgen -source=tzkt_client.go -destination=../../mocks/tzkt_client.go -package=mocks -mock_names=TzKTClient=MockTzKTClient
type TzKTClient interface {
	// ParseTransfer converts a TzKT transfer to a standardized blockchain event
	ParseTransfer(ctx context.Context, transfer *TzKTTokenTransfer) (*domain.BlockchainEvent, error)

	// ParseBigMapUpdate converts a TzKT big map update to a standardized blockchain event
	ParseBigMapUpdate(ctx context.Context, update *TzKTBigMapUpdate) (*domain.BlockchainEvent, error)

	// GetTransactionsByID retrieves transactions by its TzKT operation ID (internal database ID)
	GetTransactionsByID(ctx context.Context, txID uint64) ([]TzKTTransaction, error)

	// GetTokenOwnerBalance retrieves token balance for a specific owner address
	GetTokenOwnerBalance(ctx context.Context, contractAddress string, tokenID string, ownerAddress string) (string, error)

	// GetTokenBalances retrieves token balances for a specific token
	GetTokenBalances(ctx context.Context, contractAddress string, tokenID string) ([]TzKTTokenBalance, error)

	// GetTokenMetadata retrieves token metadata for a specific token
	GetTokenMetadata(ctx context.Context, contractAddress string, tokenID string) (map[string]interface{}, error)

	// GetTokenTransfers retrieves all token transfers for a specific token
	GetTokenTransfers(ctx context.Context, contractAddress string, tokenID string) ([]TzKTTokenTransfer, error)

	// GetTokenMetadataUpdates retrieves all metadata update transactions for a specific token
	GetTokenMetadataUpdates(ctx context.Context, contractAddress string, tokenID string) ([]TzKTBigMapUpdate, error)

	// GetTokenEvents retrieves all token-related events (transfers + metadata updates) for a specific token
	GetTokenEvents(ctx context.Context, contractAddress string, tokenID string) ([]domain.BlockchainEvent, error)

	// GetTokenBalancesByAccount retrieves all token balances for a specific account address within an anchor block level and order
	GetTokenBalancesByAccount(ctx context.Context, accountAddress string, blkLevel *uint64, isDesc bool) ([]TzKTTokenBalance, error)

	// GetTokenBalancesByAccountWithinBlockRange retrieves token balances for an account within a block range
	GetTokenBalancesByAccountWithinBlockRange(ctx context.Context, accountAddress string, fromBlock, toBlock uint64, limit int, offset int) ([]TzKTTokenBalance, error)

	// GetLatestBlock retrieves the current latest block level from the TzKT API
	GetLatestBlock(ctx context.Context) (uint64, error)

	// GetContractDeployer retrieves the deployer address for a contract
	GetContractDeployer(ctx context.Context, contractAddress string) (string, error)

	// ChainID returns the chain ID for this client
	ChainID() domain.Chain
}

// tzktClient is the concrete implementation of TzKTClient
type tzktClient struct {
	chainID    domain.Chain
	baseURL    string
	httpClient adapter.HTTPClient
	clock      adapter.Clock
}

// NewTzKTClient creates a new TzKT API client
func NewTzKTClient(
	chainID domain.Chain,
	baseURL string,
	httpClient adapter.HTTPClient,
	clock adapter.Clock,
) TzKTClient {
	return &tzktClient{
		chainID:    chainID,
		baseURL:    baseURL,
		httpClient: httpClient,
		clock:      clock,
	}
}

// GetTransactionsByID retrieves transactions by its TzKT operation ID (internal database ID)
func (c *tzktClient) GetTransactionsByID(ctx context.Context, txID uint64) ([]TzKTTransaction, error) {
	url := fmt.Sprintf("%s/v1/operations/transactions?id=%d", c.baseURL, txID)

	var txs []TzKTTransaction
	if err := c.httpClient.Get(ctx, url, &txs); err != nil {
		return nil, fmt.Errorf("failed to get transaction %d: %w", txID, err)
	}

	return txs, nil
}

// GetTokenOwnerBalance retrieves token balance for a specific owner address
func (c *tzktClient) GetTokenOwnerBalance(ctx context.Context, contractAddress string, tokenID string, ownerAddress string) (string, error) {
	// Get all owners balance
	balances, err := c.GetTokenBalances(ctx, contractAddress, tokenID)
	if err != nil {
		return "", err
	}

	// Iterate over the balances array to get the owner balance
	for _, balance := range balances {
		if balance.Account.Address != ownerAddress {
			continue
		}

		return balance.Balance, nil
	}

	return "", nil
}

// GetTokenBalances retrieves token balances for a specific token
// Returns list of holders with their balances
func (c *tzktClient) GetTokenBalances(ctx context.Context, contractAddress string, tokenID string) ([]TzKTTokenBalance, error) {
	// TzKT API: GET /v1/tokens/balances?token.contract={address}&token.tokenId={id}&limit=10000
	url := fmt.Sprintf("%s/v1/tokens/balances?token.contract=%s&token.tokenId=%s&limit=10000", c.baseURL, contractAddress, tokenID)

	var balances []TzKTTokenBalance
	if err := c.httpClient.Get(ctx, url, &balances); err != nil {
		return nil, fmt.Errorf("failed to get token balances for %s/%s: %w", contractAddress, tokenID, err)
	}

	return balances, nil
}

// GetTokenMetadata retrieves token metadata for a specific token
func (c *tzktClient) GetTokenMetadata(ctx context.Context, contractAddress string, tokenID string) (map[string]interface{}, error) {
	// TzKT API: GET /v1/tokens?contract={address}&tokenId={id}
	url := fmt.Sprintf("%s/v1/tokens?contract=%s&tokenId=%s", c.baseURL, contractAddress, tokenID)

	var tokens []TzKTToken
	if err := c.httpClient.Get(ctx, url, &tokens); err != nil {
		return nil, fmt.Errorf("failed to get token metadata for %s/%s: %w", contractAddress, tokenID, err)
	}

	if len(tokens) == 0 {
		return nil, fmt.Errorf("token not found: %s/%s", contractAddress, tokenID)
	}

	return tokens[0].Metadata, nil
}

// GetTokenTransfers retrieves all token transfers for a specific token in ascending order of block number (level)
func (c *tzktClient) GetTokenTransfers(ctx context.Context, contractAddress string, tokenID string) ([]TzKTTokenTransfer, error) {
	// TzKT API: GET /v1/tokens/transfers?token.contract={address}&token.tokenId={id}&sort.{order}=level&limit={limit}
	// FIXME handle pagination
	url := fmt.Sprintf("%s/v1/tokens/transfers?token.contract=%s&token.tokenId=%s&sort.asc=level&limit=10000", c.baseURL, contractAddress, tokenID)

	var transfers []TzKTTokenTransfer
	if err := c.httpClient.Get(ctx, url, &transfers); err != nil {
		return nil, fmt.Errorf("failed to get token transfers for %s/%s: %w", contractAddress, tokenID, err)
	}

	return transfers, nil
}

// GetTokenMetadataUpdates retrieves all metadata update events for a specific token
func (c *tzktClient) GetTokenMetadataUpdates(ctx context.Context, contractAddress string, tokenID string) ([]TzKTBigMapUpdate, error) {
	// Step 1: Get the bigmap pointer for token_metadata
	bigMapsURL := fmt.Sprintf("%s/v1/contracts/%s/bigmaps?tags.any=token_metadata", c.baseURL, contractAddress)

	var bigMaps []TzKTBigMap
	if err := c.httpClient.Get(ctx, bigMapsURL, &bigMaps); err != nil {
		return nil, fmt.Errorf("failed to get contract bigmaps: %w", err)
	}

	if len(bigMaps) == 0 {
		// No token_metadata bigmap found, return empty list
		return []TzKTBigMapUpdate{}, nil
	}

	// Use the first active bigmap with token_metadata tag
	var tokenMetadataBigMapPtr uint64
	found := false
	for _, bm := range bigMaps {
		if bm.Active {
			tokenMetadataBigMapPtr = bm.Ptr
			found = true
			break
		}
	}

	if !found {
		return []TzKTBigMapUpdate{}, nil
	}

	// Step 2: Get updates for this bigmap with specified order and limit
	updatesURL := fmt.Sprintf("%s/v1/bigmaps/updates?bigmap=%d&sort.asc=id&limit=10000", c.baseURL, tokenMetadataBigMapPtr)

	var updates []TzKTBigMapUpdate
	if err := c.httpClient.Get(ctx, updatesURL, &updates); err != nil {
		return nil, fmt.Errorf("failed to get bigmap updates: %w", err)
	}

	// Step 3: Filter updates to only include token_metadata updates for this specific token
	var filteredUpdates []TzKTBigMapUpdate
	for _, update := range updates {
		if update.Path != "token_metadata" || fmt.Sprintf("%v", update.Content.Key) != tokenID {
			continue
		}

		filteredUpdates = append(filteredUpdates, update)
	}

	return filteredUpdates, nil
}

// hasTokenMetadataDiff checks if transaction diffs contain token_metadata updates
func (c *tzktClient) hasTokenMetadataDiff(diffs []TzKTStorageDiff) bool {
	for _, diff := range diffs {
		if diff.Path == "token_metadata" {
			return true
		}
	}
	return false
}

// GetTokenEvents retrieves all token-related events (transfers + metadata updates) for a specific token
func (c *tzktClient) GetTokenEvents(ctx context.Context, contractAddress string, tokenID string) ([]domain.BlockchainEvent, error) {
	events := make([]domain.BlockchainEvent, 0)

	// 1. Get all transfers
	transfers, err := c.GetTokenTransfers(ctx, contractAddress, tokenID)
	if err != nil {
		return nil, fmt.Errorf("failed to get token transfers: %w", err)
	}

	// Convert transfers to events
	for i := range transfers {
		transfer := transfers[i]

		event, err := c.ParseTransfer(ctx, &transfer)
		if err != nil {
			return nil, fmt.Errorf("failed to parse transfer: %w", err)
		}
		if event != nil {
			events = append(events, *event)
		}
	}

	// 2. Get all metadata update transactions
	metadataUpdates, err := c.GetTokenMetadataUpdates(ctx, contractAddress, tokenID)
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata updates: %w", err)
	}

	// Convert metadata updates to events
	for i := range metadataUpdates {
		event, err := c.ParseBigMapUpdate(ctx, &metadataUpdates[i])
		if err != nil {
			return nil, fmt.Errorf("failed to parse big map update: %w", err)
		}
		if event != nil {
			events = append(events, *event)
		}
	}

	return events, nil
}

// GetTokenBalancesByAccount retrieves all token balances for a specific account address and anchor block level and order
func (c *tzktClient) GetTokenBalancesByAccount(ctx context.Context, accountAddress string, blkLevel *uint64, isDesc bool) ([]TzKTTokenBalance, error) {
	// TzKT API: GET /v1/tokens/balances?account={address}&balance.ne=0&sort.{order}=lastLevel&limit=10000&lastLevel.lt={blkLevel}
	// This returns all tokens (with non-zero balance) owned by the specified account
	// sorted by lastLevel in descending order if isDesc is true, otherwise ascending
	url := fmt.Sprintf("%s/v1/tokens/balances?account=%s&balance.ne=0&limit=10000", c.baseURL, accountAddress)
	if blkLevel != nil {
		url += fmt.Sprintf("&lastLevel.lt=%d", *blkLevel)
	}
	if isDesc {
		url += "&sort.desc=lastLevel"
	} else {
		url += "&sort.asc=lastLevel"
	}

	var balances []TzKTTokenBalance
	if err := c.httpClient.Get(ctx, url, &balances); err != nil {
		return nil, fmt.Errorf("failed to get token balances for account %s: %w", accountAddress, err)
	}

	return balances, nil
}

// GetTokenBalancesByAccountWithinBlockRange retrieves token balances for an account within a block range
// This is used for chunk-based sweeping, supporting pagination with offset
func (c *tzktClient) GetTokenBalancesByAccountWithinBlockRange(ctx context.Context, accountAddress string, fromBlock, toBlock uint64, limit int, offset int) ([]TzKTTokenBalance, error) {
	// TzKT API: GET /v1/tokens/balances?account={address}&balance.ne=0&lastLevel.ge={fromBlock}&lastLevel.le={toBlock}&sort.asc=lastLevel&limit={limit}&offset={offset}
	url := fmt.Sprintf("%s/v1/tokens/balances?account=%s&balance.ne=0&lastLevel.ge=%d&lastLevel.le=%d&sort.asc=lastLevel&limit=%d&offset=%d",
		c.baseURL, accountAddress, fromBlock, toBlock, limit, offset)

	var balances []TzKTTokenBalance
	if err := c.httpClient.Get(ctx, url, &balances); err != nil {
		return nil, fmt.Errorf("failed to get token balances for account %s within block range [%d, %d]: %w", accountAddress, fromBlock, toBlock, err)
	}

	return balances, nil
}

// ChainID returns the chain ID for this client
func (c *tzktClient) ChainID() domain.Chain {
	return c.chainID
}

// parseTransfer converts a TzKT transfer to a standardized blockchain event
func (c *tzktClient) ParseTransfer(ctx context.Context, transfer *TzKTTokenTransfer) (*domain.BlockchainEvent, error) {
	// Parse timestamp
	timestamp, err := c.clock.Parse(time.RFC3339, transfer.Timestamp)
	if err != nil {
		timestamp = c.clock.Now()
	}

	// Determine addresses
	fromAddress := ""
	if transfer.From != nil {
		fromAddress = transfer.From.Address
	}

	toAddress := ""
	if transfer.To != nil {
		toAddress = transfer.To.Address
	}

	// Determine event type
	eventType := domain.TransferEventType(&fromAddress, &toAddress)

	// Fetch actual transaction hash
	var txHash string
	if transfer.TransactionID != nil {
		txs, err := c.GetTransactionsByID(ctx, *transfer.TransactionID)
		if err != nil {
			logger.Error(fmt.Errorf("failed to get transaction hash for ID %d: %w", *transfer.TransactionID, err))
			return nil, fmt.Errorf("failed to get transaction hash: %w", err)
		}
		if len(txs) > 0 {
			txHash = txs[0].Hash
		}
	} else {
		// FIXME figure out how to get the transaction hash
		logger.Warn("no transaction ID found for transfer", zap.Any("transfer", transfer))
	}

	event := &domain.BlockchainEvent{
		Chain:           c.chainID,
		Standard:        transfer.Token.Standard,
		ContractAddress: transfer.Token.Contract.Address,
		TokenNumber:     transfer.Token.TokenID,
		EventType:       eventType,
		FromAddress:     &fromAddress,
		ToAddress:       &toAddress,
		Quantity:        transfer.Amount,
		TxHash:          txHash,
		BlockNumber:     transfer.Level,
		BlockHash:       nil, // Block hash is optional for Tezos
		Timestamp:       timestamp,
		TxIndex:         transfer.ID, // Use transfer ID as log index
	}

	return event, nil
}

// parseBigMapUpdate converts a TzKT big map update to a standardized blockchain event
func (c *tzktClient) ParseBigMapUpdate(ctx context.Context, update *TzKTBigMapUpdate) (*domain.BlockchainEvent, error) {
	// Parse timestamp
	timestamp, err := c.clock.Parse(time.RFC3339, update.Timestamp)
	if err != nil {
		timestamp = c.clock.Now()
	}

	// Extract token ID from the key
	tokenID := fmt.Sprintf("%v", update.Content.Key)

	// Fetch actual transaction hash from TzKT API (if available)
	var txHash string
	if update.TransactionID != nil {
		txs, err := c.GetTransactionsByID(ctx, *update.TransactionID)
		if err != nil {
			logger.Error(fmt.Errorf("failed to get transaction hash for ID %d: %w", *update.TransactionID, err))
			return nil, fmt.Errorf("failed to get transaction hash: %w", err)
		}
		if len(txs) > 0 {
			txHash = txs[0].Hash
		}
	} else {
		tx, err := c.getTransactionFromBitmapUpdate(ctx, update)
		if err != nil {
			return nil, fmt.Errorf("failed to get transaction hash: %w", err)
		}
		if tx != nil {
			txHash = tx.Hash
		}
	}

	event := &domain.BlockchainEvent{
		Chain:           c.chainID,
		Standard:        domain.StandardFA2, // BigMap updates are for FA2 tokens on Tezos
		ContractAddress: update.Contract.Address,
		TokenNumber:     tokenID,
		EventType:       domain.EventTypeMetadataUpdate,
		FromAddress:     nil,
		ToAddress:       nil,
		Quantity:        "1",
		TxHash:          txHash,
		BlockNumber:     update.Level,
		BlockHash:       nil, // Block hash is optional for Tezos
		Timestamp:       timestamp,
		TxIndex:         update.ID,
	}

	return event, nil
}

// getTransactionFromBitmapUpdate gets the transaction from the bitmap update
func (c *tzktClient) getTransactionFromBitmapUpdate(ctx context.Context, updates *TzKTBigMapUpdate) (*TzKTTransaction, error) {
	txURL := fmt.Sprintf("%s/v1/operations/transactions?target=%s&level.in=%d&select=id,hash,level,timestamp,diffs&limit=10000",
		c.baseURL, updates.Contract.Address, updates.Level)

	var transactions []TzKTTransaction
	if err := c.httpClient.Get(ctx, txURL, &transactions); err != nil {
		return nil, fmt.Errorf("failed to get transactions: %w", err)
	}

	// Filter transactions that have diffs with path = "token_metadata"
	var result *TzKTTransaction
	for i := range transactions {
		tx := &transactions[i]
		if c.hasTokenMetadataDiff(tx.Diffs) {
			result = tx
			break
		}
	}

	return result, nil
}

// GetLatestBlock retrieves the current latest block level from the TzKT API
func (c *tzktClient) GetLatestBlock(ctx context.Context) (uint64, error) {
	// TzKT API: GET /v1/head
	// Returns the current head block information
	url := fmt.Sprintf("%s/v1/head", c.baseURL)

	var head struct {
		Level uint64 `json:"level"`
	}

	if err := c.httpClient.Get(ctx, url, &head); err != nil {
		return 0, fmt.Errorf("failed to get latest block from TzKT: %w", err)
	}

	return head.Level, nil
}

// TzKTOrigination represents an origination operation from the TzKT API
type TzKTOrigination struct {
	Hash   string `json:"hash"`
	Level  uint64 `json:"level"`
	Sender struct {
		Address string `json:"address"`
	} `json:"sender"`
	Initiator *struct {
		Address string `json:"address"`
	} `json:"initiator"`
	OriginatedContract struct {
		Address string `json:"address"`
	} `json:"originatedContract"`
}

// GetContractDeployer retrieves the deployer address for a contract
func (c *tzktClient) GetContractDeployer(ctx context.Context, contractAddress string) (string, error) {
	// TzKT API: GET /v1/operations/originations?originatedContract={address}&limit=1
	// This returns the origination operation that created the contract
	// We need to get both sender and initiator fields to handle factory contract patterns
	url := fmt.Sprintf("%s/v1/operations/originations?originatedContract=%s&limit=1", c.baseURL, contractAddress)

	var originations []TzKTOrigination
	if err := c.httpClient.Get(ctx, url, &originations); err != nil {
		return "", fmt.Errorf("failed to get contract origination for %s: %w", contractAddress, err)
	}

	if len(originations) == 0 {
		return "", fmt.Errorf("no origination found for contract %s", contractAddress)
	}

	origination := originations[0]

	// If initiator exists and is not null, it's the actual deployer (factory contract pattern)
	// Otherwise, sender is the deployer (direct origination)
	if origination.Initiator != nil && origination.Initiator.Address != "" {
		return origination.Initiator.Address, nil
	}

	return origination.Sender.Address, nil
}

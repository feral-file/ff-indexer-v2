package helpers

import (
	"context"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"go.uber.org/zap"

	ethadapter "github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/block"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
)

var (
	erc721TokenURIABI = mustParseABI(`[{"constant":true,"inputs":[{"name":"tokenId","type":"uint256"}],"name":"tokenURI","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"}]`)
	erc721OwnerOfABI  = mustParseABI(`[{"constant":true,"inputs":[{"name":"tokenId","type":"uint256"}],"name":"ownerOf","outputs":[{"name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"}]`)
	erc1155URIABI     = mustParseABI(`[{"constant":true,"inputs":[{"name":"id","type":"uint256"}],"name":"uri","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"}]`)
	erc1155BalanceABI = mustParseABI(`[{"constant":true,"inputs":[{"name":"account","type":"address"},{"name":"id","type":"uint256"}],"name":"balanceOf","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"}]`)
	erc1155BatchABI   = mustParseABI(`[{"constant":true,"inputs":[{"name":"accounts","type":"address[]"},{"name":"ids","type":"uint256[]"}],"name":"balanceOfBatch","outputs":[{"name":"","type":"uint256[]"}],"payable":false,"stateMutability":"view","type":"function"}]`)
)

func mustParseABI(raw string) abi.ABI {
	parsed, err := abi.JSON(strings.NewReader(raw))
	if err != nil {
		panic(fmt.Sprintf("parse embedded ABI: %v", err))
	}
	return parsed
}

func parseTokenID(tokenNumber string) (*big.Int, error) {
	tokenID, ok := new(big.Int).SetString(tokenNumber, 10)
	if !ok {
		return nil, fmt.Errorf("invalid token number: %s", tokenNumber)
	}
	return tokenID, nil
}

// ERC721TokenURI fetches the tokenURI from an ERC721 contract.
func ERC721TokenURI(ctx context.Context, ethClient ethadapter.EthClient, contractAddress, tokenNumber string) (string, error) {
	tokenID, err := parseTokenID(tokenNumber)
	if err != nil {
		return "", err
	}

	data, err := erc721TokenURIABI.Pack("tokenURI", tokenID)
	if err != nil {
		return "", fmt.Errorf("failed to pack data: %w", err)
	}

	contractAddr := common.HexToAddress(contractAddress)
	result, err := ethClient.CallContract(ctx, ethereum.CallMsg{
		To:   &contractAddr,
		Data: data,
	}, nil)
	if err != nil {
		return "", fmt.Errorf("failed to call contract: %w", err)
	}
	if len(result) == 0 {
		return "", ErrContractNotFound
	}

	var uri string
	if err := erc721TokenURIABI.UnpackIntoInterface(&uri, "tokenURI", result); err != nil {
		return "", fmt.Errorf("failed to unpack result: %w", err)
	}

	return uri, nil
}

// ERC721OwnerOf fetches the current owner of an ERC721 token.
func ERC721OwnerOf(ctx context.Context, ethClient ethadapter.EthClient, contractAddress, tokenNumber string) (string, error) {
	tokenID, err := parseTokenID(tokenNumber)
	if err != nil {
		return "", err
	}

	data, err := erc721OwnerOfABI.Pack("ownerOf", tokenID)
	if err != nil {
		return "", fmt.Errorf("failed to pack data: %w", err)
	}

	contractAddr := common.HexToAddress(contractAddress)
	result, err := ethClient.CallContract(ctx, ethereum.CallMsg{
		To:   &contractAddr,
		Data: data,
	}, nil)
	if err != nil {
		return "", fmt.Errorf("failed to call contract: %w", err)
	}
	if len(result) == 0 {
		return "", ErrContractNotFound
	}

	var owner common.Address
	if err := erc721OwnerOfABI.UnpackIntoInterface(&owner, "ownerOf", result); err != nil {
		return "", fmt.Errorf("failed to unpack result: %w", err)
	}

	return owner.Hex(), nil
}

// ERC1155URI fetches the uri from an ERC1155 contract.
func ERC1155URI(ctx context.Context, ethClient ethadapter.EthClient, contractAddress, tokenNumber string) (string, error) {
	tokenID, err := parseTokenID(tokenNumber)
	if err != nil {
		return "", err
	}

	data, err := erc1155URIABI.Pack("uri", tokenID)
	if err != nil {
		return "", fmt.Errorf("failed to pack data: %w", err)
	}

	contractAddr := common.HexToAddress(contractAddress)
	result, err := ethClient.CallContract(ctx, ethereum.CallMsg{
		To:   &contractAddr,
		Data: data,
	}, nil)
	if err != nil {
		return "", fmt.Errorf("failed to call contract: %w", err)
	}
	if len(result) == 0 {
		return "", ErrContractNotFound
	}

	var uri string
	if err := erc1155URIABI.UnpackIntoInterface(&uri, "uri", result); err != nil {
		return "", fmt.Errorf("failed to unpack result: %w", err)
	}

	return uri, nil
}

// ERC1155BalanceOf fetches the balance of a specific token ID for an owner.
func ERC1155BalanceOf(ctx context.Context, ethClient ethadapter.EthClient, contractAddress, ownerAddress, tokenNumber string) (string, error) {
	tokenID, err := parseTokenID(tokenNumber)
	if err != nil {
		return "", err
	}

	owner := common.HexToAddress(ownerAddress)
	data, err := erc1155BalanceABI.Pack("balanceOf", owner, tokenID)
	if err != nil {
		return "", fmt.Errorf("failed to pack data: %w", err)
	}

	contractAddr := common.HexToAddress(contractAddress)
	result, err := ethClient.CallContract(ctx, ethereum.CallMsg{
		To:   &contractAddr,
		Data: data,
	}, nil)
	if err != nil {
		return "", fmt.Errorf("failed to call contract: %w", err)
	}
	if len(result) == 0 {
		return "", ErrContractNotFound
	}

	var balance *big.Int
	if err := erc1155BalanceABI.UnpackIntoInterface(&balance, "balanceOf", result); err != nil {
		return "", fmt.Errorf("failed to unpack result: %w", err)
	}

	return balance.String(), nil
}

// ERC1155BalanceOfBatch fetches balances for multiple addresses for a specific token ID.
func ERC1155BalanceOfBatch(ctx context.Context, ethClient ethadapter.EthClient, contractAddress, tokenNumber string, addresses []string) (map[string]string, error) {
	tokenID, err := parseTokenID(tokenNumber)
	if err != nil {
		return nil, err
	}

	contractAddr := common.HexToAddress(contractAddress)
	result := make(map[string]string)

	const chunkSize = 200
	for i := 0; i < len(addresses); i += chunkSize {
		end := min(i+chunkSize, len(addresses))
		chunk := addresses[i:end]

		accountAddresses := make([]common.Address, len(chunk))
		tokenIDs := make([]*big.Int, len(chunk))
		for j, addr := range chunk {
			accountAddresses[j] = common.HexToAddress(addr)
			tokenIDs[j] = tokenID
		}

		data, err := erc1155BatchABI.Pack("balanceOfBatch", accountAddresses, tokenIDs)
		if err != nil {
			return nil, fmt.Errorf("failed to pack balanceOfBatch data: %w", err)
		}

		callResult, err := ethClient.CallContract(ctx, ethereum.CallMsg{
			To:   &contractAddr,
			Data: data,
		}, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to call balanceOfBatch for chunk %d-%d: %w", i, end, err)
		}
		if len(callResult) == 0 {
			return nil, ErrContractNotFound
		}

		var balances []*big.Int
		if err := erc1155BatchABI.UnpackIntoInterface(&balances, "balanceOfBatch", callResult); err != nil {
			return nil, fmt.Errorf("failed to unpack balanceOfBatch result: %w", err)
		}
		if len(balances) != len(chunk) {
			return nil, fmt.Errorf("balanceOfBatch returned unexpected number of results: expected %d, got %d", len(chunk), len(balances))
		}

		for j, balance := range balances {
			if balance.Cmp(big.NewInt(0)) > 0 {
				result[chunk[j]] = balance.String()
			}
		}
	}

	return result, nil
}

// ERC1155TokenExists checks ERC1155 token existence via recent transfer scan and balance checks.
func ERC1155TokenExists(
	ctx context.Context,
	ethClient ethadapter.EthClient,
	pagination *PaginationHelper,
	blockProvider block.BlockProvider,
	contractAddress, tokenNumber string,
) (bool, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	tokenID, err := parseTokenID(tokenNumber)
	if err != nil {
		return false, err
	}

	contractAddr := common.HexToAddress(contractAddress)
	zeroAddress := common.HexToAddress(domain.ETHEREUM_ZERO_ADDRESS)

	latestBlock, err := blockProvider.GetLatestBlock(timeoutCtx)
	if err != nil {
		return false, fmt.Errorf("failed to get latest block: %w", err)
	}

	const maxBlockThreshold = uint64(10_000_000)
	var fromBlock uint64
	if latestBlock > maxBlockThreshold {
		fromBlock = latestBlock - maxBlockThreshold
	}

	logger.InfoCtx(timeoutCtx, "Checking ERC1155 token existence via recent transfers",
		zap.String("contract", contractAddress),
		zap.String("tokenNumber", tokenNumber),
		zap.Uint64("fromBlock", fromBlock),
		zap.Uint64("toBlock", latestBlock),
	)

	query := ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(fromBlock),
		ToBlock:   new(big.Int).SetUint64(latestBlock),
		Addresses: []common.Address{contractAddr},
		Topics: [][]common.Hash{
			{ERC1155TransferSingleEventSignature},
		},
	}

	logs, err := pagination.FilterLogsWithPagination(timeoutCtx, query)
	if err != nil && timeoutCtx.Err() != context.DeadlineExceeded {
		return false, fmt.Errorf("failed to fetch transfer logs: %w", err)
	}

	const maxRecipientsToCheck = 5
	type recipientInfo struct {
		address     common.Address
		blockNumber uint64
		logIndex    uint
	}
	var recentRecipients []recipientInfo
	seenRecipients := make(map[common.Address]bool)

	for _, vLog := range logs {
		if len(vLog.Topics) != 4 || len(vLog.Data) < 64 {
			continue
		}

		logTokenID := new(big.Int).SetBytes(vLog.Data[0:32])
		if logTokenID.Cmp(tokenID) != 0 {
			continue
		}

		toAddr := common.BytesToAddress(vLog.Topics[3].Bytes())
		if toAddr == zeroAddress {
			continue
		}

		if !seenRecipients[toAddr] {
			recentRecipients = append(recentRecipients, recipientInfo{
				address:     toAddr,
				blockNumber: vLog.BlockNumber,
				logIndex:    vLog.Index,
			})
			seenRecipients[toAddr] = true
		}
	}

	if len(recentRecipients) == 0 {
		logger.InfoCtx(timeoutCtx, "No recent ERC1155 transfers found in scan window, assuming token doesn't exist",
			zap.String("contract", contractAddress),
			zap.String("tokenNumber", tokenNumber),
			zap.Uint64("scannedBlocks", latestBlock-fromBlock),
		)
		return false, nil
	}

	for i := 0; i < len(recentRecipients)-1; i++ {
		for j := i + 1; j < len(recentRecipients); j++ {
			if recentRecipients[j].blockNumber > recentRecipients[i].blockNumber ||
				(recentRecipients[j].blockNumber == recentRecipients[i].blockNumber &&
					recentRecipients[j].logIndex > recentRecipients[i].logIndex) {
				recentRecipients[i], recentRecipients[j] = recentRecipients[j], recentRecipients[i]
			}
		}
	}

	if len(recentRecipients) > maxRecipientsToCheck {
		recentRecipients = recentRecipients[:maxRecipientsToCheck]
	}

	for i, recipient := range recentRecipients {
		balanceStr, err := ERC1155BalanceOf(timeoutCtx, ethClient, contractAddress, recipient.address.Hex(), tokenNumber)
		if err != nil {
			logger.WarnCtx(timeoutCtx, "Failed to check balance for recipient, trying next",
				zap.String("recipient", recipient.address.Hex()),
				zap.Int("recipientIndex", i),
				zap.Error(err),
			)
			continue
		}

		balance, ok := new(big.Int).SetString(balanceStr, 10)
		if !ok {
			continue
		}

		if balance.Cmp(big.NewInt(0)) > 0 {
			return true, nil
		}
	}

	return false, nil
}

// ParseERC721TransferLog parses an ERC721 Transfer event log into base, filling semantic fields.
// base must already contain log-derived metadata from helpers.BaseEventFromLog.
func ParseERC721TransferLog(vLog types.Log, base domain.BlockchainEvent) (*domain.BlockchainEvent, error) {
	if len(vLog.Topics) == 3 {
		return nil, nil
	}
	if len(vLog.Topics) != 4 {
		return nil, fmt.Errorf("invalid Transfer event: expected 3 or 4 topics, got %d", len(vLog.Topics))
	}

	event := base
	event.Standard = domain.StandardERC721
	fromAddress := common.BytesToAddress(vLog.Topics[1].Bytes()).Hex()
	event.FromAddress = &fromAddress
	toAddress := common.BytesToAddress(vLog.Topics[2].Bytes()).Hex()
	event.ToAddress = &toAddress
	event.TokenNumber = new(big.Int).SetBytes(vLog.Topics[3].Bytes()).String()
	event.Quantity = "1"
	event.EventType = domain.TransferEventType(event.FromAddress, event.ToAddress)
	return &event, nil
}

// ParseERC1155TransferBatch parses a TransferBatch event and extracts transfers for a specific token ID.
// It decodes ABI-encoded dynamic arrays (ids[] and values[]) from the event log data and constructs
// BlockchainEvent structures for all transfers matching the specified tokenID.
//
// Parameters:
//   - ctx: context for cancellation and logging
//   - chainID: blockchain identifier to tag events
//   - vLog: raw Ethereum event log from TransferBatch event
//   - tokenID: filter for specific token ID (only matching transfers are returned)
//   - blockProvider: cached block metadata provider for timestamp lookup
//   - fallbackNow: function providing current time if timestamp lookup fails
//
// Returns a slice of BlockchainEvent structs representing individual token transfers within the batch.
func ParseERC1155TransferBatch(
	ctx context.Context,
	chainID domain.Chain,
	vLog types.Log,
	tokenID *big.Int,
	blockProvider block.BlockProvider,
	fallbackNow func() time.Time,
) ([]domain.BlockchainEvent, error) {
	// TransferBatch(address indexed operator, address indexed from, address indexed to, uint256[] ids, uint256[] values)
	// Data contains ABI-encoded arrays: ids[] and values[]

	if len(vLog.Topics) != 4 {
		return nil, fmt.Errorf("invalid TransferBatch event: expected 4 topics, got %d", len(vLog.Topics))
	}

	// Parse addresses from topics
	fromAddress := common.BytesToAddress(vLog.Topics[2].Bytes()).Hex()
	toAddress := common.BytesToAddress(vLog.Topics[3].Bytes()).Hex()

	// Decode the dynamic arrays from Data field
	// The data contains two dynamic arrays: uint256[] ids, uint256[] values
	// ABI encoding for dynamic arrays:
	// [0:32]   offset to ids array
	// [32:64]  offset to values array
	// [64:96]  length of ids array
	// [96:...] ids array elements (32 bytes each)
	// [...:..] length of values array
	// [...:..] values array elements (32 bytes each)

	if len(vLog.Data) < 128 {
		return nil, fmt.Errorf("insufficient data for TransferBatch event: got %d bytes", len(vLog.Data))
	}

	// Parse ids array
	idsOffset := new(big.Int).SetBytes(vLog.Data[0:32]).Uint64()
	if idsOffset+32 > uint64(len(vLog.Data)) {
		return nil, fmt.Errorf("invalid ids offset: %d", idsOffset)
	}

	idsLength := new(big.Int).SetBytes(vLog.Data[idsOffset : idsOffset+32]).Uint64()
	if idsOffset+32+idsLength*32 > uint64(len(vLog.Data)) {
		return nil, fmt.Errorf("invalid ids array length: %d", idsLength)
	}

	// Parse values array
	valuesOffset := new(big.Int).SetBytes(vLog.Data[32:64]).Uint64()
	if valuesOffset+32 > uint64(len(vLog.Data)) {
		return nil, fmt.Errorf("invalid values offset: %d", valuesOffset)
	}

	valuesLength := new(big.Int).SetBytes(vLog.Data[valuesOffset : valuesOffset+32]).Uint64()
	if valuesOffset+32+valuesLength*32 > uint64(len(vLog.Data)) {
		return nil, fmt.Errorf("invalid values array length: %d", valuesLength)
	}

	if idsLength != valuesLength {
		return nil, fmt.Errorf("ids and values array length mismatch: %d != %d", idsLength, valuesLength)
	}

	// Extract events for matching token IDs
	events := make([]domain.BlockchainEvent, 0)

	for i := range idsLength {
		idStart := idsOffset + 32 + i*32
		idEnd := idStart + 32
		id := new(big.Int).SetBytes(vLog.Data[idStart:idEnd])

		// Only process if this is the token we're looking for
		if id.Cmp(tokenID) != 0 {
			continue
		}

		valueStart := valuesOffset + 32 + i*32
		valueEnd := valueStart + 32
		value := new(big.Int).SetBytes(vLog.Data[valueStart:valueEnd])

		// Get block timestamp
		blockTimestamp, err := blockProvider.GetBlockTimestamp(ctx, vLog.BlockNumber)
		if err != nil {
			logger.WarnCtx(ctx, "Failed to get block timestamp, using current time",
				zap.Error(err),
				zap.Uint64("blockNumber", vLog.BlockNumber))
			blockTimestamp = fallbackNow()
		}

		blockHash := vLog.BlockHash.Hex()

		// Create event
		event := domain.BlockchainEvent{
			Chain:           chainID,
			ContractAddress: vLog.Address.Hex(),
			Standard:        domain.StandardERC1155,
			TokenNumber:     id.String(),
			FromAddress:     &fromAddress,
			ToAddress:       &toAddress,
			Quantity:        value.String(),
			TxHash:          vLog.TxHash.Hex(),
			BlockNumber:     vLog.BlockNumber,
			BlockHash:       &blockHash,
			TxIndex:         uint64(vLog.TxIndex), //nolint:gosec,G115
			LogIndex:        uint64(vLog.Index),   //nolint:gosec,G115
			Timestamp:       blockTimestamp,
		}
		event.EventType = domain.TransferEventType(event.FromAddress, event.ToAddress)

		events = append(events, event)
	}

	return events, nil
}

// ERC1155ReplayBalances calculates all current ERC1155 token balances by replaying transfer events.
// It scans backward from the latest block with a maximum threshold of 10M blocks.
// If the operation times out (30 seconds), it returns partial balances gracefully.
//
// Reason: This function provides a way to calculate all holders of an ERC1155 token by replaying
// historical transfer events, which is necessary because ERC1155 doesn't provide an enumeration
// mechanism for token holders.
//
// Trade-offs:
// - Scanning only recent 10M blocks may miss historical transfers, resulting in incomplete balances
// - Fetching all contract events and filtering client-side is inefficient (can't filter by token ID at RPC level)
// - For contracts with millions of events, this can still hit Infura's 10k log limitation repeatedly
//
// Constraints:
// - 30-second timeout to prevent indefinite blocking
// - Returns partial results if timeout occurs (better than complete failure)
// - Only processes TransferSingle events (TransferBatch support is TODO)
func ERC1155ReplayBalances(
	ctx context.Context,
	pagination *PaginationHelper,
	blockProvider block.BlockProvider,
	contractAddress, tokenNumber string,
) (map[string]string, error) {
	// Create a timeout context (30 seconds) to prevent indefinite blocking
	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Parse token number to big.Int
	tokenID, ok := new(big.Int).SetString(tokenNumber, 10)
	if !ok {
		return nil, fmt.Errorf("invalid token number: %s", tokenNumber)
	}

	contractAddr := common.HexToAddress(contractAddress)

	// Get the latest block number using cached provider
	latestBlock, err := blockProvider.GetLatestBlock(timeoutCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest block: %w", err)
	}

	// Calculate fromBlock: scan backward from latest block with max 10M blocks threshold
	const maxBlockThreshold = uint64(10_000_000)
	var fromBlock uint64
	if latestBlock > maxBlockThreshold {
		fromBlock = latestBlock - maxBlockThreshold
	} else {
		fromBlock = 0
	}

	logger.InfoCtx(ctx, "Fetching ERC1155 balances",
		zap.String("contract", contractAddress),
		zap.String("tokenNumber", tokenNumber),
		zap.Uint64("fromBlock", fromBlock),
		zap.Uint64("toBlock", latestBlock),
		zap.Uint64("blockRange", latestBlock-fromBlock),
	)

	// For ERC1155, token ID is in data, not topics, so we fetch all TransferSingle events for this contract
	// and filter by token ID later
	query := ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(fromBlock),
		ToBlock:   new(big.Int).SetUint64(latestBlock),
		Addresses: []common.Address{contractAddr},
		Topics: [][]common.Hash{
			{
				ERC1155TransferSingleEventSignature,
				//ERC1155TransferBatchEventSignature, // FIXME: Handle batch transfers properly
			},
		},
	}

	// Fetch logs with pagination to handle Infura's 10k limitation
	// If timeout occurs, we'll get partial logs
	logs, err := pagination.FilterLogsWithPagination(timeoutCtx, query)
	if err != nil {
		// If context deadline exceeded, return partial balances with warning
		if timeoutCtx.Err() == context.DeadlineExceeded {
			logger.WarnCtx(timeoutCtx, "ERC1155 balance fetch timed out, returning partial balances",
				zap.String("contract", contractAddress),
				zap.String("tokenNumber", tokenNumber),
				zap.Int("partialLogsCount", len(logs)),
				zap.Uint64("fromBlock", fromBlock),
				zap.Uint64("toBlock", latestBlock),
			)
			// Continue with partial logs - better than nothing
		} else {
			return nil, fmt.Errorf("failed to filter logs: %w", err)
		}
	}

	// Build balances map by replaying transfer logs
	balances := make(map[string]*big.Int)

	for _, vLog := range logs {
		// ERC1155 TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value)
		if len(vLog.Topics) != 4 {
			logger.WarnCtx(timeoutCtx, "Invalid ERC1155 TransferSingle event: unexpected topic count",
				zap.Int("topics", len(vLog.Topics)),
				zap.String("txHash", vLog.TxHash.Hex()))
			continue
		}
		if len(vLog.Data) < 64 {
			logger.WarnCtx(timeoutCtx, "Invalid ERC1155 TransferSingle event: insufficient data",
				zap.String("txHash", vLog.TxHash.Hex()))
			continue
		}

		// Parse data: first 32 bytes = token ID, next 32 bytes = value
		logTokenID := new(big.Int).SetBytes(vLog.Data[0:32])
		// Filter by token ID
		if logTokenID.Cmp(tokenID) != 0 {
			continue
		}

		fromAddr := common.BytesToAddress(vLog.Topics[2].Bytes()).Hex()
		toAddr := common.BytesToAddress(vLog.Topics[3].Bytes()).Hex()
		quantity := new(big.Int).SetBytes(vLog.Data[32:64])

		// Process transfer: subtract from 'from' address, add to 'to' address
		if fromAddr != "" && fromAddr != domain.ETHEREUM_ZERO_ADDRESS {
			if balances[fromAddr] == nil {
				balances[fromAddr] = big.NewInt(0)
			}
			balances[fromAddr] = new(big.Int).Sub(balances[fromAddr], quantity)
		}

		if toAddr != "" && toAddr != domain.ETHEREUM_ZERO_ADDRESS {
			if balances[toAddr] == nil {
				balances[toAddr] = big.NewInt(0)
			}
			balances[toAddr] = new(big.Int).Add(balances[toAddr], quantity)
		}
	}

	// Convert to string map and filter out zero/negative balances
	result := make(map[string]string)
	for addr, balance := range balances {
		if balance.Cmp(big.NewInt(0)) > 0 {
			result[addr] = balance.String()
		}
	}

	return result, nil
}

// ERC1155BalanceAndEventsForOwner fetches the current balance and transfer events for a specific ERC1155 token and owner.
// This is optimized for owner-specific indexing by filtering events where owner is sender or receiver.
// Supports both TransferSingle and TransferBatch events.
//
// Reason: Provides complete ownership history for an ERC1155 token holder, which is essential for
// provenance tracking and ownership verification.
//
// Trade-offs:
// - Scans from genesis for complete history, which can be slow for long-lived contracts
// - Executes 4 parallel queries (sender/receiver × TransferSingle/TransferBatch) for efficiency
// - Uses both on-chain balance call (for current state) and event replay (for history)
//
// Constraints:
// - Requires accurate block timestamps via blockProvider
// - Falls back to current time if timestamp lookup fails
func ERC1155BalanceAndEventsForOwner(
	ctx context.Context,
	ethClient ethadapter.EthClient,
	pagination *PaginationHelper,
	blockProvider block.BlockProvider,
	chainID domain.Chain,
	fallbackNow func() time.Time,
	contractAddress, tokenNumber, ownerAddress string,
) (string, []domain.BlockchainEvent, error) {
	// Parse token number to big.Int
	tokenID, ok := new(big.Int).SetString(tokenNumber, 10)
	if !ok {
		return "", nil, fmt.Errorf("invalid token number: %s", tokenNumber)
	}

	contractAddr := common.HexToAddress(contractAddress)
	owner := common.HexToAddress(ownerAddress)
	ownerHash := common.BytesToHash(owner.Bytes())

	// Step 1: Get current balance via contract call (most reliable for current state)
	balance, err := ERC1155BalanceOf(ctx, ethClient, contractAddress, ownerAddress, tokenNumber)
	if err != nil {
		return "", nil, fmt.Errorf("failed to get ERC1155 balance: %w", err)
	}

	// Step 2: Get the latest block number
	latestBlock, err := blockProvider.GetLatestBlock(ctx)
	if err != nil {
		return "", nil, fmt.Errorf("failed to get latest block: %w", err)
	}

	// Calculate fromBlock: scan from genesis (we want complete history for this owner)
	// Note: We could optimize by scanning from contract deployment block if needed
	fromBlock := uint64(0)

	logger.InfoCtx(ctx, "Fetching ERC1155 events for owner",
		zap.String("contract", contractAddress),
		zap.String("tokenNumber", tokenNumber),
		zap.String("owner", ownerAddress),
		zap.Uint64("fromBlock", fromBlock),
		zap.Uint64("toBlock", latestBlock),
	)

	// Step 3: Define queries to fetch TransferSingle and TransferBatch events involving this owner
	queries := []ethereum.FilterQuery{
		// TransferSingle where owner is `from` (topic[2])
		{
			FromBlock: new(big.Int).SetUint64(fromBlock),
			ToBlock:   new(big.Int).SetUint64(latestBlock),
			Addresses: []common.Address{contractAddr},
			Topics: [][]common.Hash{
				{ERC1155TransferSingleEventSignature},
				nil,         // any operator
				{ownerHash}, // from address
			},
		},
		// TransferSingle where owner is `to` (topic[3])
		{
			FromBlock: new(big.Int).SetUint64(fromBlock),
			ToBlock:   new(big.Int).SetUint64(latestBlock),
			Addresses: []common.Address{contractAddr},
			Topics: [][]common.Hash{
				{ERC1155TransferSingleEventSignature},
				nil,         // any operator
				nil,         // any from address
				{ownerHash}, // to address
			},
		},
		// TransferBatch where owner is `from` (topic[2])
		{
			FromBlock: new(big.Int).SetUint64(fromBlock),
			ToBlock:   new(big.Int).SetUint64(latestBlock),
			Addresses: []common.Address{contractAddr},
			Topics: [][]common.Hash{
				{ERC1155TransferBatchEventSignature},
				nil,         // any operator
				{ownerHash}, // from address
			},
		},
		// TransferBatch where owner is `to` (topic[3])
		{
			FromBlock: new(big.Int).SetUint64(fromBlock),
			ToBlock:   new(big.Int).SetUint64(latestBlock),
			Addresses: []common.Address{contractAddr},
			Topics: [][]common.Hash{
				{ERC1155TransferBatchEventSignature},
				nil,         // any operator
				nil,         // any from address
				{ownerHash}, // to address
			},
		},
	}

	// Step 4: Execute all queries in parallel
	type queryResult struct {
		logs []types.Log
		err  error
	}

	resultsCh := make(chan queryResult, len(queries))
	for _, q := range queries {
		query := q // capture loop variable
		go func() {
			logs, err := pagination.FilterLogsWithPagination(ctx, query)
			resultsCh <- queryResult{logs: logs, err: err}
		}()
	}

	// Collect results
	var allLogs []types.Log
	for range queries {
		result := <-resultsCh
		if result.err != nil {
			return "", nil, fmt.Errorf("failed to fetch logs: %w", result.err)
		}
		allLogs = append(allLogs, result.logs...)
	}

	// Step 5: Sort logs by block number and log index for chronological processing
	sort.Slice(allLogs, func(i, j int) bool {
		if allLogs[i].BlockNumber != allLogs[j].BlockNumber {
			return allLogs[i].BlockNumber < allLogs[j].BlockNumber
		}
		return allLogs[i].Index < allLogs[j].Index
	})

	// Step 6: Parse logs and extract events for the specific token
	events := make([]domain.BlockchainEvent, 0)

	for _, vLog := range allLogs {
		if len(vLog.Topics) < 1 {
			continue
		}

		switch vLog.Topics[0] {
		case ERC1155TransferSingleEventSignature:
			// TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value)
			if len(vLog.Topics) != 4 || len(vLog.Data) < 64 {
				logger.WarnCtx(ctx, "Invalid TransferSingle event",
					zap.String("txHash", vLog.TxHash.Hex()),
					zap.Int("topics", len(vLog.Topics)))
				continue
			}

			// Parse token ID from data (first 32 bytes)
			logTokenID := new(big.Int).SetBytes(vLog.Data[0:32])

			// Filter by token ID - only include events for the token we care about
			if logTokenID.Cmp(tokenID) != 0 {
				continue
			}

			// Parse TransferSingle event inline
			fromAddress := common.BytesToAddress(vLog.Topics[2].Bytes()).Hex()
			toAddress := common.BytesToAddress(vLog.Topics[3].Bytes()).Hex()
			quantity := new(big.Int).SetBytes(vLog.Data[32:64])

			blockTimestamp, err := blockProvider.GetBlockTimestamp(ctx, vLog.BlockNumber)
			if err != nil {
				logger.WarnCtx(ctx, "Failed to get block timestamp, using current time",
					zap.Error(err),
					zap.Uint64("blockNumber", vLog.BlockNumber))
				blockTimestamp = fallbackNow()
			}

			blockHash := vLog.BlockHash.Hex()
			event := domain.BlockchainEvent{
				Chain:           chainID,
				ContractAddress: vLog.Address.Hex(),
				Standard:        domain.StandardERC1155,
				TokenNumber:     logTokenID.String(),
				FromAddress:     &fromAddress,
				ToAddress:       &toAddress,
				Quantity:        quantity.String(),
				TxHash:          vLog.TxHash.Hex(),
				BlockNumber:     vLog.BlockNumber,
				BlockHash:       &blockHash,
				TxIndex:         uint64(vLog.TxIndex), //nolint:gosec,G115
				LogIndex:        uint64(vLog.Index),   //nolint:gosec,G115
				Timestamp:       blockTimestamp,
			}
			event.EventType = domain.TransferEventType(event.FromAddress, event.ToAddress)
			events = append(events, event)

		case ERC1155TransferBatchEventSignature:
			// TransferBatch(address indexed operator, address indexed from, address indexed to, uint256[] ids, uint256[] values)
			if len(vLog.Topics) != 4 || len(vLog.Data) < 64 {
				logger.WarnCtx(ctx, "Invalid TransferBatch event",
					zap.String("txHash", vLog.TxHash.Hex()),
					zap.Int("topics", len(vLog.Topics)))
				continue
			}

			// Parse the batch event and extract only transfers for our token
			batchEvents, err := ParseERC1155TransferBatch(
				ctx,
				chainID,
				vLog,
				tokenID,
				blockProvider,
				fallbackNow,
			)
			if err != nil {
				logger.WarnCtx(ctx, "Failed to parse TransferBatch event",
					zap.Error(err),
					zap.String("txHash", vLog.TxHash.Hex()))
				continue
			}

			events = append(events, batchEvents...)
		}
	}

	logger.InfoCtx(ctx, "Fetched ERC1155 events for owner",
		zap.String("contract", contractAddress),
		zap.String("tokenNumber", tokenNumber),
		zap.String("owner", ownerAddress),
		zap.Int("eventCount", len(events)),
		zap.String("currentBalance", balance),
	)

	return balance, events, nil
}

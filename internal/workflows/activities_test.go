package workflows_test

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"go.temporal.io/sdk/temporal"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/metadata"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/providers/cloudflare"
	"github.com/feral-file/ff-indexer-v2/internal/providers/tezos"
	"github.com/feral-file/ff-indexer-v2/internal/store"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
	"github.com/feral-file/ff-indexer-v2/internal/workflows"
)

// testExecutorMocks contains all the mocks needed for testing the executor
type testExecutorMocks struct {
	ctrl             *gomock.Controller
	store            *mocks.MockStore
	metadataResolver *mocks.MockMetadataResolver
	metadataEnhancer *mocks.MockMetadataEnhancer
	ethClient        *mocks.MockEthereumProviderClient
	tzktClient       *mocks.MockTzKTClient
	mediaProcessor   *mocks.MockMediaProcessor
	json             *mocks.MockJSON
	clock            *mocks.MockClock
	executor         workflows.Executor
}

// setupTestExecutor creates all the mocks and executor for testing
func setupTestExecutor(t *testing.T) *testExecutorMocks {
	// Initialize logger for tests (required for activities that log)
	err := logger.Initialize(logger.Config{
		Debug: true,
	})
	if err != nil {
		t.Fatalf("Failed to initialize logger: %v", err)
	}

	ctrl := gomock.NewController(t)

	tm := &testExecutorMocks{
		ctrl:             ctrl,
		store:            mocks.NewMockStore(ctrl),
		metadataResolver: mocks.NewMockMetadataResolver(ctrl),
		metadataEnhancer: mocks.NewMockMetadataEnhancer(ctrl),
		ethClient:        mocks.NewMockEthereumProviderClient(ctrl),
		tzktClient:       mocks.NewMockTzKTClient(ctrl),
		mediaProcessor:   mocks.NewMockMediaProcessor(ctrl),
		json:             mocks.NewMockJSON(ctrl),
		clock:            mocks.NewMockClock(ctrl),
	}

	tm.executor = workflows.NewExecutor(
		tm.store,
		tm.metadataResolver,
		tm.metadataEnhancer,
		tm.ethClient,
		tm.tzktClient,
		tm.mediaProcessor,
		tm.json,
		tm.clock,
	)

	return tm
}

// tearDownTestExecutor cleans up the test mocks
func tearDownTestExecutor(mocks *testExecutorMocks) {
	mocks.ctrl.Finish()
}

// Helper functions
func stringPtr(s string) *string {
	return &s
}

// ====================================================================================
// CheckTokenExists Tests
// ====================================================================================

func TestCheckTokenExists_Success(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")

	// Mock store to return a token
	token := &schema.Token{
		ID:       1,
		TokenCID: tokenCID.String(),
	}
	mocks.store.EXPECT().
		GetTokenByTokenCID(ctx, tokenCID.String()).
		Return(token, nil)

	exists, err := mocks.executor.CheckTokenExists(ctx, tokenCID)

	assert.NoError(t, err)
	assert.True(t, exists)
}

func TestCheckTokenExists_NotFound(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")

	// Mock store to return nil (token not found)
	mocks.store.EXPECT().
		GetTokenByTokenCID(ctx, tokenCID.String()).
		Return(nil, nil)

	exists, err := mocks.executor.CheckTokenExists(ctx, tokenCID)

	assert.NoError(t, err)
	assert.False(t, exists)
}

func TestCheckTokenExists_InvalidTokenCID(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	invalidTokenCID := domain.TokenCID("")

	exists, err := mocks.executor.CheckTokenExists(ctx, invalidTokenCID)

	assert.Error(t, err)
	assert.Equal(t, domain.ErrInvalidTokenCID, err)
	assert.False(t, exists)
}

func TestCheckTokenExists_StoreError(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")

	// Mock store to return an error
	storeErr := errors.New("database error")
	mocks.store.EXPECT().
		GetTokenByTokenCID(ctx, tokenCID.String()).
		Return(nil, storeErr)

	exists, err := mocks.executor.CheckTokenExists(ctx, tokenCID)

	assert.Error(t, err)
	assert.False(t, exists)
	assert.Contains(t, err.Error(), "failed to check if token exists")
}

// ====================================================================================
// CreateTokenMint Tests
// ====================================================================================

func TestCreateTokenMint_Success_ERC721(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	fromAddr := domain.ETHEREUM_ZERO_ADDRESS
	toAddr := "0xowner123"
	timestamp := time.Now()

	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeMint,
		FromAddress:     &fromAddr,
		ToAddress:       &toAddr,
		Quantity:        "1",
		TxHash:          "0xtx123",
		BlockNumber:     100,
		BlockHash:       stringPtr("0xblock123"),
		Timestamp:       timestamp,
	}

	rawEventData := []byte(`{"chain":"eip155:1"}`)

	// Mock JSON marshal
	mocks.json.EXPECT().
		Marshal(event).
		Return(rawEventData, nil)

	// Mock store CreateTokenMint
	mocks.store.EXPECT().
		CreateTokenMint(ctx, gomock.Any()).
		DoAndReturn(func(ctx context.Context, input store.CreateTokenMintInput) error {
			assert.Equal(t, event.TokenCID().String(), input.Token.TokenCID)
			assert.Equal(t, event.Chain, input.Token.Chain)
			assert.Equal(t, event.Standard, input.Token.Standard)
			assert.Equal(t, event.ContractAddress, input.Token.ContractAddress)
			assert.Equal(t, event.TokenNumber, input.Token.TokenNumber)
			assert.Equal(t, toAddr, *input.Token.CurrentOwner)
			assert.False(t, input.Token.Burned)
			assert.Equal(t, toAddr, input.Balance.OwnerAddress)
			assert.Equal(t, "1", input.Balance.Quantity)
			assert.Equal(t, schema.ProvenanceEventTypeMint, input.ProvenanceEvent.EventType)
			return nil
		})

	err := mocks.executor.CreateTokenMint(ctx, event)

	assert.NoError(t, err)
}

func TestCreateTokenMint_Success_ERC1155(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	fromAddr := domain.ETHEREUM_ZERO_ADDRESS
	toAddr := "0xowner123"
	timestamp := time.Now()

	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC1155,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeMint,
		FromAddress:     &fromAddr,
		ToAddress:       &toAddr,
		Quantity:        "5",
		TxHash:          "0xtx123",
		BlockNumber:     100,
		BlockHash:       stringPtr("0xblock123"),
		Timestamp:       timestamp,
	}

	rawEventData := []byte(`{"chain":"eip155:1"}`)

	// Mock JSON marshal
	mocks.json.EXPECT().
		Marshal(event).
		Return(rawEventData, nil)

	// For ERC1155, we need to fetch the balance from the chain
	mocks.ethClient.EXPECT().
		ERC1155BalanceOf(ctx, event.ContractAddress, toAddr, event.TokenNumber).
		Return("10", nil) // Current balance is 10 after minting 5

	// Mock store CreateTokenMint
	mocks.store.EXPECT().
		CreateTokenMint(ctx, gomock.Any()).
		DoAndReturn(func(ctx context.Context, input store.CreateTokenMintInput) error {
			assert.Equal(t, "10", input.Balance.Quantity) // Should use fetched balance
			return nil
		})

	err := mocks.executor.CreateTokenMint(ctx, event)

	assert.NoError(t, err)
}

func TestCreateTokenMint_Success_FA2(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	toAddr := "tz1VSUr8wwNhLAzempoch5d6hLRiTh8Cjcjb"
	timestamp := time.Now()

	event := &domain.BlockchainEvent{
		Chain:           domain.ChainTezosMainnet,
		Standard:        domain.StandardFA2,
		ContractAddress: "KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton",
		TokenNumber:     "1",
		EventType:       domain.EventTypeMint,
		FromAddress:     nil, // FA2 mint has no from address
		ToAddress:       &toAddr,
		Quantity:        "3",
		TxHash:          "op123",
		BlockNumber:     100,
		BlockHash:       stringPtr("block123"),
		Timestamp:       timestamp,
	}

	rawEventData := []byte(`{"chain":"tezos:mainnet"}`)

	// Mock JSON marshal
	mocks.json.EXPECT().
		Marshal(event).
		Return(rawEventData, nil)

	// For FA2, we need to fetch the balance from TzKT
	mocks.tzktClient.EXPECT().
		GetTokenOwnerBalance(ctx, event.ContractAddress, event.TokenNumber, toAddr).
		Return("7", nil) // Current balance is 7 after minting 3

	// Mock store CreateTokenMint
	mocks.store.EXPECT().
		CreateTokenMint(ctx, gomock.Any()).
		DoAndReturn(func(ctx context.Context, input store.CreateTokenMintInput) error {
			assert.Equal(t, "7", input.Balance.Quantity) // Should use fetched balance
			return nil
		})

	err := mocks.executor.CreateTokenMint(ctx, event)

	assert.NoError(t, err)
}

func TestCreateTokenMint_InvalidEvent(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()

	// Create an invalid event (missing required fields)
	event := &domain.BlockchainEvent{
		Chain: domain.ChainEthereumMainnet,
		// Missing other required fields
	}

	err := mocks.executor.CreateTokenMint(ctx, event)

	assert.Error(t, err)
	assert.Equal(t, domain.ErrInvalidBlockchainEvent, err)
}

func TestCreateTokenMint_JSONMarshalError(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	fromAddr := "0x0000000000000000000000000000000000000000"
	toAddr := "0xowner123"
	timestamp := time.Now()

	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeMint,
		FromAddress:     &fromAddr,
		ToAddress:       &toAddr,
		Quantity:        "1",
		TxHash:          "0xtx123",
		BlockNumber:     100,
		BlockHash:       stringPtr("0xblock123"),
		Timestamp:       timestamp,
	}

	// Mock JSON marshal to return error
	marshalErr := errors.New("json marshal error")
	mocks.json.EXPECT().
		Marshal(event).
		Return(nil, marshalErr)

	err := mocks.executor.CreateTokenMint(ctx, event)

	assert.Error(t, err)
	assert.Equal(t, marshalErr, err)
}

func TestCreateTokenMint_ERC1155BalanceError(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	fromAddr := domain.ETHEREUM_ZERO_ADDRESS
	toAddr := "0xowner123"
	timestamp := time.Now()

	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC1155,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeMint,
		FromAddress:     &fromAddr,
		ToAddress:       &toAddr,
		Quantity:        "5",
		TxHash:          "0xtx123",
		BlockNumber:     100,
		BlockHash:       stringPtr("0xblock123"),
		Timestamp:       timestamp,
	}

	rawEventData := []byte(`{"chain":"eip155:1"}`)

	// Mock JSON marshal
	mocks.json.EXPECT().
		Marshal(event).
		Return(rawEventData, nil)

	// Mock ERC1155BalanceOf to return error
	balanceErr := errors.New("failed to get balance")
	mocks.ethClient.EXPECT().
		ERC1155BalanceOf(ctx, event.ContractAddress, toAddr, event.TokenNumber).
		Return("", balanceErr)

	err := mocks.executor.CreateTokenMint(ctx, event)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get ERC1155 token balance")
}

func TestCreateTokenMint_StoreError(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	fromAddr := domain.ETHEREUM_ZERO_ADDRESS
	toAddr := "0xowner123"
	timestamp := time.Now()

	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeMint,
		FromAddress:     &fromAddr,
		ToAddress:       &toAddr,
		Quantity:        "1",
		TxHash:          "0xtx123",
		BlockNumber:     100,
		BlockHash:       stringPtr("0xblock123"),
		Timestamp:       timestamp,
	}

	rawEventData := []byte(`{"chain":"eip155:1"}`)

	// Mock JSON marshal
	mocks.json.EXPECT().
		Marshal(event).
		Return(rawEventData, nil)

	// Mock store CreateTokenMint to return error
	storeErr := errors.New("database error")
	mocks.store.EXPECT().
		CreateTokenMint(ctx, gomock.Any()).
		Return(storeErr)

	err := mocks.executor.CreateTokenMint(ctx, event)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create token mint")
}

// ====================================================================================
// UpdateTokenTransfer Tests
// ====================================================================================

func TestUpdateTokenTransfer_Success(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	fromAddr := "0xsender123"
	toAddr := "0xreceiver123"
	timestamp := time.Now()

	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeTransfer,
		FromAddress:     &fromAddr,
		ToAddress:       &toAddr,
		Quantity:        "1",
		TxHash:          "0xtx123",
		BlockNumber:     100,
		BlockHash:       stringPtr("0xblock123"),
		Timestamp:       timestamp,
	}

	rawEventData := []byte(`{"chain":"eip155:1"}`)

	// Mock JSON marshal
	mocks.json.EXPECT().
		Marshal(event).
		Return(rawEventData, nil)

	// Mock store UpdateTokenTransfer
	mocks.store.EXPECT().
		UpdateTokenTransfer(ctx, gomock.Any()).
		DoAndReturn(func(ctx context.Context, input store.UpdateTokenTransferInput) error {
			assert.Equal(t, event.TokenCID().String(), input.TokenCID)
			assert.Equal(t, toAddr, *input.CurrentOwner)
			assert.NotNil(t, input.SenderBalanceUpdate)
			assert.Equal(t, fromAddr, input.SenderBalanceUpdate.OwnerAddress)
			assert.Equal(t, "1", input.SenderBalanceUpdate.Delta)
			assert.NotNil(t, input.ReceiverBalanceUpdate)
			assert.Equal(t, toAddr, input.ReceiverBalanceUpdate.OwnerAddress)
			assert.Equal(t, "1", input.ReceiverBalanceUpdate.Delta)
			assert.Equal(t, schema.ProvenanceEventTypeTransfer, input.ProvenanceEvent.EventType)
			assert.Equal(t, event.FromAddress, input.ProvenanceEvent.FromAddress)
			assert.Equal(t, event.ToAddress, input.ProvenanceEvent.ToAddress)
			assert.Equal(t, event.Quantity, input.ProvenanceEvent.Quantity)
			assert.Equal(t, event.TxHash, input.ProvenanceEvent.TxHash)
			assert.Equal(t, event.BlockNumber, input.ProvenanceEvent.BlockNumber)
			assert.Equal(t, event.BlockHash, input.ProvenanceEvent.BlockHash)
			assert.Equal(t, event.Timestamp, input.ProvenanceEvent.Timestamp)
			assert.Equal(t, rawEventData, input.ProvenanceEvent.Raw)
			return nil
		})

	err := mocks.executor.UpdateTokenTransfer(ctx, event)

	assert.NoError(t, err)
}

func TestUpdateTokenTransfer_InvalidEvent(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()

	// Create an invalid event
	event := &domain.BlockchainEvent{}

	err := mocks.executor.UpdateTokenTransfer(ctx, event)

	assert.Error(t, err)
	assert.Equal(t, domain.ErrInvalidBlockchainEvent, err)
}

func TestUpdateTokenTransfer_MarshalError(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	fromAddr := "0xsender123"
	toAddr := "0xreceiver123"
	timestamp := time.Now()

	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeTransfer,
		FromAddress:     &fromAddr,
		ToAddress:       &toAddr,
		Quantity:        "1",
		TxHash:          "0xtx123",
		BlockNumber:     100,
		BlockHash:       stringPtr("0xblock123"),
		Timestamp:       timestamp,
	}

	// Mock JSON marshal to return error
	marshalErr := errors.New("marshal error")
	mocks.json.EXPECT().
		Marshal(event).
		Return(nil, marshalErr)

	err := mocks.executor.UpdateTokenTransfer(ctx, event)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to marshal event")
}

func TestUpdateTokenTransfer_StoreError(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	fromAddr := "0xsender123"
	toAddr := "0xreceiver123"
	timestamp := time.Now()

	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeTransfer,
		FromAddress:     &fromAddr,
		ToAddress:       &toAddr,
		Quantity:        "1",
		TxHash:          "0xtx123",
		BlockNumber:     100,
		BlockHash:       stringPtr("0xblock123"),
		Timestamp:       timestamp,
	}

	rawEventData := []byte(`{"chain":"eip155:1"}`)

	// Mock JSON marshal
	mocks.json.EXPECT().
		Marshal(event).
		Return(rawEventData, nil)

	// Mock store UpdateTokenTransfer to return error
	storeErr := errors.New("database error")
	mocks.store.EXPECT().
		UpdateTokenTransfer(ctx, gomock.Any()).
		Return(storeErr)

	err := mocks.executor.UpdateTokenTransfer(ctx, event)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to update token transfer")
}

// ====================================================================================
// UpdateTokenBurn Tests
// ====================================================================================

func TestUpdateTokenBurn_Success(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	fromAddr := "0xowner123"
	toAddr := domain.ETHEREUM_ZERO_ADDRESS
	timestamp := time.Now()

	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeBurn,
		FromAddress:     &fromAddr,
		ToAddress:       &toAddr,
		Quantity:        "1",
		TxHash:          "0xtx123",
		BlockNumber:     100,
		BlockHash:       stringPtr("0xblock123"),
		Timestamp:       timestamp,
	}

	rawEventData := []byte(`{"chain":"eip155:1"}`)

	// Mock JSON marshal
	mocks.json.EXPECT().
		Marshal(event).
		Return(rawEventData, nil)

	// Mock store UpdateTokenBurn
	mocks.store.EXPECT().
		UpdateTokenBurn(ctx, gomock.Any()).
		DoAndReturn(func(ctx context.Context, input store.CreateTokenBurnInput) error {
			assert.Equal(t, event.TokenCID().String(), input.TokenCID)
			assert.NotNil(t, input.SenderBalanceUpdate)
			assert.Equal(t, fromAddr, input.SenderBalanceUpdate.OwnerAddress)
			assert.Equal(t, "1", input.SenderBalanceUpdate.Delta)
			assert.Equal(t, schema.ProvenanceEventTypeBurn, input.ProvenanceEvent.EventType)
			assert.Equal(t, event.FromAddress, input.ProvenanceEvent.FromAddress)
			assert.Equal(t, event.ToAddress, input.ProvenanceEvent.ToAddress)
			assert.Equal(t, event.Quantity, input.ProvenanceEvent.Quantity)
			assert.Equal(t, event.TxHash, input.ProvenanceEvent.TxHash)
			assert.Equal(t, event.BlockNumber, input.ProvenanceEvent.BlockNumber)
			assert.Equal(t, event.BlockHash, input.ProvenanceEvent.BlockHash)
			assert.Equal(t, event.Timestamp, input.ProvenanceEvent.Timestamp)
			assert.Equal(t, rawEventData, input.ProvenanceEvent.Raw)
			return nil
		})

	err := mocks.executor.UpdateTokenBurn(ctx, event)

	assert.NoError(t, err)
}

func TestUpdateTokenBurn_InvalidEvent(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()

	// Create an invalid event
	event := &domain.BlockchainEvent{}

	err := mocks.executor.UpdateTokenBurn(ctx, event)

	assert.Error(t, err)
	assert.Equal(t, domain.ErrInvalidBlockchainEvent, err)
}

func TestUpdateTokenBurn_MarshalError(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	fromAddr := "0xowner123"
	toAddr := domain.ETHEREUM_ZERO_ADDRESS
	timestamp := time.Now()

	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeBurn,
		FromAddress:     &fromAddr,
		ToAddress:       &toAddr,
		Quantity:        "1",
		TxHash:          "0xtx123",
		BlockNumber:     100,
		BlockHash:       stringPtr("0xblock123"),
		Timestamp:       timestamp,
	}

	// Mock JSON marshal to return error
	marshalErr := errors.New("marshal error")
	mocks.json.EXPECT().
		Marshal(event).
		Return(nil, marshalErr)

	err := mocks.executor.UpdateTokenBurn(ctx, event)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to marshal event")
}

func TestUpdateTokenBurn_StoreError(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	fromAddr := "0xowner123"
	toAddr := domain.ETHEREUM_ZERO_ADDRESS
	timestamp := time.Now()

	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeBurn,
		FromAddress:     &fromAddr,
		ToAddress:       &toAddr,
		Quantity:        "1",
		TxHash:          "0xtx123",
		BlockNumber:     100,
		BlockHash:       stringPtr("0xblock123"),
		Timestamp:       timestamp,
	}

	rawEventData := []byte(`{"chain":"eip155:1"}`)

	// Mock JSON marshal
	mocks.json.EXPECT().
		Marshal(event).
		Return(rawEventData, nil)

	// Mock store UpdateTokenBurn to return error
	storeErr := errors.New("database error")
	mocks.store.EXPECT().
		UpdateTokenBurn(ctx, gomock.Any()).
		Return(storeErr)

	err := mocks.executor.UpdateTokenBurn(ctx, event)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to update token burn")
}

// ====================================================================================
// CreateMetadataUpdate Tests
// ====================================================================================

func TestCreateMetadataUpdate_Success(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	timestamp := time.Now()

	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeMetadataUpdate,
		FromAddress:     nil,
		ToAddress:       nil,
		Quantity:        "1",
		TxHash:          "0xtx123",
		BlockNumber:     100,
		BlockHash:       stringPtr("0xblock123"),
		Timestamp:       timestamp,
	}

	rawEventData := []byte(`{"chain":"eip155:1"}`)

	// Mock JSON marshal
	mocks.json.EXPECT().
		Marshal(event).
		Return(rawEventData, nil)

	// Mock store CreateMetadataUpdate
	mocks.store.EXPECT().
		CreateMetadataUpdate(ctx, gomock.Any()).
		DoAndReturn(func(ctx context.Context, input store.CreateMetadataUpdateInput) error {
			assert.Equal(t, event.TokenCID().String(), input.TokenCID)
			assert.Equal(t, schema.ProvenanceEventTypeMetadataUpdate, input.ProvenanceEvent.EventType)
			assert.Equal(t, event.FromAddress, input.ProvenanceEvent.FromAddress)
			assert.Equal(t, event.ToAddress, input.ProvenanceEvent.ToAddress)
			assert.Equal(t, event.Quantity, input.ProvenanceEvent.Quantity)
			assert.Equal(t, event.TxHash, input.ProvenanceEvent.TxHash)
			assert.Equal(t, event.BlockNumber, input.ProvenanceEvent.BlockNumber)
			assert.Equal(t, event.BlockHash, input.ProvenanceEvent.BlockHash)
			assert.Equal(t, event.Timestamp, input.ProvenanceEvent.Timestamp)
			assert.Equal(t, rawEventData, input.ProvenanceEvent.Raw)
			return nil
		})

	err := mocks.executor.CreateMetadataUpdate(ctx, event)

	assert.NoError(t, err)
}

func TestCreateMetadataUpdate_InvalidEvent(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()

	// Create an invalid event
	event := &domain.BlockchainEvent{}

	err := mocks.executor.CreateMetadataUpdate(ctx, event)

	assert.Error(t, err)
	assert.Equal(t, domain.ErrInvalidBlockchainEvent, err)
}

func TestCreateMetadataUpdate_MarshalError(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	timestamp := time.Now()

	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeMetadataUpdate,
		FromAddress:     nil,
		ToAddress:       nil,
		Quantity:        "1",
		TxHash:          "0xtx123",
		BlockNumber:     100,
		BlockHash:       stringPtr("0xblock123"),
		Timestamp:       timestamp,
	}

	// Mock JSON marshal to return error
	marshalErr := errors.New("marshal error")
	mocks.json.EXPECT().
		Marshal(event).
		Return(nil, marshalErr)

	err := mocks.executor.CreateMetadataUpdate(ctx, event)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to marshal event")
}

func TestCreateMetadataUpdate_StoreError(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	timestamp := time.Now()

	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeMetadataUpdate,
		FromAddress:     nil,
		ToAddress:       nil,
		Quantity:        "1",
		TxHash:          "0xtx123",
		BlockNumber:     100,
		BlockHash:       stringPtr("0xblock123"),
		Timestamp:       timestamp,
	}

	rawEventData := []byte(`{"chain":"eip155:1"}`)

	// Mock JSON marshal
	mocks.json.EXPECT().
		Marshal(event).
		Return(rawEventData, nil)

	// Mock store CreateMetadataUpdate to return error
	storeErr := errors.New("database error")
	mocks.store.EXPECT().
		CreateMetadataUpdate(ctx, gomock.Any()).
		Return(storeErr)

	err := mocks.executor.CreateMetadataUpdate(ctx, event)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create metadata update")
}

// ====================================================================================
// FetchTokenMetadata Tests
// ====================================================================================

func TestFetchTokenMetadata_Success(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")

	expectedMetadata := &metadata.NormalizedMetadata{
		Name:        "Test NFT",
		Description: "Test Description",
		Image:       "https://example.com/image.png",
		Raw:         map[string]interface{}{"name": "Test NFT"},
	}

	// Mock metadata resolver
	mocks.metadataResolver.EXPECT().
		Resolve(ctx, tokenCID).
		Return(expectedMetadata, nil)

	result, err := mocks.executor.FetchTokenMetadata(ctx, tokenCID)

	assert.NoError(t, err)
	assert.Equal(t, expectedMetadata, result)
}

func TestFetchTokenMetadata_InvalidTokenCID(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	invalidTokenCID := domain.TokenCID("")

	result, err := mocks.executor.FetchTokenMetadata(ctx, invalidTokenCID)

	assert.Error(t, err)
	assert.Equal(t, domain.ErrInvalidTokenCID, err)
	assert.Nil(t, result)
}

func TestFetchTokenMetadata_ResolverError(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")

	// Mock metadata resolver to return error
	resolverErr := errors.New("resolver error")
	mocks.metadataResolver.EXPECT().
		Resolve(ctx, tokenCID).
		Return(nil, resolverErr)

	result, err := mocks.executor.FetchTokenMetadata(ctx, tokenCID)

	assert.Error(t, err)
	assert.Equal(t, resolverErr, err)
	assert.Nil(t, result)
}

// ====================================================================================
// UpsertTokenMetadata Tests
// ====================================================================================

func TestUpsertTokenMetadata_Success_NewMetadata(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")
	now := time.Now()

	normalizedMetadata := &metadata.NormalizedMetadata{
		Name:        "Test NFT",
		Description: "Test Description",
		Image:       "https://example.com/image.png",
		Animation:   "https://example.com/animation.mp4",
		Artists: []metadata.Artist{
			{DID: "did:example:artist1", Name: "Artist 1"},
		},
		Raw: map[string]interface{}{"name": "Test NFT"},
	}

	token := &schema.Token{
		ID:       1,
		TokenCID: tokenCID.String(),
	}

	// Mock store to return token with no existing metadata
	mocks.store.EXPECT().
		GetTokenWithMetadataByTokenCID(ctx, tokenCID.String()).
		Return(&store.TokensWithMetadataResult{
			Token:    token,
			Metadata: nil,
		}, nil)

	// Mock metadata resolver RawHash
	metadataJSON := []byte(`{"name":"Test NFT"}`)
	hash := []byte("hash123")
	mocks.metadataResolver.EXPECT().
		RawHash(normalizedMetadata).
		Return(hash, metadataJSON, nil)

	// Mock clock
	mocks.clock.EXPECT().
		Now().
		Return(now)

	// Mock store UpsertTokenMetadata
	mocks.store.EXPECT().
		UpsertTokenMetadata(ctx, gomock.Any()).
		DoAndReturn(func(ctx context.Context, input store.CreateTokenMetadataInput) error {
			assert.Equal(t, token.ID, input.TokenID)
			assert.Equal(t, metadataJSON, input.OriginJSON)
			assert.Equal(t, metadataJSON, input.LatestJSON)
			assert.Equal(t, hex.EncodeToString(hash), *input.LatestHash)
			assert.Equal(t, schema.EnrichmentLevelNone, input.EnrichmentLevel)
			assert.Equal(t, &now, input.LastRefreshedAt)
			assert.Equal(t, &normalizedMetadata.Image, input.ImageURL)
			assert.Equal(t, &normalizedMetadata.Animation, input.AnimationURL)
			assert.Equal(t, &normalizedMetadata.Name, input.Name)
			assert.Equal(t, &normalizedMetadata.Description, input.Description)
			assert.Nil(t, input.MimeType)
			assert.Len(t, input.Artists, 1)
			return nil
		})

	err := mocks.executor.UpsertTokenMetadata(ctx, tokenCID, normalizedMetadata)

	assert.NoError(t, err)
}

func TestUpsertTokenMetadata_Success_UpdateExisting(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")
	now := time.Now()

	normalizedMetadata := &metadata.NormalizedMetadata{
		Name:        "Updated NFT",
		Description: "Updated Description",
		Image:       "https://example.com/image-new.png",
		Raw:         map[string]interface{}{"name": "Updated NFT"},
	}

	token := &schema.Token{
		ID:       1,
		TokenCID: tokenCID.String(),
	}

	existingOriginJSON := []byte(`{"name":"Original NFT"}`)
	existingMetadata := &schema.TokenMetadata{
		TokenID:    1,
		OriginJSON: existingOriginJSON,
	}

	// Mock store to return token with existing metadata
	mocks.store.EXPECT().
		GetTokenWithMetadataByTokenCID(ctx, tokenCID.String()).
		Return(&store.TokensWithMetadataResult{
			Token:    token,
			Metadata: existingMetadata,
		}, nil)

	// Mock metadata resolver RawHash
	metadataJSON := []byte(`{"name":"Updated NFT"}`)
	hash := []byte("newhash123")
	mocks.metadataResolver.EXPECT().
		RawHash(normalizedMetadata).
		Return(hash, metadataJSON, nil)

	// Mock clock
	mocks.clock.EXPECT().
		Now().
		Return(now)

	// Mock store UpsertTokenMetadata
	mocks.store.EXPECT().
		UpsertTokenMetadata(ctx, gomock.Any()).
		DoAndReturn(func(ctx context.Context, input store.CreateTokenMetadataInput) error {
			// Should preserve original origin JSON
			assert.Equal(t, existingOriginJSON, input.OriginJSON)
			// But update latest JSON
			assert.Equal(t, metadataJSON, input.LatestJSON)
			return nil
		})

	err := mocks.executor.UpsertTokenMetadata(ctx, tokenCID, normalizedMetadata)

	assert.NoError(t, err)
}

func TestUpsertTokenMetadata_InvalidTokenCID(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	invalidTokenCID := domain.TokenCID("")

	normalizedMetadata := &metadata.NormalizedMetadata{
		Name: "Test NFT",
	}

	err := mocks.executor.UpsertTokenMetadata(ctx, invalidTokenCID, normalizedMetadata)

	assert.Error(t, err)
	assert.Equal(t, domain.ErrInvalidTokenCID, err)
}

func TestUpsertTokenMetadata_TokenNotFound(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")

	normalizedMetadata := &metadata.NormalizedMetadata{
		Name: "Test NFT",
	}

	// Mock store to return nil (token not found)
	mocks.store.EXPECT().
		GetTokenWithMetadataByTokenCID(ctx, tokenCID.String()).
		Return(nil, nil)

	err := mocks.executor.UpsertTokenMetadata(ctx, tokenCID, normalizedMetadata)

	assert.Error(t, err)
	assert.Equal(t, domain.ErrTokenNotFound, err)
}

func TestUpsertTokenMetadata_RawHashError(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")

	normalizedMetadata := &metadata.NormalizedMetadata{
		Name: "Test NFT",
	}

	token := &schema.Token{
		ID:       1,
		TokenCID: tokenCID.String(),
	}

	// Mock store to return token
	mocks.store.EXPECT().
		GetTokenWithMetadataByTokenCID(ctx, tokenCID.String()).
		Return(&store.TokensWithMetadataResult{
			Token:    token,
			Metadata: nil,
		}, nil)

	// Mock metadata resolver RawHash to return error
	hashErr := errors.New("hash error")
	mocks.metadataResolver.EXPECT().
		RawHash(normalizedMetadata).
		Return(nil, nil, hashErr)

	err := mocks.executor.UpsertTokenMetadata(ctx, tokenCID, normalizedMetadata)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get raw hash")
}

func TestUpsertTokenMetadata_StoreError(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")
	now := time.Now()

	normalizedMetadata := &metadata.NormalizedMetadata{
		Name: "Test NFT",
		Raw:  map[string]interface{}{"name": "Test NFT"},
	}

	token := &schema.Token{
		ID:       1,
		TokenCID: tokenCID.String(),
	}

	// Mock store to return token
	mocks.store.EXPECT().
		GetTokenWithMetadataByTokenCID(ctx, tokenCID.String()).
		Return(&store.TokensWithMetadataResult{
			Token:    token,
			Metadata: nil,
		}, nil)

	// Mock metadata resolver RawHash
	metadataJSON := []byte(`{"name":"Test NFT"}`)
	hash := []byte("hash123")
	mocks.metadataResolver.EXPECT().
		RawHash(normalizedMetadata).
		Return(hash, metadataJSON, nil)

	// Mock clock
	mocks.clock.EXPECT().
		Now().
		Return(now)

	// Mock store UpsertTokenMetadata to return error
	storeErr := errors.New("database error")
	mocks.store.EXPECT().
		UpsertTokenMetadata(ctx, gomock.Any()).
		Return(storeErr)

	err := mocks.executor.UpsertTokenMetadata(ctx, tokenCID, normalizedMetadata)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to upsert token metadata")
}

// ====================================================================================
// EnhanceTokenMetadata Tests
// ====================================================================================

func TestEnhanceTokenMetadata_Success(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")

	normalizedMetadata := &metadata.NormalizedMetadata{
		Name:  "Test NFT",
		Image: "https://example.com/image.png",
	}

	token := &schema.Token{
		ID:       1,
		TokenCID: tokenCID.String(),
	}

	enhancedMetadata := &metadata.EnhancedMetadata{
		Vendor:     schema.VendorArtBlocks,
		VendorJSON: []byte(`{"artist":"Artist Name"}`),
		Name:       stringPtr("Enhanced NFT Name"),
		ImageURL:   stringPtr("https://vendor.com/image.png"),
		Artists: []metadata.Artist{
			{DID: "did:example:artist1", Name: "Artist 1"},
		},
	}

	// Mock store to return token
	mocks.store.EXPECT().
		GetTokenByTokenCID(ctx, tokenCID.String()).
		Return(token, nil)

	// Mock metadata enhancer
	mocks.metadataEnhancer.EXPECT().
		Enhance(ctx, tokenCID, normalizedMetadata).
		Return(enhancedMetadata, nil)

	// Mock VendorJsonHash
	hash := []byte("vendorhash123")
	mocks.metadataEnhancer.EXPECT().
		VendorJsonHash(enhancedMetadata).
		Return(hash, nil)

	// Mock store UpsertEnrichmentSource
	mocks.store.EXPECT().
		UpsertEnrichmentSource(ctx, gomock.Any()).
		DoAndReturn(func(ctx context.Context, input store.CreateEnrichmentSourceInput) error {
			assert.Equal(t, token.ID, input.TokenID)
			assert.Equal(t, schema.VendorArtBlocks, input.Vendor)
			assert.Equal(t, enhancedMetadata.VendorJSON, input.VendorJSON)
			assert.Equal(t, hex.EncodeToString(hash), *input.VendorHash)
			assert.Equal(t, enhancedMetadata.Name, input.Name)
			assert.Equal(t, enhancedMetadata.ImageURL, input.ImageURL)
			assert.Len(t, input.Artists, 1)
			return nil
		})

	result, err := mocks.executor.EnhanceTokenMetadata(ctx, tokenCID, normalizedMetadata)

	assert.NoError(t, err)
	assert.Equal(t, enhancedMetadata, result)
}

func TestEnhanceTokenMetadata_NoEnhancementAvailable(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")

	normalizedMetadata := &metadata.NormalizedMetadata{
		Name: "Test NFT",
	}

	token := &schema.Token{
		ID:       1,
		TokenCID: tokenCID.String(),
	}

	// Mock store to return token
	mocks.store.EXPECT().
		GetTokenByTokenCID(ctx, tokenCID.String()).
		Return(token, nil)

	// Mock metadata enhancer to return nil (no enhancement available)
	mocks.metadataEnhancer.EXPECT().
		Enhance(ctx, tokenCID, normalizedMetadata).
		Return(nil, nil)

	result, err := mocks.executor.EnhanceTokenMetadata(ctx, tokenCID, normalizedMetadata)

	assert.NoError(t, err)
	assert.Nil(t, result)
}

func TestEnhanceTokenMetadata_InvalidTokenCID(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	invalidTokenCID := domain.TokenCID("")

	normalizedMetadata := &metadata.NormalizedMetadata{
		Name: "Test NFT",
	}

	result, err := mocks.executor.EnhanceTokenMetadata(ctx, invalidTokenCID, normalizedMetadata)

	assert.Error(t, err)
	assert.Equal(t, domain.ErrInvalidTokenCID, err)
	assert.Nil(t, result)
}

func TestEnhanceTokenMetadata_TokenNotFound(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")

	normalizedMetadata := &metadata.NormalizedMetadata{
		Name: "Test NFT",
	}

	// Mock store to return nil (token not found)
	mocks.store.EXPECT().
		GetTokenByTokenCID(ctx, tokenCID.String()).
		Return(nil, nil)

	result, err := mocks.executor.EnhanceTokenMetadata(ctx, tokenCID, normalizedMetadata)

	assert.Error(t, err)
	assert.Equal(t, domain.ErrTokenNotFound, err)
	assert.Nil(t, result)
}

func TestEnhanceTokenMetadata_EnhancerError(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")

	normalizedMetadata := &metadata.NormalizedMetadata{
		Name: "Test NFT",
	}

	token := &schema.Token{
		ID:       1,
		TokenCID: tokenCID.String(),
	}

	// Mock store to return token
	mocks.store.EXPECT().
		GetTokenByTokenCID(ctx, tokenCID.String()).
		Return(token, nil)

	// Mock metadata enhancer to return error
	enhancerErr := errors.New("enhancer error")
	mocks.metadataEnhancer.EXPECT().
		Enhance(ctx, tokenCID, normalizedMetadata).
		Return(nil, enhancerErr)

	result, err := mocks.executor.EnhanceTokenMetadata(ctx, tokenCID, normalizedMetadata)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to enhance metadata")
	assert.Nil(t, result)
}

func TestEnhanceTokenMetadata_VendorHashError(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")

	normalizedMetadata := &metadata.NormalizedMetadata{
		Name: "Test NFT",
	}

	token := &schema.Token{
		ID:       1,
		TokenCID: tokenCID.String(),
	}

	enhancedMetadata := &metadata.EnhancedMetadata{
		Vendor:     schema.VendorArtBlocks,
		VendorJSON: []byte(`{"artist":"Artist Name"}`),
	}

	// Mock store to return token
	mocks.store.EXPECT().
		GetTokenByTokenCID(ctx, tokenCID.String()).
		Return(token, nil)

	// Mock metadata enhancer
	mocks.metadataEnhancer.EXPECT().
		Enhance(ctx, tokenCID, normalizedMetadata).
		Return(enhancedMetadata, nil)

	// Mock VendorJsonHash to return error
	hashErr := errors.New("hash error")
	mocks.metadataEnhancer.EXPECT().
		VendorJsonHash(enhancedMetadata).
		Return(nil, hashErr)

	result, err := mocks.executor.EnhanceTokenMetadata(ctx, tokenCID, normalizedMetadata)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get vendor JSON hash")
	assert.Nil(t, result)
}

func TestEnhanceTokenMetadata_StoreError(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")

	normalizedMetadata := &metadata.NormalizedMetadata{
		Name: "Test NFT",
	}

	token := &schema.Token{
		ID:       1,
		TokenCID: tokenCID.String(),
	}

	enhancedMetadata := &metadata.EnhancedMetadata{
		Vendor:     schema.VendorArtBlocks,
		VendorJSON: []byte(`{"artist":"Artist Name"}`),
	}

	// Mock store to return token
	mocks.store.EXPECT().
		GetTokenByTokenCID(ctx, tokenCID.String()).
		Return(token, nil)

	// Mock metadata enhancer
	mocks.metadataEnhancer.EXPECT().
		Enhance(ctx, tokenCID, normalizedMetadata).
		Return(enhancedMetadata, nil)

	// Mock VendorJsonHash
	hash := []byte("vendorhash123")
	mocks.metadataEnhancer.EXPECT().
		VendorJsonHash(enhancedMetadata).
		Return(hash, nil)

	// Mock store UpsertEnrichmentSource to return error
	storeErr := errors.New("database error")
	mocks.store.EXPECT().
		UpsertEnrichmentSource(ctx, gomock.Any()).
		Return(storeErr)

	result, err := mocks.executor.EnhanceTokenMetadata(ctx, tokenCID, normalizedMetadata)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to upsert enrichment source")
	assert.Nil(t, result)
}

// ====================================================================================
// GetEthereumTokenCIDsByOwnerWithinBlockRange Tests
// ====================================================================================

func TestGetEthereumTokenCIDsByOwnerWithinBlockRange_Success(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	address := "0xdadB0d80178819F2319190D340ce9A924f783711"
	fromBlock := uint64(100)
	toBlock := uint64(200)

	expectedTokens := []domain.TokenWithBlock{
		{
			TokenCID:    domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1"),
			BlockNumber: 150,
		},
		{
			TokenCID:    domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "2"),
			BlockNumber: 175,
		},
	}

	// Mock ethClient
	mocks.ethClient.EXPECT().
		GetTokenCIDsByOwnerAndBlockRange(ctx, address, fromBlock, toBlock).
		Return(expectedTokens, nil)

	result, err := mocks.executor.GetEthereumTokenCIDsByOwnerWithinBlockRange(ctx, address, fromBlock, toBlock)

	assert.NoError(t, err)
	assert.Equal(t, expectedTokens, result)
}

func TestGetEthereumTokenCIDsByOwnerWithinBlockRange_UnsupportedAddress(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	address := "tz1fPKAtsYydh4f1wfWNfeNxWYu72TmM48fu" // Tezos address
	fromBlock := uint64(100)
	toBlock := uint64(200)

	result, err := mocks.executor.GetEthereumTokenCIDsByOwnerWithinBlockRange(ctx, address, fromBlock, toBlock)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported blockchain for address")
	assert.Nil(t, result)
}

func TestGetEthereumTokenCIDsByOwnerWithinBlockRange_ClientError(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	address := "0xdadB0d80178819F2319190D340ce9A924f783711"
	fromBlock := uint64(100)
	toBlock := uint64(200)

	// Mock ethClient to return error
	clientErr := errors.New("client error")
	mocks.ethClient.EXPECT().
		GetTokenCIDsByOwnerAndBlockRange(ctx, address, fromBlock, toBlock).
		Return(nil, clientErr)

	result, err := mocks.executor.GetEthereumTokenCIDsByOwnerWithinBlockRange(ctx, address, fromBlock, toBlock)

	assert.Error(t, err)
	assert.Equal(t, clientErr, err)
	assert.Nil(t, result)
}

// ====================================================================================
// GetTezosTokenCIDsByAccountWithinBlockRange Tests
// ====================================================================================

func TestGetTezosTokenCIDsByAccountWithinBlockRange_Success(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	address := "tz1owner123"
	fromBlock := uint64(100)
	toBlock := uint64(200)
	const pageSize = tezos.MAX_PAGE_SIZE // 10,000

	// Create first page - exactly pageSize results to trigger pagination
	balances1 := make([]tezos.TzKTTokenBalance, pageSize)
	for i := 0; i < pageSize; i++ {
		balances1[i] = tezos.TzKTTokenBalance{
			Token: tezos.TzKTTokenInfo{
				Contract: tezos.TzKTContract{
					Address: "KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton",
				},
				TokenID:  fmt.Sprintf("%d", i),
				Standard: "fa2",
			},
			LastLevel: 150 + uint64(i%100), //nolint:gosec,G115
		}
	}

	// Create second page - partial results (less than pageSize) to end pagination
	balances2 := make([]tezos.TzKTTokenBalance, 50)
	for i := 0; i < 50; i++ {
		balances2[i] = tezos.TzKTTokenBalance{
			Token: tezos.TzKTTokenInfo{
				Contract: tezos.TzKTContract{
					Address: "KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton",
				},
				TokenID:  fmt.Sprintf("%d", pageSize+i),
				Standard: "fa2",
			},
			LastLevel: 160 + uint64(i), //nolint:gosec,G115
		}
	}

	// Mock first API call with offset 0 - returns full page (exactly pageSize results)
	mocks.tzktClient.EXPECT().
		GetTokenBalancesByAccountWithinBlockRange(ctx, address, fromBlock, toBlock, pageSize, 0).
		Return(balances1, nil)

	// Mock ChainID calls for first page (one per token)
	mocks.tzktClient.EXPECT().
		ChainID().
		Return(domain.ChainTezosMainnet).
		Times(pageSize)

	// Mock second API call with offset = pageSize - returns partial page (< pageSize results)
	// This triggers end of pagination since len(balances2) < pageSize
	mocks.tzktClient.EXPECT().
		GetTokenBalancesByAccountWithinBlockRange(ctx, address, fromBlock, toBlock, pageSize, pageSize).
		Return(balances2, nil)

	// Mock ChainID calls for second page (one per token)
	mocks.tzktClient.EXPECT().
		ChainID().
		Return(domain.ChainTezosMainnet).
		Times(len(balances2))

	result, err := mocks.executor.GetTezosTokenCIDsByAccountWithinBlockRange(ctx, address, fromBlock, toBlock)

	assert.NoError(t, err)
	assert.Len(t, result, pageSize+len(balances2), "Should return all tokens from both pages")

	// Verify pagination worked correctly
	assert.Equal(t, uint64(150), result[0].BlockNumber, "First token from page 1")
	assert.Equal(t, uint64(160), result[pageSize].BlockNumber, "First token from page 2")
	assert.Equal(t, uint64(209), result[len(result)-1].BlockNumber, "Last token from page 2")
}

func TestGetTezosTokenCIDsByAccountWithinBlockRange_EmptyResult(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	address := "tz1owner123"
	fromBlock := uint64(100)
	toBlock := uint64(200)

	// Mock TzKT client to return empty balances
	mocks.tzktClient.EXPECT().
		GetTokenBalancesByAccountWithinBlockRange(ctx, address, fromBlock, toBlock, gomock.Any(), 0).
		Return([]tezos.TzKTTokenBalance{}, nil)

	result, err := mocks.executor.GetTezosTokenCIDsByAccountWithinBlockRange(ctx, address, fromBlock, toBlock)

	assert.NoError(t, err)
	assert.Empty(t, result)
}

func TestGetTezosTokenCIDsByAccountWithinBlockRange_ClientError(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	address := "tz1owner123"
	fromBlock := uint64(100)
	toBlock := uint64(200)

	// Mock TzKT client to return error
	clientErr := errors.New("tzkt error")
	mocks.tzktClient.EXPECT().
		GetTokenBalancesByAccountWithinBlockRange(ctx, address, fromBlock, toBlock, gomock.Any(), 0).
		Return(nil, clientErr)

	result, err := mocks.executor.GetTezosTokenCIDsByAccountWithinBlockRange(ctx, address, fromBlock, toBlock)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get token balances from TzKT")
	assert.Nil(t, result)
}

// ====================================================================================
// GetLatestEthereumBlock Tests
// ====================================================================================

func TestGetLatestEthereumBlock_Success(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	expectedBlock := uint64(12345678)

	// Mock ethClient HeaderByNumber
	header := &types.Header{
		Number: big.NewInt(int64(expectedBlock)),
	}
	mocks.ethClient.EXPECT().
		HeaderByNumber(ctx, nil).
		Return(header, nil)

	result, err := mocks.executor.GetLatestEthereumBlock(ctx)

	assert.NoError(t, err)
	assert.Equal(t, expectedBlock, result)
}

func TestGetLatestEthereumBlock_ClientError(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()

	// Mock ethClient HeaderByNumber to return error
	clientErr := errors.New("client error")
	mocks.ethClient.EXPECT().
		HeaderByNumber(ctx, nil).
		Return(nil, clientErr)

	result, err := mocks.executor.GetLatestEthereumBlock(ctx)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get latest block")
	assert.Equal(t, uint64(0), result)
}

// ====================================================================================
// GetLatestTezosBlock Tests
// ====================================================================================

func TestGetLatestTezosBlock_Success(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	expectedBlock := uint64(234567)

	// Mock tzktClient GetLatestBlock
	mocks.tzktClient.EXPECT().
		GetLatestBlock(ctx).
		Return(expectedBlock, nil)

	result, err := mocks.executor.GetLatestTezosBlock(ctx)

	assert.NoError(t, err)
	assert.Equal(t, expectedBlock, result)
}

func TestGetLatestTezosBlock_ClientError(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()

	// Mock tzktClient GetLatestBlock to return error
	clientErr := errors.New("tzkt error")
	mocks.tzktClient.EXPECT().
		GetLatestBlock(ctx).
		Return(uint64(0), clientErr)

	result, err := mocks.executor.GetLatestTezosBlock(ctx)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get latest Tezos block")
	assert.Equal(t, uint64(0), result)
}

// ====================================================================================
// GetIndexingBlockRangeForAddress Tests
// ====================================================================================

func TestGetIndexingBlockRangeForAddress_Success(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	address := "0xowner123"
	chainID := domain.ChainEthereumMainnet
	minBlock := uint64(100)
	maxBlock := uint64(200)

	// Mock store GetIndexingBlockRangeForAddress
	mocks.store.EXPECT().
		GetIndexingBlockRangeForAddress(ctx, address, chainID).
		Return(minBlock, maxBlock, nil)

	result, err := mocks.executor.GetIndexingBlockRangeForAddress(ctx, address, chainID)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, minBlock, result.MinBlock)
	assert.Equal(t, maxBlock, result.MaxBlock)
}

func TestGetIndexingBlockRangeForAddress_StoreError(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	address := "0xowner123"
	chainID := domain.ChainEthereumMainnet

	// Mock store GetIndexingBlockRangeForAddress to return error
	storeErr := errors.New("database error")
	mocks.store.EXPECT().
		GetIndexingBlockRangeForAddress(ctx, address, chainID).
		Return(uint64(0), uint64(0), storeErr)

	result, err := mocks.executor.GetIndexingBlockRangeForAddress(ctx, address, chainID)

	assert.Error(t, err)
	assert.Nil(t, result)
}

// ====================================================================================
// UpdateIndexingBlockRangeForAddress Tests
// ====================================================================================

func TestUpdateIndexingBlockRangeForAddress_Success(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	address := "0xowner123"
	chainID := domain.ChainEthereumMainnet
	minBlock := uint64(100)
	maxBlock := uint64(200)

	// Mock store UpdateIndexingBlockRangeForAddress
	mocks.store.EXPECT().
		UpdateIndexingBlockRangeForAddress(ctx, address, chainID, minBlock, maxBlock).
		Return(nil)

	err := mocks.executor.UpdateIndexingBlockRangeForAddress(ctx, address, chainID, minBlock, maxBlock)

	assert.NoError(t, err)
}

func TestUpdateIndexingBlockRangeForAddress_StoreError(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	address := "0xowner123"
	chainID := domain.ChainEthereumMainnet
	minBlock := uint64(100)
	maxBlock := uint64(200)

	// Mock store UpdateIndexingBlockRangeForAddress to return error
	storeErr := errors.New("database error")
	mocks.store.EXPECT().
		UpdateIndexingBlockRangeForAddress(ctx, address, chainID, minBlock, maxBlock).
		Return(storeErr)

	err := mocks.executor.UpdateIndexingBlockRangeForAddress(ctx, address, chainID, minBlock, maxBlock)

	assert.Error(t, err)
}

// ====================================================================================
// EnsureWatchedAddressExists Tests
// ====================================================================================

func TestEnsureWatchedAddressExists_Success(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	address := "0xowner123"
	chain := domain.ChainEthereumMainnet

	// Mock store EnsureWatchedAddressExists
	mocks.store.EXPECT().
		EnsureWatchedAddressExists(ctx, address, chain).
		Return(nil)

	err := mocks.executor.EnsureWatchedAddressExists(ctx, address, chain)

	assert.NoError(t, err)
}

func TestEnsureWatchedAddressExists_StoreError(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	address := "0xowner123"
	chain := domain.ChainEthereumMainnet

	// Mock store EnsureWatchedAddressExists to return error
	storeErr := errors.New("database error")
	mocks.store.EXPECT().
		EnsureWatchedAddressExists(ctx, address, chain).
		Return(storeErr)

	err := mocks.executor.EnsureWatchedAddressExists(ctx, address, chain)

	assert.Error(t, err)
}

// ====================================================================================
// IndexTokenWithMinimalProvenancesByBlockchainEvent Tests
// ====================================================================================

func TestIndexTokenWithMinimalProvenancesByBlockchainEvent_Success_ERC721_TokenExists(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	fromAddr := "0xsender123"
	toAddr := "0xreceiver123"
	timestamp := time.Now()

	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeTransfer,
		FromAddress:     &fromAddr,
		ToAddress:       &toAddr,
		Quantity:        "1",
		TxHash:          "0xtx123",
		BlockNumber:     100,
		BlockHash:       stringPtr("0xblock123"),
		Timestamp:       timestamp,
	}

	tokenCID := event.TokenCID()

	// Mock CheckTokenExists to return true (token exists in DB)
	token := &schema.Token{
		ID:       1,
		TokenCID: tokenCID.String(),
	}
	mocks.store.EXPECT().
		GetTokenByTokenCID(ctx, tokenCID.String()).
		Return(token, nil)

	// Mock JSON marshal
	rawEventData := []byte(`{"chain":"eip155:1"}`)
	mocks.json.EXPECT().
		Marshal(event).
		Return(rawEventData, nil)

	// Mock store CreateTokenWithProvenances
	mocks.store.EXPECT().
		CreateTokenWithProvenances(ctx, gomock.Any()).
		DoAndReturn(func(ctx context.Context, input store.CreateTokenWithProvenancesInput) error {
			assert.Equal(t, tokenCID.String(), input.Token.TokenCID)
			assert.Equal(t, domain.ChainEthereumMainnet, input.Token.Chain)
			assert.Equal(t, domain.StandardERC721, input.Token.Standard)
			assert.False(t, input.Token.Burned)
			// For ERC721 transfer, we expect 2 balances (from and to addresses)
			// From address has balance 0 (lost the token), to address has balance 1
			assert.Len(t, input.Balances, 1) // Only to address has positive balance
			assert.Equal(t, toAddr, input.Balances[0].OwnerAddress)
			assert.Equal(t, "1", input.Balances[0].Quantity)
			assert.Len(t, input.Events, 1)
			assert.Equal(t, schema.ProvenanceEventTypeTransfer, input.Events[0].EventType)
			return nil
		})

	err := mocks.executor.IndexTokenWithMinimalProvenancesByBlockchainEvent(ctx, event)

	assert.NoError(t, err)
}

func TestIndexTokenWithMinimalProvenancesByBlockchainEvent_Success_ERC721_TokenNotInDB(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	fromAddr := domain.ETHEREUM_ZERO_ADDRESS
	toAddr := "0xreceiver123"
	timestamp := time.Now()

	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeMint,
		FromAddress:     &fromAddr,
		ToAddress:       &toAddr,
		Quantity:        "1",
		TxHash:          "0xtx123",
		BlockNumber:     100,
		BlockHash:       stringPtr("0xblock123"),
		Timestamp:       timestamp,
	}

	tokenCID := event.TokenCID()

	// Mock CheckTokenExists to return false (token doesn't exist in DB)
	mocks.store.EXPECT().
		GetTokenByTokenCID(ctx, tokenCID.String()).
		Return(nil, nil)

	// Mock verifyTokenExistsOnChain to return true (token exists on chain)
	mocks.ethClient.EXPECT().
		TokenExists(ctx, event.ContractAddress, event.TokenNumber, event.Standard).
		Return(true, nil)

	// Mock JSON marshal
	rawEventData := []byte(`{"chain":"eip155:1"}`)
	mocks.json.EXPECT().
		Marshal(event).
		Return(rawEventData, nil)

	// Mock store CreateTokenWithProvenances
	mocks.store.EXPECT().
		CreateTokenWithProvenances(ctx, gomock.Any()).
		DoAndReturn(func(ctx context.Context, input store.CreateTokenWithProvenancesInput) error {
			assert.Equal(t, tokenCID.String(), input.Token.TokenCID)
			assert.Len(t, input.Balances, 1)
			assert.Equal(t, toAddr, input.Balances[0].OwnerAddress)
			assert.Equal(t, "1", input.Balances[0].Quantity)
			return nil
		})

	err := mocks.executor.IndexTokenWithMinimalProvenancesByBlockchainEvent(ctx, event)

	assert.NoError(t, err)
}

func TestIndexTokenWithMinimalProvenancesByBlockchainEvent_Success_ERC1155(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	fromAddr := "0xsender123"
	toAddr := "0xreceiver123"
	timestamp := time.Now()

	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC1155,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeTransfer,
		FromAddress:     &fromAddr,
		ToAddress:       &toAddr,
		Quantity:        "5",
		TxHash:          "0xtx123",
		BlockNumber:     100,
		BlockHash:       stringPtr("0xblock123"),
		Timestamp:       timestamp,
	}

	tokenCID := event.TokenCID()

	// Mock CheckTokenExists to return true
	token := &schema.Token{
		ID:       1,
		TokenCID: tokenCID.String(),
	}
	mocks.store.EXPECT().
		GetTokenByTokenCID(ctx, tokenCID.String()).
		Return(token, nil)

	// Mock JSON marshal
	rawEventData := []byte(`{"chain":"eip155:1"}`)
	mocks.json.EXPECT().
		Marshal(event).
		Return(rawEventData, nil)

	// Mock ERC1155BalanceOf for both addresses
	mocks.ethClient.EXPECT().
		ERC1155BalanceOf(ctx, event.ContractAddress, fromAddr, event.TokenNumber).
		Return("3", nil) // Sender still has 3 after transfer

	mocks.ethClient.EXPECT().
		ERC1155BalanceOf(ctx, event.ContractAddress, toAddr, event.TokenNumber).
		Return("5", nil) // Receiver has 5 after transfer

	// Mock store CreateTokenWithProvenances
	mocks.store.EXPECT().
		CreateTokenWithProvenances(ctx, gomock.Any()).
		DoAndReturn(func(ctx context.Context, input store.CreateTokenWithProvenancesInput) error {
			assert.Len(t, input.Balances, 2) // Both addresses have positive balances
			assert.Equal(t, fromAddr, input.Balances[0].OwnerAddress)
			assert.Equal(t, "3", input.Balances[0].Quantity)
			assert.Equal(t, toAddr, input.Balances[1].OwnerAddress)
			assert.Equal(t, "5", input.Balances[1].Quantity)
			return nil
		})

	err := mocks.executor.IndexTokenWithMinimalProvenancesByBlockchainEvent(ctx, event)

	assert.NoError(t, err)
}

func TestIndexTokenWithMinimalProvenancesByBlockchainEvent_Success_FA2(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	fromAddr := "tz1sender123"
	toAddr := "tz1receiver123"
	timestamp := time.Now()

	event := &domain.BlockchainEvent{
		Chain:           domain.ChainTezosMainnet,
		Standard:        domain.StandardFA2,
		ContractAddress: "KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton",
		TokenNumber:     "1",
		EventType:       domain.EventTypeTransfer,
		FromAddress:     &fromAddr,
		ToAddress:       &toAddr,
		Quantity:        "2",
		TxHash:          "op123",
		BlockNumber:     100,
		BlockHash:       stringPtr("block123"),
		Timestamp:       timestamp,
	}

	tokenCID := event.TokenCID()

	// Mock CheckTokenExists to return true
	token := &schema.Token{
		ID:       1,
		TokenCID: tokenCID.String(),
	}
	mocks.store.EXPECT().
		GetTokenByTokenCID(ctx, tokenCID.String()).
		Return(token, nil)

	// Mock JSON marshal
	rawEventData := []byte(`{"chain":"tezos:mainnet"}`)
	mocks.json.EXPECT().
		Marshal(event).
		Return(rawEventData, nil)

	// Mock GetTokenOwnerBalance for both addresses
	mocks.tzktClient.EXPECT().
		GetTokenOwnerBalance(ctx, event.ContractAddress, event.TokenNumber, fromAddr).
		Return("1", nil) // Sender has 1 left

	mocks.tzktClient.EXPECT().
		GetTokenOwnerBalance(ctx, event.ContractAddress, event.TokenNumber, toAddr).
		Return("2", nil) // Receiver has 2

	// Mock store CreateTokenWithProvenances
	mocks.store.EXPECT().
		CreateTokenWithProvenances(ctx, gomock.Any()).
		DoAndReturn(func(ctx context.Context, input store.CreateTokenWithProvenancesInput) error {
			assert.Len(t, input.Balances, 2) // Both addresses have positive balances
			assert.Equal(t, fromAddr, input.Balances[0].OwnerAddress)
			assert.Equal(t, "1", input.Balances[0].Quantity)
			assert.Equal(t, toAddr, input.Balances[1].OwnerAddress)
			assert.Equal(t, "2", input.Balances[1].Quantity)
			return nil
		})

	err := mocks.executor.IndexTokenWithMinimalProvenancesByBlockchainEvent(ctx, event)

	assert.NoError(t, err)
}

func TestIndexTokenWithMinimalProvenancesByBlockchainEvent_InvalidEvent(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()

	// Create an invalid event
	event := &domain.BlockchainEvent{}

	err := mocks.executor.IndexTokenWithMinimalProvenancesByBlockchainEvent(ctx, event)

	assert.Error(t, err)
	assert.Equal(t, domain.ErrInvalidBlockchainEvent, err)
}

func TestIndexTokenWithMinimalProvenancesByBlockchainEvent_TokenNotFoundOnChain(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	fromAddr := domain.ETHEREUM_ZERO_ADDRESS
	toAddr := "0xreceiver123"
	timestamp := time.Now()

	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeMint,
		FromAddress:     &fromAddr,
		ToAddress:       &toAddr,
		Quantity:        "1",
		TxHash:          "0xtx123",
		BlockNumber:     100,
		BlockHash:       stringPtr("0xblock123"),
		Timestamp:       timestamp,
	}

	tokenCID := event.TokenCID()

	// Mock CheckTokenExists to return false (token doesn't exist in DB)
	mocks.store.EXPECT().
		GetTokenByTokenCID(ctx, tokenCID.String()).
		Return(nil, nil)

	// Mock verifyTokenExistsOnChain to return false (token doesn't exist on chain)
	mocks.ethClient.EXPECT().
		TokenExists(ctx, event.ContractAddress, event.TokenNumber, event.Standard).
		Return(false, nil)

	err := mocks.executor.IndexTokenWithMinimalProvenancesByBlockchainEvent(ctx, event)

	assert.Error(t, err)
	// This should return a non-retryable error
	assert.IsType(t, &temporal.ApplicationError{}, err)
	var appErr *temporal.ApplicationError
	errOk := errors.As(err, &appErr)
	assert.True(t, errOk)
	assert.True(t, appErr.NonRetryable())
	assert.Contains(t, err.Error(), "token not found on chain")
}

func TestIndexTokenWithMinimalProvenancesByBlockchainEvent_BalanceFetchError(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	fromAddr := "0xsender123"
	toAddr := "0xreceiver123"
	timestamp := time.Now()

	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC1155,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeTransfer,
		FromAddress:     &fromAddr,
		ToAddress:       &toAddr,
		Quantity:        "5",
		TxHash:          "0xtx123",
		BlockNumber:     100,
		BlockHash:       stringPtr("0xblock123"),
		Timestamp:       timestamp,
	}

	tokenCID := event.TokenCID()

	// Mock CheckTokenExists to return true
	token := &schema.Token{
		ID:       1,
		TokenCID: tokenCID.String(),
	}
	mocks.store.EXPECT().
		GetTokenByTokenCID(ctx, tokenCID.String()).
		Return(token, nil)

	// Mock JSON marshal
	rawEventData := []byte(`{"chain":"eip155:1"}`)
	mocks.json.EXPECT().
		Marshal(event).
		Return(rawEventData, nil)

	// Mock ERC1155BalanceOf to return error
	balanceErr := errors.New("failed to get balance")
	mocks.ethClient.EXPECT().
		ERC1155BalanceOf(ctx, event.ContractAddress, fromAddr, event.TokenNumber).
		Return("", balanceErr)

	err := mocks.executor.IndexTokenWithMinimalProvenancesByBlockchainEvent(ctx, event)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get ERC1155 token balance")
}

// ====================================================================================
// IndexTokenWithMinimalProvenancesByTokenCID Tests
// ====================================================================================

func TestIndexTokenWithMinimalProvenancesByTokenCID_Success_ERC721(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	contractAddress := "0x1234567890123456789012345678901234567890"
	tokenNumber := "1"
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, contractAddress, tokenNumber)
	ownerAddr := "0xowner123"

	// Mock CheckTokenExists to return false (token doesn't exist in DB)
	mocks.store.EXPECT().
		GetTokenByTokenCID(ctx, tokenCID.String()).
		Return(nil, nil)

	// Mock verifyTokenExistsOnChain to return true
	mocks.ethClient.EXPECT().
		TokenExists(ctx, contractAddress, tokenNumber, domain.StandardERC721).
		Return(true, nil)

	// Mock ERC721OwnerOf to return current owner
	mocks.ethClient.EXPECT().
		ERC721OwnerOf(ctx, contractAddress, tokenNumber).
		Return(ownerAddr, nil)

	// Mock store CreateTokenWithProvenances
	mocks.store.EXPECT().
		CreateTokenWithProvenances(ctx, gomock.Any()).
		DoAndReturn(func(ctx context.Context, input store.CreateTokenWithProvenancesInput) error {
			assert.Equal(t, tokenCID.String(), input.Token.TokenCID)
			assert.Equal(t, ownerAddr, *input.Token.CurrentOwner)
			assert.False(t, input.Token.Burned)
			assert.Len(t, input.Balances, 1)
			assert.Equal(t, ownerAddr, input.Balances[0].OwnerAddress)
			assert.Equal(t, "1", input.Balances[0].Quantity)
			assert.Empty(t, input.Events) // No events for minimal provenance by TokenCID
			return nil
		})

	err := mocks.executor.IndexTokenWithMinimalProvenancesByTokenCID(ctx, tokenCID)

	assert.NoError(t, err)
}

func TestIndexTokenWithMinimalProvenancesByTokenCID_Success_ERC721_Burned(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	contractAddress := "0x1234567890123456789012345678901234567890"
	tokenNumber := "1"
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, contractAddress, tokenNumber)

	// Mock CheckTokenExists to return false
	mocks.store.EXPECT().
		GetTokenByTokenCID(ctx, tokenCID.String()).
		Return(nil, nil)

	// Mock verifyTokenExistsOnChain to return true
	mocks.ethClient.EXPECT().
		TokenExists(ctx, contractAddress, tokenNumber, domain.StandardERC721).
		Return(true, nil)

	// Mock ERC721OwnerOf to return zero address (burned)
	mocks.ethClient.EXPECT().
		ERC721OwnerOf(ctx, contractAddress, tokenNumber).
		Return(domain.ETHEREUM_ZERO_ADDRESS, nil)

	// Mock store CreateTokenWithProvenances
	mocks.store.EXPECT().
		CreateTokenWithProvenances(ctx, gomock.Any()).
		DoAndReturn(func(ctx context.Context, input store.CreateTokenWithProvenancesInput) error {
			assert.True(t, input.Token.Burned)
			assert.Empty(t, input.Balances) // No balances for burned token
			return nil
		})

	err := mocks.executor.IndexTokenWithMinimalProvenancesByTokenCID(ctx, tokenCID)

	assert.NoError(t, err)
}

func TestIndexTokenWithMinimalProvenancesByTokenCID_Success_ERC1155(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	contractAddress := "0x1234567890123456789012345678901234567890"
	tokenNumber := "1"
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC1155, contractAddress, tokenNumber)

	// Mock CheckTokenExists to return false
	mocks.store.EXPECT().
		GetTokenByTokenCID(ctx, tokenCID.String()).
		Return(nil, nil)

	// Mock verifyTokenExistsOnChain to return true
	mocks.ethClient.EXPECT().
		TokenExists(ctx, contractAddress, tokenNumber, domain.StandardERC1155).
		Return(true, nil)

	// Mock ERC1155Balances to return balances map
	balances := map[string]string{
		"0xowner1": "10",
		"0xowner2": "5",
	}
	mocks.ethClient.EXPECT().
		ERC1155Balances(ctx, contractAddress, tokenNumber).
		Return(balances, nil)

	// Mock store CreateTokenWithProvenances
	mocks.store.EXPECT().
		CreateTokenWithProvenances(ctx, gomock.Any()).
		DoAndReturn(func(ctx context.Context, input store.CreateTokenWithProvenancesInput) error {
			assert.Equal(t, tokenCID.String(), input.Token.TokenCID)
			assert.Len(t, input.Balances, 2)
			assert.False(t, input.Token.Burned)
			return nil
		})

	err := mocks.executor.IndexTokenWithMinimalProvenancesByTokenCID(ctx, tokenCID)

	assert.NoError(t, err)
}

func TestIndexTokenWithMinimalProvenancesByTokenCID_Success_FA2(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	tokenCID := domain.NewTokenCID(domain.ChainTezosMainnet, domain.StandardFA2, "KT1abc123", "1")

	// Mock CheckTokenExists to return false
	mocks.store.EXPECT().
		GetTokenByTokenCID(ctx, tokenCID.String()).
		Return(nil, nil)

	// Mock GetTokenMetadata to verify token exists on chain
	mocks.tzktClient.EXPECT().
		GetTokenMetadata(ctx, "KT1abc123", "1").
		Return(map[string]interface{}{"name": "Test Token"}, nil)

	// Mock GetTokenBalances to return balances
	balances := []tezos.TzKTTokenBalance{
		{
			Account: tezos.TzKTAccount{Address: "tz1owner1"},
			Balance: "10",
		},
		{
			Account: tezos.TzKTAccount{Address: "tz1owner2"},
			Balance: "5",
		},
	}
	mocks.tzktClient.EXPECT().
		GetTokenBalances(ctx, "KT1abc123", "1").
		Return(balances, nil)

	// Mock store CreateTokenWithProvenances
	mocks.store.EXPECT().
		CreateTokenWithProvenances(ctx, gomock.Any()).
		DoAndReturn(func(ctx context.Context, input store.CreateTokenWithProvenancesInput) error {
			assert.Equal(t, tokenCID.String(), input.Token.TokenCID)
			assert.Len(t, input.Balances, 2)
			assert.False(t, input.Token.Burned)
			return nil
		})

	err := mocks.executor.IndexTokenWithMinimalProvenancesByTokenCID(ctx, tokenCID)

	assert.NoError(t, err)
}

func TestIndexTokenWithMinimalProvenancesByTokenCID_TokenNotFoundOnChain(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")

	// Mock CheckTokenExists to return false
	mocks.store.EXPECT().
		GetTokenByTokenCID(ctx, tokenCID.String()).
		Return(nil, nil)

	// Mock verifyTokenExistsOnChain to return false
	mocks.ethClient.EXPECT().
		TokenExists(ctx, "0x1234567890123456789012345678901234567890", "1", domain.StandardERC721).
		Return(false, nil)

	err := mocks.executor.IndexTokenWithMinimalProvenancesByTokenCID(ctx, tokenCID)

	assert.Error(t, err)
	assert.IsType(t, &temporal.ApplicationError{}, err)
	var appErr *temporal.ApplicationError
	errOk := errors.As(err, &appErr)
	assert.True(t, errOk)
	assert.True(t, appErr.NonRetryable())
	assert.Contains(t, err.Error(), "token not found on chain")
}

func TestIndexTokenWithMinimalProvenancesByTokenCID_ERC1155Timeout(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	contractAddress := "0x1234567890123456789012345678901234567890"
	tokenNumber := "1"
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC1155, contractAddress, tokenNumber)

	// Mock CheckTokenExists to return false
	mocks.store.EXPECT().
		GetTokenByTokenCID(ctx, tokenCID.String()).
		Return(nil, nil)

	// Mock verifyTokenExistsOnChain to return true
	mocks.ethClient.EXPECT().
		TokenExists(ctx, contractAddress, tokenNumber, domain.StandardERC1155).
		Return(true, nil)

	// Mock ERC1155Balances to return timeout error (but with partial balances)
	mocks.ethClient.EXPECT().
		ERC1155Balances(ctx, contractAddress, tokenNumber).
		Return(nil, context.DeadlineExceeded)

	// Mock store CreateTokenWithProvenances - should continue with partial balances
	mocks.store.EXPECT().
		CreateTokenWithProvenances(ctx, gomock.Any()).
		DoAndReturn(func(ctx context.Context, input store.CreateTokenWithProvenancesInput) error {
			assert.Equal(t, tokenCID.String(), input.Token.TokenCID)
			assert.Len(t, input.Balances, 0) // No balances
			return nil
		})

	err := mocks.executor.IndexTokenWithMinimalProvenancesByTokenCID(ctx, tokenCID)

	assert.NoError(t, err) // Should not error on timeout, continues with partial balances
}

// ====================================================================================
// IndexTokenWithFullProvenancesByTokenCID Tests
// ====================================================================================

func TestIndexTokenWithFullProvenancesByTokenCID_Success_ERC721(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	contractAddress := "0x1234567890123456789012345678901234567890"
	tokenNumber := "1"
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, contractAddress, tokenNumber)

	// Mock CheckTokenExists to return false
	mocks.store.EXPECT().
		GetTokenByTokenCID(ctx, tokenCID.String()).
		Return(nil, nil)

	// Mock verifyTokenExistsOnChain to return true
	mocks.ethClient.EXPECT().
		TokenExists(ctx, contractAddress, tokenNumber, domain.StandardERC721).
		Return(true, nil)

	// Mock GetTokenEvents to return events
	mintEvent := domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: contractAddress,
		TokenNumber:     tokenNumber,
		EventType:       domain.EventTypeMint,
		FromAddress:     stringPtr(domain.ETHEREUM_ZERO_ADDRESS),
		ToAddress:       stringPtr("0xowner1"),
		Quantity:        "1",
		TxHash:          "0xtx1",
		BlockNumber:     100,
		Timestamp:       time.Now(),
	}
	transferEvent := domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: contractAddress,
		TokenNumber:     tokenNumber,
		EventType:       domain.EventTypeTransfer,
		FromAddress:     stringPtr("0xowner1"),
		ToAddress:       stringPtr("0xowner2"),
		Quantity:        "1",
		TxHash:          "0xtx2",
		BlockNumber:     150,
		Timestamp:       time.Now(),
	}
	events := []domain.BlockchainEvent{mintEvent, transferEvent}

	mocks.ethClient.EXPECT().
		GetTokenEvents(ctx, contractAddress, tokenNumber, domain.StandardERC721).
		Return(events, nil)

	// Mock JSON marshal for each event
	mocks.json.EXPECT().
		Marshal(gomock.Any()).
		Return([]byte(`{"event":"test"}`), nil).
		Times(2)

	// Mock store CreateTokenWithProvenances
	mocks.store.EXPECT().
		CreateTokenWithProvenances(ctx, gomock.Any()).
		DoAndReturn(func(ctx context.Context, input store.CreateTokenWithProvenancesInput) error {
			assert.Equal(t, tokenCID.String(), input.Token.TokenCID)
			assert.Equal(t, "0xowner2", *input.Token.CurrentOwner) // Latest owner
			assert.False(t, input.Token.Burned, "Token should not be burned")

			// Verify ERC721 balance is automatically added for current owner
			assert.Len(t, input.Balances, 1, "Should have exactly one balance for ERC721")
			assert.Equal(t, "0xowner2", input.Balances[0].OwnerAddress, "Balance should be for current owner")
			assert.Equal(t, "1", input.Balances[0].Quantity, "ERC721 balance should be 1")

			// Verify events
			assert.Len(t, input.Events, 2) // Both events included
			return nil
		})

	err := mocks.executor.IndexTokenWithFullProvenancesByTokenCID(ctx, tokenCID)

	assert.NoError(t, err)
}

func TestIndexTokenWithFullProvenancesByTokenCID_Success_ERC721_Burned(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	contractAddress := "0x1234567890123456789012345678901234567890"
	tokenNumber := "1"
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, contractAddress, tokenNumber)

	// Mock CheckTokenExists to return false
	mocks.store.EXPECT().
		GetTokenByTokenCID(ctx, tokenCID.String()).
		Return(nil, nil)

	// Mock verifyTokenExistsOnChain to return true
	mocks.ethClient.EXPECT().
		TokenExists(ctx, contractAddress, tokenNumber, domain.StandardERC721).
		Return(true, nil)

	// Mock GetTokenEvents to return events including burn
	mintEvent := domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: contractAddress,
		TokenNumber:     tokenNumber,
		EventType:       domain.EventTypeMint,
		FromAddress:     stringPtr(domain.ETHEREUM_ZERO_ADDRESS),
		ToAddress:       stringPtr("0xowner1"),
		Quantity:        "1",
		TxHash:          "0xtx1",
		BlockNumber:     100,
		Timestamp:       time.Now().Add(-2 * time.Hour),
	}
	burnEvent := domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: contractAddress,
		TokenNumber:     tokenNumber,
		EventType:       domain.EventTypeBurn,
		FromAddress:     stringPtr("0xowner1"),
		ToAddress:       stringPtr(domain.ETHEREUM_ZERO_ADDRESS),
		Quantity:        "1",
		TxHash:          "0xtx2",
		BlockNumber:     150,
		Timestamp:       time.Now().Add(-1 * time.Hour),
	}
	events := []domain.BlockchainEvent{mintEvent, burnEvent}

	mocks.ethClient.EXPECT().
		GetTokenEvents(ctx, contractAddress, tokenNumber, domain.StandardERC721).
		Return(events, nil)

	// Mock JSON marshal for each event
	mocks.json.EXPECT().
		Marshal(gomock.Any()).
		Return([]byte(`{"event":"test"}`), nil).
		Times(2)

	// Mock store CreateTokenWithProvenances
	mocks.store.EXPECT().
		CreateTokenWithProvenances(ctx, gomock.Any()).
		DoAndReturn(func(ctx context.Context, input store.CreateTokenWithProvenancesInput) error {
			assert.Equal(t, tokenCID.String(), input.Token.TokenCID)
			assert.NotNil(t, input.Token.CurrentOwner, "Burned ERC721 still has current owner (zero address)")
			assert.Equal(t, domain.ETHEREUM_ZERO_ADDRESS, *input.Token.CurrentOwner)
			assert.True(t, input.Token.Burned, "Token should be marked as burned for ERC721 with burn event")

			// For ERC721, balance is added for current owner if not burned
			assert.Len(t, input.Balances, 0, "ERC721 adds balance for current owner if not burned")

			// Verify events
			assert.Len(t, input.Events, 2)
			return nil
		})

	err := mocks.executor.IndexTokenWithFullProvenancesByTokenCID(ctx, tokenCID)

	assert.NoError(t, err)
}

func TestIndexTokenWithFullProvenancesByTokenCID_Success_ERC1155_Burned(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	contractAddress := "0x1234567890123456789012345678901234567890"
	tokenNumber := "1"
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC1155, contractAddress, tokenNumber)

	// Mock CheckTokenExists to return false
	mocks.store.EXPECT().
		GetTokenByTokenCID(ctx, tokenCID.String()).
		Return(nil, nil)

	// Mock verifyTokenExistsOnChain to return true
	mocks.ethClient.EXPECT().
		TokenExists(ctx, contractAddress, tokenNumber, domain.StandardERC1155).
		Return(true, nil)

	// Mock GetTokenEvents - mint and burn events
	mintEvent := domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC1155,
		ContractAddress: contractAddress,
		TokenNumber:     tokenNumber,
		EventType:       domain.EventTypeMint,
		FromAddress:     stringPtr(domain.ETHEREUM_ZERO_ADDRESS),
		ToAddress:       stringPtr("0xowner1"),
		Quantity:        "10",
		TxHash:          "0xtx1",
		BlockNumber:     100,
		Timestamp:       time.Now().Add(-2 * time.Hour),
	}
	burnEvent := domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC1155,
		ContractAddress: contractAddress,
		TokenNumber:     tokenNumber,
		EventType:       domain.EventTypeBurn,
		FromAddress:     stringPtr("0xowner1"),
		ToAddress:       stringPtr(domain.ETHEREUM_ZERO_ADDRESS),
		Quantity:        "10",
		TxHash:          "0xtx2",
		BlockNumber:     150,
		Timestamp:       time.Now().Add(-1 * time.Hour),
	}
	events := []domain.BlockchainEvent{mintEvent, burnEvent}

	mocks.ethClient.EXPECT().
		GetTokenEvents(ctx, contractAddress, tokenNumber, domain.StandardERC1155).
		Return(events, nil)

	// Mock ERC1155BalanceOf for the owner address (will return 0 for burned token)
	// The function fetches balances for all addresses in events, even if ultimately burned
	mocks.ethClient.EXPECT().
		ERC1155BalanceOf(ctx, contractAddress, "0xowner1", tokenNumber).
		Return("0", nil) // Balance is 0 after burning

	// Mock JSON marshal for each event
	mocks.json.EXPECT().
		Marshal(gomock.Any()).
		Return([]byte(`{"event":"test"}`), nil).
		Times(2)

	// Mock store CreateTokenWithProvenances
	mocks.store.EXPECT().
		CreateTokenWithProvenances(ctx, gomock.Any()).
		DoAndReturn(func(ctx context.Context, input store.CreateTokenWithProvenancesInput) error {
			assert.Equal(t, tokenCID.String(), input.Token.TokenCID)
			assert.Nil(t, input.Token.CurrentOwner, "ERC1155 should not have current owner")
			assert.True(t, input.Token.Burned, "Token should be marked as burned for ERC1155 with no balances")

			// Verify no balances for burned ERC1155 (balance of 0 is not added)
			assert.Len(t, input.Balances, 0, "Burned ERC1155 should have no balances (0 balances are filtered out)")

			// Verify events
			assert.Len(t, input.Events, 2)
			return nil
		})

	err := mocks.executor.IndexTokenWithFullProvenancesByTokenCID(ctx, tokenCID)

	assert.NoError(t, err)
}

func TestIndexTokenWithFullProvenancesByTokenCID_Success_FA2(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	contractAddress := "KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton"
	tokenNumber := "1"
	tokenCID := domain.NewTokenCID(domain.ChainTezosMainnet, domain.StandardFA2, contractAddress, tokenNumber)

	// Mock CheckTokenExists to return false
	mocks.store.EXPECT().
		GetTokenByTokenCID(ctx, tokenCID.String()).
		Return(nil, nil)

	// Mock GetTokenMetadata to verify token exists on chain
	mocks.tzktClient.EXPECT().
		GetTokenMetadata(ctx, contractAddress, tokenNumber).
		Return(map[string]interface{}{"name": "Test Token"}, nil)

	// Mock GetTokenBalances
	balances := []tezos.TzKTTokenBalance{
		{
			Account: tezos.TzKTAccount{Address: "tz1owner1"},
			Balance: "5",
		},
	}
	mocks.tzktClient.EXPECT().
		GetTokenBalances(ctx, contractAddress, tokenNumber).
		Return(balances, nil)

	// Mock GetTokenEvents
	mintEvent := domain.BlockchainEvent{
		Chain:           domain.ChainTezosMainnet,
		Standard:        domain.StandardFA2,
		ContractAddress: contractAddress,
		TokenNumber:     tokenNumber,
		EventType:       domain.EventTypeMint,
		FromAddress:     nil,
		ToAddress:       stringPtr("tz1owner1"),
		Quantity:        "5",
		TxHash:          "op1",
		BlockNumber:     100,
		Timestamp:       time.Now(),
	}
	events := []domain.BlockchainEvent{mintEvent}

	mocks.tzktClient.EXPECT().
		GetTokenEvents(ctx, contractAddress, tokenNumber).
		Return(events, nil)

	// Mock JSON marshal
	mocks.json.EXPECT().
		Marshal(gomock.Any()).
		Return([]byte(`{"event":"test"}`), nil)

	// Mock store CreateTokenWithProvenances
	mocks.store.EXPECT().
		CreateTokenWithProvenances(ctx, gomock.Any()).
		DoAndReturn(func(ctx context.Context, input store.CreateTokenWithProvenancesInput) error {
			assert.Equal(t, tokenCID.String(), input.Token.TokenCID)
			assert.Nil(t, input.Token.CurrentOwner, "FA2 should not have current owner (multi-edition)")
			assert.False(t, input.Token.Burned, "Token should not be burned when balances exist")

			// Verify balances
			assert.Len(t, input.Balances, 1)
			assert.Equal(t, "tz1owner1", input.Balances[0].OwnerAddress)
			assert.Equal(t, "5", input.Balances[0].Quantity)

			// Verify events
			assert.Len(t, input.Events, 1)
			return nil
		})

	err := mocks.executor.IndexTokenWithFullProvenancesByTokenCID(ctx, tokenCID)

	assert.NoError(t, err)
}

func TestIndexTokenWithFullProvenancesByTokenCID_Success_FA2_Burned(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	contractAddress := "KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton"
	tokenNumber := "1"
	tokenCID := domain.NewTokenCID(domain.ChainTezosMainnet, domain.StandardFA2, contractAddress, tokenNumber)

	// Mock CheckTokenExists to return false
	mocks.store.EXPECT().
		GetTokenByTokenCID(ctx, tokenCID.String()).
		Return(nil, nil)

	// Mock GetTokenMetadata to verify token exists on chain
	mocks.tzktClient.EXPECT().
		GetTokenMetadata(ctx, contractAddress, tokenNumber).
		Return(map[string]interface{}{"name": "Test Token"}, nil)

	// Mock GetTokenBalances - empty balances means token is burned
	balances := []tezos.TzKTTokenBalance{}
	mocks.tzktClient.EXPECT().
		GetTokenBalances(ctx, contractAddress, tokenNumber).
		Return(balances, nil)

	// Mock GetTokenEvents
	mintEvent := domain.BlockchainEvent{
		Chain:           domain.ChainTezosMainnet,
		Standard:        domain.StandardFA2,
		ContractAddress: contractAddress,
		TokenNumber:     tokenNumber,
		EventType:       domain.EventTypeMint,
		FromAddress:     nil,
		ToAddress:       stringPtr("tz1owner1"),
		Quantity:        "5",
		TxHash:          "op1",
		BlockNumber:     100,
		Timestamp:       time.Now().Add(-2 * time.Hour),
	}
	burnEvent := domain.BlockchainEvent{
		Chain:           domain.ChainTezosMainnet,
		Standard:        domain.StandardFA2,
		ContractAddress: contractAddress,
		TokenNumber:     tokenNumber,
		EventType:       domain.EventTypeBurn,
		FromAddress:     stringPtr("tz1owner1"),
		ToAddress:       nil,
		Quantity:        "5",
		TxHash:          "op2",
		BlockNumber:     150,
		Timestamp:       time.Now().Add(-1 * time.Hour),
	}
	events := []domain.BlockchainEvent{mintEvent, burnEvent}

	mocks.tzktClient.EXPECT().
		GetTokenEvents(ctx, contractAddress, tokenNumber).
		Return(events, nil)

	// Mock JSON marshal for each event
	mocks.json.EXPECT().
		Marshal(gomock.Any()).
		Return([]byte(`{"event":"test"}`), nil).
		Times(2)

	// Mock store CreateTokenWithProvenances
	mocks.store.EXPECT().
		CreateTokenWithProvenances(ctx, gomock.Any()).
		DoAndReturn(func(ctx context.Context, input store.CreateTokenWithProvenancesInput) error {
			assert.Equal(t, tokenCID.String(), input.Token.TokenCID)
			assert.Nil(t, input.Token.CurrentOwner, "FA2 should not have current owner")
			assert.True(t, input.Token.Burned, "Token should be marked as burned for FA2 with no balances")

			// Verify no balances
			assert.Len(t, input.Balances, 0, "Burned FA2 should have no balances")

			// Verify events
			assert.Len(t, input.Events, 2)
			return nil
		})

	err := mocks.executor.IndexTokenWithFullProvenancesByTokenCID(ctx, tokenCID)

	assert.NoError(t, err)
}

func TestIndexTokenWithFullProvenancesByTokenCID_TokenNotFoundOnChain(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")

	// Mock CheckTokenExists to return false
	mocks.store.EXPECT().
		GetTokenByTokenCID(ctx, tokenCID.String()).
		Return(nil, nil)

	// Mock verifyTokenExistsOnChain to return false
	mocks.ethClient.EXPECT().
		TokenExists(ctx, "0x1234567890123456789012345678901234567890", "1", domain.StandardERC721).
		Return(false, nil)

	err := mocks.executor.IndexTokenWithFullProvenancesByTokenCID(ctx, tokenCID)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "token not found on chain")
}

func TestIndexTokenWithFullProvenancesByTokenCID_GetEventsError(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	contractAddress := "0x1234567890123456789012345678901234567890"
	tokenNumber := "1"
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, contractAddress, tokenNumber)

	// Mock CheckTokenExists to return false
	mocks.store.EXPECT().
		GetTokenByTokenCID(ctx, tokenCID.String()).
		Return(nil, nil)

	// Mock verifyTokenExistsOnChain to return true
	mocks.ethClient.EXPECT().
		TokenExists(ctx, contractAddress, tokenNumber, domain.StandardERC721).
		Return(true, nil)

	// Mock GetTokenEvents to return error
	eventsErr := errors.New("failed to get events")
	mocks.ethClient.EXPECT().
		GetTokenEvents(ctx, contractAddress, tokenNumber, domain.StandardERC721).
		Return(nil, eventsErr)

	err := mocks.executor.IndexTokenWithFullProvenancesByTokenCID(ctx, tokenCID)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to fetch token events from Ethereum")
}

// ====================================================================================
// IndexMediaFile Tests
// ====================================================================================

func TestIndexMediaFile_Success(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	url := "https://example.com/image.png"

	// Mock store GetMediaAssetBySourceURL to return nil (not exists)
	mocks.mediaProcessor.EXPECT().
		Provider().
		Return(cloudflare.CLOUDFLARE_PROVIDER_NAME)

	mocks.store.EXPECT().
		GetMediaAssetBySourceURL(ctx, url, schema.StorageProviderCloudflare).
		Return(nil, nil)

	// Mock mediaProcessor.Process to succeed
	mocks.mediaProcessor.EXPECT().
		Process(ctx, url).
		Return(nil)

	err := mocks.executor.IndexMediaFile(ctx, url)

	assert.NoError(t, err)
}

func TestIndexMediaFile_EmptyURL(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	url := ""

	err := mocks.executor.IndexMediaFile(ctx, url)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "media URL is empty")
}

func TestIndexMediaFile_AlreadyExists(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	url := "https://example.com/image.png"

	// Mock store GetMediaAssetBySourceURL to return existing asset
	existingAsset := &schema.MediaAsset{
		ID:        1,
		SourceURL: url,
	}

	mocks.mediaProcessor.EXPECT().
		Provider().
		Return(cloudflare.CLOUDFLARE_PROVIDER_NAME)

	mocks.store.EXPECT().
		GetMediaAssetBySourceURL(ctx, url, schema.StorageProviderCloudflare).
		Return(existingAsset, nil)

	err := mocks.executor.IndexMediaFile(ctx, url)

	// Should succeed without processing (skips existing)
	assert.NoError(t, err)
}

func TestIndexMediaFile_CheckExistingError(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	url := "https://example.com/image.png"

	// Mock store GetMediaAssetBySourceURL to return error
	storeErr := errors.New("database error")

	mocks.mediaProcessor.EXPECT().
		Provider().
		Return(cloudflare.CLOUDFLARE_PROVIDER_NAME)

	mocks.store.EXPECT().
		GetMediaAssetBySourceURL(ctx, url, schema.StorageProviderCloudflare).
		Return(nil, storeErr)

	err := mocks.executor.IndexMediaFile(ctx, url)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to check existing media asset")
}

func TestIndexMediaFile_UnsupportedMediaFile(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	url := "https://example.com/unsupported.xyz"

	// Mock store GetMediaAssetBySourceURL to return nil (not exists)
	mocks.mediaProcessor.EXPECT().
		Provider().
		Return(cloudflare.CLOUDFLARE_PROVIDER_NAME)

	mocks.store.EXPECT().
		GetMediaAssetBySourceURL(ctx, url, schema.StorageProviderCloudflare).
		Return(nil, nil)

	// Mock mediaProcessor.Process to return unsupported file error
	mocks.mediaProcessor.EXPECT().
		Process(ctx, url).
		Return(domain.ErrUnsupportedMediaFile)

	err := mocks.executor.IndexMediaFile(ctx, url)

	// Should not return error for known skip errors
	assert.NoError(t, err)
}

func TestIndexMediaFile_UnsupportedSelfHostedMediaFile(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	url := "https://mysite.com/image.png"

	// Mock store GetMediaAssetBySourceURL to return nil (not exists)
	mocks.mediaProcessor.EXPECT().
		Provider().
		Return(cloudflare.CLOUDFLARE_PROVIDER_NAME)

	mocks.store.EXPECT().
		GetMediaAssetBySourceURL(ctx, url, schema.StorageProviderCloudflare).
		Return(nil, nil)

	// Mock mediaProcessor.Process to return unsupported self-hosted error
	mocks.mediaProcessor.EXPECT().
		Process(ctx, url).
		Return(domain.ErrUnsupportedSelfHostedMediaFile)

	err := mocks.executor.IndexMediaFile(ctx, url)

	// Should not return error for known skip errors
	assert.NoError(t, err)
}

func TestIndexMediaFile_ExceededMaxFileSize(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	url := "https://example.com/huge-file.mp4"

	// Mock store GetMediaAssetBySourceURL to return nil (not exists)
	mocks.mediaProcessor.EXPECT().
		Provider().
		Return(cloudflare.CLOUDFLARE_PROVIDER_NAME)

	mocks.store.EXPECT().
		GetMediaAssetBySourceURL(ctx, url, schema.StorageProviderCloudflare).
		Return(nil, nil)

	// Mock mediaProcessor.Process to return exceeded max file size error
	mocks.mediaProcessor.EXPECT().
		Process(ctx, url).
		Return(domain.ErrExceededMaxFileSize)

	err := mocks.executor.IndexMediaFile(ctx, url)

	// Should not return error for known skip errors
	assert.NoError(t, err)
}

func TestIndexMediaFile_MissingContentLength(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	url := "https://example.com/no-content-length.png"

	// Mock store GetMediaAssetBySourceURL to return nil (not exists)
	mocks.mediaProcessor.EXPECT().
		Provider().
		Return(cloudflare.CLOUDFLARE_PROVIDER_NAME)

	mocks.store.EXPECT().
		GetMediaAssetBySourceURL(ctx, url, schema.StorageProviderCloudflare).
		Return(nil, nil)

	// Mock mediaProcessor.Process to return missing content length error
	mocks.mediaProcessor.EXPECT().
		Process(ctx, url).
		Return(domain.ErrMissingContentLength)

	err := mocks.executor.IndexMediaFile(ctx, url)

	// Should not return error for known skip errors
	assert.NoError(t, err)
}

func TestIndexMediaFile_ProcessError(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	url := "https://example.com/image.png"

	// Mock store GetMediaAssetBySourceURL to return nil (not exists)
	mocks.mediaProcessor.EXPECT().
		Provider().
		Return(cloudflare.CLOUDFLARE_PROVIDER_NAME)

	mocks.store.EXPECT().
		GetMediaAssetBySourceURL(ctx, url, schema.StorageProviderCloudflare).
		Return(nil, nil)

	// Mock mediaProcessor.Process to return unexpected error
	processErr := errors.New("failed to upload")
	mocks.mediaProcessor.EXPECT().
		Process(ctx, url).
		Return(processErr)

	err := mocks.executor.IndexMediaFile(ctx, url)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to process media file")
}

func TestIndexMediaFile_ProviderSelfHosted(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	url := "https://example.com/image.png"

	// Mock store GetMediaAssetBySourceURL to return nil (not exists)
	// Test with self-hosted provider
	mocks.mediaProcessor.EXPECT().
		Provider().
		Return(schema.StorageProviderSelfHosted.String())

	mocks.store.EXPECT().
		GetMediaAssetBySourceURL(ctx, url, schema.StorageProviderSelfHosted).
		Return(nil, nil)

	// Mock mediaProcessor.Process to succeed
	mocks.mediaProcessor.EXPECT().
		Process(ctx, url).
		Return(nil)

	err := mocks.executor.IndexMediaFile(ctx, url)

	assert.NoError(t, err)
}

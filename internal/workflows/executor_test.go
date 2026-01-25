package workflows_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/metadata"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum"
	"github.com/feral-file/ff-indexer-v2/internal/providers/tezos"
	"github.com/feral-file/ff-indexer-v2/internal/registry"
	"github.com/feral-file/ff-indexer-v2/internal/store"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
	"github.com/feral-file/ff-indexer-v2/internal/uri"
	"github.com/feral-file/ff-indexer-v2/internal/webhook"
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
	json             *mocks.MockJSON
	clock            *mocks.MockClock
	httpClient       *mocks.MockHTTPClient
	io               *mocks.MockIO
	temporalActivity *mocks.MockActivity
	blacklist        *mocks.MockBlacklistRegistry
	urlChecker       *mocks.MockURLChecker
	dataURIChecker   *mocks.MockDataURIChecker
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
		json:             mocks.NewMockJSON(ctrl),
		clock:            mocks.NewMockClock(ctrl),
		httpClient:       mocks.NewMockHTTPClient(ctrl),
		io:               mocks.NewMockIO(ctrl),
		temporalActivity: mocks.NewMockActivity(ctrl),
		blacklist:        mocks.NewMockBlacklistRegistry(ctrl),
		urlChecker:       mocks.NewMockURLChecker(ctrl),
		dataURIChecker:   mocks.NewMockDataURIChecker(ctrl),
	}

	tm.executor = workflows.NewExecutor(
		tm.store,
		tm.metadataResolver,
		tm.metadataEnhancer,
		tm.ethClient,
		tm.tzktClient,
		tm.json,
		tm.clock,
		tm.httpClient,
		tm.io,
		tm.temporalActivity,
		tm.blacklist,
		tm.urlChecker,
		tm.dataURIChecker,
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
// ResolveTokenMetadata Tests
// ====================================================================================

func TestResolveTokenMetadata_Success(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")

	mimeType := "image/png"
	animationURL := "https://example.com/animation.mp4"
	publisherName := "Test Publisher"
	publisherURL := "https://publisher.com"

	expectedMetadata := &metadata.NormalizedMetadata{
		Name:        "Test NFT",
		Description: "Test Description",
		Image:       "https://example.com/image.png",
		Animation:   animationURL,
		Artists: []metadata.Artist{
			{DID: "did:example:artist1", Name: "Artist 1"},
			{DID: "did:example:artist2", Name: "Artist 2"},
		},
		Publisher: &metadata.Publisher{
			Name: (*registry.PublisherName)(&publisherName),
			URL:  &publisherURL,
		},
		MimeType: &mimeType,
		Raw:      map[string]interface{}{"name": "Test NFT"},
	}

	tokenID := uint64(123)
	existingOriginJSON := []byte(`{"original":"metadata"}`)
	tokenWithMetadata := &store.TokensWithMetadataResult{
		Token: &schema.Token{
			ID:       tokenID,
			TokenCID: tokenCID.String(),
		},
		Metadata: &schema.TokenMetadata{
			TokenID:    tokenID,
			OriginJSON: existingOriginJSON,
		},
	}

	metadataJSON := []byte(`{"name":"Test NFT","image":"https://example.com/image.png"}`)
	hash := []byte("testhash123")

	now := time.Now()

	// Mock metadata resolver
	mocks.metadataResolver.EXPECT().
		Resolve(ctx, tokenCID).
		Return(expectedMetadata, nil)

	// Mock store to return token with existing metadata
	mocks.store.EXPECT().
		GetTokenWithMetadataByTokenCID(ctx, tokenCID.String()).
		Return(tokenWithMetadata, nil)

	// Mock RawHash
	mocks.metadataResolver.EXPECT().
		RawHash(expectedMetadata).
		Return(hash, metadataJSON, nil)

	// Mock clock
	mocks.clock.EXPECT().
		Now().
		Return(now)

	// Mock store UpsertTokenMetadata
	mocks.store.EXPECT().
		UpsertTokenMetadata(ctx, gomock.Any()).
		DoAndReturn(func(ctx context.Context, input store.CreateTokenMetadataInput) error {
			assert.Equal(t, tokenID, input.TokenID)
			assert.Equal(t, existingOriginJSON, input.OriginJSON) // Should preserve existing origin
			assert.Equal(t, metadataJSON, input.LatestJSON)
			assert.Equal(t, hex.EncodeToString(hash), *input.LatestHash)
			assert.Equal(t, schema.EnrichmentLevelNone, input.EnrichmentLevel)
			assert.Equal(t, now, input.LastRefreshedAt)
			assert.Equal(t, &expectedMetadata.Image, input.ImageURL)
			assert.Equal(t, &animationURL, input.AnimationURL)
			assert.Equal(t, &expectedMetadata.Name, input.Name)
			assert.Equal(t, &expectedMetadata.Description, input.Description)
			assert.Equal(t, &mimeType, input.MimeType)
			assert.Len(t, input.Artists, 2)
			assert.Equal(t, "did:example:artist1", string(input.Artists[0].DID))
			assert.Equal(t, "Artist 1", input.Artists[0].Name)
			assert.NotNil(t, input.Publisher)
			assert.Equal(t, publisherName, *input.Publisher.Name)
			assert.Equal(t, &publisherURL, input.Publisher.URL)
			return nil
		})

	result, err := mocks.executor.ResolveTokenMetadata(ctx, tokenCID)

	assert.NoError(t, err)
	assert.Equal(t, expectedMetadata, result)
}

func TestResolveTokenMetadata_InvalidTokenCID(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	invalidTokenCID := domain.TokenCID("")

	result, err := mocks.executor.ResolveTokenMetadata(ctx, invalidTokenCID)

	assert.Error(t, err)
	assert.Equal(t, domain.ErrInvalidTokenCID, err)
	assert.Nil(t, result)
}

func TestResolveTokenMetadata_ResolverError(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")

	// Mock metadata resolver to return error
	resolverErr := errors.New("resolver error")
	mocks.metadataResolver.EXPECT().
		Resolve(ctx, tokenCID).
		Return(nil, resolverErr)

	result, err := mocks.executor.ResolveTokenMetadata(ctx, tokenCID)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to resolve token metadata")
	assert.Nil(t, result)
}

func TestResolveTokenMetadata_GetTokenWithMetadataError(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")

	expectedMetadata := &metadata.NormalizedMetadata{
		Name:  "Test NFT",
		Image: "https://example.com/image.png",
	}

	// Mock metadata resolver
	mocks.metadataResolver.EXPECT().
		Resolve(ctx, tokenCID).
		Return(expectedMetadata, nil)

	// Mock store to return error
	storeErr := errors.New("database error")
	mocks.store.EXPECT().
		GetTokenWithMetadataByTokenCID(ctx, tokenCID.String()).
		Return(nil, storeErr)

	result, err := mocks.executor.ResolveTokenMetadata(ctx, tokenCID)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get token with metadata")
	assert.Nil(t, result)
}

func TestResolveTokenMetadata_TokenNotFound(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")

	expectedMetadata := &metadata.NormalizedMetadata{
		Name:  "Test NFT",
		Image: "https://example.com/image.png",
	}

	// Mock metadata resolver
	mocks.metadataResolver.EXPECT().
		Resolve(ctx, tokenCID).
		Return(expectedMetadata, nil)

	// Mock store to return nil (token not found)
	mocks.store.EXPECT().
		GetTokenWithMetadataByTokenCID(ctx, tokenCID.String()).
		Return(nil, nil)

	result, err := mocks.executor.ResolveTokenMetadata(ctx, tokenCID)

	assert.Error(t, err)
	assert.Equal(t, domain.ErrTokenNotFound, err)
	assert.Nil(t, result)
}

func TestResolveTokenMetadata_RawHashError(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")

	expectedMetadata := &metadata.NormalizedMetadata{
		Name:  "Test NFT",
		Image: "https://example.com/image.png",
	}

	tokenID := uint64(123)
	tokenWithMetadata := &store.TokensWithMetadataResult{
		Token: &schema.Token{
			ID:       tokenID,
			TokenCID: tokenCID.String(),
		},
		Metadata: nil, // No existing metadata
	}

	// Mock metadata resolver
	mocks.metadataResolver.EXPECT().
		Resolve(ctx, tokenCID).
		Return(expectedMetadata, nil)

	// Mock store to return token
	mocks.store.EXPECT().
		GetTokenWithMetadataByTokenCID(ctx, tokenCID.String()).
		Return(tokenWithMetadata, nil)

	// Mock RawHash to return error
	hashErr := errors.New("hash error")
	mocks.metadataResolver.EXPECT().
		RawHash(expectedMetadata).
		Return(nil, nil, hashErr)

	result, err := mocks.executor.ResolveTokenMetadata(ctx, tokenCID)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get raw hash")
	assert.Nil(t, result)
}

func TestResolveTokenMetadata_UpsertMetadataError(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")

	expectedMetadata := &metadata.NormalizedMetadata{
		Name:  "Test NFT",
		Image: "https://example.com/image.png",
	}

	tokenID := uint64(123)
	tokenWithMetadata := &store.TokensWithMetadataResult{
		Token: &schema.Token{
			ID:       tokenID,
			TokenCID: tokenCID.String(),
		},
		Metadata: nil,
	}

	metadataJSON := []byte(`{"name":"Test NFT"}`)
	hash := []byte("testhash123")
	now := time.Now()

	// Mock metadata resolver
	mocks.metadataResolver.EXPECT().
		Resolve(ctx, tokenCID).
		Return(expectedMetadata, nil)

	// Mock store to return token
	mocks.store.EXPECT().
		GetTokenWithMetadataByTokenCID(ctx, tokenCID.String()).
		Return(tokenWithMetadata, nil)

	// Mock RawHash
	mocks.metadataResolver.EXPECT().
		RawHash(expectedMetadata).
		Return(hash, metadataJSON, nil)

	// Mock clock
	mocks.clock.EXPECT().
		Now().
		Return(now)

	// Mock store UpsertTokenMetadata to return error
	upsertErr := errors.New("upsert error")
	mocks.store.EXPECT().
		UpsertTokenMetadata(ctx, gomock.Any()).
		Return(upsertErr)

	result, err := mocks.executor.ResolveTokenMetadata(ctx, tokenCID)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to upsert token metadata")
	assert.Nil(t, result)
}

func TestResolveTokenMetadata_NoExistingMetadata_UsesMetadataJSONAsOrigin(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")

	expectedMetadata := &metadata.NormalizedMetadata{
		Name:        "Test NFT",
		Description: "Description",
		Image:       "https://example.com/image.png",
	}

	tokenID := uint64(123)
	tokenWithMetadata := &store.TokensWithMetadataResult{
		Token: &schema.Token{
			ID:       tokenID,
			TokenCID: tokenCID.String(),
		},
		Metadata: nil, // No existing metadata
	}

	metadataJSON := []byte(`{"name":"Test NFT","image":"https://example.com/image.png"}`)
	hash := []byte("testhash123")
	now := time.Now()

	// Mock metadata resolver
	mocks.metadataResolver.EXPECT().
		Resolve(ctx, tokenCID).
		Return(expectedMetadata, nil)

	// Mock store to return token without metadata
	mocks.store.EXPECT().
		GetTokenWithMetadataByTokenCID(ctx, tokenCID.String()).
		Return(tokenWithMetadata, nil)

	// Mock RawHash
	mocks.metadataResolver.EXPECT().
		RawHash(expectedMetadata).
		Return(hash, metadataJSON, nil)

	// Mock clock
	mocks.clock.EXPECT().
		Now().
		Return(now)

	// Mock store UpsertTokenMetadata - verify origin JSON is same as metadata JSON
	mocks.store.EXPECT().
		UpsertTokenMetadata(ctx, gomock.Any()).
		DoAndReturn(func(ctx context.Context, input store.CreateTokenMetadataInput) error {
			assert.Equal(t, tokenID, input.TokenID)
			assert.Equal(t, metadataJSON, input.OriginJSON) // Should use metadata JSON as origin
			assert.Equal(t, metadataJSON, input.LatestJSON)
			return nil
		})

	result, err := mocks.executor.ResolveTokenMetadata(ctx, tokenCID)

	assert.NoError(t, err)
	assert.Equal(t, expectedMetadata, result)
}

func TestResolveTokenMetadata_WithArtistsNoPublisher(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")

	expectedMetadata := &metadata.NormalizedMetadata{
		Name:        "Test NFT",
		Description: "Test Description",
		Image:       "https://example.com/image.png",
		Artists: []metadata.Artist{
			{DID: "did:example:artist1", Name: "Artist 1"},
		},
		Publisher: nil, // No publisher
	}

	tokenID := uint64(123)
	tokenWithMetadata := &store.TokensWithMetadataResult{
		Token: &schema.Token{
			ID:       tokenID,
			TokenCID: tokenCID.String(),
		},
		Metadata: &schema.TokenMetadata{
			TokenID:    tokenID,
			OriginJSON: []byte(`{"original":"metadata"}`),
		},
	}

	metadataJSON := []byte(`{"name":"Test NFT"}`)
	hash := []byte("testhash123")
	now := time.Now()

	// Mock metadata resolver
	mocks.metadataResolver.EXPECT().
		Resolve(ctx, tokenCID).
		Return(expectedMetadata, nil)

	// Mock store
	mocks.store.EXPECT().
		GetTokenWithMetadataByTokenCID(ctx, tokenCID.String()).
		Return(tokenWithMetadata, nil)

	// Mock RawHash
	mocks.metadataResolver.EXPECT().
		RawHash(expectedMetadata).
		Return(hash, metadataJSON, nil)

	// Mock clock
	mocks.clock.EXPECT().
		Now().
		Return(now)

	// Mock store UpsertTokenMetadata
	mocks.store.EXPECT().
		UpsertTokenMetadata(ctx, gomock.Any()).
		DoAndReturn(func(ctx context.Context, input store.CreateTokenMetadataInput) error {
			assert.Len(t, input.Artists, 1)
			assert.Equal(t, "did:example:artist1", string(input.Artists[0].DID))
			assert.Nil(t, input.Publisher)
			return nil
		})

	result, err := mocks.executor.ResolveTokenMetadata(ctx, tokenCID)

	assert.NoError(t, err)
	assert.Equal(t, expectedMetadata, result)
}

func TestResolveTokenMetadata_WithPublisherNoArtists(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")

	publisherName := "Test Publisher"
	publisherURL := "https://publisher.com"
	expectedMetadata := &metadata.NormalizedMetadata{
		Name:        "Test NFT",
		Description: "Test Description",
		Image:       "https://example.com/image.png",
		Artists:     []metadata.Artist{}, // Empty artists
		Publisher: &metadata.Publisher{
			Name: (*registry.PublisherName)(&publisherName),
			URL:  &publisherURL,
		},
	}

	tokenID := uint64(123)
	tokenWithMetadata := &store.TokensWithMetadataResult{
		Token: &schema.Token{
			ID:       tokenID,
			TokenCID: tokenCID.String(),
		},
		Metadata: &schema.TokenMetadata{
			TokenID:    tokenID,
			OriginJSON: []byte(`{"original":"metadata"}`),
		},
	}

	metadataJSON := []byte(`{"name":"Test NFT"}`)
	hash := []byte("testhash123")
	now := time.Now()

	// Mock metadata resolver
	mocks.metadataResolver.EXPECT().
		Resolve(ctx, tokenCID).
		Return(expectedMetadata, nil)

	// Mock store
	mocks.store.EXPECT().
		GetTokenWithMetadataByTokenCID(ctx, tokenCID.String()).
		Return(tokenWithMetadata, nil)

	// Mock RawHash
	mocks.metadataResolver.EXPECT().
		RawHash(expectedMetadata).
		Return(hash, metadataJSON, nil)

	// Mock clock
	mocks.clock.EXPECT().
		Now().
		Return(now)

	// Mock store UpsertTokenMetadata
	mocks.store.EXPECT().
		UpsertTokenMetadata(ctx, gomock.Any()).
		DoAndReturn(func(ctx context.Context, input store.CreateTokenMetadataInput) error {
			assert.Empty(t, input.Artists)
			assert.NotNil(t, input.Publisher)
			assert.Equal(t, publisherName, *input.Publisher.Name)
			return nil
		})

	result, err := mocks.executor.ResolveTokenMetadata(ctx, tokenCID)

	assert.NoError(t, err)
	assert.Equal(t, expectedMetadata, result)
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
	limit := 1000
	order := domain.BlockScanOrderAsc

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
		GetTokenCIDsByOwnerAndBlockRange(ctx, address, fromBlock, toBlock, limit, order, mocks.blacklist).
		Return(domain.TokenWithBlockRangeResult{
			Tokens:             expectedTokens,
			EffectiveFromBlock: fromBlock,
			EffectiveToBlock:   toBlock,
		}, nil)

	result, err := mocks.executor.GetEthereumTokenCIDsByOwnerWithinBlockRange(ctx, address, fromBlock, toBlock, limit, order)

	assert.NoError(t, err)
	assert.Equal(t, expectedTokens, result.Tokens)
}

func TestGetEthereumTokenCIDsByOwnerWithinBlockRange_UnsupportedAddress(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	address := "tz1fPKAtsYydh4f1wfWNfeNxWYu72TmM48fu" // Tezos address
	fromBlock := uint64(100)
	toBlock := uint64(200)
	limit := 1000
	order := domain.BlockScanOrderAsc

	result, err := mocks.executor.GetEthereumTokenCIDsByOwnerWithinBlockRange(ctx, address, fromBlock, toBlock, limit, order)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported blockchain for address")
	assert.Empty(t, result)
}

func TestGetEthereumTokenCIDsByOwnerWithinBlockRange_ClientError(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	address := "0xdadB0d80178819F2319190D340ce9A924f783711"
	fromBlock := uint64(100)
	toBlock := uint64(200)
	limit := 1000
	order := domain.BlockScanOrderAsc

	// Mock ethClient to return error
	clientErr := errors.New("client error")
	mocks.ethClient.EXPECT().
		GetTokenCIDsByOwnerAndBlockRange(ctx, address, fromBlock, toBlock, limit, order, mocks.blacklist).
		Return(domain.TokenWithBlockRangeResult{}, clientErr)

	result, err := mocks.executor.GetEthereumTokenCIDsByOwnerWithinBlockRange(ctx, address, fromBlock, toBlock, limit, order)

	assert.Error(t, err)
	assert.Equal(t, clientErr, err)
	assert.Empty(t, result)
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

	// Mock blacklist checks for first page - all not blacklisted
	mocks.blacklist.EXPECT().
		IsTokenCIDBlacklisted(gomock.Any()).
		Return(false).
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

	// Mock blacklist checks for second page - all not blacklisted
	mocks.blacklist.EXPECT().
		IsTokenCIDBlacklisted(gomock.Any()).
		Return(false).
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

func TestGetTezosTokenCIDsByAccountWithinBlockRange_BlacklistFiltering(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	address := "tz1owner123"
	fromBlock := uint64(100)
	toBlock := uint64(200)

	// Create balances where some contracts are blacklisted
	balances := []tezos.TzKTTokenBalance{
		{
			Token: tezos.TzKTTokenInfo{
				Contract: tezos.TzKTContract{
					Address: "KT1GoodContract1111111111111111111",
				},
				TokenID:  "1",
				Standard: "fa2",
			},
			LastLevel: 150,
		},
		{
			Token: tezos.TzKTTokenInfo{
				Contract: tezos.TzKTContract{
					Address: "KT1BlacklistedContract1111111111",
				},
				TokenID:  "1",
				Standard: "fa2",
			},
			LastLevel: 160,
		},
		{
			Token: tezos.TzKTTokenInfo{
				Contract: tezos.TzKTContract{
					Address: "KT1GoodContract1111111111111111111",
				},
				TokenID:  "2",
				Standard: "fa2",
			},
			LastLevel: 175,
		},
		{
			Token: tezos.TzKTTokenInfo{
				Contract: tezos.TzKTContract{
					Address: "KT1BlacklistedContract2222222222",
				},
				TokenID:  "5",
				Standard: "fa2",
			},
			LastLevel: 180,
		},
	}

	// Mock TzKT client
	mocks.tzktClient.EXPECT().
		GetTokenBalancesByAccountWithinBlockRange(ctx, address, fromBlock, toBlock, gomock.Any(), 0).
		Return(balances, nil)

	// Mock ChainID calls (one per token)
	mocks.tzktClient.EXPECT().
		ChainID().
		Return(domain.ChainTezosMainnet).
		Times(len(balances))

	// Mock blacklist checks
	// First token - not blacklisted
	mocks.blacklist.EXPECT().
		IsTokenCIDBlacklisted(domain.NewTokenCID(domain.ChainTezosMainnet, "fa2", "KT1GoodContract1111111111111111111", "1")).
		Return(false)

	// Second token - blacklisted
	mocks.blacklist.EXPECT().
		IsTokenCIDBlacklisted(domain.NewTokenCID(domain.ChainTezosMainnet, "fa2", "KT1BlacklistedContract1111111111", "1")).
		Return(true)

	// Third token - not blacklisted
	mocks.blacklist.EXPECT().
		IsTokenCIDBlacklisted(domain.NewTokenCID(domain.ChainTezosMainnet, "fa2", "KT1GoodContract1111111111111111111", "2")).
		Return(false)

	// Fourth token - blacklisted
	mocks.blacklist.EXPECT().
		IsTokenCIDBlacklisted(domain.NewTokenCID(domain.ChainTezosMainnet, "fa2", "KT1BlacklistedContract2222222222", "5")).
		Return(true)

	result, err := mocks.executor.GetTezosTokenCIDsByAccountWithinBlockRange(ctx, address, fromBlock, toBlock)

	assert.NoError(t, err)
	assert.Len(t, result, 2, "Should filter out 2 blacklisted tokens")
	assert.Equal(t, uint64(150), result[0].BlockNumber, "First non-blacklisted token")
	assert.Equal(t, uint64(175), result[1].BlockNumber, "Second non-blacklisted token")
}

func TestGetTezosTokenCIDsByAccountWithinBlockRange_AllBlacklisted(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	address := "tz1owner123"
	fromBlock := uint64(100)
	toBlock := uint64(200)

	// All contracts are blacklisted
	balances := []tezos.TzKTTokenBalance{
		{
			Token: tezos.TzKTTokenInfo{
				Contract: tezos.TzKTContract{
					Address: "KT1BlacklistedContract1111111111",
				},
				TokenID:  "1",
				Standard: "fa2",
			},
			LastLevel: 150,
		},
		{
			Token: tezos.TzKTTokenInfo{
				Contract: tezos.TzKTContract{
					Address: "KT1BlacklistedContract2222222222",
				},
				TokenID:  "1",
				Standard: "fa2",
			},
			LastLevel: 175,
		},
	}

	// Mock TzKT client
	mocks.tzktClient.EXPECT().
		GetTokenBalancesByAccountWithinBlockRange(ctx, address, fromBlock, toBlock, gomock.Any(), 0).
		Return(balances, nil)

	// Mock ChainID calls
	mocks.tzktClient.EXPECT().
		ChainID().
		Return(domain.ChainTezosMainnet).
		Times(len(balances))

	// Mock blacklist checks - all blacklisted
	mocks.blacklist.EXPECT().
		IsTokenCIDBlacklisted(gomock.Any()).
		Return(true).
		Times(len(balances))

	result, err := mocks.executor.GetTezosTokenCIDsByAccountWithinBlockRange(ctx, address, fromBlock, toBlock)

	assert.NoError(t, err)
	assert.Empty(t, result, "Should return empty list when all tokens are blacklisted")
}

// ====================================================================================
// GetLatestEthereumBlock Tests
// ====================================================================================

func TestGetLatestEthereumBlock_Success(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	expectedBlock := uint64(12345678)

	// Mock ethClient GetLatestBlock
	mocks.ethClient.EXPECT().
		GetLatestBlock(ctx).
		Return(expectedBlock, nil)

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
		GetLatestBlock(ctx).
		Return(uint64(0), clientErr)

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
	dailyQuota := 1000

	// Mock store EnsureWatchedAddressExists
	mocks.store.EXPECT().
		EnsureWatchedAddressExists(ctx, address, chain, dailyQuota).
		Return(nil)

	err := mocks.executor.EnsureWatchedAddressExists(ctx, address, chain, dailyQuota)

	assert.NoError(t, err)
}

func TestEnsureWatchedAddressExists_StoreError(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	address := "0xowner123"
	chain := domain.ChainEthereumMainnet
	dailyQuota := 1000

	// Mock store EnsureWatchedAddressExists to return error
	storeErr := errors.New("database error")
	mocks.store.EXPECT().
		EnsureWatchedAddressExists(ctx, address, chain, dailyQuota).
		Return(storeErr)

	err := mocks.executor.EnsureWatchedAddressExists(ctx, address, chain, dailyQuota)

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

	// Mock JSON marshal for the main event
	rawEventData := []byte(`{"chain":"eip155:1"}`)
	mocks.json.EXPECT().
		Marshal(event).
		Return(rawEventData, nil)

	// Mock GetERC1155BalanceAndEventsForOwner for sender
	senderEvents := []domain.BlockchainEvent{*event}
	mocks.ethClient.EXPECT().
		GetERC1155BalanceAndEventsForOwner(ctx, event.ContractAddress, event.TokenNumber, fromAddr).
		Return("3", senderEvents, nil) // Sender still has 3 after transfer

	// Mock JSON marshal for sender events
	mocks.json.EXPECT().
		Marshal(senderEvents[0]).
		Return(rawEventData, nil)

	// Mock GetERC1155BalanceAndEventsForOwner for receiver
	receiverEvents := []domain.BlockchainEvent{*event}
	mocks.ethClient.EXPECT().
		GetERC1155BalanceAndEventsForOwner(ctx, event.ContractAddress, event.TokenNumber, toAddr).
		Return("5", receiverEvents, nil) // Receiver has 5 after transfer

	// Mock JSON marshal for receiver events
	mocks.json.EXPECT().
		Marshal(receiverEvents[0]).
		Return(rawEventData, nil)

	// Mock store CreateTokenWithProvenances
	mocks.store.EXPECT().
		CreateTokenWithProvenances(ctx, gomock.Any()).
		DoAndReturn(func(ctx context.Context, input store.CreateTokenWithProvenancesInput) error {
			assert.Len(t, input.Balances, 2) // Both addresses have positive balances
			assert.Equal(t, fromAddr, input.Balances[0].OwnerAddress)
			assert.Equal(t, "3", input.Balances[0].Quantity)
			assert.Equal(t, toAddr, input.Balances[1].OwnerAddress)
			assert.Equal(t, "5", input.Balances[1].Quantity)
			// Should have main event + sender event + receiver event = 3 events
			assert.Len(t, input.Events, 3)
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

	// Mock JSON marshal
	rawEventData := []byte(`{"chain":"eip155:1"}`)
	mocks.json.EXPECT().
		Marshal(event).
		Return(rawEventData, nil)

	// Mock GetERC1155BalanceAndEventsForOwner to return error
	balanceErr := errors.New("failed to get balance")
	mocks.ethClient.EXPECT().
		GetERC1155BalanceAndEventsForOwner(ctx, event.ContractAddress, event.TokenNumber, fromAddr).
		Return("", nil, balanceErr)

	err := mocks.executor.IndexTokenWithMinimalProvenancesByBlockchainEvent(ctx, event)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to add balance for from address")
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

	err := mocks.executor.IndexTokenWithMinimalProvenancesByTokenCID(ctx, tokenCID, nil)

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

	err := mocks.executor.IndexTokenWithMinimalProvenancesByTokenCID(ctx, tokenCID, nil)

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

	err := mocks.executor.IndexTokenWithMinimalProvenancesByTokenCID(ctx, tokenCID, nil)

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

	err := mocks.executor.IndexTokenWithMinimalProvenancesByTokenCID(ctx, tokenCID, nil)

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

	err := mocks.executor.IndexTokenWithMinimalProvenancesByTokenCID(ctx, tokenCID, nil)

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

	err := mocks.executor.IndexTokenWithMinimalProvenancesByTokenCID(ctx, tokenCID, nil)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get ERC1155 balances from Ethereum")
}

func TestIndexTokenWithMinimalProvenancesByTokenCID_Success_ERC1155_WithOwner(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	contractAddress := "0x1234567890123456789012345678901234567890"
	tokenNumber := "100"
	ownerAddress := "0xowner123"
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC1155, contractAddress, tokenNumber)

	// Mock GetERC1155BalanceAndEventsForOwner - owner-specific call
	balance := "5"
	blockHash := "0xblockhash1"
	events := []domain.BlockchainEvent{
		{
			Chain:           domain.ChainEthereumMainnet,
			Standard:        domain.StandardERC1155,
			ContractAddress: contractAddress,
			TokenNumber:     tokenNumber,
			EventType:       domain.EventTypeMint,
			FromAddress:     stringPtr(domain.ETHEREUM_ZERO_ADDRESS),
			ToAddress:       stringPtr(ownerAddress),
			Quantity:        "5",
			TxHash:          "0xtx1",
			BlockNumber:     100,
			BlockHash:       &blockHash,
			TxIndex:         0,
			Timestamp:       time.Now(),
		},
	}

	mocks.ethClient.EXPECT().
		GetERC1155BalanceAndEventsForOwner(ctx, contractAddress, tokenNumber, ownerAddress).
		Return(balance, events, nil)

	// Mock JSON marshal for events
	rawEventData := []byte(`{"event":"data"}`)
	mocks.json.EXPECT().
		Marshal(events[0]).
		Return(rawEventData, nil)

	// Mock store UpsertTokenBalanceForOwner
	mocks.store.EXPECT().
		UpsertTokenBalanceForOwner(ctx, gomock.Any()).
		DoAndReturn(func(ctx context.Context, input store.UpsertTokenBalanceForOwnerInput) error {
			assert.Equal(t, tokenCID.String(), input.Token.TokenCID)
			assert.Equal(t, ownerAddress, input.OwnerAddress)
			assert.Equal(t, "5", input.Quantity)
			assert.Len(t, input.Events, 1, "Should have owner-specific event")
			assert.Equal(t, ownerAddress, *input.Events[0].ToAddress)
			return nil
		})

	err := mocks.executor.IndexTokenWithMinimalProvenancesByTokenCID(ctx, tokenCID, &ownerAddress)

	assert.NoError(t, err)
}

func TestIndexTokenWithMinimalProvenancesByTokenCID_Success_ERC1155_WithOwner_MultipleEvents(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	contractAddress := "0x1234567890123456789012345678901234567890"
	tokenNumber := "100"
	ownerAddress := "0xowner123"
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC1155, contractAddress, tokenNumber)

	// Mock GetERC1155BalanceAndEventsForOwner with multiple events (including TransferBatch)
	balance := "10"
	blockHash := "0xblockhash1"
	events := []domain.BlockchainEvent{
		{
			Chain:           domain.ChainEthereumMainnet,
			Standard:        domain.StandardERC1155,
			ContractAddress: contractAddress,
			TokenNumber:     tokenNumber,
			EventType:       domain.EventTypeMint,
			FromAddress:     stringPtr(domain.ETHEREUM_ZERO_ADDRESS),
			ToAddress:       stringPtr(ownerAddress),
			Quantity:        "5",
			TxHash:          "0xtx1",
			BlockNumber:     100,
			BlockHash:       &blockHash,
			TxIndex:         0,
			Timestamp:       time.Now(),
		},
		{
			Chain:           domain.ChainEthereumMainnet,
			Standard:        domain.StandardERC1155,
			ContractAddress: contractAddress,
			TokenNumber:     tokenNumber,
			EventType:       domain.EventTypeTransfer,
			FromAddress:     stringPtr("0xother"),
			ToAddress:       stringPtr(ownerAddress),
			Quantity:        "5",
			TxHash:          "0xtx2",
			BlockNumber:     150,
			BlockHash:       &blockHash,
			TxIndex:         1,
			Timestamp:       time.Now(),
		},
	}

	mocks.ethClient.EXPECT().
		GetERC1155BalanceAndEventsForOwner(ctx, contractAddress, tokenNumber, ownerAddress).
		Return(balance, events, nil)

	// Mock JSON marshal for events
	rawEventData := []byte(`{"event":"data"}`)
	mocks.json.EXPECT().
		Marshal(events[0]).
		Return(rawEventData, nil)
	mocks.json.EXPECT().
		Marshal(events[1]).
		Return(rawEventData, nil)

	// Mock store UpsertTokenBalanceForOwner
	mocks.store.EXPECT().
		UpsertTokenBalanceForOwner(ctx, gomock.Any()).
		DoAndReturn(func(ctx context.Context, input store.UpsertTokenBalanceForOwnerInput) error {
			assert.Equal(t, tokenCID.String(), input.Token.TokenCID)
			assert.Equal(t, ownerAddress, input.OwnerAddress)
			assert.Equal(t, "10", input.Quantity)
			assert.Len(t, input.Events, 2, "Should have 2 owner-specific events")
			return nil
		})

	err := mocks.executor.IndexTokenWithMinimalProvenancesByTokenCID(ctx, tokenCID, &ownerAddress)

	assert.NoError(t, err)
}

func TestIndexTokenWithMinimalProvenancesByTokenCID_Success_ERC1155_WithOwner_ZeroBalance(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	contractAddress := "0x1234567890123456789012345678901234567890"
	tokenNumber := "100"
	ownerAddress := "0xowner123"
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC1155, contractAddress, tokenNumber)

	// Mock GetERC1155BalanceAndEventsForOwner - owner transferred away all tokens
	balance := "0"
	blockHash := "0xblockhash1"
	events := []domain.BlockchainEvent{
		{
			Chain:           domain.ChainEthereumMainnet,
			Standard:        domain.StandardERC1155,
			ContractAddress: contractAddress,
			TokenNumber:     tokenNumber,
			EventType:       domain.EventTypeMint,
			FromAddress:     stringPtr(domain.ETHEREUM_ZERO_ADDRESS),
			ToAddress:       stringPtr(ownerAddress),
			Quantity:        "5",
			TxHash:          "0xtx1",
			BlockNumber:     100,
			BlockHash:       &blockHash,
			TxIndex:         0,
			Timestamp:       time.Now(),
		},
		{
			Chain:           domain.ChainEthereumMainnet,
			Standard:        domain.StandardERC1155,
			ContractAddress: contractAddress,
			TokenNumber:     tokenNumber,
			EventType:       domain.EventTypeTransfer,
			FromAddress:     stringPtr(ownerAddress),
			ToAddress:       stringPtr("0xother"),
			Quantity:        "5",
			TxHash:          "0xtx2",
			BlockNumber:     150,
			BlockHash:       &blockHash,
			TxIndex:         1,
			Timestamp:       time.Now(),
		},
	}

	mocks.ethClient.EXPECT().
		GetERC1155BalanceAndEventsForOwner(ctx, contractAddress, tokenNumber, ownerAddress).
		Return(balance, events, nil)

	err := mocks.executor.IndexTokenWithMinimalProvenancesByTokenCID(ctx, tokenCID, &ownerAddress)

	assert.Error(t, err)
	assert.IsType(t, &temporal.ApplicationError{}, err)
	var appErr *temporal.ApplicationError
	errOk := errors.As(err, &appErr)
	assert.True(t, errOk)
	assert.True(t, appErr.NonRetryable())
	assert.Contains(t, err.Error(), "balance is not a positive numeric value")
}

func TestIndexTokenWithMinimalProvenancesByTokenCID_ERC1155_WithOwner_GetBalanceError(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	contractAddress := "0x1234567890123456789012345678901234567890"
	tokenNumber := "100"
	ownerAddress := "0xowner123"
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC1155, contractAddress, tokenNumber)

	// Mock GetERC1155BalanceAndEventsForOwner to return error
	expectedError := errors.New("rpc error")
	mocks.ethClient.EXPECT().
		GetERC1155BalanceAndEventsForOwner(ctx, contractAddress, tokenNumber, ownerAddress).
		Return("", nil, expectedError)

	err := mocks.executor.IndexTokenWithMinimalProvenancesByTokenCID(ctx, tokenCID, &ownerAddress)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get ERC1155 balance and events for owner")
}

func TestIndexTokenWithMinimalProvenancesByTokenCID_ERC1155_WithOwner_StoreError(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	contractAddress := "0x1234567890123456789012345678901234567890"
	tokenNumber := "100"
	ownerAddress := "0xowner123"
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC1155, contractAddress, tokenNumber)

	// Mock GetERC1155BalanceAndEventsForOwner
	balance := "5"
	events := []domain.BlockchainEvent{}

	mocks.ethClient.EXPECT().
		GetERC1155BalanceAndEventsForOwner(ctx, contractAddress, tokenNumber, ownerAddress).
		Return(balance, events, nil)

	// Mock store UpsertTokenBalanceForOwner to return error
	expectedError := errors.New("database error")
	mocks.store.EXPECT().
		UpsertTokenBalanceForOwner(ctx, gomock.Any()).
		Return(expectedError)

	err := mocks.executor.IndexTokenWithMinimalProvenancesByTokenCID(ctx, tokenCID, &ownerAddress)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to upsert token balance for owner")
}

func TestIndexTokenWithMinimalProvenancesByTokenCID_WithOwner_TokenNotFoundOnChain(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")
	ownerAddress := "0xowner123"

	mocks.ethClient.EXPECT().
		ERC721OwnerOf(ctx, "0x1234567890123456789012345678901234567890", "1").
		Return("", ethereum.ErrExecutionReverted)

	err := mocks.executor.IndexTokenWithMinimalProvenancesByTokenCID(ctx, tokenCID, &ownerAddress)

	assert.Error(t, err)
	assert.IsType(t, &temporal.ApplicationError{}, err)
	var appErr *temporal.ApplicationError
	errOk := errors.As(err, &appErr)
	assert.True(t, errOk)
	assert.True(t, appErr.NonRetryable())
	assert.Contains(t, err.Error(), "token not found on chain")
}

func TestIndexTokenWithMinimalProvenancesByTokenCID_WithOwner_ContractUnreachable(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC1155, "0x1234567890123456789012345678901234567890", "1")
	ownerAddress := "0xowner123"

	mocks.ethClient.EXPECT().
		GetERC1155BalanceAndEventsForOwner(ctx, "0x1234567890123456789012345678901234567890", "1", "0xowner123").
		Return("", nil, ethereum.ErrOutOfGas)

	err := mocks.executor.IndexTokenWithMinimalProvenancesByTokenCID(ctx, tokenCID, &ownerAddress)

	assert.Error(t, err)
	assert.IsType(t, &temporal.ApplicationError{}, err)
	var appErr *temporal.ApplicationError
	errOk := errors.As(err, &appErr)
	assert.True(t, errOk)
	assert.True(t, appErr.NonRetryable())
	assert.Contains(t, err.Error(), "contract is unreachable")
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

	// Mock ERC1155BalanceOfBatch for all addresses in events (will return empty map for burned token with 0 balance)
	// The function fetches balances for all addresses in events, even if ultimately burned
	mocks.ethClient.EXPECT().
		ERC1155BalanceOfBatch(ctx, contractAddress, tokenNumber, []string{"0xowner1"}).
		Return(map[string]string{}, nil) // Empty map because balance is 0 (filtered out)

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

func TestIndexTokenWithFullProvenancesByTokenCID_GetEventsError(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	contractAddress := "0x1234567890123456789012345678901234567890"
	tokenNumber := "1"
	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, contractAddress, tokenNumber)

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
// Webhook Activities Tests
// ====================================================================================

func TestGetActiveWebhookClientsByEventType_Success(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	eventType := "token.indexing.queryable"
	expectedClients := []*schema.WebhookClient{
		{
			ID:               1,
			ClientID:         "client-123",
			WebhookURL:       "https://example.com/webhook",
			WebhookSecret:    "736563726574",
			EventFilters:     []byte(`["token.indexing.queryable"]`),
			IsActive:         true,
			RetryMaxAttempts: 5,
		},
	}

	mocks.store.EXPECT().
		GetActiveWebhookClientsByEventType(ctx, eventType).
		Return(expectedClients, nil)

	result, err := mocks.executor.GetActiveWebhookClientsByEventType(ctx, eventType)

	assert.NoError(t, err)
	assert.Equal(t, expectedClients, result)
}

func TestGetActiveWebhookClientsByEventType_StoreError(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	eventType := "token.indexing.queryable"
	expectedError := errors.New("database error")

	mocks.store.EXPECT().
		GetActiveWebhookClientsByEventType(ctx, eventType).
		Return(nil, expectedError)

	result, err := mocks.executor.GetActiveWebhookClientsByEventType(ctx, eventType)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Equal(t, expectedError, err)
}

func TestGetWebhookClientByID_Success(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	clientID := "client-123"
	expectedClient := &schema.WebhookClient{
		ID:               1,
		ClientID:         clientID,
		WebhookURL:       "https://example.com/webhook",
		WebhookSecret:    "736563726574",
		EventFilters:     []byte(`["*"]`),
		IsActive:         true,
		RetryMaxAttempts: 5,
	}

	mocks.store.EXPECT().
		GetWebhookClientByID(ctx, clientID).
		Return(expectedClient, nil)

	result, err := mocks.executor.GetWebhookClientByID(ctx, clientID)

	assert.NoError(t, err)
	assert.Equal(t, expectedClient, result)
}

func TestGetWebhookClientByID_NotFound(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	clientID := "non-existent"

	mocks.store.EXPECT().
		GetWebhookClientByID(ctx, clientID).
		Return(nil, nil)

	result, err := mocks.executor.GetWebhookClientByID(ctx, clientID)

	assert.NoError(t, err)
	assert.Nil(t, result)
}

func TestCreateWebhookDeliveryRecord_Success(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	delivery := &schema.WebhookDelivery{
		ClientID:       "client-123",
		EventID:        "event-456",
		EventType:      "token.indexing.queryable",
		WorkflowID:     "workflow-789",
		WorkflowRunID:  "run-012",
		DeliveryStatus: schema.WebhookDeliveryStatusPending,
		Attempts:       0,
	}
	event := webhook.WebhookEvent{
		EventID:   "event-456",
		EventType: "token.indexing.queryable",
		Timestamp: time.Now(),
		Data: webhook.EventData{
			TokenCID:    "eip155:1:erc721:0xabc:1",
			Chain:       "eip155:1",
			Standard:    "erc721",
			Contract:    "0xabc",
			TokenNumber: "1",
		},
	}

	// Mock JSON marshal for event
	mocks.json.EXPECT().
		Marshal(gomock.Any()).
		Return([]byte(`{"event":"test"}`), nil)

	// Mock create webhook delivery record succeeds
	mocks.store.EXPECT().
		CreateWebhookDelivery(ctx, gomock.Any()).
		DoAndReturn(func(ctx context.Context, d *schema.WebhookDelivery) error {
			// Verify payload was set
			assert.NotNil(t, d.Payload)
			// Set ID to simulate database insertion
			d.ID = 123
			return nil
		})

	deliveryID, err := mocks.executor.CreateWebhookDeliveryRecord(ctx, delivery, event)

	assert.NoError(t, err)
	assert.Equal(t, uint64(123), deliveryID)
	assert.NotNil(t, delivery.Payload)
}

func TestCreateWebhookDeliveryRecord_MarshalError(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	delivery := &schema.WebhookDelivery{
		ClientID:  "client-123",
		EventID:   "event-456",
		EventType: "token.indexing.queryable",
	}
	event := webhook.WebhookEvent{
		EventID:   "event-456",
		EventType: "token.indexing.queryable",
		Timestamp: time.Now(),
		Data: webhook.EventData{
			TokenCID: "eip155:1:erc721:0xabc:1",
		},
	}
	mocks.json.EXPECT().
		Marshal(gomock.Any()).
		Return(nil, errors.New("marshal error"))

	deliveryID, err := mocks.executor.CreateWebhookDeliveryRecord(ctx, delivery, event)

	assert.Error(t, err)
	assert.Equal(t, uint64(0), deliveryID)
}

func TestCreateWebhookDeliveryRecord_StoreError(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	delivery := &schema.WebhookDelivery{
		ClientID:       "client-123",
		EventID:        "event-456",
		EventType:      "token.indexing.queryable",
		DeliveryStatus: schema.WebhookDeliveryStatusPending,
	}
	event := webhook.WebhookEvent{
		EventID:   "event-456",
		EventType: "token.indexing.queryable",
		Timestamp: time.Now(),
		Data: webhook.EventData{
			TokenCID: "eip155:1:erc721:0xabc:1",
		},
	}
	expectedError := errors.New("database error")

	// Mock JSON marshal for event
	mocks.json.EXPECT().
		Marshal(gomock.Any()).
		Return([]byte(`{"event":"test"}`), nil)

	// Mock create webhook delivery record fails
	mocks.store.EXPECT().
		CreateWebhookDelivery(ctx, gomock.Any()).
		Return(expectedError)

	deliveryID, err := mocks.executor.CreateWebhookDeliveryRecord(ctx, delivery, event)

	assert.Error(t, err)
	assert.Equal(t, uint64(0), deliveryID)
	assert.Equal(t, expectedError, err)
}

func TestDeliverWebhookHTTP_Success(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	client := &schema.WebhookClient{
		ClientID:      "client-123",
		WebhookURL:    "https://example.com/webhook",
		WebhookSecret: "7365637265742d6b6579",
	}
	event := webhook.WebhookEvent{
		EventID:   "event-456",
		EventType: "token.indexing.queryable",
		Timestamp: time.Now(),
		Data: webhook.EventData{
			TokenCID: "eip155:1:erc721:0xabc:1",
		},
	}
	deliveryID := uint64(789)

	// Mock GetInfo from temporal activity
	mocks.temporalActivity.EXPECT().
		GetInfo(ctx).
		Return(activity.Info{Attempt: 1})

	// Mock successful HTTP response
	statusCode := 200
	mockResponse := &http.Response{
		StatusCode: statusCode,
		Body:       io.NopCloser(bytes.NewBuffer([]byte(`{"status":"success"}`))),
	}

	mocks.httpClient.EXPECT().
		PostWithHeadersNoRetry(ctx, client.WebhookURL, gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, url string, headers map[string]string, body interface{}) (*http.Response, error) {
			// Verify headers
			assert.Equal(t, "application/json", headers["Content-Type"])
			assert.NotEmpty(t, headers["X-Webhook-Signature"])
			assert.Equal(t, event.EventID, headers["X-Webhook-Event-ID"])
			assert.Equal(t, event.EventType, headers["X-Webhook-Event-Type"])
			assert.NotEmpty(t, headers["X-Webhook-Timestamp"])
			assert.Equal(t, "FF-Indexer-Webhook/2.0", headers["User-Agent"])
			return mockResponse, nil
		})

	// Mock read all from io reader succeeds
	mocks.io.EXPECT().
		ReadAll(io.LimitReader(mockResponse.Body, 4*1024)).
		Return([]byte(`{"status":"success"}`), nil)

	// Mock update webhook delivery status succeeds
	mocks.store.EXPECT().
		UpdateWebhookDeliveryStatus(ctx, deliveryID, schema.WebhookDeliveryStatusSuccess, 1, &statusCode, `{"status":"success"}`, "").
		Return(nil)

	result, err := mocks.executor.DeliverWebhookHTTP(ctx, client, event, deliveryID)

	assert.NoError(t, err)
	assert.True(t, result.Success)
	assert.Equal(t, statusCode, result.StatusCode)
	assert.Equal(t, `{"status":"success"}`, result.Body)
}

func TestDeliverWebhookHTTP_HTTPError(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	client := &schema.WebhookClient{
		ClientID:      "client-123",
		WebhookURL:    "https://example.com/webhook",
		WebhookSecret: "7365637265742d6b6579",
	}
	event := webhook.WebhookEvent{
		EventID:   "event-456",
		EventType: "token.indexing.queryable",
		Timestamp: time.Now(),
		Data: webhook.EventData{
			TokenCID: "eip155:1:erc721:0xabc:1",
		},
	}
	deliveryID := uint64(789)
	expectedError := errors.New("connection refused")

	// Mock GetInfo from temporal activity
	mocks.temporalActivity.EXPECT().
		GetInfo(ctx).
		Return(activity.Info{Attempt: 1})

	// Mock failed HTTP response
	mocks.httpClient.EXPECT().
		PostWithHeadersNoRetry(ctx, client.WebhookURL, gomock.Any(), gomock.Any()).
		Return(nil, expectedError)

	// Mock update webhook delivery status succeeds
	mocks.store.EXPECT().
		UpdateWebhookDeliveryStatus(ctx, deliveryID, schema.WebhookDeliveryStatusFailed, gomock.Any(), nil, "", expectedError.Error()).
		Return(nil)

	result, err := mocks.executor.DeliverWebhookHTTP(ctx, client, event, deliveryID)

	assert.Error(t, err)
	assert.False(t, result.Success)
	assert.Contains(t, result.Error, expectedError.Error())
}

func TestDeliverWebhookHTTP_Non2xxStatusCode(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	client := &schema.WebhookClient{
		ClientID:      "client-123",
		WebhookURL:    "https://example.com/webhook",
		WebhookSecret: "7365637265742d6b6579",
	}
	event := webhook.WebhookEvent{
		EventID:   "event-456",
		EventType: "token.indexing.queryable",
		Timestamp: time.Now(),
		Data: webhook.EventData{
			TokenCID: "eip155:1:erc721:0xabc:1",
		},
	}
	deliveryID := uint64(789)

	// Mock GetInfo from temporal activity
	mocks.temporalActivity.EXPECT().
		GetInfo(ctx).
		Return(activity.Info{Attempt: 1})

	// Mock 500 error response
	statusCode := 500
	mockResponse := &http.Response{
		StatusCode: statusCode,
		Body:       io.NopCloser(bytes.NewBuffer([]byte(`{"error":"internal server error"}`))),
	}

	mocks.httpClient.EXPECT().
		PostWithHeadersNoRetry(ctx, client.WebhookURL, gomock.Any(), gomock.Any()).
		Return(mockResponse, nil)

	// Mock read all from io reader succeeds
	mocks.io.EXPECT().
		ReadAll(io.LimitReader(mockResponse.Body, 4*1024)).
		Return([]byte(`{"error":"internal server error"}`), nil)

	// Mock update webhook delivery status fails
	mocks.store.EXPECT().
		UpdateWebhookDeliveryStatus(ctx, deliveryID, schema.WebhookDeliveryStatusFailed, gomock.Any(), &statusCode, `{"error":"internal server error"}`, gomock.Any()).
		Return(nil)

	result, err := mocks.executor.DeliverWebhookHTTP(ctx, client, event, deliveryID)

	assert.Error(t, err)
	assert.False(t, result.Success)
	assert.Equal(t, statusCode, result.StatusCode)
	assert.Contains(t, result.Body, "internal server error")
}

func TestDeliverWebhookHTTP_ReadBodyError(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	client := &schema.WebhookClient{
		ClientID:      "client-123",
		WebhookURL:    "https://example.com/webhook",
		WebhookSecret: "7365637265742d6b6579",
	}
	event := webhook.WebhookEvent{
		EventID:   "event-456",
		EventType: "token.indexing.queryable",
		Timestamp: time.Now(),
		Data: webhook.EventData{
			TokenCID: "eip155:1:erc721:0xabc:1",
		},
	}
	deliveryID := uint64(789)
	readError := errors.New("failed to read body")

	// Mock GetInfo from temporal activity
	mocks.temporalActivity.EXPECT().
		GetInfo(ctx).
		Return(activity.Info{Attempt: 1})

	// Mock successful HTTP response but body read fails
	mockResponse := &http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(bytes.NewBuffer([]byte(`{"status":"success"}`))),
	}

	mocks.httpClient.EXPECT().
		PostWithHeadersNoRetry(ctx, client.WebhookURL, gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, url string, headers map[string]string, body interface{}) (*http.Response, error) {
			return mockResponse, nil
		})

	// Mock read all from io reader fails
	mocks.io.EXPECT().
		ReadAll(io.LimitReader(mockResponse.Body, 4*1024)).
		Return(nil, readError)

	// Even though body read fails, delivery should succeed with empty body
	// The error is logged but doesn't cause the delivery to fail
	statusCode := 200
	mocks.store.EXPECT().
		UpdateWebhookDeliveryStatus(ctx, deliveryID, schema.WebhookDeliveryStatusSuccess, gomock.Any(), &statusCode, "", "").
		Return(nil)

	result, err := mocks.executor.DeliverWebhookHTTP(ctx, client, event, deliveryID)

	// Delivery should succeed even if we couldn't read the body
	assert.NoError(t, err)
	assert.True(t, result.Success)
	assert.Equal(t, 200, result.StatusCode)
	assert.Empty(t, result.Body, "Body should be empty when read fails")
}

func TestDeliverWebhookHTTP_UpdateStatusError(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	client := &schema.WebhookClient{
		ClientID:      "client-123",
		WebhookURL:    "https://example.com/webhook",
		WebhookSecret: "7365637265742d6b6579",
	}
	event := webhook.WebhookEvent{
		EventID:   "event-456",
		EventType: "token.indexing.queryable",
		Timestamp: time.Now(),
		Data: webhook.EventData{
			TokenCID: "eip155:1:erc721:0xabc:1",
		},
	}
	deliveryID := uint64(789)
	updateError := errors.New("failed to update status")

	// Mock GetInfo from temporal activity
	mocks.temporalActivity.EXPECT().
		GetInfo(ctx).
		Return(activity.Info{Attempt: 1})

	// Mock successful HTTP response
	mockResponse := &http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(bytes.NewBuffer([]byte(`{"status":"success"}`))),
	}

	// Mock successful HTTP response
	mocks.httpClient.EXPECT().
		PostWithHeadersNoRetry(ctx, client.WebhookURL, gomock.Any(), gomock.Any()).
		Return(mockResponse, nil)

	// Mock read all from io reader succeeds
	mocks.io.EXPECT().
		ReadAll(io.LimitReader(mockResponse.Body, 4*1024)).
		Return([]byte(`{"status":"success"}`), nil)

	// Mock update webhook delivery status fails
	mocks.store.EXPECT().
		UpdateWebhookDeliveryStatus(ctx, deliveryID, schema.WebhookDeliveryStatusSuccess, gomock.Any(), gomock.Any(), `{"status":"success"}`, "").
		Return(updateError)

	// Should still succeed even if status update fails (logged but not returned as error)
	result, err := mocks.executor.DeliverWebhookHTTP(ctx, client, event, deliveryID)

	assert.NoError(t, err)
	assert.True(t, result.Success)
	assert.Equal(t, 200, result.StatusCode)
}

// ====================================================================================
// Address Indexing Job Activities Tests
// ====================================================================================

func TestCreateIndexingJob_Success(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	address := "0x1234567890123456789012345678901234567890"
	chain := domain.ChainEthereumMainnet
	workflowID := "test-workflow-123"
	workflowRunID := "test-run-456"

	expectedInput := store.CreateAddressIndexingJobInput{
		Address:       address,
		Chain:         chain,
		Status:        schema.IndexingJobStatusRunning,
		WorkflowID:    workflowID,
		WorkflowRunID: &workflowRunID,
	}

	mocks.store.EXPECT().
		CreateAddressIndexingJob(ctx, expectedInput).
		Return(nil)

	err := mocks.executor.CreateIndexingJob(ctx, address, chain, workflowID, &workflowRunID)

	assert.NoError(t, err)
}

func TestCreateIndexingJob_StoreError(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	address := "0x1234567890123456789012345678901234567890"
	chain := domain.ChainEthereumMainnet
	workflowID := "test-workflow-error"
	storeError := errors.New("database connection failed")

	mocks.store.EXPECT().
		CreateAddressIndexingJob(ctx, gomock.Any()).
		Return(storeError)

	err := mocks.executor.CreateIndexingJob(ctx, address, chain, workflowID, nil)

	assert.Error(t, err)
	assert.Equal(t, storeError, err)
}

func TestUpdateIndexingJobStatus_ToCompleted(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	workflowID := "test-workflow-123"
	status := schema.IndexingJobStatusCompleted
	timestamp := time.Now().UTC()

	mocks.store.EXPECT().
		UpdateAddressIndexingJobStatus(ctx, workflowID, status, timestamp).
		Return(nil)

	err := mocks.executor.UpdateIndexingJobStatus(ctx, workflowID, status, timestamp)

	assert.NoError(t, err)
}

func TestUpdateIndexingJobStatus_StoreError(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	workflowID := "test-workflow-error"
	status := schema.IndexingJobStatusCompleted
	timestamp := time.Now().UTC()
	storeError := errors.New("database update failed")

	mocks.store.EXPECT().
		UpdateAddressIndexingJobStatus(ctx, workflowID, status, timestamp).
		Return(storeError)

	err := mocks.executor.UpdateIndexingJobStatus(ctx, workflowID, status, timestamp)

	assert.Error(t, err)
	assert.Equal(t, storeError, err)
}

func TestUpdateIndexingJobProgress_Success(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	workflowID := "test-workflow-123"
	tokensProcessed := 50
	minBlock := uint64(1000)
	maxBlock := uint64(2000)

	mocks.store.EXPECT().
		UpdateAddressIndexingJobProgress(ctx, workflowID, tokensProcessed, minBlock, maxBlock).
		Return(nil)

	err := mocks.executor.UpdateIndexingJobProgress(ctx, workflowID, tokensProcessed, minBlock, maxBlock)

	assert.NoError(t, err)
}

func TestUpdateIndexingJobProgress_StoreError(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	workflowID := "test-workflow-error"
	tokensProcessed := 100
	minBlock := uint64(7000)
	maxBlock := uint64(8000)
	storeError := errors.New("database update failed")

	mocks.store.EXPECT().
		UpdateAddressIndexingJobProgress(ctx, workflowID, tokensProcessed, minBlock, maxBlock).
		Return(storeError)

	err := mocks.executor.UpdateIndexingJobProgress(ctx, workflowID, tokensProcessed, minBlock, maxBlock)

	assert.Error(t, err)
	assert.Equal(t, storeError, err)
}

// ====================================================================================
// CheckMediaURLsHealthAndUpdateViewability Tests
// ====================================================================================

func TestCheckMediaURLsHealthAndUpdateViewability_Success_SingleHealthyURL(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	tokenCID := "ethereum-mainnet:erc721:0x1234567890123456789012345678901234567890:1"
	imageURL := "https://example.com/image.jpg"
	mediaURLs := []string{imageURL}

	// Mock URL health check - healthy
	mocks.urlChecker.EXPECT().
		Check(ctx, imageURL).
		Return(uri.HealthCheckResult{
			Status: uri.HealthStatusHealthy,
			Error:  nil,
		})

	// Mock database update for media health
	mocks.store.EXPECT().
		UpdateTokenMediaHealthByURL(ctx, imageURL, schema.MediaHealthStatusHealthy, nil).
		Return(nil)

	// Mock token retrieval
	token := &schema.Token{
		ID:       uint64(123),
		TokenCID: tokenCID,
	}
	mocks.store.EXPECT().
		GetTokenByTokenCID(ctx, tokenCID).
		Return(token, nil)

	// Mock viewability update - token becomes viewable
	changes := []store.TokenViewabilityChange{
		{
			TokenID:     token.ID,
			TokenCID:    tokenCID,
			OldViewable: false,
			NewViewable: true,
		},
	}
	mocks.store.EXPECT().
		BatchUpdateTokensViewability(ctx, []uint64{token.ID}).
		Return(changes, nil)

	result, err := mocks.executor.CheckMediaURLsHealthAndUpdateViewability(ctx, tokenCID, mediaURLs)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.True(t, result.IsViewable)
	assert.Equal(t, 1, len(result.HealthyURLs))
	assert.Equal(t, imageURL, result.HealthyURLs[0])
}

func TestCheckMediaURLsHealthAndUpdateViewability_Success_SingleBrokenURL(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	tokenCID := "ethereum-mainnet:erc721:0x1234567890123456789012345678901234567890:1"
	imageURL := "https://example.com/broken-image.jpg"
	mediaURLs := []string{imageURL}
	errorMsg := "404 Not Found"

	// Mock URL health check - broken
	mocks.urlChecker.EXPECT().
		Check(ctx, imageURL).
		Return(uri.HealthCheckResult{
			Status: uri.HealthStatusBroken,
			Error:  &errorMsg,
		})

	// Mock database update for media health
	mocks.store.EXPECT().
		UpdateTokenMediaHealthByURL(ctx, imageURL, schema.MediaHealthStatusBroken, &errorMsg).
		Return(nil)

	// Mock token retrieval
	token := &schema.Token{
		ID:       uint64(123),
		TokenCID: tokenCID,
	}
	mocks.store.EXPECT().
		GetTokenByTokenCID(ctx, tokenCID).
		Return(token, nil)

	// Mock viewability update - no changes (still not viewable)
	mocks.store.EXPECT().
		BatchUpdateTokensViewability(ctx, []uint64{token.ID}).
		Return([]store.TokenViewabilityChange{}, nil)

	// Mock get viewability status - not viewable
	mocks.store.EXPECT().
		GetTokensViewabilityByIDs(ctx, []uint64{token.ID}).
		Return([]store.TokenViewabilityInfo{
			{
				TokenID:    token.ID,
				TokenCID:   tokenCID,
				IsViewable: false,
			},
		}, nil)

	result, err := mocks.executor.CheckMediaURLsHealthAndUpdateViewability(ctx, tokenCID, mediaURLs)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.False(t, result.IsViewable)
	assert.Equal(t, 0, len(result.HealthyURLs)) // No healthy URLs
}

func TestCheckMediaURLsHealthAndUpdateViewability_Success_AlreadyViewableNoChange(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	tokenCID := "ethereum-mainnet:erc721:0x1234567890123456789012345678901234567890:1"
	imageURL := "https://example.com/image.jpg"
	mediaURLs := []string{imageURL}

	// Mock URL health check - healthy
	mocks.urlChecker.EXPECT().
		Check(ctx, imageURL).
		Return(uri.HealthCheckResult{
			Status: uri.HealthStatusHealthy,
			Error:  nil,
		})

	// Mock database update for media health
	mocks.store.EXPECT().
		UpdateTokenMediaHealthByURL(ctx, imageURL, schema.MediaHealthStatusHealthy, (*string)(nil)).
		Return(nil)

	// Mock token retrieval
	token := &schema.Token{
		ID:       uint64(123),
		TokenCID: tokenCID,
	}
	mocks.store.EXPECT().
		GetTokenByTokenCID(ctx, tokenCID).
		Return(token, nil)

	// Mock viewability update - no changes (was already viewable, still viewable)
	mocks.store.EXPECT().
		BatchUpdateTokensViewability(ctx, []uint64{token.ID}).
		Return([]store.TokenViewabilityChange{}, nil)

	// Mock get viewability status - viewable
	mocks.store.EXPECT().
		GetTokensViewabilityByIDs(ctx, []uint64{token.ID}).
		Return([]store.TokenViewabilityInfo{
			{
				TokenID:    token.ID,
				TokenCID:   tokenCID,
				IsViewable: true, // Token is viewable
			},
		}, nil)

	result, err := mocks.executor.CheckMediaURLsHealthAndUpdateViewability(ctx, tokenCID, mediaURLs)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.True(t, result.IsViewable) // Should return true even though there were no changes
	assert.Equal(t, 1, len(result.HealthyURLs))
	assert.Equal(t, imageURL, result.HealthyURLs[0])
}

func TestCheckMediaURLsHealthAndUpdateViewability_Success_MultipleURLs(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	tokenCID := "ethereum-mainnet:erc721:0x1234567890123456789012345678901234567890:1"
	imageURL := "https://example.com/image.jpg"
	animationURL := "https://example.com/animation.mp4"
	mediaURLs := []string{imageURL, animationURL}

	// Mock URL health checks - both healthy
	mocks.urlChecker.EXPECT().
		Check(ctx, imageURL).
		Return(uri.HealthCheckResult{
			Status: uri.HealthStatusHealthy,
			Error:  nil,
		})

	mocks.urlChecker.EXPECT().
		Check(ctx, animationURL).
		Return(uri.HealthCheckResult{
			Status: uri.HealthStatusHealthy,
			Error:  nil,
		})

	// Mock database updates for media health
	mocks.store.EXPECT().
		UpdateTokenMediaHealthByURL(ctx, imageURL, schema.MediaHealthStatusHealthy, nil).
		Return(nil)

	mocks.store.EXPECT().
		UpdateTokenMediaHealthByURL(ctx, animationURL, schema.MediaHealthStatusHealthy, nil).
		Return(nil)

	// Mock token retrieval
	token := &schema.Token{
		ID:       uint64(123),
		TokenCID: tokenCID,
	}
	mocks.store.EXPECT().
		GetTokenByTokenCID(ctx, tokenCID).
		Return(token, nil)

	// Mock viewability update - token becomes viewable
	changes := []store.TokenViewabilityChange{
		{
			TokenID:     token.ID,
			TokenCID:    tokenCID,
			OldViewable: false,
			NewViewable: true,
		},
	}
	mocks.store.EXPECT().
		BatchUpdateTokensViewability(ctx, []uint64{token.ID}).
		Return(changes, nil)

	result, err := mocks.executor.CheckMediaURLsHealthAndUpdateViewability(ctx, tokenCID, mediaURLs)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.True(t, result.IsViewable)
	assert.Equal(t, 2, len(result.HealthyURLs))
}

func TestCheckMediaURLsHealthAndUpdateViewability_Success_DataURI_Healthy(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	tokenCID := "ethereum-mainnet:erc721:0x1234567890123456789012345678901234567890:1"
	dataURI := "data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTAwIiBoZWlnaHQ9IjEwMCI+PC9zdmc+"
	mediaURLs := []string{dataURI}

	// Mock data URI health check - healthy
	mocks.dataURIChecker.EXPECT().
		Check(dataURI).
		Return(uri.DataURICheckResult{
			Valid: true,
			Error: nil,
		})

	// Mock database update for media health
	mocks.store.EXPECT().
		UpdateTokenMediaHealthByURL(ctx, dataURI, schema.MediaHealthStatusHealthy, nil).
		Return(nil)

	// Mock token retrieval
	token := &schema.Token{
		ID:       uint64(123),
		TokenCID: tokenCID,
	}
	mocks.store.EXPECT().
		GetTokenByTokenCID(ctx, tokenCID).
		Return(token, nil)

	// Mock viewability update - token becomes viewable
	changes := []store.TokenViewabilityChange{
		{
			TokenID:     token.ID,
			TokenCID:    tokenCID,
			OldViewable: false,
			NewViewable: true,
		},
	}
	mocks.store.EXPECT().
		BatchUpdateTokensViewability(ctx, []uint64{token.ID}).
		Return(changes, nil)

	result, err := mocks.executor.CheckMediaURLsHealthAndUpdateViewability(ctx, tokenCID, mediaURLs)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.True(t, result.IsViewable)
	assert.Equal(t, 1, len(result.HealthyURLs))
}

func TestCheckMediaURLsHealthAndUpdateViewability_Success_DataURI_Invalid(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	tokenCID := "ethereum-mainnet:erc721:0x1234567890123456789012345678901234567890:1"
	// Use a valid data URI format (has comma) but with invalid base64 content
	dataURI := "data:image/svg+xml;base64,invalid!!content"
	mediaURLs := []string{dataURI}
	errorMsg := "invalid data URI format"

	// Mock data URI health check - invalid
	mocks.dataURIChecker.EXPECT().
		Check(dataURI).
		Return(uri.DataURICheckResult{
			Valid: false,
			Error: &errorMsg,
		})

	// Mock database update for media health
	mocks.store.EXPECT().
		UpdateTokenMediaHealthByURL(ctx, dataURI, schema.MediaHealthStatusBroken, &errorMsg).
		Return(nil)

	// Mock token retrieval
	token := &schema.Token{
		ID:       uint64(123),
		TokenCID: tokenCID,
	}
	mocks.store.EXPECT().
		GetTokenByTokenCID(ctx, tokenCID).
		Return(token, nil)

	// Mock viewability update - no changes
	mocks.store.EXPECT().
		BatchUpdateTokensViewability(ctx, []uint64{token.ID}).
		Return([]store.TokenViewabilityChange{}, nil)

	// Mock get viewability status - not viewable
	mocks.store.EXPECT().
		GetTokensViewabilityByIDs(ctx, []uint64{token.ID}).
		Return([]store.TokenViewabilityInfo{
			{
				TokenID:    token.ID,
				TokenCID:   tokenCID,
				IsViewable: false,
			},
		}, nil)

	result, err := mocks.executor.CheckMediaURLsHealthAndUpdateViewability(ctx, tokenCID, mediaURLs)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.False(t, result.IsViewable)
	assert.Equal(t, 0, len(result.HealthyURLs))
}

func TestCheckMediaURLsHealthAndUpdateViewability_Success_MixedURLsAndDataURI(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	tokenCID := "ethereum-mainnet:erc721:0x1234567890123456789012345678901234567890:1"
	imageURL := "https://example.com/image.jpg"
	dataURI := "data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTAwIiBoZWlnaHQ9IjEwMCI+PC9zdmc+"
	mediaURLs := []string{imageURL, dataURI}

	// Mock URL health check
	mocks.urlChecker.EXPECT().
		Check(ctx, imageURL).
		Return(uri.HealthCheckResult{
			Status: uri.HealthStatusHealthy,
			Error:  nil,
		})

	// Mock data URI health check
	mocks.dataURIChecker.EXPECT().
		Check(dataURI).
		Return(uri.DataURICheckResult{
			Valid: true,
			Error: nil,
		})

	// Mock database updates
	mocks.store.EXPECT().
		UpdateTokenMediaHealthByURL(ctx, imageURL, schema.MediaHealthStatusHealthy, nil).
		Return(nil)

	mocks.store.EXPECT().
		UpdateTokenMediaHealthByURL(ctx, dataURI, schema.MediaHealthStatusHealthy, nil).
		Return(nil)

	// Mock token retrieval
	token := &schema.Token{
		ID:       uint64(123),
		TokenCID: tokenCID,
	}
	mocks.store.EXPECT().
		GetTokenByTokenCID(ctx, tokenCID).
		Return(token, nil)

	// Mock viewability update
	changes := []store.TokenViewabilityChange{
		{
			TokenID:     token.ID,
			TokenCID:    tokenCID,
			OldViewable: false,
			NewViewable: true,
		},
	}
	mocks.store.EXPECT().
		BatchUpdateTokensViewability(ctx, []uint64{token.ID}).
		Return(changes, nil)

	result, err := mocks.executor.CheckMediaURLsHealthAndUpdateViewability(ctx, tokenCID, mediaURLs)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.True(t, result.IsViewable)
	assert.Equal(t, 2, len(result.HealthyURLs)) // Both URLs are healthy (image + dataURI)
}

func TestCheckMediaURLsHealthAndUpdateViewability_Success_UnknownStatus(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	tokenCID := "ethereum-mainnet:erc721:0x1234567890123456789012345678901234567890:1"
	imageURL := "https://example.com/image.jpg"
	mediaURLs := []string{imageURL}
	errorMsg := "temporary network error"

	// Mock URL health check - transient error (unknown)
	mocks.urlChecker.EXPECT().
		Check(ctx, imageURL).
		Return(uri.HealthCheckResult{
			Status: uri.HealthStatusTransientError,
			Error:  &errorMsg,
		})

	// Mock database update for media health
	mocks.store.EXPECT().
		UpdateTokenMediaHealthByURL(ctx, imageURL, schema.MediaHealthStatusUnknown, &errorMsg).
		Return(nil)

	// Mock token retrieval
	token := &schema.Token{
		ID:       uint64(123),
		TokenCID: tokenCID,
	}
	mocks.store.EXPECT().
		GetTokenByTokenCID(ctx, tokenCID).
		Return(token, nil)

	// Mock viewability update - no changes
	mocks.store.EXPECT().
		BatchUpdateTokensViewability(ctx, []uint64{token.ID}).
		Return([]store.TokenViewabilityChange{}, nil)

	// Mock get viewability status - not viewable
	mocks.store.EXPECT().
		GetTokensViewabilityByIDs(ctx, []uint64{token.ID}).
		Return([]store.TokenViewabilityInfo{
			{
				TokenID:    token.ID,
				TokenCID:   tokenCID,
				IsViewable: false,
			},
		}, nil)

	result, err := mocks.executor.CheckMediaURLsHealthAndUpdateViewability(ctx, tokenCID, mediaURLs)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.False(t, result.IsViewable)
	assert.Equal(t, 0, len(result.HealthyURLs))
}

func TestCheckMediaURLsHealthAndUpdateViewability_EmptyURLList(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	tokenCID := "ethereum-mainnet:erc721:0x1234567890123456789012345678901234567890:1"
	mediaURLs := []string{}

	// No URLs to check, should return false

	result, err := mocks.executor.CheckMediaURLsHealthAndUpdateViewability(ctx, tokenCID, mediaURLs)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.False(t, result.IsViewable)
	assert.Equal(t, 0, len(result.HealthyURLs))
}

func TestCheckMediaURLsHealthAndUpdateViewability_TokenNotFound(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	tokenCID := "ethereum-mainnet:erc721:0x1234567890123456789012345678901234567890:1"
	imageURL := "https://example.com/image.jpg"
	mediaURLs := []string{imageURL}

	// Mock URL health check
	mocks.urlChecker.EXPECT().
		Check(ctx, imageURL).
		Return(uri.HealthCheckResult{
			Status: uri.HealthStatusHealthy,
			Error:  nil,
		})

	// Mock database update for media health
	mocks.store.EXPECT().
		UpdateTokenMediaHealthByURL(ctx, imageURL, schema.MediaHealthStatusHealthy, nil).
		Return(nil)

	// Mock token retrieval - not found
	mocks.store.EXPECT().
		GetTokenByTokenCID(ctx, tokenCID).
		Return(nil, nil)

	result, err := mocks.executor.CheckMediaURLsHealthAndUpdateViewability(ctx, tokenCID, mediaURLs)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Equal(t, domain.ErrTokenNotFound, err)
}

func TestCheckMediaURLsHealthAndUpdateViewability_GetTokenError(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	tokenCID := "ethereum-mainnet:erc721:0x1234567890123456789012345678901234567890:1"
	imageURL := "https://example.com/image.jpg"
	mediaURLs := []string{imageURL}
	dbError := errors.New("database connection error")

	// Mock URL health check
	mocks.urlChecker.EXPECT().
		Check(ctx, imageURL).
		Return(uri.HealthCheckResult{
			Status: uri.HealthStatusHealthy,
			Error:  nil,
		})

	// Mock database update for media health
	mocks.store.EXPECT().
		UpdateTokenMediaHealthByURL(ctx, imageURL, schema.MediaHealthStatusHealthy, nil).
		Return(nil)

	// Mock token retrieval - error
	mocks.store.EXPECT().
		GetTokenByTokenCID(ctx, tokenCID).
		Return(nil, dbError)

	result, err := mocks.executor.CheckMediaURLsHealthAndUpdateViewability(ctx, tokenCID, mediaURLs)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to get token")
}

func TestCheckMediaURLsHealthAndUpdateViewability_BatchUpdateViewabilityError(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	tokenCID := "ethereum-mainnet:erc721:0x1234567890123456789012345678901234567890:1"
	imageURL := "https://example.com/image.jpg"
	mediaURLs := []string{imageURL}
	dbError := errors.New("failed to update viewability")

	// Mock URL health check
	mocks.urlChecker.EXPECT().
		Check(ctx, imageURL).
		Return(uri.HealthCheckResult{
			Status: uri.HealthStatusHealthy,
			Error:  nil,
		})

	// Mock database update for media health
	mocks.store.EXPECT().
		UpdateTokenMediaHealthByURL(ctx, imageURL, schema.MediaHealthStatusHealthy, nil).
		Return(nil)

	// Mock token retrieval
	token := &schema.Token{
		ID:       uint64(123),
		TokenCID: tokenCID,
	}
	mocks.store.EXPECT().
		GetTokenByTokenCID(ctx, tokenCID).
		Return(token, nil)

	// Mock viewability update - error
	mocks.store.EXPECT().
		BatchUpdateTokensViewability(ctx, []uint64{token.ID}).
		Return(nil, dbError)

	result, err := mocks.executor.CheckMediaURLsHealthAndUpdateViewability(ctx, tokenCID, mediaURLs)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to update token viewability")
}

func TestCheckMediaURLsHealthAndUpdateViewability_UpdateMediaHealthError_NonFatal(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	tokenCID := "ethereum-mainnet:erc721:0x1234567890123456789012345678901234567890:1"
	imageURL := "https://example.com/image.jpg"
	mediaURLs := []string{imageURL}
	dbError := errors.New("failed to update media health")

	// Mock URL health check
	mocks.urlChecker.EXPECT().
		Check(ctx, imageURL).
		Return(uri.HealthCheckResult{
			Status: uri.HealthStatusHealthy,
			Error:  nil,
		})

	// Mock database update for media health - fails but should be non-fatal
	mocks.store.EXPECT().
		UpdateTokenMediaHealthByURL(ctx, imageURL, schema.MediaHealthStatusHealthy, nil).
		Return(dbError)

	// Mock token retrieval - should still proceed
	token := &schema.Token{
		ID:       uint64(123),
		TokenCID: tokenCID,
	}
	mocks.store.EXPECT().
		GetTokenByTokenCID(ctx, tokenCID).
		Return(token, nil)

	// Mock viewability update
	changes := []store.TokenViewabilityChange{
		{
			TokenID:     token.ID,
			TokenCID:    tokenCID,
			OldViewable: false,
			NewViewable: true,
		},
	}
	mocks.store.EXPECT().
		BatchUpdateTokensViewability(ctx, []uint64{token.ID}).
		Return(changes, nil)

	// Should succeed despite media health update error
	result, err := mocks.executor.CheckMediaURLsHealthAndUpdateViewability(ctx, tokenCID, mediaURLs)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.True(t, result.IsViewable)
	assert.Equal(t, 1, len(result.HealthyURLs))
}

func TestCheckMediaURLsHealthAndUpdateViewability_GetViewabilityError(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	tokenCID := "ethereum-mainnet:erc721:0x1234567890123456789012345678901234567890:1"
	imageURL := "https://example.com/image.jpg"
	mediaURLs := []string{imageURL}
	dbError := errors.New("failed to get viewability")

	// Mock URL health check
	mocks.urlChecker.EXPECT().
		Check(ctx, imageURL).
		Return(uri.HealthCheckResult{
			Status: uri.HealthStatusHealthy,
			Error:  nil,
		})

	// Mock database update for media health
	mocks.store.EXPECT().
		UpdateTokenMediaHealthByURL(ctx, imageURL, schema.MediaHealthStatusHealthy, (*string)(nil)).
		Return(nil)

	// Mock token retrieval
	token := &schema.Token{
		ID:       uint64(123),
		TokenCID: tokenCID,
	}
	mocks.store.EXPECT().
		GetTokenByTokenCID(ctx, tokenCID).
		Return(token, nil)

	// Mock viewability update - no changes
	mocks.store.EXPECT().
		BatchUpdateTokensViewability(ctx, []uint64{token.ID}).
		Return([]store.TokenViewabilityChange{}, nil)

	// Mock get viewability status - error
	mocks.store.EXPECT().
		GetTokensViewabilityByIDs(ctx, []uint64{token.ID}).
		Return(nil, dbError)

	result, err := mocks.executor.CheckMediaURLsHealthAndUpdateViewability(ctx, tokenCID, mediaURLs)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to get token viewability")
}

func TestCheckMediaURLsHealthAndUpdateViewability_ViewabilityInfoNotFound(t *testing.T) {
	mocks := setupTestExecutor(t)
	defer tearDownTestExecutor(mocks)

	ctx := context.Background()
	tokenCID := "ethereum-mainnet:erc721:0x1234567890123456789012345678901234567890:1"
	imageURL := "https://example.com/image.jpg"
	mediaURLs := []string{imageURL}

	// Mock URL health check
	mocks.urlChecker.EXPECT().
		Check(ctx, imageURL).
		Return(uri.HealthCheckResult{
			Status: uri.HealthStatusHealthy,
			Error:  nil,
		})

	// Mock database update for media health
	mocks.store.EXPECT().
		UpdateTokenMediaHealthByURL(ctx, imageURL, schema.MediaHealthStatusHealthy, (*string)(nil)).
		Return(nil)

	// Mock token retrieval
	token := &schema.Token{
		ID:       uint64(123),
		TokenCID: tokenCID,
	}
	mocks.store.EXPECT().
		GetTokenByTokenCID(ctx, tokenCID).
		Return(token, nil)

	// Mock viewability update - no changes
	mocks.store.EXPECT().
		BatchUpdateTokensViewability(ctx, []uint64{token.ID}).
		Return([]store.TokenViewabilityChange{}, nil)

	// Mock get viewability status - empty result
	mocks.store.EXPECT().
		GetTokensViewabilityByIDs(ctx, []uint64{token.ID}).
		Return([]store.TokenViewabilityInfo{}, nil)

	result, err := mocks.executor.CheckMediaURLsHealthAndUpdateViewability(ctx, tokenCID, mediaURLs)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "token viewability info not found")
}

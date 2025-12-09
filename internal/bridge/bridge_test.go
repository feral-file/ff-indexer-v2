package bridge_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/bridge"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	mockspkg "github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

func TestMain(m *testing.M) {
	// Initialize logger for tests
	err := logger.Initialize(logger.Config{
		Debug: false,
	})
	if err != nil {
		panic(err)
	}

	code := m.Run()
	os.Exit(code)
}

// testBridgeMocks contains all the mocks needed for testing the bridge
type testBridgeMocks struct {
	ctrl         *gomock.Controller
	natsJS       *mockspkg.MockNatsJetStream
	natsConn     *mockspkg.MockNatsConn
	jetStream    *mockspkg.MockJetStream
	store        *mockspkg.MockStore
	orchestrator *mockspkg.MockTemporalOrchestrator
	json         *mockspkg.MockJSON
	blacklist    *mockspkg.MockBlacklistRegistry
}

// setupTestBridge creates all the mocks and bridge for testing
func setupTestBridge(t *testing.T) *testBridgeMocks {
	ctrl := gomock.NewController(t)

	tm := &testBridgeMocks{
		ctrl:         ctrl,
		natsJS:       mockspkg.NewMockNatsJetStream(ctrl),
		natsConn:     mockspkg.NewMockNatsConn(ctrl),
		jetStream:    mockspkg.NewMockJetStream(ctrl),
		store:        mockspkg.NewMockStore(ctrl),
		orchestrator: mockspkg.NewMockTemporalOrchestrator(ctrl),
		json:         mockspkg.NewMockJSON(ctrl),
		blacklist:    mockspkg.NewMockBlacklistRegistry(ctrl),
	}

	return tm
}

// tearDownTestBridge cleans up the test mocks
func tearDownTestBridge(mocks *testBridgeMocks) {
	mocks.ctrl.Finish()
}

func TestBridge_NewBridge_Success(t *testing.T) {
	mocks := setupTestBridge(t)
	defer tearDownTestBridge(mocks)

	ctx := context.Background()

	config := bridge.Config{
		URL:               "nats://localhost:4222",
		StreamName:        "events",
		ConsumerName:      "bridge-consumer",
		MaxReconnects:     10,
		ReconnectWait:     1 * time.Second,
		ConnectionName:    "test-bridge",
		AckWaitTimeout:    30 * time.Second,
		MaxDeliver:        5,
		TemporalTaskQueue: "test-queue",
	}

	// Mock NATS connection
	mocks.natsJS.
		EXPECT().
		Connect(config.URL, gomock.Any()).
		Return(mocks.natsConn, mocks.jetStream, nil)

	b, err := bridge.NewBridge(
		ctx,
		config,
		mocks.natsJS,
		mocks.store,
		mocks.orchestrator,
		mocks.json,
		mocks.blacklist,
	)

	assert.NoError(t, err)
	assert.NotNil(t, b)
}

func TestBridge_NewBridge_ConnectError(t *testing.T) {
	mocks := setupTestBridge(t)
	defer tearDownTestBridge(mocks)

	ctx := context.Background()

	config := bridge.Config{
		URL:               "nats://localhost:4222",
		StreamName:        "events",
		ConsumerName:      "bridge-consumer",
		MaxReconnects:     10,
		ReconnectWait:     1 * time.Second,
		ConnectionName:    "test-bridge",
		AckWaitTimeout:    30 * time.Second,
		MaxDeliver:        5,
		TemporalTaskQueue: "test-queue",
	}

	// Mock NATS connection to return error
	mocks.natsJS.
		EXPECT().
		Connect(gomock.Any(), gomock.Any()).
		Return(nil, nil, assert.AnError)

	b, err := bridge.NewBridge(
		ctx,
		config,
		mocks.natsJS,
		mocks.store,
		mocks.orchestrator,
		mocks.json,
		mocks.blacklist,
	)

	assert.Error(t, err)
	assert.Nil(t, b)
	assert.Contains(t, err.Error(), "failed to connect to NATS")
}

func TestBridge_Run_CreateConsumerError(t *testing.T) {
	mocks := setupTestBridge(t)
	defer tearDownTestBridge(mocks)

	ctx := context.Background()

	config := bridge.Config{
		URL:               "nats://localhost:4222",
		StreamName:        "events",
		ConsumerName:      "bridge-consumer",
		MaxReconnects:     10,
		ReconnectWait:     1 * time.Second,
		ConnectionName:    "test-bridge",
		AckWaitTimeout:    30 * time.Second,
		MaxDeliver:        5,
		TemporalTaskQueue: "test-queue",
	}

	// Mock NATS connection
	mocks.natsJS.
		EXPECT().
		Connect(gomock.Any(), gomock.Any()).
		Return(mocks.natsConn, mocks.jetStream, nil)

	b, err := bridge.NewBridge(
		ctx,
		config,
		mocks.natsJS,
		mocks.store,
		mocks.orchestrator,
		mocks.json,
		mocks.blacklist,
	)
	assert.NoError(t, err)
	assert.NotNil(t, b)

	// Mock CreateOrUpdateConsumer to return error
	mocks.jetStream.
		EXPECT().
		CreateOrUpdateConsumer(gomock.Any(),
			"events",
			jetstream.ConsumerConfig{
				Durable:       config.ConsumerName,
				AckPolicy:     jetstream.AckExplicitPolicy,
				AckWait:       config.AckWaitTimeout,
				MaxDeliver:    config.MaxDeliver,
				FilterSubject: "events.*.>",
			}).
		Return(nil, assert.AnError)

	err = b.Run(ctx)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create/update consumer")
}

func TestBridge_Run_ConsumerInfoError(t *testing.T) {
	mocks := setupTestBridge(t)
	defer tearDownTestBridge(mocks)

	ctx := context.Background()

	config := bridge.Config{
		URL:               "nats://localhost:4222",
		StreamName:        "events",
		ConsumerName:      "bridge-consumer",
		MaxReconnects:     10,
		ReconnectWait:     1 * time.Second,
		ConnectionName:    "test-bridge",
		AckWaitTimeout:    30 * time.Second,
		MaxDeliver:        5,
		TemporalTaskQueue: "test-queue",
	}

	// Mock NATS connection
	mocks.natsJS.
		EXPECT().
		Connect(gomock.Any(), gomock.Any()).
		Return(mocks.natsConn, mocks.jetStream, nil)

	b, err := bridge.NewBridge(
		ctx,
		config,
		mocks.natsJS,
		mocks.store,
		mocks.orchestrator,
		mocks.json,
		mocks.blacklist,
	)
	assert.NoError(t, err)
	assert.NotNil(t, b)

	// Mock CreateOrUpdateConsumer to return a consumer with Info error
	consumer := mockspkg.NewMockConsumer(mocks.ctrl)
	consumer.EXPECT().
		Info(gomock.Any()).
		Return(nil, assert.AnError)

	mocks.jetStream.
		EXPECT().
		CreateOrUpdateConsumer(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(consumer, nil)

	err = b.Run(ctx)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get consumer info")
}

func TestBridge_Run_ConsumeError(t *testing.T) {
	mocks := setupTestBridge(t)
	defer tearDownTestBridge(mocks)

	ctx := context.Background()

	config := bridge.Config{
		URL:               "nats://localhost:4222",
		StreamName:        "events",
		ConsumerName:      "bridge-consumer",
		MaxReconnects:     10,
		ReconnectWait:     1 * time.Second,
		ConnectionName:    "test-bridge",
		AckWaitTimeout:    30 * time.Second,
		MaxDeliver:        5,
		TemporalTaskQueue: "test-queue",
	}

	// Mock NATS connection
	mocks.natsJS.
		EXPECT().
		Connect(gomock.Any(), gomock.Any()).
		Return(mocks.natsConn, mocks.jetStream, nil)

	b, err := bridge.NewBridge(
		ctx,
		config,
		mocks.natsJS,
		mocks.store,
		mocks.orchestrator,
		mocks.json,
		mocks.blacklist,
	)
	assert.NoError(t, err)
	assert.NotNil(t, b)

	// Mock CreateOrUpdateConsumer to return a consumer with Consume error
	consumer := mockspkg.NewMockConsumer(mocks.ctrl)
	consumer.EXPECT().
		Info(gomock.Any()).
		Return(&jetstream.ConsumerInfo{Name: "bridge-consumer"}, nil)
	consumer.EXPECT().
		Consume(gomock.Any(), gomock.Any()).
		Return(nil, assert.AnError)

	mocks.jetStream.
		EXPECT().
		CreateOrUpdateConsumer(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(consumer, nil)

	err = b.Run(ctx)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create subscription")
}

func TestBridge_Run_ContextCancellation(t *testing.T) {
	mocks := setupTestBridge(t)
	defer tearDownTestBridge(mocks)

	ctx, cancel := context.WithCancel(context.Background())

	config := bridge.Config{
		URL:               "nats://localhost:4222",
		StreamName:        "events",
		ConsumerName:      "bridge-consumer",
		MaxReconnects:     10,
		ReconnectWait:     1 * time.Second,
		ConnectionName:    "test-bridge",
		AckWaitTimeout:    30 * time.Second,
		MaxDeliver:        5,
		TemporalTaskQueue: "test-queue",
	}

	// Mock NATS connection
	mocks.natsJS.
		EXPECT().
		Connect(gomock.Any(), gomock.Any()).
		Return(mocks.natsConn, mocks.jetStream, nil)

	b, err := bridge.NewBridge(
		ctx,
		config,
		mocks.natsJS,
		mocks.store,
		mocks.orchestrator,
		mocks.json,
		mocks.blacklist,
	)
	assert.NoError(t, err)
	assert.NotNil(t, b)

	// Mock CreateOrUpdateConsumer to return a consumer
	consumer := mockspkg.NewMockConsumer(mocks.ctrl)
	consumeContext := mockspkg.NewMockConsumeContext(mocks.ctrl)
	consumeContext.EXPECT().
		Stop().
		AnyTimes()

	consumer.EXPECT().
		Info(gomock.Any()).
		Return(&jetstream.ConsumerInfo{Name: "bridge-consumer"}, nil)
	consumer.EXPECT().
		Consume(gomock.Any(), gomock.Any()).
		DoAndReturn(func(handler adapter.MessageHandler, opts ...jetstream.PullConsumeOpt) (adapter.ConsumeContext, error) {
			// Start a goroutine to simulate messages
			go func() {
				// Cancel context to stop the bridge
				cancel()
			}()
			return consumeContext, nil
		})

	mocks.jetStream.
		EXPECT().
		CreateOrUpdateConsumer(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(consumer, nil)

	// Use a channel to capture the error
	errChan := make(chan error, 1)
	go func() {
		errChan <- b.Run(ctx)
	}()

	// Wait for context cancellation
	select {
	case err := <-errChan:
		assert.Error(t, err)
		assert.Equal(t, context.Canceled, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Test timed out")
	}
}

func TestBridge_Close(t *testing.T) {
	mocks := setupTestBridge(t)
	defer tearDownTestBridge(mocks)

	ctx := context.Background()

	config := bridge.Config{
		URL:               "nats://localhost:4222",
		StreamName:        "events",
		ConsumerName:      "bridge-consumer",
		MaxReconnects:     10,
		ReconnectWait:     1 * time.Second,
		ConnectionName:    "test-bridge",
		AckWaitTimeout:    30 * time.Second,
		MaxDeliver:        5,
		TemporalTaskQueue: "test-queue",
	}

	// Mock NATS connection
	mocks.natsJS.
		EXPECT().
		Connect(gomock.Any(), gomock.Any()).
		Return(mocks.natsConn, mocks.jetStream, nil)

	// Mock Close
	mocks.natsConn.
		EXPECT().
		Close()

	b, err := bridge.NewBridge(
		ctx,
		config,
		mocks.natsJS,
		mocks.store,
		mocks.orchestrator,
		mocks.json,
		mocks.blacklist,
	)
	assert.NoError(t, err)
	assert.NotNil(t, b)

	b.Close()
}

func TestBridge_Close_NilConnection(t *testing.T) {
	mocks := setupTestBridge(t)
	defer tearDownTestBridge(mocks)

	ctx := context.Background()

	config := bridge.Config{
		URL:               "nats://localhost:4222",
		StreamName:        "events",
		ConsumerName:      "bridge-consumer",
		MaxReconnects:     10,
		ReconnectWait:     1 * time.Second,
		ConnectionName:    "test-bridge",
		AckWaitTimeout:    30 * time.Second,
		MaxDeliver:        5,
		TemporalTaskQueue: "test-queue",
	}

	// Mock NATS connection to return nil (simulating error case)
	mocks.natsJS.
		EXPECT().
		Connect(gomock.Any(), gomock.Any()).
		Return(nil, nil, assert.AnError)

	b, err := bridge.NewBridge(
		ctx,
		config,
		mocks.natsJS,
		mocks.store,
		mocks.orchestrator,
		mocks.json,
		mocks.blacklist,
	)
	assert.Error(t, err)
	assert.Nil(t, b)

	// Close should not panic even if b is nil
	if b != nil {
		b.Close()
	}
}

func TestBridge_ProcessMessage_Success_MintEvent(t *testing.T) {
	mocks := setupTestBridge(t)
	defer tearDownTestBridge(mocks)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config := bridge.Config{
		URL:               "nats://localhost:4222",
		StreamName:        "events",
		ConsumerName:      "bridge-consumer",
		MaxReconnects:     10,
		ReconnectWait:     1 * time.Second,
		ConnectionName:    "test-bridge",
		AckWaitTimeout:    30 * time.Second,
		MaxDeliver:        5,
		TemporalTaskQueue: "test-queue",
	}

	// Mock NATS connection
	mocks.natsJS.
		EXPECT().
		Connect(config.URL, gomock.Any()).
		Return(mocks.natsConn, mocks.jetStream, nil)

	b, err := bridge.NewBridge(
		ctx,
		config,
		mocks.natsJS,
		mocks.store,
		mocks.orchestrator,
		mocks.json,
		mocks.blacklist,
	)
	assert.NoError(t, err)
	assert.NotNil(t, b)

	// Create a mock message
	msg := mockspkg.NewMockMessage(mocks.ctrl)
	toAddr := "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb"
	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeMint,
		FromAddress:     nil,
		ToAddress:       &toAddr,
		Quantity:        "1",
		TxHash:          "0xabc123",
		BlockNumber:     1234567,
		Timestamp:       time.Now(),
		TxIndex:         1,
	}

	eventJSON := `{"chain":"eip155:1","standard":"erc721","contract_address":"0x1234567890123456789012345678901234567890","token_number":"1","event_type":"mint","from_address":null,"to_address":"0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb","quantity":"1","tx_hash":"0xabc123","block_number":1234567,"timestamp":"2024-01-01T00:00:00Z","tx_index":1}`

	// Mock message methods
	msg.
		EXPECT().
		Data().
		Return([]byte(eventJSON)).
		MinTimes(1)
	msg.
		EXPECT().
		Metadata().
		Return(&jetstream.MsgMetadata{NumDelivered: 1}, nil).
		MinTimes(1)

	// Mock JSON unmarshal
	mocks.json.EXPECT().
		Unmarshal([]byte(eventJSON), gomock.Any()).
		DoAndReturn(func(data []byte, v interface{}) error {
			eventPtr := v.(*domain.BlockchainEvent)
			*eventPtr = *event
			return nil
		})

	// Mock blacklist check (not blacklisted)
	mocks.blacklist.
		EXPECT().
		IsTokenCIDBlacklisted(event.TokenCID()).
		Return(false)

	// Mock store check (token already indexed)
	mocks.store.EXPECT().
		GetTokenByTokenCID(gomock.Any(), event.TokenCID().String()).
		Return(&schema.Token{
			ID:              1,
			TokenCID:        event.TokenCID().String(),
			Chain:           event.Chain,
			Standard:        event.Standard,
			ContractAddress: event.ContractAddress,
			TokenNumber:     event.TokenNumber,
		}, nil)

	// Mock orchestrator to execute workflow
	mocks.orchestrator.EXPECT().
		ExecuteWorkflow(gomock.Any(), gomock.Any(), gomock.Any(), event).
		Return(nil, nil)

	// Mock message Ack
	msg.EXPECT().Ack().Return(nil)

	// Set up consumer to capture message handler
	var messageHandler adapter.MessageHandler
	consumer := mockspkg.NewMockConsumer(mocks.ctrl)
	consumeContext := mockspkg.NewMockConsumeContext(mocks.ctrl)

	consumer.EXPECT().
		Info(gomock.Any()).
		Return(&jetstream.ConsumerInfo{Name: "bridge-consumer"}, nil)

	consumer.EXPECT().
		Consume(gomock.Any(), gomock.Any()).
		DoAndReturn(func(handler adapter.MessageHandler, opts ...jetstream.PullConsumeOpt) (adapter.ConsumeContext, error) {
			messageHandler = handler
			return consumeContext, nil
		})

	consumeContext.EXPECT().Stop().AnyTimes()

	mocks.jetStream.EXPECT().
		CreateOrUpdateConsumer(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(consumer, nil)

	// Start the bridge in a goroutine
	go func() {
		_ = b.Run(ctx)
	}()

	// Wait for the consumer to be set up
	time.Sleep(100 * time.Millisecond)

	// Send message through the handler
	messageHandler(msg)

	// Give goroutine time to process
	time.Sleep(200 * time.Millisecond)

	// Cancel context to stop the bridge
	cancel()
}

func TestBridge_ProcessMessage_Success_TransferEvent(t *testing.T) {
	mocks := setupTestBridge(t)
	defer tearDownTestBridge(mocks)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config := bridge.Config{
		URL:               "nats://localhost:4222",
		StreamName:        "events",
		ConsumerName:      "bridge-consumer",
		TemporalTaskQueue: "test-queue",
	}

	mocks.natsJS.EXPECT().Connect(config.URL, gomock.Any()).Return(mocks.natsConn, mocks.jetStream, nil)

	b, err := bridge.NewBridge(ctx, config, mocks.natsJS, mocks.store, mocks.orchestrator, mocks.json, mocks.blacklist)
	assert.NoError(t, err)

	msg := mockspkg.NewMockMessage(mocks.ctrl)
	fromAddr := "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEa"
	toAddr := "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb"
	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeTransfer,
		FromAddress:     &fromAddr,
		ToAddress:       &toAddr,
		Quantity:        "1",
		TxHash:          "0xabc123",
		BlockNumber:     1234567,
		Timestamp:       time.Now(),
		TxIndex:         1,
	}

	eventJSON := []byte(`{"chain":"eip155:1","standard":"erc721","contract_address":"0x1234567890123456789012345678901234567890","token_number":"1","event_type":"transfer"}`)

	msg.
		EXPECT().
		Data().
		Return(eventJSON).
		MinTimes(1)
	msg.EXPECT().Metadata().
		Return(&jetstream.MsgMetadata{NumDelivered: 1}, nil).
		MinTimes(1)

	mocks.json.
		EXPECT().
		Unmarshal(eventJSON, gomock.Any()).
		DoAndReturn(func(data []byte, v interface{}) error {
			*v.(*domain.BlockchainEvent) = *event
			return nil
		})

	mocks.blacklist.
		EXPECT().
		IsTokenCIDBlacklisted(event.TokenCID()).
		Return(false)
	mocks.store.
		EXPECT().
		GetTokenByTokenCID(gomock.Any(), event.TokenCID().String()).
		Return(&schema.Token{ID: 1}, nil)
	mocks.orchestrator.
		EXPECT().
		ExecuteWorkflow(gomock.Any(), gomock.Any(), gomock.Any(), event).
		Return(nil, nil)
	msg.EXPECT().Ack().Return(nil)

	var messageHandler adapter.MessageHandler
	consumer := mockspkg.NewMockConsumer(mocks.ctrl)
	consumeContext := mockspkg.NewMockConsumeContext(mocks.ctrl)

	consumer.
		EXPECT().
		Info(gomock.Any()).
		Return(&jetstream.ConsumerInfo{Name: "bridge-consumer"}, nil)
	consumer.
		EXPECT().
		Consume(gomock.Any(), gomock.Any()).
		DoAndReturn(func(handler adapter.MessageHandler, opts ...jetstream.PullConsumeOpt) (adapter.ConsumeContext, error) {
			messageHandler = handler
			return consumeContext, nil
		})
	consumeContext.
		EXPECT().
		Stop().
		AnyTimes()

	mocks.jetStream.
		EXPECT().
		CreateOrUpdateConsumer(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(consumer, nil)

	go func() { _ = b.Run(ctx) }()
	time.Sleep(100 * time.Millisecond)

	messageHandler(msg)
	time.Sleep(200 * time.Millisecond)

	cancel()
}

func TestBridge_ProcessMessage_Success_BurnEvent(t *testing.T) {
	mocks := setupTestBridge(t)
	defer tearDownTestBridge(mocks)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config := bridge.Config{
		URL:               "nats://localhost:4222",
		StreamName:        "events",
		ConsumerName:      "bridge-consumer",
		TemporalTaskQueue: "test-queue",
	}

	mocks.natsJS.
		EXPECT().
		Connect(config.URL, gomock.Any()).
		Return(mocks.natsConn, mocks.jetStream, nil)

	b, err := bridge.NewBridge(ctx, config, mocks.natsJS, mocks.store, mocks.orchestrator, mocks.json, mocks.blacklist)
	assert.NoError(t, err)

	msg := mockspkg.NewMockMessage(mocks.ctrl)
	fromAddr := "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb"
	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeBurn,
		FromAddress:     &fromAddr,
		ToAddress:       nil,
		Quantity:        "1",
		TxHash:          "0xabc123",
		BlockNumber:     1234567,
		Timestamp:       time.Now(),
		TxIndex:         1,
	}

	eventJSON := []byte(`{"chain":"eip155:1","standard":"erc721","contract_address":"0x1234567890123456789012345678901234567890","token_number":"1","event_type":"burn"}`)

	msg.EXPECT().
		Data().
		Return(eventJSON).
		MinTimes(1)
	msg.EXPECT().
		Metadata().
		Return(&jetstream.MsgMetadata{NumDelivered: 1}, nil).
		MinTimes(1)

	mocks.json.
		EXPECT().
		Unmarshal(eventJSON, gomock.Any()).
		DoAndReturn(func(data []byte, v interface{}) error {
			*v.(*domain.BlockchainEvent) = *event
			return nil
		})

	mocks.blacklist.
		EXPECT().
		IsTokenCIDBlacklisted(event.TokenCID()).
		Return(false)
	mocks.store.
		EXPECT().
		GetTokenByTokenCID(gomock.Any(), event.TokenCID().String()).
		Return(&schema.Token{ID: 1}, nil)
	mocks.orchestrator.
		EXPECT().
		ExecuteWorkflow(gomock.Any(), gomock.Any(), gomock.Any(), event).
		Return(nil, nil)
	msg.
		EXPECT().
		Ack().
		Return(nil)

	var messageHandler adapter.MessageHandler
	consumer := mockspkg.NewMockConsumer(mocks.ctrl)
	consumeContext := mockspkg.NewMockConsumeContext(mocks.ctrl)

	consumer.
		EXPECT().
		Info(gomock.Any()).Return(&jetstream.ConsumerInfo{Name: "bridge-consumer"}, nil)
	consumer.
		EXPECT().
		Consume(gomock.Any(), gomock.Any()).
		DoAndReturn(func(handler adapter.MessageHandler, opts ...jetstream.PullConsumeOpt) (adapter.ConsumeContext, error) {
			messageHandler = handler
			return consumeContext, nil
		})
	consumeContext.
		EXPECT().
		Stop().
		AnyTimes()

	mocks.jetStream.
		EXPECT().
		CreateOrUpdateConsumer(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(consumer, nil)

	go func() { _ = b.Run(ctx) }()
	time.Sleep(100 * time.Millisecond)

	messageHandler(msg)
	time.Sleep(200 * time.Millisecond)

	cancel()
}

func TestBridge_ProcessMessage_InvalidJSON(t *testing.T) {
	mocks := setupTestBridge(t)
	defer tearDownTestBridge(mocks)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config := bridge.Config{
		URL:               "nats://localhost:4222",
		StreamName:        "events",
		ConsumerName:      "bridge-consumer",
		TemporalTaskQueue: "test-queue",
	}

	mocks.natsJS.
		EXPECT().
		Connect(config.URL, gomock.Any()).
		Return(mocks.natsConn, mocks.jetStream, nil)

	b, err := bridge.NewBridge(ctx, config, mocks.natsJS, mocks.store, mocks.orchestrator, mocks.json, mocks.blacklist)
	assert.NoError(t, err)

	msg := mockspkg.NewMockMessage(mocks.ctrl)
	invalidJSON := []byte(`{invalid json}`)

	msg.
		EXPECT().
		Data().
		Return(invalidJSON).
		MinTimes(1)
	msg.EXPECT().Metadata().
		Return(&jetstream.MsgMetadata{NumDelivered: 1}, nil).
		MinTimes(1)

	// Mock JSON unmarshal to return error
	mocks.json.
		EXPECT().
		Unmarshal(invalidJSON, gomock.Any()).
		Return(assert.AnError)

	// Expect message to be terminated
	msg.
		EXPECT().
		Term().
		Return(nil)

	var messageHandler adapter.MessageHandler
	consumer := mockspkg.NewMockConsumer(mocks.ctrl)
	consumeContext := mockspkg.NewMockConsumeContext(mocks.ctrl)

	consumer.
		EXPECT().
		Info(gomock.Any()).
		Return(&jetstream.ConsumerInfo{Name: "bridge-consumer"}, nil)
	consumer.
		EXPECT().
		Consume(gomock.Any(), gomock.Any()).
		DoAndReturn(func(handler adapter.MessageHandler, opts ...jetstream.PullConsumeOpt) (adapter.ConsumeContext, error) {
			messageHandler = handler
			return consumeContext, nil
		})
	consumeContext.
		EXPECT().
		Stop().
		AnyTimes()

	mocks.jetStream.
		EXPECT().
		CreateOrUpdateConsumer(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(consumer, nil)

	go func() { _ = b.Run(ctx) }()
	time.Sleep(100 * time.Millisecond)

	messageHandler(msg)
	time.Sleep(200 * time.Millisecond)

	cancel()
}

func TestBridge_ProcessMessage_BlacklistedToken(t *testing.T) {
	mocks := setupTestBridge(t)
	defer tearDownTestBridge(mocks)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config := bridge.Config{
		URL:               "nats://localhost:4222",
		StreamName:        "events",
		ConsumerName:      "bridge-consumer",
		TemporalTaskQueue: "test-queue",
	}

	mocks.natsJS.
		EXPECT().
		Connect(config.URL, gomock.Any()).
		Return(mocks.natsConn, mocks.jetStream, nil)

	b, err := bridge.NewBridge(ctx, config, mocks.natsJS, mocks.store, mocks.orchestrator, mocks.json, mocks.blacklist)
	assert.NoError(t, err)

	msg := mockspkg.NewMockMessage(mocks.ctrl)
	toAddr := "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb"
	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeMint,
		FromAddress:     nil,
		ToAddress:       &toAddr,
		Quantity:        "1",
		TxHash:          "0xabc123",
		BlockNumber:     1234567,
		Timestamp:       time.Now(),
		TxIndex:         1,
	}

	eventJSON := []byte(`{"chain":"eip155:1","standard":"erc721","contract_address":"0x1234567890123456789012345678901234567890","token_number":"1","event_type":"mint"}`)

	msg.
		EXPECT().
		Data().
		Return(eventJSON).
		MinTimes(1)
	msg.EXPECT().Metadata().
		Return(&jetstream.MsgMetadata{NumDelivered: 1}, nil).
		MinTimes(1)

	mocks.json.
		EXPECT().
		Unmarshal(eventJSON, gomock.Any()).
		DoAndReturn(func(data []byte, v interface{}) error {
			*v.(*domain.BlockchainEvent) = *event
			return nil
		})

	// Mock blacklist check (token IS blacklisted)
	mocks.blacklist.
		EXPECT().
		IsTokenCIDBlacklisted(event.TokenCID()).
		Return(true)

	// Expect message to be acknowledged (dropped)
	msg.
		EXPECT().
		Ack().
		Return(nil)

	var messageHandler adapter.MessageHandler
	consumer := mockspkg.NewMockConsumer(mocks.ctrl)
	consumeContext := mockspkg.NewMockConsumeContext(mocks.ctrl)

	consumer.
		EXPECT().
		Info(gomock.Any()).
		Return(&jetstream.ConsumerInfo{Name: "bridge-consumer"}, nil)
	consumer.
		EXPECT().
		Consume(gomock.Any(), gomock.Any()).
		DoAndReturn(func(handler adapter.MessageHandler, opts ...jetstream.PullConsumeOpt) (adapter.ConsumeContext, error) {
			messageHandler = handler
			return consumeContext, nil
		})
	consumeContext.
		EXPECT().
		Stop().
		AnyTimes()

	mocks.jetStream.
		EXPECT().
		CreateOrUpdateConsumer(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(consumer, nil)

	go func() { _ = b.Run(ctx) }()
	time.Sleep(100 * time.Millisecond)

	messageHandler(msg)
	time.Sleep(200 * time.Millisecond)

	cancel()
}

func TestBridge_ProcessMessage_TokenNotIndexed_AddressNotWatched(t *testing.T) {
	mocks := setupTestBridge(t)
	defer tearDownTestBridge(mocks)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config := bridge.Config{
		URL:               "nats://localhost:4222",
		StreamName:        "events",
		ConsumerName:      "bridge-consumer",
		TemporalTaskQueue: "test-queue",
	}

	mocks.natsJS.
		EXPECT().
		Connect(config.URL, gomock.Any()).
		Return(mocks.natsConn, mocks.jetStream, nil)

	b, err := bridge.NewBridge(ctx, config, mocks.natsJS, mocks.store, mocks.orchestrator, mocks.json, mocks.blacklist)
	assert.NoError(t, err)

	msg := mockspkg.NewMockMessage(mocks.ctrl)
	toAddr := "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb"
	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeMint,
		FromAddress:     nil,
		ToAddress:       &toAddr,
		Quantity:        "1",
		TxHash:          "0xabc123",
		BlockNumber:     1234567,
		Timestamp:       time.Now(),
		TxIndex:         1,
	}

	eventJSON := []byte(`{"chain":"eip155:1","standard":"erc721","contract_address":"0x1234567890123456789012345678901234567890","token_number":"1","event_type":"mint"}`)

	msg.
		EXPECT().
		Data().
		Return(eventJSON).
		MinTimes(1)
	msg.EXPECT().Metadata().
		Return(&jetstream.MsgMetadata{NumDelivered: 1}, nil).
		MinTimes(1)

	mocks.json.
		EXPECT().
		Unmarshal(eventJSON, gomock.Any()).
		DoAndReturn(func(data []byte, v interface{}) error {
			*v.(*domain.BlockchainEvent) = *event
			return nil
		})

	mocks.blacklist.
		EXPECT().
		IsTokenCIDBlacklisted(event.TokenCID()).
		Return(false)

	// Token not indexed
	mocks.store.
		EXPECT().
		GetTokenByTokenCID(gomock.Any(), event.TokenCID().String()).
		Return(nil, nil)

	// Address not watched
	mocks.store.
		EXPECT().
		IsAnyAddressWatched(gomock.Any(), event.Chain, gomock.Any()).
		Return(false, nil)

	// Expect message to be acknowledged (dropped)
	msg.
		EXPECT().
		Ack().
		Return(nil)

	var messageHandler adapter.MessageHandler
	consumer := mockspkg.NewMockConsumer(mocks.ctrl)
	consumeContext := mockspkg.NewMockConsumeContext(mocks.ctrl)

	consumer.
		EXPECT().
		Info(gomock.Any()).
		Return(&jetstream.ConsumerInfo{Name: "bridge-consumer"}, nil)
	consumer.
		EXPECT().
		Consume(gomock.Any(), gomock.Any()).
		DoAndReturn(func(handler adapter.MessageHandler, opts ...jetstream.PullConsumeOpt) (adapter.ConsumeContext, error) {
			messageHandler = handler
			return consumeContext, nil
		})
	consumeContext.
		EXPECT().
		Stop().
		AnyTimes()

	mocks.jetStream.
		EXPECT().
		CreateOrUpdateConsumer(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(consumer, nil)

	go func() { _ = b.Run(ctx) }()
	time.Sleep(100 * time.Millisecond)

	messageHandler(msg)
	time.Sleep(200 * time.Millisecond)

	cancel()
}

func TestBridge_ProcessMessage_TokenNotIndexed_AddressWatched(t *testing.T) {
	mocks := setupTestBridge(t)
	defer tearDownTestBridge(mocks)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config := bridge.Config{
		URL:               "nats://localhost:4222",
		StreamName:        "events",
		ConsumerName:      "bridge-consumer",
		TemporalTaskQueue: "test-queue",
	}

	mocks.natsJS.
		EXPECT().
		Connect(config.URL, gomock.Any()).
		Return(mocks.natsConn, mocks.jetStream, nil)

	b, err := bridge.NewBridge(ctx, config, mocks.natsJS, mocks.store, mocks.orchestrator, mocks.json, mocks.blacklist)
	assert.NoError(t, err)

	msg := mockspkg.NewMockMessage(mocks.ctrl)
	toAddr := "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb"
	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeMint,
		FromAddress:     nil,
		ToAddress:       &toAddr,
		Quantity:        "1",
		TxHash:          "0xabc123",
		BlockNumber:     1234567,
		Timestamp:       time.Now(),
		TxIndex:         1,
	}

	eventJSON := []byte(`{"chain":"eip155:1","standard":"erc721","contract_address":"0x1234567890123456789012345678901234567890","token_number":"1","event_type":"mint"}`)

	msg.
		EXPECT().
		Data().
		Return(eventJSON).
		MinTimes(1)
	msg.EXPECT().Metadata().
		Return(&jetstream.MsgMetadata{NumDelivered: 1}, nil).
		MinTimes(1)

	mocks.json.
		EXPECT().
		Unmarshal(eventJSON, gomock.Any()).
		DoAndReturn(func(data []byte, v interface{}) error {
			*v.(*domain.BlockchainEvent) = *event
			return nil
		})

	mocks.blacklist.
		EXPECT().
		IsTokenCIDBlacklisted(event.TokenCID()).
		Return(false)

	// Token not indexed
	mocks.store.
		EXPECT().
		GetTokenByTokenCID(gomock.Any(), event.TokenCID().String()).
		Return(nil, nil)

	// Address IS watched
	mocks.store.
		EXPECT().
		IsAnyAddressWatched(gomock.Any(), event.Chain, gomock.Any()).
		Return(true, nil)

	// Should forward to worker
	mocks.orchestrator.
		EXPECT().
		ExecuteWorkflow(gomock.Any(), gomock.Any(), gomock.Any(), event).
		Return(nil, nil)

	// Expect message to be acknowledged
	msg.
		EXPECT().
		Ack().
		Return(nil)

	var messageHandler adapter.MessageHandler
	consumer := mockspkg.NewMockConsumer(mocks.ctrl)
	consumeContext := mockspkg.NewMockConsumeContext(mocks.ctrl)

	consumer.
		EXPECT().
		Info(gomock.Any()).
		Return(&jetstream.ConsumerInfo{Name: "bridge-consumer"}, nil)
	consumer.
		EXPECT().
		Consume(gomock.Any(), gomock.Any()).
		DoAndReturn(func(handler adapter.MessageHandler, opts ...jetstream.PullConsumeOpt) (adapter.ConsumeContext, error) {
			messageHandler = handler
			return consumeContext, nil
		})
	consumeContext.
		EXPECT().
		Stop().
		AnyTimes()

	mocks.jetStream.
		EXPECT().
		CreateOrUpdateConsumer(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(consumer, nil)

	go func() { _ = b.Run(ctx) }()
	time.Sleep(100 * time.Millisecond)

	messageHandler(msg)
	time.Sleep(200 * time.Millisecond)

	cancel()
}

func TestBridge_ProcessMessage_StoreError_GetToken(t *testing.T) {
	mocks := setupTestBridge(t)
	defer tearDownTestBridge(mocks)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config := bridge.Config{
		URL:               "nats://localhost:4222",
		StreamName:        "events",
		ConsumerName:      "bridge-consumer",
		TemporalTaskQueue: "test-queue",
	}

	mocks.natsJS.
		EXPECT().
		Connect(config.URL, gomock.Any()).
		Return(mocks.natsConn, mocks.jetStream, nil)

	b, err := bridge.NewBridge(ctx, config, mocks.natsJS, mocks.store, mocks.orchestrator, mocks.json, mocks.blacklist)
	assert.NoError(t, err)

	msg := mockspkg.NewMockMessage(mocks.ctrl)
	toAddr := "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb"
	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeMint,
		FromAddress:     nil,
		ToAddress:       &toAddr,
		Quantity:        "1",
		TxHash:          "0xabc123",
		BlockNumber:     1234567,
		Timestamp:       time.Now(),
		TxIndex:         1,
	}

	eventJSON := []byte(`{"chain":"eip155:1","standard":"erc721","contract_address":"0x1234567890123456789012345678901234567890","token_number":"1","event_type":"mint"}`)

	msg.
		EXPECT().
		Data().
		Return(eventJSON).
		MinTimes(1)
	msg.EXPECT().Metadata().
		Return(&jetstream.MsgMetadata{NumDelivered: 1}, nil).
		MinTimes(1)

	mocks.json.
		EXPECT().
		Unmarshal(eventJSON, gomock.Any()).
		DoAndReturn(func(data []byte, v interface{}) error {
			*v.(*domain.BlockchainEvent) = *event
			return nil
		})

	mocks.blacklist.
		EXPECT().
		IsTokenCIDBlacklisted(event.TokenCID()).
		Return(false)

	// Store returns error
	mocks.store.
		EXPECT().
		GetTokenByTokenCID(gomock.Any(), event.TokenCID().String()).
		Return(nil, assert.AnError)

	// Expect message to be NAKed due to store error
	msg.
		EXPECT().
		Nak().
		Return(nil)

	var messageHandler adapter.MessageHandler
	consumer := mockspkg.NewMockConsumer(mocks.ctrl)
	consumeContext := mockspkg.NewMockConsumeContext(mocks.ctrl)

	consumer.
		EXPECT().
		Info(gomock.Any()).Return(&jetstream.ConsumerInfo{Name: "bridge-consumer"}, nil)
	consumer.
		EXPECT().
		Consume(gomock.Any(), gomock.Any()).
		DoAndReturn(func(handler adapter.MessageHandler, opts ...jetstream.PullConsumeOpt) (adapter.ConsumeContext, error) {
			messageHandler = handler
			return consumeContext, nil
		})
	consumeContext.
		EXPECT().
		Stop().
		AnyTimes()

	mocks.jetStream.
		EXPECT().
		CreateOrUpdateConsumer(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(consumer, nil)

	go func() { _ = b.Run(ctx) }()
	time.Sleep(100 * time.Millisecond)

	messageHandler(msg)
	time.Sleep(200 * time.Millisecond)

	cancel()
}

func TestBridge_ProcessMessage_StoreError_IsAddressWatched(t *testing.T) {
	mocks := setupTestBridge(t)
	defer tearDownTestBridge(mocks)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config := bridge.Config{
		URL:               "nats://localhost:4222",
		StreamName:        "events",
		ConsumerName:      "bridge-consumer",
		TemporalTaskQueue: "test-queue",
	}

	mocks.natsJS.
		EXPECT().
		Connect(config.URL, gomock.Any()).
		Return(mocks.natsConn, mocks.jetStream, nil)

	b, err := bridge.NewBridge(ctx, config, mocks.natsJS, mocks.store, mocks.orchestrator, mocks.json, mocks.blacklist)
	assert.NoError(t, err)

	msg := mockspkg.NewMockMessage(mocks.ctrl)
	toAddr := "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb"
	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeMint,
		FromAddress:     nil,
		ToAddress:       &toAddr,
		Quantity:        "1",
		TxHash:          "0xabc123",
		BlockNumber:     1234567,
		Timestamp:       time.Now(),
		TxIndex:         1,
	}

	eventJSON := []byte(`{"chain":"eip155:1","standard":"erc721","contract_address":"0x1234567890123456789012345678901234567890","token_number":"1","event_type":"mint"}`)

	msg.
		EXPECT().
		Data().
		Return(eventJSON).
		MinTimes(1)
	msg.EXPECT().Metadata().
		Return(&jetstream.MsgMetadata{NumDelivered: 1}, nil).
		MinTimes(1)

	mocks.json.
		EXPECT().
		Unmarshal(eventJSON, gomock.Any()).
		DoAndReturn(func(data []byte, v interface{}) error {
			*v.(*domain.BlockchainEvent) = *event
			return nil
		})

	mocks.blacklist.
		EXPECT().
		IsTokenCIDBlacklisted(event.TokenCID()).
		Return(false)
	mocks.store.
		EXPECT().
		GetTokenByTokenCID(gomock.Any(), event.TokenCID().String()).
		Return(nil, nil)

	// IsAnyAddressWatched returns error
	mocks.store.
		EXPECT().
		IsAnyAddressWatched(gomock.Any(), event.Chain, gomock.Any()).
		Return(false, assert.AnError)

	// Expect message to be NAKed due to store error
	msg.
		EXPECT().
		Nak().
		Return(nil)

	var messageHandler adapter.MessageHandler
	consumer := mockspkg.NewMockConsumer(mocks.ctrl)
	consumeContext := mockspkg.NewMockConsumeContext(mocks.ctrl)

	consumer.
		EXPECT().
		Info(gomock.Any()).
		Return(&jetstream.ConsumerInfo{Name: "bridge-consumer"}, nil)
	consumer.
		EXPECT().
		Consume(gomock.Any(), gomock.Any()).
		DoAndReturn(func(handler adapter.MessageHandler, opts ...jetstream.PullConsumeOpt) (adapter.ConsumeContext, error) {
			messageHandler = handler
			return consumeContext, nil
		})
	consumeContext.
		EXPECT().
		Stop().
		AnyTimes()

	mocks.jetStream.
		EXPECT().
		CreateOrUpdateConsumer(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(consumer, nil)

	go func() { _ = b.Run(ctx) }()
	time.Sleep(100 * time.Millisecond)

	messageHandler(msg)
	time.Sleep(200 * time.Millisecond)

	cancel()
}

func TestBridge_ProcessMessage_WorkflowError(t *testing.T) {
	mocks := setupTestBridge(t)
	defer tearDownTestBridge(mocks)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config := bridge.Config{
		URL:               "nats://localhost:4222",
		StreamName:        "events",
		ConsumerName:      "bridge-consumer",
		TemporalTaskQueue: "test-queue",
	}

	mocks.natsJS.
		EXPECT().
		Connect(config.URL, gomock.Any()).
		Return(mocks.natsConn, mocks.jetStream, nil)

	b, err := bridge.NewBridge(ctx, config, mocks.natsJS, mocks.store, mocks.orchestrator, mocks.json, mocks.blacklist)
	assert.NoError(t, err)

	msg := mockspkg.NewMockMessage(mocks.ctrl)
	toAddr := "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb"
	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeMint,
		FromAddress:     nil,
		ToAddress:       &toAddr,
		Quantity:        "1",
		TxHash:          "0xabc123",
		BlockNumber:     1234567,
		Timestamp:       time.Now(),
		TxIndex:         1,
	}

	eventJSON := []byte(`{"chain":"eip155:1","standard":"erc721","contract_address":"0x1234567890123456789012345678901234567890","token_number":"1","event_type":"mint"}`)

	msg.
		EXPECT().Data().
		Return(eventJSON).
		MinTimes(1)
	msg.EXPECT().Metadata().
		Return(&jetstream.MsgMetadata{NumDelivered: 1}, nil).
		MinTimes(1)

	mocks.json.
		EXPECT().
		Unmarshal(eventJSON, gomock.Any()).
		DoAndReturn(func(data []byte, v interface{}) error {
			*v.(*domain.BlockchainEvent) = *event
			return nil
		})

	mocks.blacklist.
		EXPECT().
		IsTokenCIDBlacklisted(event.TokenCID()).
		Return(false)
	mocks.store.
		EXPECT().
		GetTokenByTokenCID(gomock.Any(), event.TokenCID().String()).
		Return(&schema.Token{ID: 1}, nil)

	// Workflow execution fails
	mocks.orchestrator.
		EXPECT().
		ExecuteWorkflow(gomock.Any(), gomock.Any(), gomock.Any(), event).
		Return(nil, assert.AnError)

	// Expect message to be NAKed due to workflow error
	msg.
		EXPECT().
		Nak().
		Return(nil)

	var messageHandler adapter.MessageHandler
	consumer := mockspkg.NewMockConsumer(mocks.ctrl)
	consumeContext := mockspkg.NewMockConsumeContext(mocks.ctrl)

	consumer.
		EXPECT().
		Info(gomock.Any()).
		Return(&jetstream.ConsumerInfo{Name: "bridge-consumer"}, nil)
	consumer.
		EXPECT().
		Consume(gomock.Any(), gomock.Any()).
		DoAndReturn(func(handler adapter.MessageHandler, opts ...jetstream.PullConsumeOpt) (adapter.ConsumeContext, error) {
			messageHandler = handler
			return consumeContext, nil
		})
	consumeContext.
		EXPECT().
		Stop().
		AnyTimes()

	mocks.jetStream.
		EXPECT().
		CreateOrUpdateConsumer(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(consumer, nil)

	go func() { _ = b.Run(ctx) }()
	time.Sleep(100 * time.Millisecond)

	messageHandler(msg)
	time.Sleep(200 * time.Millisecond)

	cancel()
}

func TestBridge_ProcessMessage_Success_MetadataUpdateEvent(t *testing.T) {
	mocks := setupTestBridge(t)
	defer tearDownTestBridge(mocks)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config := bridge.Config{
		URL:               "nats://localhost:4222",
		StreamName:        "events",
		ConsumerName:      "bridge-consumer",
		TemporalTaskQueue: "test-queue",
	}

	mocks.natsJS.
		EXPECT().
		Connect(config.URL, gomock.Any()).
		Return(mocks.natsConn, mocks.jetStream, nil)

	b, err := bridge.NewBridge(ctx, config, mocks.natsJS, mocks.store, mocks.orchestrator, mocks.json, mocks.blacklist)
	assert.NoError(t, err)

	msg := mockspkg.NewMockMessage(mocks.ctrl)
	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeMetadataUpdate,
		FromAddress:     nil,
		ToAddress:       nil,
		Quantity:        "1",
		TxHash:          "0xabc123",
		BlockNumber:     1234567,
		Timestamp:       time.Now(),
		TxIndex:         1,
	}

	eventJSON := []byte(`{"chain":"eip155:1","standard":"erc721","contract_address":"0x1234567890123456789012345678901234567890","token_number":"1","event_type":"metadata_update"}`)

	msg.
		EXPECT().Data().
		Return(eventJSON).
		MinTimes(1)
	msg.
		EXPECT().
		Metadata().
		Return(&jetstream.MsgMetadata{NumDelivered: 1}, nil).
		MinTimes(1)

	mocks.json.
		EXPECT().
		Unmarshal(eventJSON, gomock.Any()).
		DoAndReturn(func(data []byte, v interface{}) error {
			*v.(*domain.BlockchainEvent) = *event
			return nil
		})

	mocks.blacklist.
		EXPECT().
		IsTokenCIDBlacklisted(event.TokenCID()).
		Return(false)
	mocks.store.
		EXPECT().
		GetTokenByTokenCID(gomock.Any(), event.TokenCID().String()).
		Return(&schema.Token{ID: 1}, nil)
	mocks.orchestrator.
		EXPECT().
		ExecuteWorkflow(gomock.Any(), gomock.Any(), gomock.Any(), event).
		Return(nil, nil)
	msg.
		EXPECT().
		Ack().
		Return(nil)

	var messageHandler adapter.MessageHandler
	consumer := mockspkg.NewMockConsumer(mocks.ctrl)
	consumeContext := mockspkg.NewMockConsumeContext(mocks.ctrl)

	consumer.
		EXPECT().
		Info(gomock.Any()).
		Return(&jetstream.ConsumerInfo{Name: "bridge-consumer"}, nil)
	consumer.
		EXPECT().
		Consume(gomock.Any(), gomock.Any()).
		DoAndReturn(func(handler adapter.MessageHandler, opts ...jetstream.PullConsumeOpt) (adapter.ConsumeContext, error) {
			messageHandler = handler
			return consumeContext, nil
		})
	consumeContext.
		EXPECT().
		Stop().
		AnyTimes()

	mocks.jetStream.
		EXPECT().
		CreateOrUpdateConsumer(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(consumer, nil)

	go func() { _ = b.Run(ctx) }()
	time.Sleep(100 * time.Millisecond)

	messageHandler(msg)
	time.Sleep(200 * time.Millisecond)

	cancel()
}

func TestBridge_ProcessMessage_AckError(t *testing.T) {
	mocks := setupTestBridge(t)
	defer tearDownTestBridge(mocks)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config := bridge.Config{
		URL:               "nats://localhost:4222",
		StreamName:        "events",
		ConsumerName:      "bridge-consumer",
		TemporalTaskQueue: "test-queue",
	}

	mocks.natsJS.
		EXPECT().
		Connect(config.URL, gomock.Any()).
		Return(mocks.natsConn, mocks.jetStream, nil)

	b, err := bridge.NewBridge(ctx, config, mocks.natsJS, mocks.store, mocks.orchestrator, mocks.json, mocks.blacklist)
	assert.NoError(t, err)

	msg := mockspkg.NewMockMessage(mocks.ctrl)
	toAddr := "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb"
	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeMint,
		FromAddress:     nil,
		ToAddress:       &toAddr,
		Quantity:        "1",
		TxHash:          "0xabc123",
		BlockNumber:     1234567,
		Timestamp:       time.Now(),
		TxIndex:         1,
	}

	eventJSON := []byte(`{"chain":"eip155:1","standard":"erc721","contract_address":"0x1234567890123456789012345678901234567890","token_number":"1","event_type":"mint"}`)

	msg.
		EXPECT().Data().
		Return(eventJSON).
		MinTimes(1)
	msg.EXPECT().Metadata().
		Return(&jetstream.MsgMetadata{NumDelivered: 1}, nil).
		MinTimes(1)

	mocks.json.
		EXPECT().
		Unmarshal(eventJSON, gomock.Any()).
		DoAndReturn(func(data []byte, v interface{}) error {
			*v.(*domain.BlockchainEvent) = *event
			return nil
		})

	mocks.blacklist.
		EXPECT().
		IsTokenCIDBlacklisted(event.TokenCID()).
		Return(false)
	mocks.store.
		EXPECT().
		GetTokenByTokenCID(gomock.Any(), event.TokenCID().String()).
		Return(&schema.Token{ID: 1}, nil)
	mocks.orchestrator.
		EXPECT().
		ExecuteWorkflow(gomock.Any(), gomock.Any(), gomock.Any(), event).
		Return(nil, nil)

	// Ack returns error (should be logged but not cause the handler to fail)
	msg.
		EXPECT().
		Ack().
		Return(assert.AnError)

	var messageHandler adapter.MessageHandler
	consumer := mockspkg.NewMockConsumer(mocks.ctrl)
	consumeContext := mockspkg.NewMockConsumeContext(mocks.ctrl)

	consumer.
		EXPECT().
		Info(gomock.Any()).
		Return(&jetstream.ConsumerInfo{Name: "bridge-consumer"}, nil)
	consumer.
		EXPECT().
		Consume(gomock.Any(), gomock.Any()).
		DoAndReturn(func(handler adapter.MessageHandler, opts ...jetstream.PullConsumeOpt) (adapter.ConsumeContext, error) {
			messageHandler = handler
			return consumeContext, nil
		})
	consumeContext.
		EXPECT().
		Stop().
		AnyTimes()

	mocks.jetStream.
		EXPECT().
		CreateOrUpdateConsumer(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(consumer, nil)

	go func() { _ = b.Run(ctx) }()
	time.Sleep(100 * time.Millisecond)

	messageHandler(msg)
	time.Sleep(200 * time.Millisecond)

	cancel()
}

func TestBridge_ProcessMessage_NakError(t *testing.T) {
	mocks := setupTestBridge(t)
	defer tearDownTestBridge(mocks)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config := bridge.Config{
		URL:               "nats://localhost:4222",
		StreamName:        "events",
		ConsumerName:      "bridge-consumer",
		TemporalTaskQueue: "test-queue",
	}

	mocks.natsJS.
		EXPECT().
		Connect(config.URL, gomock.Any()).
		Return(mocks.natsConn, mocks.jetStream, nil)

	b, err := bridge.NewBridge(ctx, config, mocks.natsJS, mocks.store, mocks.orchestrator, mocks.json, mocks.blacklist)
	assert.NoError(t, err)

	msg := mockspkg.NewMockMessage(mocks.ctrl)
	toAddr := "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb"
	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeMint,
		FromAddress:     nil,
		ToAddress:       &toAddr,
		Quantity:        "1",
		TxHash:          "0xabc123",
		BlockNumber:     1234567,
		Timestamp:       time.Now(),
		TxIndex:         1,
	}

	eventJSON := []byte(`{"chain":"eip155:1","standard":"erc721","contract_address":"0x1234567890123456789012345678901234567890","token_number":"1","event_type":"mint"}`)

	msg.
		EXPECT().
		Data().
		Return(eventJSON).
		MinTimes(1)
	msg.EXPECT().Metadata().
		Return(&jetstream.MsgMetadata{NumDelivered: 1}, nil).
		MinTimes(1)

	mocks.json.
		EXPECT().
		Unmarshal(eventJSON, gomock.Any()).
		DoAndReturn(func(data []byte, v interface{}) error {
			*v.(*domain.BlockchainEvent) = *event
			return nil
		})

	mocks.blacklist.
		EXPECT().
		IsTokenCIDBlacklisted(event.TokenCID()).
		Return(false)
	mocks.store.
		EXPECT().
		GetTokenByTokenCID(gomock.Any(), event.TokenCID().String()).
		Return(nil, assert.AnError)

	// Nak returns error (should be logged but not cause the handler to fail)
	msg.
		EXPECT().
		Nak().
		Return(assert.AnError)

	var messageHandler adapter.MessageHandler
	consumer := mockspkg.NewMockConsumer(mocks.ctrl)
	consumeContext := mockspkg.NewMockConsumeContext(mocks.ctrl)

	consumer.
		EXPECT().
		Info(gomock.Any()).
		Return(&jetstream.ConsumerInfo{Name: "bridge-consumer"}, nil)
	consumer.
		EXPECT().
		Consume(gomock.Any(), gomock.Any()).
		DoAndReturn(func(handler adapter.MessageHandler, opts ...jetstream.PullConsumeOpt) (adapter.ConsumeContext, error) {
			messageHandler = handler
			return consumeContext, nil
		})
	consumeContext.
		EXPECT().
		Stop().
		AnyTimes()

	mocks.jetStream.
		EXPECT().
		CreateOrUpdateConsumer(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(consumer, nil)

	go func() { _ = b.Run(ctx) }()
	time.Sleep(100 * time.Millisecond)

	messageHandler(msg)
	time.Sleep(200 * time.Millisecond)

	cancel()
}

func TestBridge_ProcessMessage_TermError(t *testing.T) {
	mocks := setupTestBridge(t)
	defer tearDownTestBridge(mocks)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config := bridge.Config{
		URL:               "nats://localhost:4222",
		StreamName:        "events",
		ConsumerName:      "bridge-consumer",
		TemporalTaskQueue: "test-queue",
	}

	mocks.natsJS.
		EXPECT().
		Connect(config.URL, gomock.Any()).
		Return(mocks.natsConn, mocks.jetStream, nil)

	b, err := bridge.NewBridge(ctx, config, mocks.natsJS, mocks.store, mocks.orchestrator, mocks.json, mocks.blacklist)
	assert.NoError(t, err)

	msg := mockspkg.NewMockMessage(mocks.ctrl)
	invalidJSON := []byte(`{invalid json}`)

	msg.
		EXPECT().
		Data().
		Return(invalidJSON).
		MinTimes(1)
	msg.EXPECT().Metadata().
		Return(&jetstream.MsgMetadata{NumDelivered: 1}, nil).
		MinTimes(1)

	// Mock JSON unmarshal to return error
	mocks.json.
		EXPECT().
		Unmarshal(invalidJSON, gomock.Any()).
		Return(assert.AnError)

	// Term returns error (should be logged but not cause the handler to fail)
	msg.
		EXPECT().
		Term().
		Return(assert.AnError)

	var messageHandler adapter.MessageHandler
	consumer := mockspkg.NewMockConsumer(mocks.ctrl)
	consumeContext := mockspkg.NewMockConsumeContext(mocks.ctrl)

	consumer.
		EXPECT().
		Info(gomock.Any()).
		Return(&jetstream.ConsumerInfo{Name: "bridge-consumer"}, nil)
	consumer.
		EXPECT().
		Consume(gomock.Any(), gomock.Any()).
		DoAndReturn(func(handler adapter.MessageHandler, opts ...jetstream.PullConsumeOpt) (adapter.ConsumeContext, error) {
			messageHandler = handler
			return consumeContext, nil
		})
	consumeContext.
		EXPECT().
		Stop().
		AnyTimes()

	mocks.jetStream.
		EXPECT().
		CreateOrUpdateConsumer(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(consumer, nil)

	go func() { _ = b.Run(ctx) }()
	time.Sleep(100 * time.Millisecond)

	messageHandler(msg)
	time.Sleep(200 * time.Millisecond)

	cancel()
}

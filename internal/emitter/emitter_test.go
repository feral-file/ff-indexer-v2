package emitter_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/emitter"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/messaging"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
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

// testEmitterMocks contains all the mocks needed for testing the emitter
type testEmitterMocks struct {
	ctrl       *gomock.Controller
	subscriber *mocks.MockSubscriber
	publisher  *mocks.MockPublisher
	store      *mocks.MockStore
	clock      *mocks.MockClock
	emitter    emitter.Emitter
}

// setupTestEmitter creates all the mocks and emitter for testing
func setupTestEmitter(t *testing.T) *testEmitterMocks {
	ctrl := gomock.NewController(t)

	tm := &testEmitterMocks{
		ctrl:       ctrl,
		subscriber: mocks.NewMockSubscriber(ctrl),
		publisher:  mocks.NewMockPublisher(ctrl),
		store:      mocks.NewMockStore(ctrl),
		clock:      mocks.NewMockClock(ctrl),
	}

	tm.emitter = emitter.NewEmitter(
		tm.subscriber,
		tm.publisher,
		tm.store,
		emitter.Config{
			ChainID:         domain.ChainEthereumMainnet,
			StartBlock:      0,
			CursorSaveFreq:  10,
			CursorSaveDelay: 5 * time.Second,
		},
		tm.clock,
	)

	return tm
}

// tearDownTestEmitter cleans up the test mocks
func tearDownTestEmitter(mocks *testEmitterMocks) {
	mocks.ctrl.Finish()
}

func TestEmitter_Run_WithStartBlock(t *testing.T) {
	mocks := setupTestEmitter(t)
	defer tearDownTestEmitter(mocks)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create emitter with configured start block
	emitterInstance := emitter.NewEmitter(
		mocks.subscriber,
		mocks.publisher,
		mocks.store,
		emitter.Config{
			ChainID:         domain.ChainEthereumMainnet,
			StartBlock:      1000,
			CursorSaveFreq:  10,
			CursorSaveDelay: 5 * time.Second,
		},
		mocks.clock,
	)

	// Mock clock for Now() and Since() calls
	now := time.Now()
	mocks.clock.EXPECT().Now().Return(now).MinTimes(1)
	mocks.clock.EXPECT().Since(gomock.Any()).Return(time.Duration(0)).AnyTimes()

	// Mock subscriber to call handler with an event
	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x123",
		TokenNumber:     "1",
		EventType:       domain.EventTypeTransfer,
		FromAddress:     stringPtr("0xfrom"),
		ToAddress:       stringPtr("0xto"),
		Quantity:        "1",
		TxHash:          "0xtx",
		BlockNumber:     1001,
		Timestamp:       time.Now(),
	}
	mocks.subscriber.
		EXPECT().
		SubscribeEvents(gomock.Any(), uint64(1000), gomock.Any()).
		DoAndReturn(func(ctx context.Context, fromBlock uint64, handler interface{}) error {
			handlerFunc := handler.(messaging.EventHandler)
			_ = handlerFunc(event)

			// Cancel context to stop the emitter
			cancel()
			return nil
		})

	// Mock publisher to publish event
	mocks.publisher.
		EXPECT().
		PublishEvent(gomock.Any(), event).
		Return(nil)

	// Mock store to save cursor (when block number reaches save frequency)
	// Since lastSavedBlock starts at 0 and event is at 1001, and CursorSaveFreq is 10,
	// the condition 1001 - 0 >= 10 is true, so it saves at block 1001
	mocks.store.
		EXPECT().
		SetBlockCursor(gomock.Any(), string(domain.ChainEthereumMainnet), uint64(1001)).
		Return(nil).
		AnyTimes()

	err := emitterInstance.Run(ctx)

	// Should return context canceled error
	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

func TestEmitter_Run_WithLastBlockCursor(t *testing.T) {
	mocks := setupTestEmitter(t)
	defer tearDownTestEmitter(mocks)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create emitter with no start block configured
	emitterInstance := emitter.NewEmitter(
		mocks.subscriber,
		mocks.publisher,
		mocks.store,
		emitter.Config{
			ChainID:         domain.ChainEthereumMainnet,
			StartBlock:      0, // No start block
			CursorSaveFreq:  10,
			CursorSaveDelay: 5 * time.Second,
		},
		mocks.clock,
	)

	// Mock clock for Now() and Since() calls
	now := time.Now()
	mocks.clock.EXPECT().Now().Return(now).AnyTimes()
	mocks.clock.EXPECT().Since(gomock.Any()).Return(time.Duration(0)).AnyTimes()

	// Mock store to return last block cursor
	mocks.store.
		EXPECT().
		GetBlockCursor(gomock.Any(), string(domain.ChainEthereumMainnet)).
		Return(uint64(500), nil)

	// Mock subscriber to call handler with an event
	mocks.subscriber.
		EXPECT().
		SubscribeEvents(gomock.Any(), uint64(501), gomock.Any()).
		DoAndReturn(func(ctx context.Context, fromBlock uint64, handler interface{}) error {
			// Cancel context to stop the emitter
			cancel()
			return nil
		})

	err := emitterInstance.Run(ctx)

	// Should return context canceled error
	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

func TestEmitter_Run_WithNoLastBlockCursor(t *testing.T) {
	mocks := setupTestEmitter(t)
	defer tearDownTestEmitter(mocks)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create emitter with no start block configured
	emitterInstance := emitter.NewEmitter(
		mocks.subscriber,
		mocks.publisher,
		mocks.store,
		emitter.Config{
			ChainID:         domain.ChainEthereumMainnet,
			StartBlock:      0, // No start block
			CursorSaveFreq:  10,
			CursorSaveDelay: 5 * time.Second,
		},
		mocks.clock,
	)

	// Mock store to return no last block cursor
	mocks.store.
		EXPECT().
		GetBlockCursor(gomock.Any(), string(domain.ChainEthereumMainnet)).
		Return(uint64(0), nil)

	// Mock clock for Now() and Since() calls
	now := time.Now()
	mocks.clock.EXPECT().Now().Return(now).AnyTimes()
	mocks.clock.EXPECT().Since(gomock.Any()).Return(time.Duration(0)).AnyTimes()

	// Mock subscriber to get latest block
	mocks.subscriber.
		EXPECT().
		GetLatestBlock(gomock.Any()).
		Return(uint64(1000), nil)

	// Mock subscriber to call handler with an event
	mocks.subscriber.
		EXPECT().
		SubscribeEvents(gomock.Any(), uint64(1000), gomock.Any()).
		DoAndReturn(func(ctx context.Context, fromBlock uint64, handler interface{}) error {
			// Cancel context to stop the emitter
			cancel()
			return nil
		})

	err := emitterInstance.Run(ctx)

	// Should return context canceled error
	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

func TestEmitter_Run_CursorSaveByBlockFrequency(t *testing.T) {
	mocks := setupTestEmitter(t)
	defer tearDownTestEmitter(mocks)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create emitter with cursor save frequency
	emitterInstance := emitter.NewEmitter(
		mocks.subscriber,
		mocks.publisher,
		mocks.store,
		emitter.Config{
			ChainID:         domain.ChainEthereumMainnet,
			StartBlock:      1000,
			CursorSaveFreq:  5, // Save every 5 blocks
			CursorSaveDelay: 5 * time.Second,
		},
		mocks.clock,
	)

	now := time.Now()
	mocks.clock.EXPECT().Now().Return(now).AnyTimes()
	mocks.clock.EXPECT().Since(gomock.Any()).Return(time.Duration(0)).AnyTimes()

	// Mock subscriber to call handler with multiple events
	mocks.subscriber.
		EXPECT().
		SubscribeEvents(gomock.Any(), uint64(1000), gomock.Any()).
		DoAndReturn(func(ctx context.Context, fromBlock uint64, handler interface{}) error {
			handlerFunc := handler.(messaging.EventHandler)

			// Send events at block 1000, 1005, 1010 (should trigger saves)
			events := []uint64{1000, 1005, 1010}
			for _, blockNum := range events {
				event := &domain.BlockchainEvent{
					Chain:           domain.ChainEthereumMainnet,
					Standard:        domain.StandardERC721,
					ContractAddress: "0x123",
					TokenNumber:     "1",
					EventType:       domain.EventTypeTransfer,
					FromAddress:     stringPtr("0xfrom"),
					ToAddress:       stringPtr("0xto"),
					Quantity:        "1",
					TxHash:          "0xtx",
					BlockNumber:     blockNum,
					Timestamp:       time.Now(),
				}

				mocks.publisher.
					EXPECT().
					PublishEvent(gomock.Any(), event).
					Return(nil)

				// Expect cursor save for blocks 1000, 1005, and 1010 (every 5 blocks)
				// Block 1000: 1000 - 0 >= 5, saves at 1000
				// Block 1005: 1005 - 1000 >= 5, saves at 1005
				// Block 1010: 1010 - 1005 >= 5, saves at 1010
				mocks.store.
					EXPECT().
					SetBlockCursor(gomock.Any(), string(domain.ChainEthereumMainnet), blockNum).
					Return(nil)

				if err := handlerFunc(event); err != nil {
					return err
				}
			}

			// Cancel context to stop the emitter
			cancel()
			return nil
		})

	err := emitterInstance.Run(ctx)

	// Should return context canceled error
	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

func TestEmitter_Run_GetBlockCursorError(t *testing.T) {
	mocks := setupTestEmitter(t)
	defer tearDownTestEmitter(mocks)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create emitter with no start block configured
	emitterInstance := emitter.NewEmitter(
		mocks.subscriber,
		mocks.publisher,
		mocks.store,
		emitter.Config{
			ChainID:         domain.ChainEthereumMainnet,
			StartBlock:      0, // No start block
			CursorSaveFreq:  10,
			CursorSaveDelay: 5 * time.Second,
		},
		mocks.clock,
	)

	// Mock store to return error
	mocks.store.
		EXPECT().
		GetBlockCursor(gomock.Any(), string(domain.ChainEthereumMainnet)).
		Return(uint64(0), assert.AnError)

	err := emitterInstance.Run(ctx)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get block cursor")
}

func TestEmitter_Run_GetLatestBlockError(t *testing.T) {
	mocks := setupTestEmitter(t)
	defer tearDownTestEmitter(mocks)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create emitter with no start block configured
	emitterInstance := emitter.NewEmitter(
		mocks.subscriber,
		mocks.publisher,
		mocks.store,
		emitter.Config{
			ChainID:         domain.ChainEthereumMainnet,
			StartBlock:      0, // No start block
			CursorSaveFreq:  10,
			CursorSaveDelay: 5 * time.Second,
		},
		mocks.clock,
	)

	// Mock store to return no last block cursor
	mocks.store.
		EXPECT().
		GetBlockCursor(gomock.Any(), string(domain.ChainEthereumMainnet)).
		Return(uint64(0), nil)

	// Mock subscriber to return error
	mocks.subscriber.
		EXPECT().
		GetLatestBlock(gomock.Any()).
		Return(uint64(0), assert.AnError)

	err := emitterInstance.Run(ctx)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get latest block number")
}

func TestEmitter_Run_SubscribeEventsError(t *testing.T) {
	mocks := setupTestEmitter(t)
	defer tearDownTestEmitter(mocks)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create emitter with configured start block
	emitterInstance := emitter.NewEmitter(
		mocks.subscriber,
		mocks.publisher,
		mocks.store,
		emitter.Config{
			ChainID:         domain.ChainEthereumMainnet,
			StartBlock:      1000,
			CursorSaveFreq:  10,
			CursorSaveDelay: 5 * time.Second,
		},
		mocks.clock,
	)

	// Mock clock for Now() and Since() calls
	now := time.Now()
	mocks.clock.EXPECT().Now().Return(now).AnyTimes()
	mocks.clock.EXPECT().Since(gomock.Any()).Return(time.Duration(0)).AnyTimes()

	// Mock subscriber to return error
	mocks.subscriber.
		EXPECT().
		SubscribeEvents(gomock.Any(), uint64(1000), gomock.Any()).
		Return(assert.AnError)

	err := emitterInstance.Run(ctx)

	assert.Error(t, err)
}

func TestEmitter_Run_PublishEventError(t *testing.T) {
	mocks := setupTestEmitter(t)
	defer tearDownTestEmitter(mocks)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create emitter with configured start block
	emitterInstance := emitter.NewEmitter(
		mocks.subscriber,
		mocks.publisher,
		mocks.store,
		emitter.Config{
			ChainID:         domain.ChainEthereumMainnet,
			StartBlock:      1000,
			CursorSaveFreq:  10,
			CursorSaveDelay: 5 * time.Second,
		},
		mocks.clock,
	)

	// Mock clock for Now() and Since() calls
	now := time.Now()
	mocks.clock.EXPECT().Now().Return(now).AnyTimes()
	mocks.clock.EXPECT().Since(gomock.Any()).Return(time.Duration(0)).AnyTimes()

	// Mock subscriber to call handler with an event
	mocks.subscriber.
		EXPECT().
		SubscribeEvents(gomock.Any(), uint64(1000), gomock.Any()).
		DoAndReturn(func(ctx context.Context, fromBlock uint64, handler interface{}) error {
			// Simulate event
			event := &domain.BlockchainEvent{
				Chain:           domain.ChainEthereumMainnet,
				Standard:        domain.StandardERC721,
				ContractAddress: "0x123",
				TokenNumber:     "1",
				EventType:       domain.EventTypeTransfer,
				FromAddress:     stringPtr("0xfrom"),
				ToAddress:       stringPtr("0xto"),
				Quantity:        "1",
				TxHash:          "0xtx",
				BlockNumber:     1001,
				Timestamp:       time.Now(),
			}

			handlerFunc := handler.(messaging.EventHandler)
			err := handlerFunc(event)
			if err != nil {
				return err
			}

			// Cancel context to stop the emitter
			cancel()
			return nil
		})

	// Mock publisher to return error
	mocks.publisher.
		EXPECT().
		PublishEvent(gomock.Any(), gomock.Any()).
		Return(assert.AnError)

	err := emitterInstance.Run(ctx)

	// Error should be returned from handler
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to publish event")
}

func TestEmitter_Close(t *testing.T) {
	mocks := setupTestEmitter(t)
	defer tearDownTestEmitter(mocks)

	// Mock subscriber close
	mocks.subscriber.
		EXPECT().
		Close()

	mocks.emitter.Close()
}

// Helper function to create string pointers
func stringPtr(s string) *string {
	return &s
}

package bridge

import (
	"context"
	"fmt"
	"time"

	logger "github.com/bitmark-inc/autonomy-logger"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/providers/temporal"
	"github.com/feral-file/ff-indexer-v2/internal/store"
	"github.com/feral-file/ff-indexer-v2/internal/workflows"
)

// Config holds the configuration for the event bridge
type Config struct {
	URL               string
	StreamName        string
	ConsumerName      string
	MaxReconnects     int
	ReconnectWait     time.Duration
	ConnectionName    string
	AckWaitTimeout    time.Duration
	MaxDeliver        int
	TemporalTaskQueue string
}

// Bridge defines the interface for the event bridge
type Bridge interface {
	// Run starts the event bridge
	Run(ctx context.Context) error
	// Close closes the bridge and cleans up resources
	Close()
}

type bridge struct {
	nc           adapter.NatsConn
	js           adapter.JetStream
	store        store.Store
	orchestrator temporal.TemporalOrchestrator
	json         adapter.JSON
	config       Config
}

// NewBridge creates a new event bridge
func NewBridge(
	cfg Config,
	natsJS adapter.NatsJetStream,
	st store.Store,
	orchestrator temporal.TemporalOrchestrator,
	jsonAdapter adapter.JSON,
) (Bridge, error) {
	opts := []nats.Option{
		nats.Name(cfg.ConnectionName),
		nats.MaxReconnects(cfg.MaxReconnects),
		nats.ReconnectWait(cfg.ReconnectWait),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			if err != nil {
				logger.Error(err, zap.String("message", "Disconnected from NATS"))
			}
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			logger.Info("Reconnected to NATS", zap.String("url", nc.ConnectedUrl()))
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			logger.Info("NATS connection closed")
		}),
	}

	nc, js, err := natsJS.Connect(cfg.URL, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS and create JetStream: %w", err)
	}

	b := &bridge{
		nc:           nc,
		js:           js,
		store:        st,
		orchestrator: orchestrator,
		json:         jsonAdapter,
		config:       cfg,
	}

	return b, nil
}

// shouldProcessEvent determines if an event should be forwarded to workers
// func (b *bridge) shouldProcessEvent(ctx context.Context, event *domain.BlockchainEvent) (bool, error) {
// 	// Check if token is already indexed
// 	token, err := b.store.GegtTokenByTokenCID(ctx, event.TokenCID())
// 	if err != nil {
// 		return false, fmt.Errorf("failed to check token existence: %w", err)
// 	}

// 	if token != nil {
// 		return true, nil
// 	}

// 	// Token not indexed, check if from/to addresses are watched
// 	var addresses []string
// 	if !types.StringNilOrEmpty(event.FromAddress) {
// 		addresses = append(addresses, *event.FromAddress)
// 	}
// 	if !types.StringNilOrEmpty(event.ToAddress) {
// 		addresses = append(addresses, *event.ToAddress)
// 	}

// 	if len(addresses) == 0 {
// 		return false, nil
// 	}

// 	return b.store.IsAnyAddressWatched(ctx, event.Chain, addresses)
// }

// Run starts the event bridge
func (b *bridge) Run(ctx context.Context) error {
	logger.Info("Starting event bridge", zap.String("stream", b.config.StreamName), zap.String("consumer", b.config.ConsumerName))

	// Subscribe to all event subjects
	subject := "events.*.>"

	// Create or get consumer
	consumerConfig := jetstream.ConsumerConfig{
		Durable:       b.config.ConsumerName,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       b.config.AckWaitTimeout,
		MaxDeliver:    b.config.MaxDeliver,
		FilterSubject: subject,
	}

	consumer, err := b.js.CreateOrUpdateConsumer(ctx, b.config.StreamName, consumerConfig)
	if err != nil {
		return fmt.Errorf("failed to create/update consumer: %w", err)
	}

	consumerInfo, err := consumer.Info(ctx)
	if err != nil {
		return fmt.Errorf("failed to get consumer info: %w", err)
	}
	logger.Info("Consumer created/retrieved", zap.String("consumer", consumerInfo.Name))

	// Create subscription
	msgChan := make(chan jetstream.Msg, 100)
	sub, err := consumer.Consume(func(msg jetstream.Msg) {
		msgChan <- msg
	})
	if err != nil {
		return fmt.Errorf("failed to create subscription: %w", err)
	}
	defer sub.Stop()

	logger.Info("Started consuming messages")

	// Process messages
	for {
		select {
		case <-ctx.Done():
			logger.Info("Shutting down event bridge")
			return ctx.Err()
		case msg := <-msgChan:
			// Spawn goroutine to handle message asynchronously
			go b.handleMessage(ctx, msg)
		}
	}
}

// handleMessage processes a single NATS message
func (b *bridge) handleMessage(ctx context.Context, msg jetstream.Msg) {
	// Get metadata for logging
	metadata, _ := msg.Metadata()

	// Parse event
	var event domain.BlockchainEvent
	if err := b.json.Unmarshal(msg.Data(), &event); err != nil {
		logger.Error(err, zap.String("message", "Failed to unmarshal event"))
		// Terminate message for unparseable data
		if err := msg.Term(); err != nil {
			logger.Error(err, zap.String("message", "Failed to terminate message"))
		}
		return
	}

	logger.Info("Received event",
		zap.String("chain", string(event.Chain)),
		zap.String("eventType", string(event.EventType)),
		zap.String("tokenCID", event.TokenCID().String()),
		zap.String("txHash", event.TxHash),
		zap.Uint64("deliveryCount", metadata.NumDelivered),
	)

	// // Check if event should be processed
	// shouldProcess, err := b.shouldProcessEvent(ctx, &event)
	// if err != nil {
	// 	logger.Error(err, zap.String("message", "Failed to check if event should be processed"))
	// 	// NAK to retry
	// 	if err := msg.Nak(); err != nil {
	// 		logger.Error(err, zap.String("message", "Failed to NAK message"))
	// 	}
	// 	return
	// }

	// if !shouldProcess {
	// 	logger.Info("Dropping event - token not indexed and addresses not watched",
	// 		zap.String("chain", string(event.Chain)),
	// 		zap.String("eventType", string(event.EventType)),
	// 		zap.String("tokenCID", event.TokenCID()),
	// 		zap.String("from", types.SafeString(event.FromAddress)),
	// 		zap.String("to", types.SafeString(event.ToAddress)),
	// 	)
	// 	// ACK to remove from queue
	// 	if err := msg.Ack(); err != nil {
	// 		logger.Error(err, zap.String("message", "Failed to ACK message"))
	// 	}
	// 	return
	// }

	// Forward to appropriate worker
	if err := b.forwardToWorker(ctx, &event); err != nil {
		logger.Error(err, zap.String("message", "Failed to forward event to worker"))
		// NAK to retry
		if err := msg.Nak(); err != nil {
			logger.Error(err, zap.String("message", "Failed to NAK message"))
		}
		return
	}

	// ACK message after successful processing
	if err := msg.Ack(); err != nil {
		logger.Error(err, zap.String("message", "Failed to ACK message"))
	}
}

// forwardToWorker forwards the event to appropriate worker based on event type
func (b *bridge) forwardToWorker(ctx context.Context, event *domain.BlockchainEvent) error {
	// Route to appropriate worker method based on event type
	w := workflows.NewWorkerCore(nil)
	var workflowFunc interface{}
	switch event.EventType {
	case domain.EventTypeMint:
		workflowFunc = w.IndexTokenMint
	case domain.EventTypeTransfer:
		workflowFunc = w.IndexTokenTransfer
	case domain.EventTypeBurn:
		workflowFunc = w.IndexTokenBurn
	case domain.EventTypeMetadataUpdate:
		workflowFunc = w.IndexMetadataUpdate
	case domain.EventTypeMetadataUpdateRange:
		// FIXME: Implement metadata update range workflow
		return nil
	default:
		return fmt.Errorf("unknown event type: %s", event.EventType)
	}

	opt := client.StartWorkflowOptions{
		ID:                    fmt.Sprintf("process-event-%s-%s", event.Chain, event.TxHash),
		TaskQueue:             b.config.TemporalTaskQueue,
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
		WorkflowRunTimeout:    30 * time.Minute,
	}
	_, err := b.orchestrator.ExecuteWorkflow(ctx, opt, workflowFunc, event)
	if err != nil {
		return fmt.Errorf("failed to execute workflow: %w", err)
	}

	logger.Info("Event forwarded to worker",
		zap.String("tokenCID", event.TokenCID().String()),
		zap.String("eventType", string(event.EventType)),
	)

	return nil
}

// Close closes the bridge and cleans up resources
func (b *bridge) Close() {
	if b.nc == nil {
		return
	}

	b.nc.Close()
}

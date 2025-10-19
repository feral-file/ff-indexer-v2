package jetstream

import (
	"context"
	"fmt"
	"time"

	logger "github.com/bitmark-inc/autonomy-logger"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/messaging"
)

// Config holds the configuration for NATS JetStream connection
type Config struct {
	URL            string
	StreamName     string
	MaxReconnects  int
	ReconnectWait  time.Duration
	ConnectionName string
}

type publisher struct {
	nc         adapter.NatsConn
	js         adapter.JetStream
	streamName string
	json       adapter.JSON
}

// NewPublisher creates a new NATS JetStream publisher
func NewPublisher(cfg Config, natsJS adapter.NatsJetStream, jsonAdapter adapter.JSON) (messaging.Publisher, error) {
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

	return &publisher{
		nc:         nc,
		js:         js,
		streamName: cfg.StreamName,
		json:       jsonAdapter,
	}, nil
}

// PublishEvent publishes a blockchain event to NATS JetStream
func (p *publisher) PublishEvent(ctx context.Context, event *domain.BlockchainEvent) error {
	logger.Debug("Publishing Nats event", zap.Any("event", event))

	data, err := p.json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	subject := p.buildSubject(event)

	_, err = p.js.Publish(ctx, subject, data)
	if err != nil {
		return fmt.Errorf("failed to publish event: %w", err)
	}

	return nil
}

// buildSubject constructs the NATS subject based on the event
func (p *publisher) buildSubject(event *domain.BlockchainEvent) string {
	// Format: events.{chain}.{event_type}
	// e.g., events.ethereum.transfer, events.tezos.mint
	chain := "ethereum"
	if event.Chain == "tezos:mainnet" {
		chain = "tezos"
	}

	return fmt.Sprintf("events.%s.%s", chain, event.EventType)
}

// Close closes the NATS connection
func (p *publisher) Close() {
	if p.nc == nil {
		return
	}

	p.nc.Close()
}

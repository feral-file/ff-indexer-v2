package jetstream

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	logger "github.com/bitmark-inc/autonomy-logger"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

// Publisher defines the interface for publishing events to NATS JetStream
type Publisher interface {
	// PublishEvent publishes a blockchain event to the appropriate subject
	PublishEvent(ctx context.Context, event *domain.BlockchainEvent) error
	// Close closes the connection
	Close() error
}

// Config holds the configuration for NATS JetStream connection
type Config struct {
	URL            string
	StreamName     string
	MaxReconnects  int
	ReconnectWait  time.Duration
	ConnectionName string
}

type publisher struct {
	nc         *nats.Conn
	js         jetstream.JetStream
	streamName string
}

// NewPublisher creates a new NATS JetStream publisher
func NewPublisher(cfg Config) (Publisher, error) {
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
			logger.Error(nc.LastError(), zap.String("message", "NATS connection closed"))
		}),
	}

	nc, err := nats.Connect(cfg.URL, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to create JetStream context: %w", err)
	}

	return &publisher{
		nc:         nc,
		js:         js,
		streamName: cfg.StreamName,
	}, nil
}

// PublishEvent publishes a blockchain event to NATS JetStream
func (p *publisher) PublishEvent(ctx context.Context, event *domain.BlockchainEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	subject := p.buildSubject(event)

	_, err = p.js.Publish(ctx, subject, data)
	if err != nil {
		return fmt.Errorf("%w: %v", domain.ErrPublishFailed, err)
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
func (p *publisher) Close() error {
	if p.nc != nil {
		p.nc.Close()
	}
	return nil
}

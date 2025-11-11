package jetstream

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
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
	closeOnce  sync.Once
	closeCh    chan struct{} // Signal unexpected closure
}

// NewPublisher creates a new NATS JetStream publisher
func NewPublisher(ctx context.Context, cfg Config, natsJS adapter.NatsJetStream, jsonAdapter adapter.JSON) (messaging.Publisher, error) {
	p := &publisher{
		closeCh:    make(chan struct{}),
		closeOnce:  sync.Once{},
		streamName: cfg.StreamName,
		json:       jsonAdapter,
	}
	opts := []nats.Option{
		nats.Name(cfg.ConnectionName),
		nats.MaxReconnects(cfg.MaxReconnects),
		nats.ReconnectWait(cfg.ReconnectWait),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			if err != nil {
				logger.ErrorCtx(ctx, errors.New("Disconnected from NATS"), zap.Error(err))
			}
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			logger.InfoCtx(ctx, "Reconnected to NATS", zap.String("url", nc.ConnectedUrl()))
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			if nc != nil && nc.LastError() != nil {
				logger.ErrorCtx(ctx, errors.New("NATS connection closed due to error"), zap.Error(nc.LastError()))
				p.closeOnce.Do(func() {
					close(p.closeCh)
				})
			} else {
				logger.InfoCtx(ctx, "NATS connection closed gracefully")
			}
		}),
	}

	nc, js, err := natsJS.Connect(cfg.URL, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS and create JetStream: %w", err)
	}

	p.nc = nc
	p.js = js

	return p, nil
}

// PublishEvent publishes a blockchain event to NATS JetStream
func (p *publisher) PublishEvent(ctx context.Context, event *domain.BlockchainEvent) error {
	logger.DebugCtx(ctx, "Publishing Nats event", zap.Any("event", event))

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

// CloseChan returns a channel that is closed when the publisher is closed
func (p *publisher) CloseChan() <-chan struct{} {
	return p.closeCh
}

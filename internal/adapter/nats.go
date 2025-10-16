package adapter

import (
	"context"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// NatsConn defines an interface for NATS connection operations to enable mocking
//
//go:generate mockgen -source=nats.go -destination=../mocks/nats.go -package=mocks -mock_names=NatsConn=MockNatsConn
type NatsConn interface {
	Close()
	LastError() error
	ConnectedUrl() string
}

// JetStream defines an interface for JetStream operations to enable mocking
//
//go:generate mockgen -source=nats.go -destination=../mocks/nats.go -package=mocks -mock_names=JetStream=MockJetStream
type JetStream interface {
	Publish(ctx context.Context, subject string, data []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error)
	CreateOrUpdateConsumer(ctx context.Context, stream string, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error)
	Consumer(ctx context.Context, stream string, consumer string) (jetstream.Consumer, error)
}

// NatsJetStream defines an interface for creating NATS connections and JetStream contexts
//
//go:generate mockgen -source=nats.go -destination=../mocks/nats.go -package=mocks -mock_names=NatsJetStream=MockNatsJetStream
type NatsJetStream interface {
	Connect(url string, options ...nats.Option) (NatsConn, JetStream, error)
}

// RealNatsJetStream implements NatsJetStream using the standard nats package
type RealNatsJetStream struct{}

// NewNatsJetStream creates a new real NATS JetStream
func NewNatsJetStream() NatsJetStream {
	return &RealNatsJetStream{}
}

func (n *RealNatsJetStream) Connect(url string, options ...nats.Option) (NatsConn, JetStream, error) {
	nc, err := nats.Connect(url, options...)
	if err != nil {
		return nil, nil, err
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, nil, err
	}

	return nc, js, nil
}

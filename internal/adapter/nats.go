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
	CreateOrUpdateConsumer(ctx context.Context, stream string, cfg jetstream.ConsumerConfig) (Consumer, error)
	Consumer(ctx context.Context, stream string, consumer string) (Consumer, error)
}

type MessageHandler func(msg Message)

// Consumer defines an interface for NATS JetStream consumers to enable mocking
//
//go:generate mockgen -source=nats.go -destination=../mocks/nats.go -package=mocks -mock_names=Consumer=MockNatsConsumer
type Consumer interface {
	Consume(handler MessageHandler, opts ...jetstream.PullConsumeOpt) (ConsumeContext, error)
	Info(ctx context.Context) (*jetstream.ConsumerInfo, error)
}

// ConsumeContext defines an interface for NATS JetStream consume contexts to enable mocking
//
//go:generate mockgen -source=nats.go -destination=../mocks/nats.go -package=mocks -mock_names=ConsumeContext=MockConsumeContext
type ConsumeContext interface {
	Stop()
	Drain()
	Closed() <-chan struct{}
}

// Message defines an interface for NATS JetStream messages to enable mocking
//
//go:generate mockgen -source=nats.go -destination=../mocks/nats.go -package=mocks -mock_names=Message=MockJetStreamMessage
type Message interface {
	Data() []byte
	Metadata() (*jetstream.MsgMetadata, error)
	Ack() error
	Nak() error
	Term() error
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

	// Wrap the real jetstream.JetStream to adapt it to our JetStream interface
	return nc, &jetStreamAdapter{js: js}, nil
}

// jetStreamAdapter adapts jetstream.JetStream to our JetStream interface
// This adapter is necessary because our interface returns Consumer (our interface)
// while jetstream.JetStream returns jetstream.Consumer (nats package interface)
type jetStreamAdapter struct {
	js jetstream.JetStream
}

func (a *jetStreamAdapter) Publish(ctx context.Context, subject string, data []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	return a.js.Publish(ctx, subject, data, opts...)
}

func (a *jetStreamAdapter) CreateOrUpdateConsumer(ctx context.Context, stream string, cfg jetstream.ConsumerConfig) (Consumer, error) {
	consumer, err := a.js.CreateOrUpdateConsumer(ctx, stream, cfg)
	if err != nil {
		return nil, err
	}
	// Wrap the jetstream.Consumer to adapt it to our Consumer interface
	return &consumerAdapter{consumer: consumer}, nil
}

func (a *jetStreamAdapter) Consumer(ctx context.Context, stream string, consumer string) (Consumer, error) {
	c, err := a.js.Consumer(ctx, stream, consumer)
	if err != nil {
		return nil, err
	}
	// Wrap the jetstream.Consumer to adapt it to our Consumer interface
	return &consumerAdapter{consumer: c}, nil
}

// consumerAdapter adapts jetstream.Consumer to our Consumer interface
// This adapter forwards all calls to the underlying jetstream.Consumer
type consumerAdapter struct {
	consumer jetstream.Consumer
}

func (a *consumerAdapter) Consume(handler MessageHandler, opts ...jetstream.PullConsumeOpt) (ConsumeContext, error) {
	return a.consumer.Consume(func(msg jetstream.Msg) {
		handler(msg)
	}, opts...)
}

func (a *consumerAdapter) Info(ctx context.Context) (*jetstream.ConsumerInfo, error) {
	return a.consumer.Info(ctx)
}

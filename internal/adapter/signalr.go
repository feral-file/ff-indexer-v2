package adapter

import (
	"context"

	"github.com/philippseith/signalr"
)

// SignalRClient defines an interface for SignalR client operations to enable mocking
//
//go:generate mockgen -source=signalr.go -destination=../mocks/signalr.go -package=mocks -mock_names=SignalRClient=MockSignalRClient
type SignalRClient interface {
	Start()
	Send(target string, args ...interface{}) <-chan error
	Stop()
}

// SignalR defines an interface for creating SignalR clients
//
//go:generate mockgen -source=signalr.go -destination=../mocks/signalr.go -package=mocks -mock_names=SignalR=MockSignalR
type SignalR interface {
	NewClient(ctx context.Context, address string, receiver interface{}) (SignalRClient, error)
}

// RealSignalR implements SignalR using the standard signalr package
type RealSignalR struct{}

// NewSignalR creates a new real SignalR
func NewSignalR() SignalR {
	return &RealSignalR{}
}

func (s *RealSignalR) NewClient(ctx context.Context, address string, receiver interface{}) (SignalRClient, error) {
	conn, err := signalr.NewHTTPConnection(ctx, address)
	if err != nil {
		return nil, err
	}

	return signalr.NewClient(ctx, signalr.WithConnection(conn), signalr.WithReceiver(receiver))
}

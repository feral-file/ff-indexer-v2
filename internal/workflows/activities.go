package workflows

import (
	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/providers/tezos"
	"github.com/feral-file/ff-indexer-v2/internal/store"
)

// Executor defines the interface for executing activities
//
//go:generate mockgen -source=activities.go -destination=../mocks/activities.go -package=mocks -mock_names=Executor=MockExecutor
type Executor interface{}

// executor is the concrete implementation of Executor
type executor struct {
	store     store.Store
	ethClient adapter.EthClient
	tzClient  tezos.TzKTClient
}

// NewExecutor creates a new executor instance
func NewExecutor(store store.Store, ethClient adapter.EthClient, tzClient tezos.TzKTClient) Executor {
	return &executor{
		store:     store,
		ethClient: ethClient,
		tzClient:  tzClient,
	}
}

// TODO: Implement the activities executor

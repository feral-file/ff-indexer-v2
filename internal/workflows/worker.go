package workflows

import (
	"go.temporal.io/sdk/workflow"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
)

// WorkerCore defines the interface for processing blockchain events
//
//go:generate mockgen -source=worker.go -destination=../mocks/worker_core.go -package=mocks -mock_names=WorkerCore=MockWorkerCore
type WorkerCore interface {
	// IndexTokenMint processes a token mint event
	IndexTokenMint(ctx workflow.Context, event *domain.BlockchainEvent) error

	// IndexTokenTransfer processes a token transfer event
	IndexTokenTransfer(ctx workflow.Context, event *domain.BlockchainEvent) error

	// IndexTokenBurn processes a token burn event
	IndexTokenBurn(ctx workflow.Context, event *domain.BlockchainEvent) error

	// IndexMetadataUpdate processes a metadata update event
	IndexMetadataUpdate(ctx workflow.Context, event *domain.BlockchainEvent) error

	// IndexTokenMetadata index token metadata
	IndexTokenMetadata(ctx workflow.Context, tokenCID domain.TokenCID) error

	// IndexTokenFromEvent indexes metadata and full provenances (provenance events and balances) for a token
	IndexTokenFromEvent(ctx workflow.Context, event *domain.BlockchainEvent) error

	// IndexTokens indexes multiple tokens in parallel
	IndexTokens(ctx workflow.Context, tokenCIDs []domain.TokenCID) error

	// IndexToken indexes a single token (metadata and provenances)
	IndexToken(ctx workflow.Context, tokenCID domain.TokenCID) error

	// IndexTokenOwners indexes tokens for multiple addresses
	IndexTokenOwners(ctx workflow.Context, addresses []string) error

	// IndexTokenOwner indexes all tokens held by a single address
	IndexTokenOwner(ctx workflow.Context, address string) error

	// IndexTezosTokenOwner indexes all tokens held by a Tezos address
	IndexTezosTokenOwner(ctx workflow.Context, address string) error

	// IndexEthereumTokenOwner indexes all tokens held by an Ethereum address
	IndexEthereumTokenOwner(ctx workflow.Context, address string) error

	// IndexTokenProvenances indexes all provenances (balances and events) for a token
	IndexTokenProvenances(ctx workflow.Context, tokenCID domain.TokenCID) error
}

// WorkerMedia defines the interface for processing media events
// TODO: Define the interface for media events
type WorkerMedia interface{}

type WorkerCoreConfig struct {
	// TezosChainID is the chain ID for the Tezos blockchain
	TezosChainID domain.Chain
	// EthereumChainID is the chain ID for the Ethereum blockchain
	EthereumChainID domain.Chain
	// EthereumTokenSweepStartBlock is the start block for the Ethereum token sweep
	EthereumTokenSweepStartBlock uint64
	// EthereumTokenSweepBlockChunkSize is the size of the chunk of blocks to sweep for owners
	EthereumTokenSweepBlockChunkSize uint64
	// TezosTokenSweepStartBlock is the start block for the Tezos token sweep
	TezosTokenSweepStartBlock uint64
	// TezosTokenSweepBlockChunkSize is the size of the chunk of blocks to sweep for owners
	TezosTokenSweepBlockChunkSize uint64
}

// workerCore is the concrete implementation of WorkerCore
type workerCore struct {
	config   WorkerCoreConfig
	executor Executor
}

// NewWorkerCore creates a new worker core instance
func NewWorkerCore(executor Executor, config WorkerCoreConfig) WorkerCore {
	return &workerCore{
		executor: executor,
		config:   config,
	}
}

// workerMedia is the concrete implementation of WorkerMedia
type workerMedia struct {
	executor Executor
}

// NewWorkerMedia creates a new worker media instance
func NewWorkerMedia(executor Executor) WorkerMedia {
	return &workerMedia{
		executor: executor,
	}
}

package workflows

import (
	"go.temporal.io/sdk/workflow"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/registry"
	"github.com/feral-file/ff-indexer-v2/internal/webhook"
)

// WorkerCore defines the interface for processing blockchain events
//
//go:generate mockgen -source=worker.go -destination=../mocks/worker_core.go -package=mocks -mock_names=WorkerCore=MockCoreWorker
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
	IndexTokenMetadata(ctx workflow.Context, tokenCID domain.TokenCID, address *string) error

	// IndexMultipleTokensMetadata indexes metadata for multiple tokens by triggering child workflows
	IndexMultipleTokensMetadata(ctx workflow.Context, tokenCIDs []domain.TokenCID) error

	// IndexTokenFromEvent indexes metadata and full provenances (provenance events and balances) for a token
	IndexTokenFromEvent(ctx workflow.Context, event *domain.BlockchainEvent) error

	// IndexTokens indexes multiple tokens in parallel
	IndexTokens(ctx workflow.Context, tokenCIDs []domain.TokenCID, address *string) error

	// IndexToken indexes a single token (metadata and provenances)
	IndexToken(ctx workflow.Context, tokenCID domain.TokenCID, address *string) error

	// IndexTokenOwners indexes tokens for multiple addresses
	IndexTokenOwners(ctx workflow.Context, addresses []string) error

	// IndexTokenOwner indexes all tokens held by a single address
	IndexTokenOwner(ctx workflow.Context, address string) error

	// IndexTezosTokenOwner indexes all tokens held by a Tezos address
	// jobID is optional and used for job status tracking during quota pauses
	IndexTezosTokenOwner(ctx workflow.Context, address string, jobID *string) error

	// IndexEthereumTokenOwner indexes all tokens held by an Ethereum address
	// jobID is optional and used for job status tracking during quota pauses
	IndexEthereumTokenOwner(ctx workflow.Context, address string, jobID *string) error

	// IndexTokenProvenances indexes all provenances (balances and events) for a token
	IndexTokenProvenances(ctx workflow.Context, tokenCID domain.TokenCID, address *string) error

	// NotifyWebhookClients orchestrates webhook notifications to all matching clients
	NotifyWebhookClients(ctx workflow.Context, event webhook.WebhookEvent) error

	// DeliverWebhook handles webhook delivery to a single client with retry logic
	DeliverWebhook(ctx workflow.Context, clientID string, event webhook.WebhookEvent) error
}

type WorkerCoreConfig struct {
	// TezosChainID is the chain ID for the Tezos blockchain
	TezosChainID domain.Chain
	// EthereumChainID is the chain ID for the Ethereum blockchain
	EthereumChainID domain.Chain
	// EthereumTokenSweepStartBlock is the start block for the Ethereum token sweep
	EthereumTokenSweepStartBlock uint64
	// TezosTokenSweepStartBlock is the start block for the Tezos token sweep
	TezosTokenSweepStartBlock uint64
	// EthereumOwnerFirstBatchTarget is the first-run batch target (token count) for Ethereum owner indexing.
	EthereumOwnerFirstBatchTarget int
	// EthereumOwnerSubsequentBatchTarget is the subsequent-run batch target (token count) for Ethereum owner indexing.
	EthereumOwnerSubsequentBatchTarget int
	// TezosOwnerFirstBatchTarget is the first-run batch target (token count) for Tezos owner indexing.
	TezosOwnerFirstBatchTarget int
	// TezosOwnerSubsequentBatchTarget is the subsequent-run batch target (token count) for Tezos owner indexing.
	TezosOwnerSubsequentBatchTarget int
	// MediaTaskQueue is the task queue for the media worker
	MediaTaskQueue string
	// BudgetedIndexingModeEnabled enables quota-based token indexing
	BudgetedIndexingModeEnabled bool
	// BudgetedIndexingDefaultDailyQuota is the default daily quota for budgeted indexing mode
	BudgetedIndexingDefaultDailyQuota int
}

// workerCore is the concrete implementation of WorkerCore
type workerCore struct {
	config           WorkerCoreConfig
	executor         Executor
	blacklist        registry.BlacklistRegistry
	temporalWorkflow adapter.Workflow
}

// NewWorkerCore creates a new worker core instance
func NewWorkerCore(executor Executor, config WorkerCoreConfig, blacklist registry.BlacklistRegistry, temporalWorkflow adapter.Workflow) WorkerCore {
	return &workerCore{
		executor:         executor,
		config:           config,
		blacklist:        blacklist,
		temporalWorkflow: temporalWorkflow,
	}
}

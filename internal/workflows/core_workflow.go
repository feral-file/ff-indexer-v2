package workflows

import (
	"context"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/providers/jobs"
	"github.com/feral-file/ff-indexer-v2/internal/registry"
	"github.com/feral-file/ff-indexer-v2/internal/webhook"
)

// CoreWorkflows defines the interface for processing blockchain events.
//
//go:generate mockgen -source=core_workflow.go -destination=../mocks/core_workflows.go -package=mocks -mock_names=CoreWorkflows=MockCoreWorkflows
type CoreWorkflows interface {
	// IndexTokenMint processes a token mint event
	IndexTokenMint(ctx context.Context, event *domain.BlockchainEvent) error

	// IndexTokenTransfer processes a token transfer event
	IndexTokenTransfer(ctx context.Context, event *domain.BlockchainEvent) error

	// IndexTokenBurn processes a token burn event
	IndexTokenBurn(ctx context.Context, event *domain.BlockchainEvent) error

	// IndexMetadataUpdate processes a metadata update event
	IndexMetadataUpdate(ctx context.Context, event *domain.BlockchainEvent) error

	// IndexTokenMetadata index token metadata
	IndexTokenMetadata(ctx context.Context, tokenCID domain.TokenCID, address *string) error

	// IndexMultipleTokensMetadata indexes metadata for multiple tokens by triggering child workflows
	IndexMultipleTokensMetadata(ctx context.Context, tokenCIDs []domain.TokenCID) error

	// IndexTokenFromEvent indexes metadata and full provenances (provenance events and balances) for a token
	IndexTokenFromEvent(ctx context.Context, event *domain.BlockchainEvent) error

	// IndexTokens indexes multiple tokens in parallel
	IndexTokens(ctx context.Context, tokenCIDs []domain.TokenCID, address *string) error

	// IndexToken indexes a single token (metadata and provenances)
	IndexToken(ctx context.Context, tokenCID domain.TokenCID, address *string) error

	// IndexTokenOwner indexes all tokens held by a single address
	IndexTokenOwner(ctx context.Context, address string) error

	// IndexTezosTokenOwner indexes all tokens held by a Tezos address
	// jobID is optional and used for job status tracking during quota pauses
	IndexTezosTokenOwner(ctx context.Context, address string, jobID *string) error

	// IndexEthereumTokenOwner indexes all tokens held by an Ethereum address
	// jobID is optional and used for job status tracking during quota pauses
	IndexEthereumTokenOwner(ctx context.Context, address string, jobID *string) error

	// IndexTokenProvenances indexes all provenances (balances and events) for a token
	IndexTokenProvenances(ctx context.Context, tokenCID domain.TokenCID, address *string) error

	// NotifyWebhookClients orchestrates webhook notifications to all matching clients
	NotifyWebhookClients(ctx context.Context, event webhook.WebhookEvent) error

	// DeliverWebhook handles webhook delivery to a single client with retry logic
	DeliverWebhook(ctx context.Context, clientID string, event webhook.WebhookEvent) error
}

type CoreWorkflowsConfig struct {
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
	// MediaEnabled controls whether token metadata indexing should enqueue media-indexing child workflows.
	MediaEnabled bool
	// MediaTaskQueue is the task queue for the media worker
	MediaTaskQueue string
	// BudgetedIndexingModeEnabled enables quota-based token indexing
	BudgetedIndexingModeEnabled bool
	// BudgetedIndexingDefaultDailyQuota is the default daily quota for budgeted indexing mode
	BudgetedIndexingDefaultDailyQuota int
}

// coreWorkflows is the concrete implementation of CoreWorkflows.
type coreWorkflows struct {
	config    CoreWorkflowsConfig
	executor  CoreExecutor
	blacklist registry.BlacklistRegistry
	jobQueue  jobs.JobQueue
}

// NewCoreWorkflows creates a new core workflows instance.
// jobQueue is required. Non-test call sites (Temporal client) that only need method values for ExecuteWorkflow
// may pass [jobs.NopQueue]; unit tests should use a gomock [jobs.JobQueue] implementation instead.
func NewCoreWorkflows(executor CoreExecutor, config CoreWorkflowsConfig, blacklist registry.BlacklistRegistry, jobQueue jobs.JobQueue) CoreWorkflows {
	if jobQueue == nil {
		panic("workflows: NewCoreWorkflows requires a non-nil jobQueue (see NewCoreWorkflows doc for NopQueue vs mocks)")
	}
	return &coreWorkflows{
		executor:  executor,
		config:    config,
		blacklist: blacklist,
		jobQueue:  jobQueue,
	}
}

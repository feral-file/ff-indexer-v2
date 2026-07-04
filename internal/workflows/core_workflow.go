package workflows

import (
	"context"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/providers/jobs"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/artblocks"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/feralfile"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/fxhash"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/objkt"
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
	IndexTezosTokenOwner(ctx context.Context, address string, jobID *int64) error

	// IndexEthereumTokenOwner indexes all tokens held by an Ethereum address
	// jobID is optional and used for job status tracking during quota pauses
	IndexEthereumTokenOwner(ctx context.Context, address string, jobID *int64) error

	// IndexTokenProvenances indexes all provenances (balances and events) for a token
	IndexTokenProvenances(ctx context.Context, tokenCID domain.TokenCID, address *string) error

	// IndexRelease derives token CIDs for a vendor release for an explicit list of 1-based
	// mint numbers and enqueues chunked IndexTokens jobs for the derived set.
	// vendor must be one of: artblocks, feralfile, fxhash, objkt.
	// opensea is not supported for release indexing — OpenSea tokens are indexed via
	// on-chain events where mint_number is derived from the actual token identifier
	// during enrichment.
	//
	// Exactly one of vendorReleaseID or vendorReleaseSlug must be non-empty.
	// When vendorReleaseSlug is provided, the workflow resolves it to a vendor_release_id
	// via the vendor API before CID derivation.
	//
	// CID derivation strategy per vendor:
	//   artblocks — pure math, zero API calls (requires ArtBlocksClient only for slug resolution)
	//   objkt     — pure math (token_number == mint_number); requires ObjktClient for
	//               custom-contract pre-check before CIDs are built
	//   fxhash    — calls GetGentksByIteration(min, max) then filters to mintNumbers set
	//   feralfile — calls GetSeriesArtworks(min, max) then filters to mintNumbers set
	//
	// Returns an error if CID derivation or enqueueing fails; a partial enqueue
	// (some chunks succeed, then a failure) will leave already-queued jobs running.
	IndexRelease(ctx context.Context, vendor string, vendorReleaseID string, vendorReleaseSlug string, mintNumbers []int64) error

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
	// TokenTaskQueue is the jobs.queue name for the token/core worker (async metadata and provenance children, webhooks).
	// It must match the configured token job queue and the worker pool that registers these handlers.
	TokenTaskQueue string
	// MediaEnabled controls whether token metadata indexing should enqueue media-indexing child workflows.
	MediaEnabled bool
	// MediaTaskQueue is the jobs.queue name for the media worker (IndexMediaWorkflow jobs).
	// It must match the configured media job queue when media indexing is enabled.
	MediaTaskQueue string
	// BudgetedIndexingModeEnabled enables quota-based token indexing
	BudgetedIndexingModeEnabled bool
	// BudgetedIndexingDefaultDailyQuota is the default daily quota for budgeted indexing mode
	BudgetedIndexingDefaultDailyQuota int
	// FxhashClient is required for IndexRelease with vendor=fxhash.
	// Nil disables fxhash release indexing (the job will return an error).
	FxhashClient fxhash.Client
	// FeralFileClient is required for IndexRelease with vendor=feralfile.
	// Nil disables Feral File release indexing (the job will return an error).
	FeralFileClient feralfile.Client
	// ArtBlocksClient is required for IndexRelease slug resolution with vendor=artblocks.
	// Nil disables artblocks slug-based triggering (numeric vendor_release_id still works).
	ArtBlocksClient artblocks.Client
	// ObjktClient is required for IndexRelease with vendor=objkt.
	// Before enqueuing CIDs, IndexRelease calls GetFA to verify the contract is a
	// "custom" collection. Nil disables objkt release indexing (the job returns an error).
	ObjktClient objkt.Client
}

// coreWorkflows is the concrete implementation of CoreWorkflows.
type coreWorkflows struct {
	config          CoreWorkflowsConfig
	executor        CoreExecutor
	blacklist       registry.BlacklistRegistry
	jobQueue        jobs.JobQueue
	fxhashClient    fxhash.Client    // may be nil if fxhash release indexing is not configured
	feralfileClient feralfile.Client // may be nil if Feral File release indexing is not configured
	artblocksClient artblocks.Client // may be nil; required only for artblocks slug resolution
	objktClient     objkt.Client     // required for objkt release indexing (custom-contract pre-check)
}

// NewCoreWorkflows creates a new core workflows instance.
// jobQueue is required. FxhashClient, FeralFileClient, and ObjktClient in config are
// optional but must be set for IndexRelease to work with those vendors.
func NewCoreWorkflows(executor CoreExecutor, config CoreWorkflowsConfig, blacklist registry.BlacklistRegistry, jobQueue jobs.JobQueue) CoreWorkflows {
	if jobQueue == nil {
		panic("workflows: NewCoreWorkflows requires a non-nil jobQueue (see NewCoreWorkflows doc for NopQueue vs mocks)")
	}
	return &coreWorkflows{
		executor:        executor,
		config:          config,
		blacklist:       blacklist,
		jobQueue:        jobQueue,
		fxhashClient:    config.FxhashClient,
		feralfileClient: config.FeralFileClient,
		artblocksClient: config.ArtBlocksClient,
		objktClient:     config.ObjktClient,
	}
}

package workflows_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/providers/jobs"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
	"github.com/feral-file/ff-indexer-v2/internal/types"
	"github.com/feral-file/ff-indexer-v2/internal/workflows"
)

// expectIndexTokenFullSuccess configures MockCoreExecutor for one successful IndexToken (MediaEnabled false in testTokenCore).
func expectIndexTokenFullSuccess(m *mocks.MockCoreExecutor) {
	m.EXPECT().IndexTokenWithMinimalProvenancesByTokenCID(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	m.EXPECT().SupportsTokenProvenance(gomock.Any()).Return(true)
	m.EXPECT().ResolveTokenMetadata(gomock.Any(), gomock.Any()).Return(nil, nil)
	m.EXPECT().EnhanceTokenMetadata(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil)
	m.EXPECT().CheckMediaURLsHealthAndUpdateViewability(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&workflows.MediaHealthCheckResult{IsViewable: false, HealthyURLs: nil}, nil)
	m.EXPECT().IndexTokenWithFullProvenancesByTokenCID(gomock.Any(), gomock.Any()).Return(nil)
}

func testTokenCore(t *testing.T) *coreWfDeps {
	t.Helper()
	d := newCoreWfDeps(t, workflows.CoreWorkflowsConfig{
		TezosChainID:                 domain.ChainTezosMainnet,
		EthereumChainID:              domain.ChainEthereumMainnet,
		EthereumTokenSweepStartBlock: 0,
		TezosTokenSweepStartBlock:    0,
		TokenTaskQueue:               "token_index",
		MediaTaskQueue:               "media_index",
		MediaEnabled:                 false,
	}, nil)
	stubJqAnyEnqueue(d.MockJQ)
	return d
}

// --- IndexTokenMint ---

func TestIndexTokenMint_Success(t *testing.T) {
	t.Parallel()
	d := testTokenCore(t)
	defer d.Ctrl.Finish()
	ctx, exec, bl, wf := d.Ctx, d.Exec, d.BlMock, d.Wf

	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeMint,
		ToAddress:       types.StringPtr("0xtoaddr"),
		TxHash:          "0xabcd",
		BlockNumber:     100,
		Quantity:        "1",
	}
	tcid := event.TokenCID()
	bl.EXPECT().IsTokenCIDBlacklisted(tcid).Return(false)
	exec.EXPECT().CreateTokenMint(gomock.Any(), event).Return(nil)

	err := wf.IndexTokenMint(ctx, event)
	require.NoError(t, err)
}

func TestIndexTokenMint_Blacklisted(t *testing.T) {
	t.Parallel()
	d := testTokenCore(t)
	defer d.Ctrl.Finish()
	ctx, exec, bl, wf := d.Ctx, d.Exec, d.BlMock, d.Wf
	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeMint,
		TxHash:          "0xabcd",
		BlockNumber:     100,
	}
	tcid := event.TokenCID()
	bl.EXPECT().IsTokenCIDBlacklisted(tcid).Return(true)

	err := wf.IndexTokenMint(ctx, event)
	require.NoError(t, err)
	_ = exec
}

func TestIndexTokenMint_CreateTokenMintError(t *testing.T) {
	t.Parallel()
	d := testTokenCore(t)
	defer d.Ctrl.Finish()
	ctx, exec, bl, wf := d.Ctx, d.Exec, d.BlMock, d.Wf
	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeMint,
		TxHash:          "0xabcd",
		BlockNumber:     100,
	}
	tcid := event.TokenCID()
	bl.EXPECT().IsTokenCIDBlacklisted(tcid).Return(false)
	exec.EXPECT().CreateTokenMint(gomock.Any(), event).Return(errors.New("database error"))

	err := wf.IndexTokenMint(ctx, event)
	require.Error(t, err)
}

// --- IndexTokenTransfer ---

func TestIndexTokenTransfer_TokenExists(t *testing.T) {
	t.Parallel()
	d := testTokenCore(t)
	defer d.Ctrl.Finish()
	ctx, exec, bl, wf := d.Ctx, d.Exec, d.BlMock, d.Wf
	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeTransfer,
		FromAddress:     types.StringPtr("0xfrom"),
		ToAddress:       types.StringPtr("0xto"),
		TxHash:          "0xabcd",
		BlockNumber:     100,
		Quantity:        "1",
	}
	tcid := event.TokenCID()
	bl.EXPECT().IsTokenCIDBlacklisted(tcid).Return(false)
	exec.EXPECT().CheckTokenExists(gomock.Any(), tcid).Return(true, nil)
	exec.EXPECT().UpdateTokenTransfer(gomock.Any(), event).Return(nil)

	err := wf.IndexTokenTransfer(ctx, event)
	require.NoError(t, err)
}

func TestIndexTokenTransfer_TokenDoesNotExist(t *testing.T) {
	t.Parallel()
	d := testTokenCore(t)
	defer d.Ctrl.Finish()
	ctx, exec, bl, wf := d.Ctx, d.Exec, d.BlMock, d.Wf
	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeTransfer,
		TxHash:          "0xabcd",
		BlockNumber:     100,
	}
	tcid := event.TokenCID()
	// Blacklist: once in IndexTokenTransfer, again in IndexTokenFromEvent.
	bl.EXPECT().IsTokenCIDBlacklisted(tcid).Return(false).Times(2)
	exec.EXPECT().CheckTokenExists(gomock.Any(), tcid).Return(false, nil)
	exec.EXPECT().IndexTokenWithMinimalProvenancesByBlockchainEvent(gomock.Any(), event).Return(nil)
	exec.EXPECT().SupportsTokenProvenance(tcid).Return(true)
	// async metadata + provenance via job queue; Enqueue is AnyTimes

	err := wf.IndexTokenTransfer(ctx, event)
	require.NoError(t, err)
}

// --- IndexTokenBurn ---

func TestIndexTokenBurn_Success(t *testing.T) {
	t.Parallel()
	d := testTokenCore(t)
	defer d.Ctrl.Finish()
	ctx, exec, bl, wf := d.Ctx, d.Exec, d.BlMock, d.Wf
	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeBurn,
		FromAddress:     types.StringPtr("0xfrom"),
		TxHash:          "0xabcd",
		BlockNumber:     100,
		Quantity:        "1",
	}
	tcid := event.TokenCID()
	bl.EXPECT().IsTokenCIDBlacklisted(tcid).Return(false)
	exec.EXPECT().CheckTokenExists(gomock.Any(), tcid).Return(true, nil)
	exec.EXPECT().UpdateTokenBurn(gomock.Any(), event).Return(nil)

	err := wf.IndexTokenBurn(ctx, event)
	require.NoError(t, err)
}

func TestIndexTokenBurn_TokenDoesNotExist(t *testing.T) {
	t.Parallel()
	d := testTokenCore(t)
	defer d.Ctrl.Finish()
	ctx, exec, bl, wf := d.Ctx, d.Exec, d.BlMock, d.Wf
	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeBurn,
		TxHash:          "0xabcd",
		BlockNumber:     100,
	}
	tcid := event.TokenCID()
	bl.EXPECT().IsTokenCIDBlacklisted(tcid).Return(false)
	exec.EXPECT().CheckTokenExists(gomock.Any(), tcid).Return(false, nil)

	err := wf.IndexTokenBurn(ctx, event)
	require.Error(t, err)
	require.Contains(t, err.Error(), "token doesn't exist")
}

// --- IndexTokenFromEvent / IndexTokens / IndexToken ---

func TestIndexTokenFromEvent_Success(t *testing.T) {
	t.Parallel()
	d := testTokenCore(t)
	defer d.Ctrl.Finish()
	ctx, exec, bl, wf := d.Ctx, d.Exec, d.BlMock, d.Wf
	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeMint,
		TxHash:          "0xabcd",
		BlockNumber:     100,
	}
	tcid := event.TokenCID()
	bl.EXPECT().IsTokenCIDBlacklisted(tcid).Return(false)
	exec.EXPECT().IndexTokenWithMinimalProvenancesByBlockchainEvent(gomock.Any(), event).Return(nil)
	exec.EXPECT().SupportsTokenProvenance(tcid).Return(true)

	err := wf.IndexTokenFromEvent(ctx, event)
	require.NoError(t, err)
}

func TestIndexTokens_Success(t *testing.T) {
	t.Parallel()
	d := testTokenCore(t)
	defer d.Ctrl.Finish()
	ctx, exec, bl, wf := d.Ctx, d.Exec, d.BlMock, d.Wf
	t1 := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")
	t2 := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "2")
	// errgroup can complete tokens in any order; use Times(2) per executor step.
	exec.EXPECT().IndexTokenWithMinimalProvenancesByTokenCID(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(2)
	exec.EXPECT().SupportsTokenProvenance(gomock.Any()).Return(true).Times(2)
	exec.EXPECT().ResolveTokenMetadata(gomock.Any(), gomock.Any()).Return(nil, nil).Times(2)
	exec.EXPECT().EnhanceTokenMetadata(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).Times(2)
	exec.EXPECT().CheckMediaURLsHealthAndUpdateViewability(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&workflows.MediaHealthCheckResult{IsViewable: false, HealthyURLs: nil}, nil).Times(2)
	exec.EXPECT().IndexTokenWithFullProvenancesByTokenCID(gomock.Any(), gomock.Any()).Return(nil).Times(2)
	bl.EXPECT().IsTokenCIDBlacklisted(t1).Return(false)
	bl.EXPECT().IsTokenCIDBlacklisted(t2).Return(false)

	err := wf.IndexTokens(ctx, []domain.TokenCID{t1, t2}, nil)
	require.NoError(t, err)
}

func TestIndexToken_Success(t *testing.T) {
	t.Parallel()
	d := testTokenCore(t)
	defer d.Ctrl.Finish()
	ctx, exec, bl, wf := d.Ctx, d.Exec, d.BlMock, d.Wf
	tcid := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")
	bl.EXPECT().IsTokenCIDBlacklisted(tcid).Return(false)
	expectIndexTokenFullSuccess(exec)

	err := wf.IndexToken(ctx, tcid, nil)
	require.NoError(t, err)
}

func TestIndexToken_Success_LegacyContract_SkipsFullProvenance(t *testing.T) {
	t.Parallel()
	d := testTokenCore(t)
	defer d.Ctrl.Finish()
	ctx, exec, bl, wf := d.Ctx, d.Exec, d.BlMock, d.Wf

	const legacyContract = "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb"
	tcid := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, legacyContract, "1")
	bl.EXPECT().IsTokenCIDBlacklisted(tcid).Return(false)
	exec.EXPECT().IndexTokenWithMinimalProvenancesByTokenCID(gomock.Any(), tcid, gomock.Any()).Return(nil)
	exec.EXPECT().SupportsTokenProvenance(gomock.Any()).Return(false)
	exec.EXPECT().ResolveTokenMetadata(gomock.Any(), tcid).Return(nil, nil)
	exec.EXPECT().EnhanceTokenMetadata(gomock.Any(), tcid, gomock.Any()).Return(nil, nil)
	exec.EXPECT().CheckMediaURLsHealthAndUpdateViewability(gomock.Any(), tcid.String(), gomock.Any()).
		Return(&workflows.MediaHealthCheckResult{IsViewable: false, HealthyURLs: nil}, nil)

	err := wf.IndexToken(ctx, tcid, nil)
	require.NoError(t, err)
}

func TestIndexToken_Success_LegacyContract_WithEvents_RunsFullProvenance(t *testing.T) {
	t.Parallel()
	d := testTokenCore(t)
	defer d.Ctrl.Finish()
	ctx, exec, bl, wf := d.Ctx, d.Exec, d.BlMock, d.Wf

	const legacyContract = "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb"
	tcid := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, legacyContract, "1")
	bl.EXPECT().IsTokenCIDBlacklisted(tcid).Return(false)
	exec.EXPECT().IndexTokenWithMinimalProvenancesByTokenCID(gomock.Any(), tcid, gomock.Any()).Return(nil)
	exec.EXPECT().SupportsTokenProvenance(gomock.Any()).Return(true)
	exec.EXPECT().ResolveTokenMetadata(gomock.Any(), tcid).Return(nil, nil)
	exec.EXPECT().EnhanceTokenMetadata(gomock.Any(), tcid, gomock.Any()).Return(nil, nil)
	exec.EXPECT().CheckMediaURLsHealthAndUpdateViewability(gomock.Any(), tcid.String(), gomock.Any()).
		Return(&workflows.MediaHealthCheckResult{IsViewable: false, HealthyURLs: nil}, nil)
	exec.EXPECT().IndexTokenWithFullProvenancesByTokenCID(gomock.Any(), tcid).Return(nil)

	err := wf.IndexToken(ctx, tcid, nil)
	require.NoError(t, err)
}

func TestIndexTokenFromEvent_LegacyContract_SkipsFullProvenance(t *testing.T) {
	t.Parallel()
	d := newCoreWfDeps(t, workflows.CoreWorkflowsConfig{
		TezosChainID:                 domain.ChainTezosMainnet,
		EthereumChainID:              domain.ChainEthereumMainnet,
		EthereumTokenSweepStartBlock: 0,
		TezosTokenSweepStartBlock:    0,
		TokenTaskQueue:               "token_index",
		MediaTaskQueue:               "media_index",
		MediaEnabled:                 false,
	}, nil)
	defer d.Ctrl.Finish()
	ctx, exec, bl, jq, wf := d.Ctx, d.Exec, d.BlMock, d.MockJQ, d.Wf

	const legacyContract = "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb"
	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: legacyContract,
		TokenNumber:     "1",
		EventType:       domain.EventTypeTransfer,
		TxHash:          "0xabcd",
		BlockNumber:     100,
	}
	bl.EXPECT().IsTokenCIDBlacklisted(gomock.Any()).Return(false)
	exec.EXPECT().IndexTokenWithMinimalProvenancesByBlockchainEvent(gomock.Any(), event).Return(nil)
	exec.EXPECT().SupportsTokenProvenance(gomock.Any()).Return(false)
	jq.EXPECT().Enqueue(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, opts jobs.EnqueueOptions) (*schema.Job, bool, error) {
			require.NotEqual(t, "IndexTokenProvenances", opts.Kind)
			return nil, true, nil
		}).AnyTimes()

	err := wf.IndexTokenFromEvent(ctx, event)
	require.NoError(t, err)
}

func TestIndexToken_Skip_TokenNotFound(t *testing.T) {
	t.Parallel()
	d := testTokenCore(t)
	defer d.Ctrl.Finish()
	ctx, exec, bl, wf := d.Ctx, d.Exec, d.BlMock, d.Wf
	tcid := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")
	bl.EXPECT().IsTokenCIDBlacklisted(tcid).Return(false)
	exec.EXPECT().IndexTokenWithMinimalProvenancesByTokenCID(gomock.Any(), tcid, gomock.Any()).
		Return(fmt.Errorf("%s: reverted", domain.ErrTokenNotFoundOnChain.Error()))

	err := wf.IndexToken(ctx, tcid, nil)
	require.NoError(t, err)
}

// TestIndexToken_OwnerPathERC1155_GuardMarksDeferred pins the second deferral
// path: owner-specific ERC-1155 indexing never reaches IndexTokenProvenances
// (its minimal path normally captures events), but under the EVM credit guard
// that path is balance-only, so IndexToken itself must persist the
// deferred-provenance marker or the token would be invisible to the backfill.
func TestIndexToken_OwnerPathERC1155_GuardMarksDeferred(t *testing.T) {
	t.Parallel()
	cfg := defaultCompactCoreWfConfig()
	cfg.EthereumFullProvenanceDisabled = true
	d := newCoreWfDeps(t, cfg, nil)
	defer d.Ctrl.Finish()
	stubJqAnyEnqueue(d.MockJQ)
	ctx, exec, bl, wf := d.Ctx, d.Exec, d.BlMock, d.Wf

	owner := "0x00000000000000000000000000000000000000aa"
	tcid := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC1155, "0x1234567890123456789012345678901234567890", "1")
	bl.EXPECT().IsTokenCIDBlacklisted(tcid).Return(false)
	exec.EXPECT().IndexTokenWithMinimalProvenancesByTokenCID(gomock.Any(), tcid, gomock.Any()).Return(nil)
	exec.EXPECT().MarkTokenProvenanceDeferred(gomock.Any(), tcid).Return(nil)
	exec.EXPECT().ResolveTokenMetadata(gomock.Any(), tcid).Return(nil, nil)
	exec.EXPECT().EnhanceTokenMetadata(gomock.Any(), tcid, gomock.Any()).Return(nil, nil)
	exec.EXPECT().CheckMediaURLsHealthAndUpdateViewability(gomock.Any(), tcid.String(), gomock.Any()).
		Return(&workflows.MediaHealthCheckResult{IsViewable: false, HealthyURLs: nil}, nil)

	err := wf.IndexToken(ctx, tcid, &owner)
	require.NoError(t, err)
}

// TestIndexToken_OwnerPathERC1155_NoGuardNoDeferral pins the guard's scope: with
// the guard off, the owner-specific ERC-1155 path captures events itself, so no
// deferral marker may be written (the strict mock fails on any marking call).
func TestIndexToken_OwnerPathERC1155_NoGuardNoDeferral(t *testing.T) {
	t.Parallel()
	d := testTokenCore(t)
	defer d.Ctrl.Finish()
	ctx, exec, bl, wf := d.Ctx, d.Exec, d.BlMock, d.Wf

	owner := "0x00000000000000000000000000000000000000aa"
	tcid := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC1155, "0x1234567890123456789012345678901234567890", "1")
	bl.EXPECT().IsTokenCIDBlacklisted(tcid).Return(false)
	exec.EXPECT().IndexTokenWithMinimalProvenancesByTokenCID(gomock.Any(), tcid, gomock.Any()).Return(nil)
	exec.EXPECT().ResolveTokenMetadata(gomock.Any(), tcid).Return(nil, nil)
	exec.EXPECT().EnhanceTokenMetadata(gomock.Any(), tcid, gomock.Any()).Return(nil, nil)
	exec.EXPECT().CheckMediaURLsHealthAndUpdateViewability(gomock.Any(), tcid.String(), gomock.Any()).
		Return(&workflows.MediaHealthCheckResult{IsViewable: false, HealthyURLs: nil}, nil)

	err := wf.IndexToken(ctx, tcid, &owner)
	require.NoError(t, err)
}

// TestIndexTokenFromEvent_ERC1155_GuardMarksDeferred pins the third deferral
// path: event-driven ingestion excludes ERC-1155 from full provenance because
// its minimal path normally captures balance deltas — but under the EVM credit
// guard that path is balance-only, so IndexTokenFromEvent must persist the
// deferred-provenance marker or ingestion-discovered ERC-1155 tokens would be
// invisible to the backfill.
func TestIndexTokenFromEvent_ERC1155_GuardMarksDeferred(t *testing.T) {
	t.Parallel()
	cfg := defaultCompactCoreWfConfig()
	cfg.EthereumFullProvenanceDisabled = true
	d := newCoreWfDeps(t, cfg, nil)
	defer d.Ctrl.Finish()
	stubJqAnyEnqueue(d.MockJQ)
	ctx, exec, bl, wf := d.Ctx, d.Exec, d.BlMock, d.Wf

	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC1155,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeTransfer,
		TxHash:          "0xabcd",
		BlockNumber:     100,
	}
	tcid := event.TokenCID()
	bl.EXPECT().IsTokenCIDBlacklisted(tcid).Return(false)
	exec.EXPECT().IndexTokenWithMinimalProvenancesByBlockchainEvent(gomock.Any(), event).Return(nil)
	exec.EXPECT().MarkTokenProvenanceDeferred(gomock.Any(), tcid).Return(nil)

	err := wf.IndexTokenFromEvent(ctx, event)
	require.NoError(t, err)
}

// TestIndexTokenFromEvent_ERC1155_GuardMarkingFailureFailsWorkflow pins the
// durability contract on the event path: a marking failure must fail the
// workflow (so the job retries) rather than report success and silently drop
// the token from the backfill set.
func TestIndexTokenFromEvent_ERC1155_GuardMarkingFailureFailsWorkflow(t *testing.T) {
	t.Parallel()
	cfg := defaultCompactCoreWfConfig()
	cfg.EthereumFullProvenanceDisabled = true
	d := newCoreWfDeps(t, cfg, nil)
	defer d.Ctrl.Finish()
	stubJqAnyEnqueue(d.MockJQ)
	ctx, exec, bl, wf := d.Ctx, d.Exec, d.BlMock, d.Wf

	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC1155,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeTransfer,
		TxHash:          "0xabcd",
		BlockNumber:     100,
	}
	tcid := event.TokenCID()
	bl.EXPECT().IsTokenCIDBlacklisted(tcid).Return(false)
	exec.EXPECT().IndexTokenWithMinimalProvenancesByBlockchainEvent(gomock.Any(), event).Return(nil)
	markErr := errors.New("db unavailable")
	exec.EXPECT().MarkTokenProvenanceDeferred(gomock.Any(), tcid).Return(markErr)

	err := wf.IndexTokenFromEvent(ctx, event)
	require.ErrorIs(t, err, markErr)
}

// TestIndexTokenFromEvent_ERC1155_NoGuardNoDeferral pins the guard's scope on
// the event path: with the guard off, the minimal path captures the owner's
// events itself, so no deferral marker may be written (the strict mock fails on
// any marking call).
func TestIndexTokenFromEvent_ERC1155_NoGuardNoDeferral(t *testing.T) {
	t.Parallel()
	d := testTokenCore(t)
	defer d.Ctrl.Finish()
	ctx, exec, bl, wf := d.Ctx, d.Exec, d.BlMock, d.Wf

	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC1155,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeTransfer,
		TxHash:          "0xabcd",
		BlockNumber:     100,
	}
	tcid := event.TokenCID()
	bl.EXPECT().IsTokenCIDBlacklisted(tcid).Return(false)
	exec.EXPECT().IndexTokenWithMinimalProvenancesByBlockchainEvent(gomock.Any(), event).Return(nil)

	err := wf.IndexTokenFromEvent(ctx, event)
	require.NoError(t, err)
}

// TestIndexToken_DeferralMarkingFailurePropagates pins the durability contract
// through the IndexToken caller: provenance indexing is best-effort, but a
// failed deferral marking under the credit guard must fail the job (so owner
// indexing retries) instead of returning success and permanently dropping the
// token from the operator backfill set.
func TestIndexToken_DeferralMarkingFailurePropagates(t *testing.T) {
	t.Parallel()
	cfg := defaultCompactCoreWfConfig()
	cfg.EthereumFullProvenanceDisabled = true
	d := newCoreWfDeps(t, cfg, nil)
	defer d.Ctrl.Finish()
	stubJqAnyEnqueue(d.MockJQ)
	ctx, exec, bl, wf := d.Ctx, d.Exec, d.BlMock, d.Wf

	// ERC-721 with a triggering address: shouldIndexFullProvenance is true, so
	// the guard skip (and its marking) happens inside IndexTokenProvenances.
	owner := "0x00000000000000000000000000000000000000aa"
	tcid := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")
	bl.EXPECT().IsTokenCIDBlacklisted(tcid).Return(false)
	exec.EXPECT().IndexTokenWithMinimalProvenancesByTokenCID(gomock.Any(), tcid, gomock.Any()).Return(nil)
	exec.EXPECT().SupportsTokenProvenance(tcid).Return(true)
	exec.EXPECT().ResolveTokenMetadata(gomock.Any(), tcid).Return(nil, nil)
	exec.EXPECT().EnhanceTokenMetadata(gomock.Any(), tcid, gomock.Any()).Return(nil, nil)
	exec.EXPECT().CheckMediaURLsHealthAndUpdateViewability(gomock.Any(), tcid.String(), gomock.Any()).
		Return(&workflows.MediaHealthCheckResult{IsViewable: false, HealthyURLs: nil}, nil)
	exec.EXPECT().MarkTokenProvenanceDeferred(gomock.Any(), tcid).Return(errors.New("db unavailable"))

	err := wf.IndexToken(ctx, tcid, &owner)
	require.ErrorIs(t, err, workflows.ErrDeferralMarkingFailed)
}

// TestIndexToken_OrdinaryProvenanceFailureStaysBestEffort pins the counterpart:
// without the guard, a plain provenance failure is logged and IndexToken still
// succeeds — the pre-guard best-effort contract is unchanged.
func TestIndexToken_OrdinaryProvenanceFailureStaysBestEffort(t *testing.T) {
	t.Parallel()
	d := testTokenCore(t)
	defer d.Ctrl.Finish()
	ctx, exec, bl, wf := d.Ctx, d.Exec, d.BlMock, d.Wf

	tcid := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")
	bl.EXPECT().IsTokenCIDBlacklisted(tcid).Return(false)
	exec.EXPECT().IndexTokenWithMinimalProvenancesByTokenCID(gomock.Any(), tcid, gomock.Any()).Return(nil)
	exec.EXPECT().SupportsTokenProvenance(tcid).Return(true)
	exec.EXPECT().ResolveTokenMetadata(gomock.Any(), tcid).Return(nil, nil)
	exec.EXPECT().EnhanceTokenMetadata(gomock.Any(), tcid, gomock.Any()).Return(nil, nil)
	exec.EXPECT().CheckMediaURLsHealthAndUpdateViewability(gomock.Any(), tcid.String(), gomock.Any()).
		Return(&workflows.MediaHealthCheckResult{IsViewable: false, HealthyURLs: nil}, nil)
	exec.EXPECT().IndexTokenWithFullProvenancesByTokenCID(gomock.Any(), tcid).Return(errors.New("rpc flake"))

	err := wf.IndexToken(ctx, tcid, nil)
	require.NoError(t, err)
}

// TestIndexTokens_AddresslessERC1155_GuardDefersProvenance covers the
// public-trigger route (API enqueues IndexTokens with a nil address) under the
// credit guard: minimal indexing runs (with balances withheld at the client),
// and the token must end up in the backfill set via the provenance gate's
// deferral marking. The strict mock fails the test if the expensive full
// provenance replay is attempted.
func TestIndexTokens_AddresslessERC1155_GuardDefersProvenance(t *testing.T) {
	t.Parallel()
	cfg := defaultCompactCoreWfConfig()
	cfg.EthereumFullProvenanceDisabled = true
	d := newCoreWfDeps(t, cfg, nil)
	defer d.Ctrl.Finish()
	stubJqAnyEnqueue(d.MockJQ)
	ctx, exec, bl, wf := d.Ctx, d.Exec, d.BlMock, d.Wf

	tcid := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC1155, "0x1234567890123456789012345678901234567890", "1")
	bl.EXPECT().IsTokenCIDBlacklisted(tcid).Return(false)
	exec.EXPECT().IndexTokenWithMinimalProvenancesByTokenCID(gomock.Any(), tcid, gomock.Nil()).Return(nil)
	exec.EXPECT().SupportsTokenProvenance(tcid).Return(true)
	exec.EXPECT().MarkTokenProvenanceDeferred(gomock.Any(), tcid).Return(nil)
	exec.EXPECT().ResolveTokenMetadata(gomock.Any(), tcid).Return(nil, nil)
	exec.EXPECT().EnhanceTokenMetadata(gomock.Any(), tcid, gomock.Any()).Return(nil, nil)
	exec.EXPECT().CheckMediaURLsHealthAndUpdateViewability(gomock.Any(), tcid.String(), gomock.Any()).
		Return(&workflows.MediaHealthCheckResult{IsViewable: false, HealthyURLs: nil}, nil)

	err := wf.IndexTokens(ctx, []domain.TokenCID{tcid}, nil)
	require.NoError(t, err)
}

// TestIndexTokenFromEvent_ERC721_GuardMarksDeferredSynchronously pins round-7
// durability on the event path for supported non-ERC-1155 tokens: under the
// guard, the deferral must be persisted synchronously — not delegated to the
// fire-and-forget IndexTokenProvenances enqueue, whose failure is swallowed.
// The job-queue mock rejects any provenance enqueue to prove the async path is
// not taken.
func TestIndexTokenFromEvent_ERC721_GuardMarksDeferredSynchronously(t *testing.T) {
	t.Parallel()
	cfg := defaultCompactCoreWfConfig()
	cfg.EthereumFullProvenanceDisabled = true
	d := newCoreWfDeps(t, cfg, nil)
	defer d.Ctrl.Finish()
	ctx, exec, bl, wf := d.Ctx, d.Exec, d.BlMock, d.Wf

	d.MockJQ.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, opts jobs.EnqueueOptions) (*schema.Job, bool, error) {
			require.NotEqual(t, "IndexTokenProvenances", opts.Kind,
				"guarded event path must not rely on the fire-and-forget provenance enqueue")
			return nil, true, nil
		}).
		AnyTimes()

	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeMint,
		TxHash:          "0xabcd",
		BlockNumber:     100,
	}
	tcid := event.TokenCID()
	bl.EXPECT().IsTokenCIDBlacklisted(tcid).Return(false)
	exec.EXPECT().IndexTokenWithMinimalProvenancesByBlockchainEvent(gomock.Any(), event).Return(nil)
	exec.EXPECT().SupportsTokenProvenance(tcid).Return(true)
	exec.EXPECT().MarkTokenProvenanceDeferred(gomock.Any(), tcid).Return(nil)

	err := wf.IndexTokenFromEvent(ctx, event)
	require.NoError(t, err)
}

// TestIndexTokenFromEvent_ERC721_GuardMarkingFailureFailsWorkflow pins that a
// synchronous marking failure on the supported event path fails the workflow so
// the job retries — the marker cannot be silently lost.
func TestIndexTokenFromEvent_ERC721_GuardMarkingFailureFailsWorkflow(t *testing.T) {
	t.Parallel()
	cfg := defaultCompactCoreWfConfig()
	cfg.EthereumFullProvenanceDisabled = true
	d := newCoreWfDeps(t, cfg, nil)
	defer d.Ctrl.Finish()
	stubJqAnyEnqueue(d.MockJQ)
	ctx, exec, bl, wf := d.Ctx, d.Exec, d.BlMock, d.Wf

	event := &domain.BlockchainEvent{
		Chain:           domain.ChainEthereumMainnet,
		Standard:        domain.StandardERC721,
		ContractAddress: "0x1234567890123456789012345678901234567890",
		TokenNumber:     "1",
		EventType:       domain.EventTypeMint,
		TxHash:          "0xabcd",
		BlockNumber:     100,
	}
	tcid := event.TokenCID()
	bl.EXPECT().IsTokenCIDBlacklisted(tcid).Return(false)
	exec.EXPECT().IndexTokenWithMinimalProvenancesByBlockchainEvent(gomock.Any(), event).Return(nil)
	exec.EXPECT().SupportsTokenProvenance(tcid).Return(true)
	markErr := errors.New("db unavailable")
	exec.EXPECT().MarkTokenProvenanceDeferred(gomock.Any(), tcid).Return(markErr)

	err := wf.IndexTokenFromEvent(ctx, event)
	require.ErrorIs(t, err, markErr)
}

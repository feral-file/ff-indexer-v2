package workflows_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/providers/jobs"
	"github.com/feral-file/ff-indexer-v2/internal/store"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
	"github.com/feral-file/ff-indexer-v2/internal/workflows"
)

func ownerCfg() workflows.CoreWorkflowsConfig {
	return workflows.CoreWorkflowsConfig{
		TezosChainID:                       domain.ChainTezosMainnet,
		EthereumChainID:                    domain.ChainEthereumMainnet,
		EthereumTokenSweepStartBlock:       1000,
		TezosTokenSweepStartBlock:          1000,
		EthereumOwnerFirstBatchTarget:      50,
		EthereumOwnerSubsequentBatchTarget: 50,
		TezosOwnerFirstBatchTarget:         50,
		TezosOwnerSubsequentBatchTarget:    50,
		TokenTaskQueue:                     "token_index",
		MediaTaskQueue:                     "media_index",
		MediaEnabled:                       false,
		BudgetedIndexingDefaultDailyQuota:  1000,
	}
}

func newOwnerWf(t *testing.T, cfg workflows.CoreWorkflowsConfig) *coreWfDeps {
	t.Helper()
	d := newCoreWfDeps(t, cfg, nil)
	stubJqAnyEnqueue(d.MockJQ)
	// Owner paths index many token CIDs per test; allow all without tcid-specific EXPECTs.
	d.BlMock.EXPECT().IsTokenCIDBlacklisted(gomock.Any()).Return(false).AnyTimes()
	d.Exec.EXPECT().SupportsTokenProvenance(gomock.Any()).Return(true).AnyTimes()
	return d
}

// stubSuccessfulIndexToken wires executor expectations for a successful in-process IndexToken (MediaEnabled off in ownerCfg).
func stubSuccessfulIndexToken(exec *mocks.MockCoreExecutor) {
	exec.EXPECT().IndexTokenWithMinimalProvenancesByTokenCID(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	exec.EXPECT().SupportsTokenProvenance(gomock.Any()).Return(true).AnyTimes()
	exec.EXPECT().ResolveTokenMetadata(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	exec.EXPECT().EnhanceTokenMetadata(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	exec.EXPECT().CheckMediaURLsHealthAndUpdateViewability(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&workflows.MediaHealthCheckResult{IsViewable: false, HealthyURLs: nil}, nil).AnyTimes()
	exec.EXPECT().IndexTokenWithFullProvenancesByTokenCID(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
}

// --- IndexTokenOwner ---

func TestIndexTokenOwner_UnknownAddress(t *testing.T) {
	t.Parallel()
	d := newOwnerWf(t, ownerCfg())
	defer d.Ctrl.Finish()
	wf := d.Wf
	ctx := d.Ctx
	err := wf.IndexTokenOwner(ctx, "not-a-known-chain-address-format-xyz")
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported blockchain")
}

func TestIndexTokenOwner_Ethereum_EmptyFirstRun(t *testing.T) {
	t.Parallel()
	d := newOwnerWf(t, ownerCfg())
	defer d.Ctrl.Finish()
	ctx := jobs.WithJobID(d.Ctx, 1001)
	exec, wf := d.Exec, d.Wf
	addr := "0x1234567890123456789012345678901234567890"
	chainID := domain.ChainEthereumMainnet
	latest := uint64(5000)
	exec.EXPECT().CreateIndexingJob(gomock.Any(), addr, chainID, gomock.Any()).Return(nil)
	exec.EXPECT().UpdateIndexingJobStatus(gomock.Any(), gomock.Any(), schema.IndexingJobStatusRunning, gomock.Any()).Return(nil)
	exec.EXPECT().EnsureWatchedAddressExists(gomock.Any(), addr, chainID, gomock.Any()).Return(nil)
	exec.EXPECT().GetIndexingBlockRangeForAddress(gomock.Any(), addr, chainID).Return(&workflows.BlockRangeResult{MinBlock: 0, MaxBlock: 0}, nil)
	exec.EXPECT().GetLatestEthereumBlock(gomock.Any()).Return(latest, nil)
	exec.EXPECT().GetEthereumTokenCIDsByOwnerWithinBlockRange(
		gomock.Any(), addr, uint64(1000), latest, gomock.Any(), domain.BlockScanOrderDesc,
	).Return(domain.TokenWithBlockRangeResult{
		Tokens:             nil,
		EffectiveFromBlock: 1000,
		EffectiveToBlock:   latest,
	}, nil)
	exec.EXPECT().UpdateIndexingBlockRangeForAddress(gomock.Any(), addr, chainID, uint64(1000), latest).Return(nil)
	exec.EXPECT().UpdateIndexingJobStatus(gomock.Any(), gomock.Any(), schema.IndexingJobStatusCompleted, gomock.Any()).Return(nil)

	require.NoError(t, wf.IndexTokenOwner(ctx, addr))
}

func TestIndexTokenOwner_Tezos_EmptyFirstRun(t *testing.T) {
	t.Parallel()
	d := newOwnerWf(t, ownerCfg())
	defer d.Ctrl.Finish()
	ctx := jobs.WithJobID(d.Ctx, 1002)
	exec, wf := d.Exec, d.Wf
	addr := "tz1VSUr8wwNhLAzempoch5d6hLRiTh8Cjcjb"
	chainID := domain.ChainTezosMainnet
	latest := uint64(5000)
	exec.EXPECT().CreateIndexingJob(gomock.Any(), addr, chainID, gomock.Any()).Return(nil)
	exec.EXPECT().UpdateIndexingJobStatus(gomock.Any(), gomock.Any(), schema.IndexingJobStatusRunning, gomock.Any()).Return(nil)
	exec.EXPECT().EnsureWatchedAddressExists(gomock.Any(), addr, chainID, gomock.Any()).Return(nil)
	exec.EXPECT().GetIndexingBlockRangeForAddress(gomock.Any(), addr, chainID).Return(&workflows.BlockRangeResult{MinBlock: 0, MaxBlock: 0}, nil)
	exec.EXPECT().GetLatestTezosBlock(gomock.Any()).Return(latest, nil)
	exec.EXPECT().GetTezosTokenCIDsByAccountWithinBlockRange(gomock.Any(), addr, uint64(1000), latest).Return([]domain.TokenWithBlock{}, nil)
	exec.EXPECT().UpdateIndexingBlockRangeForAddress(gomock.Any(), addr, chainID, uint64(1000), latest).Return(nil)
	exec.EXPECT().UpdateIndexingJobStatus(gomock.Any(), gomock.Any(), schema.IndexingJobStatusCompleted, gomock.Any()).Return(nil)

	require.NoError(t, wf.IndexTokenOwner(ctx, addr))
}

func TestIndexTokenOwner_JobCreateFailure_StillIndexes(t *testing.T) {
	t.Parallel()
	d := newOwnerWf(t, ownerCfg())
	defer d.Ctrl.Finish()
	ctx := jobs.WithJobID(d.Ctx, 1003)
	exec, wf := d.Exec, d.Wf
	addr := "0x1234567890123456789012345678901234567890"
	chainID := domain.ChainEthereumMainnet
	latest := uint64(5000)
	exec.EXPECT().CreateIndexingJob(gomock.Any(), addr, chainID, gomock.Any()).Return(errors.New("db error"))
	exec.EXPECT().UpdateIndexingJobStatus(gomock.Any(), gomock.Any(), schema.IndexingJobStatusRunning, gomock.Any()).Return(nil)
	exec.EXPECT().EnsureWatchedAddressExists(gomock.Any(), addr, chainID, gomock.Any()).Return(nil)
	exec.EXPECT().GetIndexingBlockRangeForAddress(gomock.Any(), addr, chainID).Return(&workflows.BlockRangeResult{MinBlock: 0, MaxBlock: 0}, nil)
	exec.EXPECT().GetLatestEthereumBlock(gomock.Any()).Return(latest, nil)
	exec.EXPECT().GetEthereumTokenCIDsByOwnerWithinBlockRange(
		gomock.Any(), addr, uint64(1000), latest, gomock.Any(), domain.BlockScanOrderDesc,
	).Return(domain.TokenWithBlockRangeResult{EffectiveFromBlock: 1000, EffectiveToBlock: latest}, nil)
	exec.EXPECT().UpdateIndexingBlockRangeForAddress(gomock.Any(), addr, chainID, uint64(1000), latest).Return(nil)
	exec.EXPECT().UpdateIndexingJobStatus(gomock.Any(), gomock.Any(), schema.IndexingJobStatusCompleted, gomock.Any()).Return(nil)

	require.NoError(t, wf.IndexTokenOwner(ctx, addr))
}

// TestIndexTokenOwner_IndexingError_MarksJobFailed matches the “child/owner path failed and job is marked failed”
// contract (legacy: TestIndexTokenOwner_ChildWorkflowFails_JobMarkedAsFailed on pre-jobs-queue main).
func TestIndexTokenOwner_IndexingError_MarksJobFailed(t *testing.T) {
	t.Parallel()
	d := newOwnerWf(t, ownerCfg())
	defer d.Ctrl.Finish()
	ctx := jobs.WithJobID(d.Ctx, 1004)
	exec, wf := d.Exec, d.Wf
	addr := "0x1234567890123456789012345678901234567890"
	chainID := domain.ChainEthereumMainnet
	latest := uint64(5000)
	innerErr := errors.New("indexing failed")
	exec.EXPECT().CreateIndexingJob(gomock.Any(), addr, chainID, gomock.Any()).Return(nil)
	exec.EXPECT().UpdateIndexingJobStatus(gomock.Any(), gomock.Any(), schema.IndexingJobStatusRunning, gomock.Any()).Return(nil)
	exec.EXPECT().EnsureWatchedAddressExists(gomock.Any(), addr, chainID, gomock.Any()).Return(nil)
	exec.EXPECT().GetIndexingBlockRangeForAddress(gomock.Any(), addr, chainID).Return(&workflows.BlockRangeResult{MinBlock: 0, MaxBlock: 0}, nil)
	exec.EXPECT().GetLatestEthereumBlock(gomock.Any()).Return(latest, nil)
	tok := domain.TokenWithBlock{
		TokenCID:    domain.NewTokenCID(chainID, domain.StandardERC721, "0xContract00000000000000000000000000000000", "1"),
		BlockNumber: 2000,
	}
	exec.EXPECT().GetEthereumTokenCIDsByOwnerWithinBlockRange(
		gomock.Any(), addr, uint64(1000), latest, gomock.Any(), domain.BlockScanOrderDesc,
	).Return(domain.TokenWithBlockRangeResult{Tokens: []domain.TokenWithBlock{tok}, EffectiveFromBlock: 1000, EffectiveToBlock: latest}, nil)
	exec.EXPECT().IndexTokenWithMinimalProvenancesByTokenCID(gomock.Any(), gomock.Any(), gomock.Any()).Return(innerErr)
	exec.EXPECT().UpdateIndexingJobStatus(gomock.Any(), gomock.Any(), schema.IndexingJobStatusFailed, gomock.Any()).Return(nil)

	err := wf.IndexTokenOwner(ctx, addr)
	require.Error(t, err)
	require.ErrorIs(t, err, innerErr)
}

// --- IndexTezosTokenOwner ---

func TestIndexTezosTokenOwner_EnsureWatchedError(t *testing.T) {
	t.Parallel()
	d := newOwnerWf(t, ownerCfg())
	defer d.Ctrl.Finish()
	ctx, exec, wf := d.Ctx, d.Exec, d.Wf
	addr := "tz1VSUr8wwNhLAzempoch5d6hLRiTh8Cjcjb"
	chainID := domain.ChainTezosMainnet
	e := errors.New("watch error")
	exec.EXPECT().EnsureWatchedAddressExists(gomock.Any(), addr, chainID, gomock.Any()).Return(e)
	err := wf.IndexTezosTokenOwner(ctx, addr, nil)
	require.ErrorIs(t, err, e)
}

// TestIndexTezosTokenOwner_GetLatestError (legacy suite retried the activity; v1 calls get-latest once).
func TestIndexTezosTokenOwner_GetLatestError(t *testing.T) {
	t.Parallel()
	d := newOwnerWf(t, ownerCfg())
	defer d.Ctrl.Finish()
	ctx, exec, wf := d.Ctx, d.Exec, d.Wf
	addr := "tz1VSUr8wwNhLAzempoch5d6hLRiTh8Cjcjb"
	chainID := domain.ChainTezosMainnet
	exec.EXPECT().EnsureWatchedAddressExists(gomock.Any(), addr, chainID, gomock.Any()).Return(nil)
	exec.EXPECT().GetIndexingBlockRangeForAddress(gomock.Any(), addr, chainID).Return(&workflows.BlockRangeResult{MinBlock: 0, MaxBlock: 0}, nil)
	exec.EXPECT().GetLatestTezosBlock(gomock.Any()).Return(uint64(0), errors.New("tzkt down"))
	err := wf.IndexTezosTokenOwner(ctx, addr, nil)
	require.Error(t, err)
}

func TestIndexTezosTokenOwner_FirstRun_50Tokens_OneChunk(t *testing.T) {
	t.Parallel()
	d := newOwnerWf(t, ownerCfg())
	defer d.Ctrl.Finish()
	ctx, exec, wf := d.Ctx, d.Exec, d.Wf
	addr := "tz1FirstRun"
	chainID := domain.ChainTezosMainnet
	latest := uint64(5000)
	tokens := make([]domain.TokenWithBlock, 50)
	for i := range 50 {
		tokens[i] = domain.TokenWithBlock{
			TokenCID:    domain.NewTokenCID(chainID, domain.StandardFA2, "KT1ABC", fmt.Sprintf("%d", i)),
			BlockNumber: uint64(2000 + i*10), //nolint:gosec,G115
		}
	}
	exec.EXPECT().EnsureWatchedAddressExists(gomock.Any(), addr, chainID, gomock.Any()).Return(nil)
	exec.EXPECT().GetIndexingBlockRangeForAddress(gomock.Any(), addr, chainID).Return(&workflows.BlockRangeResult{MinBlock: 0, MaxBlock: 0}, nil)
	exec.EXPECT().GetLatestTezosBlock(gomock.Any()).Return(latest, nil)
	exec.EXPECT().GetTezosTokenCIDsByAccountWithinBlockRange(gomock.Any(), addr, uint64(1000), latest).Return(tokens, nil)
	stubSuccessfulIndexToken(exec)
	exec.EXPECT().UpdateIndexingBlockRangeForAddress(gomock.Any(), addr, chainID, gomock.Any(), uint64(5000)).Return(nil)
	exec.EXPECT().UpdateIndexingBlockRangeForAddress(gomock.Any(), addr, chainID, uint64(1000), uint64(5000)).Return(nil)
	require.NoError(t, wf.IndexTezosTokenOwner(ctx, addr, nil))
}

// --- IndexEthereumTokenOwner ---

func TestIndexEthereumTokenOwner_FirstRun_NoNewBlocks(t *testing.T) {
	t.Parallel()
	d := newOwnerWf(t, ownerCfg())
	defer d.Ctrl.Finish()
	ctx, exec, wf := d.Ctx, d.Exec, d.Wf
	addr := "0xUpToDate0000000000000000000000000000000"
	chainID := domain.ChainEthereumMainnet
	latest := uint64(5000)
	exec.EXPECT().EnsureWatchedAddressExists(gomock.Any(), addr, chainID, gomock.Any()).Return(nil)
	exec.EXPECT().GetIndexingBlockRangeForAddress(gomock.Any(), addr, chainID).Return(
		&workflows.BlockRangeResult{MinBlock: 1000, MaxBlock: uint64(5000)}, nil)
	exec.EXPECT().GetLatestEthereumBlock(gomock.Any()).Return(latest, nil)
	require.NoError(t, wf.IndexEthereumTokenOwner(ctx, addr, nil))
}

func TestIndexEthereumTokenOwner_FirstRun_IndexTokenChunkError(t *testing.T) {
	t.Parallel()
	d := newOwnerWf(t, ownerCfg())
	defer d.Ctrl.Finish()
	ctx, exec, wf := d.Ctx, d.Exec, d.Wf
	addr := "0xFailChunk00000000000000000000000000000"
	chainID := domain.ChainEthereumMainnet
	latest := uint64(5000)
	tok := []domain.TokenWithBlock{{
		TokenCID:    domain.NewTokenCID(chainID, domain.StandardERC721, "0xC00000000000000000000000000000000000001", "1"),
		BlockNumber: 2000,
	}}
	exec.EXPECT().EnsureWatchedAddressExists(gomock.Any(), addr, chainID, gomock.Any()).Return(nil)
	exec.EXPECT().GetIndexingBlockRangeForAddress(gomock.Any(), addr, chainID).Return(&workflows.BlockRangeResult{MinBlock: 0, MaxBlock: 0}, nil)
	exec.EXPECT().GetLatestEthereumBlock(gomock.Any()).Return(latest, nil)
	exec.EXPECT().GetEthereumTokenCIDsByOwnerWithinBlockRange(
		gomock.Any(), addr, uint64(1000), latest, gomock.Any(), domain.BlockScanOrderDesc,
	).Return(domain.TokenWithBlockRangeResult{Tokens: tok, EffectiveFromBlock: 1000, EffectiveToBlock: latest}, nil)
	chunkErr := errors.New("indexing failed")
	exec.EXPECT().IndexTokenWithMinimalProvenancesByTokenCID(gomock.Any(), gomock.Any(), gomock.Any()).Return(chunkErr)
	err := wf.IndexEthereumTokenOwner(ctx, addr, nil)
	require.Error(t, err)
}

func TestIndexEthereumTokenOwner_UpdateBlockRangeError(t *testing.T) {
	t.Parallel()
	d := newOwnerWf(t, ownerCfg())
	defer d.Ctrl.Finish()
	ctx, exec, wf := d.Ctx, d.Exec, d.Wf
	addr := "0xBlockRangeErr00000000000000000000000"
	chainID := domain.ChainEthereumMainnet
	latest := uint64(5000)
	tok := []domain.TokenWithBlock{{
		TokenCID:    domain.NewTokenCID(chainID, domain.StandardERC721, "0xC00000000000000000000000000000000000001", "1"),
		BlockNumber: 2000,
	}}
	exec.EXPECT().EnsureWatchedAddressExists(gomock.Any(), addr, chainID, gomock.Any()).Return(nil)
	exec.EXPECT().GetIndexingBlockRangeForAddress(gomock.Any(), addr, chainID).Return(&workflows.BlockRangeResult{MinBlock: 0, MaxBlock: 0}, nil)
	exec.EXPECT().GetLatestEthereumBlock(gomock.Any()).Return(latest, nil)
	exec.EXPECT().GetEthereumTokenCIDsByOwnerWithinBlockRange(
		gomock.Any(), addr, uint64(1000), latest, gomock.Any(), domain.BlockScanOrderDesc,
	).Return(domain.TokenWithBlockRangeResult{Tokens: tok, EffectiveFromBlock: 1000, EffectiveToBlock: latest}, nil)
	stubSuccessfulIndexToken(exec)
	exec.EXPECT().UpdateIndexingBlockRangeForAddress(gomock.Any(), addr, chainID, gomock.Any(), gomock.Any()).
		Return(errors.New("database error"))
	err := wf.IndexEthereumTokenOwner(ctx, addr, nil)
	require.Error(t, err)
}

func TestIndexEthereumTokenOwner_BudgetedMode_QuotaReschedules(t *testing.T) {
	t.Parallel()
	cfg := ownerCfg()
	cfg.BudgetedIndexingModeEnabled = true
	cfg.BudgetedIndexingDefaultDailyQuota = 50
	d := newOwnerWf(t, cfg)
	defer d.Ctrl.Finish()
	ctx, exec, wf := d.Ctx, d.Exec, d.Wf
	addr := "0xQuotaEth00000000000000000000000000000"
	chainID := domain.ChainEthereumMainnet
	latest := uint64(5000)
	tokens := make([]domain.TokenWithBlock, 100)
	for i := range 100 {
		tokens[i] = domain.TokenWithBlock{
			TokenCID:    domain.TokenCID(fmt.Sprintf("eip155:1:erc721:0xab:%d", i)),
			BlockNumber: uint64(1000 + i), //nolint:gosec,G115
		}
	}
	reset := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	exec.EXPECT().EnsureWatchedAddressExists(gomock.Any(), addr, chainID, gomock.Any()).Return(nil)
	exec.EXPECT().GetIndexingBlockRangeForAddress(gomock.Any(), addr, chainID).Return(&workflows.BlockRangeResult{MinBlock: 0, MaxBlock: 0}, nil)
	exec.EXPECT().GetLatestEthereumBlock(gomock.Any()).Return(latest, nil)
	exec.EXPECT().GetEthereumTokenCIDsByOwnerWithinBlockRange(
		gomock.Any(), addr, uint64(1000), latest, gomock.Any(), domain.BlockScanOrderDesc,
	).Return(domain.TokenWithBlockRangeResult{
		Tokens:             tokens,
		EffectiveFromBlock: 1000,
		EffectiveToBlock:   latest,
	}, nil)
	gomock.InOrder(
		exec.EXPECT().GetQuotaInfo(gomock.Any(), addr, chainID).Return(&store.QuotaInfo{
			RemainingQuota: 50, TotalQuota: 50, QuotaExhausted: false, QuotaResetAt: reset,
		}, nil),
		exec.EXPECT().GetQuotaInfo(gomock.Any(), addr, chainID).Return(&store.QuotaInfo{
			RemainingQuota: 0, TotalQuota: 50, QuotaExhausted: true, QuotaResetAt: reset,
		}, nil),
	)
	stubSuccessfulIndexToken(exec)
	exec.EXPECT().IncrementTokensIndexed(gomock.Any(), addr, chainID, 50).Return(nil)
	exec.EXPECT().UpdateIndexingBlockRangeForAddress(gomock.Any(), addr, chainID, gomock.Any(), gomock.Any()).Return(nil)

	err := wf.IndexEthereumTokenOwner(ctx, addr, nil)
	require.Error(t, err)
	var re *jobs.RescheduleError
	require.True(t, errors.As(err, &re), "expected jobs.ErrReschedule, got %v", err)
}

func TestIndexTezosTokenOwner_BudgetedMode_QuotaReschedules(t *testing.T) {
	t.Parallel()
	cfg := ownerCfg()
	cfg.BudgetedIndexingModeEnabled = true
	cfg.BudgetedIndexingDefaultDailyQuota = 50
	d := newOwnerWf(t, cfg)
	defer d.Ctrl.Finish()
	ctx, exec, wf := d.Ctx, d.Exec, d.Wf
	addr := "tz1QuotaTez"
	chainID := domain.ChainTezosMainnet
	latest := uint64(5000)
	tokens := make([]domain.TokenWithBlock, 100)
	for i := range 100 {
		tokens[i] = domain.TokenWithBlock{
			TokenCID:    domain.TokenCID(fmt.Sprintf("tezos:mainnet:fa2:KT1q:%d", i)),
			BlockNumber: uint64(1000 + i), //nolint:gosec,G115
		}
	}
	reset := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	exec.EXPECT().EnsureWatchedAddressExists(gomock.Any(), addr, chainID, gomock.Any()).Return(nil)
	exec.EXPECT().GetIndexingBlockRangeForAddress(gomock.Any(), addr, chainID).Return(&workflows.BlockRangeResult{MinBlock: 0, MaxBlock: 0}, nil)
	exec.EXPECT().GetLatestTezosBlock(gomock.Any()).Return(latest, nil)
	exec.EXPECT().GetTezosTokenCIDsByAccountWithinBlockRange(gomock.Any(), addr, uint64(1000), latest).Return(tokens, nil)
	gomock.InOrder(
		exec.EXPECT().GetQuotaInfo(gomock.Any(), addr, chainID).Return(&store.QuotaInfo{
			RemainingQuota: 50, TotalQuota: 50, QuotaExhausted: false, QuotaResetAt: reset,
		}, nil),
		exec.EXPECT().GetQuotaInfo(gomock.Any(), addr, chainID).Return(&store.QuotaInfo{
			RemainingQuota: 0, TotalQuota: 50, QuotaExhausted: true, QuotaResetAt: reset,
		}, nil),
	)
	stubSuccessfulIndexToken(exec)
	exec.EXPECT().IncrementTokensIndexed(gomock.Any(), addr, chainID, 50).Return(nil)
	exec.EXPECT().UpdateIndexingBlockRangeForAddress(gomock.Any(), addr, chainID, gomock.Any(), gomock.Any()).Return(nil)

	err := wf.IndexTezosTokenOwner(ctx, addr, nil)
	require.Error(t, err)
	var re *jobs.RescheduleError
	require.True(t, errors.As(err, &re), "expected jobs.ErrReschedule, got %v", err)
}

// TestIndexTokenOwner_JobStatusUpdateFailures_NonFatal verifies UpdateIndexingJobStatus errors for running
// and completed are logged but do not block success (legacy: TestIndexTokenOwner_JobStatusUpdateFails_WorkflowContinues).
func TestIndexTokenOwner_JobStatusUpdateFailures_NonFatal(t *testing.T) {
	t.Parallel()
	d := newOwnerWf(t, ownerCfg())
	defer d.Ctrl.Finish()
	ctx := jobs.WithJobID(d.Ctx, 1005)
	exec, wf := d.Exec, d.Wf
	addr := "tz1JobStatusFails"
	chainID := domain.ChainTezosMainnet
	latest := uint64(5000)
	exec.EXPECT().CreateIndexingJob(gomock.Any(), addr, chainID, gomock.Any()).Return(nil)
	exec.EXPECT().UpdateIndexingJobStatus(gomock.Any(), gomock.Any(), schema.IndexingJobStatusRunning, gomock.Any()).
		Return(errors.New("status update error"))
	exec.EXPECT().EnsureWatchedAddressExists(gomock.Any(), addr, chainID, gomock.Any()).Return(nil)
	exec.EXPECT().GetIndexingBlockRangeForAddress(gomock.Any(), addr, chainID).Return(&workflows.BlockRangeResult{MinBlock: 0, MaxBlock: 0}, nil)
	exec.EXPECT().GetLatestTezosBlock(gomock.Any()).Return(latest, nil)
	exec.EXPECT().GetTezosTokenCIDsByAccountWithinBlockRange(gomock.Any(), addr, uint64(1000), latest).Return([]domain.TokenWithBlock{}, nil)
	exec.EXPECT().UpdateIndexingBlockRangeForAddress(gomock.Any(), addr, chainID, gomock.Any(), gomock.Any()).Return(nil)
	exec.EXPECT().UpdateIndexingJobStatus(gomock.Any(), gomock.Any(), schema.IndexingJobStatusCompleted, gomock.Any()).
		Return(errors.New("status update error"))
	require.NoError(t, wf.IndexTokenOwner(ctx, addr))
}

// TestIndexTokenOwner_CanceledError_UpdatesJobCanceled is the v1 stand-in for workflow cancellation: an error
// whose message includes "canceled" triggers a canceled job update (legacy: TestIndexTokenOwner_Cancellation_JobMarkedAsCanceled).
func TestIndexTokenOwner_CanceledError_UpdatesJobCanceled(t *testing.T) {
	t.Parallel()
	d := newOwnerWf(t, ownerCfg())
	defer d.Ctrl.Finish()
	ctx := jobs.WithJobID(d.Ctx, 1006)
	exec, wf := d.Exec, d.Wf
	addr := "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	chainID := domain.ChainEthereumMainnet
	latest := uint64(5000)
	canceledLike := errors.New("operation canceled by user")
	exec.EXPECT().CreateIndexingJob(gomock.Any(), addr, chainID, gomock.Any()).Return(nil)
	exec.EXPECT().UpdateIndexingJobStatus(gomock.Any(), gomock.Any(), schema.IndexingJobStatusRunning, gomock.Any()).Return(nil)
	exec.EXPECT().EnsureWatchedAddressExists(gomock.Any(), addr, chainID, gomock.Any()).Return(nil)
	exec.EXPECT().GetIndexingBlockRangeForAddress(gomock.Any(), addr, chainID).Return(&workflows.BlockRangeResult{MinBlock: 0, MaxBlock: 0}, nil)
	exec.EXPECT().GetLatestEthereumBlock(gomock.Any()).Return(latest, nil)
	tok := []domain.TokenWithBlock{{
		TokenCID:    domain.NewTokenCID(chainID, domain.StandardERC721, "0xC00000000000000000000000000000000000001", "1"),
		BlockNumber: 2000,
	}}
	exec.EXPECT().GetEthereumTokenCIDsByOwnerWithinBlockRange(
		gomock.Any(), addr, uint64(1000), latest, gomock.Any(), domain.BlockScanOrderDesc,
	).Return(domain.TokenWithBlockRangeResult{Tokens: tok, EffectiveFromBlock: 1000, EffectiveToBlock: latest}, nil)
	exec.EXPECT().IndexTokenWithMinimalProvenancesByTokenCID(gomock.Any(), gomock.Any(), gomock.Any()).Return(canceledLike)
	exec.EXPECT().UpdateIndexingJobStatus(gomock.Any(), gomock.Any(), schema.IndexingJobStatusCanceled, gomock.Any()).Return(nil)
	err := wf.IndexTokenOwner(ctx, addr)
	require.Error(t, err)
	require.Contains(t, err.Error(), "canceled")
}

// TestIndexTokenOwner_IndexingError_StatusFailedUpdateFails_StillReturnsIndexingError (legacy:
// TestIndexTokenOwner_FailedStatusUpdateAfterChildFailure_WorkflowStillFails).
func TestIndexTokenOwner_IndexingError_StatusFailedUpdateFails_StillReturnsIndexingError(t *testing.T) {
	t.Parallel()
	d := newOwnerWf(t, ownerCfg())
	defer d.Ctrl.Finish()
	ctx := jobs.WithJobID(d.Ctx, 1007)
	exec, wf := d.Exec, d.Wf
	addr := "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	chainID := domain.ChainEthereumMainnet
	latest := uint64(5000)
	inner := errors.New("indexing child failed")
	exec.EXPECT().CreateIndexingJob(gomock.Any(), addr, chainID, gomock.Any()).Return(nil)
	exec.EXPECT().UpdateIndexingJobStatus(gomock.Any(), gomock.Any(), schema.IndexingJobStatusRunning, gomock.Any()).Return(nil)
	exec.EXPECT().EnsureWatchedAddressExists(gomock.Any(), addr, chainID, gomock.Any()).Return(nil)
	exec.EXPECT().GetIndexingBlockRangeForAddress(gomock.Any(), addr, chainID).Return(&workflows.BlockRangeResult{MinBlock: 0, MaxBlock: 0}, nil)
	exec.EXPECT().GetLatestEthereumBlock(gomock.Any()).Return(latest, nil)
	tb := []domain.TokenWithBlock{{
		TokenCID:    domain.NewTokenCID(chainID, domain.StandardERC721, "0xC00000000000000000000000000000000000001", "1"),
		BlockNumber: 2000,
	}}
	exec.EXPECT().GetEthereumTokenCIDsByOwnerWithinBlockRange(
		gomock.Any(), addr, uint64(1000), latest, gomock.Any(), domain.BlockScanOrderDesc,
	).Return(domain.TokenWithBlockRangeResult{Tokens: tb, EffectiveFromBlock: 1000, EffectiveToBlock: latest}, nil)
	exec.EXPECT().IndexTokenWithMinimalProvenancesByTokenCID(gomock.Any(), gomock.Any(), gomock.Any()).Return(inner)
	exec.EXPECT().UpdateIndexingJobStatus(gomock.Any(), gomock.Any(), schema.IndexingJobStatusFailed, gomock.Any()).
		Return(errors.New("db cannot write status"))
	err := wf.IndexTokenOwner(ctx, addr)
	require.Error(t, err)
	require.ErrorIs(t, err, inner)
}

// TestIndexTezosTokenOwner_FirstRun_55Tokens_TwoChunks (legacy: TestIndexTezosTokenOwner_FirstRun_WithTokens chunking portion).
func TestIndexTezosTokenOwner_FirstRun_55Tokens_TwoChunks(t *testing.T) {
	t.Parallel()
	d := newOwnerWf(t, ownerCfg())
	defer d.Ctrl.Finish()
	ctx, exec, wf := d.Ctx, d.Exec, d.Wf
	addr := "tz1FiftyFive"
	chainID := domain.ChainTezosMainnet
	latest := uint64(5000)
	tokens := make([]domain.TokenWithBlock, 55)
	for i := range 55 {
		tokens[i] = domain.TokenWithBlock{
			TokenCID:    domain.NewTokenCID(chainID, domain.StandardFA2, "KT1Fifty5", fmt.Sprintf("%d", i)),
			BlockNumber: uint64(2000 + i*10), //nolint:gosec,G115
		}
	}
	exec.EXPECT().EnsureWatchedAddressExists(gomock.Any(), addr, chainID, gomock.Any()).Return(nil)
	exec.EXPECT().GetIndexingBlockRangeForAddress(gomock.Any(), addr, chainID).Return(&workflows.BlockRangeResult{MinBlock: 0, MaxBlock: 0}, nil)
	exec.EXPECT().GetLatestTezosBlock(gomock.Any()).Return(latest, nil)
	exec.EXPECT().GetTezosTokenCIDsByAccountWithinBlockRange(gomock.Any(), addr, uint64(1000), latest).Return(tokens, nil)
	exec.EXPECT().IndexTokenWithMinimalProvenancesByTokenCID(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(55)
	exec.EXPECT().ResolveTokenMetadata(gomock.Any(), gomock.Any()).Return(nil, nil).Times(55)
	exec.EXPECT().EnhanceTokenMetadata(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).Times(55)
	exec.EXPECT().CheckMediaURLsHealthAndUpdateViewability(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&workflows.MediaHealthCheckResult{IsViewable: false, HealthyURLs: nil}, nil).Times(55)
	exec.EXPECT().IndexTokenWithFullProvenancesByTokenCID(gomock.Any(), gomock.Any()).Return(nil).Times(55)
	exec.EXPECT().UpdateIndexingBlockRangeForAddress(gomock.Any(), addr, chainID, gomock.Any(), gomock.Any()).Return(nil).MinTimes(1)
	require.NoError(t, wf.IndexTezosTokenOwner(ctx, addr, nil))
}

// TestIndexTezosTokenOwner_JobProgressTracking (legacy: same name in older suite).
func TestIndexTezosTokenOwner_JobProgressTracking(t *testing.T) {
	t.Parallel()
	d := newOwnerWf(t, ownerCfg())
	defer d.Ctrl.Finish()
	ctx, exec, wf := d.Ctx, d.Exec, d.Wf
	addr := "tz1Progress"
	chainID := domain.ChainTezosMainnet
	latest := uint64(10000)
	jobID := int64(123)
	tokens := make([]domain.TokenWithBlock, 100)
	for i := range 100 {
		tokens[i] = domain.TokenWithBlock{
			TokenCID:    domain.NewTokenCID(chainID, domain.StandardFA2, "KT1Prog", fmt.Sprintf("%d", i)),
			BlockNumber: uint64(5000 + i*10), //nolint:gosec,G115
		}
	}
	exec.EXPECT().EnsureWatchedAddressExists(gomock.Any(), addr, chainID, gomock.Any()).Return(nil)
	exec.EXPECT().GetIndexingBlockRangeForAddress(gomock.Any(), addr, chainID).Return(&workflows.BlockRangeResult{MinBlock: 0, MaxBlock: 0}, nil)
	exec.EXPECT().GetLatestTezosBlock(gomock.Any()).Return(latest, nil)
	exec.EXPECT().GetTezosTokenCIDsByAccountWithinBlockRange(gomock.Any(), addr, uint64(1000), latest).Return(tokens, nil)
	stubSuccessfulIndexToken(exec)
	exec.EXPECT().UpdateIndexingJobProgress(gomock.Any(), jobID, 50, gomock.Any(), gomock.Any()).Return(nil).Times(1)
	exec.EXPECT().UpdateIndexingJobProgress(gomock.Any(), jobID, 50, gomock.Any(), gomock.Any()).Return(nil).Times(1)
	exec.EXPECT().UpdateIndexingBlockRangeForAddress(gomock.Any(), addr, chainID, gomock.Any(), gomock.Any()).Return(nil).MinTimes(1)
	require.NoError(t, wf.IndexTezosTokenOwner(ctx, addr, &jobID))
}

// TestIndexTezosTokenOwner_JobProgressUpdateFailure_NonFatal (legacy: TestIndexTezosTokenOwner_JobTrackingGracefulFailure).
func TestIndexTezosTokenOwner_JobProgressUpdateFailure_NonFatal(t *testing.T) {
	t.Parallel()
	d := newOwnerWf(t, ownerCfg())
	defer d.Ctrl.Finish()
	ctx, exec, wf := d.Ctx, d.Exec, d.Wf
	addr := "tz1ProgFail"
	chainID := domain.ChainTezosMainnet
	latest := uint64(5000)
	jobID := int64(123)
	tokens := make([]domain.TokenWithBlock, 10)
	for i := range 10 {
		tokens[i] = domain.TokenWithBlock{
			TokenCID:    domain.NewTokenCID(chainID, domain.StandardFA2, "KT1P", fmt.Sprintf("%d", i)),
			BlockNumber: uint64(2000 + i*10), //nolint:gosec,G115
		}
	}
	exec.EXPECT().UpdateIndexingJobProgress(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(errors.New("job tracking error")).AnyTimes()
	exec.EXPECT().EnsureWatchedAddressExists(gomock.Any(), addr, chainID, gomock.Any()).Return(nil)
	exec.EXPECT().GetIndexingBlockRangeForAddress(gomock.Any(), addr, chainID).Return(&workflows.BlockRangeResult{MinBlock: 0, MaxBlock: 0}, nil)
	exec.EXPECT().GetLatestTezosBlock(gomock.Any()).Return(latest, nil)
	exec.EXPECT().GetTezosTokenCIDsByAccountWithinBlockRange(gomock.Any(), addr, uint64(1000), latest).Return(tokens, nil)
	stubSuccessfulIndexToken(exec)
	exec.EXPECT().UpdateIndexingBlockRangeForAddress(gomock.Any(), addr, chainID, gomock.Any(), gomock.Any()).Return(nil).MinTimes(1)
	require.NoError(t, wf.IndexTezosTokenOwner(ctx, addr, &jobID))
}

package workflows_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
)

func newProvenanceWf(t *testing.T) *coreWfDeps {
	t.Helper()
	d := newCoreWfDeps(t, defaultCompactCoreWfConfig(), nil)
	stubJqAnyEnqueue(d.MockJQ)
	return d
}

func TestIndexTokenProvenances_Success(t *testing.T) {
	t.Parallel()
	d := newProvenanceWf(t)
	defer d.Ctrl.Finish()
	ctx, exec, wf := d.Ctx, d.Exec, d.Wf

	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")
	exec.EXPECT().IndexTokenWithFullProvenancesByTokenCID(gomock.Any(), tokenCID).Return(nil)
	err := wf.IndexTokenProvenances(ctx, tokenCID, nil)
	require.NoError(t, err)
}

func TestIndexTokenProvenances_ActivityError(t *testing.T) {
	t.Parallel()
	d := newProvenanceWf(t)
	defer d.Ctrl.Finish()
	ctx, exec, wf := d.Ctx, d.Exec, d.Wf

	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")
	expectedError := errors.New("failed to fetch provenances")
	exec.EXPECT().IndexTokenWithFullProvenancesByTokenCID(gomock.Any(), tokenCID).Return(expectedError)
	err := wf.IndexTokenProvenances(ctx, tokenCID, nil)
	require.ErrorIs(t, err, expectedError)
}

// TestIndexTokenProvenances_EVMSkippedWhenFullProvenanceDisabled pins the credit
// guard: with EthereumFullProvenanceDisabled, an EVM token's full provenance
// indexing is a silent no-op. The executor mock has no expectation, so any
// history replay fails the test.
func TestIndexTokenProvenances_EVMSkippedWhenFullProvenanceDisabled(t *testing.T) {
	t.Parallel()
	cfg := defaultCompactCoreWfConfig()
	cfg.EthereumFullProvenanceDisabled = true
	d := newCoreWfDeps(t, cfg, nil)
	defer d.Ctrl.Finish()
	stubJqAnyEnqueue(d.MockJQ)

	tokenCID := domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721, "0x1234567890123456789012345678901234567890", "1")
	err := d.Wf.IndexTokenProvenances(d.Ctx, tokenCID, nil)
	require.NoError(t, err)
}

// TestIndexTokenProvenances_TezosUnaffectedByEVMGuard pins the guard's scope:
// Tezos provenance is TzKT-backed (no Ethereum RPC credits), so the EVM guard
// must not gate it.
func TestIndexTokenProvenances_TezosUnaffectedByEVMGuard(t *testing.T) {
	t.Parallel()
	cfg := defaultCompactCoreWfConfig()
	cfg.EthereumFullProvenanceDisabled = true
	d := newCoreWfDeps(t, cfg, nil)
	defer d.Ctrl.Finish()
	stubJqAnyEnqueue(d.MockJQ)

	tokenCID := domain.NewTokenCID(domain.ChainTezosMainnet, domain.StandardFA2, "KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton", "1")
	d.Exec.EXPECT().IndexTokenWithFullProvenancesByTokenCID(gomock.Any(), tokenCID).Return(nil)
	err := d.Wf.IndexTokenProvenances(d.Ctx, tokenCID, nil)
	require.NoError(t, err)
}

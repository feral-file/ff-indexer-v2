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

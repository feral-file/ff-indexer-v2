package workflows_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/workflows"
)

func toStrPtr(s string) *string { return &s }

// expectIndexTokenFullSuccess configures MockCoreExecutor for one successful IndexToken (MediaEnabled false in testTokenCore).
func expectIndexTokenFullSuccess(m *mocks.MockCoreExecutor) {
	m.EXPECT().IndexTokenWithMinimalProvenancesByTokenCID(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
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
		ToAddress:       toStrPtr("0xtoaddr"),
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
		FromAddress:     toStrPtr("0xfrom"),
		ToAddress:       toStrPtr("0xto"),
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
		FromAddress:     toStrPtr("0xfrom"),
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

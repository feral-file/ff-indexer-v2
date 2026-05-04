package workflows_test

import (
	"context"
	"testing"

	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/registry"
	"github.com/feral-file/ff-indexer-v2/internal/workflows"
)

// defaultCompactCoreWfConfig is the shared minimal [workflows.CoreWorkflowsConfig] for tests that only exercise
// a narrow slice of [workflows.CoreWorkflows] (e.g. webhooks, provenance entrypoints).
func defaultCompactCoreWfConfig() workflows.CoreWorkflowsConfig {
	return workflows.CoreWorkflowsConfig{
		TezosChainID:                 domain.ChainTezosMainnet,
		EthereumChainID:              domain.ChainEthereumMainnet,
		EthereumTokenSweepStartBlock: 0,
		TezosTokenSweepStartBlock:    0,
		TokenTaskQueue:               "token_index",
		MediaTaskQueue:               "media_index",
	}
}

// coreWfDeps holds gomock-backed dependencies for [workflows.CoreWorkflows] unit tests.
// [newCoreWfDeps] always wires [mocks.MockJobQueue]; use [stubJqAnyEnqueue] when a test does not care about
// enqueues, or register explicit [MockJobQueue.EXPECT] for ordering and counts.
type coreWfDeps struct {
	Ctx  context.Context
	Ctrl *gomock.Controller
	Exec *mocks.MockCoreExecutor
	// BlMock is the concrete gomock when Bl is or wraps [*mocks.MockBlacklistRegistry], else nil.
	BlMock *mocks.MockBlacklistRegistry
	// MockJQ is the job queue passed to [workflows.NewCoreWorkflows]; same instance as the interface field on Wf.
	MockJQ *mocks.MockJobQueue
	Wf     workflows.CoreWorkflows
}

func newCoreWfDeps(t *testing.T, cfg workflows.CoreWorkflowsConfig, bl registry.BlacklistRegistry) *coreWfDeps {
	t.Helper()
	_ = logger.Initialize(logger.Config{Debug: true})
	ctrl := gomock.NewController(t)
	exec := mocks.NewMockCoreExecutor(ctrl)
	var blMock *mocks.MockBlacklistRegistry
	// Do not register a default IsTokenCIDBlacklisted expectation here: token workflow tests
	// set tcid-specific EXPECTs, and a prior gomock.Any() stub would satisfy real calls and
	// leave those specific expectations "missing" at controller finish.
	if bl == nil {
		blm := mocks.NewMockBlacklistRegistry(ctrl)
		bl = blm
		blMock = blm
	} else if bm, ok := bl.(*mocks.MockBlacklistRegistry); ok {
		blMock = bm
	}
	jq := mocks.NewMockJobQueue(ctrl)
	wf := workflows.NewCoreWorkflows(exec, cfg, bl, jq)
	return &coreWfDeps{
		Ctx:    context.Background(),
		Ctrl:   ctrl,
		Exec:   exec,
		BlMock: blMock,
		MockJQ: jq,
		Wf:     wf,
	}
}

// stubJqAnyEnqueue allows all Enqueue calls (typical for tests that do not care about job rows).
func stubJqAnyEnqueue(mjq *mocks.MockJobQueue) {
	mjq.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Return(nil, true, nil).AnyTimes()
}

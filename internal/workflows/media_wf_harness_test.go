//go:build cgo

package workflows_test

import (
	"context"
	"testing"

	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/workflows"
)

// mediaWfDeps groups dependencies for [workflows.MediaWorkflows] tests (shared with [index_media_wf_test]).
type mediaWfDeps struct {
	Ctx    context.Context
	Ctrl   *gomock.Controller
	Exec   *mocks.MockMediaExecutor
	MockJQ *mocks.MockJobQueue
	Mw     workflows.MediaWorkflows
}

func newMediaWf(t *testing.T) *mediaWfDeps {
	t.Helper()
	_ = logger.Initialize(logger.Config{Debug: true})
	ctrl := gomock.NewController(t)
	exec := mocks.NewMockMediaExecutor(ctrl)
	jq := mocks.NewMockJobQueue(ctrl)
	mw := workflows.NewMediaWorkflows(exec, jq)
	return &mediaWfDeps{
		Ctx:    context.Background(),
		Ctrl:   ctrl,
		Exec:   exec,
		MockJQ: jq,
		Mw:     mw,
	}
}

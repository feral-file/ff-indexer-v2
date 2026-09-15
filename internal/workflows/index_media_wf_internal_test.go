//go:build cgo

package workflows

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/providers/jobs"
)

func TestIsExpectedMediaSourceTimeout(t *testing.T) {
	t.Parallel()

	err := fmt.Errorf("failed to process media file: failed to transform and upload image: transformation failed: failed to get response: %w", context.DeadlineExceeded)

	require.True(t, isExpectedMediaSourceTimeout(err))
}

func TestIsExpectedMediaSourceTimeout_ProbePartialGETDeadline(t *testing.T) {
	t.Parallel()

	err := fmt.Errorf("failed to process media file: failed to get content-type via partial GET: %w", context.DeadlineExceeded)

	require.True(t, isExpectedMediaSourceTimeout(err))
}

func TestIsExpectedMediaSourceTimeout_IgnoresOtherDeadlineFailures(t *testing.T) {
	t.Parallel()

	err := fmt.Errorf("failed to check existing media asset: %w", context.DeadlineExceeded)

	require.False(t, isExpectedMediaSourceTimeout(err))
}

func TestIndexMediaWorkflow_PartialGETDeadlineStillFailsJob(t *testing.T) {
	url := "https://example.com/media.jpg"
	expectedErr := fmt.Errorf("failed to process media file: failed to get content-type via partial GET: %w", context.DeadlineExceeded)
	mw := NewMediaWorkflows(
		&stubMediaExecutor{err: expectedErr},
		nil,
		jobs.NopQueue{},
		MediaWorkflowsConfig{MediaTaskQueue: "media_index"},
	)

	err := mw.IndexMediaWorkflow(context.Background(), url)

	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestIndexMediaWorkflow_UnexpectedFailureStillFailsJob(t *testing.T) {
	url := "https://example.com/media.jpg"
	expectedErr := errors.New("database unavailable")
	mw := NewMediaWorkflows(
		&stubMediaExecutor{err: expectedErr},
		nil,
		jobs.NopQueue{},
		MediaWorkflowsConfig{MediaTaskQueue: "media_index"},
	)

	err := mw.IndexMediaWorkflow(context.Background(), url)

	require.ErrorIs(t, err, expectedErr)
}

type stubMediaExecutor struct {
	err error
}

func (e *stubMediaExecutor) IndexMediaFile(context.Context, string) error {
	return e.err
}

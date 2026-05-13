//go:build cgo

package workflows

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/logger"
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

func TestIndexMediaWorkflow_PartialGETDeadlineWarnsWithoutSentryEvent(t *testing.T) {
	transport := initializeSentryTestLogger(t)

	url := "https://example.com/media.jpg"
	expectedErr := fmt.Errorf("failed to process media file: failed to get content-type via partial GET: %w", context.DeadlineExceeded)
	mw := NewMediaWorkflows(
		&stubMediaExecutor{err: expectedErr},
		jobs.NopQueue{},
		MediaWorkflowsConfig{MediaTaskQueue: "media_index"},
	)

	err := mw.IndexMediaWorkflow(context.Background(), url)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	logger.Flush(time.Second)
	require.Empty(t, transport.Events())
}

func TestIndexMediaWorkflow_UnexpectedFailureStillSendsSentryEvent(t *testing.T) {
	transport := initializeSentryTestLogger(t)

	url := "https://example.com/media.jpg"
	expectedErr := errors.New("database unavailable")
	mw := NewMediaWorkflows(
		&stubMediaExecutor{err: expectedErr},
		jobs.NopQueue{},
		MediaWorkflowsConfig{MediaTaskQueue: "media_index"},
	)

	err := mw.IndexMediaWorkflow(context.Background(), url)

	require.ErrorIs(t, err, expectedErr)
	logger.Flush(time.Second)
	require.Len(t, transport.Events(), 1)
}

func initializeSentryTestLogger(t *testing.T) *sentry.MockTransport {
	t.Helper()

	transport := &sentry.MockTransport{}
	client, err := sentry.NewClient(sentry.ClientOptions{
		Dsn:       "https://public@example.com/1",
		Transport: transport,
	})
	require.NoError(t, err)

	require.NoError(t, logger.Initialize(logger.Config{
		Debug:        true,
		SentryDSN:    "https://public@example.com/1",
		SentryClient: client,
	}))

	return transport
}

type stubMediaExecutor struct {
	err error
}

func (e *stubMediaExecutor) IndexMediaFile(context.Context, string) error {
	return e.err
}

//go:build cgo

package workflows_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/providers/jobs"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

// ====================================================================================
// IndexMediaWorkflow Tests
// ====================================================================================

func TestIndexMediaWorkflow_Success(t *testing.T) {
	t.Parallel()
	d := newMediaWf(t)
	defer d.Ctrl.Finish()
	ctx, exec, mw := d.Ctx, d.Exec, d.Mw
	url := "https://example.com/media.jpg"
	exec.EXPECT().IndexMediaFile(gomock.Any(), url).Return(nil).Times(1)
	err := mw.IndexMediaWorkflow(ctx, url)
	require.NoError(t, err)
}

func TestIndexMediaWorkflow_ActivityError(t *testing.T) {
	t.Parallel()
	d := newMediaWf(t)
	defer d.Ctrl.Finish()
	ctx, exec, mw := d.Ctx, d.Exec, d.Mw
	expectedError := errors.New("failed to process media")
	url := "https://example.com/media.jpg"
	exec.EXPECT().IndexMediaFile(gomock.Any(), url).Return(expectedError).Times(1)
	err := mw.IndexMediaWorkflow(ctx, url)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to process media")
}

// ====================================================================================
// IndexMultipleMediaWorkflow Tests
// ====================================================================================

func TestIndexMultipleMediaWorkflow_Success(t *testing.T) {
	t.Parallel()
	d := newMediaWf(t)
	defer d.Ctrl.Finish()
	ctx, _, jq, mw := d.Ctx, d.Exec, d.MockJQ, d.Mw
	urls := []string{
		"https://example.com/media1.jpg",
		"https://example.com/media2.jpg",
		"https://example.com/media3.jpg",
	}

	var n int32
	jq.EXPECT().Enqueue(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, opts jobs.EnqueueOptions) (*schema.Job, bool, error) {
			require.Equal(t, "IndexMediaWorkflow", opts.Kind)
			atomic.AddInt32(&n, 1)
			return nil, true, nil
		}).
		AnyTimes()

	err := mw.IndexMultipleMediaWorkflow(ctx, urls)
	require.NoError(t, err)
	require.Equal(t, int32(3), atomic.LoadInt32(&n), "one enqueue per unique valid URL")
}

func TestIndexMultipleMediaWorkflow_EmptyList(t *testing.T) {
	t.Parallel()
	d := newMediaWf(t)
	defer d.Ctrl.Finish()
	ctx, _, _, mw := d.Ctx, d.Exec, d.MockJQ, d.Mw
	err := mw.IndexMultipleMediaWorkflow(ctx, []string{})
	require.NoError(t, err)
}

func TestIndexMultipleMediaWorkflow_DuplicateURLs(t *testing.T) {
	t.Parallel()
	d := newMediaWf(t)
	defer d.Ctrl.Finish()
	ctx, _, jq, mw := d.Ctx, d.Exec, d.MockJQ, d.Mw
	urls := []string{
		"https://example.com/media1.jpg",
		"https://example.com/media1.jpg",
		"https://example.com/media2.jpg",
		"https://example.com/media2.jpg",
	}

	var n int32
	jq.EXPECT().Enqueue(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ jobs.EnqueueOptions) (*schema.Job, bool, error) {
			atomic.AddInt32(&n, 1)
			return nil, true, nil
		}).
		AnyTimes()

	err := mw.IndexMultipleMediaWorkflow(ctx, urls)
	require.NoError(t, err)
	require.Equal(t, int32(2), atomic.LoadInt32(&n), "deduplicated to two unique URLs")
}

func TestIndexMultipleMediaWorkflow_InvalidURLs(t *testing.T) {
	t.Parallel()
	d := newMediaWf(t)
	defer d.Ctrl.Finish()
	ctx, _, _, mw := d.Ctx, d.Exec, d.MockJQ, d.Mw
	urls := []string{
		"not-a-url",
		"",
		"just-text",
		"https://",
	}

	err := mw.IndexMultipleMediaWorkflow(ctx, urls)
	require.NoError(t, err)
	// no Enqueue: invalid URLs are filtered
}

func TestIndexMultipleMediaWorkflow_MixedValidInvalidURLs(t *testing.T) {
	t.Parallel()
	d := newMediaWf(t)
	defer d.Ctrl.Finish()
	ctx, _, jq, mw := d.Ctx, d.Exec, d.MockJQ, d.Mw
	urls := []string{
		"https://example.com/media1.jpg",
		"not-a-url",
		"https://example.com/media2.jpg",
		"",
		"https://example.com/media3.jpg",
	}

	var n int32
	jq.EXPECT().Enqueue(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ jobs.EnqueueOptions) (*schema.Job, bool, error) {
			atomic.AddInt32(&n, 1)
			return nil, true, nil
		}).
		AnyTimes()

	err := mw.IndexMultipleMediaWorkflow(ctx, urls)
	require.NoError(t, err)
	require.Equal(t, int32(3), atomic.LoadInt32(&n))
}

func TestIndexMultipleMediaWorkflow_DataURI(t *testing.T) {
	t.Parallel()
	d := newMediaWf(t)
	defer d.Ctrl.Finish()
	ctx, _, jq, mw := d.Ctx, d.Exec, d.MockJQ, d.Mw
	urls := []string{
		"data:image/png;base64,iVBORw0KGgo=",
		"not-a-url",
	}

	var seen bool
	jq.EXPECT().Enqueue(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, opts jobs.EnqueueOptions) (*schema.Job, bool, error) {
			require.Len(t, opts.Args, 1)
			require.Equal(t, urls[0], opts.Args[0])
			seen = true
			return nil, true, nil
		}).
		Times(1)

	err := mw.IndexMultipleMediaWorkflow(ctx, urls)
	require.NoError(t, err)
	require.True(t, seen)
}

func TestIndexMultipleMediaWorkflow_EnqueueErrorNonFatal(t *testing.T) {
	t.Parallel()
	d := newMediaWf(t)
	defer d.Ctrl.Finish()
	ctx, _, jq, mw := d.Ctx, d.Exec, d.MockJQ, d.Mw
	urls := []string{
		"https://example.com/media1.jpg",
		"https://example.com/media2.jpg",
		"https://example.com/media3.jpg",
	}

	first := true
	jq.EXPECT().Enqueue(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ jobs.EnqueueOptions) (*schema.Job, bool, error) {
			if first {
				first = false
				return nil, false, errors.New("enqueue failed")
			}
			return nil, true, nil
		}).
		AnyTimes()

	err := mw.IndexMultipleMediaWorkflow(ctx, urls)
	require.NoError(t, err, "per-URL enqueue failure is non-fatal (warn + continue)")
}

func TestIndexMultipleMediaWorkflow_ChildExecutionNotObserved(t *testing.T) {
	t.Parallel()
	// v1 only enqueues media jobs; actual IndexMediaFile runs in a worker. A downstream failure
	// in that worker is not propagated to this workflow. This test kept as a no-op success path
	// when all enqueues succeed.
	d := newMediaWf(t)
	defer d.Ctrl.Finish()
	ctx, _, jq, mw := d.Ctx, d.Exec, d.MockJQ, d.Mw
	urls := []string{
		"https://example.com/media1.jpg",
		"https://example.com/media2.jpg",
	}
	jq.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Return(nil, true, nil).Times(2)
	err := mw.IndexMultipleMediaWorkflow(ctx, urls)
	require.NoError(t, err)
}

func TestIndexMultipleMediaWorkflow_SingleURLOneEnqueue(t *testing.T) {
	t.Parallel()
	d := newMediaWf(t)
	defer d.Ctrl.Finish()
	ctx, _, jq, mw := d.Ctx, d.Exec, d.MockJQ, d.Mw
	url := "https://example.com/media1.jpg"
	urls := []string{url}

	jq.EXPECT().Enqueue(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, opts jobs.EnqueueOptions) (*schema.Job, bool, error) {
			require.Equal(t, "IndexMediaWorkflow", opts.Kind)
			return nil, true, nil
		}).
		Times(1)

	err := mw.IndexMultipleMediaWorkflow(ctx, urls)
	require.NoError(t, err)
}

func TestIndexMultipleMediaWorkflow_RepeatInvocationSameURL(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	urls := []string{"https://example.com/media1.jpg"}
	for range 2 {
		d := newMediaWf(t)
		d.MockJQ.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Return(nil, true, nil).Times(1)
		err := d.Mw.IndexMultipleMediaWorkflow(ctx, urls)
		require.NoError(t, err)
		d.Ctrl.Finish()
	}
}

func TestIndexMultipleMediaWorkflow_LargeNumberOfURLs(t *testing.T) {
	t.Parallel()
	d := newMediaWf(t)
	defer d.Ctrl.Finish()
	ctx, _, jq, mw := d.Ctx, d.Exec, d.MockJQ, d.Mw
	urls := make([]string, 50)
	for i := range 50 {
		urls[i] = "https://example.com/media" + string(rune('0'+i%10)) + ".jpg"
	}

	jq.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Return(nil, true, nil).MinTimes(1)

	err := mw.IndexMultipleMediaWorkflow(ctx, urls)
	require.NoError(t, err)
}

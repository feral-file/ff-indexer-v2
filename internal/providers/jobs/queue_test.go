package jobs_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/providers/jobs"
	"github.com/feral-file/ff-indexer-v2/internal/store"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

func TestNewJobQueue_Panics(t *testing.T) {
	t.Parallel()
	require.Panics(t, func() { jobs.NewJobQueue(nil, nil) })
	s := mocks.NewMockStore(gomock.NewController(t))
	require.Panics(t, func() { jobs.NewJobQueue(s, nil) })
}

func TestJobQueue_Enqueue_Validation(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	st := mocks.NewMockStore(ctrl)
	j := mocks.NewMockJSON(ctrl)
	q := jobs.NewJobQueue(st, j)
	_, _, err := q.Enqueue(context.Background(), jobs.EnqueueOptions{Kind: "k"})
	require.Error(t, err)
	_, _, err = q.Enqueue(context.Background(), jobs.EnqueueOptions{Queue: "q"})
	require.Error(t, err)
}

func TestJobQueue_Enqueue_MarshalError(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	st := mocks.NewMockStore(ctrl)
	j := mocks.NewMockJSON(ctrl)
	mErr := errors.New("marshal boom")
	j.EXPECT().Marshal(gomock.Any()).Return(nil, mErr)
	st.EXPECT().EnqueueJob(gomock.Any(), gomock.Any()).Times(0)

	q := jobs.NewJobQueue(st, j)
	_, _, err := q.Enqueue(context.Background(), jobs.EnqueueOptions{Queue: "q1", Kind: "K1", Args: []any{1}})
	require.ErrorIs(t, err, mErr)
	require.ErrorContains(t, err, "marshal args")
}

func TestJobQueue_Enqueue_NilArgsBecomesEmptyArray(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	st := mocks.NewMockStore(ctrl)
	j := mocks.NewMockJSON(ctrl)
	ctx := context.Background()
	wantPayload := []byte("[]")
	j.EXPECT().Marshal(gomock.AssignableToTypeOf([]any{})).DoAndReturn(func(v any) ([]byte, error) {
		a := v.([]any)
		require.Empty(t, a)
		return wantPayload, nil
	})
	st.EXPECT().EnqueueJob(ctx, gomock.AssignableToTypeOf(store.EnqueueJobInput{})).DoAndReturn(
		func(_ context.Context, in store.EnqueueJobInput) (*schema.Job, bool, error) {
			require.Equal(t, wantPayload, []byte(in.Payload))
			return &schema.Job{ID: 1, Queue: "q", Kind: "K", Payload: in.Payload}, true, nil
		},
	)
	q := jobs.NewJobQueue(st, j)
	job, created, err := q.Enqueue(ctx, jobs.EnqueueOptions{Queue: "q", Kind: "K", Args: nil})
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, int64(1), job.ID)
}

func TestJobQueue_Enqueue_PassesUniqueKeyAndRunAfter(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	st := mocks.NewMockStore(ctrl)
	j := mocks.NewMockJSON(ctrl)
	ctx := context.Background()
	ra := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	uk := "dedupe-key"
	j.EXPECT().Marshal(gomock.Any()).Return([]byte(`[1,2,3]`), nil)
	st.EXPECT().EnqueueJob(ctx, gomock.Any()).DoAndReturn(
		func(_ context.Context, in store.EnqueueJobInput) (*schema.Job, bool, error) {
			require.Equal(t, &uk, in.UniqueKey)
			require.Equal(t, &ra, in.RunAfter)
			return &schema.Job{ID: 2}, true, nil
		},
	)
	q := jobs.NewJobQueue(st, j)
	_, created, err := q.Enqueue(ctx, jobs.EnqueueOptions{
		Queue: "q", Kind: "K", Args: []any{1, 2, 3},
		UniqueKey: &uk, RunAfter: &ra,
	})
	require.NoError(t, err)
	require.True(t, created)
}

func TestJobQueue_GetStatus(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	st := mocks.NewMockStore(ctrl)
	j := mocks.NewMockJSON(ctrl)
	ctx := context.Background()
	want := &schema.Job{ID: 3, Status: schema.JobStatusPending}
	st.EXPECT().GetJob(ctx, int64(3)).Return(want, nil)
	q := jobs.NewJobQueue(st, j)
	jb, err := q.GetStatus(ctx, 3)
	require.NoError(t, err)
	require.Equal(t, want, jb)
}

func TestJobQueue_Cancel(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	st := mocks.NewMockStore(ctrl)
	j := mocks.NewMockJSON(ctrl)
	ctx := context.Background()
	st.EXPECT().RequestJobCancel(ctx, int64(9)).Return(nil)
	q := jobs.NewJobQueue(st, j)
	require.NoError(t, q.Cancel(ctx, 9))
}

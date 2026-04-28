package jobs_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/providers/jobs"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

func newTestRegistry(t *testing.T) *jobs.Registry {
	t.Helper()
	return jobs.NewRegistry(adapter.NewJSON())
}

func TestNewRegistry_PanicsOnNilJSON(t *testing.T) {
	t.Parallel()
	require.Panics(t, func() { jobs.NewRegistry(nil) })
}

func TestRegistry_Register_Panics(t *testing.T) {
	t.Parallel()
	r := newTestRegistry(t)

	cases := []struct {
		name string
		fn   func()
	}{
		{
			"empty kind",
			func() { r.Register("", func(context.Context) error { return nil }) },
		},
		{
			"not a function",
			func() { r.Register("a", 42) },
		},
		{
			"no context",
			func() { r.Register("a", func() error { return nil }) },
		},
		{
			"no error return",
			func() { r.Register("a", func(context.Context) {}) },
		},
		{
			"two returns",
			func() { r.Register("a", func(context.Context) (int, error) { return 0, nil }) },
		},
		{
			"duplicate",
			func() {
				s := jobs.NewRegistry(adapter.NewJSON())
				s.Register("x", func(context.Context) error { return nil })
				s.Register("x", func(context.Context) error { return nil })
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Panics(t, c.fn)
		})
	}
}

func TestRegistry_Dispatch_NilJob(t *testing.T) {
	t.Parallel()
	r := newTestRegistry(t)
	require.Error(t, r.Dispatch(context.Background(), nil))
}

func TestRegistry_Dispatch_UnknownKind(t *testing.T) {
	t.Parallel()
	r := newTestRegistry(t)
	j := &schema.Job{Kind: "missing", Payload: []byte("[]")}
	require.Error(t, r.Dispatch(context.Background(), j))
}

func TestRegistry_Dispatch_NilContextFillsBackground(t *testing.T) {
	t.Parallel()
	r := newTestRegistry(t)
	var got context.Context
	r.Register("C", func(ctx context.Context) error {
		got = ctx
		return nil
	})
	require.NoError(t, r.Dispatch(nil, &schema.Job{Kind: "C", Payload: []byte("[]")})) //nolint:staticcheck // nil ctx exercise
	require.NotNil(t, got)
}

func TestRegistry_Dispatch_ZeroArg(t *testing.T) {
	t.Parallel()
	r := newTestRegistry(t)
	called := false
	r.Register("N", func(ctx context.Context) error {
		called = true
		return nil
	})
	require.NoError(t, r.Dispatch(context.Background(), &schema.Job{Kind: "N", Payload: nil}))
	require.NoError(t, r.Dispatch(context.Background(), &schema.Job{Kind: "N", Payload: []byte("   ")}))
	require.True(t, called)
}

func TestRegistry_Dispatch_ZeroArg_RejectsNonEmptyArray(t *testing.T) {
	t.Parallel()
	r := newTestRegistry(t)
	r.Register("N", func(ctx context.Context) error { return nil })
	err := r.Dispatch(context.Background(), &schema.Job{Kind: "N", Payload: []byte(`[1]`)})
	require.Error(t, err)
	require.Contains(t, err.Error(), "0 args")
}

func TestRegistry_Dispatch_IntArg(t *testing.T) {
	t.Parallel()
	r := newTestRegistry(t)
	var got int
	r.Register("I", func(_ context.Context, n int) error {
		got = n
		return nil
	})
	err := r.Dispatch(context.Background(), &schema.Job{Kind: "I", Payload: []byte(`[42]`)})
	require.NoError(t, err)
	require.Equal(t, 42, got)
}

func TestRegistry_Dispatch_PointerArgNull(t *testing.T) {
	t.Parallel()
	r := newTestRegistry(t)
	var got *int
	r.Register("P", func(_ context.Context, p *int) error {
		got = p
		return nil
	})
	err := r.Dispatch(context.Background(), &schema.Job{Kind: "P", Payload: []byte(`[null]`)})
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestRegistry_Dispatch_ArgCountMismatch(t *testing.T) {
	t.Parallel()
	r := newTestRegistry(t)
	r.Register("T", func(_ context.Context, a, b int) error { return nil })
	err := r.Dispatch(context.Background(), &schema.Job{Kind: "T", Payload: []byte(`[1]`)})
	require.Error(t, err)
	require.Contains(t, err.Error(), "arg count")
}

func TestRegistry_Dispatch_HandlerError(t *testing.T) {
	t.Parallel()
	r := newTestRegistry(t)
	r.Register("E", func(context.Context) error { return handlerFailureError{s: "handler failed"} })
	err := r.Dispatch(context.Background(), &schema.Job{Kind: "E", Payload: []byte("[]")})
	require.EqualError(t, err, "handler failed")
}

func TestRegistry_Dispatch_HandlerPanic_ReturnsError(t *testing.T) {
	t.Parallel()
	r := newTestRegistry(t)
	r.Register("panic", func(context.Context) error { panic("boom") })
	err := r.Dispatch(context.Background(), &schema.Job{Kind: "panic", Payload: []byte("[]")})
	require.Error(t, err)
	require.Contains(t, err.Error(), "handler panic")
	require.Contains(t, err.Error(), "boom")
}

type handlerFailureError struct{ s string }

func (e handlerFailureError) Error() string { return e.s }

func TestRegistry_Dispatch_PayloadNotJSONArray(t *testing.T) {
	t.Parallel()
	r := newTestRegistry(t)
	r.Register("A", func(_ context.Context, _ int) error { return nil })
	err := r.Dispatch(context.Background(), &schema.Job{Kind: "A", Payload: []byte(`{`)})
	require.Error(t, err)
}

func TestRegistry_Dispatch_Reschedule(t *testing.T) {
	t.Parallel()
	r := newTestRegistry(t)
	at := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	r.Register("R", func(context.Context) error { return jobs.ErrReschedule(at) })
	err := r.Dispatch(context.Background(), &schema.Job{Kind: "R", Payload: []byte("[]")})
	var re *jobs.RescheduleError
	require.ErrorAs(t, err, &re)
}

func TestRegistry_Dispatch_ArgDecodeError(t *testing.T) {
	t.Parallel()
	r := newTestRegistry(t)
	r.Register("I2", func(_ context.Context, n int) error { return nil })
	// "x" is not a JSON number
	err := r.Dispatch(context.Background(), &schema.Job{Kind: "I2", Payload: []byte(`["nope"]`)})
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "arg 0:"), err.Error())
}

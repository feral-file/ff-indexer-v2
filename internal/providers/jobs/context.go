package jobs

import "context"

type jobIDKey struct{}

// WithJobID returns a child context that carries the jobs table primary key for the
// current handler invocation. The worker sets this for each dispatch.
func WithJobID(ctx context.Context, id int64) context.Context {
	return context.WithValue(ctx, jobIDKey{}, id)
}

// JobIDFromContext returns the job id and whether it was set.
func JobIDFromContext(ctx context.Context) (int64, bool) {
	v, ok := ctx.Value(jobIDKey{}).(int64)
	return v, ok
}

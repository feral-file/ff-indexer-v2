package logger

import (
	"context"

	"go.uber.org/zap"
)

type fieldsKey struct{}

// WithFields returns a context whose structured fields are included by FromContext.
func WithFields(ctx context.Context, fields ...zap.Field) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	existing, _ := ctx.Value(fieldsKey{}).([]zap.Field)
	combined := make([]zap.Field, 0, len(existing)+len(fields))
	combined = append(combined, existing...)
	combined = append(combined, fields...)
	return context.WithValue(ctx, fieldsKey{}, combined)
}

// ContextWithJob returns a context carrying stable job fields for every handler log.
//
// Reason: job identity must be first-class structured data in both stdout and
// Cloudflare logs rather than metadata held only by an external error reporter.
// Trade-offs: repeated explicit fields at a call site can create duplicate JSON keys;
// callers should rely on these context fields for job identity. Constraints: attach
// this context before dispatch and retain it while persisting the final outcome.
func ContextWithJob(ctx context.Context, jobID int64, jobKind, queue string) context.Context {
	return WithFields(ctx,
		zap.Int64("job_id", jobID),
		zap.String("job_kind", jobKind),
		zap.String("job_queue", queue),
	)
}

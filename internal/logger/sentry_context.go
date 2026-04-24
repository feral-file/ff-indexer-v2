package logger

import (
	"context"
	"fmt"

	"github.com/getsentry/sentry-go"
)

// ContextWithSentryHub returns ctx with a fresh Sentry hub (clone of the current hub).
// Use this at the start of a unit of work so breadcrumbs and errors are isolated.
func ContextWithSentryHub(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	hub := sentry.CurrentHub().Clone()
	return sentry.SetHubOnContext(ctx, hub)
}

// ContextWithSentryJobHandler returns a context with an isolated Sentry hub and job-scoped
// tags and context, for postgres job workers (per-job / per-handler scope).
// It must be used before job dispatch so handlers and logging see only this job in Sentry.
func ContextWithSentryJobHandler(ctx context.Context, jobID int64, jobKind, queue string) context.Context {
	ctx = ContextWithSentryHub(ctx)
	hub := sentry.GetHubFromContext(ctx)
	if hub == nil {
		return ctx
	}
	sid := fmt.Sprintf("%d", jobID)
	hub.Scope().SetTag("job_id", sid)
	hub.Scope().SetTag("job_kind", jobKind)
	hub.Scope().SetTag("job_queue", queue)
	hub.Scope().SetContext("indexer_job", map[string]any{
		"job_id": jobID,
		"kind":   jobKind,
		"queue":  queue,
	})
	return ctx
}

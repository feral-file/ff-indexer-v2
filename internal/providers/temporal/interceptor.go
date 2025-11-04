package temporal

import (
	"context"

	"github.com/getsentry/sentry-go"
	"go.temporal.io/sdk/interceptor"
)

// NewSentryActivityInterceptor creates a new Sentry activity interceptor
func NewSentryActivityInterceptor() interceptor.WorkerInterceptor {
	return &SentryActivityInterceptor{
		WorkerInterceptorBase: interceptor.WorkerInterceptorBase{},
	}
}

// SentryActivityInterceptor injects Sentry hub into activity context
// This enables Sentry tracking for Temporal activities
type SentryActivityInterceptor struct {
	interceptor.WorkerInterceptorBase
}

// InterceptActivity wraps activity execution to inject Sentry hub
func (s *SentryActivityInterceptor) InterceptActivity(ctx context.Context, next interceptor.ActivityInboundInterceptor) interceptor.ActivityInboundInterceptor {
	return &sentryActivityInboundInterceptor{
		ActivityInboundInterceptorBase: interceptor.ActivityInboundInterceptorBase{
			Next: next,
		},
	}
}

type sentryActivityInboundInterceptor struct {
	interceptor.ActivityInboundInterceptorBase
}

// ExecuteActivity injects Sentry hub into activity context before execution
func (s *sentryActivityInboundInterceptor) ExecuteActivity(ctx context.Context, in *interceptor.ExecuteActivityInput) (interface{}, error) {
	// Create a new Sentry hub for this activity execution
	hub := sentry.CurrentHub().Clone()

	// Attach hub to context for Sentry tracking
	// This enables context-aware logging in activities (logger.InfoCtx, logger.ErrorCtx, etc.)
	ctx = sentry.SetHubOnContext(ctx, hub)

	// Execute activity with Sentry context using the base implementation
	return s.Next.ExecuteActivity(ctx, in)
}

package workflows

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/providers/jobs"
	"github.com/feral-file/ff-indexer-v2/internal/ratelimit"
	"github.com/feral-file/ff-indexer-v2/internal/types"
)

const (
	// indexTokenMetadataKind is the job kind IndexTokenMetadata is registered under.
	indexTokenMetadataKind = "IndexTokenMetadata"

	// metadataDeferralBaseDelay and metadataDeferralJitter space a deferred run
	// 1–3 minutes out: long enough for a provider queue to drain at its configured
	// rate, and jittered so a deferred batch does not return as one burst.
	metadataDeferralBaseDelay = time.Minute
	metadataDeferralJitter    = 2 * time.Minute

	// metadataDeferralMaxAge bounds how long one IndexTokenMetadata job keeps
	// rescheduling itself. Past it the job runs the non-deferred path, so a
	// permanently saturated provider degrades to today's behavior instead of
	// looping forever. RescheduleJob resets attempt_count, so job age is the
	// only usable bound.
	metadataDeferralMaxAge = 30 * time.Minute
)

// isSelfThrottled reports whether err means our own outbound rate limiter refused to
// send in time. The remote was never contacted, so the failure says nothing about the
// token and a later attempt is expected to succeed.
func isSelfThrottled(err error) bool {
	return errors.Is(err, adapter.ErrRateLimitWait) || errors.Is(err, ratelimit.ErrQueueTimeout)
}

// metadataDeferral returns a *jobs.RescheduleError when a self-throttled metadata
// attempt should be retried later instead of being completed with missing data, or
// nil when the caller should continue on the non-deferred path.
//
// Reason: a bulk trigger (release, owner, or API batch) can queue more requests for
// one provider than its max_queue_time admits. Before this, those tokens were indexed
// without metadata or enrichment, the job still succeeded, and nothing retried.
//
// Two callers consume the error differently:
//   - IndexTokenMetadata as its own job: the worker maps the error to RescheduleJob,
//     moving the same job back to pending (an Enqueue would only return the running
//     job, whose unique key is still active).
//   - IndexToken running it inline: IndexToken enqueues a fresh IndexTokenMetadata job
//     with that run_after and carries on with provenance (see deferMetadataJob).
//
// Constraints: only the IndexTokenMetadata job's own age is bounded, because inline
// callers can run inside long owner-indexing jobs; the fresh job they enqueue starts
// its own age. Without a readable job (no job id in ctx, or a failed lookup) the
// attempt is not deferred, so the bound can never be bypassed.
func (w *coreWorkflows) metadataDeferral(ctx context.Context, tokenCID domain.TokenCID, cause error) error {
	if !isSelfThrottled(cause) {
		return nil
	}

	jobID, ok := jobs.JobIDFromContext(ctx)
	if !ok {
		return nil
	}
	job, err := w.jobQueue.GetStatus(ctx, jobID)
	if err != nil || job == nil {
		logger.WarnCtx(ctx, "Cannot read current job; not deferring self-throttled metadata attempt",
			zap.String("tokenCID", tokenCID.String()), zap.Error(err))
		return nil
	}
	if job.Kind == indexTokenMetadataKind && time.Since(job.CreatedAt) > metadataDeferralMaxAge {
		logger.WarnCtx(ctx, "Metadata deferral window exhausted; continuing without deferral",
			zap.String("tokenCID", tokenCID.String()), zap.Duration("max_age", metadataDeferralMaxAge))
		return nil
	}

	at := time.Now().UTC().Add(metadataDeferralBaseDelay + rand.N(metadataDeferralJitter)) //nolint:gosec // G404: scheduling jitter, not security-sensitive
	logger.InfoCtx(ctx, "Deferring self-throttled metadata indexing",
		zap.String("tokenCID", tokenCID.String()), zap.Time("run_after", at), zap.Error(cause))
	return jobs.ErrReschedule(at)
}

// deferMetadataJob enqueues IndexTokenMetadata for tokenCID at re.At. IndexToken calls
// it when its inline metadata step deferred, so the token's metadata, enrichment,
// viewability, and webhooks are produced by the deferred job.
//
// The unique key matches startIndexTokenMetadataAsync: if a metadata job for the token
// is already pending or running, Enqueue returns it and no duplicate is created.
// An enqueue failure is logged rather than failing IndexToken, matching the other
// fire-and-forget children; the token then keeps whatever metadata it already had.
func (w *coreWorkflows) deferMetadataJob(ctx context.Context, tokenCID domain.TokenCID, address *string, re *jobs.RescheduleError) {
	_, _, err := w.jobQueue.Enqueue(ctx, jobs.EnqueueOptions{
		Queue:     w.config.TokenTaskQueue,
		Kind:      indexTokenMetadataKind,
		Args:      []any{tokenCID, address},
		UniqueKey: metadataUniqueKey(tokenCID),
		RunAfter:  &re.At,
	})
	if err != nil {
		logger.WarnCtx(ctx, "Failed to enqueue deferred IndexTokenMetadata",
			zap.String("tokenCID", tokenCID.String()), zap.Error(err))
	}
}

// metadataUniqueKey is the dedupe key shared by every IndexTokenMetadata enqueue, so a
// deferred job, an ingestion-triggered job, and an API refresh for the same token
// collapse into one active job.
func metadataUniqueKey(tokenCID domain.TokenCID) *string {
	return types.StringPtr("index-metadata-" + tokenCID.String())
}

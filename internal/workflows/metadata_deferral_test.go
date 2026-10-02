package workflows_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/metadata"
	"github.com/feral-file/ff-indexer-v2/internal/providers/jobs"
	"github.com/feral-file/ff-indexer-v2/internal/ratelimit"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
	"github.com/feral-file/ff-indexer-v2/internal/workflows"
)

var deferralCID = domain.NewTokenCID(domain.ChainEthereumMainnet, domain.StandardERC721,
	"0xa7d8d9ef8D8Ce8992Df33D8b8CF4Aebabd5bD270", "13000129")

// throttledFetchErr reproduces the production error chain of a tokenURI fetch our own
// limiter refused (executor → resolver → HTTP client → limiter).
func throttledFetchErr() error {
	limiter := fmt.Errorf("%w: token.artblocks.io: %w", adapter.ErrRateLimitWait,
		fmt.Errorf("%w: provider artblocks after 2m0s", ratelimit.ErrQueueTimeout))
	return fmt.Errorf("failed to resolve token metadata: failed to fetch metadata from URI x: failed to fetch URL: request failed after retries: %w", limiter)
}

// inJob returns a context carrying job id 7 and wires GetStatus to report a job of
// the given kind created age ago.
func inJob(f *coreWfDeps, kind string, age time.Duration) context.Context {
	f.MockJQ.EXPECT().GetStatus(gomock.Any(), int64(7)).
		Return(&schema.Job{ID: 7, Kind: kind, CreatedAt: time.Now().Add(-age)}, nil)
	return jobs.WithJobID(f.Ctx, 7)
}

// requireRescheduledWithin asserts err is a reschedule 1–3 minutes out.
func requireRescheduledWithin(t *testing.T, err error) {
	t.Helper()
	var re *jobs.RescheduleError
	require.ErrorAs(t, err, &re)
	delay := time.Until(re.At)
	assert.GreaterOrEqual(t, delay, 59*time.Second)
	assert.LessOrEqual(t, delay, 3*time.Minute)
}

// A self-throttled fetch inside an IndexTokenMetadata job reschedules that job and
// stops before enrichment, viewability, and webhooks: a delayed token must not be
// announced as unviewable.
func TestIndexTokenMetadata_SelfThrottledFetch_ReschedulesBeforeSideEffects(t *testing.T) {
	t.Parallel()
	f := newMetadataWf(t, true)
	defer f.Ctrl.Finish()
	ctx := inJob(f, "IndexTokenMetadata", time.Minute)

	f.Exec.EXPECT().ResolveTokenMetadata(gomock.Any(), deferralCID).Return(nil, throttledFetchErr())
	f.Exec.EXPECT().EnhanceTokenMetadata(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	f.Exec.EXPECT().CheckMediaURLsHealthAndUpdateViewability(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	f.MockJQ.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Times(0) // no webhook, no media job

	requireRescheduledWithin(t, f.Wf.IndexTokenMetadata(ctx, deferralCID, nil))
}

// A vendor call refused by ratelimit.Do (ErrQueueTimeout without ErrRateLimitWait) is
// also self-throttling and defers the same way.
func TestIndexTokenMetadata_SelfThrottledEnrichment_Reschedules(t *testing.T) {
	t.Parallel()
	f := newMetadataWf(t, true)
	defer f.Ctrl.Finish()
	ctx := inJob(f, "IndexTokenMetadata", time.Minute)

	normalized := &metadata.NormalizedMetadata{Image: "https://example.com/i.png"}
	f.Exec.EXPECT().ResolveTokenMetadata(gomock.Any(), deferralCID).Return(normalized, nil)
	f.Exec.EXPECT().EnhanceTokenMetadata(gomock.Any(), deferralCID, normalized).
		Return(nil, fmt.Errorf("failed to enhance metadata: %w", ratelimit.ErrQueueTimeout))
	f.Exec.EXPECT().CheckMediaURLsHealthAndUpdateViewability(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	f.MockJQ.EXPECT().Enqueue(gomock.Any(), gomock.Any()).Times(0)

	requireRescheduledWithin(t, f.Wf.IndexTokenMetadata(ctx, deferralCID, nil))
}

// Past the deferral window the job stops rescheduling and runs the existing path, so
// a permanently saturated provider cannot loop a job forever.
func TestIndexTokenMetadata_SelfThrottled_PastMaxAge_ContinuesWithoutDeferral(t *testing.T) {
	t.Parallel()
	f := newMetadataWf(t, true)
	defer f.Ctrl.Finish()
	stubJqAnyEnqueue(f.MockJQ)
	ctx := inJob(f, "IndexTokenMetadata", 31*time.Minute)

	f.Exec.EXPECT().ResolveTokenMetadata(gomock.Any(), deferralCID).Return(nil, throttledFetchErr())
	f.Exec.EXPECT().EnhanceTokenMetadata(gomock.Any(), deferralCID, (*metadata.NormalizedMetadata)(nil)).Return(nil, nil)
	f.Exec.EXPECT().CheckMediaURLsHealthAndUpdateViewability(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&workflows.MediaHealthCheckResult{}, nil)

	require.NoError(t, f.Wf.IndexTokenMetadata(ctx, deferralCID, nil))
}

// Only the IndexTokenMetadata job's own age is bounded: inline callers can run inside
// long owner-indexing jobs, and the job they hand off to starts its own age.
func TestIndexTokenMetadata_SelfThrottled_InsideOldParentJob_StillDefers(t *testing.T) {
	t.Parallel()
	f := newMetadataWf(t, true)
	defer f.Ctrl.Finish()
	ctx := inJob(f, "IndexTokenOwner", 6*time.Hour)

	f.Exec.EXPECT().ResolveTokenMetadata(gomock.Any(), deferralCID).Return(nil, throttledFetchErr())

	requireRescheduledWithin(t, f.Wf.IndexTokenMetadata(ctx, deferralCID, nil))
}

// Without a readable job the attempt is not deferred, so the age bound can never be
// bypassed (e.g. a failed status lookup).
func TestIndexTokenMetadata_SelfThrottled_JobLookupFails_ContinuesWithoutDeferral(t *testing.T) {
	t.Parallel()
	f := newMetadataWf(t, true)
	defer f.Ctrl.Finish()
	stubJqAnyEnqueue(f.MockJQ)
	f.MockJQ.EXPECT().GetStatus(gomock.Any(), int64(7)).Return(nil, errors.New("db down"))
	ctx := jobs.WithJobID(f.Ctx, 7)

	f.Exec.EXPECT().ResolveTokenMetadata(gomock.Any(), deferralCID).Return(nil, throttledFetchErr())
	f.Exec.EXPECT().EnhanceTokenMetadata(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil)
	f.Exec.EXPECT().CheckMediaURLsHealthAndUpdateViewability(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&workflows.MediaHealthCheckResult{}, nil)

	require.NoError(t, f.Wf.IndexTokenMetadata(ctx, deferralCID, nil))
}

// Ordinary fetch failures keep the existing non-fatal path and never consult the job.
func TestIndexTokenMetadata_NonThrottleFetchError_NotDeferred(t *testing.T) {
	t.Parallel()
	f := newMetadataWf(t, true)
	defer f.Ctrl.Finish()
	stubJqAnyEnqueue(f.MockJQ)
	f.MockJQ.EXPECT().GetStatus(gomock.Any(), gomock.Any()).Times(0)
	ctx := jobs.WithJobID(f.Ctx, 7)

	f.Exec.EXPECT().ResolveTokenMetadata(gomock.Any(), deferralCID).Return(nil, errors.New("unexpected status code 404"))
	f.Exec.EXPECT().EnhanceTokenMetadata(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil)
	f.Exec.EXPECT().CheckMediaURLsHealthAndUpdateViewability(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&workflows.MediaHealthCheckResult{}, nil)

	require.NoError(t, f.Wf.IndexTokenMetadata(ctx, deferralCID, nil))
}

// Inline in IndexToken (release / owner / API batches), a deferred metadata step is
// handed to its own delayed IndexTokenMetadata job with the shared unique key, and
// IndexToken still completes provenance and succeeds.
func TestIndexToken_SelfThrottledMetadata_EnqueuesDeferredJobAndContinues(t *testing.T) {
	t.Parallel()
	// Built directly: testTokenCore pre-stubs Enqueue, which would shadow the capture.
	d := newCoreWfDeps(t, workflows.CoreWorkflowsConfig{
		EthereumChainID: domain.ChainEthereumMainnet,
		TezosChainID:    domain.ChainTezosMainnet,
		TokenTaskQueue:  "token_index",
		MediaTaskQueue:  "media_index",
	}, nil)
	defer d.Ctrl.Finish()
	ctx := inJob(d, "IndexTokens", time.Minute)

	d.BlMock.EXPECT().IsTokenCIDBlacklisted(deferralCID).Return(false)
	d.Exec.EXPECT().IndexTokenWithMinimalProvenancesByTokenCID(gomock.Any(), deferralCID, gomock.Any()).Return(nil)
	d.Exec.EXPECT().ResolveTokenMetadata(gomock.Any(), deferralCID).Return(nil, throttledFetchErr())
	d.Exec.EXPECT().SupportsTokenProvenance(gomock.Any()).Return(true)
	d.Exec.EXPECT().IndexTokenWithFullProvenancesByTokenCID(gomock.Any(), deferralCID).Return(nil)

	var deferred *jobs.EnqueueOptions
	d.MockJQ.EXPECT().Enqueue(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, o jobs.EnqueueOptions) (*schema.Job, bool, error) {
			if o.Kind == "IndexTokenMetadata" {
				deferred = &o
			}
			return &schema.Job{}, true, nil
		}).AnyTimes()

	require.NoError(t, d.Wf.IndexToken(ctx, deferralCID, nil))

	require.NotNil(t, deferred, "deferred IndexTokenMetadata job must be enqueued")
	require.NotNil(t, deferred.RunAfter)
	delay := time.Until(*deferred.RunAfter)
	assert.GreaterOrEqual(t, delay, 59*time.Second)
	assert.LessOrEqual(t, delay, 3*time.Minute)
	require.NotNil(t, deferred.UniqueKey)
	assert.Equal(t, "index-metadata-"+deferralCID.String(), *deferred.UniqueKey)
	assert.Equal(t, []any{deferralCID, (*string)(nil)}, deferred.Args)
}

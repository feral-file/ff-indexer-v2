package tezos_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/providers/tezos"
	"github.com/feral-file/ff-indexer-v2/internal/ratelimit"
)

// expectLimited pins the provider bucket: the fetcher's request must be charged to the
// shared "tzkt" limiter, then executed. Unthrottled head fetches were part of the
// traffic mix behind the 2026-08-18 TzKT 429 crash-loop.
func expectLimited(limiter *mocks.MockLimiter) *gomock.Call {
	return limiter.EXPECT().
		Do(gomock.Any(), "tzkt", gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ string, fn ratelimit.Func) (any, error) {
			return fn(ctx)
		})
}

func TestTezosBlockFetcher_FetchLatestBlockUsesTzktLimiter(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	httpClient := mocks.NewMockHTTPClient(ctrl)
	limiter := mocks.NewMockLimiter(ctrl)
	clock := mocks.NewMockClock(ctrl)

	expectLimited(limiter)
	httpClient.EXPECT().
		GetAndUnmarshal(gomock.Any(), "https://tzkt.example/v1/head", gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, result any) error {
			return json.Unmarshal([]byte(`{"level": 14553705}`), result)
		})

	fetcher := tezos.NewTezosBlockFetcher("https://tzkt.example", httpClient, limiter, clock)
	level, err := fetcher.FetchLatestBlock(context.Background())
	require.NoError(t, err)
	assert.Equal(t, uint64(14553705), level)
}

func TestTezosBlockFetcher_FetchBlockTimestampUsesTzktLimiter(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	httpClient := mocks.NewMockHTTPClient(ctrl)
	limiter := mocks.NewMockLimiter(ctrl)
	clock := mocks.NewMockClock(ctrl)

	want := time.Date(2026, 8, 18, 9, 0, 0, 0, time.UTC)
	expectLimited(limiter)
	httpClient.EXPECT().
		GetAndUnmarshal(gomock.Any(), "https://tzkt.example/v1/blocks/42", gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, result any) error {
			return json.Unmarshal([]byte(`{"level": 42, "timestamp": "2026-08-18T09:00:00Z"}`), result)
		})
	clock.EXPECT().Parse(time.RFC3339, "2026-08-18T09:00:00Z").Return(want, nil)

	fetcher := tezos.NewTezosBlockFetcher("https://tzkt.example", httpClient, limiter, clock)
	ts, err := fetcher.FetchBlockTimestamp(context.Background(), 42)
	require.NoError(t, err)
	assert.Equal(t, want, ts)
}

// TestTezosBlockFetcher_nilLimiterPassesThrough keeps the nil contract explicit: tests
// and callers without a limiter must not panic or block.
func TestTezosBlockFetcher_nilLimiterPassesThrough(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	httpClient := mocks.NewMockHTTPClient(ctrl)
	clock := mocks.NewMockClock(ctrl)

	httpClient.EXPECT().
		GetAndUnmarshal(gomock.Any(), "https://tzkt.example/v1/head", gomock.Any()).
		Return(nil)

	fetcher := tezos.NewTezosBlockFetcher("https://tzkt.example", httpClient, nil, clock)
	_, err := fetcher.FetchLatestBlock(context.Background())
	require.NoError(t, err)
}

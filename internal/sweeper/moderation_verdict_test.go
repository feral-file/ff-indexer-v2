package sweeper_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/objkt"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/opensea"
	"github.com/feral-file/ff-indexer-v2/internal/store"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
	"github.com/feral-file/ff-indexer-v2/internal/sweeper"
	"github.com/feral-file/ff-indexer-v2/internal/types"
)

// testModerationSweeperMocks contains all the mocks needed for testing the moderation verdict sweeper
type testModerationSweeperMocks struct {
	ctrl          *gomock.Controller
	store         *mocks.MockStore
	openseaClient *mocks.MockOpenSeaClient
	objktClient   *mocks.MockObjktClient
	clock         *mocks.MockClock
	sweeper       sweeper.Sweeper
}

// moderationSweeperTestConfig mirrors the production defaults at test-friendly scale.
func moderationSweeperTestConfig() *sweeper.ModerationVerdictSweeperConfig {
	return &sweeper.ModerationVerdictSweeperConfig{
		BatchSize:              10,
		WorkerPoolSize:         2,
		InitialRecheckInterval: 24 * time.Hour,
		MaxRecheckInterval:     720 * time.Hour,
		FailureBackoffInitial:  time.Hour,
		MaxConsecutiveFailures: 5,
	}
}

// moderationSweeperLoggerOnce guards the package-global logger init. Re-initializing
// per test races with the previous test's still-draining sweeper goroutines,
// which log through the same global on their way down.
var moderationSweeperLoggerOnce sync.Once

// setupTestModerationSweeper creates all the mocks and sweeper for testing
func setupTestModerationSweeper(t *testing.T) *testModerationSweeperMocks {
	var initErr error
	moderationSweeperLoggerOnce.Do(func() {
		initErr = logger.Initialize(logger.Config{Debug: true})
	})
	if initErr != nil {
		t.Fatalf("Failed to initialize logger: %v", initErr)
	}

	ctrl := gomock.NewController(t)

	tm := &testModerationSweeperMocks{
		ctrl:          ctrl,
		store:         mocks.NewMockStore(ctrl),
		openseaClient: mocks.NewMockOpenSeaClient(ctrl),
		objktClient:   mocks.NewMockObjktClient(ctrl),
		clock:         mocks.NewMockClock(ctrl),
	}

	tm.sweeper = sweeper.NewModerationVerdictSweeper(
		moderationSweeperTestConfig(),
		tm.store,
		tm.openseaClient,
		tm.objktClient,
		tm.clock,
	)

	return tm
}

func tearDownTestModerationSweeper(tm *testModerationSweeperMocks) {
	tm.ctrl.Finish()
}

// expectIdleAfterFirstCycle arranges every subsequent due-fetch to come back empty
// and the idle sleep to elapse almost immediately, so a test can drive exactly one
// interesting cycle through Start and then Stop the sweeper.
func expectIdleAfterFirstCycle(tm *testModerationSweeperMocks) {
	tm.store.EXPECT().
		GetTokenModerationVerdictsDueForCheck(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, nil).
		AnyTimes()
	tm.clock.EXPECT().
		After(sweeper.SWEEP_CYCLE_INTERVAL).
		DoAndReturn(func(_ time.Duration) <-chan time.Time {
			ch := make(chan time.Time, 1)
			go func() {
				time.Sleep(20 * time.Millisecond)
				ch <- time.Now()
			}()
			return ch
		}).
		AnyTimes()
}

// testTokenCID builds a token CID for test fixtures. A function rather than
// string literals in the TokenCID fields: gosec G101 pattern-matches literal
// assignments to token-named fields as "hardcoded credentials".
func testTokenCID(chain, standard, contract, number string) string {
	return chain + ":" + standard + ":" + contract + ":" + number
}

// runOneSweep starts the sweeper, lets it work briefly, then stops it.
func runOneSweep(t *testing.T, tm *testModerationSweeperMocks) {
	t.Helper()
	ctx := context.Background()
	go func() {
		time.Sleep(100 * time.Millisecond)
		_ = tm.sweeper.Stop(ctx)
	}()
	require.NoError(t, tm.sweeper.Start(ctx))
}

func TestModerationVerdictSweeper_Name(t *testing.T) {
	tm := setupTestModerationSweeper(t)
	defer tearDownTestModerationSweeper(tm)

	assert.Equal(t, "moderation-verdict-sweeper", tm.sweeper.Name())
}

func TestModerationVerdictSweeper_CleanVerdict_DoublesInterval(t *testing.T) {
	tm := setupTestModerationSweeper(t)
	defer tearDownTestModerationSweeper(tm)

	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	tm.clock.EXPECT().Now().Return(now).AnyTimes()

	// Previous interval was 24h (next_check_at − last_checked_at); a clean
	// re-check doubles it to 48h.
	item := store.TokenModerationCheckItem{
		TokenID:         1,
		TokenCID:        testTokenCID("eip155:1", "erc721", "0xabc", "1"),
		Chain:           domain.ChainEthereumMainnet,
		ContractAddress: "0xabc",
		TokenNumber:     "1",
		Verdict:         schema.ModerationStatusNone,
		LastCheckedAt:   now.Add(-25 * time.Hour),
		NextCheckAt:     now.Add(-1 * time.Hour),
	}
	tm.store.EXPECT().
		GetTokenModerationVerdictsDueForCheck(gomock.Any(), schema.ModerationSourceOpenSea, 10).
		Return([]store.TokenModerationCheckItem{item}, nil).
		Times(1)
	tm.openseaClient.EXPECT().
		GetNFT(gomock.Any(), "0xabc", "1").
		Return(&opensea.NFTMetadata{IsDisabled: false}, nil)
	tm.store.EXPECT().
		UpsertTokenModerationVerdict(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, input store.UpsertTokenModerationVerdictInput) (bool, error) {
			assert.Equal(t, uint64(1), input.TokenID)
			assert.Equal(t, schema.ModerationSourceOpenSea, input.Source)
			assert.Equal(t, schema.ModerationStatusNone, input.Verdict)
			// The compare-and-set expectation must be the snapshot the due query
			// returned, or the store cannot tell a stale response from a current
			// one. Dropping it would silently reopen the overwrite race; passing
			// the wrong value would reject every write and mute the sweeper.
			if assert.NotNil(t, input.ExpectedLastCheckedAt,
				"sweeper writes must be conditional on the row they read") {
				assert.Equal(t, item.LastCheckedAt, *input.ExpectedLastCheckedAt)
			}
			assert.JSONEq(t, `{"is_disabled":false}`, string(input.Detail))
			if assert.NotNil(t, input.NextCheckAt) {
				assert.Equal(t, now.Add(48*time.Hour), *input.NextCheckAt)
			}
			return false, nil
		})
	expectIdleAfterFirstCycle(tm)

	runOneSweep(t, tm)
}

func TestModerationVerdictSweeper_FlaggedVerdict_MaxInterval(t *testing.T) {
	tm := setupTestModerationSweeper(t)
	defer tearDownTestModerationSweeper(tm)

	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	tm.clock.EXPECT().Now().Return(now).AnyTimes()

	item := store.TokenModerationCheckItem{
		TokenID:         2,
		TokenCID:        testTokenCID("eip155:1", "erc1155", "0xdef", "9"),
		ContractAddress: "0xdef",
		TokenNumber:     "9",
		Verdict:         schema.ModerationStatusNone,
		LastCheckedAt:   now.Add(-25 * time.Hour),
		NextCheckAt:     now.Add(-1 * time.Hour),
	}
	tm.store.EXPECT().
		GetTokenModerationVerdictsDueForCheck(gomock.Any(), schema.ModerationSourceOpenSea, 10).
		Return([]store.TokenModerationCheckItem{item}, nil).
		Times(1)
	tm.openseaClient.EXPECT().
		GetNFT(gomock.Any(), "0xdef", "9").
		Return(&opensea.NFTMetadata{IsDisabled: true}, nil)
	tm.store.EXPECT().
		UpsertTokenModerationVerdict(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, input store.UpsertTokenModerationVerdictInput) (bool, error) {
			assert.Equal(t, schema.ModerationStatusSpam, input.Verdict)
			// Flagged tokens poll at the fixed maximum: appeals are rare.
			if assert.NotNil(t, input.NextCheckAt) {
				assert.Equal(t, now.Add(720*time.Hour), *input.NextCheckAt)
			}
			return true, nil
		})
	expectIdleAfterFirstCycle(tm)

	runOneSweep(t, tm)
}

func TestModerationVerdictSweeper_Failure_TakesErrorBackoff(t *testing.T) {
	tm := setupTestModerationSweeper(t)
	defer tearDownTestModerationSweeper(tm)

	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	tm.clock.EXPECT().Now().Return(now).AnyTimes()

	item := store.TokenModerationCheckItem{
		TokenID:             3,
		TokenCID:            testTokenCID("eip155:1", "erc721", "0xabc", "3"),
		ContractAddress:     "0xabc",
		TokenNumber:         "3",
		ConsecutiveFailures: 0,
		LastCheckedAt:       now.Add(-25 * time.Hour),
		NextCheckAt:         now.Add(-1 * time.Hour),
	}
	tm.store.EXPECT().
		GetTokenModerationVerdictsDueForCheck(gomock.Any(), schema.ModerationSourceOpenSea, 10).
		Return([]store.TokenModerationCheckItem{item}, nil).
		Times(1)
	// A vendor 404 is "no opinion", not a verdict: the stored one must stand and
	// only the schedule advances (first failure → initial 1h backoff).
	tm.openseaClient.EXPECT().
		GetNFT(gomock.Any(), "0xabc", "3").
		Return(nil, opensea.ErrNFTNotFound)
	tm.store.EXPECT().
		RecordTokenModerationCheckFailure(gomock.Any(), uint64(3), schema.ModerationSourceOpenSea, gomock.Any(),
			now.Add(time.Hour), item.LastCheckedAt).
		Return(true, nil)
	expectIdleAfterFirstCycle(tm)

	runOneSweep(t, tm)
}

func TestModerationVerdictSweeper_Failure_PinsAtMaxAfterRepeats(t *testing.T) {
	tm := setupTestModerationSweeper(t)
	defer tearDownTestModerationSweeper(tm)

	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	tm.clock.EXPECT().Now().Return(now).AnyTimes()

	item := store.TokenModerationCheckItem{
		TokenID:             4,
		TokenCID:            testTokenCID("eip155:1", "erc721", "0xabc", "4"),
		ContractAddress:     "0xabc",
		TokenNumber:         "4",
		ConsecutiveFailures: 4, // this failure is the 5th = MaxConsecutiveFailures
		LastCheckedAt:       now.Add(-25 * time.Hour),
		NextCheckAt:         now.Add(-1 * time.Hour),
	}
	tm.store.EXPECT().
		GetTokenModerationVerdictsDueForCheck(gomock.Any(), schema.ModerationSourceOpenSea, 10).
		Return([]store.TokenModerationCheckItem{item}, nil).
		Times(1)
	tm.openseaClient.EXPECT().
		GetNFT(gomock.Any(), "0xabc", "4").
		Return(nil, errors.New("opensea: 502 bad gateway"))
	// Permanently failing rows settle at the max interval instead of leaving the
	// queue for good: quota stops burning but a recovered vendor still converges.
	tm.store.EXPECT().
		RecordTokenModerationCheckFailure(gomock.Any(), uint64(4), schema.ModerationSourceOpenSea, gomock.Any(),
			now.Add(720*time.Hour), item.LastCheckedAt).
		Return(true, nil)
	expectIdleAfterFirstCycle(tm)

	runOneSweep(t, tm)
}

func TestModerationVerdictSweeper_NoAPIKey_LeavesRowsUntouched(t *testing.T) {
	tm := setupTestModerationSweeper(t)
	defer tearDownTestModerationSweeper(tm)

	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	tm.clock.EXPECT().Now().Return(now).AnyTimes()

	item := store.TokenModerationCheckItem{
		TokenID:         5,
		TokenCID:        testTokenCID("eip155:1", "erc721", "0xabc", "5"),
		ContractAddress: "0xabc",
		TokenNumber:     "5",
		LastCheckedAt:   now.Add(-25 * time.Hour),
		NextCheckAt:     now.Add(-1 * time.Hour),
	}
	tm.store.EXPECT().
		GetTokenModerationVerdictsDueForCheck(gomock.Any(), schema.ModerationSourceOpenSea, 10).
		Return([]store.TokenModerationCheckItem{item}, nil).
		Times(1)
	// An unconfigured API key is a source-wide condition, not a per-row failure:
	// writing failure state would walk every row's backoff to the max for no
	// reason. gomock fails the test on any unexpected store write.
	tm.openseaClient.EXPECT().
		GetNFT(gomock.Any(), "0xabc", "5").
		Return(nil, opensea.ErrNoAPIKey)
	expectIdleAfterFirstCycle(tm)

	runOneSweep(t, tm)
}

// TestModerationVerdictSweeper_NoAPIKey_DoesNotHotLoop pins the throttling half of the
// unconfigured-vendor contract, which the "leaves rows untouched" test above
// cannot see: because those rows keep their next_check_at, the same batch stays
// due forever, and ErrNoAPIKey is returned before the HTTP request and the rate
// limiter, so nothing else throttles a respin. The cycle's unconditional sleep
// is what bounds it — this test's job is to confirm that sleep actually runs
// rather than being skipped.
func TestModerationVerdictSweeper_NoAPIKey_DoesNotHotLoop(t *testing.T) {
	tm := setupTestModerationSweeper(t)
	defer tearDownTestModerationSweeper(tm)

	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	tm.clock.EXPECT().Now().Return(now).AnyTimes()

	item := store.TokenModerationCheckItem{
		TokenID:         5,
		TokenCID:        testTokenCID("eip155:1", "erc721", "0xabc", "5"),
		ContractAddress: "0xabc",
		TokenNumber:     "5",
		LastCheckedAt:   now.Add(-25 * time.Hour),
		NextCheckAt:     now.Add(-1 * time.Hour),
	}

	var fetches atomic.Int32
	tm.store.EXPECT().
		GetTokenModerationVerdictsDueForCheck(gomock.Any(), schema.ModerationSourceOpenSea, 10).
		DoAndReturn(func(_ context.Context, _ schema.ModerationSource, _ int) ([]store.TokenModerationCheckItem, error) {
			fetches.Add(1)
			return []store.TokenModerationCheckItem{item}, nil
		}).
		AnyTimes()
	tm.store.EXPECT().
		GetTokenModerationVerdictsDueForCheck(gomock.Any(), schema.ModerationSourceObjkt, 10).
		Return(nil, nil).
		AnyTimes()
	tm.openseaClient.EXPECT().
		GetNFT(gomock.Any(), "0xabc", "5").
		Return(nil, opensea.ErrNoAPIKey).
		AnyTimes()
	// The idle sleep must actually be taken; without it the loop respins freely.
	tm.clock.EXPECT().
		After(sweeper.SWEEP_CYCLE_INTERVAL).
		DoAndReturn(func(_ time.Duration) <-chan time.Time {
			ch := make(chan time.Time, 1)
			go func() {
				time.Sleep(20 * time.Millisecond)
				ch <- time.Now()
			}()
			return ch
		}).
		MinTimes(1)

	runOneSweep(t, tm)

	// ~100ms of running at a 20ms mocked sleep is a handful of cycles. A
	// regression spins thousands.
	assert.Less(t, fetches.Load(), int32(50),
		"unconfigured vendor must not respin the due batch without sleeping")
}

// TestModerationVerdictSweeper_StoreError_DoesNotHotLoop covers the other way into the
// same trap as the unconfigured-vendor test above. A failing due-query issues no
// HTTP request, so neither the rate limiter nor the vendor call bounds a retry —
// only the cycle's own sleep does, and unlike the media health sweeper it mirrors,
// this sweeper sleeps even on that error path rather than returning immediately.
// A database blip would otherwise pin a core and flood Sentry for the length of
// the outage.
func TestModerationVerdictSweeper_StoreError_DoesNotHotLoop(t *testing.T) {
	tm := setupTestModerationSweeper(t)
	defer tearDownTestModerationSweeper(tm)

	tm.clock.EXPECT().Now().Return(time.Now()).AnyTimes()

	var fetches atomic.Int32
	tm.store.EXPECT().
		GetTokenModerationVerdictsDueForCheck(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ schema.ModerationSource, _ int) ([]store.TokenModerationCheckItem, error) {
			fetches.Add(1)
			return nil, errors.New("connection refused")
		}).
		AnyTimes()
	// The back-off must be taken on the error path, not just the idle path.
	tm.clock.EXPECT().
		After(sweeper.SWEEP_CYCLE_INTERVAL).
		DoAndReturn(func(_ time.Duration) <-chan time.Time {
			ch := make(chan time.Time, 1)
			go func() {
				time.Sleep(20 * time.Millisecond)
				ch <- time.Now()
			}()
			return ch
		}).
		MinTimes(1)

	runOneSweep(t, tm)

	// ~100ms of running at a 20ms mocked back-off is a handful of cycles. A
	// regression spins thousands.
	assert.Less(t, fetches.Load(), int32(50),
		"a failing due-query must back off before the cycle retries")
}

// TestModerationVerdictSweeper_UpsertError_DoesNotHotLoop covers a batch where the
// vendor answers but persisting the verdict fails, so next_check_at never moves
// and the same row stays due forever. This one does reach the rate-limited
// vendor call, so a regression burns paid API quota rather than CPU — quieter
// than a failing due-query, and more expensive.
func TestModerationVerdictSweeper_UpsertError_DoesNotHotLoop(t *testing.T) {
	tm := setupTestModerationSweeper(t)
	defer tearDownTestModerationSweeper(tm)

	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	tm.clock.EXPECT().Now().Return(now).AnyTimes()

	item := store.TokenModerationCheckItem{
		TokenID:         9,
		TokenCID:        testTokenCID("eip155:1", "erc721", "0xabc", "9"),
		ContractAddress: "0xabc",
		TokenNumber:     "9",
		LastCheckedAt:   now.Add(-25 * time.Hour),
		NextCheckAt:     now.Add(-1 * time.Hour),
	}

	var vendorCalls atomic.Int32
	tm.store.EXPECT().
		GetTokenModerationVerdictsDueForCheck(gomock.Any(), schema.ModerationSourceOpenSea, 10).
		Return([]store.TokenModerationCheckItem{item}, nil).
		AnyTimes()
	tm.store.EXPECT().
		GetTokenModerationVerdictsDueForCheck(gomock.Any(), schema.ModerationSourceObjkt, 10).
		Return(nil, nil).
		AnyTimes()
	tm.openseaClient.EXPECT().
		GetNFT(gomock.Any(), "0xabc", "9").
		DoAndReturn(func(_ context.Context, _, _ string) (*opensea.NFTMetadata, error) {
			vendorCalls.Add(1)
			return &opensea.NFTMetadata{IsDisabled: true}, nil
		}).
		AnyTimes()
	tm.store.EXPECT().
		UpsertTokenModerationVerdict(gomock.Any(), gomock.Any()).
		Return(false, errors.New("deadlock detected")).
		AnyTimes()
	tm.clock.EXPECT().
		After(sweeper.SWEEP_CYCLE_INTERVAL).
		DoAndReturn(func(_ time.Duration) <-chan time.Time {
			ch := make(chan time.Time, 1)
			go func() {
				time.Sleep(20 * time.Millisecond)
				ch <- time.Now()
			}()
			return ch
		}).
		MinTimes(1)

	runOneSweep(t, tm)

	assert.Less(t, vendorCalls.Load(), int32(50),
		"a batch that cannot persist its verdict must back off, not respin the vendor")
}

// TestModerationVerdictSweeper_MixedBatchWriteFailure_StillSleeps pins a gap the
// four tests above cannot see: they each drive a batch where the SAME single row
// fails throughout. Here the batch has two due rows — one whose write always
// succeeds, one whose write always fails — so on every cycle SOMETHING in the
// batch does move next_check_at.
//
// A version of the sweeper that decided whether to sleep from a single
// batch-level "did anything progress" flag would see that as progress and skip
// the sleep, respinning the failing row's vendor call every cycle forever, with
// no backoff, for as long as the rest of the batch kept finding real work —
// which at any real scale is most of the time. The fix removed that flag
// entirely: the cycle now sleeps unconditionally, so this failing row can be
// retried no faster than SWEEP_CYCLE_INTERVAL regardless of what else is in the
// batch.
func TestModerationVerdictSweeper_MixedBatchWriteFailure_StillSleeps(t *testing.T) {
	tm := setupTestModerationSweeper(t)
	defer tearDownTestModerationSweeper(tm)

	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	tm.clock.EXPECT().Now().Return(now).AnyTimes()

	okItem := store.TokenModerationCheckItem{
		TokenID:         20,
		TokenCID:        testTokenCID("eip155:1", "erc721", "0xabc", "20"),
		ContractAddress: "0xabc",
		TokenNumber:     "20",
		LastCheckedAt:   now.Add(-25 * time.Hour),
		NextCheckAt:     now.Add(-1 * time.Hour),
	}
	stuckItem := store.TokenModerationCheckItem{
		TokenID:         21,
		TokenCID:        testTokenCID("eip155:1", "erc721", "0xdef", "21"),
		ContractAddress: "0xdef",
		TokenNumber:     "21",
		LastCheckedAt:   now.Add(-25 * time.Hour),
		NextCheckAt:     now.Add(-1 * time.Hour),
	}

	var fetches atomic.Int32
	tm.store.EXPECT().
		GetTokenModerationVerdictsDueForCheck(gomock.Any(), schema.ModerationSourceOpenSea, 10).
		DoAndReturn(func(_ context.Context, _ schema.ModerationSource, _ int) ([]store.TokenModerationCheckItem, error) {
			fetches.Add(1)
			return []store.TokenModerationCheckItem{okItem, stuckItem}, nil
		}).
		AnyTimes()
	tm.store.EXPECT().
		GetTokenModerationVerdictsDueForCheck(gomock.Any(), schema.ModerationSourceObjkt, 10).
		Return(nil, nil).
		AnyTimes()
	tm.openseaClient.EXPECT().
		GetNFT(gomock.Any(), "0xabc", "20").
		Return(&opensea.NFTMetadata{IsDisabled: false}, nil).
		AnyTimes()
	tm.openseaClient.EXPECT().
		GetNFT(gomock.Any(), "0xdef", "21").
		Return(&opensea.NFTMetadata{IsDisabled: false}, nil).
		AnyTimes()
	tm.store.EXPECT().
		UpsertTokenModerationVerdict(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, input store.UpsertTokenModerationVerdictInput) (bool, error) {
			if input.TokenID == stuckItem.TokenID {
				return false, errors.New("deadlock detected")
			}
			return false, nil
		}).
		AnyTimes()
	tm.clock.EXPECT().
		After(sweeper.SWEEP_CYCLE_INTERVAL).
		DoAndReturn(func(_ time.Duration) <-chan time.Time {
			ch := make(chan time.Time, 1)
			go func() {
				time.Sleep(20 * time.Millisecond)
				ch <- time.Now()
			}()
			return ch
		}).
		MinTimes(1)

	runOneSweep(t, tm)

	assert.Less(t, fetches.Load(), int32(50),
		"one persistently failing write must not stop the cycle from sleeping just because another row in the same batch succeeded")
}

func TestModerationVerdictSweeper_ObjktBanned(t *testing.T) {
	tm := setupTestModerationSweeper(t)
	defer tearDownTestModerationSweeper(tm)

	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	tm.clock.EXPECT().Now().Return(now).AnyTimes()

	item := store.TokenModerationCheckItem{
		TokenID:         6,
		TokenCID:        testTokenCID("tezos:mainnet", "fa2", "KT1abc", "6"),
		Chain:           domain.ChainTezosMainnet,
		ContractAddress: "KT1abc",
		TokenNumber:     "6",
		Verdict:         schema.ModerationStatusNone,
		LastCheckedAt:   now.Add(-25 * time.Hour),
		NextCheckAt:     now.Add(-1 * time.Hour),
	}
	tm.store.EXPECT().
		GetTokenModerationVerdictsDueForCheck(gomock.Any(), schema.ModerationSourceObjkt, 10).
		Return([]store.TokenModerationCheckItem{item}, nil).
		Times(1)
	tm.objktClient.EXPECT().
		GetToken(gomock.Any(), "KT1abc", "6").
		Return(&objkt.Token{Flag: types.StringPtr(objkt.FlagBanned)}, nil)
	tm.store.EXPECT().
		UpsertTokenModerationVerdict(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, input store.UpsertTokenModerationVerdictInput) (bool, error) {
			assert.Equal(t, schema.ModerationSourceObjkt, input.Source)
			assert.Equal(t, schema.ModerationStatusSpam, input.Verdict)
			assert.JSONEq(t, `{"flag":"banned"}`, string(input.Detail))
			return true, nil
		})
	expectIdleAfterFirstCycle(tm)

	runOneSweep(t, tm)
}

// TestModerationVerdictSweeper_CleanVerdictAfterFailures_RestartsAtFloor pins that a
// transient vendor outage cannot silently demote a clean token's cadence. The
// failure path advances next_check_at while freezing last_checked_at, so the
// usual "previous interval = next_check_at − last_checked_at" derivation would
// measure the failure backoff instead — here 720h, which doubled and clamped
// would schedule the next check 30 days out on a token that is being polled
// precisely to catch a late takedown.
func TestModerationVerdictSweeper_CleanVerdictAfterFailures_RestartsAtFloor(t *testing.T) {
	tm := setupTestModerationSweeper(t)
	defer tearDownTestModerationSweeper(tm)

	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	tm.clock.EXPECT().Now().Return(now).AnyTimes()

	item := store.TokenModerationCheckItem{
		TokenID:         7,
		TokenCID:        testTokenCID("eip155:1", "erc721", "0xabc", "7"),
		ContractAddress: "0xabc",
		TokenNumber:     "7",
		Verdict:         schema.ModerationStatusNone,
		// Five prior failures pinned next_check_at at the 720h ceiling while
		// last_checked_at stayed at the last successful confirmation.
		ConsecutiveFailures: 5,
		LastCheckedAt:       now.Add(-721 * time.Hour),
		NextCheckAt:         now.Add(-1 * time.Hour),
	}
	tm.store.EXPECT().
		GetTokenModerationVerdictsDueForCheck(gomock.Any(), schema.ModerationSourceOpenSea, 10).
		Return([]store.TokenModerationCheckItem{item}, nil).
		Times(1)
	tm.openseaClient.EXPECT().
		GetNFT(gomock.Any(), "0xabc", "7").
		Return(&opensea.NFTMetadata{IsDisabled: false}, nil)
	tm.store.EXPECT().
		UpsertTokenModerationVerdict(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, input store.UpsertTokenModerationVerdictInput) (bool, error) {
			if assert.NotNil(t, input.NextCheckAt) {
				assert.Equal(t, now.Add(24*time.Hour), *input.NextCheckAt,
					"a recovered token restarts at the floor, not the failure ceiling")
			}
			return false, nil
		})
	expectIdleAfterFirstCycle(tm)

	runOneSweep(t, tm)
}

func TestModerationVerdictSweeper_NothingDue_Sleeps(t *testing.T) {
	tm := setupTestModerationSweeper(t)
	defer tearDownTestModerationSweeper(tm)

	tm.clock.EXPECT().Now().Return(time.Now()).AnyTimes()
	expectIdleAfterFirstCycle(tm)

	runOneSweep(t, tm)
}

func TestModerationVerdictSweeper_StopBeforeStart(t *testing.T) {
	tm := setupTestModerationSweeper(t)
	defer tearDownTestModerationSweeper(tm)

	require.NoError(t, tm.sweeper.Stop(context.Background()))
}

func TestModerationVerdictSweeper_DoubleStart(t *testing.T) {
	tm := setupTestModerationSweeper(t)
	defer tearDownTestModerationSweeper(tm)

	ctx := context.Background()
	tm.clock.EXPECT().Now().Return(time.Now()).AnyTimes()
	expectIdleAfterFirstCycle(tm)

	errChan := make(chan error, 1)
	go func() {
		errChan <- tm.sweeper.Start(ctx)
	}()

	time.Sleep(50 * time.Millisecond)

	err := tm.sweeper.Start(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already running")

	_ = tm.sweeper.Stop(ctx)
	require.NoError(t, <-errChan)
}

// TestModerationVerdictSweeper_StaleFailure_DoesNotHotLoop covers a case where the
// vendor call fails and the failure write also loses its compare-and-set,
// because a newer enrichment already wrote. That winner already moved
// next_check_at, so this row is genuinely no longer due once the write is
// retried against fresh state — but the mock here is stateless and keeps
// answering with the same stale row, standing in for "the DB keeps rejecting
// this specific write." The cycle sleep must still bound the retry rate.
func TestModerationVerdictSweeper_StaleFailure_DoesNotHotLoop(t *testing.T) {
	tm := setupTestModerationSweeper(t)
	defer tearDownTestModerationSweeper(tm)

	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	tm.clock.EXPECT().Now().Return(now).AnyTimes()

	item := store.TokenModerationCheckItem{
		TokenID:         11,
		TokenCID:        testTokenCID("eip155:1", "erc721", "0xabc", "11"),
		ContractAddress: "0xabc",
		TokenNumber:     "11",
		LastCheckedAt:   now.Add(-25 * time.Hour),
		NextCheckAt:     now.Add(-1 * time.Hour),
	}

	var vendorCalls atomic.Int32
	tm.store.EXPECT().
		GetTokenModerationVerdictsDueForCheck(gomock.Any(), schema.ModerationSourceOpenSea, 10).
		Return([]store.TokenModerationCheckItem{item}, nil).
		AnyTimes()
	tm.store.EXPECT().
		GetTokenModerationVerdictsDueForCheck(gomock.Any(), schema.ModerationSourceObjkt, 10).
		Return(nil, nil).
		AnyTimes()
	tm.openseaClient.EXPECT().
		GetNFT(gomock.Any(), "0xabc", "11").
		DoAndReturn(func(_ context.Context, _, _ string) (*opensea.NFTMetadata, error) {
			vendorCalls.Add(1)
			return nil, errors.New("opensea: 502 bad gateway")
		}).
		AnyTimes()
	// The compare-and-set rejects: applied=false, so this is not progress.
	tm.store.EXPECT().
		RecordTokenModerationCheckFailure(gomock.Any(), uint64(11), schema.ModerationSourceOpenSea,
			gomock.Any(), gomock.Any(), item.LastCheckedAt).
		Return(false, nil).
		AnyTimes()
	tm.clock.EXPECT().
		After(sweeper.SWEEP_CYCLE_INTERVAL).
		DoAndReturn(func(_ time.Duration) <-chan time.Time {
			ch := make(chan time.Time, 1)
			go func() {
				time.Sleep(20 * time.Millisecond)
				ch <- time.Now()
			}()
			return ch
		}).
		MinTimes(1)

	runOneSweep(t, tm)

	assert.Less(t, vendorCalls.Load(), int32(50),
		"a batch whose failure writes all lose the race must back off, not respin the vendor")
}

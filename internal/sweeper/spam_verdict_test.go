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

// testSpamSweeperMocks contains all the mocks needed for testing the spam verdict sweeper
type testSpamSweeperMocks struct {
	ctrl          *gomock.Controller
	store         *mocks.MockStore
	openseaClient *mocks.MockOpenSeaClient
	objktClient   *mocks.MockObjktClient
	clock         *mocks.MockClock
	sweeper       sweeper.Sweeper
}

// spamSweeperTestConfig mirrors the production defaults at test-friendly scale.
func spamSweeperTestConfig() *sweeper.SpamVerdictSweeperConfig {
	return &sweeper.SpamVerdictSweeperConfig{
		BatchSize:              10,
		WorkerPoolSize:         2,
		InitialRecheckInterval: 24 * time.Hour,
		MaxRecheckInterval:     720 * time.Hour,
		FailureBackoffInitial:  time.Hour,
		MaxConsecutiveFailures: 5,
	}
}

// spamSweeperLoggerOnce guards the package-global logger init. Re-initializing
// per test races with the previous test's still-draining sweeper goroutines,
// which log through the same global on their way down.
var spamSweeperLoggerOnce sync.Once

// setupTestSpamSweeper creates all the mocks and sweeper for testing
func setupTestSpamSweeper(t *testing.T) *testSpamSweeperMocks {
	var initErr error
	spamSweeperLoggerOnce.Do(func() {
		initErr = logger.Initialize(logger.Config{Debug: true})
	})
	if initErr != nil {
		t.Fatalf("Failed to initialize logger: %v", initErr)
	}

	ctrl := gomock.NewController(t)

	tm := &testSpamSweeperMocks{
		ctrl:          ctrl,
		store:         mocks.NewMockStore(ctrl),
		openseaClient: mocks.NewMockOpenSeaClient(ctrl),
		objktClient:   mocks.NewMockObjktClient(ctrl),
		clock:         mocks.NewMockClock(ctrl),
	}

	tm.sweeper = sweeper.NewSpamVerdictSweeper(
		spamSweeperTestConfig(),
		tm.store,
		tm.openseaClient,
		tm.objktClient,
		tm.clock,
	)

	return tm
}

func tearDownTestSpamSweeper(tm *testSpamSweeperMocks) {
	tm.ctrl.Finish()
}

// expectIdleAfterFirstCycle arranges every subsequent due-fetch to come back empty
// and the idle sleep to elapse almost immediately, so a test can drive exactly one
// interesting cycle through Start and then Stop the sweeper.
func expectIdleAfterFirstCycle(tm *testSpamSweeperMocks) {
	tm.store.EXPECT().
		GetTokenSpamVerdictsDueForCheck(gomock.Any(), gomock.Any(), gomock.Any()).
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
func runOneSweep(t *testing.T, tm *testSpamSweeperMocks) {
	t.Helper()
	ctx := context.Background()
	go func() {
		time.Sleep(100 * time.Millisecond)
		_ = tm.sweeper.Stop(ctx)
	}()
	require.NoError(t, tm.sweeper.Start(ctx))
}

func TestSpamVerdictSweeper_Name(t *testing.T) {
	tm := setupTestSpamSweeper(t)
	defer tearDownTestSpamSweeper(tm)

	assert.Equal(t, "spam-verdict-sweeper", tm.sweeper.Name())
}

func TestSpamVerdictSweeper_CleanVerdict_DoublesInterval(t *testing.T) {
	tm := setupTestSpamSweeper(t)
	defer tearDownTestSpamSweeper(tm)

	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	tm.clock.EXPECT().Now().Return(now).AnyTimes()

	// Previous interval was 24h (next_check_at − last_checked_at); a clean
	// re-check doubles it to 48h.
	item := store.TokenSpamCheckItem{
		TokenID:         1,
		TokenCID:        testTokenCID("eip155:1", "erc721", "0xabc", "1"),
		Chain:           domain.ChainEthereumMainnet,
		ContractAddress: "0xabc",
		TokenNumber:     "1",
		Verdict:         false,
		LastCheckedAt:   now.Add(-25 * time.Hour),
		NextCheckAt:     now.Add(-1 * time.Hour),
	}
	tm.store.EXPECT().
		GetTokenSpamVerdictsDueForCheck(gomock.Any(), schema.SpamSourceOpenSea, 10).
		Return([]store.TokenSpamCheckItem{item}, nil).
		Times(1)
	tm.openseaClient.EXPECT().
		GetNFT(gomock.Any(), "0xabc", "1").
		Return(&opensea.NFTMetadata{IsDisabled: false}, nil)
	tm.store.EXPECT().
		UpsertTokenSpamVerdict(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, input store.UpsertTokenSpamVerdictInput) (bool, error) {
			assert.Equal(t, uint64(1), input.TokenID)
			assert.Equal(t, schema.SpamSourceOpenSea, input.Source)
			assert.False(t, input.Verdict)
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

func TestSpamVerdictSweeper_FlaggedVerdict_MaxInterval(t *testing.T) {
	tm := setupTestSpamSweeper(t)
	defer tearDownTestSpamSweeper(tm)

	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	tm.clock.EXPECT().Now().Return(now).AnyTimes()

	item := store.TokenSpamCheckItem{
		TokenID:         2,
		TokenCID:        testTokenCID("eip155:1", "erc1155", "0xdef", "9"),
		ContractAddress: "0xdef",
		TokenNumber:     "9",
		Verdict:         false,
		LastCheckedAt:   now.Add(-25 * time.Hour),
		NextCheckAt:     now.Add(-1 * time.Hour),
	}
	tm.store.EXPECT().
		GetTokenSpamVerdictsDueForCheck(gomock.Any(), schema.SpamSourceOpenSea, 10).
		Return([]store.TokenSpamCheckItem{item}, nil).
		Times(1)
	tm.openseaClient.EXPECT().
		GetNFT(gomock.Any(), "0xdef", "9").
		Return(&opensea.NFTMetadata{IsDisabled: true}, nil)
	tm.store.EXPECT().
		UpsertTokenSpamVerdict(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, input store.UpsertTokenSpamVerdictInput) (bool, error) {
			assert.True(t, input.Verdict)
			// Flagged tokens poll at the fixed maximum: appeals are rare.
			if assert.NotNil(t, input.NextCheckAt) {
				assert.Equal(t, now.Add(720*time.Hour), *input.NextCheckAt)
			}
			return true, nil
		})
	expectIdleAfterFirstCycle(tm)

	runOneSweep(t, tm)
}

func TestSpamVerdictSweeper_Failure_TakesErrorBackoff(t *testing.T) {
	tm := setupTestSpamSweeper(t)
	defer tearDownTestSpamSweeper(tm)

	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	tm.clock.EXPECT().Now().Return(now).AnyTimes()

	item := store.TokenSpamCheckItem{
		TokenID:             3,
		TokenCID:            testTokenCID("eip155:1", "erc721", "0xabc", "3"),
		ContractAddress:     "0xabc",
		TokenNumber:         "3",
		ConsecutiveFailures: 0,
		LastCheckedAt:       now.Add(-25 * time.Hour),
		NextCheckAt:         now.Add(-1 * time.Hour),
	}
	tm.store.EXPECT().
		GetTokenSpamVerdictsDueForCheck(gomock.Any(), schema.SpamSourceOpenSea, 10).
		Return([]store.TokenSpamCheckItem{item}, nil).
		Times(1)
	// A vendor 404 is "no opinion", not a verdict: the stored one must stand and
	// only the schedule advances (first failure → initial 1h backoff).
	tm.openseaClient.EXPECT().
		GetNFT(gomock.Any(), "0xabc", "3").
		Return(nil, opensea.ErrNFTNotFound)
	tm.store.EXPECT().
		RecordTokenSpamCheckFailure(gomock.Any(), uint64(3), schema.SpamSourceOpenSea, gomock.Any(), now.Add(time.Hour)).
		Return(nil)
	expectIdleAfterFirstCycle(tm)

	runOneSweep(t, tm)
}

func TestSpamVerdictSweeper_Failure_PinsAtMaxAfterRepeats(t *testing.T) {
	tm := setupTestSpamSweeper(t)
	defer tearDownTestSpamSweeper(tm)

	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	tm.clock.EXPECT().Now().Return(now).AnyTimes()

	item := store.TokenSpamCheckItem{
		TokenID:             4,
		TokenCID:            testTokenCID("eip155:1", "erc721", "0xabc", "4"),
		ContractAddress:     "0xabc",
		TokenNumber:         "4",
		ConsecutiveFailures: 4, // this failure is the 5th = MaxConsecutiveFailures
		LastCheckedAt:       now.Add(-25 * time.Hour),
		NextCheckAt:         now.Add(-1 * time.Hour),
	}
	tm.store.EXPECT().
		GetTokenSpamVerdictsDueForCheck(gomock.Any(), schema.SpamSourceOpenSea, 10).
		Return([]store.TokenSpamCheckItem{item}, nil).
		Times(1)
	tm.openseaClient.EXPECT().
		GetNFT(gomock.Any(), "0xabc", "4").
		Return(nil, errors.New("opensea: 502 bad gateway"))
	// Permanently failing rows settle at the max interval instead of leaving the
	// queue for good: quota stops burning but a recovered vendor still converges.
	tm.store.EXPECT().
		RecordTokenSpamCheckFailure(gomock.Any(), uint64(4), schema.SpamSourceOpenSea, gomock.Any(), now.Add(720*time.Hour)).
		Return(nil)
	expectIdleAfterFirstCycle(tm)

	runOneSweep(t, tm)
}

func TestSpamVerdictSweeper_NoAPIKey_LeavesRowsUntouched(t *testing.T) {
	tm := setupTestSpamSweeper(t)
	defer tearDownTestSpamSweeper(tm)

	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	tm.clock.EXPECT().Now().Return(now).AnyTimes()

	item := store.TokenSpamCheckItem{
		TokenID:         5,
		TokenCID:        testTokenCID("eip155:1", "erc721", "0xabc", "5"),
		ContractAddress: "0xabc",
		TokenNumber:     "5",
		LastCheckedAt:   now.Add(-25 * time.Hour),
		NextCheckAt:     now.Add(-1 * time.Hour),
	}
	tm.store.EXPECT().
		GetTokenSpamVerdictsDueForCheck(gomock.Any(), schema.SpamSourceOpenSea, 10).
		Return([]store.TokenSpamCheckItem{item}, nil).
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

// TestSpamVerdictSweeper_NoAPIKey_DoesNotHotLoop pins the throttling half of the
// unconfigured-vendor contract, which the "leaves rows untouched" test above
// cannot see: because those rows keep their next_check_at, the same batch stays
// due forever. If the cycle counted them as work it would never sleep, and
// ErrNoAPIKey is returned before the HTTP request and the rate limiter, so
// nothing else throttles the respin — the sweeper would saturate the database
// with due-fetches. Here the store always reports the batch as due, so the only
// thing that can bound the loop is the cycle sleep.
func TestSpamVerdictSweeper_NoAPIKey_DoesNotHotLoop(t *testing.T) {
	tm := setupTestSpamSweeper(t)
	defer tearDownTestSpamSweeper(tm)

	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	tm.clock.EXPECT().Now().Return(now).AnyTimes()

	item := store.TokenSpamCheckItem{
		TokenID:         5,
		TokenCID:        testTokenCID("eip155:1", "erc721", "0xabc", "5"),
		ContractAddress: "0xabc",
		TokenNumber:     "5",
		LastCheckedAt:   now.Add(-25 * time.Hour),
		NextCheckAt:     now.Add(-1 * time.Hour),
	}

	var fetches atomic.Int32
	tm.store.EXPECT().
		GetTokenSpamVerdictsDueForCheck(gomock.Any(), schema.SpamSourceOpenSea, 10).
		DoAndReturn(func(_ context.Context, _ schema.SpamSource, _ int) ([]store.TokenSpamCheckItem, error) {
			fetches.Add(1)
			return []store.TokenSpamCheckItem{item}, nil
		}).
		AnyTimes()
	tm.store.EXPECT().
		GetTokenSpamVerdictsDueForCheck(gomock.Any(), schema.SpamSourceObjkt, 10).
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

// TestSpamVerdictSweeper_StoreError_DoesNotHotLoop covers the other way into the
// same trap as the unconfigured-vendor test above. A failing due-query returns
// before the idle sleep, and Start only logs the error before re-entering the
// cycle — so with no HTTP request issued, neither the rate limiter nor the idle
// sleep bounds the retry. A database blip would otherwise pin a core and flood
// Sentry for the length of the outage.
func TestSpamVerdictSweeper_StoreError_DoesNotHotLoop(t *testing.T) {
	tm := setupTestSpamSweeper(t)
	defer tearDownTestSpamSweeper(tm)

	tm.clock.EXPECT().Now().Return(time.Now()).AnyTimes()

	var fetches atomic.Int32
	tm.store.EXPECT().
		GetTokenSpamVerdictsDueForCheck(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ schema.SpamSource, _ int) ([]store.TokenSpamCheckItem, error) {
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

// TestSpamVerdictSweeper_UpsertError_DoesNotHotLoop covers the third way a batch
// can make no progress: the vendor answers, but persisting the verdict fails, so
// next_check_at never moves and the same rows stay due. This one does reach the
// rate-limited vendor call, so a regression burns paid API quota rather than CPU —
// quieter than the other two, and more expensive.
func TestSpamVerdictSweeper_UpsertError_DoesNotHotLoop(t *testing.T) {
	tm := setupTestSpamSweeper(t)
	defer tearDownTestSpamSweeper(tm)

	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	tm.clock.EXPECT().Now().Return(now).AnyTimes()

	item := store.TokenSpamCheckItem{
		TokenID:         9,
		TokenCID:        testTokenCID("eip155:1", "erc721", "0xabc", "9"),
		ContractAddress: "0xabc",
		TokenNumber:     "9",
		LastCheckedAt:   now.Add(-25 * time.Hour),
		NextCheckAt:     now.Add(-1 * time.Hour),
	}

	var vendorCalls atomic.Int32
	tm.store.EXPECT().
		GetTokenSpamVerdictsDueForCheck(gomock.Any(), schema.SpamSourceOpenSea, 10).
		Return([]store.TokenSpamCheckItem{item}, nil).
		AnyTimes()
	tm.store.EXPECT().
		GetTokenSpamVerdictsDueForCheck(gomock.Any(), schema.SpamSourceObjkt, 10).
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
		UpsertTokenSpamVerdict(gomock.Any(), gomock.Any()).
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

func TestSpamVerdictSweeper_ObjktBanned(t *testing.T) {
	tm := setupTestSpamSweeper(t)
	defer tearDownTestSpamSweeper(tm)

	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	tm.clock.EXPECT().Now().Return(now).AnyTimes()

	item := store.TokenSpamCheckItem{
		TokenID:         6,
		TokenCID:        testTokenCID("tezos:mainnet", "fa2", "KT1abc", "6"),
		Chain:           domain.ChainTezosMainnet,
		ContractAddress: "KT1abc",
		TokenNumber:     "6",
		Verdict:         false,
		LastCheckedAt:   now.Add(-25 * time.Hour),
		NextCheckAt:     now.Add(-1 * time.Hour),
	}
	tm.store.EXPECT().
		GetTokenSpamVerdictsDueForCheck(gomock.Any(), schema.SpamSourceObjkt, 10).
		Return([]store.TokenSpamCheckItem{item}, nil).
		Times(1)
	tm.objktClient.EXPECT().
		GetToken(gomock.Any(), "KT1abc", "6").
		Return(&objkt.Token{Flag: types.StringPtr(objkt.FlagBanned)}, nil)
	tm.store.EXPECT().
		UpsertTokenSpamVerdict(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, input store.UpsertTokenSpamVerdictInput) (bool, error) {
			assert.Equal(t, schema.SpamSourceObjkt, input.Source)
			assert.True(t, input.Verdict)
			assert.JSONEq(t, `{"flag":"banned"}`, string(input.Detail))
			return true, nil
		})
	expectIdleAfterFirstCycle(tm)

	runOneSweep(t, tm)
}

// TestSpamVerdictSweeper_CleanVerdictAfterFailures_RestartsAtFloor pins that a
// transient vendor outage cannot silently demote a clean token's cadence. The
// failure path advances next_check_at while freezing last_checked_at, so the
// usual "previous interval = next_check_at − last_checked_at" derivation would
// measure the failure backoff instead — here 720h, which doubled and clamped
// would schedule the next check 30 days out on a token that is being polled
// precisely to catch a late takedown.
func TestSpamVerdictSweeper_CleanVerdictAfterFailures_RestartsAtFloor(t *testing.T) {
	tm := setupTestSpamSweeper(t)
	defer tearDownTestSpamSweeper(tm)

	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	tm.clock.EXPECT().Now().Return(now).AnyTimes()

	item := store.TokenSpamCheckItem{
		TokenID:         7,
		TokenCID:        testTokenCID("eip155:1", "erc721", "0xabc", "7"),
		ContractAddress: "0xabc",
		TokenNumber:     "7",
		Verdict:         false,
		// Five prior failures pinned next_check_at at the 720h ceiling while
		// last_checked_at stayed at the last successful confirmation.
		ConsecutiveFailures: 5,
		LastCheckedAt:       now.Add(-721 * time.Hour),
		NextCheckAt:         now.Add(-1 * time.Hour),
	}
	tm.store.EXPECT().
		GetTokenSpamVerdictsDueForCheck(gomock.Any(), schema.SpamSourceOpenSea, 10).
		Return([]store.TokenSpamCheckItem{item}, nil).
		Times(1)
	tm.openseaClient.EXPECT().
		GetNFT(gomock.Any(), "0xabc", "7").
		Return(&opensea.NFTMetadata{IsDisabled: false}, nil)
	tm.store.EXPECT().
		UpsertTokenSpamVerdict(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, input store.UpsertTokenSpamVerdictInput) (bool, error) {
			if assert.NotNil(t, input.NextCheckAt) {
				assert.Equal(t, now.Add(24*time.Hour), *input.NextCheckAt,
					"a recovered token restarts at the floor, not the failure ceiling")
			}
			return false, nil
		})
	expectIdleAfterFirstCycle(tm)

	runOneSweep(t, tm)
}

func TestSpamVerdictSweeper_NothingDue_Sleeps(t *testing.T) {
	tm := setupTestSpamSweeper(t)
	defer tearDownTestSpamSweeper(tm)

	tm.clock.EXPECT().Now().Return(time.Now()).AnyTimes()
	expectIdleAfterFirstCycle(tm)

	runOneSweep(t, tm)
}

func TestSpamVerdictSweeper_StopBeforeStart(t *testing.T) {
	tm := setupTestSpamSweeper(t)
	defer tearDownTestSpamSweeper(tm)

	require.NoError(t, tm.sweeper.Stop(context.Background()))
}

func TestSpamVerdictSweeper_DoubleStart(t *testing.T) {
	tm := setupTestSpamSweeper(t)
	defer tearDownTestSpamSweeper(tm)

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

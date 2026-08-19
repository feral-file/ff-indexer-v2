//go:build integration

package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	pgdriver "gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/plugin/dbresolver"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

var (
	testDB      *gorm.DB
	pgContainer *postgres.PostgresContainer
	// testDSN is the DSN testDB was opened with, for tests that need their own
	// connection (e.g. dbresolver routing, which cannot run inside the
	// transaction-per-test store).
	testDSN string
)

// TestMain sets up the test database before running tests
func TestMain(m *testing.M) {
	ctx := context.Background()

	// Check if we should use an external database (for CI or local development)
	dbHost := os.Getenv("TEST_DB_HOST")
	dbPort := os.Getenv("TEST_DB_PORT")
	dbUser := os.Getenv("TEST_DB_USER")
	dbPassword := os.Getenv("TEST_DB_PASSWORD")
	dbName := os.Getenv("TEST_DB_NAME")

	var dsn string
	var err error

	if dbHost != "" {
		// Use external database
		if dbPort == "" {
			dbPort = "5432"
		}
		if dbUser == "" {
			dbUser = "postgres"
		}
		if dbPassword == "" {
			dbPassword = "postgres"
		}
		if dbName == "" {
			dbName = "test_db"
		}

		dsn = fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
			dbHost, dbPort, dbUser, dbPassword, dbName)

		fmt.Printf("Using external database: %s:%s/%s\n", dbHost, dbPort, dbName)
	} else {
		// Start a PostgreSQL container for testing
		pgContainer, err = postgres.Run(ctx,
			"postgres:18-alpine",
			postgres.WithDatabase("test_db"),
			postgres.WithUsername("postgres"),
			postgres.WithPassword("postgres"),
			testcontainers.WithWaitStrategy(
				wait.ForLog("database system is ready to accept connections").
					WithOccurrence(2).
					WithStartupTimeout(30*time.Second)),
		)
		if err != nil {
			fmt.Printf("Failed to start PostgreSQL container: %v\n", err)
			os.Exit(1)
		}

		dsn, err = pgContainer.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			fmt.Printf("Failed to get connection string: %v\n", err)
			if err := pgContainer.Terminate(ctx); err != nil {
				fmt.Printf("Failed to terminate PostgreSQL container: %v\n", err)
			}
			os.Exit(1)
		}

		fmt.Printf("Started PostgreSQL container\n")
	}

	// Connect to the database
	testDSN = dsn
	testDB, err = gorm.Open(pgdriver.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		fmt.Printf("Failed to connect to database: %v\n", err)
		if pgContainer != nil {
			if err := pgContainer.Terminate(ctx); err != nil {
				fmt.Printf("Failed to terminate PostgreSQL container: %v\n", err)
			}
		}
		os.Exit(1)
	}

	// Initialize the database schema
	err = initializeTestDatabase(testDB)
	if err != nil {
		fmt.Printf("Failed to initialize database: %v\n", err)
		if pgContainer != nil {
			if err := pgContainer.Terminate(ctx); err != nil {
				fmt.Printf("Failed to terminate PostgreSQL container: %v\n", err)
			}
		}
		os.Exit(1)
	}

	// Run tests
	code := m.Run()

	// Cleanup
	if pgContainer != nil {
		if err := pgContainer.Terminate(ctx); err != nil {
			fmt.Printf("Failed to terminate PostgreSQL container: %v\n", err)
		}
	}

	os.Exit(code)
}

// initializeTestDatabase runs the schema initialization and seed data
func initializeTestDatabase(db *gorm.DB) error {
	sqlDB, err := db.DB()
	if err != nil {
		return fmt.Errorf("failed to get sql.DB: %w", err)
	}

	// Read and execute the schema initialization SQL
	schemaPath := filepath.Join("..", "..", "db", "init_pg_db.sql")
	schemaSQL, err := os.ReadFile(schemaPath) //nolint:gosec,G304
	if err != nil {
		return fmt.Errorf("failed to read schema file: %w", err)
	}

	_, err = sqlDB.Exec(string(schemaSQL))
	if err != nil {
		return fmt.Errorf("failed to execute schema: %w", err)
	}

	// Read and execute the test seed data SQL if it exists
	seedPath := filepath.Join("..", "..", "db", "pg_test_data.sql")
	if _, err := os.Stat(seedPath); err == nil {
		seedSQL, err := os.ReadFile(seedPath) //nolint:gosec,G304
		if err != nil {
			return fmt.Errorf("failed to read seed file: %w", err)
		}

		_, err = sqlDB.Exec(string(seedSQL))
		if err != nil {
			return fmt.Errorf("failed to execute seed data: %w", err)
		}
	}

	return nil
}

// initPGTestDB initializes a test database for each test
// This function creates a new store instance and ensures clean state
func initPGTestDB(t *testing.T) Store {
	// Start a transaction for test isolation
	tx := testDB.Begin()
	require.NotNil(t, tx)
	require.NoError(t, tx.Error)

	// Store the transaction in test context for cleanup
	t.Cleanup(func() {
		tx.Rollback()
	})

	return NewPGStore(tx)
}

// cleanupPGTestDB is called after each test to clean up
// With transaction-based isolation, this is handled by the t.Cleanup rollback
func cleanupPGTestDB(t *testing.T) {
	// Cleanup is handled by transaction rollback in t.Cleanup
}

// TestPostgreSQLStore runs all store tests against PostgreSQL
func TestPostgreSQLStore(t *testing.T) {
	if testDB == nil {
		t.Fatal("Test database not initialized")
	}

	RunStoreTests(t, initPGTestDB, cleanupPGTestDB)
}

// TestConcurrentUpsertRelease verifies that UpsertRelease is safe under concurrent callers
// racing to create the same (vendor, vendor_release_id) row.
//
// This test intentionally uses the raw connection pool (testDB), not the transaction-wrapped
// store returned by initPGTestDB. Transaction-backed stores share a single connection and are
// not safe for concurrent goroutine use; the pool-backed store exercises the real concurrent
// access pattern seen in production where multiple token workers race to upsert the same new
// release. Cleanup is handled via a manual DELETE after the test.
//
// The previous FirstOrCreate implementation issued SELECT then INSERT in two separate
// statements, causing a unique-constraint race. The current ON CONFLICT path is atomic:
// all goroutines must receive the same release id with no error.
func TestConcurrentUpsertRelease(t *testing.T) {
	if testDB == nil {
		t.Skip("Test database not initialized")
	}

	const concurrency = 10
	vendor := schema.VendorArtBlocks
	vendorReleaseID := "0x000000000000000000000000000000000000eeee-concurrent-test"

	// Clean up after test regardless of outcome.
	t.Cleanup(func() {
		testDB.Exec("DELETE FROM releases WHERE vendor_release_id = ?", vendorReleaseID)
	})

	store := NewPGStore(testDB)

	var wg sync.WaitGroup
	ids := make([]uint64, concurrency)
	errs := make([]error, concurrency)

	for i := range concurrency {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			r, err := store.UpsertRelease(context.Background(), vendor, vendorReleaseID, nil, nil, nil)
			if err == nil {
				ids[idx] = r.ID
			}
			errs[idx] = err
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		require.NoError(t, err, "goroutine %d got error", i)
	}

	// All goroutines must have resolved the same release row.
	first := ids[0]
	require.NotZero(t, first)
	for i, id := range ids {
		assert.Equal(t, first, id, "goroutine %d returned a different release id", i)
	}
}

// TestModerationVerdictSurvivesConcurrentOwnershipWrite pins that an ownership write
// cannot silently revert a spam verdict that commits while it is in flight.
//
// UpdateTokenTransfer reads the token row, sets current_owner, and writes back.
// When that write-back was a full-row Save(), a verdict committing between the
// read and the write was overwritten with the stale value: token_moderation_verdicts
// still said spam, tokens.moderation_status was back to false, and nothing re-flipped it
// until the sweeper came round (24h at the earliest, 720h once the row had
// backed off).
//
// A sequential test cannot catch this — the read would already see the fresh
// verdict — so this races the two writers and asserts the invariant that
// actually matters: whenever a verdict row says spam, the materialized flag
// agrees. Like TestConcurrentUpsertRelease it uses the pool-backed testDB, since
// the transaction-wrapped store from initPGTestDB shares one connection and
// cannot be driven from concurrent goroutines.
func TestModerationVerdictSurvivesConcurrentOwnershipWrite(t *testing.T) {
	if testDB == nil {
		t.Skip("Test database not initialized")
	}

	store := NewPGStore(testDB)
	ctx := context.Background()
	const contract = "0x000000000000000000000000000000000000dddd"

	t.Cleanup(func() {
		testDB.Exec(`DELETE FROM token_moderation_verdicts WHERE token_id IN
			(SELECT id FROM tokens WHERE contract_address = ?)`, contract)
		testDB.Exec(`DELETE FROM provenance_events WHERE token_id IN
			(SELECT id FROM tokens WHERE contract_address = ?)`, contract)
		testDB.Exec(`DELETE FROM token_events WHERE token_id IN
			(SELECT id FROM tokens WHERE contract_address = ?)`, contract)
		testDB.Exec(`DELETE FROM balances WHERE token_id IN
			(SELECT id FROM tokens WHERE contract_address = ?)`, contract)
		testDB.Exec(`DELETE FROM tokens WHERE contract_address = ?`, contract)
	})

	const rounds = 12
	for round := range rounds {
		owner1 := fmt.Sprintf("0xowner_race_a_%02d", round)
		owner2 := fmt.Sprintf("0xowner_race_b_%02d", round)

		mintInput := buildTestTokenMint(
			domain.ChainEthereumMainnet,
			domain.StandardERC721,
			contract,
			fmt.Sprintf("%d", 9000+round),
			owner1,
		)
		require.NoError(t, store.CreateTokenMint(ctx, mintInput))

		token, err := store.GetTokenByTokenCID(ctx, mintInput.Token.TokenCID)
		require.NoError(t, err)
		require.NotNil(t, token)

		start := make(chan struct{})
		var wg sync.WaitGroup
		errs := make(chan error, 2)

		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, uErr := store.UpsertTokenModerationVerdict(ctx, UpsertTokenModerationVerdictInput{
				TokenID: token.ID,
				Source:  schema.ModerationSourceOpenSea,
				Verdict: schema.ModerationStatusSpam,
				Detail:  []byte(`{"is_disabled":true}`),
			})
			errs <- uErr
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs <- store.UpdateTokenTransfer(ctx, UpdateTokenTransferInput{
				TokenCID:     mintInput.Token.TokenCID,
				CurrentOwner: &owner2,
				SenderBalanceUpdate: &UpdateBalanceInput{
					OwnerAddress: owner1,
					Delta:        "1",
				},
				ReceiverBalanceUpdate: &UpdateBalanceInput{
					OwnerAddress: owner2,
					Delta:        "1",
				},
				ProvenanceEvent: buildTestProvenanceEvent(
					domain.ChainEthereumMainnet,
					schema.ProvenanceEventTypeTransfer,
					&owner1,
					&owner2,
					"1",
					fmt.Sprintf("0xrace%02d", round),
					uint64(2000+round),
				),
			})
		}()

		close(start)
		wg.Wait()
		close(errs)
		for e := range errs {
			require.NoError(t, e)
		}

		var verdict schema.TokenModerationVerdict
		require.NoError(t, testDB.Where("token_id = ? AND source = ?",
			token.ID, schema.ModerationSourceOpenSea).First(&verdict).Error)
		require.Equal(t, schema.ModerationStatusSpam, verdict.Verdict, "round %d: verdict row must record spam", round)

		after, err := store.GetTokenByTokenCID(ctx, mintInput.Token.TokenCID)
		require.NoError(t, err)
		require.NotNil(t, after)
		assert.Equal(t, schema.ModerationStatusSpam, after.ModerationStatus,
			"round %d: verdict row says spam but tokens.moderation_status was reverted by the concurrent transfer", round)
		require.NotNil(t, after.CurrentOwner, "round %d: transfer must still have applied", round)
		assert.Equal(t, owner2, *after.CurrentOwner, "round %d: transfer must still have applied", round)
	}
}

// TestStaleSweeperVerdictDoesNotOverwriteNewer pins the compare-and-set on
// UpsertTokenModerationVerdictInput.ExpectedLastCheckedAt.
//
// The sweeper reads a due row, then waits on a rate-limited vendor request before
// writing. If the enricher persists a fresher verdict for the same (token, source)
// during that window, the sweeper's older response must not land on top of it. The
// tokens-row lock serializes the two writes but cannot order the responses, so
// without the guard the last writer wins regardless of which response is newer —
// and the stale verdict would stand until the next sweep, 24h at the earliest.
//
// Simulated deterministically rather than by racing goroutines: capture what the
// sweeper would have read, let the enricher write, then attempt the sweeper's
// write with the now-stale expectation.
func TestStaleSweeperVerdictDoesNotOverwriteNewer(t *testing.T) {
	if testDB == nil {
		t.Skip("Test database not initialized")
	}

	store := NewPGStore(testDB)
	ctx := context.Background()
	const contract = "0x000000000000000000000000000000000000cccc"

	t.Cleanup(func() {
		testDB.Exec(`DELETE FROM token_moderation_verdicts WHERE token_id IN
			(SELECT id FROM tokens WHERE contract_address = ?)`, contract)
		testDB.Exec(`DELETE FROM token_events WHERE token_id IN
			(SELECT id FROM tokens WHERE contract_address = ?)`, contract)
		testDB.Exec(`DELETE FROM balances WHERE token_id IN
			(SELECT id FROM tokens WHERE contract_address = ?)`, contract)
		testDB.Exec(`DELETE FROM tokens WHERE contract_address = ?`, contract)
	})

	mintInput := buildTestTokenMint(
		domain.ChainEthereumMainnet,
		domain.StandardERC721,
		contract,
		"1",
		"0xowner_stale_race",
	)
	require.NoError(t, store.CreateTokenMint(ctx, mintInput))
	token, err := store.GetTokenByTokenCID(ctx, mintInput.Token.TokenCID)
	require.NoError(t, err)
	require.NotNil(t, token)

	// Seed the row the sweeper would later find due: vendor said clean.
	seedNext := time.Now().Add(-time.Hour)
	_, err = store.UpsertTokenModerationVerdict(ctx, UpsertTokenModerationVerdictInput{
		TokenID:     token.ID,
		Source:      ModerationSourceForTest(),
		Verdict:     schema.ModerationStatusNone,
		Detail:      []byte(`{"is_disabled":false}`),
		NextCheckAt: &seedNext,
	})
	require.NoError(t, err)

	// The sweeper picks it up and holds this snapshot while it calls the vendor.
	due, err := store.GetTokenModerationVerdictsDueForCheck(ctx, ModerationSourceForTest(), 10)
	require.NoError(t, err)
	require.Len(t, due, 1)
	sweeperSnapshot := due[0]

	// Mid-flight, the enricher persists a fresher verdict: the vendor has since
	// flagged the token.
	enricherNext := time.Now().Add(24 * time.Hour)
	changed, err := store.UpsertTokenModerationVerdict(ctx, UpsertTokenModerationVerdictInput{
		TokenID:     token.ID,
		Source:      ModerationSourceForTest(),
		Verdict:     schema.ModerationStatusSpam,
		Detail:      []byte(`{"is_disabled":true}`),
		NextCheckAt: &enricherNext,
	})
	require.NoError(t, err)
	require.True(t, changed, "the enricher's write should have flipped the combined verdict")

	// Now the sweeper's older response arrives and tries to write "clean".
	sweeperNext := time.Now().Add(48 * time.Hour)
	changed, err = store.UpsertTokenModerationVerdict(ctx, UpsertTokenModerationVerdictInput{
		TokenID:               token.ID,
		Source:                ModerationSourceForTest(),
		Verdict:               schema.ModerationStatusNone,
		Detail:                []byte(`{"is_disabled":false}`),
		NextCheckAt:           &sweeperNext,
		ExpectedLastCheckedAt: &sweeperSnapshot.LastCheckedAt,
	})
	require.NoError(t, err, "losing the race is a no-op, not an error")
	assert.False(t, changed, "a dropped write must not report a verdict change")

	var row schema.TokenModerationVerdict
	require.NoError(t, testDB.Where("token_id = ? AND source = ?",
		token.ID, ModerationSourceForTest()).First(&row).Error)
	assert.Equal(t, schema.ModerationStatusSpam, row.Verdict, "the newer enricher verdict must survive the stale sweeper write")

	after, err := store.GetTokenByTokenCID(ctx, mintInput.Token.TokenCID)
	require.NoError(t, err)
	assert.Equal(t, schema.ModerationStatusSpam, after.ModerationStatus, "materialized flag must still reflect the newer verdict")

}

// TestCurrentSweeperVerdictStillApplies is the other half of the compare-and-set
// contract, and the more dangerous one to get wrong: the guard must reject only
// stale responses. If the timestamp did not survive the round trip through
// Postgres timestamptz and back into TokenModerationCheckItem, every sweeper write
// would be rejected and the sweeper would silently stop persisting anything —
// worse than the overwrite bug the guard exists to prevent, and invisible.
//
// So the expectation here is deliberately taken from GetTokenModerationVerdictsDueForCheck,
// the query the sweeper actually reads from, rather than from a direct row read.
// Seeding next_check_at in the past is what makes that query return the row.
func TestCurrentSweeperVerdictStillApplies(t *testing.T) {
	if testDB == nil {
		t.Skip("Test database not initialized")
	}

	store := NewPGStore(testDB)
	ctx := context.Background()
	const contract = "0x000000000000000000000000000000000000bbbb"

	t.Cleanup(func() {
		testDB.Exec(`DELETE FROM token_moderation_verdicts WHERE token_id IN
			(SELECT id FROM tokens WHERE contract_address = ?)`, contract)
		testDB.Exec(`DELETE FROM token_events WHERE token_id IN
			(SELECT id FROM tokens WHERE contract_address = ?)`, contract)
		testDB.Exec(`DELETE FROM balances WHERE token_id IN
			(SELECT id FROM tokens WHERE contract_address = ?)`, contract)
		testDB.Exec(`DELETE FROM tokens WHERE contract_address = ?`, contract)
	})

	mintInput := buildTestTokenMint(
		domain.ChainEthereumMainnet,
		domain.StandardERC721,
		contract,
		"1",
		"0xowner_cas_positive",
	)
	require.NoError(t, store.CreateTokenMint(ctx, mintInput))
	token, err := store.GetTokenByTokenCID(ctx, mintInput.Token.TokenCID)
	require.NoError(t, err)
	require.NotNil(t, token)

	overdue := time.Now().Add(-time.Hour)
	_, err = store.UpsertTokenModerationVerdict(ctx, UpsertTokenModerationVerdictInput{
		TokenID:     token.ID,
		Source:      ModerationSourceForTest(),
		Verdict:     schema.ModerationStatusNone,
		Detail:      []byte(`{"is_disabled":false}`),
		NextCheckAt: &overdue,
	})
	require.NoError(t, err)

	due, err := store.GetTokenModerationVerdictsDueForCheck(ctx, ModerationSourceForTest(), 10)
	require.NoError(t, err)
	require.Len(t, due, 1, "the seeded row must be due, otherwise this test proves nothing")

	next := time.Now().Add(24 * time.Hour)
	changed, err := store.UpsertTokenModerationVerdict(ctx, UpsertTokenModerationVerdictInput{
		TokenID:               token.ID,
		Source:                ModerationSourceForTest(),
		Verdict:               schema.ModerationStatusSpam,
		Detail:                []byte(`{"is_disabled":true}`),
		NextCheckAt:           &next,
		ExpectedLastCheckedAt: &due[0].LastCheckedAt,
	})
	require.NoError(t, err)
	assert.True(t, changed,
		"a write whose expectation came straight from the due query must apply; "+
			"if this fails the guard rejects every sweeper write and the sweeper is a no-op")

	after, err := store.GetTokenByTokenCID(ctx, mintInput.Token.TokenCID)
	require.NoError(t, err)
	assert.Equal(t, schema.ModerationStatusSpam, after.ModerationStatus, "the applied verdict must be materialized")
}

// ModerationSourceForTest keeps the source choice in one place for the race tests.
func ModerationSourceForTest() schema.ModerationSource { return schema.ModerationSourceOpenSea }

// TestStaleSweeperFailureDoesNotDeferFreshRow is the failure-path counterpart to
// TestStaleSweeperVerdictDoesNotOverwriteNewer.
//
// A failed vendor request is exactly as stale as a successful one, and landing it
// after a newer enrichment does more damage: the backoff is computed by the
// sweeper from the consecutive_failures it read, while the SQL increments whatever
// is stored now. A row the enricher just reset to zero failures would take a
// long-backoff next_check_at while recording a single failure — at the sweeper's
// max, a token that was just confirmed clean gets deferred 30 days.
func TestStaleSweeperFailureDoesNotDeferFreshRow(t *testing.T) {
	if testDB == nil {
		t.Skip("Test database not initialized")
	}

	store := NewPGStore(testDB)
	ctx := context.Background()
	const contract = "0x000000000000000000000000000000000000aaab"

	t.Cleanup(func() {
		testDB.Exec(`DELETE FROM token_moderation_verdicts WHERE token_id IN
			(SELECT id FROM tokens WHERE contract_address = ?)`, contract)
		testDB.Exec(`DELETE FROM token_events WHERE token_id IN
			(SELECT id FROM tokens WHERE contract_address = ?)`, contract)
		testDB.Exec(`DELETE FROM balances WHERE token_id IN
			(SELECT id FROM tokens WHERE contract_address = ?)`, contract)
		testDB.Exec(`DELETE FROM tokens WHERE contract_address = ?`, contract)
	})

	mintInput := buildTestTokenMint(
		domain.ChainEthereumMainnet,
		domain.StandardERC721,
		contract,
		"1",
		"0xowner_stale_failure",
	)
	require.NoError(t, store.CreateTokenMint(ctx, mintInput))
	token, err := store.GetTokenByTokenCID(ctx, mintInput.Token.TokenCID)
	require.NoError(t, err)
	require.NotNil(t, token)

	overdue := time.Now().Add(-time.Hour)
	_, err = store.UpsertTokenModerationVerdict(ctx, UpsertTokenModerationVerdictInput{
		TokenID:     token.ID,
		Source:      ModerationSourceForTest(),
		Verdict:     schema.ModerationStatusNone,
		Detail:      []byte(`{"is_disabled":false}`),
		NextCheckAt: &overdue,
	})
	require.NoError(t, err)

	// The sweeper picks it up and holds this snapshot while its vendor call runs.
	due, err := store.GetTokenModerationVerdictsDueForCheck(ctx, ModerationSourceForTest(), 10)
	require.NoError(t, err)
	require.Len(t, due, 1)
	sweeperSnapshot := due[0]

	// Mid-flight the enricher succeeds and puts the row on a fresh 24h schedule.
	enricherNext := time.Now().Add(24 * time.Hour)
	_, err = store.UpsertTokenModerationVerdict(ctx, UpsertTokenModerationVerdictInput{
		TokenID:     token.ID,
		Source:      ModerationSourceForTest(),
		Verdict:     schema.ModerationStatusNone,
		Detail:      []byte(`{"is_disabled":false}`),
		NextCheckAt: &enricherNext,
	})
	require.NoError(t, err)

	fresh := getVerdictRowForTest(t, token.ID)
	require.Equal(t, 0, fresh.ConsecutiveFailures, "a successful write clears the failure state")

	// Now the sweeper's failed request lands, carrying the max backoff it computed
	// from the stale snapshot.
	staleBackoff := time.Now().Add(720 * time.Hour)
	applied, err := store.RecordTokenModerationCheckFailure(
		ctx, token.ID, ModerationSourceForTest(), "opensea: 502 bad gateway",
		staleBackoff, sweeperSnapshot.LastCheckedAt)
	require.NoError(t, err, "losing the race is a no-op, not an error")
	assert.False(t, applied, "a stale failure must not report as applied")

	after := getVerdictRowForTest(t, token.ID)
	assert.Equal(t, 0, after.ConsecutiveFailures,
		"the stale failure must not increment a counter the enricher just cleared")
	assert.Nil(t, after.LastError, "the stale failure must not stamp an error on a healthy row")
	require.NotNil(t, after.NextCheckAt)
	assert.WithinDuration(t, enricherNext, *after.NextCheckAt, time.Second,
		"the enricher's fresh schedule must survive; a 720h deferral here would hide the token for 30 days")

	// A failure whose expectation is current still applies, so the guard rejects
	// only stale responses rather than disabling failure tracking entirely. The
	// expectation deliberately comes from GetTokenModerationVerdictsDueForCheck — the
	// query the sweeper actually reads — because that is the round trip that
	// would silently reject every failure write if it ever stopped comparing
	// equal. Rewind next_check_at so the row is due again and the query returns it.
	require.NoError(t, testDB.Exec(
		`UPDATE token_moderation_verdicts SET next_check_at = ? WHERE token_id = ? AND source = ?`,
		time.Now().Add(-time.Hour), token.ID, ModerationSourceForTest()).Error)
	redue, err := store.GetTokenModerationVerdictsDueForCheck(ctx, ModerationSourceForTest(), 10)
	require.NoError(t, err)
	require.Len(t, redue, 1, "the rewound row must be due, otherwise this proves nothing")

	currentBackoff := time.Now().Add(time.Hour)
	applied, err = store.RecordTokenModerationCheckFailure(
		ctx, token.ID, ModerationSourceForTest(), "opensea: 502 bad gateway",
		currentBackoff, redue[0].LastCheckedAt)
	require.NoError(t, err)
	assert.True(t, applied, "a failure with a current expectation must apply")

	final := getVerdictRowForTest(t, token.ID)
	assert.Equal(t, 1, final.ConsecutiveFailures)
}

// getVerdictRowForTest reads the verdict row straight from the DB for assertions
// on columns the store API does not surface.
func getVerdictRowForTest(t *testing.T, tokenID uint64) schema.TokenModerationVerdict {
	t.Helper()
	var row schema.TokenModerationVerdict
	require.NoError(t, testDB.Where("token_id = ? AND source = ?",
		tokenID, ModerationSourceForTest()).First(&row).Error)
	return row
}

// TestGetAddressIndexingThrottleState_ReplicaRoutedToPrimary exercises the
// dbresolver branch of the throttle-state reads: with a resolver registered,
// both queries must run under the dbresolver.Write clause (the throttle is a
// credit-protection gate, and a lagging replica returning "no terminal job"
// would wave a costly scan through the window). The replica here points at the
// same database — the test proves the primary-routing code path executes and
// returns the just-written terminal state, not replica lag itself, which would
// need a second server.
func TestGetAddressIndexingThrottleState_ReplicaRoutedToPrimary(t *testing.T) {
	if testDB == nil {
		t.Fatal("Test database not initialized")
	}

	db, err := gorm.Open(pgdriver.Open(testDSN), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	require.NoError(t, db.Use(dbresolver.Register(dbresolver.Config{
		Replicas: []gorm.Dialector{pgdriver.Open(testDSN)},
	})))
	require.True(t, hasDBResolver(db), "harness must actually register a resolver")

	st := NewPGStore(db)
	ctx := context.Background()
	const addr = "0xthrottle-replica-routing"

	// Written through the same resolver-enabled store: the insert goes to the
	// primary, and the throttle read must see it via the Write clause.
	uk := "thr-replica-1"
	j, _, err := st.EnqueueJob(ctx, EnqueueJobInput{
		Queue:     "test_addr_idx",
		Kind:      "IndexTokenOwner",
		Payload:   []byte(`[]`),
		UniqueKey: &uk,
	})
	require.NoError(t, err)
	require.NoError(t, st.CreateAddressIndexingJob(ctx, CreateAddressIndexingJobInput{
		Address: addr,
		Chain:   domain.ChainEthereumMainnet,
		Status:  schema.IndexingJobStatusRunning,
		JobID:   j.ID,
	}))
	require.NoError(t, st.UpdateAddressIndexingJobStatus(ctx, j.ID, schema.IndexingJobStatusFailed, time.Now().UTC()))

	state, err := st.GetAddressIndexingThrottleState(ctx, addr, domain.ChainEthereumMainnet)
	require.NoError(t, err)
	require.NotNil(t, state.LatestTerminal)
	require.Equal(t, schema.IndexingJobStatusFailed, state.LatestTerminal.Status)
	require.Equal(t, 1, state.ConsecutiveFailures)
}

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

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

var (
	testDB      *gorm.DB
	pgContainer *postgres.PostgresContainer
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

// TestSpamVerdictSurvivesConcurrentOwnershipWrite pins that an ownership write
// cannot silently revert a spam verdict that commits while it is in flight.
//
// UpdateTokenTransfer reads the token row, sets current_owner, and writes back.
// When that write-back was a full-row Save(), a verdict committing between the
// read and the write was overwritten with the stale value: token_spam_verdicts
// still said spam, tokens.is_spam was back to false, and nothing re-flipped it
// until the sweeper came round (24h at the earliest, 720h once the row had
// backed off).
//
// A sequential test cannot catch this — the read would already see the fresh
// verdict — so this races the two writers and asserts the invariant that
// actually matters: whenever a verdict row says spam, the materialized flag
// agrees. Like TestConcurrentUpsertRelease it uses the pool-backed testDB, since
// the transaction-wrapped store from initPGTestDB shares one connection and
// cannot be driven from concurrent goroutines.
func TestSpamVerdictSurvivesConcurrentOwnershipWrite(t *testing.T) {
	if testDB == nil {
		t.Skip("Test database not initialized")
	}

	store := NewPGStore(testDB)
	ctx := context.Background()
	const contract = "0x000000000000000000000000000000000000dddd"

	t.Cleanup(func() {
		testDB.Exec(`DELETE FROM token_spam_verdicts WHERE token_id IN
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
			_, uErr := store.UpsertTokenSpamVerdict(ctx, UpsertTokenSpamVerdictInput{
				TokenID: token.ID,
				Source:  schema.SpamSourceOpenSea,
				Verdict: true,
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

		var verdict schema.TokenSpamVerdict
		require.NoError(t, testDB.Where("token_id = ? AND source = ?",
			token.ID, schema.SpamSourceOpenSea).First(&verdict).Error)
		require.True(t, verdict.Verdict, "round %d: verdict row must record spam", round)

		after, err := store.GetTokenByTokenCID(ctx, mintInput.Token.TokenCID)
		require.NoError(t, err)
		require.NotNil(t, after)
		assert.True(t, after.IsSpam,
			"round %d: verdict row says spam but tokens.is_spam was reverted by the concurrent transfer", round)
		require.NotNil(t, after.CurrentOwner, "round %d: transfer must still have applied", round)
		assert.Equal(t, owner2, *after.CurrentOwner, "round %d: transfer must still have applied", round)
	}
}

// TestStaleSweeperVerdictDoesNotOverwriteNewer pins the compare-and-set on
// UpsertTokenSpamVerdictInput.ExpectedLastCheckedAt.
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
		testDB.Exec(`DELETE FROM token_spam_verdicts WHERE token_id IN
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
	_, err = store.UpsertTokenSpamVerdict(ctx, UpsertTokenSpamVerdictInput{
		TokenID:     token.ID,
		Source:      SpamSourceForTest(),
		Verdict:     false,
		Detail:      []byte(`{"is_disabled":false}`),
		NextCheckAt: &seedNext,
	})
	require.NoError(t, err)

	// The sweeper picks it up and holds this snapshot while it calls the vendor.
	due, err := store.GetTokenSpamVerdictsDueForCheck(ctx, SpamSourceForTest(), 10)
	require.NoError(t, err)
	require.Len(t, due, 1)
	sweeperSnapshot := due[0]

	// Mid-flight, the enricher persists a fresher verdict: the vendor has since
	// flagged the token.
	enricherNext := time.Now().Add(24 * time.Hour)
	changed, err := store.UpsertTokenSpamVerdict(ctx, UpsertTokenSpamVerdictInput{
		TokenID:     token.ID,
		Source:      SpamSourceForTest(),
		Verdict:     true,
		Detail:      []byte(`{"is_disabled":true}`),
		NextCheckAt: &enricherNext,
	})
	require.NoError(t, err)
	require.True(t, changed, "the enricher's write should have flipped the combined verdict")

	// Now the sweeper's older response arrives and tries to write "clean".
	sweeperNext := time.Now().Add(48 * time.Hour)
	changed, err = store.UpsertTokenSpamVerdict(ctx, UpsertTokenSpamVerdictInput{
		TokenID:               token.ID,
		Source:                SpamSourceForTest(),
		Verdict:               false,
		Detail:                []byte(`{"is_disabled":false}`),
		NextCheckAt:           &sweeperNext,
		ExpectedLastCheckedAt: &sweeperSnapshot.LastCheckedAt,
	})
	require.NoError(t, err, "losing the race is a no-op, not an error")
	assert.False(t, changed, "a dropped write must not report a verdict change")

	var row schema.TokenSpamVerdict
	require.NoError(t, testDB.Where("token_id = ? AND source = ?",
		token.ID, SpamSourceForTest()).First(&row).Error)
	assert.True(t, row.Verdict, "the newer enricher verdict must survive the stale sweeper write")

	after, err := store.GetTokenByTokenCID(ctx, mintInput.Token.TokenCID)
	require.NoError(t, err)
	assert.True(t, after.IsSpam, "materialized flag must still reflect the newer verdict")

	// And the sweeper's write with a current expectation still applies, so the
	// guard rejects only stale responses rather than disabling the sweeper.
	fresh, err := store.GetTokenSpamVerdictsDueForCheck(ctx, SpamSourceForTest(), 10)
	require.NoError(t, err)
	if len(fresh) == 0 {
		// Enricher pushed next_check_at into the future, as expected; read the row
		// directly to get the current last_checked_at.
		fresh = []TokenSpamCheckItem{{TokenID: token.ID, LastCheckedAt: row.LastCheckedAt}}
	}
	clearNext := time.Now().Add(72 * time.Hour)
	changed, err = store.UpsertTokenSpamVerdict(ctx, UpsertTokenSpamVerdictInput{
		TokenID:               token.ID,
		Source:                SpamSourceForTest(),
		Verdict:               false,
		Detail:                []byte(`{"is_disabled":false}`),
		NextCheckAt:           &clearNext,
		ExpectedLastCheckedAt: &fresh[0].LastCheckedAt,
	})
	require.NoError(t, err)
	assert.True(t, changed, "a write with a current expectation must still apply")
}

// SpamSourceForTest keeps the source choice in one place for the race tests.
func SpamSourceForTest() schema.SpamSource { return schema.SpamSourceOpenSea }

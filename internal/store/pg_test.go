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
	ids := make([]int64, concurrency)
	errs := make([]error, concurrency)

	for i := range concurrency {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			r, err := store.UpsertRelease(context.Background(), vendor, vendorReleaseID)
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

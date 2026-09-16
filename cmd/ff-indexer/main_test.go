package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/config"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
)

type stubWaitGroup struct {
	err error
}

func (s stubWaitGroup) Wait() error {
	return s.err
}

func TestMain(m *testing.M) {
	if err := logger.Initialize(logger.Config{Debug: false}); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

func TestWaitForSubsystems_ReturnsErrorOnSubsystemFailure(t *testing.T) {
	cleanupCalls := 0
	cleanup := func(context.Context) error {
		cleanupCalls++
		return nil
	}

	err := waitForSubsystems(context.Background(), stubWaitGroup{err: errors.New("boom")}, cleanup, cleanup)

	assert.EqualError(t, err, "boom")
	assert.Equal(t, 2, cleanupCalls)
}

func TestWaitForSubsystems_IgnoresContextCanceled(t *testing.T) {
	cleanupCalls := 0
	cleanup := func(context.Context) error {
		cleanupCalls++
		return nil
	}

	err := waitForSubsystems(context.Background(), stubWaitGroup{err: context.Canceled}, cleanup, cleanup)

	assert.NoError(t, err)
	assert.Equal(t, 2, cleanupCalls)
}

func TestDatabaseFailureFatalLogOmitsPassword(t *testing.T) {
	const (
		helperEnv = "FF_INDEXER_TEST_DATABASE_FATAL"
		urlEnv    = "FF_INDEXER_TEST_LOG_STREAM_URL"
		password  = "database-password-must-not-leak"
	)
	if os.Getenv(helperEnv) == "1" {
		require.NoError(t, logger.Initialize(logger.Config{
			CloudflareStreamURL: os.Getenv(urlEnv),
			CloudflareAPIToken:  "test-token",
			Environment:         "test",
		}))
		database := config.DatabaseConfig{
			Host:     "database.internal",
			Port:     5432,
			DBName:   "ff_indexer",
			Password: password,
		}
		logger.Fatal("Failed to connect to database",
			append([]zap.Field{zap.Error(errors.New("connection refused"))}, databaseConnectionLogFields(database)...)...,
		)
		return
	}

	requests := make(chan []byte, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		requests <- body
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	cmd := exec.Command(os.Args[0], "-test.run=^TestDatabaseFailureFatalLogOmitsPassword$") //nolint:gosec // Run this test binary as the fatal-path helper.
	cmd.Env = append(os.Environ(), helperEnv+"=1", urlEnv+"="+server.URL)
	err := cmd.Run()
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr)
	assert.Equal(t, 1, exitErr.ExitCode())

	select {
	case body := <-requests:
		assert.NotContains(t, string(body), password)
		assert.Contains(t, string(body), "database.internal")
		assert.Contains(t, string(body), "ff_indexer")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for fatal log delivery")
	}
}

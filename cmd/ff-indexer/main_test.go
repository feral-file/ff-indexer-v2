package main

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

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

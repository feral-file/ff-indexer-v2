package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWaitForEmitterShutdown_WaitsForEmitterRunOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	natsClosed := make(chan struct{})
	resultCh := make(chan error, 1)

	go func() {
		resultCh <- waitForEmitterShutdown(ctx, errCh, natsClosed, "ethereum")
	}()

	cancel()

	select {
	case <-resultCh:
		t.Fatal("waitForEmitterShutdown returned before emitter goroutine finished")
	case <-time.After(50 * time.Millisecond):
	}

	errCh <- context.Canceled

	select {
	case err := <-resultCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("waitForEmitterShutdown did not return after emitter goroutine finished")
	}
}

func TestWaitForEmitterShutdown_ReturnsEmitterErrorDuringShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	natsClosed := make(chan struct{})
	resultCh := make(chan error, 1)

	go func() {
		resultCh <- waitForEmitterShutdown(ctx, errCh, natsClosed, "tezos")
	}()

	cancel()
	errCh <- errors.New("publish failed")

	select {
	case err := <-resultCh:
		assert.EqualError(t, err, "publish failed")
	case <-time.After(time.Second):
		t.Fatal("waitForEmitterShutdown did not return emitter error")
	}
}

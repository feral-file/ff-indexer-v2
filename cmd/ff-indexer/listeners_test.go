package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/mocks"
)

func TestRunIngestion_WaitsForRunnerOnContextCancel(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx, cancel := context.WithCancel(context.Background())
	mockRunner := mocks.NewMockIngestionRunner(ctrl)
	resultCh := make(chan error, 1)

	mockRunner.EXPECT().Run(gomock.Any()).DoAndReturn(func(ctx context.Context) error {
		<-ctx.Done()
		return context.Canceled
	})

	go func() {
		resultCh <- runIngestion(ctx, mockRunner, "ethereum")
	}()

	cancel()

	select {
	case err := <-resultCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("runIngestion did not return after ingestion goroutine finished")
	}
}

func TestRunIngestion_ReturnsRunnerErrorDuringShutdown(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx, cancel := context.WithCancel(context.Background())
	mockRunner := mocks.NewMockIngestionRunner(ctrl)
	resultCh := make(chan error, 1)

	mockRunner.EXPECT().Run(gomock.Any()).DoAndReturn(func(ctx context.Context) error {
		<-ctx.Done()
		return errors.New("dispatch failed")
	})

	go func() {
		resultCh <- runIngestion(ctx, mockRunner, "tezos")
	}()

	cancel()

	select {
	case err := <-resultCh:
		assert.EqualError(t, err, "dispatch failed")
	case <-time.After(time.Second):
		t.Fatal("runIngestion did not return ingestion error")
	}
}

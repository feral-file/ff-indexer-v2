//go:build !cgo

package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/config"
)

func TestRegisterWorkerMediaStub_DisabledMediaIsNoOp(t *testing.T) {
	run, cleanup, err := registerWorkerMedia(context.Background(), &config.AppConfig{
		MediaEnabled: false,
	}, nil)
	require.NoError(t, err)
	require.NotNil(t, run)
	require.NotNil(t, cleanup)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.ErrorIs(t, run(ctx), context.Canceled)
	require.NoError(t, cleanup(context.Background()))
}

func TestRegisterWorkerMediaStub_EnabledMediaFailsFast(t *testing.T) {
	run, cleanup, err := registerWorkerMedia(context.Background(), &config.AppConfig{
		MediaEnabled: true,
	}, nil)

	require.Error(t, err)
	require.EqualError(t, err, "media worker requires CGO_ENABLED=1 when media_enabled=true")
	require.Nil(t, run)
	require.Nil(t, cleanup)
}

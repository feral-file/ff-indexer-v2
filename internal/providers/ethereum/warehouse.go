package ethereum

import (
	"context"
	"errors"
	"fmt"
	"math/big"

	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
)

// ErrLogWarehouseUnreachable marks a startup chain check that could not reach
// the warehouse. It is a warning, not a startup failure: the routing client
// falls through to the vendor until the warehouse answers.
var ErrLogWarehouseUnreachable = errors.New("log warehouse unreachable")

// ErrLogWarehouseChainMismatch marks a warehouse that stores a different chain
// than the indexer is configured for. It is fatal: serving Sepolia history as
// mainnet (or vice versa) would silently corrupt every scan.
var ErrLogWarehouseChainMismatch = errors.New("log warehouse chain mismatch")

// CheckLogWarehouseChain verifies at startup that the warehouse serves the
// indexer's chain. Returns ErrLogWarehouseUnreachable (wrapped) when the
// warehouse does not answer — the caller logs and continues — and
// ErrLogWarehouseChainMismatch (wrapped) when it answers with another chain.
//
// Reason: the warehouse endpoint is a plain URL with no chain in it, so a
// deploy-config slip (pointing a testnet indexer at the mainnet warehouse)
// would otherwise go unnoticed until the data was wrong. The check only runs
// once, at startup, because the warehouse's chain cannot change underneath a
// running process.
func CheckLogWarehouseChain(ctx context.Context, warehouse adapter.LogWarehouse, chain domain.Chain) error {
	want, ok := chain.EIP155NumericID()
	if !ok {
		return fmt.Errorf("%w: chain %q is not an eip155 chain", ErrLogWarehouseChainMismatch, chain)
	}
	got, err := warehouse.ChainID(ctx)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrLogWarehouseUnreachable, err)
	}
	if got.Cmp(big.NewInt(int64(want))) != 0 {
		return fmt.Errorf("%w: warehouse serves chain id %s, indexer is configured for %s", ErrLogWarehouseChainMismatch, got, chain)
	}
	return nil
}

// LogWarehouseHead returns the warehouse head and true when a log warehouse is
// configured and answers; (0, false) when none is configured or it is
// unreachable. Callers plan work around the head (the owner scan sizes its
// windows by it) but never depend on it: every log fetch re-checks the head
// and falls through to the vendor on its own.
func (f *ethereumClient) LogWarehouseHead(ctx context.Context) (uint64, bool) {
	if f.guards.LogWarehouse == nil {
		return 0, false
	}
	head, err := f.guards.LogWarehouse.Head(ctx)
	if err != nil {
		logger.WarnCtx(ctx, "Log warehouse head unavailable, planning without it", zap.Error(err))
		return 0, false
	}
	return head, true
}

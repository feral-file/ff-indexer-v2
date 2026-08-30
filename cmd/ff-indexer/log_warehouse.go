package main

import (
	"context"
	"errors"
	"fmt"

	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/config"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum"
)

// dialLogWarehouse connects to the Ethereum log warehouse when
// ethereum.log_warehouse_url is set and returns nil when it is not (every
// eth_getLogs stays on the vendor). It verifies the warehouse's chain once:
// a mismatch is a startup error, an unreachable warehouse is a warning — the
// routing client falls through to the vendor until it answers, so a warehouse
// restart must never keep the indexer from starting.
//
// Both the ingestion process and the worker core dial their own client, like
// they do for the vendor; the warehouse connection is a stateless HTTP client,
// so sharing one would buy nothing.
func dialLogWarehouse(ctx context.Context, cfg config.EthereumConfig) (adapter.LogWarehouse, error) {
	if cfg.LogWarehouseURL == "" {
		return nil, nil
	}
	warehouse, err := adapter.NewLogWarehouseDialer().Dial(ctx, cfg.LogWarehouseURL, cfg.LogWarehouseTimeout)
	if err != nil {
		return nil, fmt.Errorf("dial log warehouse: %w", err)
	}
	endpoint := adapter.EndpointForLogs(cfg.LogWarehouseURL)
	err = ethereum.CheckLogWarehouseChain(ctx, warehouse, cfg.ChainID)
	switch {
	case errors.Is(err, ethereum.ErrLogWarehouseUnreachable):
		logger.WarnCtx(ctx, "Log warehouse unreachable at startup; eth_getLogs falls through to the vendor until it answers",
			zap.String("endpoint", endpoint), zap.Error(err))
	case err != nil:
		warehouse.Close()
		return nil, err
	default:
		logger.InfoCtx(ctx, "Ethereum log warehouse enabled for historical eth_getLogs",
			zap.String("endpoint", endpoint), zap.Duration("timeout", cfg.LogWarehouseTimeout))
	}
	return warehouse, nil
}

// closeLogWarehouse closes an optional warehouse client.
func closeLogWarehouse(warehouse adapter.LogWarehouse) {
	if warehouse != nil {
		warehouse.Close()
	}
}

// warehouseScanWindowBlocks returns the owner-scan window size over the
// warehouse-covered range, or 0 (no split) when no warehouse is configured.
func warehouseScanWindowBlocks(cfg config.EthereumConfig, warehouse adapter.LogWarehouse) uint64 {
	if warehouse == nil {
		return 0
	}
	return cfg.LogWarehouseScanWindowBlocks
}

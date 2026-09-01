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
// eth_getLogs stays on the vendor). The returned client is gated: nothing is
// routed through it until it has proven its chain id and capabilities
// (adapter.VerifiedLogWarehouse). That verification is attempted here so a
// permanent problem — wrong chain, a build missing a required log shape — is
// a startup error; a warehouse that merely does not answer yet is a warning,
// and the gate re-verifies on the first request it does answer.
//
// Both the ingestion process and the worker core dial their own client, like
// they do for the vendor; the warehouse connection is a stateless HTTP client,
// so sharing one would buy nothing.
func dialLogWarehouse(ctx context.Context, cfg config.EthereumConfig) (adapter.LogWarehouse, error) {
	if cfg.LogWarehouseURL == "" {
		return nil, nil
	}
	reqs, err := ethereum.LogWarehouseRequirements(cfg.ChainID)
	if err != nil {
		return nil, err
	}
	raw, err := adapter.NewLogWarehouseDialer().Dial(ctx, cfg.LogWarehouseURL, cfg.LogWarehouseTimeout)
	if err != nil {
		return nil, fmt.Errorf("dial log warehouse: %w", err)
	}
	warehouse := adapter.NewVerifiedLogWarehouse(raw, reqs, adapter.NewClock(), adapter.DefaultVerifyRetryInterval)
	endpoint := adapter.EndpointForLogs(cfg.LogWarehouseURL)
	err = warehouse.Verify(ctx)
	switch {
	case errors.Is(err, adapter.ErrLogWarehouseUnverified):
		logger.WarnCtx(ctx, "Log warehouse not answering at startup; it verifies on the first request that reaches it (until then eth_getLogs fails or falls through to the vendor per ethereum.log_warehouse_vendor_fallthrough)",
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

package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/config"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum"
)

// chainIDServer answers eth_chainId with the given hex quantity.
func chainIDServer(t *testing.T, chainID string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID json.RawMessage `json:"id"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": chainID}))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestDialLogWarehouse pins the startup glue: no URL means no warehouse (and
// no window split), a warehouse on the wrong chain is a startup error, and an
// unreachable one is tolerated so a warehouse restart never blocks the indexer.
func TestDialLogWarehouse(t *testing.T) {
	t.Parallel()
	base := config.EthereumConfig{ChainID: domain.ChainEthereumMainnet, LogWarehouseTimeout: time.Second, LogWarehouseScanWindowBlocks: 1_000_000}

	t.Run("no url is vendor-only", func(t *testing.T) {
		t.Parallel()
		wh, err := dialLogWarehouse(context.Background(), base)
		require.NoError(t, err)
		require.Nil(t, wh)
		require.Zero(t, warehouseScanWindowBlocks(base, wh), "no warehouse, no window split")
		closeLogWarehouse(wh) // must tolerate nil
	})
	t.Run("matching chain enables routing", func(t *testing.T) {
		t.Parallel()
		cfg := base
		cfg.LogWarehouseURL = chainIDServer(t, "0x1").URL
		wh, err := dialLogWarehouse(context.Background(), cfg)
		require.NoError(t, err)
		require.NotNil(t, wh)
		defer closeLogWarehouse(wh)
		require.Equal(t, uint64(1_000_000), warehouseScanWindowBlocks(cfg, wh))
	})
	t.Run("chain mismatch is fatal", func(t *testing.T) {
		t.Parallel()
		cfg := base
		cfg.LogWarehouseURL = chainIDServer(t, "0xaa36a7").URL // Sepolia
		_, err := dialLogWarehouse(context.Background(), cfg)
		require.ErrorIs(t, err, ethereum.ErrLogWarehouseChainMismatch)
	})
	t.Run("unreachable warehouse is tolerated", func(t *testing.T) {
		t.Parallel()
		srv := httptest.NewServer(http.NotFoundHandler())
		url := srv.URL
		srv.Close()
		cfg := base
		cfg.LogWarehouseURL = url
		wh, err := dialLogWarehouse(context.Background(), cfg)
		require.NoError(t, err)
		require.NotNil(t, wh, "routing stays configured; every query falls through until the warehouse answers")
		closeLogWarehouse(wh)
	})
}

package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/config"
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/providers/ethereum/helpers"
)

// warehouseServer answers eth_chainId with chainID and eth_getLogs with
// probeLogs, so the startup verification (chain id + CryptoPunks probe) can be
// driven through the real client.
func warehouseServer(t *testing.T, chainID string, probeLogs []map[string]any) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		var result any
		switch req.Method {
		case "eth_chainId":
			result = chainID
		case "eth_getLogs":
			result = probeLogs
		default:
			t.Errorf("unexpected method %s", req.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": result}))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// punksInternalTransfer is a 3-topic Transfer from the CryptoPunks contract,
// the shape the mainnet probe requires.
func punksInternalTransfer() []map[string]any {
	zero := "0x" + common.Bytes2Hex(make([]byte, 32))
	return []map[string]any{{
		"address":          "0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb",
		"topics":           []string{helpers.TransferEventSignature.Hex(), zero, zero},
		"data":             "0x",
		"blockNumber":      "0x3bcf5a",
		"transactionHash":  zero,
		"transactionIndex": "0x0",
		"blockHash":        zero,
		"logIndex":         "0x0",
		"removed":          false,
	}}
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
	t.Run("matching chain and probe enable routing", func(t *testing.T) {
		t.Parallel()
		cfg := base
		cfg.LogWarehouseURL = warehouseServer(t, "0x1", punksInternalTransfer()).URL
		wh, err := dialLogWarehouse(context.Background(), cfg)
		require.NoError(t, err)
		require.NotNil(t, wh)
		defer closeLogWarehouse(wh)
		require.Equal(t, uint64(1_000_000), warehouseScanWindowBlocks(cfg, wh))
	})
	t.Run("chain mismatch is fatal", func(t *testing.T) {
		t.Parallel()
		cfg := base
		cfg.LogWarehouseURL = warehouseServer(t, "0xaa36a7", punksInternalTransfer()).URL // Sepolia
		_, err := dialLogWarehouse(context.Background(), cfg)
		require.ErrorIs(t, err, adapter.ErrLogWarehouseChainMismatch)
	})
	t.Run("warehouse without the CryptoPunks internal Transfer is fatal", func(t *testing.T) {
		t.Parallel()
		cfg := base
		cfg.LogWarehouseURL = warehouseServer(t, "0x1", []map[string]any{}).URL
		_, err := dialLogWarehouse(context.Background(), cfg)
		require.ErrorIs(t, err, adapter.ErrLogWarehouseProbeFailed)
	})
	t.Run("non-eip155 chain is a config error", func(t *testing.T) {
		t.Parallel()
		cfg := base
		cfg.ChainID = domain.ChainTezosMainnet
		cfg.LogWarehouseURL = "http://127.0.0.1:1"
		_, err := dialLogWarehouse(context.Background(), cfg)
		require.Error(t, err)
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

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
			Params []struct {
				ERC1155ID string     `json:"erc1155Id"`
				Topics    [][]string `json:"topics"`
			} `json:"params"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		var result any
		switch req.Method {
		case "eth_chainId":
			result = chainID
		case "eth_getLogs":
			// The erc1155Id capability probes send the field; answer each with a
			// correctly filtered log for its signature (TransferSingle by data
			// word 0, URI by topic1) so a compliant warehouse is simulated.
			// Every other eth_getLogs (the CryptoPunks probe) gets probeLogs.
			switch {
			case len(req.Params) == 1 && req.Params[0].ERC1155ID != "" && probeTopic0(req.Params[0].Topics) == helpers.ERC1155URIEventSignature.Hex():
				result = erc1155URI(req.Params[0].ERC1155ID)
			case len(req.Params) == 1 && req.Params[0].ERC1155ID != "":
				result = erc1155TransferSingle(req.Params[0].ERC1155ID)
			default:
				result = probeLogs
			}
		default:
			t.Errorf("unexpected method %s", req.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": result}))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// erc1155TransferSingle is a TransferSingle log whose data word 0 is idHex (a
// 32-byte hex token id), i.e. the shape the erc1155Id capability probe accepts.
func erc1155TransferSingle(idHex string) []map[string]any {
	zero := "0x" + common.Bytes2Hex(make([]byte, 32))
	id := common.HexToHash(idHex)
	data := "0x" + common.Bytes2Hex(append(append([]byte{}, id.Bytes()...), make([]byte, 32)...)) // id || value
	return []map[string]any{{
		"address":          "0x495f947276749ce646f68ac8c248420045cb7b5e",
		"topics":           []string{helpers.ERC1155TransferSingleEventSignature.Hex(), zero, zero, zero},
		"data":             data,
		"blockNumber":      "0xd65e29",
		"transactionHash":  zero,
		"transactionIndex": "0x0",
		"blockHash":        zero,
		"logIndex":         "0x0",
		"removed":          false,
	}}
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

// probeTopic0 returns the first topic0 hex of a decoded topics filter, or "".
func probeTopic0(topics [][]string) string {
	if len(topics) > 0 && len(topics[0]) > 0 {
		return topics[0][0]
	}
	return ""
}

// erc1155URI is a URI log whose indexed topic1 is idHex, i.e. the shape the
// URI capability probe accepts.
func erc1155URI(idHex string) []map[string]any {
	zero := "0x" + common.Bytes2Hex(make([]byte, 32))
	id := common.HexToHash(idHex)
	return []map[string]any{{
		"address":          "0xd0e4847359ae76c2786d242e5f45c4f6f1abd752",
		"topics":           []string{helpers.ERC1155URIEventSignature.Hex(), id.Hex()},
		"data":             zero,
		"blockNumber":      "0x69e0c9",
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

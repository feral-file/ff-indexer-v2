package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// setWarehouseTestEnv provides the minimum required config through the
// environment so LoadAppConfig("", "") reaches validation.
func setWarehouseTestEnv(t *testing.T) {
	t.Helper()
	t.Setenv("FF_INDEXER_DATABASE_HOST", "localhost")
	t.Setenv("FF_INDEXER_DATABASE_DBNAME", "ff")
	t.Setenv("FF_INDEXER_JOBS_TOKEN_QUEUE", "token_index")
	t.Setenv("FF_INDEXER_ETHEREUM_RPC_URL", "http://rpc.invalid")
	t.Setenv("FF_INDEXER_ETHEREUM_WEBSOCKET_URL", "ws://rpc.invalid")
	t.Setenv("FF_INDEXER_TEZOS_API_URL", "http://tzkt.invalid")
	t.Setenv("FF_INDEXER_TEZOS_WEBSOCKET_URL", "ws://tzkt.invalid")
}

// TestLoadAppConfig_LogWarehouseDefaultsOff pins that a deployment without a
// warehouse URL is unchanged: routing is off and the defaults do not trip
// validation (which only engages once a URL is set).
func TestLoadAppConfig_LogWarehouseDefaultsOff(t *testing.T) {
	setWarehouseTestEnv(t)
	cfg, err := LoadAppConfig("", "")
	require.NoError(t, err)
	require.Empty(t, cfg.Ethereum.LogWarehouseURL)
	require.Equal(t, 120*time.Second, cfg.Ethereum.LogWarehouseTimeout)
	require.Equal(t, uint64(1_000_000), cfg.Ethereum.LogWarehouseScanWindowBlocks)
}

// TestLoadAppConfig_LogWarehouseEnvVarsReachConfig pins that every warehouse
// knob loads from environment variables alone — bindAllEnvVars is an explicit
// allowlist, so an unbound key would silently keep the warehouse off in a
// deployment that believes it enabled it.
func TestLoadAppConfig_LogWarehouseEnvVarsReachConfig(t *testing.T) {
	setWarehouseTestEnv(t)
	t.Setenv("FF_INDEXER_ETHEREUM_LOG_WAREHOUSE_URL", "http://10.124.0.4:8545")
	t.Setenv("FF_INDEXER_ETHEREUM_LOG_WAREHOUSE_TIMEOUT", "45s")
	t.Setenv("FF_INDEXER_ETHEREUM_LOG_WAREHOUSE_SCAN_WINDOW_BLOCKS", "250000")

	cfg, err := LoadAppConfig("", "")
	require.NoError(t, err)
	require.Equal(t, "http://10.124.0.4:8545", cfg.Ethereum.LogWarehouseURL)
	require.Equal(t, 45*time.Second, cfg.Ethereum.LogWarehouseTimeout)
	require.Equal(t, uint64(250_000), cfg.Ethereum.LogWarehouseScanWindowBlocks)
}

// TestLoadAppConfig_LogWarehouseValidation pins the three startup rejections
// once a URL is set: a non-http(s) URL, a non-positive timeout, and a zero
// scan window.
func TestLoadAppConfig_LogWarehouseValidation(t *testing.T) {
	cases := []struct {
		name    string
		env     map[string]string
		wantErr string
	}{
		{"websocket url rejected", map[string]string{
			"FF_INDEXER_ETHEREUM_LOG_WAREHOUSE_URL": "ws://10.124.0.4:8545",
		}, "log_warehouse_url must be an http(s) URL"},
		{"hostless url rejected", map[string]string{
			"FF_INDEXER_ETHEREUM_LOG_WAREHOUSE_URL": "8545",
		}, "log_warehouse_url must be an http(s) URL"},
		{"zero timeout rejected", map[string]string{
			"FF_INDEXER_ETHEREUM_LOG_WAREHOUSE_URL":     "http://10.124.0.4:8545",
			"FF_INDEXER_ETHEREUM_LOG_WAREHOUSE_TIMEOUT": "0s",
		}, "log_warehouse_timeout must be > 0"},
		{"zero scan window rejected", map[string]string{
			"FF_INDEXER_ETHEREUM_LOG_WAREHOUSE_URL":                "http://10.124.0.4:8545",
			"FF_INDEXER_ETHEREUM_LOG_WAREHOUSE_SCAN_WINDOW_BLOCKS": "0",
		}, "log_warehouse_scan_window_blocks must be > 0"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setWarehouseTestEnv(t)
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			_, err := LoadAppConfig("", "")
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

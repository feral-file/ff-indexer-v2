package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/joho/godotenv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLoadAppConfig_ShippedIPFSGatewayPool checks the effective runtime pool,
// because the shipped environment file overrides both YAML and Viper defaults.
// Copy only that tracked file so developer overrides cannot influence the test,
// and restore every environment variable that loadEnv's Overload can mutate.
func TestLoadAppConfig_ShippedIPFSGatewayPool(t *testing.T) {
	baseEnv, err := os.ReadFile(filepath.Join("..", "..", "config", ".env"))
	require.NoError(t, err)
	values, err := godotenv.Unmarshal(string(baseEnv))
	require.NoError(t, err)
	for key := range values {
		t.Setenv(key, "")
	}

	envDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(envDir, ".env"), baseEnv, 0600))
	configPath := filepath.Join(envDir, "config.yaml")
	// Supply non-secret RPC placeholders and a distinct YAML pool: the shipped
	// environment must supply the intended pool even when YAML disagrees.
	yaml := `
ethereum:
  rpc_url: https://rpc.example.com
  websocket_url: wss://ws.example.com
uri:
  ipfs_gateways:
    - https://yaml-only-gateway.example
`
	require.NoError(t, os.WriteFile(configPath, []byte(yaml), 0600))

	cfg, err := LoadAppConfig(configPath, envDir)
	require.NoError(t, err)
	expected := []string{"https://ipfs.feralfile.com", "https://ipfs.filebase.io"}
	assert.Equal(t, expected, cfg.URI.IPFSGateways)
	assert.Equal(t, expected, cfg.MediaHealthSweeper.EffectiveURI(cfg.URI).IPFSGateways)
}

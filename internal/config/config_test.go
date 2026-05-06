package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDatabaseConfig_DSN(t *testing.T) {
	tests := []struct {
		name     string
		config   DatabaseConfig
		expected string
	}{
		{
			name: "complete config",
			config: DatabaseConfig{
				Host:     "localhost",
				Port:     5432,
				User:     "testuser",
				Password: "testpass",
				DBName:   "testdb",
				SSLMode:  "require",
			},
			expected: "host=localhost port=5432 user=testuser password=testpass dbname=testdb sslmode=require",
		},
		{
			name: "with special characters in password",
			config: DatabaseConfig{
				Host:     "localhost",
				Port:     5432,
				User:     "testuser",
				Password: "p@ssw0rd!",
				DBName:   "testdb",
				SSLMode:  "disable",
			},
			expected: "host=localhost port=5432 user=testuser password=p@ssw0rd! dbname=testdb sslmode=disable",
		},
		{
			name: "minimal config",
			config: DatabaseConfig{
				Host:     "localhost",
				Port:     5432,
				User:     "user",
				Password: "pass",
				DBName:   "db",
				SSLMode:  "disable",
			},
			expected: "host=localhost port=5432 user=user password=pass dbname=db sslmode=disable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsn := tt.config.DSN()
			assert.Equal(t, tt.expected, dsn)
		})
	}
}

func TestDatabaseConfig_ReadDSN(t *testing.T) {
	c := DatabaseConfig{
		Host:     "primary",
		Port:     5432,
		ReadHost: "replica",
		ReadPort: 5433,
		User:     "u",
		Password: "p",
		DBName:   "db",
		SSLMode:  "disable",
	}
	assert.Equal(t, "host=replica port=5433 user=u password=p dbname=db sslmode=disable", c.ReadDSN())
	c.ReadPort = 0
	assert.Equal(t, "host=replica port=5432 user=u password=p dbname=db sslmode=disable", c.ReadDSN())
}

func TestLoadAppConfig(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	yaml := `
database:
  host: localhost
  user: u
  password: p
  dbname: db
jobs:
  token_queue: token_index
  media_queue: media_index
ethereum:
  rpc_url: https://rpc.example.com
  websocket_url: wss://ws.example.com
tezos:
  api_url: https://api.tzkt.io
  websocket_url: wss://ws.tzkt.io
`
	require.NoError(t, os.WriteFile(configPath, []byte(yaml), 0600))

	cfg, err := LoadAppConfig(configPath, "")
	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Equal(t, "localhost", cfg.Database.Host)
	assert.Equal(t, "db", cfg.Database.DBName)
}

func TestLoadAppConfig_requiresDatabase(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	require.NoError(t, os.WriteFile(configPath, []byte(`database: {}`), 0600))

	_, err := LoadAppConfig(configPath, "")
	require.Error(t, err)
}

func TestValidateRequiredConfigValues(t *testing.T) {
	cfg := &AppConfig{
		Database: DatabaseConfig{
			Host:   "localhost",
			DBName: "ff_indexer",
		},
		Jobs: JobsConfig{
			TokenQueue: "token_index",
			MediaQueue: "media_index",
		},
		Ethereum: EthereumConfig{
			RPCURL:       "https://rpc.example.com",
			WebSocketURL: "wss://ws.example.com",
		},
		Tezos: TezosConfig{
			APIURL:       "https://api.tzkt.io",
			WebSocketURL: "wss://ws.tzkt.io",
		},
	}

	require.NoError(t, ValidateRequiredConfigValues(cfg))
}

func TestValidateRequiredConfigValues_MediaDisabled_EmptyMediaQueue(t *testing.T) {
	cfg := &AppConfig{
		MediaEnabled: false,
		Database: DatabaseConfig{
			Host:   "localhost",
			DBName: "ff_indexer",
		},
		Jobs: JobsConfig{
			TokenQueue: "token_index",
			MediaQueue: "",
		},
		Ethereum: EthereumConfig{
			RPCURL:       "https://rpc.example.com",
			WebSocketURL: "wss://ws.example.com",
		},
		Tezos: TezosConfig{
			APIURL:       "https://api.tzkt.io",
			WebSocketURL: "wss://ws.tzkt.io",
		},
	}

	require.NoError(t, ValidateRequiredConfigValues(cfg))
}

func TestValidateRequiredConfigValues_MediaEnabled_MissingMediaQueue(t *testing.T) {
	cfg := &AppConfig{
		MediaEnabled: true,
		Database: DatabaseConfig{
			Host:   "localhost",
			DBName: "ff_indexer",
		},
		Jobs: JobsConfig{
			TokenQueue: "token_index",
			MediaQueue: "",
		},
		Ethereum: EthereumConfig{
			RPCURL:       "https://rpc.example.com",
			WebSocketURL: "wss://ws.example.com",
		},
		Tezos: TezosConfig{
			APIURL:       "https://api.tzkt.io",
			WebSocketURL: "wss://ws.tzkt.io",
		},
	}

	err := ValidateRequiredConfigValues(cfg)
	require.Error(t, err)
	assert.EqualError(t, err, "missing required config values: jobs.media_queue")
}

func TestValidateRequiredConfigValues_MissingFields(t *testing.T) {
	cfg := &AppConfig{
		Database: DatabaseConfig{
			Host: "localhost",
		},
		Jobs: JobsConfig{
			TokenQueue: "token_index",
			MediaQueue: "media_index",
		},
		Ethereum: EthereumConfig{
			RPCURL: "https://rpc.example.com",
		},
		Tezos: TezosConfig{
			APIURL: "https://api.tzkt.io",
		},
	}

	err := ValidateRequiredConfigValues(cfg)
	require.Error(t, err)
	assert.EqualError(t, err, "missing required config values: database.dbname, ethereum.websocket_url, tezos.websocket_url")
}

func TestConfigWithEnvironmentVariables(t *testing.T) {
	tmpDir := t.TempDir()

	envDir := filepath.Join(tmpDir, "env")
	require.NoError(t, os.MkdirAll(envDir, 0750))

	envFile := filepath.Join(envDir, ".env")
	envContent := `FF_INDEXER_DEBUG=true
FF_INDEXER_DATABASE_HOST=env-host
FF_INDEXER_DATABASE_PORT=3306
FF_INDEXER_DATABASE_USER=env-user
FF_INDEXER_DATABASE_PASSWORD=env-pass
FF_INDEXER_DATABASE_DBNAME=env-db
FF_INDEXER_DATABASE_SSLMODE=require
FF_INDEXER_ETHEREUM_RPC_URL=https://rpc.example.com
FF_INDEXER_ETHEREUM_WEBSOCKET_URL=wss://ws.example.com
FF_INDEXER_TEZOS_API_URL=https://api.tzkt.io
FF_INDEXER_TEZOS_WEBSOCKET_URL=wss://ws.tzkt.io
`
	require.NoError(t, os.WriteFile(envFile, []byte(envContent), 0600))

	configPath := filepath.Join(tmpDir, "config.yaml")
	configFile := `
debug: false
database:
  host: file-host
  port: 5432
  user: file-user
  password: file-pass
  dbname: file-db
  sslmode: disable
`
	require.NoError(t, os.WriteFile(configPath, []byte(configFile), 0600))

	cfg, err := LoadAppConfig(configPath, envDir)
	require.NoError(t, err)
	require.NotNil(t, cfg)

	assert.True(t, cfg.Debug)
	assert.Equal(t, "env-host", cfg.Database.Host)
	assert.Equal(t, 3306, cfg.Database.Port)
	assert.Equal(t, "env-user", cfg.Database.User)
	assert.Equal(t, "env-pass", cfg.Database.Password)
	assert.Equal(t, "env-db", cfg.Database.DBName)
	assert.Equal(t, "require", cfg.Database.SSLMode)
	assert.Equal(t, "https://api.tzkt.io", cfg.Tezos.APIURL)
	assert.Equal(t, "wss://ws.tzkt.io", cfg.Tezos.WebSocketURL)
}

func TestLoadAppConfig_MediaEnabledFromEnv(t *testing.T) {
	tmpDir := t.TempDir()

	envDir := filepath.Join(tmpDir, "env")
	require.NoError(t, os.MkdirAll(envDir, 0750))

	envContent := `FF_INDEXER_MEDIA_ENABLED=false
FF_INDEXER_DATABASE_HOST=env-host
FF_INDEXER_DATABASE_USER=env-user
FF_INDEXER_DATABASE_PASSWORD=env-pass
FF_INDEXER_DATABASE_DBNAME=env-db
FF_INDEXER_ETHEREUM_RPC_URL=https://rpc.example.com
FF_INDEXER_ETHEREUM_WEBSOCKET_URL=wss://ws.example.com
FF_INDEXER_TEZOS_API_URL=https://api.tzkt.io
FF_INDEXER_TEZOS_WEBSOCKET_URL=wss://ws.tzkt.io
`
	require.NoError(t, os.WriteFile(filepath.Join(envDir, ".env"), []byte(envContent), 0600))

	cfg, err := LoadAppConfig("", envDir)
	require.NoError(t, err)
	require.NotNil(t, cfg)

	assert.False(t, cfg.MediaEnabled)
	assert.False(t, cfg.ToWorkerMediaConfig().MediaEnabled)
}

func TestValidateSecurityConfig_allowlistDomainTooBroad(t *testing.T) {
	cfg := &AppConfig{}
	cfg.Security.SSRFProtection.Allowlist.Domains = []string{"com"}
	require.Error(t, validateSecurityConfig(cfg))
}

func TestValidateSecurityConfig_negativeMaxRedirects(t *testing.T) {
	cfg := &AppConfig{}
	cfg.Security.SSRFProtection.MaxRedirects = -1
	require.Error(t, validateSecurityConfig(cfg))
}

func TestSSRFValidatorFromProtection_invalidAllowlistIP(t *testing.T) {
	cfg := &AppConfig{}
	cfg.Security.SSRFProtection.Enabled = true
	cfg.Security.SSRFProtection.Allowlist.IPs = []string{"not-an-ip"}
	_, err := SSRFValidatorFromProtection(cfg.Security.SSRFProtection)
	require.Error(t, err)
}

func TestToWorkerCoreConfig_includesSecurityForSSRF(t *testing.T) {
	cfg := &AppConfig{}
	cfg.Security.SSRFProtection.Enabled = true
	cfg.Security.SSRFProtection.MaxRedirects = 7
	cfg.Security.SSRFProtection.BlockMulticast = true
	cfg.Security.SSRFProtection.Allowlist.Domains = []string{"cdn.example.com"}

	w := cfg.ToWorkerCoreConfig()
	require.Equal(t, cfg.Security.SSRFProtection.Enabled, w.Security.SSRFProtection.Enabled)
	require.Equal(t, 7, w.Security.SSRFProtection.MaxRedirects)
	require.True(t, w.Security.SSRFProtection.BlockMulticast)
	require.Equal(t, []string{"cdn.example.com"}, w.Security.SSRFProtection.Allowlist.Domains)

	v, err := SSRFValidatorFromProtection(w.Security.SSRFProtection)
	require.NoError(t, err)
	require.NotNil(t, v)
}

package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadEthereumEmitterConfig(t *testing.T) {
	tests := []struct {
		name        string
		configFile  string
		expectError bool
		validate    func(*testing.T, *EthereumEmitterConfig)
	}{
		{
			name: "valid config file",
			configFile: `
debug: true
sentry_dsn: "https://sentry.example.com"
worker:
  pool_size: 10
  queue_size: 500
database:
  host: localhost
  port: 5432
  user: testuser
  password: testpass
  dbname: testdb
  sslmode: require
nats:
  url: "nats://localhost:4222"
  stream_name: "TEST_STREAM"
  consumer_name: "test-consumer"
  max_reconnects: 5
  reconnect_wait: "5s"
  connection_name: "test-connection"
ethereum:
  websocket_url: "ws://localhost:8545"
  rpc_url: "http://localhost:8545"
  chain_id: "eip155:1"
  start_block: 1000
`,
			expectError: false,
			validate: func(t *testing.T, cfg *EthereumEmitterConfig) {
				assert.True(t, cfg.Debug)
				assert.Equal(t, "https://sentry.example.com", cfg.SentryDSN)
				assert.Equal(t, 10, cfg.Worker.WorkerPoolSize)
				assert.Equal(t, 500, cfg.Worker.WorkerQueueSize)
				assert.Equal(t, "localhost", cfg.Database.Host)
				assert.Equal(t, 5432, cfg.Database.Port)
				assert.Equal(t, "testuser", cfg.Database.User)
				assert.Equal(t, "testpass", cfg.Database.Password)
				assert.Equal(t, "testdb", cfg.Database.DBName)
				assert.Equal(t, "require", cfg.Database.SSLMode)
				assert.Equal(t, "nats://localhost:4222", cfg.NATS.URL)
				assert.Equal(t, "TEST_STREAM", cfg.NATS.StreamName)
				assert.Equal(t, "ws://localhost:8545", cfg.Ethereum.WebSocketURL)
				assert.Equal(t, "eip155:1", string(cfg.Ethereum.ChainID))
				assert.Equal(t, uint64(1000), cfg.Ethereum.StartBlock)
			},
		},
		{
			name: "config with defaults",
			configFile: `
database:
  host: localhost
  user: testuser
  password: testpass
  dbname: testdb
nats:
  url: "nats://localhost:4222"
ethereum:
  websocket_url: "ws://localhost:8545"
  rpc_url: "http://localhost:8545"
  chain_id: "eip155:1"
`,
			expectError: false,
			validate: func(t *testing.T, cfg *EthereumEmitterConfig) {
				// Check defaults
				assert.Equal(t, 5432, cfg.Database.Port)
				assert.Equal(t, "disable", cfg.Database.SSLMode)
				assert.Equal(t, 10, cfg.NATS.MaxReconnects)
				assert.Equal(t, "2s", cfg.NATS.ReconnectWait.String())
				assert.Equal(t, "BLOCKCHAIN_EVENTS", cfg.NATS.StreamName)
				assert.Equal(t, "eip155:1", string(cfg.Ethereum.ChainID))
			},
		},
		{
			name:        "missing config file",
			configFile:  "",
			expectError: false,
			validate:    nil,
		},
		{
			name: "invalid yaml",
			configFile: `
				database:
				  host: localhost
				  port: invalid
			`,
			expectError: true, // Invalid port should cause unmarshal error
			validate:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			var configFile string

			if tt.configFile != "" {
				configFile = filepath.Join(tmpDir, "config.yaml")
				err := os.WriteFile(configFile, []byte(tt.configFile), 0600)
				require.NoError(t, err)
			} else {
				configFile = filepath.Join(tmpDir, "nonexistent.yaml")
			}

			cfg, err := LoadEthereumEmitterConfig(configFile, "")

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, cfg)
			} else {
				if tt.validate != nil {
					require.NoError(t, err)
					require.NotNil(t, cfg)
					tt.validate(t, cfg)
				}
			}
		})
	}
}

func TestLoadTezosEmitterConfig(t *testing.T) {
	tests := []struct {
		name        string
		configFile  string
		expectError bool
		validate    func(*testing.T, *TezosEmitterConfig)
	}{
		{
			name: "valid config file",
			configFile: `
debug: true
sentry_dsn: "https://sentry.example.com"
worker:
  pool_size: 15
  queue_size: 1000
database:
  host: localhost
  port: 5432
  user: testuser
  password: testpass
  dbname: testdb
  sslmode: require
nats:
  url: "nats://localhost:4222"
  stream_name: "TEZOS_STREAM"
  consumer_name: "test-consumer"
  max_reconnects: 5
  reconnect_wait: "5s"
  connection_name: "test-connection"
tezos:
  api_url: "https://api.tzkt.io"
  websocket_url: "ws://localhost:8080"
  chain_id: "tezos:mainnet"
  start_level: 5000
`,
			expectError: false,
			validate: func(t *testing.T, cfg *TezosEmitterConfig) {
				assert.True(t, cfg.Debug)
				assert.Equal(t, "https://sentry.example.com", cfg.SentryDSN)
				assert.Equal(t, 15, cfg.Worker.WorkerPoolSize)
				assert.Equal(t, 1000, cfg.Worker.WorkerQueueSize)
				assert.Equal(t, "localhost", cfg.Database.Host)
				assert.Equal(t, 5432, cfg.Database.Port)
				assert.Equal(t, "testuser", cfg.Database.User)
				assert.Equal(t, "testpass", cfg.Database.Password)
				assert.Equal(t, "testdb", cfg.Database.DBName)
				assert.Equal(t, "require", cfg.Database.SSLMode)
				assert.Equal(t, "nats://localhost:4222", cfg.NATS.URL)
				assert.Equal(t, "TEZOS_STREAM", cfg.NATS.StreamName)
				assert.Equal(t, "https://api.tzkt.io", cfg.Tezos.APIURL)
				assert.Equal(t, "ws://localhost:8080", cfg.Tezos.WebSocketURL)
				assert.Equal(t, "tezos:mainnet", string(cfg.Tezos.ChainID))
				assert.Equal(t, uint64(5000), cfg.Tezos.StartLevel)
			},
		},
		{
			name: "config with defaults",
			configFile: `
database:
  host: localhost
  user: testuser
  password: testpass
  dbname: testdb
nats:
  url: "nats://localhost:4222"
tezos:
  api_url: "https://api.tzkt.io"
  websocket_url: "ws://localhost:8080"
  chain_id: "tezos:mainnet"
`,
			expectError: false,
			validate: func(t *testing.T, cfg *TezosEmitterConfig) {
				// Check defaults
				assert.Equal(t, 5432, cfg.Database.Port)
				assert.Equal(t, "disable", cfg.Database.SSLMode)
				assert.Equal(t, 10, cfg.NATS.MaxReconnects)
				assert.Equal(t, "2s", cfg.NATS.ReconnectWait.String())
				assert.Equal(t, "BLOCKCHAIN_EVENTS", cfg.NATS.StreamName)
				assert.Equal(t, "tezos:mainnet", string(cfg.Tezos.ChainID))
			},
		},
		{
			name:        "missing config file",
			configFile:  "",
			expectError: false,
			validate:    nil,
		},
		{
			name: "invalid yaml",
			configFile: `
				database:
				  host: localhost
				  port: invalid
			`,
			expectError: true, // Invalid port should cause unmarshal error
			validate:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			var configFile string

			if tt.configFile != "" {
				configFile = filepath.Join(tmpDir, "config.yaml")
				err := os.WriteFile(configFile, []byte(tt.configFile), 0600)
				require.NoError(t, err)
			} else {
				configFile = filepath.Join(tmpDir, "nonexistent.yaml")
			}

			cfg, err := LoadTezosEmitterConfig(configFile, "")

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, cfg)
			} else {
				if tt.validate != nil {
					require.NoError(t, err)
					require.NotNil(t, cfg)
					tt.validate(t, cfg)
				}
			}
		})
	}
}

func TestLoadEventBridgeConfig(t *testing.T) {
	tests := []struct {
		name        string
		configFile  string
		expectError bool
		validate    func(*testing.T, *EventBridgeConfig)
	}{
		{
			name: "valid config file",
			configFile: `
debug: false
sentry_dsn: "https://sentry.example.com"
database:
  host: localhost
  port: 5432
  user: testuser
  password: testpass
  dbname: testdb
  sslmode: require
nats:
  url: "nats://localhost:4222"
  stream_name: "CUSTOM_STREAM"
  consumer_name: "custom-consumer"
  max_reconnects: 5
  reconnect_wait: "5s"
  connection_name: "test-connection"
  ack_wait: "60s"
  max_deliver: 5
temporal:
  host_port: "temporal.example.com:7233"
  namespace: "production"
  token_task_queue: "custom-queue"
  max_concurrent_activity_execution_size: 100
  worker_activities_per_second: 100
blacklist_path: "/path/to/blacklist.json"
`,
			expectError: false,
			validate: func(t *testing.T, cfg *EventBridgeConfig) {
				assert.False(t, cfg.Debug)
				assert.Equal(t, "https://sentry.example.com", cfg.SentryDSN)
				assert.Equal(t, "localhost", cfg.Database.Host)
				assert.Equal(t, 5432, cfg.Database.Port)
				assert.Equal(t, "testuser", cfg.Database.User)
				assert.Equal(t, "testpass", cfg.Database.Password)
				assert.Equal(t, "testdb", cfg.Database.DBName)
				assert.Equal(t, "require", cfg.Database.SSLMode)
				assert.Equal(t, "nats://localhost:4222", cfg.NATS.URL)
				assert.Equal(t, "CUSTOM_STREAM", cfg.NATS.StreamName)
				assert.Equal(t, "custom-consumer", cfg.NATS.ConsumerName)
				assert.Equal(t, 60*time.Second, cfg.NATS.AckWait)
				assert.Equal(t, 5, cfg.NATS.MaxDeliver)
				assert.Equal(t, "temporal.example.com:7233", cfg.Temporal.HostPort)
				assert.Equal(t, "production", cfg.Temporal.Namespace)
				assert.Equal(t, "custom-queue", cfg.Temporal.TokenTaskQueue)
				assert.Equal(t, 100, cfg.Temporal.MaxConcurrentActivityExecutionSize)
				assert.Equal(t, 100.0, cfg.Temporal.WorkerActivitiesPerSecond)
				assert.Equal(t, "/path/to/blacklist.json", cfg.BlacklistPath)
			},
		},
		{
			name: "config with defaults",
			configFile: `
database:
  host: localhost
  user: testuser
  password: testpass
  dbname: testdb
nats:
  url: "nats://localhost:4222"
temporal:
  host_port: "localhost:7233"
  namespace: "default"
`,
			expectError: false,
			validate: func(t *testing.T, cfg *EventBridgeConfig) {
				// Check defaults
				assert.Equal(t, 5432, cfg.Database.Port)
				assert.Equal(t, "disable", cfg.Database.SSLMode)
				assert.Equal(t, 10, cfg.NATS.MaxReconnects)
				assert.Equal(t, "2s", cfg.NATS.ReconnectWait.String())
				assert.Equal(t, "BLOCKCHAIN_EVENTS", cfg.NATS.StreamName)
				assert.Equal(t, "event-bridge", cfg.NATS.ConsumerName)
				assert.Equal(t, 30*time.Second, cfg.NATS.AckWait)
				assert.Equal(t, 3, cfg.NATS.MaxDeliver)
				assert.Equal(t, "localhost:7233", cfg.Temporal.HostPort)
				assert.Equal(t, "default", cfg.Temporal.Namespace)
				assert.Equal(t, "token-indexing", cfg.Temporal.TokenTaskQueue)
				assert.Equal(t, 50, cfg.Temporal.MaxConcurrentActivityExecutionSize)
				assert.Equal(t, 50.0, cfg.Temporal.WorkerActivitiesPerSecond)
			},
		},
		{
			name:        "missing config file",
			configFile:  "",
			expectError: false,
			validate:    nil,
		},
		{
			name: "invalid yaml",
			configFile: `
				database:
				  host: localhost
				  port: invalid
			`,
			expectError: true, // Invalid port should cause unmarshal error
			validate:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			var configFile string

			if tt.configFile != "" {
				configFile = filepath.Join(tmpDir, "config.yaml")
				err := os.WriteFile(configFile, []byte(tt.configFile), 0600)
				require.NoError(t, err)
			} else {
				configFile = filepath.Join(tmpDir, "nonexistent.yaml")
			}

			cfg, err := LoadEventBridgeConfig(configFile, "")

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, cfg)
			} else {
				if tt.validate != nil {
					require.NoError(t, err)
					require.NotNil(t, cfg)
					tt.validate(t, cfg)
				}
			}
		})
	}
}

func TestLoadWorkerCoreConfig(t *testing.T) {
	tests := []struct {
		name        string
		configFile  string
		expectError bool
		validate    func(*testing.T, *WorkerCoreConfig)
	}{
		{
			name: "valid config file",
			configFile: `
debug: true
sentry_dsn: "https://sentry.example.com"
database:
  host: localhost
  port: 5432
  user: testuser
  password: testpass
  dbname: testdb
  sslmode: require
temporal:
  host_port: "localhost:7233"
  namespace: "default"
  token_task_queue: "token-indexing"
  max_concurrent_activity_execution_size: 50
  worker_activities_per_second: 50
ethereum:
  websocket_url: "ws://localhost:8545"
  rpc_url: "http://localhost:8545"
  chain_id: "eip155:1"
  start_block: 1000
tezos:
  api_url: "https://api.tzkt.io"
  websocket_url: "ws://localhost:8080"
  chain_id: "tezos:mainnet"
  start_level: 5000
vendors:
  artblocks_url: "https://custom-artblocks.com/graphql"
uri:
  ipfs_gateways:
    - "https://ipfs.io"
    - "https://gateway.pinata.cloud"
  arweave_gateways:
    - "https://arweave.net"
    - "https://arweave.live"
ethereum_token_sweep_start_block: 2000
tezos_token_sweep_start_block: 6000
publisher_registry_path: "/path/to/publisher.json"
blacklist_path: "/path/to/blacklist.json"
`,
			expectError: false,
			validate: func(t *testing.T, cfg *WorkerCoreConfig) {
				assert.True(t, cfg.Debug)
				assert.Equal(t, "https://sentry.example.com", cfg.SentryDSN)
				assert.Equal(t, "localhost", cfg.Database.Host)
				assert.Equal(t, 5432, cfg.Database.Port)
				assert.Equal(t, "testuser", cfg.Database.User)
				assert.Equal(t, "testpass", cfg.Database.Password)
				assert.Equal(t, "testdb", cfg.Database.DBName)
				assert.Equal(t, "require", cfg.Database.SSLMode)
				assert.Equal(t, "ws://localhost:8545", cfg.Ethereum.WebSocketURL)
				assert.Equal(t, "http://localhost:8545", cfg.Ethereum.RPCURL)
				assert.Equal(t, "eip155:1", string(cfg.Ethereum.ChainID))
				assert.Equal(t, uint64(1000), cfg.Ethereum.StartBlock)
				assert.Equal(t, "https://api.tzkt.io", cfg.Tezos.APIURL)
				assert.Equal(t, "ws://localhost:8080", cfg.Tezos.WebSocketURL)
				assert.Equal(t, "tezos:mainnet", string(cfg.Tezos.ChainID))
				assert.Equal(t, uint64(5000), cfg.Tezos.StartLevel)
				assert.Equal(t, "https://custom-artblocks.com/graphql", cfg.Vendors.ArtBlocksURL)
				assert.Len(t, cfg.URI.IPFSGateways, 2)
				assert.Len(t, cfg.URI.ArweaveGateways, 2)
				assert.Equal(t, uint64(2000), cfg.EthereumTokenSweepStartBlock)
				assert.Equal(t, uint64(6000), cfg.TezosTokenSweepStartBlock)
				assert.Equal(t, "/path/to/publisher.json", cfg.PublisherRegistryPath)
				assert.Equal(t, "/path/to/blacklist.json", cfg.BlacklistPath)
			},
		},
		{
			name: "config with defaults",
			configFile: `
database:
  host: localhost
  user: testuser
  password: testpass
  dbname: testdb
temporal:
  host_port: "localhost:7233"
ethereum:
  websocket_url: "ws://localhost:8545"
  rpc_url: "http://localhost:8545"
  chain_id: "eip155:1"
tezos:
  api_url: "https://api.tzkt.io"
  websocket_url: "ws://localhost:8080"
  chain_id: "tezos:mainnet"
`,
			expectError: false,
			validate: func(t *testing.T, cfg *WorkerCoreConfig) {
				// Check defaults
				assert.Equal(t, 5432, cfg.Database.Port)
				assert.Equal(t, "disable", cfg.Database.SSLMode)
				assert.Equal(t, "localhost:7233", cfg.Temporal.HostPort)
				assert.Equal(t, "default", cfg.Temporal.Namespace)
				assert.Equal(t, "token-indexing", cfg.Temporal.TokenTaskQueue)
				assert.Equal(t, 50, cfg.Temporal.MaxConcurrentActivityExecutionSize)
				assert.Equal(t, 50.0, cfg.Temporal.WorkerActivitiesPerSecond)
				assert.Equal(t, "https://api.tzkt.io", cfg.Tezos.APIURL)
				assert.Equal(t, "https://artblocks-mainnet.hasura.app/v1/graphql", cfg.Vendors.ArtBlocksURL)
			},
		},
		{
			name:        "missing config file",
			configFile:  "",
			expectError: false,
			validate:    nil,
		},
		{
			name: "invalid yaml",
			configFile: `
				database:
				  host: localhost
				  port: invalid
			`,
			expectError: true, // Invalid port should cause unmarshal error
			validate:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			var configFile string

			if tt.configFile != "" {
				configFile = filepath.Join(tmpDir, "config.yaml")
				err := os.WriteFile(configFile, []byte(tt.configFile), 0600)
				require.NoError(t, err)
			} else {
				configFile = filepath.Join(tmpDir, "nonexistent.yaml")
			}

			cfg, err := LoadWorkerCoreConfig(configFile, "")

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, cfg)
			} else {
				if tt.validate != nil {
					require.NoError(t, err)
					require.NotNil(t, cfg)
					tt.validate(t, cfg)
				}
			}
		})
	}
}

func TestLoadAPIConfig(t *testing.T) {
	tests := []struct {
		name        string
		configFile  string
		expectError bool
		validate    func(*testing.T, *APIConfig)
	}{
		{
			name: "valid config file",
			configFile: `
debug: true
sentry_dsn: "https://sentry.example.com"
server:
  host: "127.0.0.1"
  port: 9090
  read_timeout: 20
  write_timeout: 20
  idle_timeout: 180
database:
  host: localhost
  port: 5432
  user: testuser
  password: testpass
  dbname: testdb
temporal:
  host_port: "localhost:7233"
  namespace: "production"
auth:
  jwt_public_key: "test-public-key"
  api_keys:
    - "key1"
    - "key2"
blacklist_path: "/path/to/blacklist.json"
tezos:
  chain_id: "tezos:mainnet"
ethereum:
  chain_id: "eip155:1"
`,
			expectError: false,
			validate: func(t *testing.T, cfg *APIConfig) {
				assert.True(t, cfg.Debug)
				assert.Equal(t, "127.0.0.1", cfg.Server.Host)
				assert.Equal(t, 9090, cfg.Server.Port)
				assert.Equal(t, 20, cfg.Server.ReadTimeout)
				assert.Equal(t, 180, cfg.Server.IdleTimeout)
				assert.Equal(t, "test-public-key", cfg.Auth.JWTPublicKey)
				assert.Len(t, cfg.Auth.APIKeys, 2)
				assert.Equal(t, "/path/to/blacklist.json", cfg.BlacklistPath)
				assert.Equal(t, "tezos:mainnet", string(cfg.Tezos.ChainID))
				assert.Equal(t, "eip155:1", string(cfg.Ethereum.ChainID))
			},
		},
		{
			name:        "missing config file - should work with env vars",
			configFile:  "",
			expectError: false, // API config allows missing config file
			validate: func(t *testing.T, cfg *APIConfig) {
				// Should use defaults
				assert.NotNil(t, cfg)
				assert.False(t, cfg.Debug)                  // default
				assert.Equal(t, "0.0.0.0", cfg.Server.Host) // default
				assert.Equal(t, 8080, cfg.Server.Port)      // default
				assert.Equal(t, "tezos:mainnet", string(cfg.Tezos.ChainID))
				assert.Equal(t, "eip155:1", string(cfg.Ethereum.ChainID))
			},
		},
		{
			name: "config with defaults",
			configFile: `
database:
  host: localhost
  user: testuser
  password: testpass
  dbname: testdb
`,
			expectError: false,
			validate: func(t *testing.T, cfg *APIConfig) {
				assert.False(t, cfg.Debug)                   // default
				assert.Equal(t, "0.0.0.0", cfg.Server.Host)  // default
				assert.Equal(t, 8080, cfg.Server.Port)       // default
				assert.Equal(t, 10, cfg.Server.ReadTimeout)  // default
				assert.Equal(t, 10, cfg.Server.WriteTimeout) // default
				assert.Equal(t, 120, cfg.Server.IdleTimeout) // default
				assert.Equal(t, "tezos:mainnet", string(cfg.Tezos.ChainID))
				assert.Equal(t, "eip155:1", string(cfg.Ethereum.ChainID))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var configFile string

			if tt.configFile != "" {
				tmpDir := t.TempDir()
				configFile = filepath.Join(tmpDir, "config.yaml")
				err := os.WriteFile(configFile, []byte(tt.configFile), 0600)
				require.NoError(t, err)
			} else {
				// For missing config file, use empty string to let viper search in config/ directory
				// or use a non-existent path that viper will handle gracefully
				configFile = ""
			}

			cfg, err := LoadAPIConfig(configFile, "")

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, cfg)
			} else {
				if tt.validate != nil {
					require.NoError(t, err)
					require.NotNil(t, cfg)
					tt.validate(t, cfg)
				}
			}
		})
	}
}

func TestLoadWorkerMediaConfig(t *testing.T) {
	tests := []struct {
		name        string
		configFile  string
		expectError bool
		validate    func(*testing.T, *WorkerMediaConfig)
	}{
		{
			name: "valid config file",
			configFile: `
debug: true
sentry_dsn: "https://sentry.example.com"
database:
  host: localhost
  port: 5432
  user: testuser
  password: testpass
  dbname: testdb
  sslmode: require
temporal:
  host_port: "localhost:7233"
  namespace: "default"
  media_task_queue: "custom-media-queue"
  max_concurrent_activity_execution_size: 20
  worker_activities_per_second: 20
uri:
  ipfs_gateways:
    - "https://ipfs.io"
    - "https://gateway.pinata.cloud"
  arweave_gateways:
    - "https://arweave.net"
    - "https://arweave.live"
cloudflare:
  account_id: "test-account-id"
  api_token: "test-api-token"
max_static_image_size: 5242880
max_animated_image_size: 10485760
max_video_size: 104857600
`,
			expectError: false,
			validate: func(t *testing.T, cfg *WorkerMediaConfig) {
				assert.True(t, cfg.Debug)
				assert.Equal(t, "https://sentry.example.com", cfg.SentryDSN)
				assert.Equal(t, "localhost", cfg.Database.Host)
				assert.Equal(t, 5432, cfg.Database.Port)
				assert.Equal(t, "testuser", cfg.Database.User)
				assert.Equal(t, "testpass", cfg.Database.Password)
				assert.Equal(t, "testdb", cfg.Database.DBName)
				assert.Equal(t, "require", cfg.Database.SSLMode)
				assert.Equal(t, "localhost:7233", cfg.Temporal.HostPort)
				assert.Equal(t, "default", cfg.Temporal.Namespace)
				assert.Equal(t, "custom-media-queue", cfg.Temporal.MediaTaskQueue)
				assert.Equal(t, 20, cfg.Temporal.MaxConcurrentActivityExecutionSize)
				assert.Equal(t, 20.0, cfg.Temporal.WorkerActivitiesPerSecond)
				assert.Equal(t, "test-account-id", cfg.Cloudflare.AccountID)
				assert.Equal(t, "test-api-token", cfg.Cloudflare.APIToken)
				assert.Len(t, cfg.URI.IPFSGateways, 2)
				assert.Len(t, cfg.URI.ArweaveGateways, 2)
				assert.Equal(t, int64(5242880), cfg.MaxStaticImageSize)
				assert.Equal(t, int64(10485760), cfg.MaxAnimatedImageSize)
				assert.Equal(t, int64(104857600), cfg.MaxVideoSize)
			},
		},
		{
			name: "config with defaults",
			configFile: `
database:
  host: localhost
  user: testuser
  password: testpass
  dbname: testdb
`,
			expectError: false,
			validate: func(t *testing.T, cfg *WorkerMediaConfig) {
				// Check defaults
				assert.Equal(t, 5432, cfg.Database.Port)
				assert.Equal(t, "disable", cfg.Database.SSLMode)
				assert.Equal(t, "media-indexing", cfg.Temporal.MediaTaskQueue)
				assert.Equal(t, 10, cfg.Temporal.MaxConcurrentActivityExecutionSize)
				assert.Equal(t, 10.0, cfg.Temporal.WorkerActivitiesPerSecond)
				assert.Len(t, cfg.URI.IPFSGateways, 2)
				assert.Len(t, cfg.URI.ArweaveGateways, 1)
				assert.Equal(t, int64(10*1024*1024), cfg.MaxStaticImageSize)   // 10MB
				assert.Equal(t, int64(50*1024*1024), cfg.MaxAnimatedImageSize) // 50MB
				assert.Equal(t, int64(300*1024*1024), cfg.MaxVideoSize)        // 300MB
			},
		},
		{
			name:        "missing config file",
			configFile:  "",
			expectError: false,
			validate:    nil,
		},
		{
			name: "invalid yaml",
			configFile: `
				database:
				  host: localhost
				  port: invalid
			`,
			expectError: true, // Invalid port should cause unmarshal error
			validate:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			var configFile string

			if tt.configFile != "" {
				configFile = filepath.Join(tmpDir, "config.yaml")
				err := os.WriteFile(configFile, []byte(tt.configFile), 0600)
				require.NoError(t, err)
			} else {
				configFile = filepath.Join(tmpDir, "nonexistent.yaml")
			}

			cfg, err := LoadWorkerMediaConfig(configFile, "")

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, cfg)
			} else {
				if tt.validate != nil {
					require.NoError(t, err)
					require.NotNil(t, cfg)
					tt.validate(t, cfg)
				}
			}
		})
	}
}

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

func TestConfigWithEnvironmentVariables(t *testing.T) {
	tmpDir := t.TempDir()

	// Create temporary directory for env files
	envDir := filepath.Join(tmpDir, "env")
	err := os.MkdirAll(envDir, 0750)
	require.NoError(t, err)

	// Create .env file with environment variables
	// Note: Viper uses FF_INDEXER_ prefix, so env vars need the prefix
	envFile := filepath.Join(envDir, ".env")
	envContent := `FF_INDEXER_DEBUG=true
FF_INDEXER_DATABASE_HOST=env-host
FF_INDEXER_DATABASE_PORT=3306
FF_INDEXER_DATABASE_USER=env-user
FF_INDEXER_DATABASE_PASSWORD=env-pass
FF_INDEXER_DATABASE_DBNAME=env-db
FF_INDEXER_DATABASE_SSLMODE=require
`
	err = os.WriteFile(envFile, []byte(envContent), 0600)
	require.NoError(t, err)

	// Create config file with different values to verify env vars override
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

	err = os.WriteFile(configPath, []byte(configFile), 0600)
	require.NoError(t, err)

	// Load config with envPath pointing to the temporary env directory
	cfg, err := LoadAPIConfig(configPath, envDir)
	require.NoError(t, err)
	require.NotNil(t, cfg)

	// Verify that environment variables from .env file override config file values
	// The .env file is loaded via godotenv.Overload, which sets actual environment variables
	// Viper's AutomaticEnv then picks them up with FF_INDEXER_ prefix
	assert.True(t, cfg.Debug)                          // Should be true from .env file, not false from config
	assert.Equal(t, "env-host", cfg.Database.Host)     // Should be from .env file
	assert.Equal(t, 3306, cfg.Database.Port)           // Should be from .env file
	assert.Equal(t, "env-user", cfg.Database.User)     // Should be from .env file
	assert.Equal(t, "env-pass", cfg.Database.Password) // Should be from .env file
	assert.Equal(t, "env-db", cfg.Database.DBName)     // Should be from .env file
	assert.Equal(t, "require", cfg.Database.SSLMode)   // Should be from .env file
}

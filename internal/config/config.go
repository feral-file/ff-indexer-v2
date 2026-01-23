package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/spf13/viper"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
)

// BaseConfig holds base configuration
type BaseConfig struct {
	Debug     bool   `mapstructure:"debug"`
	SentryDSN string `mapstructure:"sentry_dsn"`
}

// URIConfig holds URI resolver configuration
type URIConfig struct {
	IPFSGateways    []string `mapstructure:"ipfs_gateways"`
	ArweaveGateways []string `mapstructure:"arweave_gateways"`
	OnchfsGateways  []string `mapstructure:"onchfs_gateways"`
}

// DatabaseConfig holds database configuration
type DatabaseConfig struct {
	Host            string        `mapstructure:"host"`
	Port            int           `mapstructure:"port"`
	ReadHost        string        `mapstructure:"read_host"`
	ReadPort        int           `mapstructure:"read_port"`
	User            string        `mapstructure:"user"`
	Password        string        `mapstructure:"password"`
	DBName          string        `mapstructure:"dbname"`
	SSLMode         string        `mapstructure:"sslmode"`
	MaxOpenConns    int           `mapstructure:"max_open_conns"`     // Maximum number of open connections to the database
	MaxIdleConns    int           `mapstructure:"max_idle_conns"`     // Maximum number of idle connections in the pool
	ConnMaxLifetime time.Duration `mapstructure:"conn_max_lifetime"`  // Maximum amount of time a connection may be reused (e.g., "5m", "1h")
	ConnMaxIdleTime time.Duration `mapstructure:"conn_max_idle_time"` // Maximum amount of time a connection may be idle (e.g., "10m", "30m")
}

// NATSConfig holds NATS JetStream configuration
type NATSConfig struct {
	URL            string        `mapstructure:"url"`
	StreamName     string        `mapstructure:"stream_name"`
	ConsumerName   string        `mapstructure:"consumer_name"`
	MaxReconnects  int           `mapstructure:"max_reconnects"`
	ReconnectWait  time.Duration `mapstructure:"reconnect_wait"`
	ConnectionName string        `mapstructure:"connection_name"`
	AckWait        time.Duration `mapstructure:"ack_wait"`
	MaxDeliver     int           `mapstructure:"max_deliver"`
}

// EthereumConfig holds Ethereum-specific configuration
type EthereumConfig struct {
	WebSocketURL         string        `mapstructure:"websocket_url"`
	RPCURL               string        `mapstructure:"rpc_url"`
	ChainID              domain.Chain  `mapstructure:"chain_id"`
	StartBlock           uint64        `mapstructure:"start_block"`
	BlockHeadTTL         time.Duration `mapstructure:"block_head_ttl"`
	BlockHeadStaleWindow time.Duration `mapstructure:"block_head_stale_window"`
}

// TezosConfig holds Tezos-specific configuration
type TezosConfig struct {
	APIURL               string        `mapstructure:"api_url"`
	WebSocketURL         string        `mapstructure:"websocket_url"`
	ChainID              domain.Chain  `mapstructure:"chain_id"`
	StartLevel           uint64        `mapstructure:"start_level"`
	BlockHeadTTL         time.Duration `mapstructure:"block_head_ttl"`
	BlockHeadStaleWindow time.Duration `mapstructure:"block_head_stale_window"`
}

// TemporalConfig holds Temporal configuration
type TemporalConfig struct {
	HostPort                           string  `mapstructure:"host_port"`
	Namespace                          string  `mapstructure:"namespace"`
	TokenTaskQueue                     string  `mapstructure:"token_task_queue"`
	MediaTaskQueue                     string  `mapstructure:"media_task_queue"`
	MaxConcurrentActivityExecutionSize int     `mapstructure:"max_concurrent_activity_execution_size"`
	WorkerActivitiesPerSecond          float64 `mapstructure:"worker_activities_per_second"`
	MaxConcurrentActivityTaskPollers   int     `mapstructure:"max_concurrent_activity_task_pollers"`
}

// VendorsConfig holds vendor API configurations
type VendorsConfig struct {
	ArtBlocksURL  string `mapstructure:"artblocks_url"`
	FeralFileURL  string `mapstructure:"feralfile_url"`
	ObjktURL      string `mapstructure:"objkt_url"`
	OpenSeaURL    string `mapstructure:"opensea_url"`
	OpenSeaAPIKey string `mapstructure:"opensea_api_key"`
}

// ServerConfig holds HTTP server configuration
type ServerConfig struct {
	Host         string `mapstructure:"host"`
	Port         int    `mapstructure:"port"`
	ReadTimeout  int    `mapstructure:"read_timeout"`  // in seconds
	WriteTimeout int    `mapstructure:"write_timeout"` // in seconds
	IdleTimeout  int    `mapstructure:"idle_timeout"`  // in seconds
}

// AuthConfig holds authentication configuration
type AuthConfig struct {
	JWTPublicKey string   `mapstructure:"jwt_public_key"`
	APIKeys      []string `mapstructure:"api_keys"`
}

// WorkerConfig holds worker configuration
type WorkerConfig struct {
	WorkerPoolSize  int `mapstructure:"pool_size"`
	WorkerQueueSize int `mapstructure:"queue_size"`
}

// EthereumEmitterConfig holds configuration for ethereum-event-emitter
type EthereumEmitterConfig struct {
	BaseConfig `mapstructure:",squash"`
	Worker     WorkerConfig   `mapstructure:"worker"`
	Database   DatabaseConfig `mapstructure:"database"`
	NATS       NATSConfig     `mapstructure:"nats"`
	Ethereum   EthereumConfig `mapstructure:"ethereum"`
}

// TezosEmitterConfig holds configuration for tezos-event-emitter
type TezosEmitterConfig struct {
	BaseConfig `mapstructure:",squash"`
	Worker     WorkerConfig   `mapstructure:"worker"`
	Database   DatabaseConfig `mapstructure:"database"`
	NATS       NATSConfig     `mapstructure:"nats"`
	Tezos      TezosConfig    `mapstructure:"tezos"`
}

// EventBridgeConfig holds configuration for event-bridge
type EventBridgeConfig struct {
	BaseConfig    `mapstructure:",squash"`
	Database      DatabaseConfig `mapstructure:"database"`
	NATS          NATSConfig     `mapstructure:"nats"`
	Temporal      TemporalConfig `mapstructure:"temporal"`
	BlacklistPath string         `mapstructure:"blacklist_path"`
}

// WorkerCoreConfig holds configuration for worker-core
type WorkerCoreConfig struct {
	BaseConfig                   `mapstructure:",squash"`
	Database                     DatabaseConfig `mapstructure:"database"`
	Temporal                     TemporalConfig `mapstructure:"temporal"`
	Ethereum                     EthereumConfig `mapstructure:"ethereum"`
	Tezos                        TezosConfig    `mapstructure:"tezos"`
	Vendors                      VendorsConfig  `mapstructure:"vendors"`
	URI                          URIConfig      `mapstructure:"uri"`
	EthereumTokenSweepStartBlock uint64         `mapstructure:"ethereum_token_sweep_start_block"`
	TezosTokenSweepStartBlock    uint64         `mapstructure:"tezos_token_sweep_start_block"`
	PublisherRegistryPath        string         `mapstructure:"publisher_registry_path"`
	BlacklistPath                string         `mapstructure:"blacklist_path"`

	// Budgeted Indexing Mode Configuration
	BudgetedIndexingEnabled           bool `mapstructure:"budgeted_indexing_enabled"`
	BudgetedIndexingDefaultDailyQuota int  `mapstructure:"budgeted_indexing_default_daily_quota"`
}

// APIConfig holds configuration for API server
type APIConfig struct {
	BaseConfig    `mapstructure:",squash"`
	Server        ServerConfig   `mapstructure:"server"`
	Database      DatabaseConfig `mapstructure:"database"`
	Temporal      TemporalConfig `mapstructure:"temporal"`
	Auth          AuthConfig     `mapstructure:"auth"`
	BlacklistPath string         `mapstructure:"blacklist_path"`
	Tezos         TezosConfig    `mapstructure:"tezos"`
	Ethereum      EthereumConfig `mapstructure:"ethereum"`
}

// CloudflareConfig holds Cloudflare configuration
type CloudflareConfig struct {
	// AccountID is the Cloudflare account ID (used for both Images and Stream)
	AccountID string `mapstructure:"account_id"`
	APIToken  string `mapstructure:"api_token"`
}

// WorkerMediaConfig holds configuration for worker-media
// RasterizerConfig holds SVG rasterizer configuration
type RasterizerConfig struct {
	// Width is the target width for SVG rasterization (0 = use SVG natural size)
	// Height is automatically calculated to maintain aspect ratio using ScaleBestFit
	Width int `mapstructure:"width"`
}

type WorkerMediaConfig struct {
	BaseConfig           `mapstructure:",squash"`
	Database             DatabaseConfig   `mapstructure:"database"`
	Temporal             TemporalConfig   `mapstructure:"temporal"`
	URI                  URIConfig        `mapstructure:"uri"`
	Cloudflare           CloudflareConfig `mapstructure:"cloudflare"`
	Rasterizer           RasterizerConfig `mapstructure:"rasterizer"`
	MaxStaticImageSize   int64            `mapstructure:"max_static_image_size"`
	MaxAnimatedImageSize int64            `mapstructure:"max_animated_image_size"`
	MaxVideoSize         int64            `mapstructure:"max_video_size"`
}

// MediaHealthSweeperConfig holds configuration for the media health sweeper
type MediaHealthSweeperConfig struct {
	URI          URIConfig     `mapstructure:"uri"`
	HTTPTimeout  time.Duration `mapstructure:"http_timeout"`
	BatchSize    int           `mapstructure:"batch_size"`
	RecheckAfter time.Duration `mapstructure:"recheck_after"`
	Worker       WorkerConfig  `mapstructure:"worker"`
}

// SweeperConfig holds configuration for the sweeper program
type SweeperConfig struct {
	BaseConfig         `mapstructure:",squash"`
	Database           DatabaseConfig           `mapstructure:"database"`
	Temporal           TemporalConfig           `mapstructure:"temporal"`
	MediaHealthSweeper MediaHealthSweeperConfig `mapstructure:"media_health_sweeper"`
}

// LoadEthereumEmitterConfig loads configuration for ethereum-event-emitter
func LoadEthereumEmitterConfig(configFile string, envPath string) (*EthereumEmitterConfig, error) {
	v := configureViper("ethereum-event-emitter", configFile, envPath)

	// Set defaults
	v.SetDefault("database.port", 5432)
	v.SetDefault("database.sslmode", "disable")
	v.SetDefault("nats.max_reconnects", 10)
	v.SetDefault("nats.reconnect_wait", "2s")
	v.SetDefault("nats.stream_name", "BLOCKCHAIN_EVENTS")
	v.SetDefault("ethereum.chain_id", "eip155:1")
	v.SetDefault("ethereum.block_head_ttl", 12)
	v.SetDefault("ethereum.block_head_stale_window", 60)
	v.SetDefault("worker.pool_size", 20)
	v.SetDefault("worker.queue_size", 2048)

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read config: %w", err)
	}

	var config EthereumEmitterConfig
	if err := v.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return &config, nil
}

// LoadTezosEmitterConfig loads configuration for tezos-event-emitter
func LoadTezosEmitterConfig(configFile string, envPath string) (*TezosEmitterConfig, error) {
	v := configureViper("tezos-event-emitter", configFile, envPath)

	// Set defaults
	v.SetDefault("database.port", 5432)
	v.SetDefault("database.sslmode", "disable")
	v.SetDefault("nats.max_reconnects", 10)
	v.SetDefault("nats.reconnect_wait", "2s")
	v.SetDefault("nats.stream_name", "BLOCKCHAIN_EVENTS")
	v.SetDefault("tezos.chain_id", "tezos:mainnet")
	v.SetDefault("tezos.block_head_ttl", 10)
	v.SetDefault("tezos.block_head_stale_window", 60)
	v.SetDefault("worker.pool_size", 20)
	v.SetDefault("worker.queue_size", 2048)

	if err := v.ReadInConfig(); err != nil {
		var error viper.ConfigFileNotFoundError
		if errors.As(err, &error) {
			// Config file not found, use environment variables
		} else {
			return nil, fmt.Errorf("failed to read config: %w", err)
		}
	}

	var config TezosEmitterConfig
	if err := v.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return &config, nil
}

// LoadEventBridgeConfig loads configuration for event-bridge
func LoadEventBridgeConfig(configFile string, envPath string) (*EventBridgeConfig, error) {
	v := configureViper("event-bridge", configFile, envPath)

	// Set defaults
	v.SetDefault("database.port", 5432)
	v.SetDefault("database.sslmode", "disable")
	v.SetDefault("nats.max_reconnects", 10)
	v.SetDefault("nats.reconnect_wait", "2s")
	v.SetDefault("nats.stream_name", "BLOCKCHAIN_EVENTS")
	v.SetDefault("nats.consumer_name", "event-bridge")
	v.SetDefault("nats.ack_wait", "30s")
	v.SetDefault("nats.max_deliver", 3)
	v.SetDefault("temporal.host_port", "localhost:7233")
	v.SetDefault("temporal.namespace", "default")
	v.SetDefault("temporal.token_task_queue", "token-indexing")
	v.SetDefault("temporal.max_concurrent_activity_execution_size", 50)
	v.SetDefault("temporal.worker_activities_per_second", 50)

	if err := v.ReadInConfig(); err != nil {
		var error viper.ConfigFileNotFoundError
		if errors.As(err, &error) {
			// Config file not found, use environment variables
		} else {
			return nil, fmt.Errorf("failed to read config: %w", err)
		}
	}

	var config EventBridgeConfig
	if err := v.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return &config, nil
}

// LoadWorkerCoreConfig loads configuration for worker-core
func LoadWorkerCoreConfig(configFile string, envPath string) (*WorkerCoreConfig, error) {
	v := configureViper("worker-core", configFile, envPath)

	// Set defaults
	v.SetDefault("database.port", 5432)
	v.SetDefault("database.sslmode", "disable")
	v.SetDefault("temporal.host_port", "localhost:7233")
	v.SetDefault("temporal.namespace", "default")
	v.SetDefault("temporal.token_task_queue", "token-indexing")
	v.SetDefault("temporal.max_concurrent_activity_execution_size", 50)
	v.SetDefault("temporal.worker_activities_per_second", 50)
	v.SetDefault("temporal.max_concurrent_activity_task_pollers", 10)
	v.SetDefault("ethereum.block_head_ttl", 12)
	v.SetDefault("ethereum.block_head_stale_window", 60)
	v.SetDefault("tezos.api_url", "https://api.tzkt.io")
	v.SetDefault("tezos.block_head_ttl", 10)
	v.SetDefault("tezos.block_head_stale_window", 60)
	v.SetDefault("vendors.artblocks_url", "https://artblocks-mainnet.hasura.app/v1/graphql")
	v.SetDefault("vendors.feralfile_url", "https://feralfile.com/api")
	v.SetDefault("vendors.objkt_url", "https://data.objkt.com/v3/graphql")
	v.SetDefault("vendors.opensea_url", "https://api.opensea.io/api/v2")
	v.SetDefault("uri.onchfs_gateways", []string{"https://onchfs.fxhash2.xyz"})
	v.SetDefault("budgeted_indexing_enabled", false)
	v.SetDefault("budgeted_indexing_default_daily_quota", 1000)

	if err := v.ReadInConfig(); err != nil {
		var error viper.ConfigFileNotFoundError
		if errors.As(err, &error) {
			// Config file not found, use environment variables
		} else {
			return nil, fmt.Errorf("failed to read config: %w", err)
		}
	}

	var config WorkerCoreConfig
	if err := v.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return &config, nil
}

// LoadAPIConfig loads configuration for API server
func LoadAPIConfig(configFile string, envPath string) (*APIConfig, error) {
	v := configureViper("api", configFile, envPath)

	// Set defaults
	v.SetDefault("debug", false)
	v.SetDefault("server.host", "0.0.0.0")
	v.SetDefault("server.port", 8080)
	v.SetDefault("server.read_timeout", 10)
	v.SetDefault("server.write_timeout", 10)
	v.SetDefault("server.idle_timeout", 120)
	v.SetDefault("database.port", 5432)
	v.SetDefault("database.sslmode", "disable")
	v.SetDefault("temporal.host_port", "localhost:7233")
	v.SetDefault("temporal.namespace", "default")
	v.SetDefault("temporal.token_task_queue", "token-indexing")
	v.SetDefault("temporal.max_concurrent_activity_execution_size", 50)
	v.SetDefault("temporal.worker_activities_per_second", 50)
	v.SetDefault("tezos.chain_id", "tezos:mainnet")
	v.SetDefault("ethereum.chain_id", "eip155:1")

	if err := v.ReadInConfig(); err != nil {
		var error viper.ConfigFileNotFoundError
		if errors.As(err, &error) {
			// Config file not found, use environment variables
		} else {
			return nil, fmt.Errorf("failed to read config: %w", err)
		}
	}

	var config APIConfig
	if err := v.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return &config, nil
}

// LoadWorkerMediaConfig loads configuration for worker-media
func LoadWorkerMediaConfig(configFile string, envPath string) (*WorkerMediaConfig, error) {
	v := configureViper("worker-media", configFile, envPath)

	// Set defaults
	v.SetDefault("database.port", 5432)
	v.SetDefault("database.sslmode", "disable")
	v.SetDefault("temporal.media_task_queue", "media-indexing")
	v.SetDefault("temporal.max_concurrent_activity_execution_size", 10)
	v.SetDefault("temporal.worker_activities_per_second", 10)
	v.SetDefault("temporal.max_concurrent_activity_task_pollers", 2)
	v.SetDefault("uri.ipfs_gateways", []string{"https://ipfs.io", "https://cloudflare-ipfs.com"})
	v.SetDefault("uri.arweave_gateways", []string{"https://arweave.net"})
	v.SetDefault("uri.onchfs_gateways", []string{"https://onchfs.fxhash2.xyz"})
	v.SetDefault("rasterizer.width", 2048)
	v.SetDefault("max_static_image_size", 10*1024*1024)   // 10MB
	v.SetDefault("max_animated_image_size", 50*1024*1024) // 50MB
	v.SetDefault("max_video_size", 300*1024*1024)         // 300MB

	if err := v.ReadInConfig(); err != nil {
		var error viper.ConfigFileNotFoundError
		if errors.As(err, &error) {
			// Config file not found, use environment variables
		} else {
			return nil, fmt.Errorf("failed to read config: %w", err)
		}
	}

	var config WorkerMediaConfig
	if err := v.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return &config, nil
}

// LoadSweeperConfig loads configuration for the sweeper program
func LoadSweeperConfig(configFile string, envPath string) (*SweeperConfig, error) {
	v := configureViper("sweeper", configFile, envPath)

	// Set defaults
	v.SetDefault("database.port", 5432)
	v.SetDefault("database.sslmode", "disable")
	v.SetDefault("database.max_open_conns", 5)
	v.SetDefault("database.max_idle_conns", 2)
	v.SetDefault("database.conn_max_lifetime", "1h")
	v.SetDefault("database.conn_max_idle_time", "10m")
	v.SetDefault("media_health_sweeper.http_timeout", "30s")
	v.SetDefault("media_health_sweeper.batch_size", 100)
	v.SetDefault("media_health_sweeper.worker.pool_size", 50)
	v.SetDefault("media_health_sweeper.worker.queue_size", 100)
	v.SetDefault("media_health_sweeper.recheck_after", "24h") // 1 day
	v.SetDefault("temporal.host_port", "localhost:7233")
	v.SetDefault("temporal.namespace", "default")
	v.SetDefault("temporal.token_task_queue", "token-indexer-task-queue")

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read config: %w", err)
	}

	var cfg SweeperConfig
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// Validate required fields
	if cfg.Database.Host == "" {
		return nil, errors.New("database.host is required")
	}
	if cfg.Database.DBName == "" {
		return nil, errors.New("database.dbname is required")
	}

	return &cfg, nil
}

// configureViper returns a viper instance with the config file and environment variables set
func configureViper(service string, configFile string, envPath string) *viper.Viper {
	v := viper.New()

	// Load environment variables
	loadEnv(envPath, service)

	// Set config file
	if configFile != "" {
		v.SetConfigFile(configFile)
	} else {
		v.SetConfigName("config")
		v.SetConfigType("yaml")
		// Search for config.yaml in multiple locations:
		// 1. Current directory
		v.AddConfigPath(".")
		// 2. Service-specific directory (e.g., cmd/sweeper/, cmd/api/)
		v.AddConfigPath(fmt.Sprintf("cmd/%s/", service))
		// 3. Config directory
		v.AddConfigPath("config/")
	}

	// Set environment variables
	v.SetEnvPrefix("FF_INDEXER")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// Explicitly bind all environment variables
	bindAllEnvVars(v)
	return v
}

// bindAllEnvVars explicitly binds all possible environment variables
// This is required for viper to map env vars to config struct fields when no config file exists
func bindAllEnvVars(v *viper.Viper) {
	// Common config keys
	commonKeys := []string{
		"debug",
		"sentry_dsn",
		// Database
		"database.host",
		"database.port",
		"database.read_host",
		"database.read_port",
		"database.user",
		"database.password",
		"database.dbname",
		"database.sslmode",
		"database.max_open_conns",
		"database.max_idle_conns",
		"database.conn_max_lifetime",
		"database.conn_max_idle_time",
		// NATS
		"nats.url",
		"nats.stream_name",
		"nats.consumer_name",
		"nats.max_reconnects",
		"nats.reconnect_wait",
		"nats.connection_name",
		"nats.ack_wait",
		"nats.max_deliver",
		// Ethereum
		"ethereum.websocket_url",
		"ethereum.rpc_url",
		"ethereum.chain_id",
		"ethereum.start_block",
		"ethereum.block_head_ttl",
		"ethereum.block_head_stale_window",
		// Tezos
		"tezos.api_url",
		"tezos.websocket_url",
		"tezos.chain_id",
		"tezos.start_level",
		"tezos.block_head_ttl",
		"tezos.block_head_stale_window",
		// Temporal
		"temporal.host_port",
		"temporal.namespace",
		"temporal.token_task_queue",
		"temporal.media_task_queue",
		"temporal.max_concurrent_activity_execution_size",
		"temporal.worker_activities_per_second",
		"temporal.max_concurrent_activity_task_pollers",
		// Vendors
		"vendors.artblocks_url",
		"vendors.feralfile_url",
		"vendors.objkt_url",
		"vendors.opensea_url",
		"vendors.opensea_api_key",
		// Server
		"server.host",
		"server.port",
		"server.read_timeout",
		"server.write_timeout",
		"server.idle_timeout",
		// Auth
		"auth.jwt_public_key",
		"auth.api_keys",
		// URI
		"uri.ipfs_gateways",
		"uri.arweave_gateways",
		"uri.onchfs_gateways",
		// Cloudflare
		"cloudflare.account_id",
		"cloudflare.api_token",
		// Worker specific
		"ethereum_token_sweep_start_block",
		"tezos_token_sweep_start_block",
		"publisher_registry_path",
		"blacklist_path",
		"budgeted_indexing_enabled",
		"budgeted_indexing_default_daily_quota",
		// Media specific
		"max_static_image_size",
		"max_animated_image_size",
		"max_video_size",
		"rasterizer.width",
		// Internal Worker config
		"worker.pool_size",
		"worker.queue_size",
		// Media Health Sweeper config
		"media_health_sweeper.http_timeout",
		"media_health_sweeper.batch_size",
		"media_health_sweeper.recheck_after",
		"media_health_sweeper.worker.pool_size",
		"media_health_sweeper.worker.queue_size",
		"media_health_sweeper.uri.ipfs_gateways",
		"media_health_sweeper.uri.arweave_gateways",
		"media_health_sweeper.uri.onchfs_gateways",
	}

	for _, key := range commonKeys {
		_ = v.BindEnv(key)
	}
}

// loadEnv loads environment variables from the config directory
func loadEnv(envPath string, service string) {
	// Always try shared base first, then local, then optional per-service local.
	envFiles := []string{".env", ".env.local"}
	if service != "" {
		envFiles = append(envFiles, ".env."+service+".local")
	}

	// Default to config directory
	if envPath == "" {
		envPath = "config/"
	}

	// Create candidates list
	for _, envFile := range envFiles {
		candidate := filepath.Join(envPath, envFile)
		_ = godotenv.Overload(candidate) // Overload lets later files override earlier ones
	}
}

// ChdirRepoRoot changes the current working directory to the repository root
func ChdirRepoRoot() {
	cwd, _ := os.Getwd()
	for range 5 {
		if _, err := os.Stat(filepath.Join(cwd, "config")); err == nil {
			_ = os.Chdir(cwd)
			return
		}
		cwd = filepath.Dir(cwd)
	}
}

// DSN returns the database connection string
func (c *DatabaseConfig) DSN() string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		c.Host, c.Port, c.User, c.Password, c.DBName, c.SSLMode)
}

// ReadDSN returns the read-replica database connection string.
// If ReadPort is not configured, it falls back to Port.
func (c *DatabaseConfig) ReadDSN() string {
	port := c.ReadPort
	if port == 0 {
		port = c.Port
	}

	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		c.ReadHost, port, c.User, c.Password, c.DBName, c.SSLMode)
}

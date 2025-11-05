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
}

// DatabaseConfig holds database configuration
type DatabaseConfig struct {
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	User     string `mapstructure:"user"`
	Password string `mapstructure:"password"`
	DBName   string `mapstructure:"dbname"`
	SSLMode  string `mapstructure:"sslmode"`
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
	WebSocketURL string       `mapstructure:"websocket_url"`
	RPCURL       string       `mapstructure:"rpc_url"`
	ChainID      domain.Chain `mapstructure:"chain_id"`
	StartBlock   uint64       `mapstructure:"start_block"`
}

// TezosConfig holds Tezos-specific configuration
type TezosConfig struct {
	APIURL       string       `mapstructure:"api_url"`
	WebSocketURL string       `mapstructure:"websocket_url"`
	ChainID      domain.Chain `mapstructure:"chain_id"`
	StartLevel   uint64       `mapstructure:"start_level"`
}

// TemporalConfig holds Temporal configuration
type TemporalConfig struct {
	HostPort                           string  `mapstructure:"host_port"`
	Namespace                          string  `mapstructure:"namespace"`
	TaskQueue                          string  `mapstructure:"task_queue"`
	MediaTaskQueue                     string  `mapstructure:"media_task_queue"`
	MaxConcurrentActivityExecutionSize int     `mapstructure:"max_concurrent_activity_execution_size"`
	WorkerActivitiesPerSecond          float64 `mapstructure:"worker_activities_per_second"`
}

// VendorsConfig holds vendor API configurations
type VendorsConfig struct {
	ArtBlocksURL string `mapstructure:"artblocks_url"`
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

// EthereumEmitterConfig holds configuration for ethereum-event-emitter
type EthereumEmitterConfig struct {
	BaseConfig `mapstructure:",squash"`
	Database   DatabaseConfig `mapstructure:"database"`
	NATS       NATSConfig     `mapstructure:"nats"`
	Ethereum   EthereumConfig `mapstructure:"ethereum"`
}

// TezosEmitterConfig holds configuration for tezos-event-emitter
type TezosEmitterConfig struct {
	BaseConfig `mapstructure:",squash"`
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
}

// APIConfig holds configuration for API server
type APIConfig struct {
	BaseConfig    `mapstructure:",squash"`
	Server        ServerConfig   `mapstructure:"server"`
	Database      DatabaseConfig `mapstructure:"database"`
	Temporal      TemporalConfig `mapstructure:"temporal"`
	Auth          AuthConfig     `mapstructure:"auth"`
	BlacklistPath string         `mapstructure:"blacklist_path"`
}

// CloudflareConfig holds Cloudflare configuration
type CloudflareConfig struct {
	// AccountID is the Cloudflare account ID (used for both Images and Stream)
	AccountID string `mapstructure:"account_id"`
	APIToken  string `mapstructure:"api_token"`
}

// WorkerMediaConfig holds configuration for worker-media
type WorkerMediaConfig struct {
	BaseConfig           `mapstructure:",squash"`
	Database             DatabaseConfig   `mapstructure:"database"`
	Temporal             TemporalConfig   `mapstructure:"temporal"`
	URI                  URIConfig        `mapstructure:"uri"`
	Cloudflare           CloudflareConfig `mapstructure:"cloudflare"`
	MaxStaticImageSize   int64            `mapstructure:"max_static_image_size"`
	MaxAnimatedImageSize int64            `mapstructure:"max_animated_image_size"`
	MaxVideoSize         int64            `mapstructure:"max_video_size"`
}

// LoadEthereumEmitterConfig loads configuration for ethereum-event-emitter
func LoadEthereumEmitterConfig(configPath string) (*EthereumEmitterConfig, error) {
	v := configureViper("ethereum-event-emitter", configPath)

	// Set defaults
	v.SetDefault("database.port", 5432)
	v.SetDefault("database.sslmode", "disable")
	v.SetDefault("nats.max_reconnects", 10)
	v.SetDefault("nats.reconnect_wait", "2s")
	v.SetDefault("nats.stream_name", "BLOCKCHAIN_EVENTS")
	v.SetDefault("ethereum.chain_id", "eip155:1")

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
func LoadTezosEmitterConfig(configPath string) (*TezosEmitterConfig, error) {
	v := configureViper("tezos-event-emitter", configPath)

	// Set defaults
	v.SetDefault("database.port", 5432)
	v.SetDefault("database.sslmode", "disable")
	v.SetDefault("nats.max_reconnects", 10)
	v.SetDefault("nats.reconnect_wait", "2s")
	v.SetDefault("nats.stream_name", "BLOCKCHAIN_EVENTS")
	v.SetDefault("tezos.chain_id", "tezos:mainnet")

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
func LoadEventBridgeConfig(configPath string) (*EventBridgeConfig, error) {
	v := configureViper("event-bridge", configPath)

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
	v.SetDefault("temporal.task_queue", "token-indexing")
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
func LoadWorkerCoreConfig(configPath string) (*WorkerCoreConfig, error) {
	v := configureViper("worker-core", configPath)

	// Set defaults
	v.SetDefault("database.port", 5432)
	v.SetDefault("database.sslmode", "disable")
	v.SetDefault("temporal.host_port", "localhost:7233")
	v.SetDefault("temporal.namespace", "default")
	v.SetDefault("temporal.task_queue", "token-indexing")
	v.SetDefault("temporal.max_concurrent_activity_execution_size", 50)
	v.SetDefault("temporal.worker_activities_per_second", 50)
	v.SetDefault("tezos.api_url", "https://api.tzkt.io")
	v.SetDefault("vendors.artblocks_url", "https://artblocks-mainnet.hasura.app/v1/graphql")

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
func LoadAPIConfig(configPath string) (*APIConfig, error) {
	v := configureViper("api", configPath)

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
	v.SetDefault("temporal.task_queue", "token-indexing")
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

	var config APIConfig
	if err := v.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	fmt.Println("Config:", config)

	return &config, nil
}

// LoadWorkerMediaConfig loads configuration for worker-media
func LoadWorkerMediaConfig(configPath string) (*WorkerMediaConfig, error) {
	v := configureViper("worker-media", configPath)

	// Set defaults
	v.SetDefault("database.port", 5432)
	v.SetDefault("database.sslmode", "disable")
	v.SetDefault("temporal.task_queue", "media-indexing")
	v.SetDefault("temporal.max_concurrent_activity_execution_size", 10)
	v.SetDefault("temporal.worker_activities_per_second", 10)
	v.SetDefault("uri.ipfs_gateways", []string{"https://ipfs.io", "https://cloudflare-ipfs.com"})
	v.SetDefault("uri.arweave_gateways", []string{"https://arweave.net"})
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

// configureViper returns a viper instance with the config file and environment variables set
func configureViper(service string, configFilePath string) *viper.Viper {
	v := viper.New()

	// Load environment variables
	loadEnv(service)

	// Set config file
	if configFilePath != "" {
		v.SetConfigFile(configFilePath)
	} else {
		v.SetConfigType("yaml")
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
		"database.user",
		"database.password",
		"database.dbname",
		"database.sslmode",
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
		// Tezos
		"tezos.api_url",
		"tezos.websocket_url",
		"tezos.chain_id",
		"tezos.start_level",
		// Temporal
		"temporal.host_port",
		"temporal.namespace",
		"temporal.task_queue",
		"temporal.media_task_queue",
		"temporal.max_concurrent_activity_execution_size",
		"temporal.worker_activities_per_second",
		// Vendors
		"vendors.artblocks_url",
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
		// Cloudflare
		"cloudflare.account_id",
		"cloudflare.api_token",
		// Worker specific
		"ethereum_token_sweep_start_block",
		"tezos_token_sweep_start_block",
		"publisher_registry_path",
		"blacklist_path",
		// Media specific
		"max_static_image_size",
		"max_animated_image_size",
		"max_video_size",
	}

	for _, key := range commonKeys {
		_ = v.BindEnv(key)
	}
}

// loadEnv loads environment variables from the config directory
func loadEnv(service string) {
	// Always try shared base first, then local, then optional per-service local.
	candidates := []string{
		"config/.env",
		"config/.env.local",
	}

	if service != "" {
		candidates = append(candidates, "config/.env."+service+".local")
	}

	for _, candidate := range candidates {
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

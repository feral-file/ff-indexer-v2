package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// BaseConfig holds base configuration
type BaseConfig struct {
	Debug     bool   `mapstructure:"debug"`
	SentryDSN string `mapstructure:"sentry_dsn"`
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
	MaxReconnects  int           `mapstructure:"max_reconnects"`
	ReconnectWait  time.Duration `mapstructure:"reconnect_wait"`
	ConnectionName string        `mapstructure:"connection_name"`
}

// EthereumConfig holds Ethereum-specific configuration
type EthereumConfig struct {
	WebSocketURL string `mapstructure:"websocket_url"`
	ChainID      string `mapstructure:"chain_id"`
	StartBlock   uint64 `mapstructure:"start_block"`
}

// TezosConfig holds Tezos-specific configuration
type TezosConfig struct {
	APIURL       string `mapstructure:"api_url"`
	WebSocketURL string `mapstructure:"websocket_url"`
	ChainID      string `mapstructure:"chain_id"`
	StartLevel   uint64 `mapstructure:"start_level"`
}

// EthereumEmitterConfig holds configuration for ethereum-event-emitter
type EthereumEmitterConfig struct {
	BaseConfig
	Database DatabaseConfig `mapstructure:"database"`
	NATS     NATSConfig     `mapstructure:"nats"`
	Ethereum EthereumConfig `mapstructure:"ethereum"`
}

// TezosEmitterConfig holds configuration for tezos-event-emitter
type TezosEmitterConfig struct {
	BaseConfig
	Database DatabaseConfig `mapstructure:"database"`
	NATS     NATSConfig     `mapstructure:"nats"`
	Tezos    TezosConfig    `mapstructure:"tezos"`
}

// LoadEthereumEmitterConfig loads configuration for ethereum-event-emitter
func LoadEthereumEmitterConfig(configPath string) (*EthereumEmitterConfig, error) {
	v := Viper(configPath)

	// Set defaults
	v.SetDefault("database.port", 5432)
	v.SetDefault("database.sslmode", "disable")
	v.SetDefault("nats.max_reconnects", 10)
	v.SetDefault("nats.reconnect_wait", "2s")
	v.SetDefault("nats.stream_name", "BLOCKCHAIN_EVENTS")
	v.SetDefault("ethereum.chain_id", "eip155:1")
	v.SetDefault("logger.level", "info")
	v.SetDefault("logger.format", "json")

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
	v := Viper(configPath)

	// Set defaults
	v.SetDefault("database.port", 5432)
	v.SetDefault("database.sslmode", "disable")
	v.SetDefault("nats.max_reconnects", 10)
	v.SetDefault("nats.reconnect_wait", "2s")
	v.SetDefault("nats.stream_name", "BLOCKCHAIN_EVENTS")
	v.SetDefault("tezos.chain_id", "tezos:mainnet")
	v.SetDefault("logger.level", "info")
	v.SetDefault("logger.format", "json")

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read config: %w", err)
	}

	var config TezosEmitterConfig
	if err := v.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return &config, nil
}

// Viper returns a viper instance with the config file and environment variables set
func Viper(configPath string) *viper.Viper {
	v := viper.New()

	// Set config file
	v.SetConfigFile(configPath)
	v.SetConfigType("yaml")
	v.AddConfigPath("/.config/")

	// Set environment variables
	v.SetEnvPrefix("FF_INDEXER")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()
	return v
}

// DSN returns the database connection string
func (c *DatabaseConfig) DSN() string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		c.Host, c.Port, c.User, c.Password, c.DBName, c.SSLMode)
}

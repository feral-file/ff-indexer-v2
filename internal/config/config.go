package config

import (
	"errors"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/spf13/viper"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/security/ssrf"
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

// WorkerPoolConfig configures one jobs.Worker poll loop (postgres job queue consumer).
type WorkerPoolConfig struct {
	Concurrency    int           `mapstructure:"concurrency"`
	PollInterval   time.Duration `mapstructure:"poll_interval"`
	BatchSize      int           `mapstructure:"batch_size"`
	CancelInterval time.Duration `mapstructure:"cancel_interval"`
}

// JobsConfig holds job queue names and worker pool settings for token_index and media_index.
type JobsConfig struct {
	TokenQueue  string           `mapstructure:"token_queue"`
	MediaQueue  string           `mapstructure:"media_queue"`
	TokenWorker WorkerPoolConfig `mapstructure:"token_worker"`
	MediaWorker WorkerPoolConfig `mapstructure:"media_worker"`
}

// VendorsConfig holds vendor API configurations
type VendorsConfig struct {
	ArtBlocksURL  string `mapstructure:"artblocks_url"`
	FeralFileURL  string `mapstructure:"feralfile_url"`
	ObjktURL      string `mapstructure:"objkt_url"`
	ObjktAPIKey   string `mapstructure:"objkt_api_key"`
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

// WorkerConfig holds worker configuration.
type WorkerConfig struct {
	WorkerPoolSize  int `mapstructure:"pool_size"`
	WorkerQueueSize int `mapstructure:"queue_size"`
}

// WorkerCoreConfig holds configuration for worker-core
type WorkerCoreConfig struct {
	BaseConfig  `mapstructure:",squash"`
	Database    DatabaseConfig    `mapstructure:"database"`
	Jobs        JobsConfig        `mapstructure:"jobs"`
	Ethereum    EthereumConfig    `mapstructure:"ethereum"`
	Tezos       TezosConfig       `mapstructure:"tezos"`
	Vendors     VendorsConfig     `mapstructure:"vendors"`
	URI         URIConfig         `mapstructure:"uri"`
	RateLimiter RateLimiterConfig `mapstructure:"rate_limiter"`
	// Security mirrors AppConfig.security for token-worker outbound HTTP (metadata / URI resolution).
	Security                     SecurityConfig `mapstructure:"security"`
	MediaEnabled                 bool           `mapstructure:"media_enabled"`
	EthereumTokenSweepStartBlock uint64         `mapstructure:"ethereum_token_sweep_start_block"`
	TezosTokenSweepStartBlock    uint64         `mapstructure:"tezos_token_sweep_start_block"`
	PublisherRegistryPath        string         `mapstructure:"publisher_registry_path"`
	BlacklistPath                string         `mapstructure:"blacklist_path"`

	// Budgeted Indexing Mode Configuration
	BudgetedIndexingEnabled           bool `mapstructure:"budgeted_indexing_enabled"`
	BudgetedIndexingDefaultDailyQuota int  `mapstructure:"budgeted_indexing_default_daily_quota"`

	// Owner Indexing Configuration (token-count targets for block-aligned chunking)
	EthereumOwnerFirstBatchTarget      int `mapstructure:"ethereum_owner_first_batch_target"`
	EthereumOwnerSubsequentBatchTarget int `mapstructure:"ethereum_owner_subsequent_batch_target"`
	TezosOwnerFirstBatchTarget         int `mapstructure:"tezos_owner_first_batch_target"`
	TezosOwnerSubsequentBatchTarget    int `mapstructure:"tezos_owner_subsequent_batch_target"`
}

// APIConfig holds configuration for API server
type APIConfig struct {
	BaseConfig    `mapstructure:",squash"`
	Server        ServerConfig   `mapstructure:"server"`
	Database      DatabaseConfig `mapstructure:"database"`
	Jobs          JobsConfig     `mapstructure:"jobs"`
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

// RateLimitConfig holds rate limiting configuration for a specific API provider
type RateLimitConfig struct {
	// RequestsPerSecond is the maximum RPS allowed by the provider
	RequestsPerSecond int `mapstructure:"requests_per_second"`

	// Burst is the maximum burst size for the token bucket (allows short bursts above RPS)
	// If not specified, defaults to RequestsPerSecond
	Burst int `mapstructure:"burst"`

	// MaxQueueTime is the maximum time a request can wait in queue for a token
	// Default: 5m
	MaxQueueTime time.Duration `mapstructure:"max_queue_time"`
}

// RateLimiterConfig holds process-local rate limiter configuration.
type RateLimiterConfig struct {
	// MaxWorkers is the maximum number of concurrent worker goroutines
	// Default: runtime.NumCPU() * 10
	MaxWorkers int `mapstructure:"max_workers"`

	// MaxQueueSize is the maximum number of tasks that can be queued
	// Default: 10000
	MaxQueueSize int `mapstructure:"max_queue_size"`

	// Provider-specific rate limits
	Providers map[string]RateLimitConfig `mapstructure:"providers"`
}

// RasterizerConfig holds SVG rasterizer configuration
type RasterizerConfig struct {
	// Width is the target width for SVG rasterization (0 = use SVG natural size)
	// Height is automatically calculated to maintain aspect ratio using ScaleBestFit
	Width int `mapstructure:"width"`

	// TimeoutMs is the maximum time to wait for page operations (default: 15000ms)
	TimeoutMs int `mapstructure:"timeout_ms"`

	// BrowserFallbackEnabled enables browser fallback for SVG rasterization
	BrowserFallbackEnabled bool `mapstructure:"browser_fallback_enabled"`
}

// TransformConfig holds configuration for image transformation
type TransformConfig struct {
	// Target sizes
	TargetImageSize   int64 `mapstructure:"target_image_size"`
	TargetImagePixels int64 `mapstructure:"target_image_pixels"`

	// Dimension limits
	MaxImageDimension         int `mapstructure:"max_image_dimension"`
	MaxAnimatedImageDimension int `mapstructure:"max_animated_image_dimension"`
	MinImageDimension         int `mapstructure:"min_image_dimension"`
	MinAnimatedImageDimension int `mapstructure:"min_animated_image_dimension"`
	ResizeStepPercentage      int `mapstructure:"resize_step_percentage"`

	// Compression settings (escape hatch)
	InitialQuality int `mapstructure:"initial_quality"`
	MinQuality     int `mapstructure:"min_quality"`
	QualityStep    int `mapstructure:"quality_step"`

	// Safety limits
	MaxInputBytes    int64 `mapstructure:"max_input_bytes"`
	MaxDecodedPixels int64 `mapstructure:"max_decoded_pixels"`

	// Timeouts
	TransformTimeout time.Duration `mapstructure:"transform_timeout"`

	// Worker pool
	WorkerConcurrency int `mapstructure:"worker_concurrency"`
}

// WorkerMediaConfig holds configuration for the media-indexing job worker.
type WorkerMediaConfig struct {
	BaseConfig `mapstructure:",squash"`
	Database   DatabaseConfig `mapstructure:"database"`
	Jobs       JobsConfig     `mapstructure:"jobs"`
	// Security mirrors AppConfig.security for media-worker outbound HTTP (URI resolution and downloads).
	Security     SecurityConfig   `mapstructure:"security"`
	MediaEnabled bool             `mapstructure:"media_enabled"`
	URI          URIConfig        `mapstructure:"uri"`
	Cloudflare   CloudflareConfig `mapstructure:"cloudflare"`
	Rasterizer   RasterizerConfig `mapstructure:"rasterizer"`
	Transform    TransformConfig  `mapstructure:"transform"`
	MaxImageSize int64            `mapstructure:"max_image_size"`
	MaxVideoSize int64            `mapstructure:"max_video_size"`
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
	Jobs               JobsConfig               `mapstructure:"jobs"`
	MediaHealthSweeper MediaHealthSweeperConfig `mapstructure:"media_health_sweeper"`
}

// SecurityConfig holds process-wide security controls (optional sections keyed under `security:`).
type SecurityConfig struct {
	SSRFProtection SSRFProtectionConfig `mapstructure:"ssrf_protection"`
}

// SSRFProtectionConfig configures outbound URL validation for the media health sweeper HTTP client.
type SSRFProtectionConfig struct {
	Enabled        bool                `mapstructure:"enabled"`
	MaxRedirects   int                 `mapstructure:"max_redirects"` // Redirect hops allowed after the initial request (see adapter.ssrfCheckRedirect).
	BlockMulticast bool                `mapstructure:"block_multicast"`
	Allowlist      SSRFAllowlistConfig `mapstructure:"allowlist"`
}

// SSRFAllowlistConfig lists destinations that bypass default SSRF block rules (use sparingly).
type SSRFAllowlistConfig struct {
	Domains []string `mapstructure:"domains"`
	IPs     []string `mapstructure:"ips"`
}

// AppConfig is the configuration for the single-process ff-indexer binary.
type AppConfig struct {
	BaseConfig         `mapstructure:",squash"`
	Server             ServerConfig             `mapstructure:"server"`
	Database           DatabaseConfig           `mapstructure:"database"`
	Auth               AuthConfig               `mapstructure:"auth"`
	Jobs               JobsConfig               `mapstructure:"jobs"`
	MediaEnabled       bool                     `mapstructure:"media_enabled"`
	Ethereum           EthereumConfig           `mapstructure:"ethereum"`
	Tezos              TezosConfig              `mapstructure:"tezos"`
	Vendors            VendorsConfig            `mapstructure:"vendors"`
	URI                URIConfig                `mapstructure:"uri"`
	RateLimiter        RateLimiterConfig        `mapstructure:"rate_limiter"`
	Cloudflare         CloudflareConfig         `mapstructure:"cloudflare"`
	Rasterizer         RasterizerConfig         `mapstructure:"rasterizer"`
	Transform          TransformConfig          `mapstructure:"transform"`
	MediaHealthSweeper MediaHealthSweeperConfig `mapstructure:"media_health_sweeper"`

	EthereumTokenSweepStartBlock uint64 `mapstructure:"ethereum_token_sweep_start_block"`
	TezosTokenSweepStartBlock    uint64 `mapstructure:"tezos_token_sweep_start_block"`
	PublisherRegistryPath        string `mapstructure:"publisher_registry_path"`
	BlacklistPath                string `mapstructure:"blacklist_path"`
	MaxImageSize                 int64  `mapstructure:"max_image_size"`
	MaxVideoSize                 int64  `mapstructure:"max_video_size"`

	BudgetedIndexingEnabled           bool `mapstructure:"budgeted_indexing_enabled"`
	BudgetedIndexingDefaultDailyQuota int  `mapstructure:"budgeted_indexing_default_daily_quota"`

	EthereumOwnerFirstBatchTarget      int `mapstructure:"ethereum_owner_first_batch_target"`
	EthereumOwnerSubsequentBatchTarget int `mapstructure:"ethereum_owner_subsequent_batch_target"`
	TezosOwnerFirstBatchTarget         int `mapstructure:"tezos_owner_first_batch_target"`
	TezosOwnerSubsequentBatchTarget    int `mapstructure:"tezos_owner_subsequent_batch_target"`

	Security SecurityConfig `mapstructure:"security"`
}

// LoadAppConfig loads unified configuration for cmd/ff-indexer.
func LoadAppConfig(configFile string, envPath string) (*AppConfig, error) {
	v := configureViper("ff-indexer", configFile, envPath)
	applyAppConfigDefaults(v)

	if err := v.ReadInConfig(); err != nil {
		var notFound viper.ConfigFileNotFoundError
		if !errors.As(err, &notFound) {
			return nil, fmt.Errorf("failed to read config: %w", err)
		}
	}

	var cfg AppConfig
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}
	if err := ValidateRequiredConfigValues(&cfg); err != nil {
		return nil, err
	}
	if err := validateSecurityConfig(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func validateSecurityConfig(cfg *AppConfig) error {
	sp := cfg.Security.SSRFProtection
	if sp.MaxRedirects < 0 {
		return fmt.Errorf("security.ssrf_protection.max_redirects must be >= 0 (got %d)", sp.MaxRedirects)
	}
	for _, raw := range sp.Allowlist.Domains {
		if err := ssrf.ValidateAllowlistDomainEntry(raw); err != nil {
			return fmt.Errorf("security.ssrf_protection.allowlist.domains: %w", err)
		}
	}
	if _, err := SSRFValidatorFromProtection(cfg.Security.SSRFProtection); err != nil {
		return err
	}
	return nil
}

// SSRFValidatorFromProtection builds an ssrf.Validator from unified SSRF settings, or nil when disabled.
// Shared by the media health sweeper, media worker, token worker (worker-core), and config validation.
func SSRFValidatorFromProtection(sp SSRFProtectionConfig) (*ssrf.Validator, error) {
	if !sp.Enabled {
		return nil, nil
	}
	opts := ssrf.Options{
		BlockMulticast: sp.BlockMulticast,
		AllowDomains:   append([]string(nil), sp.Allowlist.Domains...),
	}
	for _, s := range sp.Allowlist.IPs {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		ip, err := netip.ParseAddr(s)
		if err != nil {
			return nil, fmt.Errorf("invalid security.ssrf_protection.allowlist.ips entry %q: %w", s, err)
		}
		opts.AllowIPs = append(opts.AllowIPs, ip)
	}
	return ssrf.NewValidator(opts), nil
}

// ValidateRequiredConfigValues verifies that the unified ff-indexer process has
// the minimum required config values present before startup initializes shared
// dependencies.
func ValidateRequiredConfigValues(cfg *AppConfig) error {
	type reqField struct {
		name  string
		value string
	}
	requiredFields := []reqField{
		{name: "database.host", value: cfg.Database.Host},
		{name: "database.dbname", value: cfg.Database.DBName},
		{name: "jobs.token_queue", value: cfg.Jobs.TokenQueue},
		{name: "ethereum.rpc_url", value: cfg.Ethereum.RPCURL},
		{name: "ethereum.websocket_url", value: cfg.Ethereum.WebSocketURL},
		{name: "tezos.api_url", value: cfg.Tezos.APIURL},
		{name: "tezos.websocket_url", value: cfg.Tezos.WebSocketURL},
	}
	if cfg.MediaEnabled {
		requiredFields = append(requiredFields, reqField{name: "jobs.media_queue", value: cfg.Jobs.MediaQueue})
	}

	missingFields := make([]string, 0)
	for _, field := range requiredFields {
		if strings.TrimSpace(field.value) == "" {
			missingFields = append(missingFields, field.name)
		}
	}

	if len(missingFields) > 0 {
		return fmt.Errorf("missing required config values: %s", strings.Join(missingFields, ", "))
	}

	return nil
}

// ToAPIConfig maps AppConfig to the shape expected by the HTTP API server.
func (a *AppConfig) ToAPIConfig() *APIConfig {
	return &APIConfig{
		BaseConfig:    a.BaseConfig,
		Server:        a.Server,
		Database:      a.Database,
		Jobs:          a.Jobs,
		Auth:          a.Auth,
		BlacklistPath: a.BlacklistPath,
		Tezos:         a.Tezos,
		Ethereum:      a.Ethereum,
	}
}

// ToWorkerCoreConfig maps AppConfig for the token-indexing job worker.
func (a *AppConfig) ToWorkerCoreConfig() *WorkerCoreConfig {
	return &WorkerCoreConfig{
		BaseConfig:                         a.BaseConfig,
		Database:                           a.Database,
		Jobs:                               a.Jobs,
		Ethereum:                           a.Ethereum,
		Tezos:                              a.Tezos,
		Vendors:                            a.Vendors,
		URI:                                a.URI,
		RateLimiter:                        a.RateLimiter,
		Security:                           a.Security,
		MediaEnabled:                       a.MediaEnabled,
		EthereumTokenSweepStartBlock:       a.EthereumTokenSweepStartBlock,
		TezosTokenSweepStartBlock:          a.TezosTokenSweepStartBlock,
		PublisherRegistryPath:              a.PublisherRegistryPath,
		BlacklistPath:                      a.BlacklistPath,
		BudgetedIndexingEnabled:            a.BudgetedIndexingEnabled,
		BudgetedIndexingDefaultDailyQuota:  a.BudgetedIndexingDefaultDailyQuota,
		EthereumOwnerFirstBatchTarget:      a.EthereumOwnerFirstBatchTarget,
		EthereumOwnerSubsequentBatchTarget: a.EthereumOwnerSubsequentBatchTarget,
		TezosOwnerFirstBatchTarget:         a.TezosOwnerFirstBatchTarget,
		TezosOwnerSubsequentBatchTarget:    a.TezosOwnerSubsequentBatchTarget,
	}
}

// ToWorkerMediaConfig maps AppConfig for the media-indexing job worker.
func (a *AppConfig) ToWorkerMediaConfig() *WorkerMediaConfig {
	return &WorkerMediaConfig{
		BaseConfig:   a.BaseConfig,
		Database:     a.Database,
		Jobs:         a.Jobs,
		Security:     a.Security,
		MediaEnabled: a.MediaEnabled,
		URI:          a.URI,
		Cloudflare:   a.Cloudflare,
		Rasterizer:   a.Rasterizer,
		Transform:    a.Transform,
		MaxImageSize: a.MaxImageSize,
		MaxVideoSize: a.MaxVideoSize,
	}
}

// ToSweeperConfig maps AppConfig for the media health sweeper.
func (a *AppConfig) ToSweeperConfig() *SweeperConfig {
	return &SweeperConfig{
		BaseConfig:         a.BaseConfig,
		Database:           a.Database,
		Jobs:               a.Jobs,
		MediaHealthSweeper: a.MediaHealthSweeper,
	}
}

func applyAppConfigDefaults(v *viper.Viper) {
	// Base / API
	v.SetDefault("debug", false)
	v.SetDefault("server.host", "0.0.0.0")
	v.SetDefault("server.port", 8080)
	v.SetDefault("server.read_timeout", 10)
	v.SetDefault("server.write_timeout", 10)
	v.SetDefault("server.idle_timeout", 120)

	// Database (single process: prefer a larger shared pool)
	v.SetDefault("database.port", 5432)
	v.SetDefault("database.sslmode", "disable")
	v.SetDefault("database.max_open_conns", 80)
	v.SetDefault("database.max_idle_conns", 16)
	v.SetDefault("database.conn_max_lifetime", "5m")
	v.SetDefault("database.conn_max_idle_time", "10m")

	// Postgres job queue
	v.SetDefault("jobs.token_queue", "token_index")
	v.SetDefault("jobs.media_queue", "media_index")
	v.SetDefault("jobs.token_worker.concurrency", 5)
	v.SetDefault("jobs.token_worker.poll_interval", 2*time.Second)
	v.SetDefault("jobs.token_worker.batch_size", 100)
	v.SetDefault("jobs.token_worker.cancel_interval", 5*time.Second)
	v.SetDefault("jobs.media_worker.concurrency", 2)
	v.SetDefault("jobs.media_worker.poll_interval", 2*time.Second)
	v.SetDefault("jobs.media_worker.batch_size", 100)
	v.SetDefault("jobs.media_worker.cancel_interval", 5*time.Second)

	v.SetDefault("media_enabled", false)

	// Chains
	v.SetDefault("ethereum.chain_id", "eip155:1")
	v.SetDefault("ethereum.block_head_ttl", 12)
	v.SetDefault("ethereum.block_head_stale_window", 60)
	v.SetDefault("tezos.chain_id", "tezos:mainnet")
	v.SetDefault("tezos.api_url", "https://api.tzkt.io")
	v.SetDefault("tezos.block_head_ttl", 10)
	v.SetDefault("tezos.block_head_stale_window", 60)

	// Vendors
	v.SetDefault("vendors.artblocks_url", "https://artblocks-mainnet.hasura.app/v1/graphql")
	v.SetDefault("vendors.feralfile_url", "https://feralfile.com/api")
	v.SetDefault("vendors.objkt_url", "https://data.objkt.com/v3/graphql")
	v.SetDefault("vendors.opensea_url", "https://api.opensea.io/api/v2")

	// URI
	v.SetDefault("uri.onchfs_gateways", []string{"https://onchfs.fxhash2.xyz"})

	v.SetDefault("uri.ipfs_gateways", []string{"https://ipfs.io", "https://cloudflare-ipfs.com"})
	v.SetDefault("uri.arweave_gateways", []string{"https://arweave.net"})
	v.SetDefault("rasterizer.width", 2048)
	v.SetDefault("rasterizer.timeout_ms", 15000)
	v.SetDefault("rasterizer.browser_fallback_enabled", false)
	v.SetDefault("max_image_size", 10*1024*1024)
	v.SetDefault("max_video_size", 300*1024*1024)
	v.SetDefault("transform.target_image_size", int64(float64(10*1024*1024)*0.9))
	v.SetDefault("transform.target_image_pixels", int64(float64(50000000*0.9)))
	v.SetDefault("transform.max_image_dimension", 3840)
	v.SetDefault("transform.max_animated_image_dimension", 2048)
	v.SetDefault("transform.min_image_dimension", 1280)
	v.SetDefault("transform.min_animated_image_dimension", 640)
	v.SetDefault("transform.resize_step_percentage", 25)
	v.SetDefault("transform.initial_quality", 100)
	v.SetDefault("transform.min_quality", 60)
	v.SetDefault("transform.quality_step", 10)
	v.SetDefault("transform.max_input_bytes", 100*1024*1024)
	v.SetDefault("transform.max_decoded_pixels", int64(100000000))
	v.SetDefault("transform.transform_timeout", 60*time.Second)
	v.SetDefault("transform.worker_concurrency", 4)

	// Worker-core flat keys
	v.SetDefault("budgeted_indexing_enabled", false)
	v.SetDefault("budgeted_indexing_default_daily_quota", 1000)
	v.SetDefault("ethereum_owner_first_batch_target", 20)
	v.SetDefault("ethereum_owner_subsequent_batch_target", 3)
	v.SetDefault("tezos_owner_first_batch_target", 20)
	v.SetDefault("tezos_owner_subsequent_batch_target", 1)

	// Rate limiter
	v.SetDefault("rate_limiter.max_workers", 10)
	v.SetDefault("rate_limiter.max_queue_size", 10000)
	v.SetDefault("rate_limiter.providers.tzkt.requests_per_second", 10)
	v.SetDefault("rate_limiter.providers.tzkt.burst", 10)
	v.SetDefault("rate_limiter.providers.tzkt.max_queue_time", "15m")
	v.SetDefault("rate_limiter.providers.opensea.requests_per_second", 4)
	v.SetDefault("rate_limiter.providers.opensea.burst", 4)
	v.SetDefault("rate_limiter.providers.opensea.max_queue_time", "15m")
	v.SetDefault("rate_limiter.providers.objkt.requests_per_second", 2)
	v.SetDefault("rate_limiter.providers.objkt.burst", 2)
	v.SetDefault("rate_limiter.providers.objkt.max_queue_time", "15m")

	// Media health sweeper
	v.SetDefault("media_health_sweeper.http_timeout", "30s")
	v.SetDefault("media_health_sweeper.batch_size", 100)
	v.SetDefault("media_health_sweeper.worker.pool_size", 5)
	v.SetDefault("media_health_sweeper.worker.queue_size", 100)
	v.SetDefault("media_health_sweeper.recheck_after", "24h")

	// SSRF protection for media health HTTP client (recommended enabled in production).
	v.SetDefault("security.ssrf_protection.enabled", true)
	v.SetDefault("security.ssrf_protection.max_redirects", 3)
	v.SetDefault("security.ssrf_protection.block_multicast", false)
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
		// 2. Service-specific directory (e.g., cmd/ff-indexer/)
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
		// Job queue
		"jobs.token_queue",
		"jobs.media_queue",
		"jobs.token_worker.concurrency",
		"jobs.token_worker.poll_interval",
		"jobs.token_worker.batch_size",
		"jobs.token_worker.cancel_interval",
		"jobs.media_worker.concurrency",
		"jobs.media_worker.poll_interval",
		"jobs.media_worker.batch_size",
		"jobs.media_worker.cancel_interval",
		"media_enabled",
		// Vendors
		"vendors.artblocks_url",
		"vendors.feralfile_url",
		"vendors.objkt_url",
		"vendors.objkt_api_key",
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
		"ethereum_owner_first_batch_target",
		"ethereum_owner_subsequent_batch_target",
		"tezos_owner_first_batch_target",
		"tezos_owner_subsequent_batch_target",
		// Media specific
		"max_image_size",
		"max_video_size",
		"rasterizer.width",
		"rasterizer.timeout_ms",
		"rasterizer.browser_fallback_enabled",
		// Media Health Sweeper config
		"media_health_sweeper.http_timeout",
		"media_health_sweeper.batch_size",
		"media_health_sweeper.recheck_after",
		"media_health_sweeper.worker.pool_size",
		"media_health_sweeper.worker.queue_size",
		"media_health_sweeper.uri.ipfs_gateways",
		"media_health_sweeper.uri.arweave_gateways",
		"media_health_sweeper.uri.onchfs_gateways",
		"security.ssrf_protection.enabled",
		"security.ssrf_protection.max_redirects",
		"security.ssrf_protection.block_multicast",
		"security.ssrf_protection.allowlist.domains",
		"security.ssrf_protection.allowlist.ips",
		// Rate Limiter
		"rate_limiter.max_workers",
		"rate_limiter.max_queue_size",
		"rate_limiter.providers.tzkt.requests_per_second",
		"rate_limiter.providers.tzkt.burst",
		"rate_limiter.providers.tzkt.max_queue_time",
		"rate_limiter.providers.objkt.requests_per_second",
		"rate_limiter.providers.objkt.burst",
		"rate_limiter.providers.objkt.max_queue_time",
		"rate_limiter.providers.opensea.requests_per_second",
		"rate_limiter.providers.opensea.burst",
		"rate_limiter.providers.opensea.max_queue_time",
		// Transform
		"transform.target_image_size",
		"transform.target_image_pixels",
		"transform.max_image_dimension",
		"transform.max_animated_image_dimension",
		"transform.min_image_dimension",
		"transform.min_animated_image_dimension",
		"transform.resize_step_percentage",
		"transform.initial_quality",
		"transform.min_quality",
		"transform.quality_step",
		"transform.max_input_bytes",
		"transform.max_decoded_pixels",
		"transform.transform_timeout",
		"transform.worker_concurrency",
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

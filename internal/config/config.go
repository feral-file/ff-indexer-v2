package config

import (
	"errors"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/spf13/viper"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/media/phash"
	"github.com/feral-file/ff-indexer-v2/internal/media/probe"
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
	// ProbeMaxBytes caps how many body bytes a health probe reads for content validation
	// (0 = uri.DefaultProbeMaxBytes)
	ProbeMaxBytes int `mapstructure:"probe_max_bytes"`
	// KnownBadPageMarkers are case-insensitive substrings identifying gateway error pages
	// served with HTTP 200 (matched against HTML bodies only; operator-editable so a new
	// gateway quirk needs no deploy)
	KnownBadPageMarkers []string `mapstructure:"known_bad_page_markers"`
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
	BlockFlushTimeout    time.Duration `mapstructure:"block_flush_timeout"`

	// Credit guards against a metered RPC provider. Zero values disable each guard.
	// See ethereum.ClientGuards for full semantics; the short version:
	//
	// GetLogsSpanCap: the provider's eth_getLogs block-range cap as max
	// toBlock-fromBlock (10000 for Infura). Seeds pagination at the cap instead of
	// paying a halving cascade of rejected calls per walk.
	GetLogsSpanCap uint64 `mapstructure:"getlogs_span_cap"`
	// GetLogsCallBudget: max FilterLogs calls per pagination walk; exceeding it
	// aborts the walk. Size above ceil(chain head / span cap) — mainnet at a 10k cap
	// needs ~2,600 calls per full-history walk.
	GetLogsCallBudget int `mapstructure:"getlogs_call_budget"`
	// ScanWindowConcurrency: how many owner-scan windows are fetched from the
	// provider concurrently. The scan is purely RPC-latency-bound (measured: one
	// ~0.9s round-trip per window, windows independent of each other), so
	// wall-clock divides by roughly this factor at identical total credit cost —
	// only the request RATE rises. Windows still commit to the checkpoint cursor
	// strictly in order, so resumability is unaffected.
	//
	// Sizing is an operations decision per RPC vendor — credit-metered, flat-rate,
	// and self-hosted providers all want different values, so the binary ships a
	// conservative default (2) and deploy config sets the real number. Reason
	// from the full fan-out: every window issues the THREE merged owner-topic
	// eth_getLogs queries at once, and every token worker may run a scan, so
	//
	//   peak concurrent eth_getLogs = jobs.token_worker.concurrency × this × 3
	//
	// (5 × 2 × 3 = 30 at binary defaults; a 30-worker deployment at 4 is 360).
	// Throttling (429) is retried with backoff and the checkpoint resumes the
	// walk, so over-sizing degrades to "slower", not "broken" — but sustained
	// 429s burn the per-call retry budget, so size from the vendor's actual
	// limit rather than upward from symptoms.
	ScanWindowConcurrency int `mapstructure:"scan_window_concurrency"`
	// FullProvenanceDisabled: skip per-token history walks (full provenance and the
	// ERC-1155 owner event replay); tokens keep minimal provenance until backfilled.
	FullProvenanceDisabled bool `mapstructure:"full_provenance_disabled"`
}

// TezosConfig holds Tezos-specific configuration
type TezosConfig struct {
	APIURL               string        `mapstructure:"api_url"`
	WebSocketURL         string        `mapstructure:"websocket_url"`
	ChainID              domain.Chain  `mapstructure:"chain_id"`
	StartLevel           uint64        `mapstructure:"start_level"`
	BlockHeadTTL         time.Duration `mapstructure:"block_head_ttl"`
	BlockHeadStaleWindow time.Duration `mapstructure:"block_head_stale_window"`
	BlockFlushTimeout    time.Duration `mapstructure:"block_flush_timeout"`
}

// WorkerPoolConfig configures one jobs.Worker poll loop (postgres job queue consumer).
type WorkerPoolConfig struct {
	Concurrency    int           `mapstructure:"concurrency"`
	PollInterval   time.Duration `mapstructure:"poll_interval"`
	BatchSize      int           `mapstructure:"batch_size"`
	CancelInterval time.Duration `mapstructure:"cancel_interval"`
	// MaxAttempts is the maximum number of times a job may be claimed before it is permanently
	// failed by SweepOrphanedJobs. This breaks the crash loop caused by CGO/Rust SIGABRT panics.
	MaxAttempts int `mapstructure:"max_attempts"`
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
	FxhashURL     string `mapstructure:"fxhash_url"`
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
	Security SecurityConfig `mapstructure:"security"`
	// ModerationSweeper is mirrored here because the enricher schedules a fresh moderation
	// verdict's first sweeper re-check from moderation_sweeper.initial_recheck_interval —
	// the writer and the sweeper must share one knob or the operator guidance to
	// raise it (see the 021_reindex runbook) silently does nothing.
	ModerationSweeper            ModerationSweeperConfig `mapstructure:"moderation_sweeper"`
	MediaEnabled                 bool                    `mapstructure:"media_enabled"`
	EthereumTokenSweepStartBlock uint64                  `mapstructure:"ethereum_token_sweep_start_block"`
	TezosTokenSweepStartBlock    uint64                  `mapstructure:"tezos_token_sweep_start_block"`
	PublisherRegistryPath        string                  `mapstructure:"publisher_registry_path"`
	BlacklistPath                string                  `mapstructure:"blacklist_path"`

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

	// Per-address API throttle for TriggerAddressIndexing (see AppConfig for the
	// full contract). Zero values disable each part.
	AddressIndexingSuccessCooldown   time.Duration `mapstructure:"address_indexing_success_cooldown"`
	AddressIndexingFailureBackoff    time.Duration `mapstructure:"address_indexing_failure_backoff"`
	AddressIndexingFailureBackoffCap time.Duration `mapstructure:"address_indexing_failure_backoff_cap"`
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

// RenderProbeFingerprintConfig identifies one known-bad render (directory listing,
// gateway error page, placeholder) by its perceptual hash.
type RenderProbeFingerprintConfig struct {
	// Phash is the 64-bit DCT pHash as hex (with or without 0x prefix)
	Phash string `mapstructure:"phash"`
	// MaxDistance is the Hamming tolerance for a match (keep small: 4-8; a loose
	// tolerance hides real art)
	MaxDistance int `mapstructure:"max_distance"`
	// Label names the fingerprint for last_error and operator triage
	Label string `mapstructure:"label"`
}

// RenderProbeConfig holds L1 render probe configuration (media worker executes probes;
// the media health sweeper enqueues them).
type RenderProbeConfig struct {
	// Enabled gates both probe execution and sweeper enqueueing (default false)
	Enabled bool `mapstructure:"enabled"`
	// BatchSize is how many due URLs the sweeper enqueues per cycle
	BatchSize int `mapstructure:"batch_size"`
	// ViewportWidth/Height define the capture viewport (square by default so neither
	// orientation is biased); recorded with every capture
	ViewportWidth  int `mapstructure:"viewport_width"`
	ViewportHeight int `mapstructure:"viewport_height"`
	// TimeoutMs bounds the whole probe (navigate + settle + screenshot)
	TimeoutMs int `mapstructure:"timeout_ms"`
	// Enforce turns render verdicts into viewability gates; false is shadow mode
	// (verdicts, counters, and pHashes are recorded but nothing is ever hidden, and any
	// existing gates are released). The production rollout watches shadow data first
	// and flips this deliberately.
	Enforce bool `mapstructure:"enforce"`
	// SettleMs is how long the page runs after load before capture
	SettleMs int `mapstructure:"settle_ms"`
	// ImageSettleMs is the shortened settle for URLs classified as static raster images
	// (they paint on decode; the full window exists for generative works). <= 0 disables
	// the shortcut and every class gets SettleMs
	ImageSettleMs int `mapstructure:"image_settle_ms"`
	// BlankVarianceThreshold: frames with normalized luminance variance below this are blank
	BlankVarianceThreshold float64 `mapstructure:"blank_variance_threshold"`
	// FailureGateThreshold: consecutive blank/stalled probes before viewability gates
	// (fingerprint matches gate immediately)
	FailureGateThreshold int `mapstructure:"failure_gate_threshold"`
	// RecheckInterval schedules the next probe after rendered_ok
	RecheckInterval time.Duration `mapstructure:"recheck_interval"`
	// RetryInterval schedules the next probe after a not-yet-gating failure (debounce window)
	RetryInterval time.Duration `mapstructure:"retry_interval"`
	// BrokenRecheckInterval schedules the next probe after gating (also bounds heal
	// latency — the render probe is the only healer of render-gated rows)
	BrokenRecheckInterval time.Duration `mapstructure:"broken_recheck_interval"`
	// NoEvidenceRecheckInterval schedules the next probe after a no-evidence outcome on
	// an UNGATED row — a non-2xx served error page (e.g. a public gateway's persistent
	// HTTP 410 bot-block) or an SSRF policy refusal. Such an attempt says nothing about
	// the artwork and its cause does not change on a faster cadence, so it reprobes on a
	// slow interval. Sized long deliberately: public IPFS gateways that block headless
	// chromium are the dominant population, and rechecking them daily spent the bulk of
	// the render budget re-confirming the same block instead of covering new URLs.
	// Exceptions: a gated row keeps BrokenRecheckInterval (the probe is its only healer,
	// so that interval remains the heal-latency bound even when a recheck lands on a
	// served error page), and a transient DNS resolution failure uses RetryInterval in
	// both states.
	NoEvidenceRecheckInterval time.Duration `mapstructure:"no_evidence_recheck_interval"`
	// KnownBadFingerprints are pHashes of known-bad renders; matches gate immediately
	KnownBadFingerprints []RenderProbeFingerprintConfig `mapstructure:"known_bad_fingerprints"`
	// NoSandbox disables chromium's sandbox. Only for runtimes that cannot support it
	// (no unprivileged user namespaces, restrictive seccomp). The probe renders
	// untrusted remote pages, so an unsandboxed renderer exploit would gain the media
	// worker's process access — prefer fixing the runtime over setting this.
	NoSandbox bool `mapstructure:"no_sandbox"`
	// EgressRestricted attests that the media worker's network egress is restricted so
	// it cannot route to loopback, private, link-local, or cloud-metadata ranges.
	//
	// Required to enable the probe. In-browser request validation is hostname-based, and
	// the hostname is resolved again by chromium when it dials — the DNS-rebinding TOCTOU
	// the SSRF validator documents repo-wide. Only connect-time/peer-IP policy closes it,
	// which lives in the network, not this process. This flag does not implement that
	// control; it makes enabling the probe without it a deliberate choice rather than an
	// oversight.
	EgressRestricted bool `mapstructure:"egress_restricted"`
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
	Security     SecurityConfig `mapstructure:"security"`
	MediaEnabled bool           `mapstructure:"media_enabled"`
	// VideoProcessingEnabled when true sends video/* URLs to the configured media provider (e.g. Cloudflare Stream).
	// When false (default), video assets are skipped without upload or DB writes; image and SVG flows are unchanged.
	VideoProcessingEnabled bool              `mapstructure:"video_processing_enabled"`
	URI                    URIConfig         `mapstructure:"uri"`
	Cloudflare             CloudflareConfig  `mapstructure:"cloudflare"`
	Rasterizer             RasterizerConfig  `mapstructure:"rasterizer"`
	RenderProbe            RenderProbeConfig `mapstructure:"render_probe"`
	Transform              TransformConfig   `mapstructure:"transform"`
	MaxImageSize           int64             `mapstructure:"max_image_size"`
	MaxVideoSize           int64             `mapstructure:"max_video_size"`
}

// MediaHealthSweeperConfig holds configuration for the media health sweeper
type MediaHealthSweeperConfig struct {
	URI          URIConfig     `mapstructure:"uri"`
	HTTPTimeout  time.Duration `mapstructure:"http_timeout"`
	BatchSize    int           `mapstructure:"batch_size"`
	RecheckAfter time.Duration `mapstructure:"recheck_after"`
	Worker       WorkerConfig  `mapstructure:"worker"`
}

// EffectiveURI returns the sweeper's URI settings with unset fields inherited from the
// root uri section.
//
// Reason: the documented operator remediation for a newly identified 200 error page is
// "add a marker to uri.known_bad_page_markers" — without inheritance that setting reaches
// worker-core but silently never the scheduled sweeper, which does the bulk of health
// checking, so the bad page keeps being marked healthy. Trade-offs: inheritance is
// per-field and only for unset values, so a deployment that deliberately configures the
// nested media_health_sweeper.uri section keeps full override power. Constraints: empty
// slice and zero are the "unset" sentinels — there is no way to nest an explicit
// "no markers" override, which is acceptable because an empty marker list only ever
// means "nothing configured".
func (c *MediaHealthSweeperConfig) EffectiveURI(root URIConfig) URIConfig {
	effective := c.URI
	if len(effective.IPFSGateways) == 0 {
		effective.IPFSGateways = root.IPFSGateways
	}
	if len(effective.ArweaveGateways) == 0 {
		effective.ArweaveGateways = root.ArweaveGateways
	}
	if len(effective.OnchfsGateways) == 0 {
		effective.OnchfsGateways = root.OnchfsGateways
	}
	if effective.ProbeMaxBytes <= 0 {
		effective.ProbeMaxBytes = root.ProbeMaxBytes
	}
	if len(effective.KnownBadPageMarkers) == 0 {
		effective.KnownBadPageMarkers = root.KnownBadPageMarkers
	}
	return effective
}

// ModerationSweeperConfig holds configuration for the moderation verdict sweeper
type ModerationSweeperConfig struct {
	BatchSize              int           `mapstructure:"batch_size"`
	InitialRecheckInterval time.Duration `mapstructure:"initial_recheck_interval"`
	MaxRecheckInterval     time.Duration `mapstructure:"max_recheck_interval"`
	FailureBackoffInitial  time.Duration `mapstructure:"failure_backoff_initial"`
	MaxConsecutiveFailures int           `mapstructure:"max_consecutive_failures"`
	Worker                 WorkerConfig  `mapstructure:"worker"`
}

// SweeperConfig holds configuration for the sweeper program
type SweeperConfig struct {
	BaseConfig         `mapstructure:",squash"`
	Database           DatabaseConfig           `mapstructure:"database"`
	Jobs               JobsConfig               `mapstructure:"jobs"`
	MediaHealthSweeper MediaHealthSweeperConfig `mapstructure:"media_health_sweeper"`
	ModerationSweeper  ModerationSweeperConfig  `mapstructure:"moderation_sweeper"`
	// RenderProbe: the sweeper only reads Enabled and BatchSize (enqueue side); the
	// media worker owns execution settings. MediaEnabled guards enqueueing onto a queue
	// no worker serves in lightweight deployments.
	RenderProbe  RenderProbeConfig `mapstructure:"render_probe"`
	MediaEnabled bool              `mapstructure:"media_enabled"`
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
	BaseConfig             `mapstructure:",squash"`
	Server                 ServerConfig             `mapstructure:"server"`
	Database               DatabaseConfig           `mapstructure:"database"`
	Auth                   AuthConfig               `mapstructure:"auth"`
	Jobs                   JobsConfig               `mapstructure:"jobs"`
	MediaEnabled           bool                     `mapstructure:"media_enabled"`
	VideoProcessingEnabled bool                     `mapstructure:"video_processing_enabled"`
	Ethereum               EthereumConfig           `mapstructure:"ethereum"`
	Tezos                  TezosConfig              `mapstructure:"tezos"`
	Vendors                VendorsConfig            `mapstructure:"vendors"`
	URI                    URIConfig                `mapstructure:"uri"`
	RateLimiter            RateLimiterConfig        `mapstructure:"rate_limiter"`
	Cloudflare             CloudflareConfig         `mapstructure:"cloudflare"`
	Rasterizer             RasterizerConfig         `mapstructure:"rasterizer"`
	RenderProbe            RenderProbeConfig        `mapstructure:"render_probe"`
	Transform              TransformConfig          `mapstructure:"transform"`
	MediaHealthSweeper     MediaHealthSweeperConfig `mapstructure:"media_health_sweeper"`
	ModerationSweeper      ModerationSweeperConfig  `mapstructure:"moderation_sweeper"`

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

	// Per-address API throttle for TriggerAddressIndexing. Zero values disable
	// each part. A wallet scan is one of the most expensive operations the public
	// API can start (owner enumeration against a credit-metered RPC provider):
	// the cooldown bounds re-scan frequency after success; the backoff pair
	// halves the retry frequency of a repeatedly failing address (delay =
	// base × 2^(consecutive failures − 1), capped).
	AddressIndexingSuccessCooldown   time.Duration `mapstructure:"address_indexing_success_cooldown"`
	AddressIndexingFailureBackoff    time.Duration `mapstructure:"address_indexing_failure_backoff"`
	AddressIndexingFailureBackoffCap time.Duration `mapstructure:"address_indexing_failure_backoff_cap"`

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

	if err := validateRenderProbeConfig(&cfg.RenderProbe, cfg.MediaEnabled, cfg.Jobs.MediaQueue); err != nil {
		return err
	}

	// A negative call budget would silently disable the pagination cost backstop
	// (the guard only engages when CallBudget > 0), so a malformed value must be a
	// visible startup error, not an unguarded deployment.
	if cfg.Ethereum.GetLogsCallBudget < 0 {
		return fmt.Errorf("ethereum.getlogs_call_budget must be >= 0, got %d", cfg.Ethereum.GetLogsCallBudget)
	}
	// Zero concurrency would stall the owner scan forever (no window ever
	// fetched), and the unbounded case is what rate-limit discipline is for.
	if cfg.Ethereum.ScanWindowConcurrency < 1 {
		return fmt.Errorf("ethereum.scan_window_concurrency must be >= 1, got %d", cfg.Ethereum.ScanWindowConcurrency)
	}

	// Address-indexing throttle: negative durations would silently disable the
	// throttle (it engages only on positive values), and a cap below the base
	// would make the first backoff exceed its own ceiling.
	if cfg.AddressIndexingSuccessCooldown < 0 {
		return fmt.Errorf("address_indexing_success_cooldown must be >= 0, got %s", cfg.AddressIndexingSuccessCooldown)
	}
	if cfg.AddressIndexingFailureBackoff < 0 {
		return fmt.Errorf("address_indexing_failure_backoff must be >= 0, got %s", cfg.AddressIndexingFailureBackoff)
	}
	if cfg.AddressIndexingFailureBackoffCap < 0 {
		return fmt.Errorf("address_indexing_failure_backoff_cap must be >= 0, got %s", cfg.AddressIndexingFailureBackoffCap)
	}
	if cfg.AddressIndexingFailureBackoff > 0 && cfg.AddressIndexingFailureBackoffCap > 0 &&
		cfg.AddressIndexingFailureBackoffCap < cfg.AddressIndexingFailureBackoff {
		return fmt.Errorf("address_indexing_failure_backoff_cap (%s) must be >= address_indexing_failure_backoff (%s)",
			cfg.AddressIndexingFailureBackoffCap, cfg.AddressIndexingFailureBackoff)
	}

	return validateModerationSweeperConfig(&cfg.ModerationSweeper)
}

// validateRenderProbeConfig rejects render-probe settings whose failure mode is silent
// misbehavior rather than a visible startup error.
//
// Reason: a non-positive interval makes every probed URL immediately due again — a
// chromium render loop at sweeper cadence is a memory/CPU burn, not a crash. A malformed
// fingerprint pHash would never match anything (silently disabling the known-bad gate),
// and a Hamming tolerance above 64 or below 0 is meaningless for a 64-bit hash — worse,
// large tolerances match real art and hide it.
//
// Constraints: validated only when the probe would actually run (enabled AND media
// enabled). enabled defaults to true, so it is no longer an operator statement of
// intent — media_enabled is the explicit signal that this deployment renders. A
// lightweight deployment (media_enabled=false) therefore leaves a default-enabled probe
// inert rather than failing startup; the sweeper's enqueue gate (Enabled &&
// MediaEnabled) guarantees no jobs pile up on an unserved queue.
func validateRenderProbeConfig(c *RenderProbeConfig, mediaEnabled bool, mediaQueue string) error {
	if !c.Enabled || !mediaEnabled {
		return nil
	}

	invalid := make([]string, 0)
	if mediaQueue == "" {
		invalid = append(invalid, "render_probe.enabled requires jobs.media_queue to be set")
	}
	if !c.EgressRestricted {
		invalid = append(invalid, "render_probe.egress_restricted must be true to enable the probe: "+
			"the render probe navigates untrusted pages in a browser whose request validation is "+
			"hostname-based and therefore open to DNS rebinding at dial time. Restrict the media "+
			"worker's network egress from loopback/private/link-local/metadata ranges, then set this "+
			"flag (see docs/media_viewability.md)")
	}
	if c.BatchSize < 1 {
		invalid = append(invalid, "render_probe.batch_size must be at least 1")
	}
	// A "shortened" image settle above the full window is a sign the two knobs were
	// swapped or misunderstood; it would silently slow the image majority of the corpus.
	// (<= 0 is valid: it disables the shortcut.)
	if c.ImageSettleMs > 0 && c.SettleMs > 0 && c.ImageSettleMs > c.SettleMs {
		invalid = append(invalid, "render_probe.image_settle_ms must not exceed render_probe.settle_ms")
	}
	// The timeout bounds navigate + settle + screenshot together, so it must reserve
	// headroom beyond the settle window — checked on EFFECTIVE values because either
	// knob may be unset (<= 0 means the renderer's default): timeout_ms: 10000 with
	// settle_ms unset resolves to 10s against a 15s settle, and every probe would burn
	// its budget inside the settle sleep, record stalled, and gate healthy media after
	// the debounce. The startup self-check cannot catch this (fixtures settle short);
	// only this validation can.
	effTimeout := c.TimeoutMs
	if effTimeout <= 0 {
		effTimeout = probe.DefaultTimeoutMs
	}
	effSettle := c.SettleMs
	if effSettle <= 0 {
		effSettle = probe.DefaultSettleMs
	}
	if effTimeout < effSettle+probe.MinRenderHeadroomMs {
		invalid = append(invalid, fmt.Sprintf(
			"render_probe.timeout_ms (effective %dms) must exceed settle_ms (effective %dms) by at least %dms: "+
				"the timeout covers navigate + settle + screenshot, and anything less guarantees every probe "+
				"stalls in the settle sleep and gates healthy media after the debounce",
			effTimeout, effSettle, probe.MinRenderHeadroomMs))
	}
	// Viewport bounds are validated against the renderer's capture caps at startup
	// because the failure mode of an oversized viewport is silent and severe: chromium
	// allocates and captures the frame, the renderer rejects it post-hoc (encoded or
	// decoded size cap), every probe records stalled — and after the debounce the gate
	// hides perfectly healthy media. A config typo must be a startup error, not a
	// corpus-wide false gate. <= 0 stays valid: the renderer substitutes its defaults.
	for _, dim := range []struct {
		name  string
		value int
	}{{"render_probe.viewport_width", c.ViewportWidth}, {"render_probe.viewport_height", c.ViewportHeight}} {
		if dim.value > 0 && (dim.value < probe.MinViewportDim || dim.value > probe.MaxViewportDim) {
			invalid = append(invalid, fmt.Sprintf("%s must be within [%d, %d]",
				dim.name, probe.MinViewportDim, probe.MaxViewportDim))
		}
	}
	if c.ViewportWidth > 0 && c.ViewportHeight > 0 && c.ViewportWidth*c.ViewportHeight > probe.MaxViewportPixels {
		invalid = append(invalid, fmt.Sprintf(
			"render_probe.viewport_width x viewport_height must not exceed %d pixels: larger captures can "+
				"collide with the renderer screenshot caps, recording stalled on every probe and render-gating "+
				"healthy media", probe.MaxViewportPixels))
	}
	if c.FailureGateThreshold < 1 {
		invalid = append(invalid, "render_probe.failure_gate_threshold must be at least 1")
	}
	// A threshold at or above phash.MaxVariance calls every frame blank, so a mistyped
	// value like 1 would gate the whole corpus after the debounce threshold — exactly the
	// hide-real-art failure the probe exists to avoid. Reject it at startup instead.
	if c.BlankVarianceThreshold < 0 || c.BlankVarianceThreshold >= phash.MaxVariance {
		invalid = append(invalid, fmt.Sprintf(
			"render_probe.blank_variance_threshold must be within [0, %v): variance is a normalized "+
				"population variance, so a threshold at or above that classifies every frame as blank",
			phash.MaxVariance))
	}
	if c.RecheckInterval <= 0 {
		invalid = append(invalid, "render_probe.recheck_interval must be positive")
	}
	if c.RetryInterval <= 0 {
		invalid = append(invalid, "render_probe.retry_interval must be positive")
	}
	if c.BrokenRecheckInterval <= 0 {
		invalid = append(invalid, "render_probe.broken_recheck_interval must be positive")
	}
	if c.NoEvidenceRecheckInterval <= 0 {
		invalid = append(invalid, "render_probe.no_evidence_recheck_interval must be positive")
	}
	for _, fp := range c.KnownBadFingerprints {
		cleaned := strings.TrimPrefix(strings.TrimSpace(strings.ToLower(fp.Phash)), "0x")
		if cleaned == "" || len(cleaned) > 16 {
			invalid = append(invalid, fmt.Sprintf("render_probe.known_bad_fingerprints[%q].phash must be 1-16 hex digits", fp.Label))
		} else if _, err := strconv.ParseUint(cleaned, 16, 64); err != nil {
			invalid = append(invalid, fmt.Sprintf("render_probe.known_bad_fingerprints[%q].phash is not valid hex", fp.Label))
		}
		if fp.MaxDistance < 0 || fp.MaxDistance > 64 {
			invalid = append(invalid, fmt.Sprintf("render_probe.known_bad_fingerprints[%q].max_distance must be 0-64", fp.Label))
		}
	}

	if len(invalid) > 0 {
		return fmt.Errorf("invalid render probe config: %s", strings.Join(invalid, "; "))
	}
	return nil
}

// validateModerationSweeperConfig rejects moderation sweeper settings whose failure mode is a
// vendor-quota burn loop rather than a visible startup error.
//
// Reason: the sweeper's scheduling math assumes positive intervals — a zero or
// negative max_recheck_interval schedules every successful flagged check as
// immediately due again, and a non-positive failure_backoff_initial does the same
// after transient vendor errors. Those loops run at the vendor rate limit, so they
// silently spend the shared OpenSea/objkt budget instead of crashing. The same
// class of bug was fixed three separate times in the sweeper's own loop logic;
// this closes the configuration route into it. Constraints: only moderation_sweeper.*
// is validated here — the media health sweeper's settings predate this branch and
// keep their existing (unvalidated) behavior.
func validateModerationSweeperConfig(c *ModerationSweeperConfig) error {
	invalid := make([]string, 0)

	if c.BatchSize < 1 {
		invalid = append(invalid, "moderation_sweeper.batch_size must be at least 1")
	}
	if c.Worker.WorkerPoolSize < 1 {
		invalid = append(invalid, "moderation_sweeper.worker.pool_size must be at least 1")
	}
	if c.InitialRecheckInterval <= 0 {
		invalid = append(invalid, "moderation_sweeper.initial_recheck_interval must be positive")
	}
	if c.MaxRecheckInterval <= 0 {
		invalid = append(invalid, "moderation_sweeper.max_recheck_interval must be positive")
	}
	if c.FailureBackoffInitial <= 0 {
		invalid = append(invalid, "moderation_sweeper.failure_backoff_initial must be positive")
	}
	if c.MaxConsecutiveFailures < 1 {
		invalid = append(invalid, "moderation_sweeper.max_consecutive_failures must be at least 1")
	}
	// Relationship checks only make sense once both sides are individually valid.
	if c.InitialRecheckInterval > 0 && c.MaxRecheckInterval > 0 && c.InitialRecheckInterval > c.MaxRecheckInterval {
		invalid = append(invalid, "moderation_sweeper.initial_recheck_interval must not exceed moderation_sweeper.max_recheck_interval")
	}
	if c.FailureBackoffInitial > 0 && c.MaxRecheckInterval > 0 && c.FailureBackoffInitial > c.MaxRecheckInterval {
		invalid = append(invalid, "moderation_sweeper.failure_backoff_initial must not exceed moderation_sweeper.max_recheck_interval")
	}

	if len(invalid) > 0 {
		return fmt.Errorf("invalid config values: %s", strings.Join(invalid, "; "))
	}
	return nil
}

// ToAPIConfig maps AppConfig to the shape expected by the HTTP API server.
func (a *AppConfig) ToAPIConfig() *APIConfig {
	return &APIConfig{
		BaseConfig:                       a.BaseConfig,
		Server:                           a.Server,
		Database:                         a.Database,
		Jobs:                             a.Jobs,
		Auth:                             a.Auth,
		BlacklistPath:                    a.BlacklistPath,
		Tezos:                            a.Tezos,
		Ethereum:                         a.Ethereum,
		AddressIndexingSuccessCooldown:   a.AddressIndexingSuccessCooldown,
		AddressIndexingFailureBackoff:    a.AddressIndexingFailureBackoff,
		AddressIndexingFailureBackoffCap: a.AddressIndexingFailureBackoffCap,
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
		ModerationSweeper:                  a.ModerationSweeper,
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
		BaseConfig:             a.BaseConfig,
		Database:               a.Database,
		Jobs:                   a.Jobs,
		Security:               a.Security,
		MediaEnabled:           a.MediaEnabled,
		VideoProcessingEnabled: a.VideoProcessingEnabled,
		URI:                    a.URI,
		Cloudflare:             a.Cloudflare,
		Rasterizer:             a.Rasterizer,
		RenderProbe:            a.RenderProbe,
		Transform:              a.Transform,
		MaxImageSize:           a.MaxImageSize,
		MaxVideoSize:           a.MaxVideoSize,
	}
}

// ToSweeperConfig maps AppConfig for the sweepers (media health + spam verdict).
func (a *AppConfig) ToSweeperConfig() *SweeperConfig {
	return &SweeperConfig{
		BaseConfig:         a.BaseConfig,
		Database:           a.Database,
		Jobs:               a.Jobs,
		MediaHealthSweeper: a.MediaHealthSweeper,
		ModerationSweeper:  a.ModerationSweeper,
		RenderProbe:        a.RenderProbe,
		MediaEnabled:       a.MediaEnabled,
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
	v.SetDefault("jobs.token_worker.max_attempts", 3)
	v.SetDefault("jobs.media_worker.concurrency", 2)
	v.SetDefault("jobs.media_worker.poll_interval", 2*time.Second)
	v.SetDefault("jobs.media_worker.batch_size", 100)
	v.SetDefault("jobs.media_worker.cancel_interval", 5*time.Second)
	v.SetDefault("jobs.media_worker.max_attempts", 3)

	v.SetDefault("media_enabled", false)
	v.SetDefault("video_processing_enabled", false)

	// Chains
	v.SetDefault("ethereum.chain_id", "eip155:1")
	v.SetDefault("ethereum.block_head_ttl", 12)
	v.SetDefault("ethereum.block_head_stale_window", 60)
	v.SetDefault("ethereum.block_flush_timeout", 36*time.Second)
	// Credit guards default off (0/false = unguarded, pre-guard behavior);
	// production enables them via deploy config.
	v.SetDefault("ethereum.getlogs_span_cap", 0)
	v.SetDefault("ethereum.scan_window_concurrency", 2)
	v.SetDefault("ethereum.getlogs_call_budget", 0)
	v.SetDefault("ethereum.full_provenance_disabled", false)
	v.SetDefault("tezos.chain_id", "tezos:mainnet")
	v.SetDefault("tezos.api_url", "https://api.tzkt.io")
	v.SetDefault("tezos.block_head_ttl", 10)
	v.SetDefault("tezos.block_head_stale_window", 60)
	v.SetDefault("tezos.block_flush_timeout", 30*time.Second)
	// Vendors
	v.SetDefault("vendors.artblocks_url", "https://artblocks-mainnet.hasura.app/v1/graphql")
	v.SetDefault("vendors.feralfile_url", "https://feralfile.com/api")
	v.SetDefault("vendors.fxhash_url", "https://api.v2.fxhash.xyz/v1/graphql")
	v.SetDefault("vendors.objkt_url", "https://data.objkt.com/v3/graphql")
	v.SetDefault("vendors.opensea_url", "https://api.opensea.io/api/v2")

	// URI
	v.SetDefault("uri.onchfs_gateways", []string{"https://onchfs.fxhash2.xyz"})

	v.SetDefault("uri.ipfs_gateways", []string{"https://ipfs.io", "https://cloudflare-ipfs.com"})
	v.SetDefault("uri.arweave_gateways", []string{"https://arweave.net"})
	v.SetDefault("uri.probe_max_bytes", 32*1024)
	v.SetDefault("uri.known_bad_page_markers", []string{})
	v.SetDefault("rasterizer.width", 2048)
	v.SetDefault("rasterizer.timeout_ms", 15000)
	v.SetDefault("rasterizer.browser_fallback_enabled", false)
	v.SetDefault("render_probe.enabled", true)
	v.SetDefault("render_probe.enforce", false)
	v.SetDefault("render_probe.batch_size", 20)
	v.SetDefault("render_probe.viewport_width", 1024)
	v.SetDefault("render_probe.viewport_height", 1024)
	v.SetDefault("render_probe.timeout_ms", 90000)
	v.SetDefault("render_probe.settle_ms", 15000)
	v.SetDefault("render_probe.image_settle_ms", 2000)
	v.SetDefault("render_probe.blank_variance_threshold", 0.001)
	v.SetDefault("render_probe.failure_gate_threshold", 2)
	v.SetDefault("render_probe.recheck_interval", "168h")
	v.SetDefault("render_probe.retry_interval", "1h")
	v.SetDefault("render_probe.broken_recheck_interval", "24h")
	v.SetDefault("render_probe.no_evidence_recheck_interval", "168h")
	v.SetDefault("render_probe.no_sandbox", false)
	v.SetDefault("render_probe.egress_restricted", false)
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
	// Address-indexing throttle defaults off (0 = disabled); production enables
	// via deploy config. The cap default gives a sane bound when only the base
	// is configured.
	v.SetDefault("address_indexing_success_cooldown", 0)
	v.SetDefault("address_indexing_failure_backoff", 0)
	v.SetDefault("address_indexing_failure_backoff_cap", 24*time.Hour)

	// Rate limiter
	v.SetDefault("rate_limiter.max_workers", 10)
	v.SetDefault("rate_limiter.max_queue_size", 10000)
	v.SetDefault("rate_limiter.providers.tzkt.requests_per_second", 10)
	v.SetDefault("rate_limiter.providers.tzkt.burst", 10)
	v.SetDefault("rate_limiter.providers.tzkt.max_queue_time", "15m")
	v.SetDefault("rate_limiter.providers.opensea.requests_per_second", 4)
	v.SetDefault("rate_limiter.providers.opensea.burst", 4)
	v.SetDefault("rate_limiter.providers.opensea.max_queue_time", "15m")
	v.SetDefault("rate_limiter.providers.fxhash.requests_per_second", 2)
	v.SetDefault("rate_limiter.providers.fxhash.burst", 2)
	v.SetDefault("rate_limiter.providers.fxhash.max_queue_time", "15m")
	v.SetDefault("rate_limiter.providers.objkt.requests_per_second", 2)
	v.SetDefault("rate_limiter.providers.objkt.burst", 2)
	v.SetDefault("rate_limiter.providers.objkt.max_queue_time", "15m")

	// Media health sweeper
	v.SetDefault("media_health_sweeper.http_timeout", "30s")
	v.SetDefault("media_health_sweeper.batch_size", 100)
	v.SetDefault("media_health_sweeper.worker.pool_size", 5)
	v.SetDefault("media_health_sweeper.worker.queue_size", 100)
	v.SetDefault("media_health_sweeper.recheck_after", "24h")

	// Spam verdict sweeper. initial_recheck_interval is the single knob for a
	// fresh verdict's first re-check — the enricher and the sweeper both read it
	// at runtime; its default is anchored to store.DefaultModerationRecheckInterval
	// (enforced by a config test). Conservative batch size: each row costs one
	// vendor API call against OpenSea's ~4 rps shared budget.
	v.SetDefault("moderation_sweeper.batch_size", 100)
	v.SetDefault("moderation_sweeper.worker.pool_size", 2)
	v.SetDefault("moderation_sweeper.initial_recheck_interval", "24h")
	v.SetDefault("moderation_sweeper.max_recheck_interval", "720h")
	v.SetDefault("moderation_sweeper.failure_backoff_initial", "1h")
	v.SetDefault("moderation_sweeper.max_consecutive_failures", 5)

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
		"ethereum.block_flush_timeout",
		"ethereum.getlogs_span_cap",
		"ethereum.getlogs_call_budget",
		"ethereum.scan_window_concurrency",
		"ethereum.full_provenance_disabled",
		// Tezos
		"tezos.api_url",
		"tezos.websocket_url",
		"tezos.chain_id",
		"tezos.start_level",
		"tezos.block_head_ttl",
		"tezos.block_head_stale_window",
		"tezos.block_flush_timeout",
		// Job queue
		"jobs.token_queue",
		"jobs.media_queue",
		"jobs.token_worker.concurrency",
		"jobs.token_worker.poll_interval",
		"jobs.token_worker.batch_size",
		"jobs.token_worker.cancel_interval",
		"jobs.token_worker.max_attempts",
		"jobs.media_worker.concurrency",
		"jobs.media_worker.poll_interval",
		"jobs.media_worker.batch_size",
		"jobs.media_worker.cancel_interval",
		"jobs.media_worker.max_attempts",
		"media_enabled",
		"video_processing_enabled",
		// Vendors
		"vendors.artblocks_url",
		"vendors.feralfile_url",
		"vendors.fxhash_url",
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
		"uri.probe_max_bytes",
		"uri.known_bad_page_markers",
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
		"address_indexing_success_cooldown",
		"address_indexing_failure_backoff",
		"address_indexing_failure_backoff_cap",
		// Media specific
		"max_image_size",
		"max_video_size",
		"rasterizer.width",
		"rasterizer.timeout_ms",
		"rasterizer.browser_fallback_enabled",
		"render_probe.enabled",
		"render_probe.batch_size",
		"render_probe.viewport_width",
		"render_probe.viewport_height",
		"render_probe.timeout_ms",
		"render_probe.enforce",
		"render_probe.settle_ms",
		"render_probe.image_settle_ms",
		"render_probe.blank_variance_threshold",
		"render_probe.failure_gate_threshold",
		"render_probe.recheck_interval",
		"render_probe.retry_interval",
		"render_probe.broken_recheck_interval",
		"render_probe.no_evidence_recheck_interval",
		"render_probe.no_sandbox",
		"render_probe.egress_restricted",
		// Media Health Sweeper config
		"media_health_sweeper.http_timeout",
		"media_health_sweeper.batch_size",
		"media_health_sweeper.recheck_after",
		"media_health_sweeper.worker.pool_size",
		"media_health_sweeper.worker.queue_size",
		"media_health_sweeper.uri.ipfs_gateways",
		"media_health_sweeper.uri.arweave_gateways",
		"media_health_sweeper.uri.onchfs_gateways",
		"media_health_sweeper.uri.probe_max_bytes",
		"media_health_sweeper.uri.known_bad_page_markers",
		// Moderation Verdict Sweeper config
		"moderation_sweeper.batch_size",
		"moderation_sweeper.worker.pool_size",
		"moderation_sweeper.initial_recheck_interval",
		"moderation_sweeper.max_recheck_interval",
		"moderation_sweeper.failure_backoff_initial",
		"moderation_sweeper.max_consecutive_failures",
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
		"rate_limiter.providers.fxhash.requests_per_second",
		"rate_limiter.providers.fxhash.burst",
		"rate_limiter.providers.fxhash.max_queue_time",
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

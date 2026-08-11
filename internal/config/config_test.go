package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/media/phash"
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

func TestLoadAppConfig_FxhashRateLimiterFromEnv(t *testing.T) {
	tmpDir := t.TempDir()

	envDir := filepath.Join(tmpDir, "env")
	require.NoError(t, os.MkdirAll(envDir, 0750))

	// Override all three fxhash rate-limiter keys via environment variables.
	// These keys were missing from bindAllEnvVars; this test ensures env overrides work.
	envContent := "FF_INDEXER_RATE_LIMITER_PROVIDERS_FXHASH_REQUESTS_PER_SECOND=5\n" +
		"FF_INDEXER_RATE_LIMITER_PROVIDERS_FXHASH_BURST=10\n" +
		"FF_INDEXER_RATE_LIMITER_PROVIDERS_FXHASH_MAX_QUEUE_TIME=5m\n"
	require.NoError(t, os.WriteFile(filepath.Join(envDir, ".env"), []byte(envContent), 0600))

	configPath := filepath.Join(tmpDir, "config.yaml")
	configContent := `
database:
  host: localhost
  dbname: ff_indexer
ethereum:
  rpc_url: https://rpc.example.com
  websocket_url: wss://ws.example.com
tezos:
  websocket_url: wss://ws.tzkt.io
`
	require.NoError(t, os.WriteFile(configPath, []byte(configContent), 0600))

	cfg, err := LoadAppConfig(configPath, envDir)
	require.NoError(t, err)
	require.NotNil(t, cfg)

	fxhash := cfg.RateLimiter.Providers["fxhash"]
	assert.Equal(t, 5, fxhash.RequestsPerSecond, "fxhash RPS must be overridable via env")
	assert.Equal(t, 10, fxhash.Burst, "fxhash burst must be overridable via env")
	assert.Equal(t, 5*time.Minute, fxhash.MaxQueueTime, "fxhash max_queue_time must be overridable via env")
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

func TestLoadAppConfig_VideoProcessingEnabledDefault(t *testing.T) {
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
	assert.False(t, cfg.VideoProcessingEnabled, "video_processing_enabled should default to false")
	assert.False(t, cfg.ToWorkerMediaConfig().VideoProcessingEnabled, "WorkerMediaConfig should inherit false default")
}

func TestLoadAppConfig_VideoProcessingEnabledFromEnv(t *testing.T) {
	tmpDir := t.TempDir()
	envDir := filepath.Join(tmpDir, "env")
	require.NoError(t, os.MkdirAll(envDir, 0750))

	envContent := `FF_INDEXER_VIDEO_PROCESSING_ENABLED=true
FF_INDEXER_DATABASE_HOST=localhost
FF_INDEXER_DATABASE_USER=u
FF_INDEXER_DATABASE_PASSWORD=p
FF_INDEXER_DATABASE_DBNAME=db
FF_INDEXER_ETHEREUM_RPC_URL=https://rpc.example.com
FF_INDEXER_ETHEREUM_WEBSOCKET_URL=wss://ws.example.com
FF_INDEXER_TEZOS_API_URL=https://api.tzkt.io
FF_INDEXER_TEZOS_WEBSOCKET_URL=wss://ws.tzkt.io
FF_INDEXER_JOBS_TOKEN_QUEUE=token_index
`
	require.NoError(t, os.WriteFile(filepath.Join(envDir, ".env"), []byte(envContent), 0600))

	cfg, err := LoadAppConfig("", envDir)
	require.NoError(t, err)
	require.NotNil(t, cfg)

	assert.True(t, cfg.VideoProcessingEnabled, "env var should enable video processing")
	assert.True(t, cfg.ToWorkerMediaConfig().VideoProcessingEnabled, "WorkerMediaConfig should inherit enabled state")
}

func TestValidateSecurityConfig_allowlistDomainTooBroad(t *testing.T) {
	cfg := &AppConfig{}
	cfg.Security.SSRFProtection.Allowlist.Domains = []string{"com"}
	require.Error(t, validateSecurityConfig(cfg))
}

func TestValidateSecurityConfig_allowlistDomainRejectsIPLiteral(t *testing.T) {
	cfg := &AppConfig{}
	cfg.Security.SSRFProtection.Allowlist.Domains = []string{"127.0.0.1"}
	require.Error(t, validateSecurityConfig(cfg))
	cfg.Security.SSRFProtection.Allowlist.Domains = []string{"169.254.169.254"}
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

func TestToWorkerMediaConfig_includesSecurityForSSRF(t *testing.T) {
	cfg := &AppConfig{}
	cfg.Security.SSRFProtection.Enabled = true
	cfg.Security.SSRFProtection.MaxRedirects = 7
	cfg.Security.SSRFProtection.BlockMulticast = true
	cfg.Security.SSRFProtection.Allowlist.Domains = []string{"cdn.example.com"}

	w := cfg.ToWorkerMediaConfig()
	require.Equal(t, cfg.Security.SSRFProtection.Enabled, w.Security.SSRFProtection.Enabled)
	require.Equal(t, 7, w.Security.SSRFProtection.MaxRedirects)
	require.True(t, w.Security.SSRFProtection.BlockMulticast)
	require.Equal(t, []string{"cdn.example.com"}, w.Security.SSRFProtection.Allowlist.Domains)

	v, err := SSRFValidatorFromProtection(w.Security.SSRFProtection)
	require.NoError(t, err)
	require.NotNil(t, v)
}

func TestMediaHealthSweeperConfig_EffectiveURI(t *testing.T) {
	root := URIConfig{
		IPFSGateways:        []string{"https://ipfs.io"},
		ArweaveGateways:     []string{"https://arweave.net"},
		OnchfsGateways:      []string{"https://onchfs.fxhash2.xyz"},
		ProbeMaxBytes:       32768,
		KnownBadPageMarkers: []string{"504 gateway time-out"},
	}

	t.Run("unset nested fields inherit the root uri section", func(t *testing.T) {
		c := MediaHealthSweeperConfig{} // nothing nested configured
		got := c.EffectiveURI(root)
		assert.Equal(t, root, got, "the documented uri.known_bad_page_markers remediation must reach the sweeper")
	})

	t.Run("configured nested fields override the root, unset ones still inherit", func(t *testing.T) {
		c := MediaHealthSweeperConfig{URI: URIConfig{
			IPFSGateways:  []string{"https://sweeper-only-gateway.example"},
			ProbeMaxBytes: 1024,
		}}
		got := c.EffectiveURI(root)
		assert.Equal(t, []string{"https://sweeper-only-gateway.example"}, got.IPFSGateways)
		assert.Equal(t, 1024, got.ProbeMaxBytes)
		assert.Equal(t, root.ArweaveGateways, got.ArweaveGateways)
		assert.Equal(t, root.KnownBadPageMarkers, got.KnownBadPageMarkers)
	})
}

// TestValidateRenderProbeConfig_RequiresEgressRestriction pins the enablement gate: the
// probe's in-browser request validation is hostname-based and therefore open to DNS
// rebinding at dial time, which only network-level egress policy can close. Enabling the
// probe without attesting that control must fail at startup rather than silently ship an
// SSRF path.
func TestValidateRenderProbeConfig_RequiresEgressRestriction(t *testing.T) {
	base := RenderProbeConfig{
		Enabled:               true,
		BatchSize:             20,
		FailureGateThreshold:  2,
		RecheckInterval:       168 * time.Hour,
		RetryInterval:         time.Hour,
		BrokenRecheckInterval: 24 * time.Hour,
	}

	t.Run("enabled without egress restriction is rejected", func(t *testing.T) {
		cfg := base
		err := validateRenderProbeConfig(&cfg, true, "media_index")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "egress_restricted")
	})

	t.Run("enabled with egress restriction is accepted", func(t *testing.T) {
		cfg := base
		cfg.EgressRestricted = true
		assert.NoError(t, validateRenderProbeConfig(&cfg, true, "media_index"))
	})

	// An enabled probe with no media worker renders nothing and gates nothing; failing
	// at startup beats a deployment that looks enabled but is a no-op.
	t.Run("enabled without the media worker is rejected", func(t *testing.T) {
		cfg := base
		cfg.EgressRestricted = true
		err := validateRenderProbeConfig(&cfg, false, "media_index")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "media_enabled")
	})

	t.Run("enabled without a media queue is rejected", func(t *testing.T) {
		cfg := base
		cfg.EgressRestricted = true
		err := validateRenderProbeConfig(&cfg, true, "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "media_queue")
	})

	// A threshold at or above the metric's maximum calls every frame blank, gating the
	// whole corpus after debounce — the hide-real-art failure mode this feature exists
	// to prevent.
	t.Run("blank variance threshold must sit inside the variance domain", func(t *testing.T) {
		for _, tc := range []struct {
			name      string
			threshold float64
			valid     bool
		}{
			{"negative", -0.1, false},
			{"zero", 0, true},
			{"typical", 0.001, true},
			{"just below max", phash.MaxVariance - 0.0001, true},
			{"at max", phash.MaxVariance, false},
			{"mistyped as one", 1, false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				cfg := base
				cfg.EgressRestricted = true
				cfg.BlankVarianceThreshold = tc.threshold
				err := validateRenderProbeConfig(&cfg, true, "media_index")
				if tc.valid {
					assert.NoError(t, err)
					return
				}
				require.Error(t, err)
				assert.Contains(t, err.Error(), "blank_variance_threshold")
			})
		}
	})

	t.Run("disabled probe is inert", func(t *testing.T) {
		cfg := RenderProbeConfig{Enabled: false}
		assert.NoError(t, validateRenderProbeConfig(&cfg, false, ""), "a disabled probe's settings are not validated")
	})
}

package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/media/phash"
	"github.com/feral-file/ff-indexer-v2/internal/store"
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
	// The render probe ships observing, not blocking: a default deployment must never
	// hide a token until an operator deliberately flips enforce.
	assert.True(t, cfg.RenderProbe.Enabled, "probe observes by default")
	assert.False(t, cfg.RenderProbe.Enforce, "shadow mode by default — enforcement is an explicit decision")
}

// TestModerationSweeperDefaultMatchesStoreConstant anchors the config default to
// store.DefaultModerationRecheckInterval. Both writers read the configured
// moderation_sweeper.initial_recheck_interval at runtime, but the constant still
// serves as NewCoreExecutor's fallback when no value is threaded in — if the
// viper default drifted from it, a default-config deployment and a
// fallback-path caller would schedule first re-checks differently. Nothing but
// this test couples the two literals.
func TestModerationSweeperDefaultMatchesStoreConstant(t *testing.T) {
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
	assert.Equal(t, store.DefaultModerationRecheckInterval, cfg.ModerationSweeper.InitialRecheckInterval,
		"moderation_sweeper.initial_recheck_interval default must match store.DefaultModerationRecheckInterval")
}

func TestLoadAppConfig_requiresDatabase(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	require.NoError(t, os.WriteFile(configPath, []byte(`database: {}`), 0600))

	_, err := LoadAppConfig(configPath, "")
	require.Error(t, err)
}

// validModerationSweeperConfig returns moderation sweeper settings that pass validation, for
// tests that hand-build an AppConfig instead of going through LoadAppConfig (and
// therefore never receive the viper defaults).
func validModerationSweeperConfig() ModerationSweeperConfig {
	return ModerationSweeperConfig{
		BatchSize:              100,
		InitialRecheckInterval: 24 * time.Hour,
		MaxRecheckInterval:     720 * time.Hour,
		FailureBackoffInitial:  time.Hour,
		MaxConsecutiveFailures: 5,
		Worker:                 WorkerConfig{WorkerPoolSize: 2},
	}
}

func TestValidateRequiredConfigValues(t *testing.T) {
	cfg := &AppConfig{
		ModerationSweeper: validModerationSweeperConfig(),
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
		ModerationSweeper: validModerationSweeperConfig(),
		MediaEnabled:      false,
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
		ModerationSweeper: validModerationSweeperConfig(),
		MediaEnabled:      true,
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
		ModerationSweeper: validModerationSweeperConfig(),
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

	// enabled defaults to true, so it is not an operator statement of intent —
	// media_enabled is. A lightweight deployment must start with the default-enabled
	// probe inert, not fail; even a bad egress/threshold config is unreachable there.
	t.Run("enabled without the media worker is inert, not rejected", func(t *testing.T) {
		cfg := base
		assert.NoError(t, validateRenderProbeConfig(&cfg, false, "media_index"),
			"a probe that cannot run must not block startup, even with egress unattested")
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

	// A viewport that passes config but collides with the renderer's capture caps
	// records stalled on every probe and gates healthy media after the debounce — a
	// typo must be a startup error, not a corpus-wide false gate.
	t.Run("viewport bounds are validated against the capture caps", func(t *testing.T) {
		cases := []struct {
			name    string
			w, h    int
			valid   bool
			wantErr string
		}{
			{"default square", 1024, 1024, true, ""},
			{"zero means renderer default", 0, 0, true, ""},
			{"typo: 5000x5000 exceeds the pixel budget", 5000, 5000, false, "must not exceed"},
			{"oversized single edge", 8192, 100, false, "must be within"},
			{"sub-minimum edge", 32, 1024, false, "must be within"},
			{"widescreen inside the budget", 2048, 1024, true, ""},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				cfg := base
				cfg.EgressRestricted = true
				cfg.ViewportWidth, cfg.ViewportHeight = tc.w, tc.h
				err := validateRenderProbeConfig(&cfg, true, "media_index")
				if tc.valid {
					assert.NoError(t, err)
					return
				}
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
			})
		}
	})

	// A timeout that cannot contain the settle window plus fixed render costs makes
	// every probe stall and gate healthy media at corpus scale; the self-check cannot
	// catch it (fixtures settle short), so this must be a startup error. Validated on
	// effective values: either knob unset resolves to the renderer default.
	t.Run("timeout must reserve headroom beyond the effective settle", func(t *testing.T) {
		cases := []struct {
			name                string
			timeoutMs, settleMs int
			valid               bool
		}{
			{"defaults are consistent", 0, 0, true},
			{"explicit production pairing", 45000, 15000, true},
			{"timeout below the DEFAULT settle (the reported shape)", 10000, 0, false},
			{"timeout below an explicit settle", 10000, 15000, false},
			{"headroom squeezed under the floor", 18000, 15000, false},
			{"exactly at the floor", 20000, 15000, true},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				cfg := base
				cfg.EgressRestricted = true
				cfg.TimeoutMs, cfg.SettleMs = tc.timeoutMs, tc.settleMs
				err := validateRenderProbeConfig(&cfg, true, "media_index")
				if tc.valid {
					assert.NoError(t, err)
					return
				}
				require.Error(t, err)
				assert.Contains(t, err.Error(), "timeout_ms")
			})
		}
	})

	t.Run("disabled probe is inert", func(t *testing.T) {
		cfg := RenderProbeConfig{Enabled: false}
		assert.NoError(t, validateRenderProbeConfig(&cfg, false, ""), "a disabled probe's settings are not validated")
	})
}

// TestValidateModerationSweeperConfig rejects settings whose failure mode is a silent
// vendor-quota burn loop rather than a visible error: a non-positive
// max_recheck_interval makes every successful flagged check immediately due
// again, and a non-positive failure_backoff_initial does the same after
// transient vendor errors. The sweeper's loop guards were fixed three times for
// this exact shape; this pins the configuration route shut.
func TestValidateModerationSweeperConfig(t *testing.T) {
	valid := validModerationSweeperConfig()
	require.NoError(t, validateModerationSweeperConfig(&valid))

	cases := []struct {
		name    string
		mutate  func(*ModerationSweeperConfig)
		wantErr string
	}{
		{
			name:    "zero batch size",
			mutate:  func(c *ModerationSweeperConfig) { c.BatchSize = 0 },
			wantErr: "moderation_sweeper.batch_size",
		},
		{
			name:    "zero pool size",
			mutate:  func(c *ModerationSweeperConfig) { c.Worker.WorkerPoolSize = 0 },
			wantErr: "moderation_sweeper.worker.pool_size",
		},
		{
			name:    "zero initial recheck interval",
			mutate:  func(c *ModerationSweeperConfig) { c.InitialRecheckInterval = 0 },
			wantErr: "moderation_sweeper.initial_recheck_interval must be positive",
		},
		{
			name:    "negative initial recheck interval",
			mutate:  func(c *ModerationSweeperConfig) { c.InitialRecheckInterval = -time.Hour },
			wantErr: "moderation_sweeper.initial_recheck_interval must be positive",
		},
		{
			name:    "zero max recheck interval",
			mutate:  func(c *ModerationSweeperConfig) { c.MaxRecheckInterval = 0 },
			wantErr: "moderation_sweeper.max_recheck_interval must be positive",
		},
		{
			name:    "zero failure backoff",
			mutate:  func(c *ModerationSweeperConfig) { c.FailureBackoffInitial = 0 },
			wantErr: "moderation_sweeper.failure_backoff_initial must be positive",
		},
		{
			name:    "zero max consecutive failures",
			mutate:  func(c *ModerationSweeperConfig) { c.MaxConsecutiveFailures = 0 },
			wantErr: "moderation_sweeper.max_consecutive_failures",
		},
		{
			name:    "initial exceeds max",
			mutate:  func(c *ModerationSweeperConfig) { c.InitialRecheckInterval = c.MaxRecheckInterval + time.Hour },
			wantErr: "moderation_sweeper.initial_recheck_interval must not exceed",
		},
		{
			name:    "failure backoff exceeds max",
			mutate:  func(c *ModerationSweeperConfig) { c.FailureBackoffInitial = c.MaxRecheckInterval + time.Hour },
			wantErr: "moderation_sweeper.failure_backoff_initial must not exceed",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := validModerationSweeperConfig()
			tc.mutate(&c)
			err := validateModerationSweeperConfig(&c)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

// TestLoadAppConfig_rejectsInvalidModerationSweeper pins that the validation actually
// runs on the load path operators hit, not just as a standalone function.
func TestLoadAppConfig_rejectsInvalidModerationSweeper(t *testing.T) {
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
moderation_sweeper:
  max_recheck_interval: 0s
`
	require.NoError(t, os.WriteFile(configPath, []byte(yaml), 0600))

	_, err := LoadAppConfig(configPath, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "moderation_sweeper.max_recheck_interval")
}

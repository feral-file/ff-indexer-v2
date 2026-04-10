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
nats:
  url: nats://127.0.0.1:4222
`
	require.NoError(t, os.WriteFile(configPath, []byte(yaml), 0600))

	cfg, err := LoadAppConfig(configPath, "")
	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Equal(t, "localhost", cfg.Database.Host)
	assert.Equal(t, "db", cfg.Database.DBName)
	assert.Equal(t, "nats://127.0.0.1:4222", cfg.NATS.URL)
}

func TestLoadAppConfig_requiresDatabase(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	require.NoError(t, os.WriteFile(configPath, []byte(`database: {}`), 0600))

	_, err := LoadAppConfig(configPath, "")
	require.Error(t, err)
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
}

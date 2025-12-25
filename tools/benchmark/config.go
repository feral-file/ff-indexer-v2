package main

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// BenchmarkConfig represents the configuration file structure
type BenchmarkConfig struct {
	TemporalHost string `json:"temporal_host"`
	Namespace    string `json:"namespace"`
}

// LoadConfig loads configuration from a file
func LoadConfig(path string) (*BenchmarkConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg BenchmarkConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// SaveConfig saves configuration to a file
func SaveConfig(path string, cfg *BenchmarkConfig) error {
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	return os.WriteFile(path, data, 0644)
}

// GetDefaultConfigPath returns the default config path
func GetDefaultConfigPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ".temporal-benchmark.json"
	}
	return filepath.Join(home, ".temporal-benchmark.json")
}

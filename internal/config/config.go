package config

import (
	"fmt"
	"os"
)

// Load reads and parses config.json at path, applying defaults for absent
// fields and validating the result. The returned error wraps os.ErrNotExist
// when the file doesn't exist, so callers can use errors.Is.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}

	cfg, err := Unmarshal(data)
	if err != nil {
		return nil, fmt.Errorf("config: parse %s: %w", path, err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config: invalid %s: %w", path, err)
	}
	return cfg, nil
}

// Save validates cfg and atomically writes it to path as JSON (mode 0600).
func Save(path string, cfg *Config) error {
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("config: invalid config: %w", err)
	}

	data, err := Marshal(cfg)
	if err != nil {
		return fmt.Errorf("config: save %s: %w", path, err)
	}
	if err := writeFileAtomic(path, data, 0600); err != nil {
		return fmt.Errorf("config: save %s: %w", path, err)
	}
	return nil
}

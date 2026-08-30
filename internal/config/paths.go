package config

import (
	"fmt"
	"os"
	"path/filepath"
)

// Paths resolves the on-disk layout described in SPEC.md §3 (config.json in
// place of the spec's config.toml — see the deviation note in schema.go):
//
//	~/.config/syncat/config.json
//	~/.config/syncat/api.token
//	~/.local/share/syncat/keys/tailcat.key
//	~/.local/share/syncat/keys/identity.key
//	~/.local/share/syncat/db/index.db
//	~/.local/share/syncat/trash/<share-id>/<relpath>.<unix-ts>
//
// The config dir defaults to $XDG_CONFIG_HOME/syncat, falling back to
// ~/.config/syncat. The data dir defaults to $XDG_DATA_HOME/syncat, falling
// back to ~/.local/share/syncat. Both can be overridden (e.g. via the
// --config/--data CLI flags) so multiple daemons can run on one machine.
type Paths struct {
	ConfigDir string
	DataDir   string
}

// ResolvePaths computes the config and data directories. configOverride and
// dataOverride, if non-empty, are used verbatim in place of the XDG-derived
// defaults.
func ResolvePaths(configOverride, dataOverride string) (*Paths, error) {
	configDir := configOverride
	if configDir == "" {
		dir, err := defaultConfigDir()
		if err != nil {
			return nil, err
		}
		configDir = dir
	}

	dataDir := dataOverride
	if dataDir == "" {
		dir, err := defaultDataDir()
		if err != nil {
			return nil, err
		}
		dataDir = dir
	}

	return &Paths{ConfigDir: configDir, DataDir: dataDir}, nil
}

func defaultConfigDir() (string, error) {
	if xdg := os.Getenv("XDG_CONFIG_HOME"); xdg != "" {
		return filepath.Join(xdg, "syncat"), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("config: resolve home directory: %w", err)
	}
	return filepath.Join(home, ".config", "syncat"), nil
}

func defaultDataDir() (string, error) {
	if xdg := os.Getenv("XDG_DATA_HOME"); xdg != "" {
		return filepath.Join(xdg, "syncat"), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("config: resolve home directory: %w", err)
	}
	return filepath.Join(home, ".local", "share", "syncat"), nil
}

// ConfigFile returns the path to config.json (see the deviation note in
// schema.go for why this is .json rather than the .toml SPEC.md describes).
func (p *Paths) ConfigFile() string { return filepath.Join(p.ConfigDir, "config.json") }

// APITokenFile returns the path to api.token.
func (p *Paths) APITokenFile() string { return filepath.Join(p.ConfigDir, "api.token") }

// KeysDir returns the directory holding the node's persistent keys.
func (p *Paths) KeysDir() string { return filepath.Join(p.DataDir, "keys") }

// IdentityKeyFile returns the path to the Ed25519 application identity key.
func (p *Paths) IdentityKeyFile() string { return filepath.Join(p.KeysDir(), "identity.key") }

// TailcatKeyFile returns the path to the tailcat saved key.
func (p *Paths) TailcatKeyFile() string { return filepath.Join(p.KeysDir(), "tailcat.key") }

// DBDir returns the directory holding the SQLite index.
func (p *Paths) DBDir() string { return filepath.Join(p.DataDir, "db") }

// DBFile returns the path to the SQLite index database.
func (p *Paths) DBFile() string { return filepath.Join(p.DBDir(), "index.db") }

// TrashDir returns the root of the trash can (subdirectories per share).
func (p *Paths) TrashDir() string { return filepath.Join(p.DataDir, "trash") }

// EnsureDirs creates the config dir, data dir, and all data subdirectories,
// each with mode 0700, if they don't already exist.
func (p *Paths) EnsureDirs() error {
	dirs := []string{p.ConfigDir, p.DataDir, p.KeysDir(), p.DBDir(), p.TrashDir()}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0700); err != nil {
			return fmt.Errorf("config: create directory %s: %w", dir, err)
		}
	}
	return nil
}

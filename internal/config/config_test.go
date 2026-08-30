package config

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestSaveLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")

	cfg := Default()
	cfg.NodeName = "alice"
	cfg.Shares = []Share{{ID: "a", Name: "docs", Path: "/home/alice/docs", Permission: PermissionReadOnly, Access: map[string]string{}}}

	if err := Save(path, cfg); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	assertConfigEqual(t, got, cfg)
}

func TestSaveIsAtomicAndModeIsPrivate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	cfg := Default()
	cfg.NodeName = "alice"

	if err := Save(path, cfg); err != nil {
		t.Fatalf("Save: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0600 {
		t.Errorf("config.json mode = %o, want 0600", perm)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, e := range entries {
		if e.Name() != "config.json" {
			t.Errorf("leftover temp file in config dir: %s", e.Name())
		}
	}
}

func TestLoadMissingFileWrapsErrNotExist(t *testing.T) {
	_, err := Load(filepath.Join(t.TempDir(), "nope.json"))
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("error %v does not wrap os.ErrNotExist", err)
	}
}

func TestSaveRejectsInvalidConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	cfg := Default()
	cfg.Shares = []Share{{ID: "a", Name: "s", Path: "not/absolute", Permission: PermissionReadOnly}}

	if err := Save(path, cfg); err == nil {
		t.Fatal("expected Save to reject invalid config, got nil")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("Save should not have written a file for an invalid config")
	}
}

func TestLoadRejectsInvalidConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	// Valid JSON, but a share with a relative path.
	doc := `{"node_name":"alice","api_addr":"127.0.0.1:8347","shares":[{"id":"a","name":"s","path":"rel","permission":"read-only"}]}`
	if err := os.WriteFile(path, []byte(doc), 0600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	if _, err := Load(path); err == nil {
		t.Fatal("expected Load to reject invalid config, got nil")
	}
}

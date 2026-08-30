package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolvePathsHonorsXDGVars(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", "/xdg-config")
	t.Setenv("XDG_DATA_HOME", "/xdg-data")

	p, err := ResolvePaths("", "")
	if err != nil {
		t.Fatalf("ResolvePaths: %v", err)
	}
	if want := filepath.Join("/xdg-config", "syncat"); p.ConfigDir != want {
		t.Errorf("ConfigDir = %q, want %q", p.ConfigDir, want)
	}
	if want := filepath.Join("/xdg-data", "syncat"); p.DataDir != want {
		t.Errorf("DataDir = %q, want %q", p.DataDir, want)
	}
}

func TestResolvePathsFallsBackToHomeDir(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", "")
	t.Setenv("XDG_DATA_HOME", "")
	t.Setenv("HOME", "/home/test-user")

	p, err := ResolvePaths("", "")
	if err != nil {
		t.Fatalf("ResolvePaths: %v", err)
	}
	if want := filepath.Join("/home/test-user", ".config", "syncat"); p.ConfigDir != want {
		t.Errorf("ConfigDir = %q, want %q", p.ConfigDir, want)
	}
	if want := filepath.Join("/home/test-user", ".local", "share", "syncat"); p.DataDir != want {
		t.Errorf("DataDir = %q, want %q", p.DataDir, want)
	}
}

func TestResolvePathsHonorsOverrides(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", "/xdg-config")
	t.Setenv("XDG_DATA_HOME", "/xdg-data")

	p, err := ResolvePaths("/override/cfg", "/override/data")
	if err != nil {
		t.Fatalf("ResolvePaths: %v", err)
	}
	if p.ConfigDir != "/override/cfg" {
		t.Errorf("ConfigDir = %q, want /override/cfg", p.ConfigDir)
	}
	if p.DataDir != "/override/data" {
		t.Errorf("DataDir = %q, want /override/data", p.DataDir)
	}
}

func TestPathsDerivedFiles(t *testing.T) {
	p := &Paths{ConfigDir: "/cfg", DataDir: "/data"}

	cases := []struct {
		name string
		got  string
		want string
	}{
		{"ConfigFile", p.ConfigFile(), filepath.Join("/cfg", "config.json")},
		{"APITokenFile", p.APITokenFile(), filepath.Join("/cfg", "api.token")},
		{"KeysDir", p.KeysDir(), filepath.Join("/data", "keys")},
		{"IdentityKeyFile", p.IdentityKeyFile(), filepath.Join("/data", "keys", "identity.key")},
		{"TailcatKeyFile", p.TailcatKeyFile(), filepath.Join("/data", "keys", "tailcat.key")},
		{"DBDir", p.DBDir(), filepath.Join("/data", "db")},
		{"DBFile", p.DBFile(), filepath.Join("/data", "db", "index.db")},
		{"TrashDir", p.TrashDir(), filepath.Join("/data", "trash")},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s = %q, want %q", c.name, c.got, c.want)
		}
	}
}

func TestEnsureDirsCreatesPrivateDirectories(t *testing.T) {
	base := t.TempDir()
	p := &Paths{
		ConfigDir: filepath.Join(base, "cfg"),
		DataDir:   filepath.Join(base, "data"),
	}
	if err := p.EnsureDirs(); err != nil {
		t.Fatalf("EnsureDirs: %v", err)
	}

	for _, dir := range []string{p.ConfigDir, p.DataDir, p.KeysDir(), p.DBDir(), p.TrashDir()} {
		info, err := os.Stat(dir)
		if err != nil {
			t.Fatalf("stat %s: %v", dir, err)
		}
		if !info.IsDir() {
			t.Errorf("%s is not a directory", dir)
		}
		if perm := info.Mode().Perm(); perm != 0700 {
			t.Errorf("%s mode = %o, want 0700", dir, perm)
		}
	}

	// Idempotent: running again must not error.
	if err := p.EnsureDirs(); err != nil {
		t.Fatalf("EnsureDirs (second call): %v", err)
	}
}

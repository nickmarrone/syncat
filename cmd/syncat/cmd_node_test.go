package main

import (
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/nickmarrone/syncat/internal/config"
)

// tempPaths returns a Paths rooted in t.TempDir. Constructing the struct
// directly is how internal/config's own tests do it, and it keeps the
// XDG env vars out of the picture entirely.
func tempPaths(t *testing.T) *config.Paths {
	t.Helper()
	base := t.TempDir()
	return &config.Paths{
		ConfigDir: filepath.Join(base, "cfg"),
		DataDir:   filepath.Join(base, "data"),
	}
}

// populate creates the full on-disk layout plus the WAL sidecars and a
// trashed file, so a reset has something real to remove.
func populate(t *testing.T, p *config.Paths) {
	t.Helper()
	if err := p.EnsureDirs(); err != nil {
		t.Fatalf("EnsureDirs: %v", err)
	}
	files := []string{
		p.ConfigFile(),
		p.APITokenFile(),
		p.IdentityKeyFile(),
		p.TailcatKeyFile(),
		p.DBFile(),
		p.DBFile() + "-wal",
		p.DBFile() + "-shm",
		filepath.Join(p.TrashDir(), "shareid", "notes.txt.1757260000"),
	}
	for _, f := range files {
		if err := os.MkdirAll(filepath.Dir(f), 0700); err != nil {
			t.Fatalf("mkdir for %s: %v", f, err)
		}
		if err := os.WriteFile(f, []byte("x"), 0600); err != nil {
			t.Fatalf("write %s: %v", f, err)
		}
	}
}

func TestRemoveStateClearsEveryTargetAndNothingElse(t *testing.T) {
	p := tempPaths(t)
	populate(t, p)

	// A crashed writeFileAtomic leftover, which resetTargets globs for.
	tmpLeftover := filepath.Join(p.ConfigDir, ".tmp-config.json-12345")
	if err := os.WriteFile(tmpLeftover, []byte("partial"), 0600); err != nil {
		t.Fatal(err)
	}
	// Not ours: --config can point at a directory holding other things,
	// which is why reset deletes a known list instead of the whole dir.
	decoy := filepath.Join(p.ConfigDir, "notes-from-the-user.txt")
	if err := os.WriteFile(decoy, []byte("keep me"), 0600); err != nil {
		t.Fatal(err)
	}

	targets := resetTargets(p)
	if !slices.Contains(targets, tmpLeftover) {
		t.Errorf("resetTargets did not glob the .tmp-* leftover: %v", targets)
	}
	if slices.Contains(targets, decoy) {
		t.Errorf("resetTargets included an unrelated file: %v", targets)
	}

	if err := removeState(targets); err != nil {
		t.Fatalf("removeState: %v", err)
	}

	for _, gone := range []string{
		p.ConfigFile(), p.APITokenFile(), p.KeysDir(), p.DBDir(), p.TrashDir(), tmpLeftover,
	} {
		if _, err := os.Lstat(gone); !os.IsNotExist(err) {
			t.Errorf("%s survived the reset (err=%v)", gone, err)
		}
	}
	if _, err := os.Stat(decoy); err != nil {
		t.Errorf("unrelated file was deleted: %v", err)
	}

	// init's own first step puts the directory skeleton back.
	if err := p.EnsureDirs(); err != nil {
		t.Fatalf("EnsureDirs after reset: %v", err)
	}
	for _, dir := range []string{p.KeysDir(), p.DBDir(), p.TrashDir()} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("read %s: %v", dir, err)
		}
		if len(entries) != 0 {
			t.Errorf("%s came back non-empty: %v", dir, entries)
		}
	}
}

func TestRemoveStateOnCleanLayoutIsNoError(t *testing.T) {
	p := tempPaths(t)
	if err := removeState(resetTargets(p)); err != nil {
		t.Fatalf("removeState on absent paths: %v", err)
	}
}

func TestKeptPathsListsSharesAndSubscriptions(t *testing.T) {
	cfg := &config.Config{
		Shares:        []config.Share{{Path: "/home/u/Documents"}, {Path: "/home/u/Photos"}},
		Subscriptions: []config.Subscription{{LocalPath: "/home/u/FromBob"}},
	}
	got := keptPaths(cfg)
	want := []string{"/home/u/Documents", "/home/u/FromBob", "/home/u/Photos"}
	if !slices.Equal(got, want) {
		t.Errorf("keptPaths = %v, want %v", got, want)
	}
	if got := keptPaths(nil); got != nil {
		t.Errorf("keptPaths(nil) = %v, want nil", got)
	}
}

func TestConfirmResetRequiresTheExactWord(t *testing.T) {
	tests := []struct {
		input string
		ok    bool
	}{
		{"RESET\n", true},
		{"  RESET  \n", true},
		{"RESET", true}, // no trailing newline still answers
		{"reset\n", false},
		{"Reset\n", false},
		{"yes\n", false},
		{"y\n", false},
		{"\n", false},
		{"", false}, // immediate EOF
	}
	for _, tt := range tests {
		err := confirmReset(strings.NewReader(tt.input), &strings.Builder{}, []string{"/some/file"}, nil)
		if tt.ok && err != nil {
			t.Errorf("confirmReset(%q) = %v, want nil", tt.input, err)
		}
		if !tt.ok && err == nil {
			t.Errorf("confirmReset(%q) = nil, want an error", tt.input)
		}
	}
}

func TestConfirmResetPromptNamesWhatGoesAndWhatStays(t *testing.T) {
	var out strings.Builder
	targets := []string{"/data/keys", "/data/db"}
	kept := []string{"/home/u/Documents"}
	if err := confirmReset(strings.NewReader("RESET\n"), &out, targets, kept); err != nil {
		t.Fatalf("confirmReset: %v", err)
	}
	text := out.String()
	for _, want := range append(append([]string{}, targets...), kept...) {
		if !strings.Contains(text, want) {
			t.Errorf("prompt never mentioned %s:\n%s", want, text)
		}
	}
	if !strings.Contains(text, resetConfirmWord) {
		t.Errorf("prompt never says what to type:\n%s", text)
	}
}

func TestDaemonRunningFollowsTheConfiguredAPIAddr(t *testing.T) {
	p := tempPaths(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	cfg := config.Default()
	cfg.NodeName = "test"
	cfg.APIAddr = addr
	if err := config.Save(p.ConfigFile(), cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}

	if got, running := daemonRunning(p); !running || got != addr {
		ln.Close()
		t.Fatalf("daemonRunning = (%q, %v), want (%q, true)", got, running, addr)
	}

	ln.Close()
	if got, running := daemonRunning(p); running {
		t.Errorf("daemonRunning = (%q, true) after the listener closed, want false", got)
	}
}

func TestDaemonRunningFallsBackWhenConfigIsUnreadable(t *testing.T) {
	p := tempPaths(t)
	if err := os.MkdirAll(p.ConfigDir, 0700); err != nil {
		t.Fatal(err)
	}
	// The corrupt-config case is one of the reasons to reset at all, so
	// the liveness probe has to survive it rather than refuse to run.
	if err := os.WriteFile(p.ConfigFile(), []byte("{not json"), 0600); err != nil {
		t.Fatal(err)
	}
	addr, _ := daemonRunning(p)
	if addr != config.DefaultAPIAddr {
		t.Errorf("daemonRunning addr = %q, want the default %q", addr, config.DefaultAPIAddr)
	}
}

func TestInitRejectsYesWithoutReset(t *testing.T) {
	err := cmdInit(tempPaths(t), []string{"--yes"})
	if err == nil || !strings.Contains(err.Error(), "--yes") {
		t.Errorf("cmdInit --yes = %v, want an error explaining --yes needs --reset", err)
	}
}

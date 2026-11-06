package main

import (
	"bytes"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nickmarrone/syncat/internal/api/wire"
	"github.com/nickmarrone/syncat/internal/config"
)

// populateForReset is populate plus a valid config.json whose api_addr
// names a port this test controls. Without it, resetState's "is a daemon
// running?" check falls back to the default 127.0.0.1:8347, where a
// developer's real daemon may well be listening.
func populateForReset(t *testing.T, apiAddr string, sharePath string) *config.Paths {
	t.Helper()
	p := tempPaths(t)
	populate(t, p)
	cfg := config.Default()
	cfg.APIAddr = apiAddr
	cfg.Shares = []config.Share{{ID: "s1", Name: "docs", Path: sharePath, Permission: config.PermissionReadWrite}}
	if err := config.Save(p.ConfigFile(), cfg); err != nil {
		t.Fatal(err)
	}
	return p
}

// closedAddr is a loopback address with nothing listening on it.
func closedAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

func stateGone(p *config.Paths) bool {
	for _, f := range []string{p.ConfigFile(), p.APITokenFile(), p.IdentityKeyFile(), p.TailcatKeyFile(), p.DBFile()} {
		if _, err := os.Stat(f); err == nil {
			return false
		}
	}
	return true
}

func TestResetStateWithYesRemovesStateAndKeepsShares(t *testing.T) {
	share := t.TempDir()
	if err := os.WriteFile(filepath.Join(share, "keep.txt"), []byte("mine"), 0o600); err != nil {
		t.Fatal(err)
	}
	p := populateForReset(t, closedAddr(t), share)
	if err := resetState(p, strings.NewReader(""), &bytes.Buffer{}, true); err != nil {
		t.Fatalf("resetState --yes: %v", err)
	}
	if !stateGone(p) {
		t.Error("resetState --yes left config, keys, or the index behind")
	}
	if data, err := os.ReadFile(filepath.Join(share, "keep.txt")); err != nil || string(data) != "mine" {
		t.Errorf("resetState touched a shared folder: %q, %v", data, err)
	}
}

func TestResetStatePromptTakesOnlyTheExactWord(t *testing.T) {
	share := t.TempDir()
	for _, tc := range []struct {
		typed  string
		resets bool
	}{
		{"RESET\n", true},
		{"reset\n", false},
		{"y\n", false},
		{"", false},
	} {
		p := populateForReset(t, closedAddr(t), share)
		var out bytes.Buffer
		err := resetState(p, strings.NewReader(tc.typed), &out, false)
		if tc.resets {
			if err != nil || !stateGone(p) {
				t.Errorf("typing %q: err=%v, state gone=%v; want a reset", tc.typed, err, stateGone(p))
			}
			if !strings.Contains(out.String(), share) {
				t.Errorf("the prompt did not name the shared folder it keeps:\n%s", out.String())
			}
			continue
		}
		if err == nil {
			t.Errorf("typing %q: reset went ahead", tc.typed)
		}
		if _, statErr := os.Stat(p.IdentityKeyFile()); statErr != nil {
			t.Errorf("typing %q: the identity key is gone after a declined reset", tc.typed)
		}
	}
}

func TestResetStateRefusesWhileTheDaemonIsListening(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	p := populateForReset(t, ln.Addr().String(), t.TempDir())
	err = resetState(p, strings.NewReader("RESET\n"), &bytes.Buffer{}, true)
	if err == nil || !strings.Contains(err.Error(), "stop the syncat daemon") {
		t.Fatalf("resetState with a live daemon: err = %v", err)
	}
	if stateGone(p) {
		t.Error("resetState removed state while a daemon was listening")
	}
}

func TestSummarizeTrashEvent(t *testing.T) {
	items := func(kinds ...string) []wire.TrashItem {
		var out []wire.TrashItem
		for _, k := range kinds {
			out = append(out, wire.TrashItem{Kind: k})
		}
		return out
	}
	for _, tc := range []struct {
		kinds []string
		want  string
	}{
		{[]string{"deleted"}, "deleted 1 file"},
		{[]string{"deleted", "deleted", "overwritten"}, "deleted 2 files, overwrote 1 file"},
		{[]string{"migrated", "migrated"}, "migrated 2 files"},
		{[]string{""}, "kept 1 file"},
		{nil, ""},
	} {
		if got := summarizeTrashEvent(wire.TrashEvent{Items: items(tc.kinds...)}); got != tc.want {
			t.Errorf("summarizeTrashEvent(%q) = %q, want %q", tc.kinds, got, tc.want)
		}
	}
}

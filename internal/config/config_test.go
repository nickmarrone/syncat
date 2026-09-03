package config

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
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

func TestApplyDefaultsFillsZeroValues(t *testing.T) {
	cfg := &Config{
		NodeName: "alice",
		Shares:   []Share{{ID: "a", Name: "s", Path: "/abs", Permission: PermissionReadOnly}},
	}
	cfg.ApplyDefaults()

	if cfg.APIAddr != DefaultAPIAddr {
		t.Errorf("APIAddr = %q, want %q", cfg.APIAddr, DefaultAPIAddr)
	}
	if cfg.TrashRetentionDays != DefaultTrashRetentionDays {
		t.Errorf("TrashRetentionDays = %d, want %d", cfg.TrashRetentionDays, DefaultTrashRetentionDays)
	}
	if cfg.RescanIntervalSeconds != DefaultRescanIntervalSeconds {
		t.Errorf("RescanIntervalSeconds = %d, want %d", cfg.RescanIntervalSeconds, DefaultRescanIntervalSeconds)
	}
	if cfg.Shares[0].Access == nil {
		t.Errorf("Shares[0].Access = nil, want non-nil map")
	}
}

func TestApplyDefaultsPreservesExplicitValues(t *testing.T) {
	cfg := &Config{
		APIAddr:               "127.0.0.1:9999",
		TrashRetentionDays:    14,
		RescanIntervalSeconds: 60,
	}
	cfg.ApplyDefaults()

	if cfg.APIAddr != "127.0.0.1:9999" {
		t.Errorf("APIAddr = %q, want unchanged", cfg.APIAddr)
	}
	if cfg.TrashRetentionDays != 14 {
		t.Errorf("TrashRetentionDays = %d, want unchanged", cfg.TrashRetentionDays)
	}
	if cfg.RescanIntervalSeconds != 60 {
		t.Errorf("RescanIntervalSeconds = %d, want unchanged", cfg.RescanIntervalSeconds)
	}
}

func TestDefaultReturnsDocumentedValues(t *testing.T) {
	cfg := Default()
	if cfg.APIAddr != DefaultAPIAddr {
		t.Errorf("APIAddr = %q, want %q", cfg.APIAddr, DefaultAPIAddr)
	}
	if cfg.TrashRetentionDays != DefaultTrashRetentionDays {
		t.Errorf("TrashRetentionDays = %d, want %d", cfg.TrashRetentionDays, DefaultTrashRetentionDays)
	}
	if cfg.RescanIntervalSeconds != DefaultRescanIntervalSeconds {
		t.Errorf("RescanIntervalSeconds = %d, want %d", cfg.RescanIntervalSeconds, DefaultRescanIntervalSeconds)
	}
	if cfg.NodeName != "" {
		t.Errorf("NodeName = %q, want empty", cfg.NodeName)
	}
}

func TestValidateAcceptsWellFormedConfig(t *testing.T) {
	cfg := Default()
	cfg.NodeName = "alice"
	cfg.Shares = []Share{{ID: "a", Name: "docs", Path: "/home/alice/docs", Permission: PermissionReadWrite}}
	cfg.Subscriptions = []Subscription{{Peer: "bob", ShareID: "a", LocalPath: "/home/alice/bob", Mode: ModeMirror}}

	if err := cfg.Validate(); err != nil {
		t.Errorf("Validate() = %v, want nil", err)
	}
}

func TestValidateRejectsEmptyAPIAddr(t *testing.T) {
	cfg := Default()
	cfg.APIAddr = ""
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for empty api_addr, got nil")
	}
}

func TestValidateRejectsBadPermission(t *testing.T) {
	cfg := Default()
	cfg.Shares = []Share{{ID: "a", Name: "s", Path: "/abs/path", Permission: "read-execute"}}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for invalid permission, got nil")
	}
}

func TestValidateRejectsRelativeSharePath(t *testing.T) {
	cfg := Default()
	cfg.Shares = []Share{{ID: "a", Name: "s", Path: "relative/path", Permission: PermissionReadOnly}}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for relative share path, got nil")
	}
}

func TestValidateRejectsBadSubscriptionMode(t *testing.T) {
	cfg := Default()
	cfg.Subscriptions = []Subscription{{Peer: "bob", ShareID: "a", LocalPath: "/abs", Mode: "sideways"}}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for invalid subscription mode, got nil")
	}
}

func TestNewShareIDLengthAndUniqueness(t *testing.T) {
	seen := make(map[string]bool)
	for i := 0; i < 1000; i++ {
		id, err := NewShareID()
		if err != nil {
			t.Fatalf("NewShareID: %v", err)
		}
		if len(id) != 16 { // 8 bytes, hex-encoded
			t.Fatalf("NewShareID() = %q, want 16 hex chars", id)
		}
		for _, r := range id {
			if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f')) {
				t.Fatalf("NewShareID() = %q contains non-hex character %q", id, r)
			}
		}
		if seen[id] {
			t.Fatalf("NewShareID() produced duplicate id %q", id)
		}
		seen[id] = true
	}
}

func TestMarshalUnmarshalRoundTrip(t *testing.T) {
	cfg := &Config{
		NodeName:              "alice",
		APIAddr:               "127.0.0.1:9999",
		TrashRetentionDays:    14,
		RescanIntervalSeconds: 60,
		Debug:                 true,
		GlobalIgnores:         []string{"*.tmp", "node_modules/"},
		Peers: []Peer{
			{Name: "bob", Token: "sc1abc", Enabled: true},
			{Name: "carol", Token: "sc1def", Enabled: false},
		},
		Shares: []Share{
			{
				ID:               "deadbeefcafef00d",
				Name:             "docs",
				Path:             "/home/alice/docs",
				Permission:       PermissionReadWrite,
				ApprovalRequired: true,
				Access: map[string]string{
					"peerkey1": "granted",
					"peerkey2": "denied",
				},
			},
		},
		Subscriptions: []Subscription{
			{Peer: "bob", ShareID: "deadbeefcafef00d", LocalPath: "/home/alice/bob-docs", Mode: ModeMirror, Paused: false},
		},
	}

	data, err := Marshal(cfg)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	got, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	assertConfigEqual(t, got, cfg)
}

func TestUnmarshalPeerEnabledDefaultsTrueWhenAbsent(t *testing.T) {
	doc := `{
		"node_name": "alice",
		"peers": [
			{"name": "bob", "token": "sc1abc"},
			{"name": "carol", "token": "sc1def", "enabled": false}
		]
	}`

	cfg, err := Unmarshal([]byte(doc))
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if len(cfg.Peers) != 2 {
		t.Fatalf("got %d peers, want 2", len(cfg.Peers))
	}
	if !cfg.Peers[0].Enabled {
		t.Errorf("peer[0] (bob, enabled absent) Enabled = false, want default true")
	}
	if cfg.Peers[1].Enabled {
		t.Errorf("peer[1] (carol, enabled=false) Enabled = true, want false")
	}
}

func TestUnmarshalPeerEnabledRoundTripsWhenAbsent(t *testing.T) {
	// A peer written without Enabled explicitly set should still marshal
	// (Enabled: true, matching the Go zero-value-free default) and, more
	// importantly, an already-absent field in hand-edited JSON should come
	// back enabled.
	cfg := &Config{Peers: []Peer{{Name: "bob", Token: "sc1abc", Enabled: true}}}
	cfg.ApplyDefaults()

	data, err := Marshal(cfg)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if !got.Peers[0].Enabled {
		t.Errorf("Peers[0].Enabled = false, want true")
	}
}

func TestUnmarshalAppliesTopLevelDefaults(t *testing.T) {
	cfg, err := Unmarshal([]byte(`{"node_name": "alice"}`))
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if cfg.APIAddr != DefaultAPIAddr {
		t.Errorf("APIAddr = %q, want %q", cfg.APIAddr, DefaultAPIAddr)
	}
	if cfg.TrashRetentionDays != DefaultTrashRetentionDays {
		t.Errorf("TrashRetentionDays = %d, want %d", cfg.TrashRetentionDays, DefaultTrashRetentionDays)
	}
	if cfg.RescanIntervalSeconds != DefaultRescanIntervalSeconds {
		t.Errorf("RescanIntervalSeconds = %d, want %d", cfg.RescanIntervalSeconds, DefaultRescanIntervalSeconds)
	}
	if cfg.Debug {
		t.Errorf("Debug = true, want false when absent from config")
	}
}

func TestUnmarshalRejectsInvalidJSON(t *testing.T) {
	if _, err := Unmarshal([]byte(`not json`)); err == nil {
		t.Fatal("expected error for invalid JSON, got nil")
	}
}

func assertConfigEqual(t *testing.T, got, want *Config) {
	t.Helper()
	if got.NodeName != want.NodeName {
		t.Errorf("NodeName = %q, want %q", got.NodeName, want.NodeName)
	}
	if got.APIAddr != want.APIAddr {
		t.Errorf("APIAddr = %q, want %q", got.APIAddr, want.APIAddr)
	}
	if got.TrashRetentionDays != want.TrashRetentionDays {
		t.Errorf("TrashRetentionDays = %d, want %d", got.TrashRetentionDays, want.TrashRetentionDays)
	}
	if got.RescanIntervalSeconds != want.RescanIntervalSeconds {
		t.Errorf("RescanIntervalSeconds = %d, want %d", got.RescanIntervalSeconds, want.RescanIntervalSeconds)
	}
	if got.Debug != want.Debug {
		t.Errorf("Debug = %v, want %v", got.Debug, want.Debug)
	}
	if len(got.Peers) != len(want.Peers) {
		t.Fatalf("got %d peers, want %d", len(got.Peers), len(want.Peers))
	}
	for i := range want.Peers {
		if got.Peers[i] != want.Peers[i] {
			t.Errorf("Peers[%d] = %+v, want %+v", i, got.Peers[i], want.Peers[i])
		}
	}
	if len(got.Shares) != len(want.Shares) {
		t.Fatalf("got %d shares, want %d", len(got.Shares), len(want.Shares))
	}
	for i := range want.Shares {
		gs, ws := got.Shares[i], want.Shares[i]
		if gs.ID != ws.ID || gs.Name != ws.Name || gs.Path != ws.Path || gs.Permission != ws.Permission || gs.ApprovalRequired != ws.ApprovalRequired {
			t.Errorf("Shares[%d] = %+v, want %+v", i, gs, ws)
		}
		if len(gs.Access) != len(ws.Access) {
			t.Errorf("Shares[%d].Access = %v, want %v", i, gs.Access, ws.Access)
		}
		for k, v := range ws.Access {
			if gs.Access[k] != v {
				t.Errorf("Shares[%d].Access[%q] = %q, want %q", i, k, gs.Access[k], v)
			}
		}
	}
	if len(got.Subscriptions) != len(want.Subscriptions) {
		t.Fatalf("got %d subscriptions, want %d", len(got.Subscriptions), len(want.Subscriptions))
	}
	for i := range want.Subscriptions {
		if got.Subscriptions[i] != want.Subscriptions[i] {
			t.Errorf("Subscriptions[%d] = %+v, want %+v", i, got.Subscriptions[i], want.Subscriptions[i])
		}
	}
}

func TestPathContains(t *testing.T) {
	tests := []struct {
		parent, child string
		want          bool
	}{
		{"/a/code", "/a/code", true},
		{"/a/code", "/a/code/sub", true},
		{"/a/code", "/a/code/deep/nested/file", true},
		{"/a/code/", "/a/code", true},
		{"/a/code", "/a/code/../code/sub", true},
		{"/a", "/a/code", true},

		// The prefix-string trap: these share a textual prefix but are
		// unrelated directories.
		{"/a/code", "/a/codebase", false},
		{"/a/code", "/a/code2", false},

		{"/a/code/sub", "/a/code", false},
		{"/a/code", "/b/code", false},
		{"/a/code", "/", false},
	}
	for _, tt := range tests {
		if got := pathContains(tt.parent, tt.child); got != tt.want {
			t.Errorf("pathContains(%q, %q) = %v, want %v", tt.parent, tt.child, got, tt.want)
		}
	}
}

func TestPathsOverlapIsSymmetric(t *testing.T) {
	pairs := [][2]string{
		{"/a/code", "/a/code"},
		{"/a/code", "/a/code/sub"},
		{"/a", "/a/code"},
		{"/a/code", "/a/codebase"},
		{"/a/code", "/b/code"},
	}
	for _, p := range pairs {
		if pathsOverlap(p[0], p[1]) != pathsOverlap(p[1], p[0]) {
			t.Errorf("pathsOverlap not symmetric for %q, %q", p[0], p[1])
		}
	}
}

// subscribedConfig is a node syncing a peer's share down into /home/n/code.
func subscribedConfig() *Config {
	c := Default()
	c.Subscriptions = []Subscription{{
		Peer:      "alice",
		ShareID:   "a1b2c3d4e5f60718",
		LocalPath: "/home/n/code",
		Mode:      ModeMirror,
	}}
	return c
}

func TestCheckSharePathRejectsSubscribedTree(t *testing.T) {
	c := subscribedConfig()
	for _, path := range []string{
		"/home/n/code",         // the subscribed directory itself
		"/home/n/code/pkg",     // a directory inside it
		"/home/n",              // a parent, which would re-export it
		"/home/n/code/../code", // the same directory, spelled indirectly
	} {
		err := c.CheckSharePath(path)
		if err == nil {
			t.Errorf("CheckSharePath(%q) = nil, want rejection", path)
			continue
		}
		if !strings.Contains(err.Error(), "offered back out") {
			t.Errorf("CheckSharePath(%q) error = %v, want the re-share explanation", path, err)
		}
	}
}

func TestCheckSharePathAllowsUnrelated(t *testing.T) {
	c := subscribedConfig()
	for _, path := range []string{
		"/home/n/docs",
		"/home/n/codebase", // shares a prefix but is a different directory
		"/srv/data",
	} {
		if err := c.CheckSharePath(path); err != nil {
			t.Errorf("CheckSharePath(%q) = %v, want nil", path, err)
		}
	}
}

func TestCheckSharePathRequiresAbsolute(t *testing.T) {
	c := Default()
	if err := c.CheckSharePath("relative/path"); err == nil {
		t.Fatal("CheckSharePath accepted a relative path")
	}
}

// sharingConfig is a node offering /home/n/code to its peers.
func sharingConfig() *Config {
	c := Default()
	c.Shares = []Share{{
		ID:         "0011223344556677",
		Name:       "code",
		Path:       "/home/n/code",
		Permission: PermissionReadWrite,
		Access:     map[string]string{},
	}}
	return c
}

// The rule has to hold whichever side is added second.
func TestCheckSubscriptionPathRejectsSharedTree(t *testing.T) {
	c := sharingConfig()
	for _, path := range []string{
		"/home/n/code",
		"/home/n/code/vendor",
		"/home/n",
	} {
		if err := c.CheckSubscriptionPath(path); err == nil {
			t.Errorf("CheckSubscriptionPath(%q) = nil, want rejection", path)
		}
	}
}

func TestCheckSubscriptionPathAllowsUnrelated(t *testing.T) {
	c := sharingConfig()
	if err := c.CheckSubscriptionPath("/home/n/from-alice"); err != nil {
		t.Errorf("CheckSubscriptionPath = %v, want nil", err)
	}
}

// A hand-edited config.json must not be able to smuggle the overlap past the
// API-layer checks.
func TestValidateRejectsReSharedSubscription(t *testing.T) {
	c := subscribedConfig()
	c.Shares = []Share{{
		ID:         "0011223344556677",
		Name:       "code",
		Path:       "/home/n/code/pkg",
		Permission: PermissionReadWrite,
		Access:     map[string]string{},
	}}
	err := c.Validate()
	if err == nil {
		t.Fatal("Validate accepted a share nested inside a subscription")
	}
	if !strings.Contains(err.Error(), "offered back out") {
		t.Errorf("Validate error = %v, want the re-share explanation", err)
	}
}

func TestValidateAllowsDisjointShareAndSubscription(t *testing.T) {
	c := subscribedConfig()
	c.Shares = []Share{{
		ID:         "0011223344556677",
		Name:       "docs",
		Path:       "/home/n/docs",
		Permission: PermissionReadWrite,
		Access:     map[string]string{},
	}}
	if err := c.Validate(); err != nil {
		t.Errorf("Validate = %v, want nil", err)
	}
}

func TestValidateRejectsRelativeSubscriptionPath(t *testing.T) {
	c := Default()
	c.Subscriptions = []Subscription{{
		Peer: "alice", ShareID: "a1b2c3d4e5f60718",
		LocalPath: "relative/dir", Mode: ModeMirror,
	}}
	if err := c.Validate(); err == nil {
		t.Fatal("Validate accepted a relative subscription local path")
	}
}

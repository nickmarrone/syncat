package config

import "testing"

func TestMarshalUnmarshalRoundTrip(t *testing.T) {
	cfg := &Config{
		NodeName:              "alice",
		APIAddr:               "127.0.0.1:9999",
		TrashRetentionDays:    14,
		RescanIntervalSeconds: 60,
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

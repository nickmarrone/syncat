package config

import "testing"

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

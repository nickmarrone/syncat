package config

import "testing"

func TestParseTrashMaxSize(t *testing.T) {
	good := map[string]TrashMaxSize{
		"25%":   {Percent: 25},
		"100%":  {Percent: 100},
		"2GiB":  {Bytes: 2 << 30},
		"500MB": {Bytes: 500e6},
		"1024":  {Bytes: 1024},
		"3 KiB": {Bytes: 3 << 10},
	}
	for in, want := range good {
		got, err := ParseTrashMaxSize(in)
		if err != nil || got != want {
			t.Errorf("ParseTrashMaxSize(%q) = %+v, %v; want %+v", in, got, err, want)
		}
	}
	for _, in := range []string{"", "0%", "101%", "-5MB", "two GiB", "1.5GiB", "0"} {
		if _, err := ParseTrashMaxSize(in); err == nil {
			t.Errorf("ParseTrashMaxSize(%q) succeeded, want an error", in)
		}
	}
}

func TestTrashMaxSizeLimit(t *testing.T) {
	if got := (TrashMaxSize{Percent: 25}).Limit(1000); got != 250 {
		t.Errorf("25%% of 1000 = %d, want 250", got)
	}
	if got := (TrashMaxSize{Bytes: 77}).Limit(1000); got != 77 {
		t.Errorf("absolute limit = %d, want 77", got)
	}
}

func TestSafetyForFillsDefaults(t *testing.T) {
	cfg := Default()
	cfg.TrashRetentionDays = 14
	cfg.Shares = []Share{{ID: "s1", Safety: Safety{HoldDeletePercent: 80}}}
	cfg.Subscriptions = []Subscription{{ShareID: "s2", Safety: Safety{HoldDeleteMin: 3, TrashMaxSize: "1GiB"}}}

	if got, want := cfg.SafetyFor("s1"), (Safety{80, DefaultHoldDeleteMin, 14, DefaultTrashMaxSize}); got != want {
		t.Errorf("SafetyFor(s1) = %+v, want %+v", got, want)
	}
	if got, want := cfg.SafetyFor("s2"), (Safety{DefaultHoldDeletePercent, 3, 14, "1GiB"}); got != want {
		t.Errorf("SafetyFor(s2) = %+v, want %+v", got, want)
	}
}

func TestSafetyRoundTripsThroughJSON(t *testing.T) {
	cfg := Default()
	cfg.Shares = []Share{{ID: "s1", Name: "docs", Permission: PermissionReadWrite, Safety: Safety{HoldDeletePercent: 30, TrashMaxSize: "10%"}}}
	cfg.Subscriptions = []Subscription{{Peer: "p", ShareID: "s2", Mode: ModeMirror, Safety: Safety{HoldDeleteMin: 5, TrashRetentionDays: 7}}}
	data, err := Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	back, err := Unmarshal(data)
	if err != nil {
		t.Fatal(err)
	}
	if back.Shares[0].Safety != cfg.Shares[0].Safety {
		t.Errorf("share safety = %+v, want %+v", back.Shares[0].Safety, cfg.Shares[0].Safety)
	}
	if back.Subscriptions[0].Safety != cfg.Subscriptions[0].Safety {
		t.Errorf("subscription safety = %+v, want %+v", back.Subscriptions[0].Safety, cfg.Subscriptions[0].Safety)
	}
}

func TestSafetyValidate(t *testing.T) {
	for _, s := range []Safety{{HoldDeletePercent: 101}, {HoldDeleteMin: -1}, {TrashRetentionDays: -2}, {TrashMaxSize: "lots"}} {
		if err := s.Validate(); err == nil {
			t.Errorf("Validate(%+v) succeeded, want an error", s)
		}
	}
	if err := (Safety{}).Validate(); err != nil {
		t.Errorf("zero Safety: %v", err)
	}
}

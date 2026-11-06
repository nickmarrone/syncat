package config

import "testing"

// A negative rescan interval is rejected; zero stays valid and means the
// default (ApplyDefaults fills it in), like the other top-level fields.
func TestValidateRescanInterval(t *testing.T) {
	for _, tc := range []struct {
		seconds int
		ok      bool
	}{{-1, false}, {-300, false}, {0, true}, {1, true}, {DefaultRescanIntervalSeconds, true}} {
		cfg := Default()
		cfg.RescanIntervalSeconds = tc.seconds
		if err := cfg.Validate(); (err == nil) != tc.ok {
			t.Errorf("Validate() with rescan_interval_seconds %d = %v, want ok=%v", tc.seconds, err, tc.ok)
		}
	}
}

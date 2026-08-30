package config

import "testing"

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

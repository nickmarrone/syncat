package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A hand-edited config.json with an api_addr the daemon would refuse to
// bind must be rejected on load, not discovered on the next daemon start.
func TestLoadRejectsNonLoopbackAPIAddr(t *testing.T) {
	for _, addr := range []string{"0.0.0.0:8347", "[::]:8347", "192.0.2.1:8347", "example.com:8347", "not-an-address", "127.0.0.1", "127.0.0.1:"} {
		t.Run(addr, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			doc := `{"node_name":"alice","api_addr":"` + addr + `"}`
			if err := os.WriteFile(path, []byte(doc), 0600); err != nil {
				t.Fatal(err)
			}
			_, err := Load(path)
			if err == nil {
				t.Fatalf("Load accepted api_addr %q", addr)
			}
			if !strings.Contains(err.Error(), "loopback") {
				t.Errorf("Load error %q does not say the API is loopback-only", err)
			}

			cfg := Default()
			cfg.APIAddr = addr
			if err := cfg.Validate(); err == nil {
				t.Errorf("Validate accepted api_addr %q", addr)
			}
		})
	}
}

func TestValidateAcceptsLoopbackAPIAddr(t *testing.T) {
	for _, addr := range []string{"127.0.0.1:8347", "localhost:8347", "[::1]:8347", ":8347", "127.0.0.2:8347"} {
		cfg := Default()
		cfg.APIAddr = addr
		if err := cfg.Validate(); err != nil {
			t.Errorf("Validate rejected loopback api_addr %q: %v", addr, err)
		}
	}
}

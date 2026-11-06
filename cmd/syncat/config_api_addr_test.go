package main

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/nickmarrone/syncat/internal/config"
)

// An api_addr the daemon would refuse must be refused when it is set, not
// on the next daemon start: a non-loopback or malformed api_addr leaves the
// daemon unable to start and the CLI unable to reach it at all (#52).
func TestConfigSetRejectsAPIAddrTheDaemonWouldRefuse(t *testing.T) {
	c := newCLINode(t)
	before, err := os.ReadFile(c.paths.ConfigFile())
	if err != nil {
		t.Fatal(err)
	}
	for _, addr := range []string{
		"0.0.0.0:8347",
		"not-an-address",
		"[::]:8347",
		"192.0.2.1:8347",
		"example.com:8347",
		"127.0.0.1",
	} {
		out, err := c.run("config", "set", "api_addr", addr)
		if err == nil {
			t.Errorf("config set api_addr %s was accepted\noutput: %s", addr, out)
			continue
		}
		if !strings.Contains(err.Error(), "loopback") {
			t.Errorf("config set api_addr %s: err = %q, want it to say the API is loopback-only", addr, err)
		}
	}
	after, err := os.ReadFile(c.paths.ConfigFile())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Errorf("rejected config set calls changed config.json:\n%s", after)
	}
}

func TestConfigSetAcceptsLoopbackAPIAddr(t *testing.T) {
	c := newCLINode(t)
	for _, addr := range []string{"localhost:9999", "127.0.0.1:9999", "[::1]:9999"} {
		c.ok("config", "set", "api_addr", addr)
		cfg, err := config.Load(c.paths.ConfigFile())
		if err != nil {
			t.Fatal(err)
		}
		if cfg.APIAddr != addr {
			t.Errorf("after config set api_addr %s, api_addr = %q", addr, cfg.APIAddr)
		}
	}
}

// A config.json written by an older version may already hold a bad
// api_addr. Load now rejects it, but `config set api_addr` must still be
// able to repair it, since that is the fix the error points at.
func TestConfigSetRepairsBadAPIAddrAlreadyOnDisk(t *testing.T) {
	c := newCLINode(t)
	data, err := os.ReadFile(c.paths.ConfigFile())
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Unmarshal(data)
	if err != nil {
		t.Fatal(err)
	}
	cfg.APIAddr = "0.0.0.0:8347"
	bad, err := config.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(c.paths.ConfigFile(), bad, 0o600); err != nil {
		t.Fatal(err)
	}

	// A command that needs a valid config says how to fix it.
	c.fails("config set api_addr", "status")

	// Setting some other field does not paper over the bad api_addr.
	c.fails("loopback", "config", "set", "node_name", "renamed")

	c.ok("config", "set", "api_addr", "127.0.0.1:8347")
	got, err := config.Load(c.paths.ConfigFile())
	if err != nil {
		t.Fatalf("config.json still invalid after repair: %v", err)
	}
	if got.APIAddr != "127.0.0.1:8347" || got.NodeName != cfg.NodeName {
		t.Errorf("after repair: api_addr = %q, node_name = %q", got.APIAddr, got.NodeName)
	}
}

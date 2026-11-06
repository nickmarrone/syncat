package main

import (
	"bytes"
	"os"
	"testing"
)

// A negative rescan interval is meaningless, and the daemon refuses a
// config.json that holds one, so `config set` must refuse it when it is
// set rather than leave it for the next daemon start to trip over.
func TestConfigSetRejectsNegativeRescanInterval(t *testing.T) {
	c := newCLINode(t)
	before, err := os.ReadFile(c.paths.ConfigFile())
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range []string{"-5", "-1"} {
		if _, err := c.run("config", "set", "rescan_interval_seconds", v); err == nil {
			t.Errorf("config set rescan_interval_seconds %s was accepted", v)
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

package main

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/nickmarrone/syncat/internal/config"
)

// A value the daemon would refuse must be refused when it is set, not on
// the next daemon start: a negative trash retention would put the sweep's
// age cutoff in the future and empty every trash that uses the default.
func TestConfigSetRejectsValuesTheDaemonWouldRefuse(t *testing.T) {
	c := newCLINode(t)
	before, err := os.ReadFile(c.paths.ConfigFile())
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range [][2]string{
		{"trash_retention_days", "-1"},
	} {
		if _, err := c.run("config", "set", tc[0], tc[1]); err == nil {
			t.Errorf("config set %s %s was accepted", tc[0], tc[1])
		} else if !strings.Contains(err.Error(), "at least 1") {
			t.Errorf("config set %s %s: err = %q, want it to say the value must be at least 1", tc[0], tc[1], err)
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

// 0 means "use the default", as it does everywhere else in the config.
func TestConfigSetTrashRetentionZeroMeansDefault(t *testing.T) {
	c := newCLINode(t)
	c.ok("config", "set", "trash_retention_days", "7")
	c.ok("config", "set", "trash_retention_days", "0")
	cfg, err := config.Load(c.paths.ConfigFile())
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TrashRetentionDays != config.DefaultTrashRetentionDays {
		t.Errorf("trash_retention_days after setting 0 = %d, want the default %d",
			cfg.TrashRetentionDays, config.DefaultTrashRetentionDays)
	}
}

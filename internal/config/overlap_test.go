package config

import (
	"strings"
	"testing"
)

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

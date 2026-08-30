package config

import (
	"errors"
	"fmt"
	"path/filepath"
)

// Deviation from SPEC.md §3: the spec describes config.toml, but §10's
// dependency budget (tailcat, modernc.org/sqlite, fsnotify, fxamacker/cbor,
// a gitignore matcher, x/crypto) lists no TOML library. To stay inside that
// budget this implementation uses the stdlib encoding/json package instead,
// and the file is named config.json. The schema (field names, defaults,
// validation) is otherwise exactly as specified. See serialize.go for the
// on-disk encoding.

// Defaults for top-level config fields, applied when a value is absent (or
// zero) on load.
const (
	DefaultAPIAddr               = "127.0.0.1:8347"
	DefaultTrashRetentionDays    = 30
	DefaultRescanIntervalSeconds = 300
)

// Permission values for Share.Permission.
const (
	PermissionReadOnly  = "read-only"
	PermissionReadWrite = "read-write"
)

// Mode values for Subscription.Mode.
const (
	ModeMirror      = "mirror"
	ModeReceiveOnly = "receive-only"
)

// Config is the top-level shape of config.json (SPEC.md §3).
type Config struct {
	NodeName              string
	APIAddr               string
	TrashRetentionDays    int
	RescanIntervalSeconds int
	GlobalIgnores         []string
	Peers                 []Peer
	Shares                []Share
	Subscriptions         []Subscription
}

// Peer is a configured peer: another node added by pasting its token.
type Peer struct {
	Name    string
	Token   string
	Enabled bool
}

// Share is a local directory this node offers.
type Share struct {
	ID               string
	Name             string
	Path             string
	Permission       string
	ApprovalRequired bool
	// Access maps a peer's Ed25519 public key (hex) to "granted" or
	// "denied".
	Access map[string]string
}

// Subscription is this node's decision to sync a peer's share into a local
// directory.
type Subscription struct {
	Peer      string
	ShareID   string
	LocalPath string
	Mode      string
	Paused    bool
}

// Default returns a Config populated with the documented defaults and an
// empty node name.
func Default() *Config {
	return &Config{
		APIAddr:               DefaultAPIAddr,
		TrashRetentionDays:    DefaultTrashRetentionDays,
		RescanIntervalSeconds: DefaultRescanIntervalSeconds,
	}
}

// ApplyDefaults fills in zero-valued top-level fields with their documented
// defaults and ensures every Share.Access map is non-nil. It does not (and
// cannot, given only a *Config) distinguish "absent from the file" from
// "explicitly set to the zero value" for fields like Peer.Enabled whose
// default is non-zero (true) — that distinction is handled during
// unmarshaling, in serialize.go, before defaults collapse to a plain bool.
func (c *Config) ApplyDefaults() {
	if c.APIAddr == "" {
		c.APIAddr = DefaultAPIAddr
	}
	if c.TrashRetentionDays == 0 {
		c.TrashRetentionDays = DefaultTrashRetentionDays
	}
	if c.RescanIntervalSeconds == 0 {
		c.RescanIntervalSeconds = DefaultRescanIntervalSeconds
	}
	for i := range c.Shares {
		if c.Shares[i].Access == nil {
			c.Shares[i].Access = map[string]string{}
		}
	}
}

// Validate checks invariants Load/Save both enforce: known share
// permissions, known subscription modes, and absolute share paths.
func (c *Config) Validate() error {
	var errs []error

	if c.APIAddr == "" {
		errs = append(errs, errors.New("api_addr must not be empty"))
	}

	for i, s := range c.Shares {
		if s.Permission != PermissionReadOnly && s.Permission != PermissionReadWrite {
			errs = append(errs, fmt.Errorf("share[%d] (%s): invalid permission %q (want %q or %q)",
				i, s.Name, s.Permission, PermissionReadOnly, PermissionReadWrite))
		}
		if s.Path != "" && !filepath.IsAbs(s.Path) {
			errs = append(errs, fmt.Errorf("share[%d] (%s): path %q must be absolute", i, s.Name, s.Path))
		}
	}

	for i, sub := range c.Subscriptions {
		if sub.Mode != ModeMirror && sub.Mode != ModeReceiveOnly {
			errs = append(errs, fmt.Errorf("subscription[%d] (peer %s, share %s): invalid mode %q (want %q or %q)",
				i, sub.Peer, sub.ShareID, sub.Mode, ModeMirror, ModeReceiveOnly))
		}
	}

	return errors.Join(errs...)
}

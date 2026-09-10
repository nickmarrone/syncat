// Package config owns everything under SPEC.md §3: the on-disk config
// file and its schema (config.go), the node's persistent keys and the
// sc1 token format built from them (keys.go), and the XDG path layout
// everything else is resolved against (paths.go).
package config

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Deviation from SPEC.md §3: the spec describes config.toml, but §10's
// dependency budget (tailcat, modernc.org/sqlite, fsnotify, fxamacker/cbor,
// a gitignore matcher, x/crypto) lists no TOML library. To stay inside that
// budget this implementation uses the stdlib encoding/json package instead,
// and the file is named config.json. The schema (field names, defaults,
// validation) is otherwise exactly as specified. See Marshal/Unmarshal
// below for the on-disk encoding.

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

// --- the config schema (SPEC.md §3) ------------------------------------

// Config is the top-level shape of config.json (SPEC.md §3).
type Config struct {
	NodeName              string
	APIAddr               string
	TrashRetentionDays    int
	RescanIntervalSeconds int
	Debug                 bool
	GlobalIgnores         []string
	Peers                 []Peer
	PendingPeers          []PendingPeer
	Shares                []Share
	Subscriptions         []Subscription
}

// Peer is a configured peer: another node added by pasting its token.
type Peer struct {
	Name    string
	Token   string
	Enabled bool
}

// PendingPeer is a validated unknown inbound Hello retained for explicit
// approval. Token is stored alongside configured peer tokens in the same
// mode-0600 config so approval can establish peering without another paste.
type PendingPeer struct {
	Name      string
	Token     string
	FirstSeen time.Time
	LastSeen  time.Time
}

// Share is a local directory this node offers.
type Share struct {
	ID               string
	Name             string
	Path             string
	Permission       string
	ApprovalRequired bool
	// Access maps a peer's Ed25519 public key (hex) to its pending, granted,
	// denied, or revoked access state.
	Access map[string]string
}

// NewShareID returns a random 8-byte hex-encoded (16 character) share id,
// generated at share creation and stable for the share's life (SPEC.md §3).
func NewShareID() (string, error) {
	raw := make([]byte, 8)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("config: generate share id: %w", err)
	}
	return hex.EncodeToString(raw), nil
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
// unmarshaling, in docToConfig, before defaults collapse to a plain bool.
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
		if sub.LocalPath != "" && !filepath.IsAbs(sub.LocalPath) {
			errs = append(errs, fmt.Errorf("subscription[%d] (peer %s, share %s): local path %q must be absolute",
				i, sub.Peer, sub.ShareID, sub.LocalPath))
		}
	}

	// A directory synced down from a peer must not be offered back out as
	// one of our own shares; see pathsOverlap's block comment for why.
	// Checked here as well as at the API layer so a hand-edited config.json
	// cannot smuggle it past.
	for i, s := range c.Shares {
		if s.Path == "" {
			continue
		}
		for j, sub := range c.Subscriptions {
			if sub.LocalPath == "" || !pathsOverlap(s.Path, sub.LocalPath) {
				continue
			}
			errs = append(errs, fmt.Errorf(
				"share[%d] (%s) path %q overlaps subscription[%d] local path %q (peer %s, share %s): "+
					"a directory synced from another node cannot be offered back out",
				i, s.Name, s.Path, j, sub.LocalPath, sub.Peer, sub.ShareID))
		}
	}

	return errors.Join(errs...)
}

// The rest of this file is the only place that knows the on-disk encoding
// of Config. Swapping the wire format later (see the deviation note at the
// top of the file) should only require changes here: Marshal/Unmarshal's
// signatures, and the rest of the package's use of them, stay the same.

// --- on-disk encoding --------------------------------------------------

// configDoc mirrors Config for JSON encoding, using SPEC.md §3's
// snake_case field names.
type configDoc struct {
	NodeName              string            `json:"node_name"`
	APIAddr               string            `json:"api_addr"`
	TrashRetentionDays    int               `json:"trash_retention_days"`
	RescanIntervalSeconds int               `json:"rescan_interval_seconds"`
	Debug                 bool              `json:"debug"`
	GlobalIgnores         []string          `json:"global_ignores,omitempty"`
	Peers                 []peerDoc         `json:"peers,omitempty"`
	PendingPeers          []pendingPeerDoc  `json:"pending_peers,omitempty"`
	Shares                []shareDoc        `json:"shares,omitempty"`
	Subscriptions         []subscriptionDoc `json:"subscriptions,omitempty"`
}

// peerDoc mirrors Peer, except Enabled is a pointer so Unmarshal can tell
// "absent from the file" (defaults to true) apart from an explicit false.
type peerDoc struct {
	Name    string `json:"name"`
	Token   string `json:"token"`
	Enabled *bool  `json:"enabled,omitempty"`
}

type pendingPeerDoc struct {
	Name      string    `json:"name"`
	Token     string    `json:"token"`
	FirstSeen time.Time `json:"first_seen"`
	LastSeen  time.Time `json:"last_seen"`
}

type shareDoc struct {
	ID               string            `json:"id"`
	Name             string            `json:"name"`
	Path             string            `json:"path"`
	Permission       string            `json:"permission"`
	ApprovalRequired bool              `json:"approval_required"`
	Access           map[string]string `json:"access,omitempty"`
}

// subscriptionDoc mirrors Subscription 1:1 (no fields need
// pointer-defaulting), so the two convert with a plain type conversion.
type subscriptionDoc struct {
	Peer      string `json:"peer"`
	ShareID   string `json:"share_id"`
	LocalPath string `json:"local_path"`
	Mode      string `json:"mode"`
	Paused    bool   `json:"paused"`
}

// Marshal encodes cfg as indented JSON (config.json stays hand-editable).
func Marshal(cfg *Config) ([]byte, error) {
	doc := configToDoc(cfg)
	data, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("config: marshal: %w", err)
	}
	return data, nil
}

// Unmarshal decodes JSON bytes into a Config, applying defaults (including
// Peer.Enabled defaulting to true when absent).
func Unmarshal(data []byte) (*Config, error) {
	var doc configDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("config: unmarshal: %w", err)
	}
	cfg := docToConfig(&doc)
	cfg.ApplyDefaults()
	return cfg, nil
}

func configToDoc(cfg *Config) *configDoc {
	doc := &configDoc{
		NodeName:              cfg.NodeName,
		APIAddr:               cfg.APIAddr,
		TrashRetentionDays:    cfg.TrashRetentionDays,
		RescanIntervalSeconds: cfg.RescanIntervalSeconds,
		Debug:                 cfg.Debug,
		GlobalIgnores:         cfg.GlobalIgnores,
	}
	for _, p := range cfg.Peers {
		enabled := p.Enabled
		doc.Peers = append(doc.Peers, peerDoc{Name: p.Name, Token: p.Token, Enabled: &enabled})
	}
	for _, p := range cfg.PendingPeers {
		doc.PendingPeers = append(doc.PendingPeers, pendingPeerDoc(p))
	}
	for _, s := range cfg.Shares {
		doc.Shares = append(doc.Shares, shareDoc{
			ID:               s.ID,
			Name:             s.Name,
			Path:             s.Path,
			Permission:       s.Permission,
			ApprovalRequired: s.ApprovalRequired,
			Access:           s.Access,
		})
	}
	for _, sub := range cfg.Subscriptions {
		doc.Subscriptions = append(doc.Subscriptions, subscriptionDoc(sub))
	}
	return doc
}

func docToConfig(doc *configDoc) *Config {
	cfg := &Config{
		NodeName:              doc.NodeName,
		APIAddr:               doc.APIAddr,
		TrashRetentionDays:    doc.TrashRetentionDays,
		RescanIntervalSeconds: doc.RescanIntervalSeconds,
		Debug:                 doc.Debug,
		GlobalIgnores:         doc.GlobalIgnores,
	}
	for _, p := range doc.Peers {
		enabled := true
		if p.Enabled != nil {
			enabled = *p.Enabled
		}
		cfg.Peers = append(cfg.Peers, Peer{Name: p.Name, Token: p.Token, Enabled: enabled})
	}
	for _, p := range doc.PendingPeers {
		cfg.PendingPeers = append(cfg.PendingPeers, PendingPeer(p))
	}
	for _, s := range doc.Shares {
		cfg.Shares = append(cfg.Shares, Share{
			ID:               s.ID,
			Name:             s.Name,
			Path:             s.Path,
			Permission:       s.Permission,
			ApprovalRequired: s.ApprovalRequired,
			Access:           s.Access,
		})
	}
	for _, sub := range doc.Subscriptions {
		cfg.Subscriptions = append(cfg.Subscriptions, Subscription(sub))
	}
	return cfg
}

// --- loading and saving ------------------------------------------------

// Load reads and parses config.json at path, applying defaults for absent
// fields and validating the result. The returned error wraps os.ErrNotExist
// when the file doesn't exist, so callers can use errors.Is.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}

	cfg, err := Unmarshal(data)
	if err != nil {
		return nil, fmt.Errorf("config: parse %s: %w", path, err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config: invalid %s: %w", path, err)
	}
	return cfg, nil
}

// Save validates cfg and atomically writes it to path as JSON (mode 0600).
func Save(path string, cfg *Config) error {
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("config: invalid config: %w", err)
	}

	data, err := Marshal(cfg)
	if err != nil {
		return fmt.Errorf("config: save %s: %w", path, err)
	}
	if err := writeFileAtomic(path, data, 0600); err != nil {
		return fmt.Errorf("config: save %s: %w", path, err)
	}
	return nil
}

// --- share/subscription overlap ----------------------------------------
//
// A directory that this node syncs down from a peer must not also be offered
// back out as one of this node's own shares.
//
// SPEC.md §5 makes fan-out hub-and-spoke: "a share offered to multiple peers
// propagates through the offerer (hub). Peers of the same share do not talk to
// each other in v1." Re-offering a subscribed directory would quietly build a
// chain A -> B -> C out of two distinct share IDs covering the same files on
// B. The index is keyed by (share_id, relpath), so B would track each file
// twice under independent version vectors, and a change arriving on one share
// would look like a fresh local edit to the other — the two shares would
// bounce edits between themselves indefinitely.
//
// The rule is symmetric and covers nesting in both directions, because sharing
// a parent of a subscribed directory re-exports its contents just as surely as
// sharing the directory itself.

// pathContains reports whether child is parent or lies beneath it. The
// comparison is lexical after cleaning, so it is not fooled by "/a/code"
// against "/a/codebase" the way a string-prefix test would be.
//
// It deliberately does no filesystem I/O: Validate runs on every load and save,
// including for paths that do not exist yet. That means a symlink pointing from
// a share into a subscribed tree is not caught here; resolving symlinks belongs
// with the scanner, which already has to walk real directories.
func pathContains(parent, child string) bool {
	rel, err := filepath.Rel(filepath.Clean(parent), filepath.Clean(child))
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}

// pathsOverlap reports whether two directory trees intersect: either path is
// the other, or one contains the other.
func pathsOverlap(a, b string) bool {
	return pathContains(a, b) || pathContains(b, a)
}

// ErrPathOverlap identifies the symmetric share/subscription tree conflict.
var ErrPathOverlap = errors.New("share and subscription paths overlap")

// CheckSharePath reports whether path may be offered as a local share,
// given the subscriptions already in c.
func (c *Config) CheckSharePath(path string) error {
	if !filepath.IsAbs(path) {
		return fmt.Errorf("share path %q must be absolute", path)
	}
	for _, sub := range c.Subscriptions {
		if sub.LocalPath != "" && pathsOverlap(path, sub.LocalPath) {
			return fmt.Errorf(
				"%w: cannot share %q: it overlaps %q, which is synced down from peer %q (share %s); "+
					"a directory received from another node cannot be offered back out",
				ErrPathOverlap, path, sub.LocalPath, sub.Peer, sub.ShareID)
		}
	}
	return nil
}

// CheckSubscriptionPath reports whether localPath may be used as the local
// destination for a subscription, given the shares already in c. It is the
// mirror of CheckSharePath: the rule has to hold whichever side is added
// second.
func (c *Config) CheckSubscriptionPath(localPath string) error {
	if !filepath.IsAbs(localPath) {
		return fmt.Errorf("subscription local path %q must be absolute", localPath)
	}
	for _, s := range c.Shares {
		if s.Path != "" && pathsOverlap(localPath, s.Path) {
			return fmt.Errorf(
				"%w: cannot sync into %q: it overlaps local share %q (%s), which this node offers to peers; "+
					"a directory received from another node cannot be offered back out",
				ErrPathOverlap, localPath, s.Path, s.Name)
		}
	}
	return nil
}

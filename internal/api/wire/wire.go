// Package wire is the REST API's JSON: every request's response shape,
// one Go type per shape. internal/api builds them from internal/core's
// status structs, and the CLI decodes the daemon's responses into the same
// types, so the two cannot drift apart.
//
// It imports nothing from syncat, so a client depends on the shapes and
// nothing else: not on the server, and not on the node behind it (SPEC.md
// §1). Field tags are snake_case, matching the wire protocol's naming
// (SPEC.md §4: share_id, approval_required, ...).
package wire

import "time"

// Status is GET /api/status.
type Status struct {
	// Version is this daemon's build, as version.String() renders it.
	Version string `json:"version"`

	NodeName  string `json:"node_name"`
	NodeToken string `json:"node_token"`
	ShortID   string `json:"short_id"`
	PeerKey   string `json:"peer_key"`

	StartedAt     time.Time `json:"started_at"`
	UptimeSeconds float64   `json:"uptime_seconds"`

	Peers         []Peer               `json:"peers"`
	Shares        []Share              `json:"shares"`
	RemoteShares  []RemoteShare        `json:"remote_shares"`
	Subscriptions []Subscription       `json:"subscriptions"`
	Holds         []Hold               `json:"holds"`
	Rejected      []RejectedConnection `json:"rejected_connections"`
	Network       map[string]any       `json:"network"`
}

type Peer struct {
	ID              string         `json:"id"` // full Ed25519 public key, hex, same as PeerKey
	PeerKey         string         `json:"peer_key"`
	ShortID         string         `json:"short_id"`
	Name            string         `json:"name"`
	RemoteName      string         `json:"remote_name"`
	Enabled         bool           `json:"enabled"`
	State           string         `json:"state"`
	LastError       string         `json:"last_error,omitempty"`
	LastConnectedAt time.Time      `json:"last_connected_at,omitempty"`
	ConnectedSince  time.Time      `json:"connected_since,omitempty"`
	Network         map[string]any `json:"network"`
	// Path is absent until the current connection's network path has been
	// sampled, which is the normal state for a peer that is not connected.
	Path *PeerPath `json:"path,omitempty"`
}

// PeerPath reports how traffic to a peer is travelling. Exactly one of
// Endpoint and Relay is set: a direct path names the peer's address, a
// relayed one names the relay.
type PeerPath struct {
	Direct    bool      `json:"direct"`
	Endpoint  string    `json:"endpoint,omitempty"`
	Relay     string    `json:"relay,omitempty"`
	SampledAt time.Time `json:"sampled_at"`
}

type PendingPeer struct {
	ID        string    `json:"id"`
	PeerKey   string    `json:"peer_key"`
	ShortID   string    `json:"short_id"`
	Name      string    `json:"name"`
	FirstSeen time.Time `json:"first_seen"`
}

type Approval struct {
	ID        string    `json:"id"`
	Kind      string    `json:"kind"`
	PeerKey   string    `json:"peer_key"`
	PeerName  string    `json:"peer_name,omitempty"`
	ShareID   string    `json:"share_id,omitempty"`
	ShareName string    `json:"share_name,omitempty"`
	CreatedAt time.Time `json:"created_at,omitempty"`
}

type ShareAccess struct {
	PeerKey  string `json:"peer_key"`
	PeerName string `json:"peer_name,omitempty"`
	Access   string `json:"access"`
}

type Share struct {
	ID               string        `json:"id"`
	Name             string        `json:"name"`
	Path             string        `json:"path"`
	Permission       string        `json:"permission"`
	ApprovalRequired bool          `json:"approval_required"`
	Access           []ShareAccess `json:"access"`
	Safety           Safety        `json:"safety"`
}

// Safety is a share's or subscription's hold and trash settings, with
// defaults filled in.
type Safety struct {
	HoldDeletePercent  int    `json:"hold_delete_percent"`
	HoldDeleteMin      int    `json:"hold_delete_min"`
	TrashRetentionDays int    `json:"trash_retention_days"`
	TrashMaxSize       string `json:"trash_max_size"`
}

// Hold is one share, or one peer's link to it, on hold. Problem and
// Remedy are the words to show the user, so clients never interpret Reason
// themselves.
type Hold struct {
	ShareID     string    `json:"share_id"`
	ShareName   string    `json:"share_name,omitempty"`
	PeerKey     string    `json:"peer_key,omitempty"` // empty for this node's own copy
	PeerName    string    `json:"peer_name,omitempty"`
	Reason      string    `json:"reason"`
	Problem     string    `json:"problem"` // a clause: "the folder is missing or unreadable"
	Remedy      string    `json:"remedy"`  // a clause: "restore it, or remove the share"
	Deletes     int       `json:"deletes"`
	Live        int       `json:"live"`
	Since       time.Time `json:"since"`
	Confirmed   bool      `json:"confirmed"`
	Confirmable bool      `json:"confirmable"`
}

type RemoteShare struct {
	PeerKey          string `json:"peer_key"`
	PeerName         string `json:"peer_name,omitempty"`
	ShareID          string `json:"share_id"`
	Name             string `json:"name"`
	Permission       string `json:"permission"`
	ApprovalRequired bool   `json:"approval_required"`
	Access           string `json:"access"`
}

type Warning struct {
	ShareID  string    `json:"share_id"`
	RelPath  string    `json:"rel_path"`
	At       time.Time `json:"at"`
	Reverted bool      `json:"reverted"`
	Reason   string    `json:"reason"`
}

type Subscription struct {
	ID        string    `json:"id"`
	PeerKey   string    `json:"peer_key"`
	PeerName  string    `json:"peer_name,omitempty"`
	ShareID   string    `json:"share_id"`
	ShareName string    `json:"share_name,omitempty"`
	LocalPath string    `json:"local_path"`
	Mode      string    `json:"mode"`
	Paused    bool      `json:"paused"`
	Access    string    `json:"access"`
	Connected bool      `json:"connected"`
	Warnings  []Warning `json:"warnings,omitempty"`
	Safety    Safety    `json:"safety"`
}

type RejectedConnection struct {
	PeerKey  string    `json:"peer_key,omitempty"`
	PeerName string    `json:"peer_name,omitempty"`
	At       time.Time `json:"at"`
	Reason   string    `json:"reason"`
}

// Trash is one share's trash: its events, newest first, and its size
// against its limit.
type Trash struct {
	ShareID       string       `json:"share_id"`
	Events        []TrashEvent `json:"events"`
	Size          int64        `json:"size"`
	Limit         int64        `json:"limit"`
	RetentionDays int          `json:"retention_days"`
}

type TrashEvent struct {
	ID     string      `json:"id"`
	At     time.Time   `json:"at"`
	Label  string      `json:"label"`
	Source string      `json:"source,omitempty"`
	Size   int64       `json:"size"`
	Items  []TrashItem `json:"items"`
}

type TrashItem struct {
	RelPath string `json:"rel_path"`
	Kind    string `json:"kind,omitempty"`
	Size    int64  `json:"size"`
}

// PeerList is GET /api/peers.
type PeerList struct {
	Peers        []Peer        `json:"peers"`
	PendingPeers []PendingPeer `json:"pending_peers"`
}

// ApprovalList is GET /api/approvals.
type ApprovalList struct {
	Approvals []Approval `json:"approvals"`
}

// ShareList is GET /api/shares.
type ShareList struct {
	Shares []Share `json:"shares"`
}

// RemoteShareList is GET /api/remote-shares.
type RemoteShareList struct {
	RemoteShares []RemoteShare `json:"remote_shares"`
}

// SubscriptionList is GET /api/subscriptions.
type SubscriptionList struct {
	Subscriptions []Subscription `json:"subscriptions"`
}

// HoldList is GET /api/holds.
type HoldList struct {
	Holds []Hold `json:"holds"`
}

// Restored is POST /api/shares/{id}/trash/undo: the paths put back.
type Restored struct {
	Restored []string `json:"restored"`
}

// Error is every error response.
type Error struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

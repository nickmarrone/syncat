package core

import "time"

// This file defines the read-only status snapshot Node.Status returns —
// the contract Phase 8's REST handlers (GET /api/status, /api/peers,
// /api/shares, /api/remote-shares, /api/subscriptions) marshal to JSON and
// Phase 9's UI renders. Every type here is a plain struct of exported,
// JSON-friendly fields (strings, numbers, bools, times, slices, maps of
// string to plain struct) deliberately with no encoding/json or HTTP
// import anywhere in this package (SPEC.md §12) — Phase 8 owns marshaling.

// ConnState is a peer connection's current state, per SPEC.md §2's dial/
// backoff/handshake flow.
type ConnState string

const (
	// ConnStateDisconnected means no connection attempt is currently in
	// flight or established, and none has ever succeeded (or the peer is
	// disabled).
	ConnStateDisconnected ConnState = "disconnected"
	// ConnStateConnecting means a dial or handshake is currently in
	// progress.
	ConnStateConnecting ConnState = "connecting"
	// ConnStateConnected means an authenticated connection is up (having
	// won SPEC.md §2.4's dedup, if applicable).
	ConnStateConnected ConnState = "connected"
	// ConnStateBackingOff means the last attempt failed and the next is
	// scheduled after transport.Backoff's delay.
	ConnStateBackingOff ConnState = "backing_off"
)

// PeerStatus is one configured peer's connection state, for GET
// /api/peers and the dashboard's peer cards.
type PeerStatus struct {
	// PeerKey is the peer's full Ed25519 public key, hex-encoded — the
	// stable identifier Node's mutation API (RemovePeer, share Access
	// grants, Subscription.Peer) uses throughout, since a Peer's Name is
	// just a display label and its Token can in principle be re-pasted.
	PeerKey string
	// ShortID is the first 8 bytes of PeerKey, hex-encoded — matches
	// config.IdentityKey.ShortID and the version-vector node id this peer
	// appears under in synced files' version vectors.
	ShortID string
	// Name is the locally configured display name for this peer.
	Name string
	// RemoteName is the display name the peer itself reported in its
	// Hello, from the most recent successful handshake. Empty if we have
	// never connected to this peer.
	RemoteName string
	Enabled    bool
	State      ConnState
	// LastError is the most recent dial/handshake failure's message, or
	// empty if the last attempt (if any) succeeded.
	LastError string
	// LastConnectedAt is when a connection to this peer was last
	// established (zero if never).
	LastConnectedAt time.Time
	// ConnectedSince is when the *current* connection (if State is
	// ConnStateConnected) was established; zero otherwise.
	ConnectedSince time.Time
}

// ShareAccessEntry is one peer's access state for a locally offered share.
type ShareAccessEntry struct {
	PeerKey string
	// PeerName is the configured display name for PeerKey, empty if
	// PeerKey isn't (or is no longer) a configured peer.
	PeerName string
	// Access is one of protocol.AccessNone/Pending/Granted/Denied/Revoked.
	Access string
}

// ShareStatus is one locally offered share, for GET /api/shares.
type ShareStatus struct {
	ShareID          string
	Name             string
	Path             string
	Permission       string
	ApprovalRequired bool
	// Access lists every peer with a recorded access decision for this
	// share (config.Share.Access), in no particular order.
	Access []ShareAccessEntry
}

// RemoteShareStatus is one share a peer offers us, for GET
// /api/remote-shares — built from the ShareList messages peers have sent.
type RemoteShareStatus struct {
	PeerKey          string
	PeerName         string
	ShareID          string
	Name             string
	Permission       string
	ApprovalRequired bool
	// Access is our access state to this share, as last reported by the
	// peer (protocol.AccessNone/Pending/Granted/Denied/Revoked).
	Access string
}

// WarningStatus is one recorded anomaly for a subscription: a receive-only
// "locally modified" warning (SPEC.md §1/§5) or — future work, see the
// package doc comment on Node.Status — a genuine conflict-copy event.
// Currently sourced entirely from sync.Session.LocallyModifiedWarnings.
type WarningStatus struct {
	ShareID  string
	RelPath  string
	At       time.Time
	Reverted bool
	Reason   string
}

// SubscriptionStatus is one configured subscription, for GET
// /api/subscriptions.
type SubscriptionStatus struct {
	PeerKey   string
	PeerName  string
	ShareID   string
	ShareName string // the offerer's share name, if known from its ShareList
	LocalPath string
	Mode      string
	Paused    bool
	// Access is our access state to the subscribed share, as last reported
	// by the offerer (protocol.AccessNone/Pending/Granted/Denied/Revoked).
	Access string
	// Connected reports whether the offering peer's session currently has
	// this share actively added (i.e. access was granted and the
	// connection is up).
	Connected bool
	Warnings  []WarningStatus
}

// TransferStatus describes one in-flight file transfer. SPEC.md §8/§9 asks
// for transfer progress in the status surface; internal/sync.Session does
// not yet expose per-transfer byte counters (only aggregate concurrency
// instrumentation used by its own tests), so Node.Status always reports an
// empty Transfers slice today. The field is kept in the snapshot shape so
// Phase 8/9 can render it as soon as Session grows that hook — see the
// package doc comment.
type TransferStatus struct {
	PeerKey          string
	ShareID          string
	RelPath          string
	Direction        string // "pull" or "push"
	BytesTransferred int64
	TotalBytes       int64
	StartedAt        time.Time
}

// RejectedConnection records one inbound connection whose handshake
// authenticated as an Ed25519 key that is not a configured peer (SPEC.md
// §2.3's pending-peer queue is deferred past the MVP — see Node's doc
// comment — but the attempt is still recorded here so Phase 8/9 can
// surface "someone tried to connect").
type RejectedConnection struct {
	PeerKey  string // the unknown key, hex-encoded, if the handshake got that far
	PeerName string // the name they claimed, if any
	At       time.Time
	Reason   string
}

// Status is Node's complete read-only snapshot: node identity, uptime,
// every peer's connection state, our shares and who has access to them,
// what peers offer us, our subscriptions and their sync state, in-flight
// transfers, and recent warnings. Every field is plain, JSON-friendly data
// (SPEC.md §12: no encoding/json import here) — Phase 8 marshals this
// directly for GET /api/status and friends.
type Status struct {
	NodeName  string
	NodeToken string
	ShortID   string
	PeerKey   string // this node's own full Ed25519 public key, hex-encoded

	StartedAt     time.Time
	UptimeSeconds float64

	Peers         []PeerStatus
	Shares        []ShareStatus
	RemoteShares  []RemoteShareStatus
	Subscriptions []SubscriptionStatus
	Transfers     []TransferStatus

	RejectedConnections []RejectedConnection
}

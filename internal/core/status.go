package core

import (
	"time"

	"github.com/nickmarrone/syncat/internal/config"
	"github.com/nickmarrone/syncat/internal/protocol"
)

// This file defines the read-only status snapshot Node.Status returns —
// the contract internal/api's handlers (GET /api/status, /api/peers,
// /api/shares, /api/remote-shares, /api/subscriptions) marshal to JSON and
// the web UI renders. Every type here is a plain struct of exported,
// JSON-friendly fields (strings, numbers, bools, times, slices, maps of
// string to plain struct), deliberately with no encoding/json or HTTP
// import anywhere in this package (SPEC.md §12) — internal/api owns
// marshaling. (ConnState, which PeerStatus.State carries, lives with the
// connection state machine in peer.go.)

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
	Network        PeerNetworkStatus
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

// RejectedConnection records one inbound connection whose handshake did not
// become a session. Valid unknown identities are also retained in the
// standalone approval queue; this bounded history preserves diagnostics.
type RejectedConnection struct {
	PeerKey  string // the unknown key, hex-encoded, if the handshake got that far
	PeerName string // the name they claimed, if any
	At       time.Time
	Reason   string
}

// Status is Node's complete read-only snapshot: node identity, uptime,
// every peer's connection state, our shares and who has access to them,
// what peers offer us, our subscriptions and their sync state, and recent
// warnings. Every field is plain, JSON-friendly data (SPEC.md §12: no
// encoding/json import here) — internal/api marshals this directly for
// GET /api/status and friends.
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

	RejectedConnections []RejectedConnection
	Network             NetworkStatus
}

// Status builds a read-only snapshot of the entire node (see status.go's
// doc comment for the contract). It's safe to call at any time, including
// concurrently with the peer manager's own activity — every field is
// copied out from under its owning lock before being assembled.
func (n *Node) Status() Status {
	n.cfgMu.RLock()
	cfg := n.cfg
	nodeName := cfg.NodeName

	shares := make([]ShareStatus, 0, len(cfg.Shares))
	for _, s := range cfg.Shares {
		access := make([]ShareAccessEntry, 0, len(s.Access))
		for peerKey, a := range s.Access {
			access = append(access, ShareAccessEntry{PeerKey: peerKey, PeerName: peerNameLocked(cfg, peerKey), Access: a})
		}
		shares = append(shares, ShareStatus{
			ShareID: s.ID, Name: s.Name, Path: s.Path, Permission: s.Permission,
			ApprovalRequired: s.ApprovalRequired, Access: access,
		})
	}

	subs := make([]SubscriptionStatus, 0, len(cfg.Subscriptions))
	for _, sub := range cfg.Subscriptions {
		subs = append(subs, SubscriptionStatus{
			PeerKey: sub.Peer, PeerName: peerNameLocked(cfg, sub.Peer),
			ShareID: sub.ShareID, LocalPath: sub.LocalPath, Mode: sub.Mode, Paused: sub.Paused,
		})
	}
	n.cfgMu.RUnlock()

	peerConns := n.snapshotPeers()
	byKey := make(map[string]*peerConn, len(peerConns))
	peerStatuses := make([]PeerStatus, 0, len(peerConns))
	network := NetworkStatus{
		InboundHandshakesActive: len(n.inboundHandshakes),
		RejectedHandshakes:      n.rejectedHandshakes.Load(),
		RejectedOverload:        n.rejectedOverload.Load(),
	}
	var remoteShares []RemoteShareStatus
	for _, pc := range peerConns {
		byKey[pc.peerKeyHex] = pc
		peerStatus := pc.snapshot()
		peerStatuses = append(peerStatuses, peerStatus)
		network.Totals = addPeerNetworkStatus(network.Totals, peerStatus.Network)
		remoteShares = append(remoteShares, pc.remoteShareStatuses()...)
	}

	for i := range subs {
		pc := byKey[subs[i].PeerKey]
		if pc == nil {
			subs[i].Access = protocol.AccessNone
			continue
		}
		access, connected, warnings, shareName := pc.subscriptionState(subs[i].ShareID)
		subs[i].Access = access
		subs[i].Connected = connected
		subs[i].Warnings = warnings
		subs[i].ShareName = shareName
	}

	n.rejectedMu.Lock()
	rejected := append([]RejectedConnection(nil), n.rejected...)
	n.rejectedMu.Unlock()

	now := n.clock.Now()
	return Status{
		NodeName:  nodeName,
		NodeToken: n.localToken(),
		ShortID:   n.identity.ShortID(),
		PeerKey:   n.PeerKey(),

		StartedAt:     n.startTime,
		UptimeSeconds: now.Sub(n.startTime).Seconds(),

		Peers:         peerStatuses,
		Shares:        shares,
		RemoteShares:  remoteShares,
		Subscriptions: subs,

		RejectedConnections: rejected,
		Network:             network,
	}
}

// peerNameLocked returns the configured display name for peerKeyHex, or ""
// if it is not a configured peer. cfg must be read under cfgMu.
func peerNameLocked(cfg *config.Config, peerKeyHex string) string {
	if i := findPeerIndex(cfg, peerKeyHex); i >= 0 {
		return cfg.Peers[i].Name
	}
	return ""
}

func (pc *peerConn) snapshot() PeerStatus {
	pc.mu.Lock()
	status := PeerStatus{
		PeerKey: pc.peerKeyHex, ShortID: pc.peerShort, Name: pc.name, RemoteName: pc.remoteName,
		Enabled: pc.enabled, State: pc.state, LastError: pc.lastErr,
		LastConnectedAt: pc.lastConnectedAt, ConnectedSince: pc.connectedSince,
	}
	counters, totals, sess := pc.network, pc.sessionTotals, pc.session
	pc.mu.Unlock()
	if sess != nil {
		totals = addSessionStats(totals, sess.Stats(), true)
	}
	status.Network = PeerNetworkStatus{
		DialAttempts: counters.dialAttempts, DialFailures: counters.dialFailures,
		TransportFailures: counters.transportFailures, HandshakeFailures: counters.handshakeFailures,
		Backoffs: counters.backoffs, TotalBackoff: time.Duration(counters.backoffNanos), LastBackoff: time.Duration(counters.lastBackoffNanos),
		Connections: counters.connections, Reconnects: counters.reconnects, DedupLosses: counters.dedupLosses,
		PingsSent: counters.pingsSent, PongsReceived: counters.pongsReceived, DeadConnections: counters.deadConnections,
		LastPingRTT: time.Duration(counters.lastPingRTTNanos), Session: toSessionNetworkStatus(totals),
	}
	return status
}

func (pc *peerConn) remoteShareStatuses() []RemoteShareStatus {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	out := make([]RemoteShareStatus, 0, len(pc.remoteShares))
	for _, e := range pc.remoteShares {
		out = append(out, RemoteShareStatus{
			PeerKey: pc.peerKeyHex, PeerName: pc.name, ShareID: e.ShareID, Name: e.Name,
			Permission: e.Permission, ApprovalRequired: e.ApprovalRequired, Access: e.Access,
		})
	}
	return out
}

// subscriptionState reports what this peer connection currently knows
// about one subscribed shareID: our access state as last reported by the
// offerer, whether the share is actively syncing on the live session, its
// display name (from the offerer's ShareList, if seen), and any recorded
// receive-only "locally modified" warnings for it (sourced from
// syncsvc.Session.LocallyModifiedWarnings — see status.go's WarningStatus
// doc comment for what isn't covered yet).
func (pc *peerConn) subscriptionState(shareID string) (access string, connected bool, warnings []WarningStatus, shareName string) {
	pc.mu.Lock()
	access = pc.subAccess[shareID]
	connected = pc.activeShares != nil && pc.activeShares[shareID]
	for _, e := range pc.remoteShares {
		if e.ShareID == shareID {
			shareName = e.Name
			break
		}
	}
	sess := pc.session
	pc.mu.Unlock()

	if access == "" {
		access = protocol.AccessNone
	}
	if sess != nil {
		for _, w := range sess.LocallyModifiedWarnings() {
			if w.ShareID != shareID {
				continue
			}
			warnings = append(warnings, WarningStatus{
				ShareID: w.ShareID, RelPath: w.RelPath, At: w.At, Reverted: w.Reverted, Reason: w.Reason,
			})
		}
	}
	return access, connected, warnings, shareName
}

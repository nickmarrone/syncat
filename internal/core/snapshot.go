package core

import (
	"github.com/nickmarrone/syncat/internal/protocol"
)

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
	var remoteShares []RemoteShareStatus
	for _, pc := range peerConns {
		byKey[pc.peerKeyHex] = pc
		peerStatuses = append(peerStatuses, pc.snapshot())
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
	}
}

func (pc *peerConn) snapshot() PeerStatus {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return PeerStatus{
		PeerKey: pc.peerKeyHex, ShortID: pc.peerShort, Name: pc.name, RemoteName: pc.remoteName,
		Enabled: pc.enabled, State: pc.state, LastError: pc.lastErr,
		LastConnectedAt: pc.lastConnectedAt, ConnectedSince: pc.connectedSince,
	}
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

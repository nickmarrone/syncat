package core

import (
	"context"

	"github.com/nickmarrone/syncat/internal/config"
	"github.com/nickmarrone/syncat/internal/protocol"
	syncsvc "github.com/nickmarrone/syncat/internal/sync"
)

// This file is the share-access negotiation between two nodes (SPEC.md §6):
// the ShareList / SubscribeRequest / AccessUpdate exchange. The offerer
// side grants or denies a peer's request for a share we offer; the
// subscriber side reacts to the offerer's answer; and the announcement
// helpers (re)send our share list and subscription requests. peer.go is
// the per-connection state machine and mutations.go the config-mutation
// API. Both call into here.

// --- offerer side: a peer wants access to a share we offer ----------------

// provisionShareForRequest is SPEC.md §6's grant decision point and the
// synchronous half of handling a SubscribeRequest: it must run on the
// session's read-loop goroutine (called directly from handleControl,
// never from a spawned goroutine), because it establishes sess's local
// share config, via sess.AddShare, before the read loop advances to
// read whatever frame the peer sends next. Once granted, this node itself
// sends an IndexUpdate for this share moments later (finishSubscribeRequest,
// via startSyncing -> Session.SyncShare) on the SAME connection, and if
// the peer's own inbound traffic interleaves with that, Session's read
// loop must already know about this share by the time it dispatches
// anything referencing it, a race that showed up in practice as
// Session logging "index update for unknown share" when the two halves of
// this handling ran on independent, unordered goroutines.
//
// Returns the share's config, the access decision to persist and report,
// and whether a matching share was found at all.
func (n *Node) provisionShareForRequest(pc *peerConn, sess *syncsvc.Session, shareID string) (config.Share, string, bool) {
	n.cfgMu.RLock()
	i := findShareIndex(n.cfg, shareID)
	var share config.Share
	if i >= 0 {
		share = n.cfg.Shares[i]
	}
	n.cfgMu.RUnlock()
	if i < 0 {
		return config.Share{}, "", false // unknown share; no Error code defined for this in SPEC.md §4
	}

	access := share.Access[pc.peerKeyHex]
	if !share.ApprovalRequired {
		access = protocol.AccessGranted
	} else if access == "" || access == protocol.AccessNone {
		access = protocol.AccessPending
	}

	// A pending, denied, or revoked request must not make the share known to
	// the session. A later grant can provision it, on this same open
	// connection, when an operator grants the request.
	if access != protocol.AccessGranted {
		return share, access, true
	}
	pc.grantShare(sess, shareID, share.Path, offeredDirection(share))
	return share, access, true
}

// finishSubscribeRequest is the asynchronous remainder of handling a
// SubscribeRequest: recording the decision and answering the peer. A
// decision that changes the config is answered by that change's effects
// (applyAccess). One already on record, such as a grant renewed on every
// reconnect, changes nothing, so it is answered here, on the session that
// asked; provisionShareForRequest has already granted the share on it.
func (n *Node) finishSubscribeRequest(pc *peerConn, sess *syncsvc.Session, share config.Share, access string) {
	changed := false
	if _, err := n.mutateConfig(func(cfg *config.Config) error {
		i := findShareIndex(cfg, share.ID)
		if i < 0 {
			return mutationErrorf(MutationNotFound, "share %s is not configured", share.ID)
		}
		if cfg.Shares[i].Access[pc.peerKeyHex] != access {
			cfg.Shares[i].Access[pc.peerKeyHex] = access
			changed = true
		}
		return nil
	}); err != nil {
		n.logger.Printf("core: set %s access to %s for %s: %v", share.ID, access, pc.peerKeyHex, err)
		return
	}
	if changed {
		return
	}
	n.sendAccessUpdate(sess, pc.peerKeyHex, share.ID, access)
	if access == protocol.AccessGranted {
		n.startSyncing(n.ctx, sess, share.ID)
	}
}

// --- subscriber side: the offerer answered our SubscribeRequest -----------

// provisionAccessUpdate is AccessUpdate's synchronous half, see
// provisionShareForRequest's doc comment for why sess.AddShare must run
// here, on the read-loop goroutine, rather than in a spawned goroutine:
// the offerer sends its own IndexUpdate for this share immediately after
// granting, and this session must already know the share by the time
// that frame (or any later one) is dispatched.
//
// Returns whether there's asynchronous follow-up work to do (the initial
// SyncShare and local rescan, only for a fresh, unpaused grant).
func (n *Node) provisionAccessUpdate(pc *peerConn, sess *syncsvc.Session, msg protocol.AccessUpdate) bool {
	n.cfgMu.RLock()
	var subCopy config.Subscription
	found := false
	for _, s := range n.cfg.Subscriptions {
		if s.Peer == pc.peerKeyHex && s.ShareID == msg.ShareID {
			subCopy, found = s, true
			break
		}
	}
	n.cfgMu.RUnlock()
	if !found {
		return false
	}

	pc.setSubscriptionAccess(msg.ShareID, msg.Access)

	switch msg.Access {
	case protocol.AccessGranted:
		if subCopy.Paused {
			return false
		}
		pc.grantShare(sess, msg.ShareID, subCopy.LocalPath, subscriberDirection(pc.offeredPermission(msg.ShareID), subCopy.Mode))
		return true

	case protocol.AccessDenied, protocol.AccessRevoked:
		pc.neuterShare(msg.ShareID)
	}
	return false
}

// subscriberDirection is the Direction a subscription syncs in: receive-only
// when the subscriber chose that mode, and also whenever the offerer's share
// is read-only, whatever mode was asked for (SPEC.md §5, §6). permission is
// the offerer's permission for the share, "" if not (yet) known.
func subscriberDirection(permission, mode string) syncsvc.Direction {
	dir := syncsvc.DirectionFor("", mode)
	if permission == config.PermissionReadOnly {
		dir.OutboundBlocked = true
	}
	return dir
}

// refreshSubscriptionDirections re-derives the direction of every active
// subscription to pc's shares after a ShareList, which is how the offerer
// announces a permission change on a live connection. A subscription that
// stops being receive-only starts sending its index again.
func (n *Node) refreshSubscriptionDirections(ctx context.Context, pc *peerConn, sess *syncsvc.Session) {
	n.cfgMu.RLock()
	var subs []config.Subscription
	for _, s := range n.cfg.Subscriptions {
		if s.Peer == pc.peerKeyHex && !s.Paused {
			subs = append(subs, s)
		}
	}
	n.cfgMu.RUnlock()

	for _, s := range subs {
		old, known := sess.ShareDirection(s.ShareID)
		dir := subscriberDirection(pc.offeredPermission(s.ShareID), s.Mode)
		if !known || old == dir || pc.activeSession(s.ShareID) != sess {
			continue
		}
		pc.reconfigureShare(s.ShareID, s.LocalPath, dir)
		if old.OutboundBlocked && !dir.OutboundBlocked {
			shareID := s.ShareID
			n.goTracked(func() { n.startSyncing(ctx, sess, shareID) })
		}
	}
}

// finishAccessUpdate is AccessUpdate's asynchronous half: the initial
// full sync in both directions. rescanShare picks up any pre-existing local
// content in the subscription directory, and startSyncing pushes our index
// and pulls whatever we already owe ourselves from the offerer's.
func (n *Node) finishAccessUpdate(ctx context.Context, sess *syncsvc.Session, shareID string) {
	if err := n.rescanShare(n.ctx, shareID); err != nil {
		n.logger.Printf("core: initial local scan for subscription %s: %v", shareID, err)
	}
	n.startSyncing(ctx, sess, shareID)
}

// --- share list / subscribe-request announcements --------------------------

func (n *Node) sendShareList(sess *syncsvc.Session, peerKeyHex string) {
	entries := n.buildShareList(peerKeyHex)
	if err := sess.Writer().WriteLatestMessage("share-list", protocol.MsgShareList, protocol.ShareList{Shares: entries}); err != nil {
		n.logger.Printf("core: send share list to %s: %v", peerKeyHex, err)
	}
}

func (n *Node) buildShareList(peerKeyHex string) []protocol.ShareListEntry {
	n.cfgMu.RLock()
	defer n.cfgMu.RUnlock()
	entries := make([]protocol.ShareListEntry, 0, len(n.cfg.Shares))
	for _, s := range n.cfg.Shares {
		access := s.Access[peerKeyHex]
		if access == "" {
			access = protocol.AccessNone
		}
		entries = append(entries, protocol.ShareListEntry{
			ShareID: s.ID, Name: s.Name, Permission: s.Permission,
			ApprovalRequired: s.ApprovalRequired, Access: access,
		})
	}
	return entries
}

func (n *Node) requestSubscriptions(sess *syncsvc.Session, peerKeyHex string) {
	n.cfgMu.RLock()
	var toRequest []string
	for _, sub := range n.cfg.Subscriptions {
		if sub.Peer == peerKeyHex && !sub.Paused {
			toRequest = append(toRequest, sub.ShareID)
		}
	}
	n.cfgMu.RUnlock()
	for _, shareID := range toRequest {
		n.sendSubscribeRequest(sess, peerKeyHex, shareID)
	}
}

func (n *Node) sendSubscribeRequest(sess *syncsvc.Session, peerKeyHex, shareID string) {
	if err := sess.Writer().WriteMessage(protocol.MsgSubscribeRequest, protocol.SubscribeRequest{ShareID: shareID}); err != nil {
		n.logger.Printf("core: send subscribe request %s to %s: %v", shareID, peerKeyHex, err)
	}
}

func (n *Node) sendAccessUpdate(sess *syncsvc.Session, peerKeyHex, shareID, access string) {
	if err := sess.Writer().WriteLatestMessage("access-update\x00"+shareID, protocol.MsgAccessUpdate, protocol.AccessUpdate{ShareID: shareID, Access: access}); err != nil {
		n.logger.Printf("core: send access update for %s to %s: %v", shareID, peerKeyHex, err)
	}
}

// broadcastShareList re-announces our share list to every currently
// connected peer (SPEC.md §4: ShareList is sent "on connect + on
// change").
func (n *Node) broadcastShareList() {
	for _, pc := range n.snapshotPeers() {
		if sess := pc.currentSession(); sess != nil {
			n.sendShareList(sess, pc.peerKeyHex)
		}
	}
}

// neuterShareOnSessions blocks a share on every currently-connected
// session and stops counting it as active for propagation, see
// peerConn.neuterShare for what that means and why it is safe.
func (n *Node) neuterShareOnSessions(shareID string) {
	for _, pc := range n.snapshotPeers() {
		pc.neuterShare(shareID)
	}
}

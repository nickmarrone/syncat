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
// API — both call into here.

// --- offerer side: a peer wants access to a share we offer ----------------

// provisionShareForRequest is SPEC.md §6's grant decision point (the MVP
// auto-grants unconditionally — see the package doc comment) and the
// synchronous half of handling a SubscribeRequest: it must run on the
// session's read-loop goroutine (called directly from handleControl,
// never from a spawned goroutine), because it establishes sess's local
// share config — via sess.AddShare — before the read loop advances to
// read whatever frame the peer sends next. Once granted, this node itself
// sends an IndexUpdate for this share moments later (finishSubscribeRequest,
// via SetShareAccess -> Session.SyncShare) on the SAME connection, and if
// the peer's own inbound traffic interleaves with that, Session's read
// loop must already know about this share by the time it dispatches
// anything referencing it — a race that showed up in practice as
// Session logging "index update for unknown share" when the two halves of
// this handling ran on independent, unordered goroutines.
//
// Returns the share's config (for finishSubscribeRequest) and whether a
// matching share was found at all.
func (n *Node) provisionShareForRequest(pc *peerConn, sess *syncsvc.Session, shareID string) (config.Share, bool) {
	n.cfgMu.RLock()
	i := findShareIndex(n.cfg, shareID)
	var share config.Share
	if i >= 0 {
		share = n.cfg.Shares[i]
	}
	n.cfgMu.RUnlock()
	if i < 0 {
		return config.Share{}, false // unknown share; no Error code defined for this in SPEC.md §4
	}

	// TODO(approvals): SPEC.md §6 says a share with ApprovalRequired=true
	// should instead queue this for UI/CLI approval and only grant (and
	// only then provision the session) on an explicit decision. The MVP
	// auto-grants regardless — see the package doc comment — so the
	// share's ApprovalRequired flag is intentionally unused here; it's
	// still persisted in config for that future queue. Enforcing it will
	// need to reconcile with this function's "must run synchronously"
	// requirement above, since an approval decision can't be synchronous
	// with an inbound frame that arrived before a human ever acts on it.
	sess.AddShare(syncsvc.ShareConfig{ShareID: shareID, Root: share.Path, Direction: syncsvc.DirectionFor(share.Permission, ""), Ignore: n.shareIgnoreFunc(shareID)})
	pc.markShareActive(shareID)
	return share, true
}

// finishSubscribeRequest is the asynchronous remainder of handling a
// SubscribeRequest: persisting the grant and notifying the peer
// (SetShareAccess also re-applies the same sess.AddShare provisionShareForRequest
// already did — a harmless idempotent overwrite — since SetShareAccess is
// also the REST API's direct entry point and shouldn't have a
// provision-already-done special case).
func (n *Node) finishSubscribeRequest(pc *peerConn, share config.Share) {
	if err := n.SetShareAccess(share.ID, pc.peerKeyHex, protocol.AccessGranted); err != nil {
		n.logger.Printf("core: grant %s to %s: %v", share.ID, pc.peerKeyHex, err)
	}
}

// --- subscriber side: the offerer answered our SubscribeRequest -----------

// provisionAccessUpdate is AccessUpdate's synchronous half — see
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
		direction := syncsvc.DirectionFor("", subCopy.Mode)
		sess.AddShare(syncsvc.ShareConfig{ShareID: msg.ShareID, Root: subCopy.LocalPath, Direction: direction, Ignore: n.shareIgnoreFunc(msg.ShareID)})
		pc.markShareActive(msg.ShareID)
		return true

	case protocol.AccessDenied, protocol.AccessRevoked:
		pc.neuterShare(msg.ShareID)
	}
	return false
}

// finishAccessUpdate is AccessUpdate's asynchronous half: the initial
// full sync in both directions (SyncShare pushes our current state;
// rescanShare picks up any pre-existing local content in the subscription
// directory and pushes that too).
func (n *Node) finishAccessUpdate(ctx context.Context, sess *syncsvc.Session, shareID string) {
	if err := sess.SyncShare(ctx, shareID); err != nil {
		n.logger.Printf("core: initial sync for subscription %s: %v", shareID, err)
	}
	if err := n.rescanShare(n.ctx, shareID); err != nil {
		n.logger.Printf("core: initial local scan for subscription %s: %v", shareID, err)
	}
	// Neither of the two above notices work we already owed this peer. They
	// push our state outwards -- our index to the peer, our directory into
	// our index -- and the reconciler that decides what to *pull* only runs
	// when the peer sends rows. After a reconnect the peer usually sends
	// none: its index sync is incremental, and its cursor says we already
	// have everything it has. Anything a previous session left unfinished --
	// a pull interrupted by the disconnect, most obviously -- is then
	// invisible to both sides. See Session.ReconcileShare.
	if err := sess.ReconcileShare(ctx, shareID); err != nil {
		n.logger.Printf("core: reconcile subscription %s on connect: %v", shareID, err)
	}
}

// --- share list / subscribe-request announcements --------------------------

func (n *Node) sendShareList(sess *syncsvc.Session, peerKeyHex string) {
	entries := n.buildShareList(peerKeyHex)
	if err := sess.Writer().WriteMessage(protocol.MsgShareList, protocol.ShareList{Shares: entries}); err != nil {
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
		if err := sess.Writer().WriteMessage(protocol.MsgSubscribeRequest, protocol.SubscribeRequest{ShareID: shareID}); err != nil {
			n.logger.Printf("core: send subscribe request %s to %s: %v", shareID, peerKeyHex, err)
		}
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
// session and stops counting it as active for propagation — see
// peerConn.neuterShare for what that means and why it is safe.
func (n *Node) neuterShareOnSessions(shareID string) {
	for _, pc := range n.snapshotPeers() {
		pc.neuterShare(shareID)
	}
}

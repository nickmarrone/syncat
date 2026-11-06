package core

import (
	"context"
	"encoding/hex"
	"maps"
	"slices"

	"github.com/nickmarrone/syncat/internal/config"
	"github.com/nickmarrone/syncat/internal/protocol"
	syncsvc "github.com/nickmarrone/syncat/internal/sync"
)

// This file is the one place a config change reaches the running node.
// Mutations (mutations.go, approvals.go) only edit the config; mutateConfig
// then hands the old and new config here, diffConfig works out what changed,
// and applyConfigChange makes the watchers, peer connections, sessions,
// announcements, and holds match. A mutation cannot forget an effect,
// because it never lists them.
//
// Startup does not come through here (see Open): it has jobs of its own,
// such as sweeping interrupted transfers, and it runs before peers can
// connect.

// configChange is what one config change means for the running node.
// diffConfig computes it from the configs alone, so it can be tested
// without a Node.
type configChange struct {
	peersAdded   []config.Peer
	peersRemoved []string // peer keys

	sharesAdded      []config.Share
	sharesRemoved    []string       // share ids
	sharesRedirected []config.Share // offered shares whose permission changed

	// access is every peer whose recorded access to a share that is still
	// offered changed. A peer removed with its access entries is not here:
	// its connection goes, and with it everything access would change.
	access []accessChange

	subsStarted []config.Subscription // added, or resumed
	subsStopped []config.Subscription // removed, or paused

	// subsRemoved is the subscriptions gone from the config altogether
	// (paused ones are not: pausing keeps the local index state). Their
	// index state is forgotten, see Store.ForgetSubscription.
	subsRemoved []config.Subscription

	// announce is set when the share list every peer sees changed: a share
	// added or removed, or its name, permission, or approval setting.
	announce bool

	nodeRenamed bool
}

type accessChange struct {
	share   config.Share // as it is in the new config
	peerKey string
	access  string
}

// diffConfig returns what changed between old and new. Its lists follow
// new's order (old's for removals), with access changes in peer-key order,
// so the effects run in a stable order.
func diffConfig(old, new *config.Config) configChange {
	var c configChange

	oldPeers := peerKeys(old)
	newPeers := peerKeys(new)
	for _, p := range new.Peers {
		if key, ok := peerKeyOf(p); ok && !oldPeers[key] {
			c.peersAdded = append(c.peersAdded, p)
		}
	}
	for _, p := range old.Peers {
		if key, ok := peerKeyOf(p); ok && !newPeers[key] {
			c.peersRemoved = append(c.peersRemoved, key)
		}
	}

	oldShares := make(map[string]config.Share, len(old.Shares))
	for _, s := range old.Shares {
		oldShares[s.ID] = s
	}
	newShares := make(map[string]bool, len(new.Shares))
	for _, s := range new.Shares {
		newShares[s.ID] = true
		was, existed := oldShares[s.ID]
		if !existed {
			c.sharesAdded = append(c.sharesAdded, s)
			c.announce = true
			continue
		}
		if s.Permission != was.Permission {
			c.sharesRedirected = append(c.sharesRedirected, s)
		}
		if s.Name != was.Name || s.Permission != was.Permission || s.ApprovalRequired != was.ApprovalRequired {
			c.announce = true
		}
		for _, key := range slices.Sorted(maps.Keys(s.Access)) {
			if a := s.Access[key]; a != was.Access[key] {
				c.access = append(c.access, accessChange{share: s, peerKey: key, access: a})
			}
		}
	}
	for _, s := range old.Shares {
		if !newShares[s.ID] {
			c.sharesRemoved = append(c.sharesRemoved, s.ID)
			c.announce = true
		}
	}

	type subKey struct{ peer, share string }
	running := func(cfg *config.Config) map[subKey]bool {
		m := make(map[subKey]bool, len(cfg.Subscriptions))
		for _, s := range cfg.Subscriptions {
			if !s.Paused {
				m[subKey{s.Peer, s.ShareID}] = true
			}
		}
		return m
	}
	oldRunning, newRunning := running(old), running(new)
	for _, s := range new.Subscriptions {
		if k := (subKey{s.Peer, s.ShareID}); newRunning[k] && !oldRunning[k] {
			c.subsStarted = append(c.subsStarted, s)
		}
	}
	for _, s := range old.Subscriptions {
		if k := (subKey{s.Peer, s.ShareID}); oldRunning[k] && !newRunning[k] {
			c.subsStopped = append(c.subsStopped, s)
		}
	}

	for _, s := range old.Subscriptions {
		if findSubscriptionIndex(new, s.Peer, s.ShareID) < 0 {
			c.subsRemoved = append(c.subsRemoved, s)
		}
	}

	c.nodeRenamed = new.NodeName != old.NodeName
	return c
}

func peerKeyOf(p config.Peer) (string, bool) {
	tok, err := config.ParseToken(p.Token)
	if err != nil {
		return "", false
	}
	return hex.EncodeToString(tok.ID), true
}

func peerKeys(cfg *config.Config) map[string]bool {
	m := make(map[string]bool, len(cfg.Peers))
	for _, p := range cfg.Peers {
		if key, ok := peerKeyOf(p); ok {
			m[key] = true
		}
	}
	return m
}

// applyConfigChange makes the running node match new, which replaced old.
// mutateConfig calls it after the swap, with cfgMu released (effects take
// peerConn locks, which must never be acquired under cfgMu) and mutateMu
// held, so changes take effect in the order they were made.
//
// Things stop before things start, so a share moving between peers or
// paths never runs twice, and announcements go last, once the sessions
// they describe are set up.
func (n *Node) applyConfigChange(old, new *config.Config) {
	c := diffConfig(old, new)

	for _, key := range c.peersRemoved {
		n.peersMu.Lock()
		pc := n.peers[key]
		delete(n.peers, key)
		n.peersMu.Unlock()
		if pc != nil {
			pc.cancel()
		}
	}
	for _, s := range c.subsStopped {
		n.stopShareWatch(s.ShareID)
		if pc := n.lookupPeer(s.Peer); pc != nil {
			pc.neuterShare(s.ShareID)
		}
	}
	for _, id := range c.sharesRemoved {
		n.stopShareWatch(id)
		n.neuterShareOnSessions(id)
	}
	// A removed subscription forgets its index state, so subscribing again
	// (into any folder) starts like a first subscription instead of reading
	// the new folder as the old copy with every file deleted.
	for _, s := range c.subsRemoved {
		// A reconcile pass that was mid-flight when the subscription went
		// would otherwise write rows for the old folder after they are
		// forgotten, and the next folder would inherit them.
		if pc := n.lookupPeer(s.Peer); pc != nil {
			pc.waitShareIdle()
		}
		keepFiles := findShareIndex(new, s.ShareID) >= 0 || subscribesToShare(new, s.ShareID)
		if err := n.store.ForgetSubscription(n.ctx, s.Peer, s.ShareID, !keepFiles); err != nil {
			n.logger.Printf("core: forget subscription to share %s: %v", s.ShareID, err)
		}
	}
	// Whatever the change removed can never be scanned or synced again, so
	// a hold on it could never clear.
	n.holds.Retain(configuredLinks(new))

	for _, s := range c.sharesAdded {
		if _, err := n.startShareWatch(s.ID, s.Path); err != nil {
			n.logger.Printf("core: start watcher for share %s: %v", s.ID, err)
		} else if err := n.rescanShare(n.ctx, s.ID); err != nil {
			n.logger.Printf("core: initial scan for share %s: %v", s.ID, err)
		}
	}
	for _, s := range c.sharesRedirected {
		for _, pc := range n.snapshotPeers() {
			pc.reconfigureShare(s.ID, s.Path, offeredDirection(s))
		}
	}
	for _, p := range c.peersAdded {
		if err := n.activatePeer(p); err != nil {
			n.logger.Printf("core: start peer %q: %v", p.Name, err)
		}
	}
	for _, s := range c.subsStarted {
		if _, err := n.startShareWatch(s.ShareID, s.LocalPath); err != nil {
			n.logger.Printf("core: start watcher for subscription %s: %v", s.ShareID, err)
		}
		if pc := n.lookupPeer(s.Peer); pc != nil {
			if sess := pc.currentSession(); sess != nil {
				n.sendSubscribeRequest(sess, s.Peer, s.ShareID)
			}
		}
	}
	for _, a := range c.access {
		n.applyAccess(a, !c.announce)
	}
	if c.announce {
		n.broadcastShareList()
	}
	if c.nodeRenamed {
		n.refreshToken(new.NodeName)
	}
}

// applyAccess carries a change to one peer's access to a share onto that
// peer's live session, if it has one. sendList says whether to send the
// peer its new share list, which an access change alters; a caller about
// to send every peer theirs passes false.
func (n *Node) applyAccess(a accessChange, sendList bool) {
	pc := n.lookupPeer(a.peerKey)
	if pc == nil {
		return
	}
	sess := pc.currentSession()
	if sess == nil {
		return
	}
	// Apply authorization locally before reporting it remotely. In
	// particular, a grant must make the share available before the peer can
	// react to AccessUpdate, while every non-grant must fail closed first.
	if a.access == protocol.AccessGranted {
		pc.grantShare(sess, a.share.ID, a.share.Path, offeredDirection(a.share))
	} else {
		pc.neuterShare(a.share.ID)
	}
	n.sendAccessUpdate(sess, a.peerKey, a.share.ID, a.access)
	// An access decision changes what this peer's ShareList looks like, and
	// SPEC.md §4 says ShareList is sent on connect *and on change*. The
	// AccessUpdate above doesn't cover it: the subscriber files that against
	// a subscription it already has, while the access it shows for an
	// offered share comes from the ShareList and nowhere else. Without this,
	// a share we just granted keeps reading as "no access" on the peer's
	// side, and a grant made before the peer ever subscribes reaches it
	// with nothing at all until the next reconnect.
	if sendList {
		n.sendShareList(sess, a.peerKey)
	}
	if a.access == protocol.AccessGranted {
		n.startSyncing(n.ctx, sess, a.share.ID)
	}
}

// startSyncing brings a share that just became active on sess up to date
// in both directions: SyncShare pushes this node's index to the peer, and
// ReconcileShare pulls whatever this node already owes itself from the
// peer's. The second is not redundant. The reconciler otherwise runs only
// when the peer sends rows, and after a reconnect the peer usually sends
// none: its index sync is incremental, and its cursor says this node
// already has everything it has. Anything a previous session left
// unfinished, a pull interrupted by the disconnect most obviously, would
// then be invisible to both sides.
func (n *Node) startSyncing(ctx context.Context, sess *syncsvc.Session, shareID string) {
	if err := sess.SyncShare(ctx, shareID); err != nil {
		n.logger.Printf("core: sync share %s: %v", shareID, err)
	}
	if err := sess.ReconcileShare(ctx, shareID); err != nil {
		n.logger.Printf("core: reconcile share %s: %v", shareID, err)
	}
}

// offeredDirection is the Direction a share this node offers syncs in on
// a peer's session.
func offeredDirection(s config.Share) syncsvc.Direction {
	return syncsvc.DirectionFor(s.Permission, "")
}

// refreshToken rebuilds the cached sc1 token, which embeds the node's
// name as a "suggested display name" (SPEC.md §2).
func (n *Node) refreshToken(name string) {
	addr, err := n.transport.LocalAddress()
	if err != nil {
		return
	}
	tok, err := config.EncodeToken(addr, n.identity.Public(), name)
	if err != nil {
		return
	}
	n.tokenMu.Lock()
	n.token = tok
	n.tokenMu.Unlock()
}

// subscribesToShare reports whether any subscription in cfg is to shareID.
func subscribesToShare(cfg *config.Config, shareID string) bool {
	for _, s := range cfg.Subscriptions {
		if s.ShareID == shareID {
			return true
		}
	}
	return false
}

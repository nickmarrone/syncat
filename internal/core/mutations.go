package core

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"

	"github.com/nickmarrone/syncat/internal/config"
	"github.com/nickmarrone/syncat/internal/index"
	"github.com/nickmarrone/syncat/internal/protocol"
	syncsvc "github.com/nickmarrone/syncat/internal/sync"
)

// This file is Node's config-mutation API: everything the REST layer
// (internal/api) and the CLI call to change what this node peers with,
// offers, and subscribes to. Every entry point resolves its human-typed
// references (see resolve.go), mutates a clone of the config under cfgMu,
// persists it, and only then applies the live effects — watchers started
// or stopped, sessions re-synced — with cfgMu released.
//
// Lock ordering: config mutations always clone-mutate-swap under cfgMu;
// live effects (which take peerConn.mu) are applied after cfgMu is
// released, so cfgMu is never held while acquiring a peerConn's lock.

// --- config mutation helpers ---------------------------------------------

// cloneConfig returns a deep-enough copy of cfg for mutateConfig's
// clone-mutate-swap pattern: every slice/map mutation methods touch is
// copied, so a failed mutation (validation or save error) never leaves
// n.cfg's live, in-use structures partially edited.
func cloneConfig(cfg *config.Config) *config.Config {
	out := *cfg
	out.GlobalIgnores = append([]string(nil), cfg.GlobalIgnores...)
	out.Peers = append([]config.Peer(nil), cfg.Peers...)
	out.Shares = make([]config.Share, len(cfg.Shares))
	for i, s := range cfg.Shares {
		out.Shares[i] = s
		out.Shares[i].Access = make(map[string]string, len(s.Access))
		for k, v := range s.Access {
			out.Shares[i].Access[k] = v
		}
	}
	out.Subscriptions = append([]config.Subscription(nil), cfg.Subscriptions...)
	return &out
}

// mutateConfig applies fn to a clone of the current config, persists the
// clone (config.Save, which also validates), and — only on success —
// swaps it in as the live config. Callers apply live effects (starting/
// stopping watchers and sessions) after mutateConfig returns successfully,
// outside cfgMu, matching every mutation method in this file.
func (n *Node) mutateConfig(fn func(cfg *config.Config) error) (*config.Config, error) {
	n.cfgMu.Lock()
	defer n.cfgMu.Unlock()
	newCfg := cloneConfig(n.cfg)
	if err := fn(newCfg); err != nil {
		return nil, err
	}
	if err := config.Save(n.paths.ConfigFile(), newCfg); err != nil {
		return nil, fmt.Errorf("save config: %w", err)
	}
	n.cfg = newCfg
	return newCfg, nil
}

func findPeerIndex(cfg *config.Config, peerKeyHex string) int {
	for i, p := range cfg.Peers {
		if pt, err := config.ParseToken(p.Token); err == nil && hex.EncodeToString(pt.ID) == peerKeyHex {
			return i
		}
	}
	return -1
}

func findShareIndex(cfg *config.Config, shareID string) int {
	for i, s := range cfg.Shares {
		if s.ID == shareID {
			return i
		}
	}
	return -1
}

func findSubscriptionIndex(cfg *config.Config, peerKeyHex, shareID string) int {
	for i, s := range cfg.Subscriptions {
		if s.Peer == peerKeyHex && s.ShareID == shareID {
			return i
		}
	}
	return -1
}

// --- peer mutations --------------------------------------------------------

// AddPeer adds a peer by its pasted sc1 token (SPEC.md §2), persists it,
// and starts dialing it immediately. It returns the peer's canonical
// identifier (its full Ed25519 public key, hex-encoded) — every other
// peer-scoped method in this package's API takes that same identifier.
func (n *Node) AddPeer(name, token string) (string, error) {
	tok, err := config.ParseToken(token)
	if err != nil {
		return "", fmt.Errorf("core: add peer: %w", err)
	}
	peerKeyHex := hex.EncodeToString(tok.ID)
	if peerKeyHex == n.PeerKey() {
		return "", fmt.Errorf("core: add peer: this is this node's own token — paste the token printed by `syncat token` on the *other* node")
	}
	if name == "" {
		name = tok.Name
	}

	p := config.Peer{Name: name, Token: token, Enabled: true}
	if _, err := n.mutateConfig(func(cfg *config.Config) error {
		if findPeerIndex(cfg, peerKeyHex) >= 0 {
			return fmt.Errorf("peer %s is already configured", peerKeyHex)
		}
		cfg.Peers = append(cfg.Peers, p)
		return nil
	}); err != nil {
		return "", fmt.Errorf("core: add peer: %w", err)
	}

	pc, err := newPeerConn(n, p)
	if err != nil {
		return "", fmt.Errorf("core: add peer: %w", err)
	}
	n.peersMu.Lock()
	n.peers[peerKeyHex] = pc
	n.peersMu.Unlock()
	n.goTracked(pc.runSupervisor)
	n.debugf("core: added peer %q (%s)", name, pc.peerShort)
	return peerKeyHex, nil
}

// RemovePeer removes a configured peer and every trace of it from the rest
// of the config: subscriptions to its shares, and its entries in our own
// shares' access lists. It also closes any active connection to it.
//
// The subscriptions go because a subscription names the peer that offers
// the share (config.Subscription.Peer): once that peer is gone there is no
// node left to sync it with, so leaving it configured would keep a watcher
// running and keep it listed by `syncat status` forever, permanently
// stuck. Removal is config-only, matching RemoveSubscription — the local
// copy of the files stays on disk, it just stops being synced.
//
// The access entries go because config.Share.Access is keyed by the peer's
// Ed25519 public key, and buildShareList reads that map directly with no
// reference to whether the key still belongs to a configured peer. A
// leftover "granted" is therefore not just clutter: re-adding that same
// key later — the obvious thing to do after removing a peer by mistake, or
// to re-pair after one side is rebuilt — would silently restore its
// previous access to every share, with no new approval and nothing shown
// to the user. Removing a peer should mean it starts from nothing if it
// ever comes back.
func (n *Node) RemovePeer(peerRef string) error {
	// Best-effort, matching RemoveSubscription: an unresolvable ref falls
	// through to findPeerIndex's own "is not configured" error rather than
	// being rejected here.
	peerKeyHex := peerRef
	if pc, err := n.resolvePeerRef(peerRef); err == nil {
		peerKeyHex = pc.peerKeyHex
	}
	var droppedSubs []config.Subscription
	var droppedAccess []string // share ids we revoked this peer's access to
	if _, err := n.mutateConfig(func(cfg *config.Config) error {
		i := findPeerIndex(cfg, peerKeyHex)
		if i < 0 {
			return fmt.Errorf("peer %s is not configured", peerKeyHex)
		}
		cfg.Peers = append(cfg.Peers[:i], cfg.Peers[i+1:]...)

		kept := make([]config.Subscription, 0, len(cfg.Subscriptions))
		for _, s := range cfg.Subscriptions {
			if s.Peer == peerKeyHex {
				droppedSubs = append(droppedSubs, s)
				continue
			}
			kept = append(kept, s)
		}
		cfg.Subscriptions = kept

		// Safe to mutate in place: mutateConfig hands us a clone whose
		// Access maps are themselves freshly built (see cloneConfig).
		for j := range cfg.Shares {
			if _, ok := cfg.Shares[j].Access[peerKeyHex]; !ok {
				continue
			}
			delete(cfg.Shares[j].Access, peerKeyHex)
			droppedAccess = append(droppedAccess, cfg.Shares[j].ID)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("core: remove peer: %w", err)
	}

	// No need to neuter these on the peer's live session the way
	// RemoveSubscription does: cancelling pc below tears the whole session
	// down anyway.
	for _, s := range droppedSubs {
		n.stopShareWatch(s.ShareID)
		n.logger.Printf("core: remove peer %s: dropped subscription to share %s (local copy left at %s)", peerKeyHex, s.ShareID, s.LocalPath)
	}
	for _, shareID := range droppedAccess {
		n.logger.Printf("core: remove peer %s: revoked its access to share %s", peerKeyHex, shareID)
	}

	n.peersMu.Lock()
	pc := n.peers[peerKeyHex]
	delete(n.peers, peerKeyHex)
	n.peersMu.Unlock()
	if pc != nil {
		pc.cancel()
	}
	n.debugf("core: removed peer %s (dropped %d subscription(s), revoked access to %d share(s))", peerKeyHex, len(droppedSubs), len(droppedAccess))
	return nil
}

// --- share mutations --------------------------------------------------------

// AddShare offers a new local directory as a share (SPEC.md §3/§6):
// validates path (including the CheckSharePath overlap rule), generates a
// share id, persists it, starts watching it, and announces it to every
// connected peer.
func (n *Node) AddShare(path, name, permission string, approvalRequired bool) (string, error) {
	if permission != config.PermissionReadOnly && permission != config.PermissionReadWrite {
		return "", fmt.Errorf("core: add share: invalid permission %q", permission)
	}
	id, err := config.NewShareID()
	if err != nil {
		return "", fmt.Errorf("core: add share: %w", err)
	}

	if _, err := n.mutateConfig(func(cfg *config.Config) error {
		if err := cfg.CheckSharePath(path); err != nil {
			return err
		}
		cfg.Shares = append(cfg.Shares, config.Share{
			ID: id, Name: name, Path: path, Permission: permission,
			ApprovalRequired: approvalRequired, Access: map[string]string{},
		})
		return nil
	}); err != nil {
		return "", fmt.Errorf("core: add share: %w", err)
	}

	if _, err := n.startShareWatch(id, path); err != nil {
		n.logger.Printf("core: add share: start watcher for %s: %v", id, err)
	} else if err := n.rescanShare(n.ctx, id); err != nil {
		n.logger.Printf("core: add share: initial scan for %s: %v", id, err)
	}
	n.broadcastShareList()
	n.debugf("core: added share %q (%s) at %s [%s, approval=%v]", name, id, path, permission, approvalRequired)
	return id, nil
}

// RemoveShare stops offering a share: persists the removal, stops its
// watcher, neuters it on any live sessions (see the package doc comment),
// and announces the updated share list.
func (n *Node) RemoveShare(shareRef string) error {
	shareID, err := n.resolveLocalShareRef(shareRef)
	if err != nil {
		return fmt.Errorf("core: remove share: %w", err)
	}
	if _, err := n.mutateConfig(func(cfg *config.Config) error {
		i := findShareIndex(cfg, shareID)
		if i < 0 {
			return fmt.Errorf("share %s is not configured", shareID)
		}
		cfg.Shares = append(cfg.Shares[:i], cfg.Shares[i+1:]...)
		return nil
	}); err != nil {
		return fmt.Errorf("core: remove share: %w", err)
	}

	n.stopShareWatch(shareID)
	n.neuterShareOnSessions(shareID)
	n.broadcastShareList()
	n.debugf("core: removed share %s", shareID)
	return nil
}

// RenameShare updates a share's display name.
func (n *Node) RenameShare(shareRef, name string) error {
	shareID, err := n.resolveLocalShareRef(shareRef)
	if err != nil {
		return fmt.Errorf("core: rename share: %w", err)
	}
	if _, err := n.mutateConfig(func(cfg *config.Config) error {
		i := findShareIndex(cfg, shareID)
		if i < 0 {
			return fmt.Errorf("share %s is not configured", shareID)
		}
		cfg.Shares[i].Name = name
		return nil
	}); err != nil {
		return fmt.Errorf("core: rename share: %w", err)
	}
	n.broadcastShareList()
	n.debugf("core: renamed share %s to %q", shareID, name)
	return nil
}

// SetSharePermission changes a share's permission (SPEC.md §6), updating
// the Direction every currently-connected, currently-granted session for
// this share uses from this point on.
func (n *Node) SetSharePermission(shareRef, permission string) error {
	if permission != config.PermissionReadOnly && permission != config.PermissionReadWrite {
		return fmt.Errorf("core: set share permission: invalid permission %q", permission)
	}
	shareID, err := n.resolveLocalShareRef(shareRef)
	if err != nil {
		return fmt.Errorf("core: set share permission: %w", err)
	}
	if _, err := n.mutateConfig(func(cfg *config.Config) error {
		i := findShareIndex(cfg, shareID)
		if i < 0 {
			return fmt.Errorf("share %s is not configured", shareID)
		}
		cfg.Shares[i].Permission = permission
		return nil
	}); err != nil {
		return fmt.Errorf("core: set share permission: %w", err)
	}

	n.sharesMu.Lock()
	sw := n.shareWatches[shareID]
	n.sharesMu.Unlock()
	root := ""
	if sw != nil {
		root = sw.root
	}
	direction := syncsvc.DirectionFor(permission, "")
	for _, pc := range n.snapshotPeers() {
		pc.mu.Lock()
		sess := pc.session
		active := pc.activeShares != nil && pc.activeShares[shareID]
		pc.mu.Unlock()
		if sess != nil && active {
			sess.AddShare(syncsvc.ShareConfig{ShareID: shareID, Root: root, Direction: direction})
		}
	}
	n.broadcastShareList()
	n.debugf("core: share %s permission set to %s", shareID, permission)
	return nil
}

// SetShareApprovalRequired toggles a share's approval_required flag.
// SPEC.md §6's approval queue is deferred (see the package doc comment),
// so this is persisted but has no live effect yet — every SubscribeRequest
// is still auto-granted.
func (n *Node) SetShareApprovalRequired(shareRef string, required bool) error {
	shareID, err := n.resolveLocalShareRef(shareRef)
	if err != nil {
		return fmt.Errorf("core: set share approval required: %w", err)
	}
	if _, err := n.mutateConfig(func(cfg *config.Config) error {
		i := findShareIndex(cfg, shareID)
		if i < 0 {
			return fmt.Errorf("share %s is not configured", shareID)
		}
		cfg.Shares[i].ApprovalRequired = required
		return nil
	}); err != nil {
		return fmt.Errorf("core: set share approval required: %w", err)
	}
	n.broadcastShareList()
	n.debugf("core: share %s approval_required set to %v", shareID, required)
	return nil
}

// SetShareAccess grants, denies, or revokes one peer's access to a share
// (SPEC.md §6), persisting the decision and, if the peer is connected,
// pushing AccessUpdate and adding/neutering the share on that session
// immediately.
func (n *Node) SetShareAccess(shareRef, peerRef, access string) error {
	switch access {
	case protocol.AccessGranted, protocol.AccessDenied, protocol.AccessRevoked:
	default:
		return fmt.Errorf("core: set share access: invalid access %q", access)
	}
	shareID, err := n.resolveLocalShareRef(shareRef)
	if err != nil {
		return fmt.Errorf("core: set share access: %w", err)
	}
	// The peer ref is resolved best-effort, not strictly: an access entry
	// may legitimately name a key this node has no peer for (a peer removed
	// while its grant lingered), and refusing those would make the stale
	// entry impossible to revoke.
	peerKeyHex := peerRef
	if pc, err := n.resolvePeerRef(peerRef); err == nil {
		peerKeyHex = pc.peerKeyHex
	}

	var share config.Share
	if _, err := n.mutateConfig(func(cfg *config.Config) error {
		i := findShareIndex(cfg, shareID)
		if i < 0 {
			return fmt.Errorf("share %s is not configured", shareID)
		}
		if cfg.Shares[i].Access == nil {
			cfg.Shares[i].Access = map[string]string{}
		}
		cfg.Shares[i].Access[peerKeyHex] = access
		share = cfg.Shares[i]
		return nil
	}); err != nil {
		return fmt.Errorf("core: set share access: %w", err)
	}
	// Before the live-effect early returns below: the decision is persisted
	// at this point regardless of whether the peer happens to be connected
	// to receive the AccessUpdate, and it is the decision that matters here.
	n.debugf("core: share %s access for peer %s set to %s", shareID, peerKeyHex, access)

	pc := n.lookupPeer(peerKeyHex)
	if pc == nil {
		return nil
	}
	sess := pc.currentSession()
	if sess == nil {
		return nil
	}

	if err := sess.Writer().WriteMessage(protocol.MsgAccessUpdate, protocol.AccessUpdate{ShareID: shareID, Access: access}); err != nil {
		n.logger.Printf("core: send access update for %s to %s: %v", shareID, peerKeyHex, err)
	}
	if access == protocol.AccessGranted {
		sess.AddShare(syncsvc.ShareConfig{ShareID: shareID, Root: share.Path, Direction: syncsvc.DirectionFor(share.Permission, "")})
		pc.markShareActive(shareID)
		if err := sess.SyncShare(n.ctx, shareID); err != nil {
			n.logger.Printf("core: sync share %s to %s: %v", shareID, peerKeyHex, err)
		}
		// The offerer half of the same gap finishAccessUpdate closes on the
		// subscriber side: a read-write share means work can be owed in this
		// direction too, and a reconnect is the moment to look.
		if err := sess.ReconcileShare(n.ctx, shareID); err != nil {
			n.logger.Printf("core: reconcile share %s for %s on connect: %v", shareID, peerKeyHex, err)
		}
	} else {
		pc.neuterShare(shareID)
	}
	return nil
}

// --- subscription mutations -------------------------------------------------

// AddSubscription subscribes to a peer's share (SPEC.md §1/§6): validates
// the peer and share ids and the local path (CheckSubscriptionPath),
// creates the local path if needed, persists the subscription, starts
// watching the local copy, and — if already connected to the peer — sends
// SubscribeRequest immediately.
//
// peerRef and shareRef are references in the sense of matchRef: a
// canonical id, a display name, or a unique id prefix. Both are resolved
// against what is actually configured rather than taken on faith, because a
// subscription naming a peer we don't have fails *silently* and
// permanently: nothing sends a SubscribeRequest for it (lookupPeer returns
// nil here, and requestSubscriptions never matches it on any later
// reconnect), so it sits in config looking configured while no bytes ever
// move.
func (n *Node) AddSubscription(peerRef, shareRef, localPath, mode string) error {
	if mode != config.ModeMirror && mode != config.ModeReceiveOnly {
		return fmt.Errorf("core: add subscription: invalid mode %q", mode)
	}
	pc, err := n.resolvePeerRef(peerRef)
	if err != nil {
		return fmt.Errorf("core: add subscription: %w", err)
	}
	peerKeyHex := pc.peerKeyHex
	shareID, err := n.resolveOfferedShareRef(pc, shareRef)
	if err != nil {
		return fmt.Errorf("core: add subscription: %w", err)
	}
	if err := os.MkdirAll(localPath, 0o700); err != nil {
		return fmt.Errorf("core: add subscription: create local path: %w", err)
	}

	if _, err := n.mutateConfig(func(cfg *config.Config) error {
		if err := cfg.CheckSubscriptionPath(localPath); err != nil {
			return err
		}
		for _, s := range cfg.Subscriptions {
			if s.Peer == peerKeyHex && s.ShareID == shareID {
				return fmt.Errorf("subscription to peer %s share %s already exists", peerKeyHex, shareID)
			}
		}
		cfg.Subscriptions = append(cfg.Subscriptions, config.Subscription{
			Peer: peerKeyHex, ShareID: shareID, LocalPath: localPath, Mode: mode,
		})
		return nil
	}); err != nil {
		return fmt.Errorf("core: add subscription: %w", err)
	}

	if _, err := n.startShareWatch(shareID, localPath); err != nil {
		n.logger.Printf("core: add subscription: start watcher for %s: %v", shareID, err)
	}

	if sess := pc.currentSession(); sess != nil {
		if err := sess.Writer().WriteMessage(protocol.MsgSubscribeRequest, protocol.SubscribeRequest{ShareID: shareID}); err != nil {
			n.logger.Printf("core: send subscribe request %s to %s: %v", shareID, peerKeyHex, err)
		}
	}
	n.debugf("core: added subscription to share %s from peer %s at %s [%s]", shareID, peerKeyHex, localPath, mode)
	return nil
}

// RemoveSubscription stops syncing a subscription: persists the removal,
// stops its watcher, and neuters it on the offering peer's live session
// (see the package doc comment).
func (n *Node) RemoveSubscription(peerRef, shareRef string) error {
	peerKeyHex, shareID := n.resolveSubscriptionRef(peerRef, shareRef)
	if _, err := n.mutateConfig(func(cfg *config.Config) error {
		idx := findSubscriptionIndex(cfg, peerKeyHex, shareID)
		if idx < 0 {
			return fmt.Errorf("subscription to peer %s share %s is not configured", peerKeyHex, shareID)
		}
		cfg.Subscriptions = append(cfg.Subscriptions[:idx], cfg.Subscriptions[idx+1:]...)
		return nil
	}); err != nil {
		return fmt.Errorf("core: remove subscription: %w", err)
	}

	n.stopShareWatch(shareID)
	if pc := n.lookupPeer(peerKeyHex); pc != nil {
		pc.neuterShare(shareID)
	}
	n.debugf("core: removed subscription to share %s from peer %s (local copy left on disk)", shareID, peerKeyHex)
	return nil
}

// PauseSubscription pauses or resumes a subscription: paused stops the
// local watcher and neuters the share on the live session; resuming
// restarts the watcher and re-requests access.
func (n *Node) PauseSubscription(peerRef, shareRef string, paused bool) error {
	peerKeyHex, shareID := n.resolveSubscriptionRef(peerRef, shareRef)
	var sub config.Subscription
	if _, err := n.mutateConfig(func(cfg *config.Config) error {
		idx := findSubscriptionIndex(cfg, peerKeyHex, shareID)
		if idx < 0 {
			return fmt.Errorf("subscription to peer %s share %s is not configured", peerKeyHex, shareID)
		}
		cfg.Subscriptions[idx].Paused = paused
		sub = cfg.Subscriptions[idx]
		return nil
	}); err != nil {
		return fmt.Errorf("core: pause subscription: %w", err)
	}

	pc := n.lookupPeer(peerKeyHex)

	if paused {
		n.stopShareWatch(shareID)
		if pc != nil {
			pc.neuterShare(shareID)
		}
		n.debugf("core: paused subscription to share %s from peer %s", shareID, peerKeyHex)
		return nil
	}

	if _, err := n.startShareWatch(shareID, sub.LocalPath); err != nil {
		n.logger.Printf("core: resume subscription: start watcher for %s: %v", shareID, err)
	}
	if pc != nil {
		if sess := pc.currentSession(); sess != nil {
			if err := sess.Writer().WriteMessage(protocol.MsgSubscribeRequest, protocol.SubscribeRequest{ShareID: shareID}); err != nil {
				n.logger.Printf("core: send subscribe request %s to %s: %v", shareID, peerKeyHex, err)
			}
		}
	}
	n.debugf("core: resumed subscription to share %s from peer %s", shareID, peerKeyHex)
	return nil
}

// --- node mutation -----------------------------------------------------------

// RenameNode changes this node's display name, persisting it and
// recomputing the cached sc1 token (which embeds the name as a
// "suggested display name" — SPEC.md §2).
func (n *Node) RenameNode(name string) error {
	if _, err := n.mutateConfig(func(cfg *config.Config) error {
		cfg.NodeName = name
		return nil
	}); err != nil {
		return fmt.Errorf("core: rename node: %w", err)
	}

	if addr, err := n.transport.LocalAddress(); err == nil {
		if tok, err := config.EncodeToken(addr, n.identity.Public(), name); err == nil {
			n.tokenMu.Lock()
			n.token = tok
			n.tokenMu.Unlock()
		}
	}
	n.debugf("core: node renamed to %q", name)
	return nil
}

// --- trash (SPEC.md §7) -----------------------------------------------------

// shareOrSubscriptionRoot resolves shareID to the local directory its
// trash entries live under: the share's own path if this node offers it,
// or the local subscription path if this node subscribes to it from a
// peer. Reads the live in-memory config under cfgMu, so it reflects any
// mutation that has already been applied in this process.
func (n *Node) shareOrSubscriptionRoot(shareID string) (string, error) {
	n.cfgMu.RLock()
	defer n.cfgMu.RUnlock()
	for _, s := range n.cfg.Shares {
		if s.ID == shareID {
			return s.Path, nil
		}
	}
	for _, sub := range n.cfg.Subscriptions {
		if sub.ShareID == shareID {
			return sub.LocalPath, nil
		}
	}
	return "", fmt.Errorf("share %s is not a local share or subscription", shareID)
}

// ListTrash returns every trashed entry for shareID (SPEC.md §7),
// most-recently-trashed first. shareID must be a locally offered share or
// subscription; an empty result (not an error) means nothing has been
// trashed for it yet.
func (n *Node) ListTrash(shareRef string) ([]syncsvc.TrashEntry, error) {
	shareID, err := n.resolveShareOrSubscriptionRef(shareRef)
	if err != nil {
		return nil, fmt.Errorf("core: list trash: %w", err)
	}
	if _, err := n.shareOrSubscriptionRoot(shareID); err != nil {
		return nil, fmt.Errorf("core: list trash: %w", err)
	}
	entries, err := n.trash.List(shareID)
	if err != nil {
		return nil, fmt.Errorf("core: list trash: %w", err)
	}
	return entries, nil
}

// RestoreTrash restores the most-recently-trashed entry for shareID/relPath
// (SPEC.md §7): copies the trashed content back into the share's live root
// with a bumped version vector, then triggers an immediate rescan so the
// restore propagates to peers right away (unlike the offline `syncat
// trash restore` path, which requires a later rescan since no daemon is
// running to trigger one). Returns an error wrapping
// syncsvc.ErrRestoreDestExists if the destination is already occupied.
func (n *Node) RestoreTrash(ctx context.Context, shareRef, relPath string) (index.FileRow, error) {
	shareID, err := n.resolveShareOrSubscriptionRef(shareRef)
	if err != nil {
		return index.FileRow{}, fmt.Errorf("core: restore trash: %w", err)
	}
	root, err := n.shareOrSubscriptionRoot(shareID)
	if err != nil {
		return index.FileRow{}, fmt.Errorf("core: restore trash: %w", err)
	}

	entries, err := n.trash.List(shareID)
	if err != nil {
		return index.FileRow{}, fmt.Errorf("core: restore trash: %w", err)
	}
	var match *syncsvc.TrashEntry
	for i := range entries {
		if entries[i].RelPath == relPath {
			match = &entries[i]
			break
		}
	}
	if match == nil {
		return index.FileRow{}, fmt.Errorf("core: restore trash: no trashed entry %q for share %s", relPath, shareID)
	}

	row, err := n.trash.Restore(ctx, n.store, n.identity.ShortID(), root, *match)
	if err != nil {
		return index.FileRow{}, fmt.Errorf("core: restore trash: %w", err)
	}
	if err := n.rescanShare(ctx, shareID); err != nil {
		n.logger.Printf("core: restore trash: rescan %s after restore: %v", shareID, err)
	}
	n.debugf("core: restored %s/%s from trash", shareID, relPath)
	return row, nil
}

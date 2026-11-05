package core

import (
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"

	"github.com/nickmarrone/syncat/internal/config"
	"github.com/nickmarrone/syncat/internal/protocol"
	syncsvc "github.com/nickmarrone/syncat/internal/sync"
)

// This file is Node's config-mutation API: everything the REST layer
// (internal/api) and the CLI call to change what this node peers with,
// offers, and subscribes to. Every entry point resolves its human-typed
// references (see resolve.go) and edits the config through mutateConfig.
// None of them applies a live effect: effects.go derives those from the
// change itself.
//
// Lock ordering: mutateMu, then cfgMu. A change is swapped in under cfgMu;
// its live effects (which take peerConn.mu) are applied after cfgMu is
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
	out.PendingPeers = append([]config.PendingPeer(nil), cfg.PendingPeers...)
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
// clone (config.Save, which also validates), and, only on success, swaps
// it in as the live config and applies its live effects (see effects.go).
// fn only edits the config; it must not touch anything running.
//
// mutateMu is held throughout, so one change's effects are applied before
// the next change is made, in the order the changes were made. Nothing an
// effect runs may call mutateConfig itself; work that must is handed to a
// tracked goroutine (see finishSubscribeRequest).
func (n *Node) mutateConfig(fn func(cfg *config.Config) error) (*config.Config, error) {
	n.mutateMu.Lock()
	defer n.mutateMu.Unlock()
	oldCfg, newCfg, err := n.swapConfig(fn)
	if err != nil {
		return nil, err
	}
	n.applyConfigChange(oldCfg, newCfg)
	return newCfg, nil
}

// swapConfig is mutateConfig's config half, under cfgMu. Neither config it
// returns is ever modified again: every change works on a clone.
func (n *Node) swapConfig(fn func(cfg *config.Config) error) (oldCfg, newCfg *config.Config, err error) {
	n.cfgMu.Lock()
	defer n.cfgMu.Unlock()
	oldCfg = n.cfg
	newCfg = cloneConfig(oldCfg)
	if err := fn(newCfg); err != nil {
		return nil, nil, err
	}
	if err := config.Save(n.paths.ConfigFile(), newCfg); err != nil {
		return nil, nil, fmt.Errorf("save config: %w", err)
	}
	n.cfg = newCfg
	return oldCfg, newCfg, nil
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

// AddPeer adds a peer by its pasted sc1 token (SPEC.md §2) and persists
// it; its effect starts dialing it. It returns the peer's canonical
// identifier (its full Ed25519 public key, hex-encoded). Every other
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
			return mutationErrorf(MutationConflict, "peer %s is already configured", peerKeyHex)
		}
		for i := len(cfg.PendingPeers) - 1; i >= 0; i-- {
			if key, ok := pendingPeerKey(cfg.PendingPeers[i]); ok && key == peerKeyHex {
				cfg.PendingPeers = append(cfg.PendingPeers[:i], cfg.PendingPeers[i+1:]...)
			}
		}
		cfg.Peers = append(cfg.Peers, p)
		return nil
	}); err != nil {
		return "", fmt.Errorf("core: add peer: %w", err)
	}
	return peerKeyHex, nil
}

// activatePeer registers a peer added at runtime and starts dialing it.
func (n *Node) activatePeer(p config.Peer) error {
	pc, err := newPeerConn(n, p)
	if err != nil {
		return err
	}
	n.peersMu.Lock()
	n.peers[pc.peerKeyHex] = pc
	n.peersMu.Unlock()
	n.goTracked(pc.runSupervisor)
	n.debugf("core: added peer %q (%s)", p.Name, pc.peerShort)
	return nil
}

// RemovePeer removes a configured peer and every trace of it from the rest
// of the config: subscriptions to its shares, and its entries in our own
// shares' access lists. Its effects close any connection to it and stop
// watching those subscriptions.
//
// The subscriptions go because a subscription names the peer that offers
// the share (config.Subscription.Peer): once that peer is gone there is no
// node left to sync it with, so leaving it configured would keep a watcher
// running and keep it listed by `syncat status` forever, permanently
// stuck. Removal is config-only, matching RemoveSubscription. The local
// copy of the files stays on disk, it just stops being synced.
//
// The access entries go because config.Share.Access is keyed by the peer's
// Ed25519 public key, and buildShareList reads that map directly with no
// reference to whether the key still belongs to a configured peer. A
// leftover "granted" is therefore not just clutter: re-adding that same
// key later (the obvious thing to do after removing a peer by mistake, or
// to re-pair after one side is rebuilt) would silently restore its
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
			return mutationErrorf(MutationNotFound, "peer %s is not configured", peerKeyHex)
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

	for _, s := range droppedSubs {
		n.logger.Printf("core: remove peer %s: dropped subscription to share %s (local copy left at %s)", peerKeyHex, s.ShareID, s.LocalPath)
	}
	for _, shareID := range droppedAccess {
		n.logger.Printf("core: remove peer %s: revoked its access to share %s", peerKeyHex, shareID)
	}
	n.debugf("core: removed peer %s (dropped %d subscription(s), revoked access to %d share(s))", peerKeyHex, len(droppedSubs), len(droppedAccess))
	return nil
}

// --- share mutations --------------------------------------------------------

// AddShare offers a new local directory as a share (SPEC.md §3/§6):
// validates path (including the CheckSharePath overlap rule), generates a
// share id, and persists it. Its effects start watching it and announce it
// to every connected peer.
func (n *Node) AddShare(path, name, permission string, approvalRequired bool) (string, error) {
	if permission != config.PermissionReadOnly && permission != config.PermissionReadWrite {
		return "", fmt.Errorf("core: add share: invalid permission %q", permission)
	}
	id, err := config.NewShareID()
	if err != nil {
		return "", fmt.Errorf("core: add share: %w", err)
	}
	if err := syncsvc.EnsureReservedDir(path); errors.Is(err, syncsvc.ErrReservedDirTaken) {
		return "", mutationError(MutationConflict, fmt.Errorf("core: add share: %w; move it away first", err))
	}

	if _, err := n.mutateConfig(func(cfg *config.Config) error {
		if err := cfg.CheckSharePath(path); err != nil {
			if errors.Is(err, config.ErrPathOverlap) {
				return mutationError(MutationConflict, err)
			}
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
	n.debugf("core: added share %q (%s) at %s [%s, approval=%v]", name, id, path, permission, approvalRequired)
	return id, nil
}

// RemoveShare stops offering a share. Its effects stop its watcher,
// neuter it on any live sessions (see the package doc comment), and
// announce the updated share list.
func (n *Node) RemoveShare(shareRef string) error {
	shareID, err := n.resolveLocalShareRef(shareRef)
	if err != nil {
		return fmt.Errorf("core: remove share: %w", err)
	}
	if _, err := n.mutateConfig(func(cfg *config.Config) error {
		i := findShareIndex(cfg, shareID)
		if i < 0 {
			return mutationErrorf(MutationNotFound, "share %s is not configured", shareID)
		}
		cfg.Shares = append(cfg.Shares[:i], cfg.Shares[i+1:]...)
		return nil
	}); err != nil {
		return fmt.Errorf("core: remove share: %w", err)
	}
	n.debugf("core: removed share %s", shareID)
	return nil
}

// ShareChange is a set of edits to one share this node offers. A nil
// field, or a peer missing from Access, is left as it is.
type ShareChange struct {
	Name             *string
	Permission       *string // config.PermissionReadOnly or PermissionReadWrite
	ApprovalRequired *bool
	// Access maps a peer ref to the access to record for it: pending,
	// granted, denied, or revoked (SPEC.md §6).
	Access map[string]string
}

// UpdateShare applies every edit in ch to one share as a single config
// change, so it is saved and announced once, and either all of it applies
// or none of it does. Its effects carry a new permission onto the sessions
// syncing the share, and an access decision onto that peer's session:
// granting or neutering the share there and telling the peer.
func (n *Node) UpdateShare(shareRef string, ch ShareChange) error {
	return n.updateShare("update share", shareRef, ch)
}

// RenameShare updates a share's display name.
func (n *Node) RenameShare(shareRef, name string) error {
	return n.updateShare("rename share", shareRef, ShareChange{Name: &name})
}

// SetSharePermission changes a share's permission (SPEC.md §6), and with
// it the Direction every session syncing the share uses from now on.
func (n *Node) SetSharePermission(shareRef, permission string) error {
	return n.updateShare("set share permission", shareRef, ShareChange{Permission: &permission})
}

// SetShareApprovalRequired toggles a share's approval_required flag. It
// applies to subsequent requests; existing explicit decisions are retained.
func (n *Node) SetShareApprovalRequired(shareRef string, required bool) error {
	return n.updateShare("set share approval required", shareRef, ShareChange{ApprovalRequired: &required})
}

// SetShareAccess records pending access, or grants, denies, or revokes one
// peer's access to a share (SPEC.md §6).
func (n *Node) SetShareAccess(shareRef, peerRef, access string) error {
	return n.updateShare("set share access", shareRef, ShareChange{Access: map[string]string{peerRef: access}})
}

func (n *Node) updateShare(op, shareRef string, ch ShareChange) error {
	if ch.Permission != nil && *ch.Permission != config.PermissionReadOnly && *ch.Permission != config.PermissionReadWrite {
		return mutationErrorf(MutationInvalid, "core: %s: invalid permission %q", op, *ch.Permission)
	}
	// Peer refs are resolved best-effort, not strictly: an access entry may
	// legitimately name a key this node has no peer for (a peer removed
	// while its grant lingered), and refusing those would make the stale
	// entry impossible to revoke.
	access := make(map[string]string, len(ch.Access))
	for peerRef, a := range ch.Access {
		switch a {
		case protocol.AccessPending, protocol.AccessGranted, protocol.AccessDenied, protocol.AccessRevoked:
		default:
			return mutationErrorf(MutationInvalid, "core: %s: invalid access %q", op, a)
		}
		peerKeyHex := peerRef
		if pc, err := n.resolvePeerRef(peerRef); err == nil {
			peerKeyHex = pc.peerKeyHex
		}
		access[peerKeyHex] = a
	}
	shareID, err := n.resolveLocalShareRef(shareRef)
	if err != nil {
		return fmt.Errorf("core: %s: %w", op, err)
	}

	if _, err := n.mutateConfig(func(cfg *config.Config) error {
		i := findShareIndex(cfg, shareID)
		if i < 0 {
			return mutationErrorf(MutationNotFound, "share %s is not configured", shareID)
		}
		s := &cfg.Shares[i]
		if ch.Name != nil {
			s.Name = *ch.Name
		}
		if ch.Permission != nil {
			s.Permission = *ch.Permission
		}
		if ch.ApprovalRequired != nil {
			s.ApprovalRequired = *ch.ApprovalRequired
		}
		if len(access) > 0 && s.Access == nil {
			s.Access = map[string]string{}
		}
		maps.Copy(s.Access, access)
		return nil
	}); err != nil {
		return fmt.Errorf("core: %s: %w", op, err)
	}
	n.debugf("core: share %s updated: %s", shareID, describeShareChange(ch, access))
	return nil
}

// describeShareChange is ch for the debug log, with peer refs resolved.
func describeShareChange(ch ShareChange, access map[string]string) string {
	var parts []string
	if ch.Name != nil {
		parts = append(parts, fmt.Sprintf("name %q", *ch.Name))
	}
	if ch.Permission != nil {
		parts = append(parts, "permission "+*ch.Permission)
	}
	if ch.ApprovalRequired != nil {
		parts = append(parts, fmt.Sprintf("approval_required %v", *ch.ApprovalRequired))
	}
	for _, key := range slices.Sorted(maps.Keys(access)) {
		parts = append(parts, fmt.Sprintf("access for peer %s %s", key, access[key]))
	}
	if len(parts) == 0 {
		return "nothing"
	}
	return strings.Join(parts, ", ")
}

// --- subscription mutations -------------------------------------------------

// AddSubscription subscribes to a peer's share (SPEC.md §1/§6): validates
// the peer and share ids and the local path (CheckSubscriptionPath),
// creates the local path if needed, and persists the subscription. Its
// effects start watching the local copy and, if already connected to the
// peer, send SubscribeRequest immediately.
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
		return mutationError(MutationInvalid, fmt.Errorf("core: add subscription: %w", err))
	}
	peerKeyHex := pc.peerKeyHex
	shareID, err := n.resolveOfferedShareRef(pc, shareRef)
	if err != nil {
		return mutationError(MutationInvalid, fmt.Errorf("core: add subscription: %w", err))
	}
	if err := os.MkdirAll(localPath, 0o700); err != nil {
		return fmt.Errorf("core: add subscription: create local path: %w", err)
	}
	if err := syncsvc.EnsureReservedDir(localPath); errors.Is(err, syncsvc.ErrReservedDirTaken) {
		return mutationError(MutationConflict, fmt.Errorf("core: add subscription: %w; move it away first", err))
	}

	if _, err := n.mutateConfig(func(cfg *config.Config) error {
		if err := cfg.CheckSubscriptionPath(localPath); err != nil {
			if errors.Is(err, config.ErrPathOverlap) {
				return mutationError(MutationConflict, err)
			}
			return err
		}
		for _, s := range cfg.Subscriptions {
			if s.Peer == peerKeyHex && s.ShareID == shareID {
				return mutationErrorf(MutationConflict, "subscription to peer %s share %s already exists", peerKeyHex, shareID)
			}
		}
		cfg.Subscriptions = append(cfg.Subscriptions, config.Subscription{
			Peer: peerKeyHex, ShareID: shareID, LocalPath: localPath, Mode: mode,
		})
		return nil
	}); err != nil {
		return fmt.Errorf("core: add subscription: %w", err)
	}
	n.debugf("core: added subscription to share %s from peer %s at %s [%s]", shareID, peerKeyHex, localPath, mode)
	return nil
}

// RemoveSubscription stops syncing a subscription. Its effects stop its
// watcher and neuter it on the offering peer's live session (see the
// package doc comment).
func (n *Node) RemoveSubscription(peerRef, shareRef string) error {
	peerKeyHex, shareID := n.resolveSubscriptionRef(peerRef, shareRef)
	if _, err := n.mutateConfig(func(cfg *config.Config) error {
		idx := findSubscriptionIndex(cfg, peerKeyHex, shareID)
		if idx < 0 {
			return mutationErrorf(MutationNotFound, "subscription to peer %s share %s is not configured", peerKeyHex, shareID)
		}
		cfg.Subscriptions = append(cfg.Subscriptions[:idx], cfg.Subscriptions[idx+1:]...)
		return nil
	}); err != nil {
		return fmt.Errorf("core: remove subscription: %w", err)
	}
	n.debugf("core: removed subscription to share %s from peer %s (local copy left on disk)", shareID, peerKeyHex)
	return nil
}

// PauseSubscription pauses or resumes a subscription. Pausing stops the
// local watcher and neuters the share on the live session; resuming
// restarts the watcher and re-requests access.
func (n *Node) PauseSubscription(peerRef, shareRef string, paused bool) error {
	peerKeyHex, shareID := n.resolveSubscriptionRef(peerRef, shareRef)
	if _, err := n.mutateConfig(func(cfg *config.Config) error {
		idx := findSubscriptionIndex(cfg, peerKeyHex, shareID)
		if idx < 0 {
			return mutationErrorf(MutationNotFound, "subscription to peer %s share %s is not configured", peerKeyHex, shareID)
		}
		cfg.Subscriptions[idx].Paused = paused
		return nil
	}); err != nil {
		return fmt.Errorf("core: pause subscription: %w", err)
	}
	if paused {
		n.debugf("core: paused subscription to share %s from peer %s", shareID, peerKeyHex)
	} else {
		n.debugf("core: resumed subscription to share %s from peer %s", shareID, peerKeyHex)
	}
	return nil
}

// SetSafety replaces the hold and trash settings of a share this node
// offers or subscribes to (see config.Safety; zero fields mean "use the
// default"). It returns the settings now in force, defaults filled in.
// They are read fresh by every scan, reconcile pass, and trash sweep, so
// nothing else needs restarting.
func (n *Node) SetSafety(shareRef string, s config.Safety) (config.Safety, error) {
	shareID, err := n.resolveShareOrSubscriptionRef(shareRef)
	if err != nil {
		return config.Safety{}, fmt.Errorf("core: set safety: %w", err)
	}
	if err := s.Validate(); err != nil {
		return config.Safety{}, mutationError(MutationInvalid, fmt.Errorf("core: set safety: %w", err))
	}
	cfg, err := n.mutateConfig(func(cfg *config.Config) error {
		found := false
		if i := findShareIndex(cfg, shareID); i >= 0 {
			cfg.Shares[i].Safety, found = s, true
		}
		for i := range cfg.Subscriptions {
			if cfg.Subscriptions[i].ShareID == shareID {
				cfg.Subscriptions[i].Safety, found = s, true
			}
		}
		if !found {
			return mutationErrorf(MutationNotFound, "share %s is not a local share or subscription", shareID)
		}
		return nil
	})
	if err != nil {
		return config.Safety{}, fmt.Errorf("core: set safety: %w", err)
	}
	n.debugf("core: share %s safety set to %+v", shareID, s)
	return cfg.SafetyFor(shareID), nil
}

// Safety returns the raw (unfilled) settings for a share or subscription,
// for a caller that wants to change one field and keep the rest.
func (n *Node) Safety(shareRef string) (config.Safety, error) {
	shareID, err := n.resolveShareOrSubscriptionRef(shareRef)
	if err != nil {
		return config.Safety{}, fmt.Errorf("core: safety: %w", err)
	}
	n.cfgMu.RLock()
	defer n.cfgMu.RUnlock()
	if i := findShareIndex(n.cfg, shareID); i >= 0 {
		return n.cfg.Shares[i].Safety, nil
	}
	for _, sub := range n.cfg.Subscriptions {
		if sub.ShareID == shareID {
			return sub.Safety, nil
		}
	}
	return config.Safety{}, mutationErrorf(MutationNotFound, "share %s is not a local share or subscription", shareID)
}

// --- node mutation -----------------------------------------------------------

// RenameNode changes this node's display name. Its effect recomputes the
// cached sc1 token, which embeds the name as a "suggested display name"
// (SPEC.md §2).
func (n *Node) RenameNode(name string) error {
	if _, err := n.mutateConfig(func(cfg *config.Config) error {
		cfg.NodeName = name
		return nil
	}); err != nil {
		return fmt.Errorf("core: rename node: %w", err)
	}
	n.debugf("core: node renamed to %q", name)
	return nil
}

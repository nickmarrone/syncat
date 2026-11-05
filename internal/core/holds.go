package core

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/nickmarrone/syncat/internal/config"
	"github.com/nickmarrone/syncat/internal/hold"
	"github.com/nickmarrone/syncat/internal/index"
	"github.com/nickmarrone/syncat/internal/protocol"
)

// This file connects the node to its hold guard (n.holds, see
// internal/hold and GLOSSARY.md), which owns every rule about when a change
// is held and when a hold ends. The node only feeds it:
//
//   - rescanShare asks it before a local scan result is applied;
//   - every session asks the closure shareGuardFunc builds before it
//     applies a peer's changes;
//   - mutateConfig has it forget the holds of whatever a config change
//     removed, and Open does the same at startup;
//   - the API lists holds and confirms them through the methods below.

// holdOptions is what n's hold guard needs from the node.
func (n *Node) holdOptions() hold.Options {
	return hold.Options{
		Safety:   n.safetyFor,
		Now:      n.clock.Now,
		Logger:   n.logger,
		PeerName: n.peerDisplayName,
	}
}

func (n *Node) safetyFor(shareID string) config.Safety {
	n.cfgMu.RLock()
	defer n.cfgMu.RUnlock()
	return n.cfg.SafetyFor(shareID)
}

// peerDisplayName returns a configured peer's name, or "" if peerKey is
// not one.
func (n *Node) peerDisplayName(peerKey string) string {
	if pc := n.lookupPeer(peerKey); pc != nil {
		return pc.name
	}
	return ""
}

// configuredLinks returns whether a hold's link still names something cfg
// configures: this node's own copy of a share it offers or subscribes to,
// or a peer's link to one (a configured peer with access to a share this
// node offers, or this node's subscription to that peer's share).
// Everything else can never be scanned or synced again, so its hold could
// never clear. Forgetting one is always safe: if the link comes back, its
// next change is measured afresh and held again if it still needs to be.
//
// It copies what it needs out of cfg, so the result stays valid after cfg
// is replaced.
func configuredLinks(cfg *config.Config) func(hold.Link) bool {
	local := make(map[string]bool, len(cfg.Shares)+len(cfg.Subscriptions))
	access := make(map[hold.Link]bool)
	open := make(map[string]bool, len(cfg.Shares))
	for _, s := range cfg.Shares {
		local[s.ID] = true
		open[s.ID] = !s.ApprovalRequired
		for key, a := range s.Access {
			switch a {
			case protocol.AccessGranted, protocol.AccessDenied, protocol.AccessRevoked:
				access[hold.Link{ShareID: s.ID, PeerKey: key}] = a == protocol.AccessGranted
			}
		}
	}
	subscribed := make(map[hold.Link]bool, len(cfg.Subscriptions))
	for _, s := range cfg.Subscriptions {
		local[s.ShareID] = true
		subscribed[hold.Link{ShareID: s.ShareID, PeerKey: s.Peer}] = true
	}
	peers := make(map[string]bool, len(cfg.Peers))
	for _, p := range cfg.Peers {
		if tok, err := config.ParseToken(p.Token); err == nil {
			peers[hex.EncodeToString(tok.ID)] = true
		}
	}
	return func(l hold.Link) bool {
		if l.PeerKey == "" {
			return local[l.ShareID]
		}
		return subscribed[l] || (peers[l.PeerKey] && hasAccess(l, access, open))
	}
}

// hasAccess returns whether l's peer may sync l's share, which this node
// offers: a grant always counts, a denial or revocation never does, and
// otherwise (no decision, or one still pending) the peer may only if the
// share needs no approval. That last part is provisionShareForRequest's
// rule, but here a revocation counts even on such a share, since it shuts
// the peer out until it subscribes again.
func hasAccess(l hold.Link, access map[hold.Link]bool, open map[string]bool) bool {
	if granted, decided := access[l]; decided {
		return granted
	}
	return open[l.ShareID]
}

// shareGuardFunc returns the syncsvc.ShareConfig.Guard for shareID on the
// session with peerKey.
func (n *Node) shareGuardFunc(shareID, peerKey string) func(local []index.FileRow, deleting []string) bool {
	return func(local []index.FileRow, deleting []string) bool {
		return n.holds.Pass(shareID, peerKey, local, deleting)
	}
}

// reconcileShareOnSessions re-runs the reconcile pass for shareID on every
// session it is active on, so peers' changes a hold on this node's own
// copy stopped are applied without waiting for the peers to send more.
func (n *Node) reconcileShareOnSessions(shareID string) {
	for _, pc := range n.snapshotPeers() {
		sess := pc.activeSession(shareID)
		if sess == nil {
			continue
		}
		n.goTracked(func() {
			if err := sess.ReconcileShare(n.ctx, shareID); err != nil {
				n.logger.Printf("core: reconcile %s with %s: %v", shareID, pc.name, err)
			}
		})
	}
}

// HoldStatus is one hold as the API and UI show it.
type HoldStatus struct {
	hold.Hold

	ShareName string
	PeerName  string // "" for this node's own copy, or for a peer with no name
}

// Holds returns every current hold, oldest first.
func (n *Node) Holds() []HoldStatus {
	list := n.holds.List()
	out := make([]HoldStatus, 0, len(list))
	for _, h := range list {
		out = append(out, n.holdStatus(h))
	}
	return out
}

func (n *Node) holdStatus(h hold.Hold) HoldStatus {
	return HoldStatus{Hold: h, ShareName: n.shareDisplayName(h.ShareID), PeerName: n.peerDisplayName(h.PeerKey)}
}

// shareDisplayName returns a share's or subscription's name, or "" if this
// node has none for it.
func (n *Node) shareDisplayName(shareID string) string {
	n.cfgMu.RLock()
	for _, s := range n.cfg.Shares {
		if s.ID == shareID {
			n.cfgMu.RUnlock()
			return s.Name
		}
	}
	n.cfgMu.RUnlock()
	for _, pc := range n.snapshotPeers() {
		for _, e := range pc.offeredShares() {
			if e.ShareID == shareID {
				return e.Name
			}
		}
	}
	return ""
}

// ConfirmHold lets a held change through and runs it now: the scan (for
// this node's own hold) or reconcile pass (for a peer's) that would have
// been held applies instead. peerRef names the peer whose hold to confirm;
// it may be empty when the share has only one hold.
func (n *Node) ConfirmHold(ctx context.Context, shareRef, peerRef string) (HoldStatus, error) {
	shareID, err := n.resolveShareOrSubscriptionRef(shareRef)
	if err != nil {
		return HoldStatus{}, fmt.Errorf("core: confirm hold: %w", err)
	}
	link := hold.Link{ShareID: shareID}
	if peerRef != "" {
		pc, err := n.resolvePeerRef(peerRef)
		if err != nil {
			return HoldStatus{}, mutationError(MutationInvalid, fmt.Errorf("core: confirm hold: %w", err))
		}
		link.PeerKey = pc.peerKeyHex
	} else if link, err = n.holds.Only(shareID); err != nil {
		return HoldStatus{}, holdMutationError(err)
	}
	h, err := n.holds.Confirm(link)
	if err != nil {
		return HoldStatus{}, holdMutationError(err)
	}

	if link.PeerKey == "" {
		if err := n.rescanShare(ctx, shareID); err != nil {
			n.logger.Printf("core: confirm hold: rescan %s: %v", shareID, err)
		}
	} else if pc := n.lookupPeer(link.PeerKey); pc != nil {
		if sess := pc.activeSession(shareID); sess != nil {
			if err := sess.ReconcileShare(ctx, shareID); err != nil {
				n.logger.Printf("core: confirm hold: reconcile %s with %s: %v", shareID, pc.name, err)
			}
		}
	}
	return n.holdStatus(h), nil
}

// holdMutationError classifies an error from the hold guard's Only or
// Confirm for the API: no such hold is a miss, anything else is a request
// that cannot be carried out.
func holdMutationError(err error) error {
	if errors.Is(err, hold.ErrNotHeld) {
		return mutationError(MutationNotFound, err)
	}
	return mutationError(MutationInvalid, err)
}

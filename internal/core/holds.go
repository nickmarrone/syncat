package core

import (
	"context"
	"errors"
	"fmt"

	"github.com/nickmarrone/syncat/internal/config"
	"github.com/nickmarrone/syncat/internal/hold"
	"github.com/nickmarrone/syncat/internal/index"
)

// This file connects the node to its hold guard (n.holds, see
// internal/hold and GLOSSARY.md), which owns every rule about when a change
// is held and when a hold ends. The node only feeds it:
//
//   - rescanShare asks it before a local scan result is applied;
//   - every session asks the closure shareGuardFunc builds before it
//     applies a peer's changes;
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

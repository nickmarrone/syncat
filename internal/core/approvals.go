package core

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/nickmarrone/syncat/internal/config"
	"github.com/nickmarrone/syncat/internal/protocol"
)

const (
	ApprovalPeer  = "peer"
	ApprovalShare = "share"

	maxPendingPeers          = 128
	pendingPeerWriteInterval = time.Minute
)

// Approval is one actionable item in the combined peer/share queue. It never
// exposes a peer token; only safe identity and share metadata leave core.
type Approval struct {
	ID        string
	Kind      string
	PeerKey   string
	PeerName  string
	ShareID   string
	ShareName string
	CreatedAt time.Time
}

func peerApprovalID(peerKey string) string { return "peer-" + peerKey }
func shareApprovalID(shareID, peerKey string) string {
	return "share-" + shareID + "-" + peerKey
}

func pendingPeerKey(p config.PendingPeer) (string, bool) {
	tok, err := config.ParseToken(p.Token)
	if err != nil {
		return "", false
	}
	return hex.EncodeToString(tok.ID), true
}

// Approvals returns a deterministic snapshot of pending unknown peers and
// pending requests for approval-required shares.
func (n *Node) Approvals() []Approval {
	n.cfgMu.RLock()
	defer n.cfgMu.RUnlock()
	peerNames := make(map[string]string, len(n.cfg.Peers))
	for _, p := range n.cfg.Peers {
		if tok, err := config.ParseToken(p.Token); err == nil {
			peerNames[hex.EncodeToString(tok.ID)] = p.Name
		}
	}
	out := make([]Approval, 0, len(n.cfg.PendingPeers))
	for _, p := range n.cfg.PendingPeers {
		key, ok := pendingPeerKey(p)
		if !ok {
			continue
		}
		out = append(out, Approval{ID: peerApprovalID(key), Kind: ApprovalPeer, PeerKey: key, PeerName: p.Name, CreatedAt: p.FirstSeen})
	}
	for _, share := range n.cfg.Shares {
		for peerKey, access := range share.Access {
			if access != protocol.AccessPending {
				continue
			}
			out = append(out, Approval{
				ID: shareApprovalID(share.ID, peerKey), Kind: ApprovalShare,
				PeerKey: peerKey, PeerName: peerNames[peerKey], ShareID: share.ID, ShareName: share.Name,
			})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Kind != out[j].Kind {
			return out[i].Kind < out[j].Kind
		}
		return out[i].ID < out[j].ID
	})
	return out
}

// recordPendingPeer persists a validated unknown inbound Hello. Repeated
// attempts update metadata at most once per minute, bounding disk writes.
func (n *Node) recordPendingPeer(h protocol.Hello) {
	tok, err := config.ParseToken(h.Token)
	if err != nil || !bytes.Equal(tok.ID, h.Ed25519Pub) {
		return
	}
	peerKey := hex.EncodeToString(tok.ID)
	if n.lookupPeer(peerKey) != nil {
		return
	}
	now := n.clock.Now()
	n.cfgMu.RLock()
	for _, pending := range n.cfg.PendingPeers {
		if key, ok := pendingPeerKey(pending); ok && key == peerKey && now.Sub(pending.LastSeen) < pendingPeerWriteInterval {
			n.cfgMu.RUnlock()
			return
		}
	}
	n.cfgMu.RUnlock()

	if _, err := n.mutateConfig(func(cfg *config.Config) error {
		if findPeerIndex(cfg, peerKey) >= 0 {
			return nil
		}
		for i := range cfg.PendingPeers {
			if key, ok := pendingPeerKey(cfg.PendingPeers[i]); ok && key == peerKey {
				cfg.PendingPeers[i].Name = h.NodeName
				cfg.PendingPeers[i].Token = h.Token
				cfg.PendingPeers[i].LastSeen = now
				return nil
			}
		}
		if len(cfg.PendingPeers) >= maxPendingPeers {
			return errors.New("pending peer queue is full")
		}
		cfg.PendingPeers = append(cfg.PendingPeers, config.PendingPeer{Name: h.NodeName, Token: h.Token, FirstSeen: now, LastSeen: now})
		return nil
	}); err != nil {
		n.logger.Printf("core: record pending peer %s: %v", peerKey, err)
		return
	}
	n.debugf("core: queued pending peer %s", peerKey)
}

// DecideApproval grants or denies one item returned by Approvals.
func (n *Node) DecideApproval(id, decision string) error {
	if decision != "grant" && decision != "deny" {
		return mutationErrorf(MutationInvalid, "core: approval: invalid decision %q", decision)
	}
	var found *Approval
	for _, approval := range n.Approvals() {
		if approval.ID == id {
			a := approval
			found = &a
			break
		}
	}
	if found == nil {
		return mutationErrorf(MutationNotFound, "approval %s is not pending", id)
	}
	if found.Kind == ApprovalShare {
		access := protocol.AccessGranted
		if decision == "deny" {
			access = protocol.AccessDenied
		}
		return n.SetShareAccess(found.ShareID, found.PeerKey, access)
	}
	if decision == "deny" {
		return n.removePendingPeer(found.PeerKey)
	}
	return n.ApprovePendingPeer(found.PeerKey)
}

// ApprovePendingPeer converts a queued Hello into a configured, enabled peer
// in one config transaction, then starts its normal supervisor.
func (n *Node) ApprovePendingPeer(ref string) error {
	query := strings.ToLower(strings.TrimPrefix(ref, "peer-"))
	if query == "" {
		return mutationErrorf(MutationInvalid, "core: approve pending peer: empty peer reference")
	}
	peerKey := ""
	var approved config.Peer
	if _, err := n.mutateConfig(func(cfg *config.Config) error {
		idx := -1
		for i, pending := range cfg.PendingPeers {
			key, ok := pendingPeerKey(pending)
			if ok && (key == query || strings.HasPrefix(key, query)) {
				if idx >= 0 {
					return mutationErrorf(MutationInvalid, "pending peer %q is ambiguous", ref)
				}
				idx, peerKey = i, key
			}
		}
		if idx < 0 {
			return mutationErrorf(MutationNotFound, "pending peer %s is not configured", ref)
		}
		if findPeerIndex(cfg, peerKey) >= 0 {
			return mutationErrorf(MutationConflict, "peer %s is already configured", peerKey)
		}
		pending := cfg.PendingPeers[idx]
		approved = config.Peer{Name: pending.Name, Token: pending.Token, Enabled: true}
		cfg.PendingPeers = append(cfg.PendingPeers[:idx], cfg.PendingPeers[idx+1:]...)
		cfg.Peers = append(cfg.Peers, approved)
		return nil
	}); err != nil {
		return fmt.Errorf("core: approve pending peer: %w", err)
	}
	if err := n.activatePeer(approved, peerKey); err != nil {
		return fmt.Errorf("core: approve pending peer: %w", err)
	}
	return nil
}

func (n *Node) removePendingPeer(peerKey string) error {
	if _, err := n.mutateConfig(func(cfg *config.Config) error {
		for i, pending := range cfg.PendingPeers {
			if key, ok := pendingPeerKey(pending); ok && key == peerKey {
				cfg.PendingPeers = append(cfg.PendingPeers[:i], cfg.PendingPeers[i+1:]...)
				return nil
			}
		}
		return mutationErrorf(MutationNotFound, "pending peer %s is not configured", peerKey)
	}); err != nil {
		return fmt.Errorf("core: deny pending peer: %w", err)
	}
	return nil
}

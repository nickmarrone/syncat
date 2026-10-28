package core

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"github.com/nickmarrone/syncat/internal/config"
	"github.com/nickmarrone/syncat/internal/index"
	"github.com/nickmarrone/syncat/internal/protocol"
	syncsvc "github.com/nickmarrone/syncat/internal/sync"
)

// This file is the hold (see GLOSSARY.md): the guard that stops a change
// that looks like a disaster from being recorded or spread. It runs in two
// places. rescanShare asks holdForScan before a local scan result is
// applied, and every session asks the closure shareGuardFunc builds before
// it applies a peer's changes. A hold is per (share, peer) link: peerKey ""
// is this node's own copy of the share, which stops every peer's changes
// too, since applying them into a folder in that state is how the damage
// would spread the other way.
//
// A hold ends one of two ways. The folder is fixed, so the next scan or
// pass no longer trips a trigger and the hold clears itself. Or the user
// confirms it (ConfirmHold), and the next pass that would have been held
// applies instead.

// Hold reasons, stored in index.Hold.Reason and shown to the user.
const (
	// HoldRootUnavailable: the share's root is missing or unreadable while
	// the index still has live files. It cannot be confirmed: there is no
	// change to let through, only a folder to restore or a share to remove.
	HoldRootUnavailable = "root-unavailable"

	// HoldEverythingVanished: every live file in the share would be
	// deleted. This is an unmounted drive or an emptied folder, so it holds
	// whatever the share's thresholds say.
	HoldEverythingVanished = "everything-vanished"

	// HoldBulkDelete: the change would delete at least the share's
	// configured percentage and number of live files.
	HoldBulkDelete = "bulk-delete"

	// HoldReservedDirTaken: the folder has a .syncat/ that syncat did not
	// create, so it has nowhere safe to keep the trash. It cannot be
	// confirmed; it clears once that directory is moved away.
	HoldReservedDirTaken = "reserved-dir-taken"
)

// holdConfirmable reports whether a hold with reason has a change the user
// can let through.
func holdConfirmable(reason string) bool {
	return reason != HoldRootUnavailable && reason != HoldReservedDirTaken
}

// holdKey identifies one held link: a share, and either a peer's hex key
// or "" for this node's own scans.
type holdKey struct{ shareID, peerKey string }

func loadHolds(ctx context.Context, store *index.Store) (map[holdKey]index.Hold, error) {
	list, err := store.ListHolds(ctx)
	if err != nil {
		return nil, err
	}
	holds := make(map[holdKey]index.Hold, len(list))
	for _, h := range list {
		holds[holdKey{h.ShareID, h.PeerKey}] = h
	}
	return holds, nil
}

// holdReason returns why a change that would delete deletes of a share's
// live files puts it on hold under s, or "" if it does not. Fewer than two
// live files never counts as everything vanishing: deleting the only file
// in a share is an ordinary edit.
func holdReason(s config.Safety, deletes, live int) string {
	if deletes == 0 {
		return ""
	}
	if live >= 2 && deletes >= live {
		return HoldEverythingVanished
	}
	if deletes >= s.HoldDeleteMin && deletes*100 >= s.HoldDeletePercent*live {
		return HoldBulkDelete
	}
	return ""
}

func (n *Node) safetyFor(shareID string) config.Safety {
	n.cfgMu.RLock()
	defer n.cfgMu.RUnlock()
	return n.cfg.SafetyFor(shareID)
}

func (n *Node) getHold(shareID, peerKey string) (index.Hold, bool) {
	n.holdsMu.Lock()
	defer n.holdsMu.Unlock()
	h, ok := n.holds[holdKey{shareID, peerKey}]
	return h, ok
}

// placeHold records a hold, logging it the first time. A hold that is
// already in place keeps its original Since, so the UI shows how long the
// share has been stuck, not when it last re-tripped.
func (n *Node) placeHold(h index.Hold) error {
	key := holdKey{h.ShareID, h.PeerKey}
	n.holdsMu.Lock()
	defer n.holdsMu.Unlock()
	old, existed := n.holds[key]
	if existed {
		if old.Reason == h.Reason && old.Deletes == h.Deletes && old.Live == h.Live {
			return nil
		}
		h.Since = old.Since
	}
	if err := n.store.PutHold(n.ctx, h); err != nil {
		return err
	}
	n.holds[key] = h
	if !existed {
		n.logger.Printf("core: share %s on hold%s: %s (%d of %d file(s) would be deleted); fix the folder, or confirm with `syncat share confirm-deletes`",
			h.ShareID, n.holdPeerSuffix(h.PeerKey), h.Reason, h.Deletes, h.Live)
	}
	return nil
}

// clearHold removes the hold on (shareID, peerKey), if there is one, and
// reports whether there was.
func (n *Node) clearHold(shareID, peerKey, why string) bool {
	key := holdKey{shareID, peerKey}
	n.holdsMu.Lock()
	defer n.holdsMu.Unlock()
	if _, ok := n.holds[key]; !ok {
		return false
	}
	if err := n.store.DeleteHold(n.ctx, shareID, peerKey); err != nil {
		n.logger.Printf("core: clear hold on share %s: %v", shareID, err)
		return false
	}
	delete(n.holds, key)
	n.logger.Printf("core: share %s hold%s cleared: %s", shareID, n.holdPeerSuffix(peerKey), why)
	return true
}

func (n *Node) holdPeerSuffix(peerKey string) string {
	if peerKey == "" {
		return ""
	}
	if pc := n.lookupPeer(peerKey); pc != nil {
		return " for changes from " + pc.name
	}
	return " for changes from peer " + peerKey
}

// liveFileCount counts existing's live files: not tombstones, not parked,
// not directories. It is the denominator every hold trigger uses.
func liveFileCount(existing map[string]index.FileRow) int {
	n := 0
	for _, r := range existing {
		if !r.Deleted && !r.Ignored && r.Type != protocol.FileTypeDir {
			n++
		}
	}
	return n
}

// rootUnavailable reports whether a scan failed because the share's root
// itself is missing or unreadable, rather than for some transient reason.
func rootUnavailable(root string) bool {
	f, err := os.Open(root)
	if err != nil {
		return true
	}
	defer f.Close()
	_, err = f.Readdirnames(1)
	return err != nil && !errors.Is(err, io.EOF)
}

// holdForScan decides whether a local scan result may be applied. It
// returns true when the scan is held and must be dropped. A confirmed hold
// lets this one scan through and clears.
func (n *Node) holdForScan(shareID string, result *index.ScanResult, existing map[string]index.FileRow) bool {
	deletes := 0
	for _, r := range result.Deleted {
		if r.Type != protocol.FileTypeDir {
			deletes++
		}
	}
	live := liveFileCount(existing)
	reason := holdReason(n.safetyFor(shareID), deletes, live)
	if reason == "" {
		n.clearLocalHold(shareID, "the folder looks normal again")
		return false
	}
	if h, ok := n.getHold(shareID, ""); ok && h.Confirmed {
		n.clearLocalHold(shareID, "deletes confirmed")
		return false
	}
	if err := n.placeHold(index.Hold{ShareID: shareID, Reason: reason, Deletes: deletes, Live: live, Since: n.clock.Now()}); err != nil {
		n.logger.Printf("core: hold share %s: %v", shareID, err)
	}
	return true
}

// holdForScanError places a root-unavailable hold when a scan failed
// because the share's root is gone while the index still has live files.
func (n *Node) holdForScanError(shareID, root string, existing map[string]index.FileRow) {
	live := liveFileCount(existing)
	if live == 0 || !rootUnavailable(root) {
		return
	}
	if err := n.placeHold(index.Hold{ShareID: shareID, Reason: HoldRootUnavailable, Deletes: live, Live: live, Since: n.clock.Now()}); err != nil {
		n.logger.Printf("core: hold share %s: %v", shareID, err)
	}
}

// clearLocalHold clears this node's own hold on shareID and, if there was
// one, re-runs the reconcile passes it stopped, so peers' changes held back
// in the meantime are applied without waiting for them to send more.
func (n *Node) clearLocalHold(shareID, why string) {
	if !n.clearHold(shareID, "", why) {
		return
	}
	n.reconcileShareOnSessions(shareID)
}

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

// shareGuardFunc returns the syncsvc.ShareConfig.Guard for shareID on the
// session with peerKey. It refuses every pass while this node's own copy
// of the share is held, and otherwise holds the peer's link when its
// change trips a trigger.
func (n *Node) shareGuardFunc(shareID, peerKey string) func(deletes, live int) bool {
	return func(deletes, live int) bool {
		if _, held := n.getHold(shareID, ""); held {
			return false
		}
		reason := holdReason(n.safetyFor(shareID), deletes, live)
		if reason == "" {
			n.clearHold(shareID, peerKey, "the peer's changes look normal again")
			return true
		}
		if h, ok := n.getHold(shareID, peerKey); ok && h.Confirmed {
			n.clearHold(shareID, peerKey, "deletes confirmed")
			return true
		}
		if err := n.placeHold(index.Hold{ShareID: shareID, PeerKey: peerKey, Reason: reason, Deletes: deletes, Live: live, Since: n.clock.Now()}); err != nil {
			n.logger.Printf("core: hold share %s: %v", shareID, err)
		}
		return false
	}
}

// HoldStatus is one hold as the API and UI show it.
type HoldStatus struct {
	ShareID   string
	ShareName string
	PeerKey   string // "" for this node's own copy
	PeerName  string
	Reason    string
	Deletes   int
	Live      int
	Since     time.Time
	Confirmed bool

	// Confirmable is false for a root-unavailable hold, which has no
	// change to let through.
	Confirmable bool
}

// Holds returns every current hold, oldest first.
func (n *Node) Holds() []HoldStatus {
	n.holdsMu.Lock()
	list := make([]index.Hold, 0, len(n.holds))
	for _, h := range n.holds {
		list = append(list, h)
	}
	n.holdsMu.Unlock()
	sort.Slice(list, func(i, j int) bool {
		if !list[i].Since.Equal(list[j].Since) {
			return list[i].Since.Before(list[j].Since)
		}
		return list[i].ShareID+list[i].PeerKey < list[j].ShareID+list[j].PeerKey
	})

	out := make([]HoldStatus, 0, len(list))
	for _, h := range list {
		hs := HoldStatus{
			ShareID: h.ShareID, ShareName: n.shareDisplayName(h.ShareID),
			PeerKey: h.PeerKey, Reason: h.Reason, Deletes: h.Deletes, Live: h.Live,
			Since: h.Since, Confirmed: h.Confirmed, Confirmable: holdConfirmable(h.Reason),
		}
		if pc := n.lookupPeer(h.PeerKey); pc != nil {
			hs.PeerName = pc.name
		}
		out = append(out, hs)
	}
	return out
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

// ConfirmHold lets a held change through: the next scan (for this node's
// own hold) or reconcile pass (for a peer's) that would have been held
// applies instead. peerRef names the peer whose hold to confirm; it may be
// empty when the share has only one hold.
func (n *Node) ConfirmHold(ctx context.Context, shareRef, peerRef string) (HoldStatus, error) {
	shareID, err := n.resolveShareOrSubscriptionRef(shareRef)
	if err != nil {
		return HoldStatus{}, fmt.Errorf("core: confirm hold: %w", err)
	}

	n.holdsMu.Lock()
	var candidates []index.Hold
	for _, h := range n.holds {
		if h.ShareID == shareID {
			candidates = append(candidates, h)
		}
	}
	n.holdsMu.Unlock()

	var h index.Hold
	switch {
	case peerRef != "":
		pc, err := n.resolvePeerRef(peerRef)
		if err != nil {
			return HoldStatus{}, mutationError(MutationInvalid, fmt.Errorf("core: confirm hold: %w", err))
		}
		found := false
		for _, c := range candidates {
			if c.PeerKey == pc.peerKeyHex {
				h, found = c, true
			}
		}
		if !found {
			return HoldStatus{}, mutationErrorf(MutationNotFound, "share %s is not on hold for changes from %s", shareID, pc.name)
		}
	case len(candidates) == 1:
		h = candidates[0]
	case len(candidates) == 0:
		return HoldStatus{}, mutationErrorf(MutationNotFound, "share %s is not on hold", shareID)
	default:
		return HoldStatus{}, mutationErrorf(MutationInvalid, "share %s has %d holds; name the peer whose changes to confirm", shareID, len(candidates))
	}
	switch h.Reason {
	case HoldRootUnavailable:
		return HoldStatus{}, mutationErrorf(MutationInvalid, "share %s is on hold because its folder is missing or unreadable; there is nothing to confirm. Restore the folder, or remove the share", shareID)
	case HoldReservedDirTaken:
		return HoldStatus{}, mutationErrorf(MutationInvalid, "share %s is on hold because its folder already has a %s directory that syncat did not create; there is nothing to confirm. Move that directory away", shareID, syncsvc.ReservedDirName)
	}

	h.Confirmed = true
	if err := n.store.PutHold(ctx, h); err != nil {
		return HoldStatus{}, fmt.Errorf("core: confirm hold: %w", err)
	}
	n.holdsMu.Lock()
	n.holds[holdKey{h.ShareID, h.PeerKey}] = h
	n.holdsMu.Unlock()

	if h.PeerKey == "" {
		if err := n.rescanShare(ctx, shareID); err != nil {
			n.logger.Printf("core: confirm hold: rescan %s: %v", shareID, err)
		}
	} else if pc := n.lookupPeer(h.PeerKey); pc != nil {
		if sess := pc.activeSession(shareID); sess != nil {
			if err := sess.ReconcileShare(ctx, shareID); err != nil {
				n.logger.Printf("core: confirm hold: reconcile %s with %s: %v", shareID, pc.name, err)
			}
		}
	}

	for _, hs := range n.Holds() {
		if hs.ShareID == h.ShareID && hs.PeerKey == h.PeerKey {
			return hs, nil
		}
	}
	// The confirmed pass already ran and cleared the hold.
	return HoldStatus{ShareID: h.ShareID, PeerKey: h.PeerKey, Reason: h.Reason, Deletes: h.Deletes, Live: h.Live, Since: h.Since, Confirmed: true, Confirmable: true}, nil
}

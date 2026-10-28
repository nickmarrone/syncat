package core

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/nickmarrone/syncat/internal/config"
	"github.com/nickmarrone/syncat/internal/index"
	"github.com/nickmarrone/syncat/internal/protocol"
	syncsvc "github.com/nickmarrone/syncat/internal/sync"
)

// This file is Node's side of the trash (see GLOSSARY.md and
// internal/sync/trash.go): listing and undoing a share's trash events,
// expiring them on the share's retention and size limits, and moving the
// old data-dir trash into the shares once.

// trashSweepInterval is how often every share's trash is checked against
// its retention and size limits. Hourly rather than daily because the size
// limit is the one that matters when a big change lands, and a day is a
// long time for the trash to sit over it.
const trashSweepInterval = time.Hour

// shareOrSubscriptionRoot resolves shareID to the local directory it
// syncs: the share's own path if this node offers it, or the local
// subscription path if this node subscribes to it from a peer.
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
	return "", mutationErrorf(MutationNotFound, "share %s is not a local share or subscription", shareID)
}

// TrashStatus is one share's trash, as the API and UI show it.
type TrashStatus struct {
	ShareID string
	Events  []syncsvc.TrashEventInfo // newest first
	Size    int64                    // bytes the trash holds
	Limit   int64                    // the size limit in bytes, from the share's trash_max_size
	// RetentionDays is how long an event is kept, at most.
	RetentionDays int
}

// ListTrash returns a share's trash events, newest first, with its size
// against its limit.
func (n *Node) ListTrash(ctx context.Context, shareRef string) (TrashStatus, error) {
	shareID, err := n.resolveShareOrSubscriptionRef(shareRef)
	if err != nil {
		return TrashStatus{}, fmt.Errorf("core: list trash: %w", err)
	}
	root, err := n.shareOrSubscriptionRoot(shareID)
	if err != nil {
		return TrashStatus{}, fmt.Errorf("core: list trash: %w", err)
	}
	events, err := n.trash.List(root)
	if err != nil {
		return TrashStatus{}, fmt.Errorf("core: list trash: %w", err)
	}
	st := TrashStatus{ShareID: shareID, Events: events}
	for _, ev := range events {
		st.Size += ev.Size
	}
	safety := n.safetyFor(shareID)
	st.RetentionDays = safety.TrashRetentionDays
	if st.Limit, err = n.trashLimit(ctx, shareID, safety); err != nil {
		return TrashStatus{}, fmt.Errorf("core: list trash: %w", err)
	}
	return st, nil
}

// trashLimit resolves a share's trash_max_size to bytes against the total
// size of its live files.
func (n *Node) trashLimit(ctx context.Context, shareID string, s config.Safety) (int64, error) {
	max, err := config.ParseTrashMaxSize(s.TrashMaxSize)
	if err != nil {
		return 0, err
	}
	if max.Percent == 0 {
		return max.Bytes, nil
	}
	rows, err := n.store.ListShare(ctx, shareID, false)
	if err != nil {
		return 0, err
	}
	var live int64
	for _, r := range rows {
		if r.Type != protocol.FileTypeDir {
			live += r.Size
		}
	}
	return max.Limit(live), nil
}

// UndoTrash restores files from one trash event: the paths in relpaths, or
// the whole event if relpaths is empty. A file each restore replaces goes
// into the trash itself, so an undo can be undone. The restored files are
// then sent to the share's peers as new changes.
func (n *Node) UndoTrash(ctx context.Context, shareRef, eventID string, relpaths []string) ([]index.FileRow, error) {
	shareID, err := n.resolveShareOrSubscriptionRef(shareRef)
	if err != nil {
		return nil, fmt.Errorf("core: undo trash: %w", err)
	}
	root, err := n.shareOrSubscriptionRoot(shareID)
	if err != nil {
		return nil, fmt.Errorf("core: undo trash: %w", err)
	}
	rows, restoreErr := n.trash.Restore(ctx, n.store, n.identity.ShortID(), shareID, root, eventID, relpaths)
	if len(rows) > 0 {
		// Restore already wrote the bumped rows, so a rescan would find
		// nothing new to send; push them out directly.
		n.propagateShare(ctx, shareID)
		n.debugf("core: restored %d file(s) of %s from trash event %q", len(rows), shareID, eventID)
	}
	if restoreErr != nil {
		switch {
		case errors.Is(restoreErr, syncsvc.ErrTrashEntryNotFound):
			return rows, mutationError(MutationNotFound, fmt.Errorf("core: undo trash: %w", restoreErr))
		case errors.Is(restoreErr, syncsvc.ErrRestoreDestExists):
			return rows, mutationError(MutationConflict, fmt.Errorf("core: undo trash: %w", restoreErr))
		}
		return rows, fmt.Errorf("core: undo trash: %w", restoreErr)
	}
	return rows, nil
}

// startTrashSweeper expires trash events on trashSweepInterval for the
// life of the node. Like the journal sweeper, the first pass is one
// interval after startup.
func (n *Node) startTrashSweeper() {
	n.goTracked(func() {
		for {
			select {
			case <-n.ctx.Done():
				return
			case <-n.clock.After(trashSweepInterval):
				n.sweepTrash()
			}
		}
	})
}

// sweepTrash runs one expiry pass over every watched share's trash.
//
// Expiry deletes the user's last copy of a file, so every event it removes
// gets a log line, and so does every failure; a pass that removes nothing
// (the usual case) stays quiet.
func (n *Node) sweepTrash() {
	n.sharesMu.Lock()
	roots := make(map[string]string, len(n.shareWatches))
	for id, sw := range n.shareWatches {
		roots[id] = sw.root
	}
	n.sharesMu.Unlock()

	for shareID, root := range roots {
		safety := n.safetyFor(shareID)
		limit, err := n.trashLimit(n.ctx, shareID, safety)
		if err != nil {
			n.logger.Printf("core: trash sweep of %s failed: %v", shareID, err)
			continue
		}
		res, err := n.trash.Sweep(root, time.Duration(safety.TrashRetentionDays)*24*time.Hour, limit)
		if err != nil {
			n.logger.Printf("core: trash sweep of %s failed after removing %d event%s: %v", shareID, res.Purged, plural(res.Purged, "", "s"), err)
			continue
		}
		if res.Purged > 0 {
			n.logger.Printf("core: trash sweep removed %d event%s from %s (older than %d days, or over the %d-byte limit)",
				res.Purged, plural(res.Purged, "", "s"), shareID, safety.TrashRetentionDays, limit)
		}
	}
}

// migrateLegacyTrash moves the data-dir trash older versions kept, at
// <datadir>/trash/<share-id>/, into each share's own trash, once, before
// any session can start applying changes. Anything it can't move (a share
// that no longer exists, a file that fails to move) stays where it is and
// is logged; nothing is deleted that wasn't moved.
func (n *Node) migrateLegacyTrash(cfg *config.Config) {
	legacyRoot := n.paths.LegacyTrashDir()
	entries, err := os.ReadDir(legacyRoot)
	if err != nil {
		if !os.IsNotExist(err) {
			n.logger.Printf("core: open: read old trash %s: %v", legacyRoot, err)
		}
		return
	}
	roots := map[string]string{}
	for _, s := range cfg.Shares {
		roots[s.ID] = s.Path
	}
	for _, sub := range cfg.Subscriptions {
		roots[sub.ShareID] = sub.LocalPath
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		legacyDir := filepath.Join(legacyRoot, e.Name())
		root, ok := roots[e.Name()]
		if !ok {
			n.logger.Printf("core: open: old trash for share %s, which is no longer configured, left at %s; delete it once you're sure you don't need it", e.Name(), legacyDir)
			continue
		}
		if err := syncsvc.EnsureReservedDir(root); err != nil {
			n.logger.Printf("core: open: old trash for %s left at %s: %v", e.Name(), legacyDir, err)
			continue
		}
		moved, err := n.trash.MigrateLegacyTrash(n.ctx, legacyDir, root)
		if moved > 0 {
			n.logger.Printf("core: open: moved %d file(s) of old trash for %s into %s", moved, e.Name(), filepath.Join(root, syncsvc.ReservedDirName, "trash"))
		}
		if err != nil {
			n.logger.Printf("core: open: some old trash for %s could not be moved and is still at %s: %v", e.Name(), legacyDir, err)
		}
	}
	_ = os.Remove(legacyRoot) // only succeeds once everything has moved out
}

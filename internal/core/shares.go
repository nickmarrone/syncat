package core

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/nickmarrone/syncat/internal/index"
	syncsvc "github.com/nickmarrone/syncat/internal/sync"
)

// This file wires each locally-relevant directory — a share we offer, or a
// subscription's local copy — to internal/index's Scanner/Watcher and
// keeps the index in sync with disk (SPEC.md §5 "Scanning"), independent
// of which (if any) peers are currently connected. rescanShare is the
// bridge from "the filesystem changed" to "our peers find out": it scans,
// bumps the version vector for whatever actually changed locally (SPEC.md
// §5: "local modification bumps the local counter"), persists it, and —
// only if something changed — re-syncs the share on every session that
// currently has it active (propagateShare).

// startShareWatch begins watching root for shareID: an initial fsnotify
// watch tree plus the periodic full rescan that remains the source of
// truth (SPEC.md §5). It does not itself perform the first scan — callers
// that want one immediately (Open, AddShare, AddSubscription) call
// rescanShare explicitly afterward.
func (n *Node) startShareWatch(shareID, root string, role shareRole) (*shareWatch, error) {
	n.cfgMu.RLock()
	ignores := append([]string(nil), n.cfg.GlobalIgnores...)
	rescanSeconds := n.cfg.RescanIntervalSeconds
	n.cfgMu.RUnlock()

	matcher := index.NewMatcher(ignores)
	scanner := index.NewScanner(os.DirFS(root), matcher)

	watcher, err := index.NewWatcher(index.WatcherOptions{
		Root:           root,
		Clock:          asIndexClock(n.clock),
		RescanInterval: time.Duration(rescanSeconds) * time.Second,
		OnDirty:        func([]string) { n.rescanShareAsync(shareID) },
		OnPeriodic:     func() { n.rescanShareAsync(shareID) },
		OnWarn: func(format string, args ...any) {
			n.logger.Printf("core: watcher %s: "+format, append([]any{shareID}, args...)...)
		},
	})
	if err != nil {
		return nil, fmt.Errorf("core: start watcher for %s: %w", shareID, err)
	}

	sw := &shareWatch{shareID: shareID, root: root, role: role, scanner: scanner, watcher: watcher}
	n.sharesMu.Lock()
	if old, ok := n.shareWatches[shareID]; ok {
		// Replacing an existing watch (e.g. AddShare called again for a
		// share id that somehow already had one — shouldn't happen given
		// config validation, but never leak the old watcher's goroutines).
		n.sharesMu.Unlock()
		old.watcher.Close()
		n.sharesMu.Lock()
	}
	n.shareWatches[shareID] = sw
	n.sharesMu.Unlock()
	return sw, nil
}

// stopShareWatch stops and removes shareID's watcher, if any. Safe to call
// for a shareID with no watch (a no-op).
func (n *Node) stopShareWatch(shareID string) {
	n.sharesMu.Lock()
	sw, ok := n.shareWatches[shareID]
	delete(n.shareWatches, shareID)
	n.sharesMu.Unlock()
	if !ok {
		return
	}
	if err := sw.watcher.Close(); err != nil {
		n.logger.Printf("core: close watcher for %s: %v", shareID, err)
	}
}

// rescanShareAsync triggers a background rescan, tracked by n.wg so Close
// waits for it. Used by watcher callbacks (OnDirty/OnPeriodic), which
// must never block the watcher's own goroutine.
func (n *Node) rescanShareAsync(shareID string) {
	n.goTracked(func() {
		if n.ctx.Err() != nil {
			return
		}
		if err := n.rescanShare(n.ctx, shareID); err != nil {
			n.logger.Printf("core: rescan %s: %v", shareID, err)
		}
	})
}

// rescanShare runs one full scan of shareID's directory (SPEC.md §5),
// bumps the local version-vector counter for every row that actually
// changed content (Added/ContentChanged/Deleted — not MetadataOnly, which
// carries no content change: see index.Scanner.Scan's doc comment),
// persists the result, and propagates it to every session currently
// syncing this share.
func (n *Node) rescanShare(ctx context.Context, shareID string) error {
	n.sharesMu.Lock()
	sw, ok := n.shareWatches[shareID]
	n.sharesMu.Unlock()
	if !ok {
		return fmt.Errorf("core: rescan: %s has no active watch", shareID)
	}

	existing, err := n.store.ListShareMap(ctx, shareID)
	if err != nil {
		return fmt.Errorf("core: rescan %s: list existing: %w", shareID, err)
	}
	result, err := sw.scanner.Scan(ctx, shareID, existing)
	if err != nil {
		return fmt.Errorf("core: rescan %s: scan: %w", shareID, err)
	}

	nodeID := n.identity.ShortID()
	bump := func(rows []index.FileRow) {
		for i := range rows {
			rows[i].Version = syncsvc.Bump(rows[i].Version, nodeID)
		}
	}
	bump(result.Added)
	bump(result.ContentChanged)
	bump(result.Deleted)

	if err := n.store.ApplyScanResult(ctx, result); err != nil {
		return fmt.Errorf("core: rescan %s: apply: %w", shareID, err)
	}

	if len(result.Added) == 0 && len(result.ContentChanged) == 0 && len(result.Deleted) == 0 {
		return nil
	}
	n.propagateShare(ctx, shareID)
	return nil
}

// propagateShare re-sends a full IndexUpdate for shareID (Session.SyncShare
// only offers a full-snapshot resend, not a delta — see its doc comment)
// to every currently-connected session that has this share active, i.e.
// every peer with granted access to a share we offer, or the single
// offerer of a subscription (SPEC.md §5 "Fan-out": a share offered to
// multiple peers propagates through the offerer; peers of the same share
// never sync directly with each other).
func (n *Node) propagateShare(ctx context.Context, shareID string) {
	for _, pc := range n.snapshotPeers() {
		pc.mu.Lock()
		sess := pc.session
		active := pc.activeShares != nil && pc.activeShares[shareID]
		pc.mu.Unlock()
		if sess == nil || !active {
			continue
		}
		if err := sess.SyncShare(ctx, shareID); err != nil {
			n.logger.Printf("core: propagate %s to %s: %v", shareID, pc.name, err)
		}
	}
}

// RescanShare runs an immediate, synchronous full scan of shareID and
// propagates the result, exactly like the periodic/fsnotify-triggered
// path. Exposed for Phase 8's CLI/API (a manual "rescan now") and for
// tests that want deterministic sync timing without waiting on the
// watcher's real debounce/periodic timers.
func (n *Node) RescanShare(ctx context.Context, shareID string) error {
	return n.rescanShare(ctx, shareID)
}

package core

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
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

// shareWatch is one local directory Node keeps indexed: either a share we
// offer (root = config.Share.Path) or a subscription's local copy (root =
// config.Subscription.LocalPath).
type shareWatch struct {
	shareID string
	root    string
	watcher *index.Watcher

	// mu guards the ignore matcher and the scanner built from it. Both
	// are replaced whenever the share's .syncatignore changes, which can
	// happen at any time while sessions are reading the matcher through
	// the predicate Node.shareIgnoreFunc hands to internal/sync.
	mu         sync.RWMutex
	scanner    *index.Scanner
	ignore     *index.Matcher
	ignoreSize int64 // .syncatignore's size and mtime at last load; both
	ignoreMod  int64 // zero when the file is absent
}

// startShareWatch begins watching root for shareID: an initial fsnotify
// watch tree plus the periodic full rescan that remains the source of
// truth (SPEC.md §5). It does not itself perform the first scan — callers
// that want one immediately (Open, AddShare, AddSubscription) call
// rescanShare explicitly afterward.
func (n *Node) startShareWatch(shareID, root string) (*shareWatch, error) {
	n.cfgMu.RLock()
	rescanSeconds := n.cfg.RescanIntervalSeconds
	n.cfgMu.RUnlock()

	sw := &shareWatch{shareID: shareID, root: root}
	n.refreshIgnore(sw)

	watcher, err := index.NewWatcher(index.WatcherOptions{
		Root:  root,
		Clock: asIndexClock(n.clock),
		// Closed over sw directly, not via n.shareIgnoreFunc: the
		// watcher builds its initial watch tree inside NewWatcher, which
		// runs before sw is registered in n.shareWatches, so a map
		// lookup would find nothing and silently skip the pruning on the
		// one tree where it matters most.
		Ignore:         sw.matches,
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

	sw.watcher = watcher
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

// refreshIgnore (re)loads sw's share-root .syncatignore and rebuilds the
// matcher and scanner from it, combined with the current global ignore
// list. It is cheap and idempotent: when the file's size and mtime are
// unchanged since the last load, and the global list has not been
// swapped, nothing is rebuilt.
//
// Reloading here — from rescanShare — rather than from a dedicated watcher
// hook is deliberate. rescanShare is the single funnel every trigger
// already passes through (startup, the fsnotify debounce, and the periodic
// scan that SPEC.md §5 calls the source of truth), and editing
// .syncatignore is itself an event in the share root, so a change takes
// effect one debounce window later with no new plumbing and no ordering
// hazard against a scan already in flight.
func (n *Node) refreshIgnore(sw *shareWatch) {
	n.cfgMu.RLock()
	ignores := append([]string(nil), n.cfg.GlobalIgnores...)
	n.cfgMu.RUnlock()

	path := filepath.Join(sw.root, index.IgnoreFileName)
	var size, mod int64
	if fi, err := os.Stat(path); err == nil && !fi.IsDir() {
		size, mod = fi.Size(), fi.ModTime().UnixNano()
	}

	sw.mu.RLock()
	unchanged := sw.ignore != nil && sw.ignoreSize == size && sw.ignoreMod == mod
	sw.mu.RUnlock()
	if unchanged {
		return
	}

	var rules *index.IgnoreRules
	if size != 0 || mod != 0 {
		f, err := os.Open(path)
		if err != nil {
			// Keep whatever we last compiled rather than falling back to
			// "ignore nothing": the safe direction for an unreadable
			// ignore file is to keep excluding what the user excluded,
			// not to silently start indexing and publishing all of it.
			n.logger.Printf("core: share %s: read %s: %v (keeping previous ignore rules)", sw.shareID, index.IgnoreFileName, err)
			return
		}
		parsed, warnings, err := index.ParseIgnore(f)
		f.Close()
		if err != nil {
			n.logger.Printf("core: share %s: parse %s: %v (keeping previous ignore rules)", sw.shareID, index.IgnoreFileName, err)
			return
		}
		for _, w := range warnings {
			n.logger.Printf("core: share %s: %s %s", sw.shareID, index.IgnoreFileName, w)
		}
		rules = parsed
	}

	matcher := index.NewMatcherWithRules(ignores, rules)
	sw.mu.Lock()
	sw.ignore = matcher
	sw.scanner = index.NewScanner(os.DirFS(sw.root), matcher)
	sw.ignoreSize, sw.ignoreMod = size, mod
	sw.mu.Unlock()
}

// shareIgnoreFunc returns the predicate internal/sync (and the watcher)
// consult to decide whether a share-relative path is excluded. It resolves
// the share's current matcher on every call, so an edit to .syncatignore
// takes effect without re-adding the share or reconnecting a session.
// Paths under a share with no local watch are never ignored.
func (n *Node) shareIgnoreFunc(shareID string) func(relpath string, isDir bool) bool {
	return func(relpath string, isDir bool) bool {
		n.sharesMu.Lock()
		sw, ok := n.shareWatches[shareID]
		n.sharesMu.Unlock()
		if !ok {
			return false
		}
		return sw.matches(relpath, isDir)
	}
}

// matches reports whether relpath is excluded by sw's current ignore rules.
func (sw *shareWatch) matches(relpath string, isDir bool) bool {
	sw.mu.RLock()
	m := sw.ignore
	sw.mu.RUnlock()
	return m.MatchPath(relpath, isDir)
}

// currentScanner returns the scanner built from sw's latest ignore rules.
func (sw *shareWatch) currentScanner() *index.Scanner {
	sw.mu.RLock()
	defer sw.mu.RUnlock()
	return sw.scanner
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

	// Pick up any edit to .syncatignore before this scan reads the tree,
	// so a rule the user just saved takes effect on this pass rather than
	// the next one.
	n.refreshIgnore(sw)

	existing, err := n.store.ListShareMap(ctx, shareID)
	if err != nil {
		return fmt.Errorf("core: rescan %s: list existing: %w", shareID, err)
	}
	result, err := sw.currentScanner().Scan(ctx, shareID, existing)
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

	if len(result.Ignored) > 0 {
		// Not propagated as a change in its own right: the rows were
		// dropped from the index without a tombstone precisely so that
		// peers are not told to delete anything (see
		// index.ScanResult.Ignored). Peers simply stop seeing these paths
		// in our next snapshot and keep their copies.
		n.logger.Printf("core: rescan %s: %d path(s) newly ignored, dropped from the index (no tombstones; peers keep their copies)", shareID, len(result.Ignored))
	}

	if len(result.Added) == 0 && len(result.ContentChanged) == 0 && len(result.Deleted) == 0 {
		return nil
	}
	// Only reached when the scan found a real content change, so this does
	// not fire on the idle periodic rescan — but a busy share still rescans
	// on every fsnotify batch, which is why it is behind the debug flag.
	// The counts are the useful part: they are exactly what is about to be
	// pushed to every peer, so a share that keeps re-scanning the same file
	// is visible here and nowhere else.
	if n.debugEnabled() {
		n.logger.Printf("core: rescan %s: %d added, %d changed, %d deleted; propagating", shareID, len(result.Added), len(result.ContentChanged), len(result.Deleted))
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
	n.propagateShareExcept(ctx, shareID, nil)
}

// propagateShareExcept is propagateShare with one peer left out: the one a
// change just arrived from, which its own Session has already answered with
// a delta at the end of handleIndexUpdate.
//
// This is the other half of SPEC.md §5's fan-out rule. A change that
// originates locally reaches every peer through rescanShare, but a change
// *pulled* from one peer reaches the rest only through here — see
// syncsvc.Session.SetAppliedHandler for why no rescan will do it, and
// scripts/net/08-three-node-fanout.sh for the case that proves it.
func (n *Node) propagateShareExcept(ctx context.Context, shareID string, except *peerConn) {
	for _, pc := range n.snapshotPeers() {
		if pc == except {
			continue
		}
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

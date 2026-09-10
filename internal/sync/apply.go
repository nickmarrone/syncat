// apply.go holds the Session methods that write reconcile results to
// disk: executing each [Action] against the share directory (installing
// pulled content atomically, deleting, preserving conflict copies) and
// routing every destructive step through the trash hook. session.go owns
// the Session type and drives these; transfer.go moves the bytes.
package sync

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/nickmarrone/syncat/internal/index"
	"github.com/nickmarrone/syncat/internal/protocol"
)

// --- applying actions to disk ------------------------------------------

// applyAction executes one reconciled Action against the filesystem,
// returning the index.FileRow(s) that must now be persisted (empty/nil
// if nothing changed — e.g. ActionNone, ActionLocallyModified, or a non-empty
// directory delete that was skipped). The caller (handleIndexUpdate)
// persists the returned rows via Store.PutFile and, unless outbound is
// blocked, reports them back to the peer.
// TempFilePrefix is the name prefix apply gives its staging files. It is
// exported so internal/core can sweep orphans (see SweepTempFiles) and so
// path.go's validation and index's ignore list can agree on one spelling.
const TempFilePrefix = ".syncat.tmp."

// SweepTempFiles removes staging files left in root by transfers that never
// finished, and returns how many it removed.
//
// Apply stages a download in a temp file beside its destination and only
// renames it into place once the content verifies, so a transfer interrupted
// part-way leaves that file behind. In-process failures unwind and clean up
// after themselves; a killed daemon cannot, and the orphan is then permanent.
// That is not cosmetic for a tool built to move large files: the leftover is
// full-size, it sits in the user's own synced directory, and it is hidden, so
// nothing draws attention to it while it accumulates one copy per crash.
//
// Meant to be called at startup, before any session exists — with no transfer
// in flight, every file matching the prefix is by definition an orphan.
// Calling it while a transfer is running would delete that transfer's staging
// file, so don't.
func SweepTempFiles(root string) (int, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("sync: sweep temp files in %s: %w", root, err)
	}
	removed := 0
	var firstErr error
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() {
			// Staging files are written beside their destination, so a
			// nested share needs the subdirectories swept too.
			n, err := SweepTempFiles(filepath.Join(root, name))
			removed += n
			if err != nil && firstErr == nil {
				firstErr = err
			}
			continue
		}
		if !strings.HasPrefix(name, TempFilePrefix) {
			continue
		}
		if err := os.Remove(filepath.Join(root, name)); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("sync: sweep temp files in %s: %w", root, err)
			continue
		}
		removed++
	}
	return removed, firstErr
}

func (s *Session) applyAction(ctx context.Context, cfg ShareConfig, a Action) ([]index.FileRow, error) {
	now := time.Now()

	switch a.Kind {
	case ActionNone:
		return nil, nil

	case ActionLocallyModified:
		return s.applyLocallyModified(ctx, cfg, a, now)

	case ActionPull:
		if a.Source == SourceRemote {
			if err := s.pullAndInstall(ctx, cfg.ShareID, cfg.Root, a.RelPath, a.RelPath, a.Resolved, a.SourceVersion); err != nil {
				return nil, err
			}
		}
		return []index.FileRow{index.FileRowFromInfo(cfg.ShareID, a.Resolved, now)}, nil

	case ActionResurrect:
		if a.Source == SourceRemote {
			if err := s.pullAndInstall(ctx, cfg.ShareID, cfg.Root, a.RelPath, a.RelPath, a.Resolved, a.SourceVersion); err != nil {
				return nil, err
			}
		}
		// SourceLocal: the surviving modification is already the current
		// local file; only the index's version vector needs to advance.
		return []index.FileRow{index.FileRowFromInfo(cfg.ShareID, a.Resolved, now)}, nil

	case ActionDelete:
		deleted, err := s.applyDelete(ctx, cfg, a.RelPath)
		if err != nil {
			return nil, err
		}
		if !deleted {
			// Non-empty directory: kept, per SPEC.md §5. Don't record a
			// tombstone for content that's still actually there.
			return nil, nil
		}
		return []index.FileRow{index.FileRowFromInfo(cfg.ShareID, a.Resolved, now)}, nil

	case ActionConflictCopy:
		return s.applyConflictCopy(ctx, cfg, a, now)

	default:
		return nil, fmt.Errorf("sync: apply: unknown action kind %v for %s", a.Kind, a.RelPath)
	}
}

// applyLocallyModified executes an ActionLocallyModified: SPEC.md §1's
// receive-only revert. A receive-only subscriber's local edit is never
// propagated outward (that's what OutboundBlocked already prevents in
// reconcile.go); this is the other half — reverting the local edit itself
// once the offerer's own copy has something new to overwrite it with.
//
// reconcile.go raises ActionLocallyModified in two situations, only one of
// which carries real remote content to revert to:
//
//   - a.Source == SourceRemote: the offerer's copy is concurrently
//     different from ours (reconcileConcurrent's receive-only branch).
//     a.Resolved is the offerer's current content — exactly what SPEC.md
//     §1 says to overwrite the local edit with "after a trash copy is
//     taken". This goes through pullAndInstall exactly like an ordinary
//     ActionPull would, which is what actually takes the trash copy (via
//     the same trashHook call site every other overwrite uses) before the
//     new content lands — so the abort-on-trash-failure guarantee applies
//     here for free, with no separate code path to keep in sync.
//   - a.Source == SourceNone (reconcileOne's !hasRemote branch): a file
//     that exists only locally under a receive-only subscription. The
//     offerer has no corresponding content at all yet, so there is
//     nothing to revert *to* — SPEC.md §1 only promises a revert "when the
//     offerer's copy next changes", which hasn't happened. This case is
//     flagged (recorded as a warning) but left on disk untouched; a later
//     IndexUpdate that gives the offerer's side real content for this
//     relpath will re-reconcile as the SourceRemote case above.
func (s *Session) applyLocallyModified(ctx context.Context, cfg ShareConfig, a Action, now time.Time) ([]index.FileRow, error) {
	w := LocallyModifiedWarning{
		ShareID: cfg.ShareID,
		RelPath: a.RelPath,
		At:      now,
		Reason:  a.Reason,
	}

	if a.Source != SourceRemote {
		// Log only when the divergence is new. This branch is re-entered
		// for the same file by every reconcile pass for as long as the
		// offerer has nothing to revert to, so logging unconditionally
		// repeated one line per stray file per pass, forever.
		if s.recordWarning(w) {
			s.logf("locally modified, flagged only (no offerer content to revert to yet): %s/%s (%s)", cfg.ShareID, a.RelPath, a.Reason)
		}
		return nil, nil
	}

	s.logf("locally modified under receive-only subscription, reverting via trash: %s/%s (%s)", cfg.ShareID, a.RelPath, a.Reason)
	if err := s.pullAndInstall(ctx, cfg.ShareID, cfg.Root, a.RelPath, a.RelPath, a.Resolved, a.SourceVersion); err != nil {
		return nil, fmt.Errorf("sync: revert locally modified %s: %w", a.RelPath, err)
	}
	w.Reverted = true
	// A revert resolves the divergence: the local copy now matches the
	// offerer's. Replace the standing warning with this one (so the UI
	// shows the revert) rather than leaving a stale "flagged" entry
	// alongside it.
	s.recordWarning(w)
	return []index.FileRow{index.FileRowFromInfo(cfg.ShareID, a.Resolved, now)}, nil
}

// applyDelete removes relpath from the filesystem, if present, and reports
// whether the delete actually happened (false for a non-empty directory,
// which SPEC.md §5 says to keep). Idempotent: a relpath that's already
// absent is reported as deleted (true), since a tombstone is still the
// correct index state.
func (s *Session) applyDelete(ctx context.Context, cfg ShareConfig, relpath string) (bool, error) {
	absPath, err := JoinSharePath(cfg.Root, relpath)
	if err != nil {
		return false, fmt.Errorf("sync: delete %s: %w", relpath, err)
	}

	fi, statErr := os.Lstat(absPath)
	if statErr != nil {
		if os.IsNotExist(statErr) {
			return true, nil
		}
		return false, fmt.Errorf("sync: delete %s: stat: %w", relpath, statErr)
	}

	if fi.IsDir() {
		// Directory deletes apply only when empty after children sync
		// (SPEC.md §5); os.Remove itself refuses a non-empty directory,
		// which is exactly the check we want.
		if err := os.Remove(absPath); err != nil {
			s.logf("keeping non-empty directory %s/%s (remote deleted it): %v", cfg.ShareID, relpath, err)
			return false, nil
		}
		return true, nil
	}

	// --- trash hook (SPEC.md §7) ---
	// The one place a file/symlink is discarded on a peer's behalf via a
	// delete: moved into <datadir>/trash/<share-id>/<relpath>.<unix-ts>
	// (Trash.Put) before the remove below ever runs. A trash failure
	// aborts here, before anything is removed, rather than risking data
	// loss because the safety net itself failed.
	if err := s.trashHook(ctx, cfg.ShareID, relpath, absPath); err != nil {
		return false, fmt.Errorf("sync: delete %s: trash: %w", relpath, err)
	}

	if err := os.Remove(absPath); err != nil && !os.IsNotExist(err) {
		return false, fmt.Errorf("sync: delete %s: %w", relpath, err)
	}
	return true, nil
}

// applyConflictCopy executes an ActionConflictCopy: the winner ends up at
// a.RelPath, the loser (preserved, not discarded) at a.ConflictRelPath.
// Exactly one of {winner, loser} is remote content and the other is
// already the current local file (reconcile.go's reconcileConcurrent never
// produces two remote or two local sides), so this is always exactly one
// pull plus, when the loser is local, one same-filesystem rename to
// preserve it before the winner's content lands on top of a.RelPath.
//
// Partial success: the two sides of a conflict copy are independent
// operations (a rename that can't fail for network reasons, and a pull
// that can — e.g. the peer's file moved on between our reconcile pass
// computing its expected version and our FileRequest reaching them, a
// legitimate race in a system with two nodes reconciling concurrently).
// If one half succeeds and the other doesn't, applyConflictCopy returns
// the row(s) for whatever half actually landed on disk alongside the
// error, rather than discarding a successful rename/pull just because its
// sibling failed. The caller persists whatever rows come back regardless
// of err (see handleIndexUpdate) so a transient failure here can never
// orphan a file that's already on disk, nor silently drop an index row
// for it. The half that failed simply gets no row; if it was the winning
// content, the next reconcile round (triggered by the peer's own delta)
// sees a plain dominates-relationship instead of a conflict and retries it
// as an ordinary ActionPull. If it was the losing content, the peer's own
// independent conflict-copy resolution (it computes its own conflict path
// concurrently, the mirror image of this one) still reaches us as an
// ordinary remote-only file on a later round, so the two sides still
// converge to the same file set either way.
func (s *Session) applyConflictCopy(ctx context.Context, cfg ShareConfig, a Action, now time.Time) ([]index.FileRow, error) {
	switch {
	case a.Source == SourceLocal && a.ConflictSource == SourceRemote:
		// Winner is already the current local file at a.RelPath — left
		// untouched, so its row is always safe to report.
		winnerInfo := a.Resolved
		winnerInfo.RelPath = a.RelPath
		winnerRow := index.FileRowFromInfo(cfg.ShareID, winnerInfo, now)

		// Pull the loser: it's addressed on the wire by the *original*
		// relpath, since that's still how the peer's own index refers to
		// it (the peer hasn't renamed anything; only our local view is
		// gaining a new conflict-copy path). a.ConflictInfo.Version is used
		// verbatim as the wire version — unlike a.Resolved, ConflictInfo is
		// never rewritten with a merged/bumped version (see its doc
		// comment in reconcile.go), so it's already exactly what the peer
		// advertised.
		if err := s.pullAndInstall(ctx, cfg.ShareID, cfg.Root, a.RelPath, a.ConflictRelPath, a.ConflictInfo, a.ConflictInfo.Version); err != nil {
			return []index.FileRow{winnerRow}, fmt.Errorf("sync: conflict copy %s: pull loser: %w", a.RelPath, err)
		}

		loserInfo := a.ConflictInfo
		loserInfo.RelPath = a.ConflictRelPath
		return []index.FileRow{winnerRow, index.FileRowFromInfo(cfg.ShareID, loserInfo, now)}, nil

	case a.Source == SourceRemote && a.ConflictSource == SourceLocal:
		// Loser is our current local content at a.RelPath: preserve it by
		// renaming it to a.ConflictRelPath *before* the winner's bytes are
		// pulled on top of a.RelPath. Same directory, same filesystem, so
		// this is a plain atomic rename — no temp file or hash check
		// needed, the bytes are already known-good local content, and (no
		// network involved) it either fully succeeds or leaves nothing
		// changed.
		srcAbs, err := JoinSharePath(cfg.Root, a.RelPath)
		if err != nil {
			return nil, fmt.Errorf("sync: conflict copy %s: %w", a.RelPath, err)
		}
		dstAbs, err := JoinSharePath(cfg.Root, a.ConflictRelPath)
		if err != nil {
			return nil, fmt.Errorf("sync: conflict copy %s: %w", a.RelPath, err)
		}
		if err := os.MkdirAll(filepath.Dir(dstAbs), 0o700); err != nil {
			return nil, fmt.Errorf("sync: conflict copy %s: mkdir: %w", a.RelPath, err)
		}
		if err := os.Rename(srcAbs, dstAbs); err != nil {
			return nil, fmt.Errorf("sync: conflict copy %s: preserve loser at %s: %w", a.RelPath, a.ConflictRelPath, err)
		}

		loserInfo := a.ConflictInfo
		loserInfo.RelPath = a.ConflictRelPath
		loserRow := index.FileRowFromInfo(cfg.ShareID, loserInfo, now)

		if err := s.pullAndInstall(ctx, cfg.ShareID, cfg.Root, a.RelPath, a.RelPath, a.Resolved, a.SourceVersion); err != nil {
			return []index.FileRow{loserRow}, fmt.Errorf("sync: conflict copy %s: pull winner: %w", a.RelPath, err)
		}

		winnerInfo := a.Resolved
		winnerInfo.RelPath = a.RelPath
		return []index.FileRow{index.FileRowFromInfo(cfg.ShareID, winnerInfo, now), loserRow}, nil

	default:
		return nil, fmt.Errorf("sync: conflict copy %s: unexpected source combination (winner=%s loser=%s)",
			a.RelPath, a.Source, a.ConflictSource)
	}
}

// pullAndInstall fetches wireRelPath's content from the peer (addressed on
// the wire exactly as the peer's own index knows it) and atomically
// installs it at share root under destRelPath — usually the same path,
// except for a conflict copy's loser (see applyConflictCopy). This is
// SPEC.md §5's "applying remote changes" sequence:
//
//  1. download to .syncat.tmp.<rand> in the destination directory (same
//     filesystem as the destination, so the final rename is atomic);
//  2. verify sha256 against what the peer advertised (info.SHA256) — a
//     mismatch aborts and never touches the destination;
//  3. fsync the temp file and set its mtime/mode;
//  4. rename(2) into place.
//
// wireVersion is the version named in the FileRequest — deliberately a
// separate parameter from info.Version, because they can legitimately
// differ: info carries the version this content will be persisted under
// locally (for a conflict resolution, Merge(local, remote) plus a local
// Bump — see Action.Resolved's doc comment), while wireVersion must be the
// version the peer actually advertised, or the peer's own freshness check
// (handleFileRequest's Equal(row.Version, req.Version) in transfer.go) can
// never match. Every caller passes the right one via Action.SourceVersion
// (or, for a conflict copy's loser, ConflictInfo.Version, which is never
// rewritten in the first place). The temp file is removed on every error
// path, including a panic, via a single deferred cleanup keyed off a
// "committed" flag.
func (s *Session) pullAndInstall(ctx context.Context, shareID, root, wireRelPath, destRelPath string, info protocol.FileInfo, wireVersion protocol.VersionVector) error {
	destAbs, err := JoinSharePath(root, destRelPath)
	if err != nil {
		return fmt.Errorf("sync: install %s: %w", destRelPath, err)
	}
	if err := os.MkdirAll(filepath.Dir(destAbs), 0o700); err != nil {
		return fmt.Errorf("sync: install %s: mkdir: %w", destRelPath, err)
	}

	if info.Type == protocol.FileTypeDir {
		if err := os.MkdirAll(destAbs, 0o700); err != nil {
			return fmt.Errorf("sync: install dir %s: %w", destRelPath, err)
		}
		return nil
	}

	tmp, err := os.CreateTemp(filepath.Dir(destAbs), TempFilePrefix+"*")
	if err != nil {
		return fmt.Errorf("sync: install %s: create temp file: %w", destRelPath, err)
	}
	tmpPath := tmp.Name()
	committed := false
	defer func() {
		// Runs on every return path, including a panic unwinding through
		// this deferred call — the temp file never survives a failed
		// apply.
		_ = tmp.Close()
		if !committed {
			_ = os.Remove(tmpPath)
		}
	}()

	hasher := sha256.New()
	n, err := s.pullFile(ctx, shareID, wireRelPath, wireVersion, info.Size, io.MultiWriter(tmp, hasher))
	if err != nil {
		return fmt.Errorf("sync: install %s: %w", destRelPath, err)
	}
	if n != info.Size {
		return fmt.Errorf("sync: install %s: got %d bytes, peer advertised size %d", destRelPath, n, info.Size)
	}
	sum := hasher.Sum(nil)
	if !bytes.Equal(sum, info.SHA256) {
		return fmt.Errorf("sync: install %s: sha256 mismatch: got %x, peer advertised %x", destRelPath, sum, info.SHA256)
	}

	if info.Type == protocol.FileTypeSymlink {
		if err := s.installSymlink(ctx, shareID, destRelPath, destAbs, tmpPath); err != nil {
			return err
		}
		committed = true
		return nil
	}

	if err := s.finalizeFile(ctx, shareID, destRelPath, destAbs, tmp, info); err != nil {
		return err
	}
	committed = true
	return nil
}

// installSymlink completes pullAndInstall for a symlink: the pulled bytes
// in tmpPath are the link's target string (see serveSymlink), so read
// them, clear whatever currently occupies destAbs via the trash hook, and
// create the link. tmpPath itself is left for pullAndInstall's deferred
// cleanup to remove.
func (s *Session) installSymlink(ctx context.Context, shareID, destRelPath, destAbs, tmpPath string) error {
	target, err := os.ReadFile(tmpPath)
	if err != nil {
		return fmt.Errorf("sync: install symlink %s: read target: %w", destRelPath, err)
	}
	if err := s.replaceWithTrashHook(ctx, shareID, destRelPath, destAbs); err != nil {
		return err
	}
	if err := os.Symlink(string(target), destAbs); err != nil {
		return fmt.Errorf("sync: install symlink %s: %w", destRelPath, err)
	}
	return nil
}

// finalizeFile completes pullAndInstall for a regular file whose verified
// content is in tmp: fsync and close it, set its mode and mtime from info,
// trash whatever currently occupies destAbs, and rename it into place
// (steps 3 and 4 of pullAndInstall's sequence). On success tmp's path no
// longer exists — it *is* destAbs now.
func (s *Session) finalizeFile(ctx context.Context, shareID, destRelPath, destAbs string, tmp *os.File, info protocol.FileInfo) error {
	tmpPath := tmp.Name()
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("sync: install %s: fsync: %w", destRelPath, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("sync: install %s: close: %w", destRelPath, err)
	}
	if info.Mode != 0 {
		if err := os.Chmod(tmpPath, os.FileMode(info.Mode)); err != nil {
			return fmt.Errorf("sync: install %s: chmod: %w", destRelPath, err)
		}
	}
	mtime := time.Unix(0, info.MTimeNS)
	if err := os.Chtimes(tmpPath, mtime, mtime); err != nil {
		return fmt.Errorf("sync: install %s: chtimes: %w", destRelPath, err)
	}

	// --- trash hook (SPEC.md §7) ---
	// destAbs may already exist here (we're overwriting a file the peer's
	// version dominates, resolving a conflict's winner, or reverting a
	// receive-only subscriber's local edit). This is the other place —
	// alongside applyDelete's — old content is discarded on a peer's
	// behalf, and it runs strictly before the rename below: a trash
	// failure returns here and the rename never happens, so the original
	// content is never destroyed just because the safety net failed.
	if err := s.trashHook(ctx, shareID, destRelPath, destAbs); err != nil {
		return fmt.Errorf("sync: install %s: trash: %w", destRelPath, err)
	}

	if err := os.Rename(tmpPath, destAbs); err != nil {
		return fmt.Errorf("sync: install %s: rename into place: %w", destRelPath, err)
	}
	return nil
}

// replaceWithTrashHook removes an existing destAbs (if any) via the same
// trash hook as the regular file path, ahead of an os.Symlink call, which
// unlike os.Rename cannot itself overwrite an existing path.
func (s *Session) replaceWithTrashHook(ctx context.Context, shareID, relpath, destAbs string) error {
	if _, err := os.Lstat(destAbs); err == nil {
		if err := s.trashHook(ctx, shareID, relpath, destAbs); err != nil {
			return fmt.Errorf("sync: install symlink %s: trash: %w", relpath, err)
		}
		if err := os.Remove(destAbs); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("sync: install symlink %s: remove existing: %w", relpath, err)
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("sync: install symlink %s: stat existing: %w", relpath, err)
	}
	return nil
}

// trashHook is the single, obvious slot "move old content to trash before
// overwrite/delete" (SPEC.md §7) plugs into. It's called immediately
// before every destructive operation that discards existing destination
// content on a peer's behalf: applyDelete's os.Remove, and
// pullAndInstall's overwrite (both the rename-into-place path and the
// symlink-replace path via replaceWithTrashHook). destAbs may or may not
// currently exist — Trash.Put no-ops when it doesn't.
//
// If s.trash is nil (no Trash configured — e.g. a test exercising apply
// logic that doesn't care about trash, or trash deliberately disabled),
// this is a no-op, matching this hook's pre-Phase-6 behavior. Once a Trash
// is set via SetTrash, every call site above gets real trash-can behavior
// with no further change to this file: the hook always runs strictly
// before its caller's destructive step, and an error here aborts that
// step rather than proceeding to destroy data.
func (s *Session) trashHook(ctx context.Context, shareID, relpath, destAbs string) error {
	if s.trash == nil {
		return nil
	}
	return s.trash.Put(ctx, shareID, relpath, destAbs)
}

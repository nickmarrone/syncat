package sync

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/nickmarrone/syncat/internal/index"
	"github.com/nickmarrone/syncat/internal/protocol"
)

// applyAction executes one 5a Action against the filesystem, returning the
// index.FileRow(s) that must now be persisted (empty/nil if nothing
// changed — e.g. ActionNone, ActionLocallyModified, or a non-empty
// directory delete that was skipped). The caller (handleIndexUpdate)
// persists the returned rows via Store.PutFile and, unless outbound is
// blocked, reports them back to the peer.
func (s *Session) applyAction(ctx context.Context, cfg ShareConfig, a Action) ([]index.FileRow, error) {
	now := time.Now()

	switch a.Kind {
	case ActionNone, ActionLocallyModified:
		if a.Kind == ActionLocallyModified {
			s.logf("locally modified under receive-only subscription: %s/%s (%s)", cfg.ShareID, a.RelPath, a.Reason)
		}
		return nil, nil

	case ActionPull:
		if a.Source == SourceRemote {
			if err := s.pullAndInstall(ctx, cfg.ShareID, cfg.Root, a.RelPath, a.RelPath, a.Resolved); err != nil {
				return nil, err
			}
		}
		return []index.FileRow{index.FileRowFromInfo(cfg.ShareID, a.Resolved, now)}, nil

	case ActionResurrect:
		if a.Source == SourceRemote {
			if err := s.pullAndInstall(ctx, cfg.ShareID, cfg.Root, a.RelPath, a.RelPath, a.Resolved); err != nil {
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

	// --- trash hook (Phase 6, SPEC.md §7) ---
	// This is the one place a file/symlink is discarded on a peer's
	// behalf via a delete. Phase 6 replaces trashHook's body with a
	// rename into <datadir>/trash/<share-id>/<relpath>.<unix-ts>; nothing
	// else here needs to change.
	if err := trashHook(ctx, cfg.ShareID, relpath, absPath); err != nil {
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
		// gaining a new conflict-copy path).
		if err := s.pullAndInstall(ctx, cfg.ShareID, cfg.Root, a.RelPath, a.ConflictRelPath, a.ConflictInfo); err != nil {
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

		if err := s.pullAndInstall(ctx, cfg.ShareID, cfg.Root, a.RelPath, a.RelPath, a.Resolved); err != nil {
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
// The temp file is removed on every error path, including a panic, via a
// single deferred cleanup keyed off a "committed" flag.
func (s *Session) pullAndInstall(ctx context.Context, shareID, root, wireRelPath, destRelPath string, info protocol.FileInfo) error {
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

	tmp, err := os.CreateTemp(filepath.Dir(destAbs), ".syncat.tmp.*")
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
	n, err := s.pullFile(ctx, shareID, wireRelPath, info.Version, io.MultiWriter(tmp, hasher))
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
		target, err := os.ReadFile(tmpPath)
		if err != nil {
			return fmt.Errorf("sync: install symlink %s: read target: %w", destRelPath, err)
		}
		if err := replaceWithTrashHook(ctx, shareID, destRelPath, destAbs); err != nil {
			return err
		}
		if err := os.Symlink(string(target), destAbs); err != nil {
			return fmt.Errorf("sync: install symlink %s: %w", destRelPath, err)
		}
		committed = true
		return nil
	}

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

	// --- trash hook (Phase 6, SPEC.md §7) ---
	// destAbs may already exist here (we're overwriting a file the peer's
	// version dominates, or resolving a conflict's winner). This is the
	// other place — alongside applyDelete's — old content is discarded on
	// a peer's behalf; Phase 6 fills in trashHook the same way there.
	if err := trashHook(ctx, shareID, destRelPath, destAbs); err != nil {
		return fmt.Errorf("sync: install %s: trash: %w", destRelPath, err)
	}

	if err := os.Rename(tmpPath, destAbs); err != nil {
		return fmt.Errorf("sync: install %s: rename into place: %w", destRelPath, err)
	}
	committed = true
	return nil
}

// replaceWithTrashHook removes an existing destAbs (if any) via the same
// trash hook as the regular file path, ahead of an os.Symlink call, which
// unlike os.Rename cannot itself overwrite an existing path.
func replaceWithTrashHook(ctx context.Context, shareID, relpath, destAbs string) error {
	if _, err := os.Lstat(destAbs); err == nil {
		if err := trashHook(ctx, shareID, relpath, destAbs); err != nil {
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

// trashHook is the single, obvious slot Phase 6 plugs "move old content to
// trash before overwrite/delete" (SPEC.md §7) into. It's called
// immediately before every destructive operation that discards existing
// destination content on a peer's behalf: applyDelete's os.Remove, and
// pullAndInstall's overwrite (both the rename-into-place path and the
// symlink-replace path via replaceWithTrashHook). destAbs may or may not
// currently exist — trashHook is expected to no-op when it doesn't, the
// same as its future Phase 6 implementation will.
//
// Today this is a no-op: trash is out of scope for Phase 5b. Phase 6
// replaces the body with a rename (same filesystem) or copy+delete (cross
// filesystem) of destAbs into
// <datadir>/trash/<share-id>/<relpath>.<unix-ts>, and nothing else in this
// file needs to change.
func trashHook(ctx context.Context, shareID, relpath, destAbs string) error {
	_ = ctx
	_ = shareID
	_ = relpath
	_ = destAbs
	return nil
}

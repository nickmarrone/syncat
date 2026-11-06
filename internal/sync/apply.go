// apply.go holds the Session methods that write reconcile results to
// disk: executing each [Action] against the share directory (installing
// pulled content atomically, deleting, preserving conflict copies) and
// routing every destructive step through the trash hook. session.go owns
// the Session type and drives these; transfer.go moves the bytes.
package sync

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
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
// if nothing changed, e.g. ActionNone, ActionLocallyModified, or a non-empty
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
// Meant to be called at startup, before any session exists. With no transfer
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
		if e.IsDir() && name == ReservedDirName {
			// syncat's own directory (the trash) never holds a staging file.
			continue
		}
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
// reconcile.go). This is the other half, reverting the local edit itself
// once the offerer's own copy has something new to overwrite it with.
//
// reconcile.go raises ActionLocallyModified in two situations, only one of
// which carries real remote content to revert to:
//
//   - a.Source == SourceRemote: the offerer's copy is concurrently
//     different from ours (reconcileConcurrent's receive-only branch).
//     a.Resolved is the offerer's current content, exactly what SPEC.md
//     §1 says to overwrite the local edit with "after a trash copy is
//     taken". This goes through pullAndInstall exactly like an ordinary
//     ActionPull would, which is what actually takes the trash copy (via
//     the same trashHook call site every other overwrite uses) before the
//     new content lands, so the abort-on-trash-failure guarantee applies
//     here for free, with no separate code path to keep in sync.
//   - a.Source == SourceNone (reconcileOne's !hasRemote branch): a file
//     that exists only locally under a receive-only subscription. The
//     offerer has no corresponding content at all yet, so there is
//     nothing to revert *to*. SPEC.md §1 only promises a revert "when the
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
	root, err := os.OpenRoot(cfg.Root)
	if err != nil {
		return false, fmt.Errorf("sync: delete %s: open share root: %w", relpath, err)
	}
	defer root.Close()
	rel := filepath.FromSlash(relpath)

	fi, statErr := root.Lstat(rel)
	if statErr != nil {
		if os.IsNotExist(statErr) {
			return true, nil
		}
		return false, fmt.Errorf("sync: delete %s: stat: %w", relpath, statErr)
	}

	if fi.IsDir() {
		// Directory deletes apply only when empty after children sync
		// (SPEC.md §5); Remove itself refuses a non-empty directory,
		// which is exactly the check we want.
		if err := root.Remove(rel); err != nil {
			s.logf("keeping non-empty directory %s/%s (remote deleted it): %v", cfg.ShareID, relpath, err)
			return false, nil
		}
		return true, nil
	}

	// --- trash hook ---
	// The one place a file/symlink is discarded on a peer's behalf via a
	// delete: moved into this pass's trash event (see trash.go) before the
	// remove below ever runs. A trash failure aborts here, before anything
	// is removed, rather than risking data loss because the safety net
	// itself failed.
	if err := s.trashHook(ctx, relpath, absPath, TrashDeleted); err != nil {
		return false, fmt.Errorf("sync: delete %s: trash: %w", relpath, err)
	}

	if err := root.Remove(rel); err != nil && !os.IsNotExist(err) {
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
// that can, e.g. the peer's file moved on between our reconcile pass
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
// independent conflict-copy resolution (it names the copy from the same
// losing version, so it computes this same conflict path) still reaches us
// as an ordinary remote-only file on a later round, so the two sides still
// converge to the same file set either way.
func (s *Session) applyConflictCopy(ctx context.Context, cfg ShareConfig, a Action, now time.Time) ([]index.FileRow, error) {
	switch {
	case a.Source == SourceLocal && a.ConflictSource == SourceRemote:
		// Winner is already the current local file at a.RelPath, left
		// untouched, so its row is always safe to report.
		winnerInfo := a.Resolved
		winnerInfo.RelPath = a.RelPath
		winnerRow := index.FileRowFromInfo(cfg.ShareID, winnerInfo, now)

		// Pull the loser: it's addressed on the wire by the *original*
		// relpath, since that's still how the peer's own index refers to
		// it (the peer hasn't renamed anything; only our local view is
		// gaining a new conflict-copy path). a.ConflictInfo.Version is used
		// verbatim as the wire version. Unlike a.Resolved, ConflictInfo is
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
		// this is a plain atomic rename, no temp file or hash check
		// needed, the bytes are already known-good local content, and (no
		// network involved) it either fully succeeds or leaves nothing
		// changed.
		if _, err := JoinSharePath(cfg.Root, a.RelPath); err != nil {
			return nil, fmt.Errorf("sync: conflict copy %s: %w", a.RelPath, err)
		}
		if _, err := JoinSharePath(cfg.Root, a.ConflictRelPath); err != nil {
			return nil, fmt.Errorf("sync: conflict copy %s: %w", a.RelPath, err)
		}
		root, err := os.OpenRoot(cfg.Root)
		if err != nil {
			return nil, fmt.Errorf("sync: conflict copy %s: open share root: %w", a.RelPath, err)
		}
		defer root.Close()
		srcRel, dstRel := filepath.FromSlash(a.RelPath), filepath.FromSlash(a.ConflictRelPath)
		if err := root.MkdirAll(filepath.Dir(dstRel), 0o700); err != nil {
			return nil, fmt.Errorf("sync: conflict copy %s: mkdir: %w", a.RelPath, err)
		}
		if err := root.Rename(srcRel, dstRel); err != nil {
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
// installs it at share root under destRelPath, usually the same path,
// except for a conflict copy's loser (see applyConflictCopy). This is
// SPEC.md §5's "applying remote changes" sequence:
//
//  1. download to .syncat.tmp.<rand> in the destination directory (same
//     filesystem as the destination, so the final rename is atomic);
//  2. verify sha256 against what the peer advertised (info.SHA256); a
//     mismatch aborts and never touches the destination;
//  3. fsync the temp file and set its mtime/mode;
//  4. rename(2) into place.
//
// wireVersion is the version named in the FileRequest, deliberately a
// separate parameter from info.Version, because they can legitimately
// differ: info carries the version this content will be persisted under
// locally (for a conflict resolution, Merge(local, remote) plus a local
// Bump, see Action.Resolved's doc comment), while wireVersion must be the
// version the peer actually advertised, or the peer's own freshness check
// (handleFileRequest's Equal(row.Version, req.Version) in transfer.go) can
// never match. Every caller passes the right one via Action.SourceVersion
// (or, for a conflict copy's loser, ConflictInfo.Version, which is never
// rewritten in the first place). The temp file is removed on every error
// path, including a panic, via a single deferred cleanup keyed off a
// "committed" flag.
func (s *Session) pullAndInstall(ctx context.Context, shareID, root, wireRelPath, destRelPath string, info protocol.FileInfo, wireVersion protocol.VersionVector) error {
	if info.Type == protocol.FileTypeSymlink {
		// Reconcile already drops these; refuse here too so no future
		// caller can install one (SPEC.md §5: symlinks are not synced).
		return fmt.Errorf("sync: install %s: symlinks are not synced", destRelPath)
	}
	destAbs, err := JoinSharePath(root, destRelPath)
	if err != nil {
		return fmt.Errorf("sync: install %s: %w", destRelPath, err)
	}
	// Every write below goes through shareRoot, which refuses to resolve
	// a path out of the share even if a symlink appears after
	// JoinSharePath checked the parents.
	shareRoot, err := os.OpenRoot(root)
	if err != nil {
		return fmt.Errorf("sync: install %s: open share root: %w", destRelPath, err)
	}
	defer shareRoot.Close()
	destRel := filepath.FromSlash(destRelPath)
	if err := shareRoot.MkdirAll(filepath.Dir(destRel), 0o700); err != nil {
		return fmt.Errorf("sync: install %s: mkdir: %w", destRelPath, err)
	}

	if info.Type == protocol.FileTypeDir {
		if err := shareRoot.MkdirAll(destRel, 0o700); err != nil {
			return fmt.Errorf("sync: install dir %s: %w", destRelPath, err)
		}
		return nil
	}

	tmp, tmpRel, err := createTempIn(shareRoot, filepath.Dir(destRel))
	if err != nil {
		return fmt.Errorf("sync: install %s: create temp file: %w", destRelPath, err)
	}
	committed := false
	defer func() {
		// Runs on every return path, including a panic unwinding through
		// this deferred call, so the temp file never survives a failed
		// apply.
		_ = tmp.Close()
		if !committed {
			_ = shareRoot.Remove(tmpRel)
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
		s.stats.hashFailures.Add(1)
		return fmt.Errorf("sync: install %s: sha256 mismatch: got %x, peer advertised %x", destRelPath, sum, info.SHA256)
	}

	if err := s.finalizeFile(ctx, shareRoot, destRelPath, destAbs, destRel, tmp, tmpRel, info); err != nil {
		return err
	}
	committed = true
	return nil
}

// createTempIn creates a new, empty TempFilePrefix file in dir (relative
// to root), the os.Root equivalent of os.CreateTemp, and returns it with
// its root-relative name.
func createTempIn(root *os.Root, dir string) (*os.File, string, error) {
	var buf [8]byte
	for range 100 {
		if _, err := rand.Read(buf[:]); err != nil {
			return nil, "", err
		}
		name := filepath.Join(dir, TempFilePrefix+hex.EncodeToString(buf[:]))
		f, err := root.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
		if err == nil {
			return f, name, nil
		}
		if !errors.Is(err, fs.ErrExist) {
			return nil, "", err
		}
	}
	return nil, "", errors.New("could not pick an unused temp file name")
}

// finalizeFile completes pullAndInstall for a regular file whose verified
// content is in tmp (at tmpRel under root): fsync and close it, set its
// mode and mtime from info, trash whatever currently occupies destAbs, and
// rename it into place at destRel (steps 3 and 4 of pullAndInstall's
// sequence). On success tmpRel no longer exists, it *is* destRel now.
func (s *Session) finalizeFile(ctx context.Context, root *os.Root, destRelPath, destAbs, destRel string, tmp *os.File, tmpRel string, info protocol.FileInfo) error {
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("sync: install %s: fsync: %w", destRelPath, err)
	}
	if info.Mode != 0 {
		if err := tmp.Chmod(os.FileMode(info.Mode)); err != nil {
			return fmt.Errorf("sync: install %s: chmod: %w", destRelPath, err)
		}
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("sync: install %s: close: %w", destRelPath, err)
	}
	mtime := time.Unix(0, info.MTimeNS)
	if err := root.Chtimes(tmpRel, mtime, mtime); err != nil {
		return fmt.Errorf("sync: install %s: chtimes: %w", destRelPath, err)
	}

	// --- trash hook ---
	// destAbs may already exist here (we're overwriting a file the peer's
	// version dominates, resolving a conflict's winner, or reverting a
	// receive-only subscriber's local edit). This is the other place,
	// alongside applyDelete's, where old content is discarded on a peer's
	// behalf, and it runs strictly before the rename below: a trash
	// failure returns here and the rename never happens, so the original
	// content is never destroyed just because the safety net failed.
	if err := s.trashHook(ctx, destRelPath, destAbs, TrashOverwritten); err != nil {
		return fmt.Errorf("sync: install %s: trash: %w", destRelPath, err)
	}

	if err := root.Rename(tmpRel, destRel); err != nil {
		return fmt.Errorf("sync: install %s: rename into place: %w", destRelPath, err)
	}
	return nil
}

// trashHook is the single, obvious slot "move old content to the trash
// before overwrite/delete" plugs into. It's called immediately before every
// destructive operation that discards existing destination content on a
// peer's behalf: applyDelete's remove, and finalizeFile's rename into
// place. destAbs may or may not currently exist;
// TrashEvent.Put no-ops when it doesn't. kind is TrashDeleted or
// TrashOverwritten.
//
// The file goes into the trash event reconcileLocked started for this pass
// (withTrashEvent), so everything one pass with one peer discards is one
// event the user can undo at once. If s.trash is nil (no Trash configured,
// e.g. a test exercising apply logic that doesn't care about trash), this
// is a no-op. With a Trash but no event in ctx it fails, aborting the
// destructive step, rather than discarding the file untrashed: every apply
// runs inside a reconcile pass, so that would be a bug, and failing closed
// is the safe way for it to show.
func (s *Session) trashHook(ctx context.Context, relpath, destAbs, kind string) error {
	if s.trash == nil {
		return nil
	}
	ev := trashEventFrom(ctx)
	if ev == nil {
		return fmt.Errorf("sync: trash: no trash event for this pass")
	}
	return ev.Put(ctx, relpath, destAbs, kind)
}

type trashEventKey struct{}

// withTrashEvent returns ctx carrying ev, the trash event every trashHook
// call under it records into. It travels in the context because the apply
// path is several calls deep (applyAction, pullAndInstall, finalizeFile)
// and none of the others has any use for it.
func withTrashEvent(ctx context.Context, ev *TrashEvent) context.Context {
	return context.WithValue(ctx, trashEventKey{}, ev)
}

func trashEventFrom(ctx context.Context) *TrashEvent {
	ev, _ := ctx.Value(trashEventKey{}).(*TrashEvent)
	return ev
}

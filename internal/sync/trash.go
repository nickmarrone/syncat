package sync

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/nickmarrone/syncat/internal/index"
	"github.com/nickmarrone/syncat/internal/protocol"
)

// This file implements SPEC.md §7's trash can and the janitor that
// expires its entries.
//
// Structural local-vs-remote enforcement: Put (the only thing that ever
// writes into the trash) is called from exactly two places in this
// package — applyDelete and pullAndInstall, both in apply.go, both firing
// only from applyAction, which in turn only ever runs from
// handleIndexUpdate: the reconcile-and-apply pass triggered by a peer's
// IndexUpdate. A local deletion the user makes is observed by
// internal/index's Scanner/Watcher and persisted via
// Store.ApplyScanResult — a code path that lives in internal/index, which
// has no dependency on internal/sync (indeed cannot: internal/sync already
// imports internal/index, so the reverse would be a cycle) and therefore
// cannot reach Trash.Put even by mistake. See trash_test.go for a test
// exercising both paths side by side.

// --- the trash can: put, list, restore ---------------------------------

// ErrRestoreDestExists is returned by Trash.Restore when the destination
// relpath is already occupied by something on disk. Restore refuses to
// clobber it silently; the caller must move/remove the conflicting path
// (or, in a future UI, offer to write the restore under a conflict-copy
// name) and retry.
var ErrRestoreDestExists = errors.New("sync: trash: restore destination already exists")

// trashSuffixRe recognizes the "<ts>" or "<ts>-<n>" suffix Trash.Put
// appends to the last path element of a trashed file's original relpath.
// Group 1 is the original last path element; group 2 is the unix
// timestamp; group 3 (unused beyond disambiguating file names on disk) is
// the collision counter, if any.
var trashSuffixRe = regexp.MustCompile(`^(.*)\.(\d{1,19})(?:-(\d+))?$`)

// Trash implements the trash can described in SPEC.md §7: any
// remote-initiated delete or overwrite moves the old content to
// <datadir>/trash/<share-id>/<relpath>.<unix-ts> before the change lands.
//
// The zero value is not usable; construct with NewTrash.
type Trash struct {
	root  string // <datadir>/trash, absolute
	clock Clock

	// rename performs the same-filesystem fast path; it's os.Rename in
	// production. Tests substitute a wrapper that reports a cross-device
	// error so the copy+delete fallback is exercised without needing a
	// second real filesystem in the test environment (see trash_test.go).
	rename func(oldpath, newpath string) error
}

// NewTrash returns a Trash rooted at root (normally config.Paths.TrashDir()).
// clock supplies the timestamp trashed entries are named with; time.Now is
// used if clock is nil.
func NewTrash(root string, clock Clock) *Trash {
	if clock == nil {
		clock = time.Now
	}
	return &Trash{root: root, clock: clock, rename: os.Rename}
}

// Put moves the file or symlink at srcAbs — the current content at
// shareID/relpath, about to be overwritten or deleted on a peer's behalf —
// into the trash. It is a no-op (nil error) if srcAbs doesn't currently
// exist, since there is then nothing to preserve.
//
// Same-filesystem case: an atomic rename. Cross-filesystem case (the data
// dir and a share directory are not guaranteed to share a mount): the
// content is copied to the trash, fsynced, and only then is the original
// removed — so a crash or error mid-copy leaves the original file intact
// rather than losing it (the partially written trash copy, if any, is
// cleaned up on error).
//
// A collision — the same relpath trashed twice within the same unix
// second — never overwrites an earlier trash entry: Put finds the next
// free "<ts>-<n>" name instead.
func (tr *Trash) Put(ctx context.Context, shareID, relpath, srcAbs string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if strings.ContainsAny(shareID, "/\\") || shareID == "" {
		return fmt.Errorf("sync: trash: invalid share id %q", shareID)
	}

	fi, err := os.Lstat(srcAbs)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // nothing to trash
		}
		return fmt.Errorf("sync: trash: stat %s: %w", srcAbs, err)
	}

	base, err := tr.destPath(shareID, relpath)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(base), 0o700); err != nil {
		return fmt.Errorf("sync: trash: mkdir: %w", err)
	}
	dest, err := uniquePath(base)
	if err != nil {
		return fmt.Errorf("sync: trash: %w", err)
	}

	if err := tr.rename(srcAbs, dest); err != nil {
		if !isCrossDevice(err) {
			return fmt.Errorf("sync: trash: move %s: %w", srcAbs, err)
		}
		if err := copyThenRemove(srcAbs, dest, fi); err != nil {
			return fmt.Errorf("sync: trash: copy %s: %w", srcAbs, err)
		}
	}
	return nil
}

// destPath returns the (not-yet-disambiguated) trash path for shareID's
// relpath at the current clock time: <root>/<shareID>/<relpath>.<unix-ts>.
// The relpath portion is built with JoinSharePath, so it goes through the
// same validation/containment guarantee as every other wire-derived path
// in this package; only our own trusted "." + timestamp suffix is appended
// after, directly to the resulting absolute path.
func (tr *Trash) destPath(shareID, relpath string) (string, error) {
	shareTrashRoot := filepath.Join(tr.root, shareID)
	base, err := JoinSharePath(shareTrashRoot, relpath)
	if err != nil {
		return "", fmt.Errorf("sync: trash: %w", err)
	}
	ts := tr.clock().Unix()
	return base + "." + strconv.FormatInt(ts, 10), nil
}

// uniquePath returns base if nothing exists there yet, or base + "-N" for
// the smallest N >= 1 that's free, so two trash entries never collide.
func uniquePath(base string) (string, error) {
	if _, err := os.Lstat(base); err != nil {
		if os.IsNotExist(err) {
			return base, nil
		}
		return "", fmt.Errorf("stat %s: %w", base, err)
	}
	for n := 1; n < 100000; n++ {
		candidate := fmt.Sprintf("%s-%d", base, n)
		if _, err := os.Lstat(candidate); err != nil {
			if os.IsNotExist(err) {
				return candidate, nil
			}
			return "", fmt.Errorf("stat %s: %w", candidate, err)
		}
	}
	return "", fmt.Errorf("too many collisions for %s", base)
}

// isCrossDevice reports whether err is (or wraps) the "invalid
// cross-device link" error os.Rename returns when its two paths are on
// different filesystems.
func isCrossDevice(err error) bool {
	var linkErr *os.LinkError
	if errors.As(err, &linkErr) {
		return errors.Is(linkErr.Err, syscall.EXDEV)
	}
	return errors.Is(err, syscall.EXDEV)
}

// copyThenRemove implements the cross-filesystem fallback: copy srcAbs's
// content to dest (fsyncing before the copy is considered durable), then
// remove srcAbs only once the copy fully succeeded. On any failure, a
// partially written dest is cleaned up and srcAbs is left untouched.
func copyThenRemove(srcAbs, dest string, fi os.FileInfo) error {
	if err := copyFileOrSymlink(srcAbs, dest, fi); err != nil {
		return err
	}
	if err := os.Remove(srcAbs); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove original %s after copy: %w", srcAbs, err)
	}
	return nil
}

// copyFileOrSymlink durably copies src to dst: for a symlink, by
// recreating the link at dst with the same target; for a regular file, by
// writing the full content to a freshly created dst and fsyncing it before
// returning. dst must not already exist (O_EXCL) — callers are expected to
// have already picked a free path. On any error, a partially written dst
// is removed so callers never see a half-copied file on disk.
func copyFileOrSymlink(src, dst string, fi os.FileInfo) error {
	if fi.Mode()&os.ModeSymlink != 0 {
		target, err := os.Readlink(src)
		if err != nil {
			return fmt.Errorf("readlink %s: %w", src, err)
		}
		if err := os.Symlink(target, dst); err != nil {
			return fmt.Errorf("symlink %s: %w", dst, err)
		}
		return nil
	}

	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("open %s: %w", src, err)
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, fi.Mode().Perm())
	if err != nil {
		return fmt.Errorf("create %s: %w", dst, err)
	}
	committed := false
	defer func() {
		_ = out.Close()
		if !committed {
			_ = os.Remove(dst)
		}
	}()

	if _, err := io.Copy(out, in); err != nil {
		return fmt.Errorf("copy %s -> %s: %w", src, dst, err)
	}
	if err := out.Sync(); err != nil {
		return fmt.Errorf("fsync %s: %w", dst, err)
	}
	if err := out.Close(); err != nil {
		return fmt.Errorf("close %s: %w", dst, err)
	}
	committed = true

	mtime := fi.ModTime()
	_ = os.Chtimes(dst, mtime, mtime) // best-effort; never fails the copy

	return nil
}

// Entry describes one trashed file: enough for a UI trash browser
// (relpath, trashed-at time, size) and, opaquely, enough for Restore to
// find the exact physical trash file to bring back — including
// disambiguating two entries that happen to share both RelPath and
// TrashedAt (a same-second collision), which is why Restore takes an
// Entry rather than a (relpath, time) pair.
type Entry struct {
	ShareID   string
	RelPath   string
	TrashedAt time.Time
	Size      int64

	trashAbs string // absolute path of the physical trash file
}

// List returns every trashed entry for shareID, most-recently-trashed
// first. A trash directory that doesn't exist yet (nothing has ever been
// trashed for this share) is reported as an empty list, not an error.
// Files under the share's trash directory whose name doesn't match the
// "<relpath-last-element>.<unix-ts>[-n]" pattern Put produces are skipped
// rather than aborting the whole listing.
func (tr *Trash) List(shareID string) ([]Entry, error) {
	root := filepath.Join(tr.root, shareID)
	if _, err := os.Stat(root); err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("sync: trash: list %s: %w", shareID, err)
	}

	var entries []Entry
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return nil //nolint:nilerr // shouldn't happen under WalkDir(root, ...); skip defensively
		}
		relSlash := filepath.ToSlash(rel)
		dir, base := path.Split(relSlash)
		m := trashSuffixRe.FindStringSubmatch(base)
		if m == nil {
			return nil // not a name Put produced; skip
		}
		ts, err := strconv.ParseInt(m[2], 10, 64)
		if err != nil {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		entries = append(entries, Entry{
			ShareID:   shareID,
			RelPath:   dir + m[1],
			TrashedAt: time.Unix(ts, 0),
			Size:      info.Size(),
			trashAbs:  p,
		})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("sync: trash: list %s: %w", shareID, err)
	}

	sort.Slice(entries, func(i, j int) bool {
		if !entries[i].TrashedAt.Equal(entries[j].TrashedAt) {
			return entries[i].TrashedAt.After(entries[j].TrashedAt)
		}
		return entries[i].RelPath < entries[j].RelPath
	})
	return entries, nil
}

// Restore implements SPEC.md §7's restore: the trashed content named by e
// is copied back to shareRoot/e.RelPath (the trash entry itself is left in
// place — a restore is a copy, not a move, so the trash entry is still
// there for the janitor or a later restore), and the corresponding index
// row is written with nodeID's counter bumped over whatever version the
// index currently holds for that path, so the restored file is strictly
// newer and propagates outward as a new change on the next reconcile pass.
//
// If shareRoot/e.RelPath is already occupied by something (any file,
// symlink, or directory), Restore refuses and returns an error wrapping
// ErrRestoreDestExists rather than silently clobbering it — the caller
// must clear the path (or, in a future UI, choose to restore under a
// different name) and retry.
func (tr *Trash) Restore(ctx context.Context, store *index.Store, nodeID, shareRoot string, e Entry) (index.FileRow, error) {
	if ctx.Err() != nil {
		return index.FileRow{}, ctx.Err()
	}
	if e.trashAbs == "" {
		return index.FileRow{}, fmt.Errorf("sync: trash: restore: entry was not produced by List")
	}

	destAbs, err := JoinSharePath(shareRoot, e.RelPath)
	if err != nil {
		return index.FileRow{}, fmt.Errorf("sync: trash: restore: %w", err)
	}

	if _, err := os.Lstat(destAbs); err == nil {
		return index.FileRow{}, fmt.Errorf("sync: trash: restore %s: %w", e.RelPath, ErrRestoreDestExists)
	} else if !os.IsNotExist(err) {
		return index.FileRow{}, fmt.Errorf("sync: trash: restore %s: stat destination: %w", e.RelPath, err)
	}

	if err := os.MkdirAll(filepath.Dir(destAbs), 0o700); err != nil {
		return index.FileRow{}, fmt.Errorf("sync: trash: restore %s: mkdir: %w", e.RelPath, err)
	}

	srcFI, err := os.Lstat(e.trashAbs)
	if err != nil {
		return index.FileRow{}, fmt.Errorf("sync: trash: restore %s: stat trash copy: %w", e.RelPath, err)
	}
	if err := copyFileOrSymlink(e.trashAbs, destAbs, srcFI); err != nil {
		return index.FileRow{}, fmt.Errorf("sync: trash: restore %s: %w", e.RelPath, err)
	}

	row, err := buildRestoredRow(ctx, store, nodeID, e.ShareID, e.RelPath, destAbs)
	if err != nil {
		return index.FileRow{}, err
	}
	if err := store.PutFile(ctx, row); err != nil {
		return index.FileRow{}, fmt.Errorf("sync: trash: restore %s: put index row: %w", e.RelPath, err)
	}
	return row, nil
}

// buildRestoredRow stats/hashes the just-restored file at destAbs and
// returns the FileRow to persist: nodeID's counter bumped over the
// index's current version for shareID/relpath (0 if there is none, e.g.
// the row was previously purged as an old tombstone).
func buildRestoredRow(ctx context.Context, store *index.Store, nodeID, shareID, relpath, destAbs string) (index.FileRow, error) {
	fi, err := os.Lstat(destAbs)
	if err != nil {
		return index.FileRow{}, fmt.Errorf("sync: trash: restore %s: stat restored file: %w", relpath, err)
	}

	var current index.FileRow
	if existing, err := store.GetFile(ctx, shareID, relpath); err == nil {
		current = existing
	} else if !errors.Is(err, index.ErrNotFound) {
		return index.FileRow{}, fmt.Errorf("sync: trash: restore %s: read current version: %w", relpath, err)
	}

	typ := protocol.FileTypeFile
	var sum []byte
	if fi.Mode()&os.ModeSymlink != 0 {
		typ = protocol.FileTypeSymlink
		target, err := os.Readlink(destAbs)
		if err != nil {
			return index.FileRow{}, fmt.Errorf("sync: trash: restore %s: readlink: %w", relpath, err)
		}
		h := sha256.Sum256([]byte(target))
		sum = h[:]
	} else {
		data, err := os.ReadFile(destAbs)
		if err != nil {
			return index.FileRow{}, fmt.Errorf("sync: trash: restore %s: read: %w", relpath, err)
		}
		h := sha256.Sum256(data)
		sum = h[:]
	}

	return index.FileRow{
		ShareID:   shareID,
		RelPath:   relpath,
		Type:      typ,
		Size:      fi.Size(),
		MTimeNS:   fi.ModTime().UnixNano(),
		Mode:      uint32(fi.Mode().Perm()),
		SHA256:    sum,
		Version:   Bump(current.Version, nodeID),
		Deleted:   false,
		UpdatedAt: time.Now(),
	}, nil
}

// --- the janitor: purging expired entries ------------------------------

// DefaultJanitorInterval is how often the janitor sweeps the trash for
// expired entries in production (SPEC.md §7: "a daily janitor purges older
// entries").
const DefaultJanitorInterval = 24 * time.Hour

// JanitorClock abstracts wall-clock time for the janitor: both "how old is
// this trash entry" (Now) and "when does the next sweep fire" (After).
// This mirrors protocol.Clock and index.Clock exactly (each package
// defines its own copy rather than sharing one, for the same
// leaf-package-independence reason documented on protocol.Clock) — it is
// a different, richer type from this package's own Clock (a plain
// time.Time-returning func used only to timestamp conflict/trash names),
// which has no After and so can't drive deterministic interval timing in
// tests. JanitorClock lets a test replace both "now" and the sweep timer
// with a fake, with no real sleeping (see trash_test.go).
type JanitorClock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

type realJanitorClock struct{}

func (realJanitorClock) Now() time.Time                         { return time.Now() }
func (realJanitorClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// RealJanitorClock is the production JanitorClock, backed by the time
// package.
var RealJanitorClock JanitorClock = realJanitorClock{}

// Janitor purges trash entries older than a configured retention window
// (config.TrashRetentionDays, default 30 — SPEC.md §7). It follows the
// same injectable-clock, explicit-Close pattern as protocol.Keepalive and
// index.Watcher.
//
// The zero value is not usable; construct with NewJanitor.
type Janitor struct {
	trash     *Trash
	retention time.Duration
	clock     JanitorClock
	interval  time.Duration
	onSweep   func(purged int, err error)

	stop chan struct{}
	wg   sync.WaitGroup
}

// NewJanitor returns a Janitor that purges trash entries older than
// retention from trash, checking every interval (DefaultJanitorInterval if
// <= 0). clock is used both to decide "how old" and to drive the sweep
// timing; RealJanitorClock is used if clock is nil. onSweep, if non-nil, is
// called after every sweep the background loop runs (including a failed
// one) with the number of entries purged and any error — tests use it to
// observe sweeps deterministically instead of racing the background
// goroutine.
func NewJanitor(trash *Trash, retention time.Duration, clock JanitorClock, interval time.Duration, onSweep func(purged int, err error)) *Janitor {
	if clock == nil {
		clock = RealJanitorClock
	}
	if interval <= 0 {
		interval = DefaultJanitorInterval
	}
	return &Janitor{
		trash:     trash,
		retention: retention,
		clock:     clock,
		interval:  interval,
		onSweep:   onSweep,
		stop:      make(chan struct{}),
	}
}

// Start begins the janitor's periodic sweep loop in a background
// goroutine. It does not sweep immediately; the first sweep happens after
// one interval (matching index.Watcher's periodicLoop — a fresh daemon
// start shouldn't immediately start deleting things before its first
// scheduled tick). Call Sweep directly for an immediate, synchronous pass
// (e.g. right after startup, or from the CLI).
func (j *Janitor) Start(ctx context.Context) {
	j.wg.Add(1)
	go j.loop(ctx)
}

func (j *Janitor) loop(ctx context.Context) {
	defer j.wg.Done()
	for {
		select {
		case <-j.stop:
			return
		case <-ctx.Done():
			return
		case <-j.clock.After(j.interval):
			purged, err := j.Sweep(ctx)
			if j.onSweep != nil {
				j.onSweep(purged, err)
			}
		}
	}
}

// Close stops the janitor's background loop and waits for it to exit.
// Safe to call more than once, and even if Start was never called.
func (j *Janitor) Close() error {
	select {
	case <-j.stop:
		// already closed
	default:
		close(j.stop)
	}
	j.wg.Wait()
	return nil
}

// Sweep runs one purge pass immediately and synchronously: every trash
// entry older than j.retention is permanently deleted; everything else is
// left alone. It returns the number of entries purged.
//
// Sweep never walks outside j.trash.root (WalkDir is rooted there, and a
// directory walk does not follow symlinks into other trees), and
// tolerates the trash root not existing yet (a fresh install that has
// never trashed anything) by treating that as an empty, no-op sweep.
func (j *Janitor) Sweep(ctx context.Context) (int, error) {
	root := j.trash.root
	if _, err := os.Stat(root); err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("sync: janitor: stat trash root: %w", err)
	}

	cutoff := j.clock.Now().Add(-j.retention)
	purged := 0

	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if d.IsDir() {
			return nil
		}
		base := filepath.Base(p)
		m := trashSuffixRe.FindStringSubmatch(base)
		if m == nil {
			return nil // not one of ours; leave it alone
		}
		ts, err := parseUnixSeconds(m[2])
		if err != nil {
			return nil
		}
		if ts.Before(cutoff) {
			if rmErr := os.Remove(p); rmErr != nil && !os.IsNotExist(rmErr) {
				return fmt.Errorf("remove %s: %w", p, rmErr)
			}
			purged++
		}
		return nil
	})
	if err != nil {
		return purged, fmt.Errorf("sync: janitor: sweep: %w", err)
	}

	pruneEmptyDirs(root)
	return purged, nil
}

// pruneEmptyDirs best-effort removes now-empty share subdirectories left
// behind after a sweep purges every entry under them, so an old share's
// trash directory doesn't accumulate forever as an empty husk. Failures
// are ignored: this cleanup is cosmetic, never worth failing a sweep over.
// Only a plain os.Remove is used (never RemoveAll), so a directory that
// still has live content in it simply fails to remove and is left alone —
// no risk to anything not already expired.
func pruneEmptyDirs(root string) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		dir := filepath.Join(root, e.Name())
		sub, err := os.ReadDir(dir)
		if err == nil && len(sub) == 0 {
			_ = os.Remove(dir)
		}
	}
}

func parseUnixSeconds(s string) (time.Time, error) {
	var sec int64
	if _, err := fmt.Sscanf(s, "%d", &sec); err != nil {
		return time.Time{}, err
	}
	return time.Unix(sec, 0), nil
}

package sync

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/json"
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

// This file implements the trash (see GLOSSARY.md and
// docs/adr/0001-trash-lives-in-the-share-hold-guards-disasters.md): an undo
// for the changes peers make. It is not a backup.
//
// The trash lives inside each synced folder, at .syncat/trash/, so a user
// can browse it and restore from it with a file manager, every move into
// it is a rename on the same filesystem, and the user's own backup of the
// folder covers it. It is organised by event, one folder per reconcile
// pass with one peer, laid out like the share itself:
//
//	.syncat/trash/2026-10-27 10.32.05 from laptop/notes/ideas.md
//
// Each event also has a small log, .syncat/trash-events/<event>.jsonl,
// recording who caused it and whether each file was deleted or
// overwritten. The log is kept beside the trash rather than inside the
// event folder so the folder holds only the user's files.
//
// Structural local-vs-remote enforcement: TrashEvent.Put (the only thing
// that ever writes into the trash on a peer's behalf) is reached only
// through Session.trashHook in apply.go, which runs only from applyAction,
// which runs only from a reconcile pass over a peer's changes. A deletion
// the user makes locally is observed by internal/index's Scanner/Watcher,
// which cannot reach this package (internal/sync imports internal/index,
// so the reverse would be a cycle). The trash's other writer, Restore,
// moves the file a restore replaces into the trash, so a restore can be
// undone too.

// ReservedDirName is the directory syncat keeps at the root of every synced
// folder. It is never scanned or synced (see index.ReservedDirName) and no
// peer-supplied path may point into it (see ValidateRelPath).
const ReservedDirName = index.ReservedDirName

const (
	trashDirName    = "trash"
	trashLogDirName = "trash-events"
	reservedReadme  = "README"
)

// reservedReadmeText marks .syncat/ as syncat's own. A .syncat/ without it
// was made by someone else, and syncat refuses to use it (see
// EnsureReservedDir).
const reservedReadmeText = `This folder belongs to syncat. It is never synced to other nodes.

trash/ holds files that a peer deleted or overwrote here, one folder per
change. To undo a change, move its files back, or use the trash page of the
web UI or ` + "`syncat trash`" + `. Old changes are removed after the share's
retention period, or when the trash outgrows its size limit.

The trash is an undo, not a backup. Use a backup tool for this folder too.
`

// Kinds of trash entry, recorded in an event's log.
const (
	TrashDeleted     = "deleted"
	TrashOverwritten = "overwritten"
	TrashMigrated    = "migrated"
)

// ErrReservedDirTaken is returned by EnsureReservedDir when the folder
// already has a .syncat/ that syncat did not create.
var ErrReservedDirTaken = errors.New("sync: folder already has a " + ReservedDirName + " directory that syncat did not create")

// ErrRestoreDestExists is returned by Trash.Restore when a directory
// occupies the path a file would be restored to. A file there is moved to
// the trash instead (see Restore).
var ErrRestoreDestExists = errors.New("sync: trash: a directory is in the way of the restore")

// ErrTrashEntryNotFound is returned by Trash.Restore for an event or path
// the trash does not hold.
var ErrTrashEntryNotFound = errors.New("sync: trash: no such trash entry")

// EnsureReservedDir creates root/.syncat/ (with its README) if it does not
// exist, and returns ErrReservedDirTaken if it exists without the README,
// meaning someone else made it and its contents are not syncat's to manage.
func EnsureReservedDir(root string) error {
	dir := filepath.Join(root, ReservedDirName)
	readme := filepath.Join(dir, reservedReadme)
	fi, err := os.Lstat(dir)
	switch {
	case err == nil && !fi.IsDir():
		return fmt.Errorf("%w (%s is a file)", ErrReservedDirTaken, dir)
	case err == nil:
		if _, err := os.Lstat(readme); err != nil {
			if os.IsNotExist(err) {
				return fmt.Errorf("%w (%s)", ErrReservedDirTaken, dir)
			}
			return fmt.Errorf("sync: check %s: %w", readme, err)
		}
		return nil
	case !os.IsNotExist(err):
		return fmt.Errorf("sync: check %s: %w", dir, err)
	}
	if err := os.Mkdir(dir, 0o700); err != nil {
		return fmt.Errorf("sync: create %s: %w", dir, err)
	}
	if err := os.WriteFile(readme, []byte(reservedReadmeText), 0o600); err != nil {
		return fmt.Errorf("sync: create %s: %w", readme, err)
	}
	return nil
}

// --- the trash and its events ------------------------------------------

// Trash creates trash events and lists, restores, and expires them. One
// Trash serves every share; each operation names the share's root.
//
// The zero value is not usable; construct with NewTrash.
type Trash struct {
	clock Clock

	// rename performs the same-filesystem fast path; it's os.Rename in
	// production. Tests substitute a wrapper that reports a cross-device
	// error so the copy+delete fallback is exercised without needing a
	// second real filesystem. A mount point inside a share makes that
	// fallback real even though the trash is in the share.
	rename func(oldpath, newpath string) error

	// mu serializes choosing event folder names, so two events started in
	// the same second for the same peer never pick the same one.
	mu sync.Mutex
}

// NewTrash returns a Trash. clock timestamps events; time.Now is used if
// it is nil.
func NewTrash(clock Clock) *Trash {
	if clock == nil {
		clock = time.Now
	}
	return &Trash{clock: clock, rename: os.Rename}
}

// TrashEvent is one change being recorded in a share's trash: everything a
// single reconcile pass with one peer deleted or overwrote, or everything a
// single restore replaced. Its folder is created on the first Put, so a
// pass that discards nothing leaves nothing behind.
//
// Put is safe for concurrent use; a reconcile pass applies actions on
// several workers at once.
type TrashEvent struct {
	tr     *Trash
	root   string
	label  string
	source string
	at     time.Time

	mu  sync.Mutex
	id  string // folder name, "" until the first Put
	log *os.File
}

// Event sources, recorded in an event's log header.
const (
	TrashSourcePeer     = "peer"
	TrashSourceRestore  = "restore"
	TrashSourceMigrated = "migrated"
)

// BeginPeerEvent starts the event for one reconcile pass in the share at
// root with the peer named peerName.
func (tr *Trash) BeginPeerEvent(root, peerName string) *TrashEvent {
	return tr.begin(root, "from "+peerName, TrashSourcePeer, tr.clock())
}

func (tr *Trash) begin(root, label, source string, at time.Time) *TrashEvent {
	return &TrashEvent{tr: tr, root: root, label: label, source: source, at: at}
}

// eventHeader is the first line of an event's log.
type eventHeader struct {
	At     time.Time `json:"at"`
	Label  string    `json:"label"`
	Source string    `json:"source"`
}

// eventLine is every later line of an event's log: one trashed file.
// Stored is its path inside the event folder, which differs from Path only
// when one event trashed the same path twice.
type eventLine struct {
	Path   string `json:"path"`
	Kind   string `json:"kind"`
	Stored string `json:"stored"`
}

// eventNameUnsafe matches what an event folder name may not contain. A
// peer's name is chosen by its user, so it is reduced to characters that
// are safe in a path on every platform.
var eventNameUnsafe = regexp.MustCompile(`[^A-Za-z0-9 ._()-]+`)

// eventTimeLayout is the leading timestamp of an event folder name, in
// local time, sortable, and free of ':' (which Windows and macOS's Finder
// both mishandle).
const eventTimeLayout = "2006-01-02 15.04.05"

func eventFolderName(at time.Time, label string) string {
	label = strings.TrimSpace(eventNameUnsafe.ReplaceAllString(label, "_"))
	if len(label) > 48 {
		label = label[:48]
	}
	name := at.Local().Format(eventTimeLayout)
	if label != "" {
		name += " " + label
	}
	return name
}

func trashDir(root string) string    { return filepath.Join(root, ReservedDirName, trashDirName) }
func trashLogDir(root string) string { return filepath.Join(root, ReservedDirName, trashLogDirName) }

// open creates the event's folder and log. Called with ev.mu held.
func (ev *TrashEvent) open() error {
	if ev.id != "" {
		return nil
	}
	if err := EnsureReservedDir(ev.root); err != nil {
		return err
	}
	for _, d := range []string{trashDir(ev.root), trashLogDir(ev.root)} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return fmt.Errorf("sync: trash: %w", err)
		}
	}

	ev.tr.mu.Lock()
	defer ev.tr.mu.Unlock()
	base := eventFolderName(ev.at, ev.label)
	id := base
	for n := 2; ; n++ {
		err := os.Mkdir(filepath.Join(trashDir(ev.root), id), 0o700)
		if err == nil {
			break
		}
		if !os.IsExist(err) {
			return fmt.Errorf("sync: trash: create event: %w", err)
		}
		if n > 10000 {
			return fmt.Errorf("sync: trash: too many events named %q", base)
		}
		id = fmt.Sprintf("%s (%d)", base, n)
	}

	f, err := os.OpenFile(filepath.Join(trashLogDir(ev.root), id+".jsonl"), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("sync: trash: create event log: %w", err)
	}
	if err := writeJSONLine(f, eventHeader{At: ev.at, Label: ev.label, Source: ev.source}); err != nil {
		f.Close()
		return fmt.Errorf("sync: trash: write event log: %w", err)
	}
	ev.id, ev.log = id, f
	return nil
}

func writeJSONLine(w io.Writer, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = w.Write(append(data, '\n'))
	return err
}

// Put moves the file or symlink at srcAbs, the current content at relpath,
// into the event, recording it as kind. It is a no-op if srcAbs doesn't
// exist, since there is then nothing to keep.
//
// Same-filesystem case: an atomic rename. Cross-filesystem case (a mount
// inside the share): the content is copied, fsynced, checked, and only
// then is the original removed, so a failure part-way leaves the original
// where it was. A Put that fails must abort the destructive step its
// caller was about to take.
func (ev *TrashEvent) Put(ctx context.Context, relpath, srcAbs, kind string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	fi, err := os.Lstat(srcAbs)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("sync: trash: stat %s: %w", srcAbs, err)
	}

	ev.mu.Lock()
	defer ev.mu.Unlock()
	if err := ev.open(); err != nil {
		return err
	}

	eventDir := filepath.Join(trashDir(ev.root), ev.id)
	stored, dest, err := freeStoredPath(eventDir, relpath)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dest), 0o700); err != nil {
		return fmt.Errorf("sync: trash: mkdir: %w", err)
	}
	if err := ev.tr.rename(srcAbs, dest); err != nil {
		if !isCrossDevice(err) {
			return fmt.Errorf("sync: trash: move %s: %w", srcAbs, err)
		}
		if err := copyThenRemove(srcAbs, dest, fi); err != nil {
			return fmt.Errorf("sync: trash: copy %s: %w", srcAbs, err)
		}
	}
	if err := writeJSONLine(ev.log, eventLine{Path: relpath, Kind: kind, Stored: stored}); err != nil {
		// The file is safe in the event folder; only its description is
		// missing, and List falls back to walking the folder for that.
		return fmt.Errorf("sync: trash: write event log: %w", err)
	}
	return nil
}

// Close closes the event's log. Safe to call on an event that never
// trashed anything, and more than once.
func (ev *TrashEvent) Close() error {
	ev.mu.Lock()
	defer ev.mu.Unlock()
	if ev.log == nil {
		return nil
	}
	err := ev.log.Close()
	ev.log = nil
	return err
}

// freeStoredPath returns where relpath goes inside eventDir: relpath
// itself, or "<name> (N)<ext>" if this event already holds that path.
func freeStoredPath(eventDir, relpath string) (stored, abs string, err error) {
	abs, err = JoinSharePath(eventDir, relpath)
	if err != nil {
		return "", "", fmt.Errorf("sync: trash: %w", err)
	}
	if _, err := os.Lstat(abs); os.IsNotExist(err) {
		return relpath, abs, nil
	}
	dir, base := path.Split(relpath)
	ext := path.Ext(base)
	stem := strings.TrimSuffix(base, ext)
	for n := 2; n < 10000; n++ {
		stored = fmt.Sprintf("%s%s (%d)%s", dir, stem, n, ext)
		abs, err = JoinSharePath(eventDir, stored)
		if err != nil {
			return "", "", fmt.Errorf("sync: trash: %w", err)
		}
		if _, err := os.Lstat(abs); os.IsNotExist(err) {
			return stored, abs, nil
		}
	}
	return "", "", fmt.Errorf("sync: trash: too many copies of %s in one event", relpath)
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
// content to dest (fsyncing it, and checking its size, before the copy is
// considered durable), then remove srcAbs only once the copy fully
// succeeded. On any failure, a partially written dest is cleaned up and
// srcAbs is left untouched.
func copyThenRemove(srcAbs, dest string, fi os.FileInfo) error {
	if err := copyFileOrSymlink(srcAbs, dest, fi); err != nil {
		return err
	}
	if fi.Mode().IsRegular() {
		got, err := os.Lstat(dest)
		if err != nil || got.Size() != fi.Size() {
			_ = os.Remove(dest)
			return fmt.Errorf("copy of %s is incomplete", srcAbs)
		}
	}
	if err := os.Remove(srcAbs); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove original %s after copy: %w", srcAbs, err)
	}
	return nil
}

// copyFileOrSymlink durably copies src to dst: for a symlink, by
// recreating the link at dst with the same target; for a regular file, by
// writing the full content to a freshly created dst and fsyncing it before
// returning. dst must not already exist (O_EXCL). On any error, a
// partially written dst is removed so callers never see a half-copied file
// on disk.
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

// moveOrCopy moves src to dst, falling back to copyThenRemove across
// filesystems.
func (tr *Trash) moveOrCopy(src, dst string) error {
	fi, err := os.Lstat(src)
	if err != nil {
		return err
	}
	if err := tr.rename(src, dst); err != nil {
		if !isCrossDevice(err) {
			return err
		}
		return copyThenRemove(src, dst, fi)
	}
	return nil
}

// --- listing -------------------------------------------------------------

// TrashEventInfo describes one event in a share's trash.
type TrashEventInfo struct {
	ID     string // the event's folder name under .syncat/trash/
	At     time.Time
	Label  string // e.g. "from laptop"
	Source string // TrashSourcePeer, TrashSourceRestore, TrashSourceMigrated, or "" if its log is missing
	Items  []TrashItem
	Size   int64 // bytes the event's files take
}

// TrashItem is one file in a trash event.
type TrashItem struct {
	RelPath string // where it was in the share
	Kind    string // TrashDeleted, TrashOverwritten, TrashMigrated, or "" if unknown
	Size    int64

	stored string // its path inside the event folder
}

// List returns the events in the trash of the share at root, newest first.
// A share with no trash yet has none. Files the user has already moved out
// of an event are left out of it, and an event with nothing left is left
// out entirely.
func (tr *Trash) List(root string) ([]TrashEventInfo, error) {
	dirs, err := os.ReadDir(trashDir(root))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("sync: trash: list: %w", err)
	}
	var events []TrashEventInfo
	for _, d := range dirs {
		if !d.IsDir() {
			continue
		}
		ev, err := readEvent(root, d.Name())
		if err != nil {
			return nil, err
		}
		if len(ev.Items) > 0 {
			events = append(events, ev)
		}
	}
	sort.Slice(events, func(i, j int) bool {
		if !events[i].At.Equal(events[j].At) {
			return events[i].At.After(events[j].At)
		}
		return events[i].ID > events[j].ID
	})
	return events, nil
}

// readEvent builds one event's description from its log, checked against
// what is actually in its folder. Anything in the folder the log does not
// mention (the log was lost, or the user put something there) is listed
// with an unknown kind, so it can still be restored.
func readEvent(root, id string) (TrashEventInfo, error) {
	eventDir := filepath.Join(trashDir(root), id)
	ev := TrashEventInfo{ID: id}

	var lines []eventLine
	if f, err := os.Open(filepath.Join(trashLogDir(root), id+".jsonl")); err == nil {
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 64*1024), 1<<20)
		first := true
		for sc.Scan() {
			if first {
				first = false
				var h eventHeader
				if json.Unmarshal(sc.Bytes(), &h) == nil {
					ev.At, ev.Label, ev.Source = h.At, h.Label, h.Source
				}
				continue
			}
			var l eventLine
			if json.Unmarshal(sc.Bytes(), &l) == nil && l.Stored != "" {
				lines = append(lines, l)
			}
		}
		f.Close()
	}
	if ev.At.IsZero() {
		if fi, err := os.Stat(eventDir); err == nil {
			ev.At = fi.ModTime()
		}
	}

	described := make(map[string]bool, len(lines))
	for _, l := range lines {
		abs, err := JoinSharePath(eventDir, l.Stored)
		if err != nil {
			continue
		}
		fi, err := os.Lstat(abs)
		if err != nil || fi.IsDir() {
			continue
		}
		described[l.Stored] = true
		ev.Items = append(ev.Items, TrashItem{RelPath: l.Path, Kind: l.Kind, Size: fi.Size(), stored: l.Stored})
		ev.Size += fi.Size()
	}

	err := filepath.WalkDir(eventDir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil //nolint:nilerr // an unreadable corner of the trash is skipped, not fatal
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(eventDir, p)
		if err != nil {
			return nil //nolint:nilerr // can't happen under WalkDir(eventDir)
		}
		stored := filepath.ToSlash(rel)
		if described[stored] || ValidateRelPath(stored) != nil {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil //nolint:nilerr // vanished mid-walk
		}
		ev.Items = append(ev.Items, TrashItem{RelPath: stored, Size: info.Size(), stored: stored})
		ev.Size += info.Size()
		return nil
	})
	if err != nil {
		return TrashEventInfo{}, fmt.Errorf("sync: trash: read event %s: %w", id, err)
	}
	sort.Slice(ev.Items, func(i, j int) bool { return ev.Items[i].RelPath < ev.Items[j].RelPath })
	return ev, nil
}

// --- restoring -------------------------------------------------------------

// Restore puts files from one trash event back into the share at root:
// the paths in relpaths, or every file in the event if relpaths is empty.
// Each file is moved back, so it leaves the trash, and its index row is
// written with nodeID's counter bumped over whatever the index holds, so
// the restore propagates to peers as a new change.
//
// A file that is in the way is moved into a new "replaced by restore"
// event first, so a restore can itself be undone and never loses anything.
// A directory in the way is not moved; that path fails with
// ErrRestoreDestExists and the rest of the restore goes ahead.
//
// It returns the rows written, and an error joining every path that failed.
func (tr *Trash) Restore(ctx context.Context, store *index.Store, nodeID, shareID, root, eventID string, relpaths []string) ([]index.FileRow, error) {
	if strings.ContainsAny(eventID, `/\`) || eventID == "" || eventID == "." || eventID == ".." {
		return nil, fmt.Errorf("%w: event %q", ErrTrashEntryNotFound, eventID)
	}
	if _, err := os.Stat(filepath.Join(trashDir(root), eventID)); err != nil {
		return nil, fmt.Errorf("%w: event %q", ErrTrashEntryNotFound, eventID)
	}
	ev, err := readEvent(root, eventID)
	if err != nil {
		return nil, err
	}

	items := ev.Items
	if len(relpaths) > 0 {
		byPath := make(map[string]TrashItem, len(ev.Items))
		for _, it := range ev.Items {
			// The latest copy of a path in the event wins, matching what
			// the share held just before the event ended.
			if prev, ok := byPath[it.RelPath]; !ok || it.stored > prev.stored {
				byPath[it.RelPath] = it
			}
		}
		items = items[:0:0]
		for _, p := range relpaths {
			it, ok := byPath[p]
			if !ok {
				return nil, fmt.Errorf("%w: %q in event %q", ErrTrashEntryNotFound, p, eventID)
			}
			items = append(items, it)
		}
	}

	replaced := tr.begin(root, "replaced by restore", TrashSourceRestore, tr.clock())
	defer replaced.Close()
	eventDir := filepath.Join(trashDir(root), eventID)

	var rows []index.FileRow
	var errs []error
	for _, it := range items {
		if ctx.Err() != nil {
			errs = append(errs, ctx.Err())
			break
		}
		row, err := tr.restoreOne(ctx, store, nodeID, shareID, root, eventDir, it, replaced)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		rows = append(rows, row)
	}
	pruneEmptyEvent(root, eventID)
	return rows, errors.Join(errs...)
}

func (tr *Trash) restoreOne(ctx context.Context, store *index.Store, nodeID, shareID, root, eventDir string, it TrashItem, replaced *TrashEvent) (index.FileRow, error) {
	src, err := JoinSharePath(eventDir, it.stored)
	if err != nil {
		return index.FileRow{}, fmt.Errorf("sync: trash: restore %s: %w", it.RelPath, err)
	}
	dest, err := JoinSharePath(root, it.RelPath)
	if err != nil {
		return index.FileRow{}, fmt.Errorf("sync: trash: restore %s: %w", it.RelPath, err)
	}
	if fi, err := os.Lstat(dest); err == nil {
		if fi.IsDir() {
			return index.FileRow{}, fmt.Errorf("sync: trash: restore %s: %w", it.RelPath, ErrRestoreDestExists)
		}
		if err := replaced.Put(ctx, it.RelPath, dest, TrashOverwritten); err != nil {
			return index.FileRow{}, fmt.Errorf("sync: trash: restore %s: keep the file it replaces: %w", it.RelPath, err)
		}
	} else if !os.IsNotExist(err) {
		return index.FileRow{}, fmt.Errorf("sync: trash: restore %s: stat destination: %w", it.RelPath, err)
	}
	if err := os.MkdirAll(filepath.Dir(dest), 0o700); err != nil {
		return index.FileRow{}, fmt.Errorf("sync: trash: restore %s: mkdir: %w", it.RelPath, err)
	}
	if err := tr.moveOrCopy(src, dest); err != nil {
		return index.FileRow{}, fmt.Errorf("sync: trash: restore %s: %w", it.RelPath, err)
	}

	row, err := buildRestoredRow(ctx, store, nodeID, shareID, it.RelPath, dest)
	if err != nil {
		return index.FileRow{}, err
	}
	if err := store.PutFile(ctx, row); err != nil {
		return index.FileRow{}, fmt.Errorf("sync: trash: restore %s: put index row: %w", it.RelPath, err)
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
	h := sha256.New()
	if fi.Mode()&os.ModeSymlink != 0 {
		typ = protocol.FileTypeSymlink
		target, err := os.Readlink(destAbs)
		if err != nil {
			return index.FileRow{}, fmt.Errorf("sync: trash: restore %s: readlink: %w", relpath, err)
		}
		h.Write([]byte(target))
	} else {
		f, err := os.Open(destAbs)
		if err != nil {
			return index.FileRow{}, fmt.Errorf("sync: trash: restore %s: read: %w", relpath, err)
		}
		_, err = io.Copy(h, f)
		f.Close()
		if err != nil {
			return index.FileRow{}, fmt.Errorf("sync: trash: restore %s: read: %w", relpath, err)
		}
	}

	return index.FileRow{
		ShareID:   shareID,
		RelPath:   relpath,
		Type:      typ,
		Size:      fi.Size(),
		MTimeNS:   fi.ModTime().UnixNano(),
		Mode:      uint32(fi.Mode().Perm()),
		SHA256:    h.Sum(nil),
		Version:   Bump(current.Version, nodeID),
		Deleted:   false,
		UpdatedAt: time.Now(),
	}, nil
}

// pruneEmptyEvent removes an event whose folder no longer holds any file,
// along with its log. Only os.Remove is used on the folders, never
// RemoveAll, so a folder that still holds something is left alone.
func pruneEmptyEvent(root, eventID string) {
	eventDir := filepath.Join(trashDir(root), eventID)
	var dirs []string
	hasFile := false
	_ = filepath.WalkDir(eventDir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			hasFile = true // unreadable: assume something is there
			return fs.SkipDir
		}
		if d.IsDir() {
			dirs = append(dirs, p)
		} else {
			hasFile = true
		}
		return nil
	})
	if hasFile {
		return
	}
	for i := len(dirs) - 1; i >= 0; i-- {
		_ = os.Remove(dirs[i])
	}
	_ = os.Remove(filepath.Join(trashLogDir(root), eventID+".jsonl"))
}

// --- expiry ------------------------------------------------------------

// TrashSweep is the result of one Trash.Sweep over a share.
type TrashSweep struct {
	Purged int   // events removed
	Size   int64 // bytes the trash holds afterwards
}

// Sweep expires events from the trash of the share at root: every event
// older than maxAge, then, oldest first, as many more as it takes to bring
// the trash under maxBytes. Events are only ever removed whole. The newest
// event is never removed for size, even when it alone is over the limit,
// because a large change is exactly the one someone may want to undo.
func (tr *Trash) Sweep(root string, maxAge time.Duration, maxBytes int64) (TrashSweep, error) {
	events, err := tr.List(root)
	if err != nil {
		return TrashSweep{}, err
	}
	// List is newest first; walk oldest first.
	sort.SliceStable(events, func(i, j int) bool { return events[i].At.Before(events[j].At) })

	var total int64
	for _, ev := range events {
		total += ev.Size
	}
	cutoff := tr.clock().Add(-maxAge)
	var res TrashSweep
	var errs []error
	for i, ev := range events {
		expired := ev.At.Before(cutoff)
		overSize := total > maxBytes && i < len(events)-1
		if !expired && !overSize {
			continue
		}
		if err := removeEvent(root, ev.ID); err != nil {
			errs = append(errs, err)
			continue
		}
		total -= ev.Size
		res.Purged++
	}
	res.Size = total

	// An event the user emptied by hand is left as bare folders, which List
	// never reports; tidy those up too.
	if dirs, err := os.ReadDir(trashDir(root)); err == nil {
		for _, d := range dirs {
			if d.IsDir() {
				pruneEmptyEvent(root, d.Name())
			}
		}
	}
	return res, errors.Join(errs...)
}

// removeEvent deletes one event's folder and log. id comes from reading
// the trash directory, so it names a direct child of it; it is checked
// again anyway before anything is removed recursively.
func removeEvent(root, id string) error {
	if id == "" || id == "." || id == ".." || strings.ContainsAny(id, `/\`) {
		return fmt.Errorf("sync: trash: refusing to remove event %q", id)
	}
	if err := os.RemoveAll(filepath.Join(trashDir(root), id)); err != nil {
		return fmt.Errorf("sync: trash: remove event %s: %w", id, err)
	}
	if err := os.Remove(filepath.Join(trashLogDir(root), id+".jsonl")); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("sync: trash: remove event log %s: %w", id, err)
	}
	return nil
}

// --- migrating the old trash -------------------------------------------

// legacyTrashSuffixRe recognizes the "<ts>" or "<ts>-<n>" suffix the old
// trash appended to the last element of a trashed file's relpath, under
// <datadir>/trash/<share-id>/. Group 1 is the original name, group 2 the
// unix timestamp.
var legacyTrashSuffixRe = regexp.MustCompile(`^(.*)\.(\d{1,19})(?:-(\d+))?$`)

// MigrateLegacyTrash moves the old trash for one share, legacyDir (that
// is, <datadir>/trash/<share-id>), into the share's trash at root. Entries
// trashed in the same second were almost always one change, so each
// distinct second becomes one event, "migrated from old trash". Each file
// is moved (or, across filesystems, copied, fsynced, and checked before the
// original is removed), so a failure leaves the rest of the old trash in
// place to try again. Empty directories left behind are removed.
//
// It returns how many files it moved, and an error joining every one it
// could not.
func (tr *Trash) MigrateLegacyTrash(ctx context.Context, legacyDir, root string) (int, error) {
	type legacyEntry struct {
		abs, relpath string
	}
	bySecond := map[int64][]legacyEntry{}
	err := filepath.WalkDir(legacyDir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(legacyDir, p)
		if err != nil {
			return nil //nolint:nilerr // can't happen under WalkDir(legacyDir)
		}
		dir, base := path.Split(filepath.ToSlash(rel))
		m := legacyTrashSuffixRe.FindStringSubmatch(base)
		if m == nil {
			return nil // not a name the old trash produced; leave it
		}
		sec, err := strconv.ParseInt(m[2], 10, 64)
		if err != nil {
			return nil //nolint:nilerr // out of range: not ours
		}
		relpath := dir + m[1]
		if ValidateRelPath(relpath) != nil {
			return nil
		}
		bySecond[sec] = append(bySecond[sec], legacyEntry{abs: p, relpath: relpath})
		return nil
	})
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("sync: trash: read old trash %s: %w", legacyDir, err)
	}

	secs := make([]int64, 0, len(bySecond))
	for s := range bySecond {
		secs = append(secs, s)
	}
	sort.Slice(secs, func(i, j int) bool { return secs[i] < secs[j] })

	moved := 0
	var errs []error
	for _, sec := range secs {
		ev := tr.begin(root, "migrated from old trash", TrashSourceMigrated, time.Unix(sec, 0))
		for _, e := range bySecond[sec] {
			if err := ev.Put(ctx, e.relpath, e.abs, TrashMigrated); err != nil {
				errs = append(errs, err)
				continue
			}
			moved++
		}
		ev.Close()
	}
	pruneEmptyDirsUnder(legacyDir)
	return moved, errors.Join(errs...)
}

// pruneEmptyDirsUnder removes every empty directory under dir, and dir
// itself if that leaves it empty. Only os.Remove is used, so anything that
// still holds a file stays.
func pruneEmptyDirsUnder(dir string) {
	var dirs []string
	_ = filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err == nil && d.IsDir() {
			dirs = append(dirs, p)
		}
		return nil
	})
	for i := len(dirs) - 1; i >= 0; i-- {
		_ = os.Remove(dirs[i])
	}
}

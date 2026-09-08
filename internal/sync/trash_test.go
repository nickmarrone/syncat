package sync

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/nickmarrone/syncat/internal/index"
	"github.com/nickmarrone/syncat/internal/protocol"
)

const trashTestShareID = "share-trash-1"

func fixedTrashClock(sec int64) Clock {
	return func() time.Time { return time.Unix(sec, 0) }
}

func newTestTrash(t *testing.T, clockSec int64) (*Trash, string) {
	t.Helper()
	dir := t.TempDir()
	root := filepath.Join(dir, "trash")
	return NewTrash(root, fixedTrashClock(clockSec)), root
}

func writeShareFile(t *testing.T, root, relpath, content string) string {
	t.Helper()
	full := filepath.Join(root, filepath.FromSlash(relpath))
	if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	return full
}

// 1. Trash on overwrite / delete: old content preserved, correct bytes.
func TestTrash_PutMovesContentIntoTrash(t *testing.T) {
	shareDir := t.TempDir()
	srcAbs := writeShareFile(t, shareDir, "dir1/notes.txt", "old content")

	tr, trashRoot := newTestTrash(t, 1735689600)

	if err := tr.Put(context.Background(), trashTestShareID, "dir1/notes.txt", srcAbs); err != nil {
		t.Fatalf("Put: %v", err)
	}

	if _, err := os.Lstat(srcAbs); !os.IsNotExist(err) {
		t.Fatalf("source should be gone after trash, stat err = %v", err)
	}

	want := filepath.Join(trashRoot, trashTestShareID, "dir1", "notes.txt.1735689600")
	data, err := os.ReadFile(want)
	if err != nil {
		t.Fatalf("read trashed file %s: %v", want, err)
	}
	if string(data) != "old content" {
		t.Fatalf("trashed content = %q, want %q", data, "old content")
	}
}

// 2. Nothing to trash: no-op, not an error.
func TestTrash_PutNoOpWhenSourceMissing(t *testing.T) {
	shareDir := t.TempDir()
	missing := filepath.Join(shareDir, "gone.txt")

	tr, trashRoot := newTestTrash(t, 1735689600)
	if err := tr.Put(context.Background(), trashTestShareID, "gone.txt", missing); err != nil {
		t.Fatalf("Put on missing source: %v", err)
	}
	if entries, err := os.ReadDir(trashRoot); err == nil && len(entries) != 0 {
		t.Fatalf("expected no trash created, got %v", entries)
	}
}

// 3. Cross-filesystem fallback: force EXDEV from rename and confirm
// copy+delete still preserves content and removes the original. We can't
// rely on a second real filesystem being available in the test
// environment, so the "same filesystem?" decision is injected via a
// wrapped rename func that reports the same error os.Rename would return
// crossing a real mount boundary.
func TestTrash_CrossDeviceFallback(t *testing.T) {
	shareDir := t.TempDir()
	srcAbs := writeShareFile(t, shareDir, "big.bin", "cross-device content")

	tr, trashRoot := newTestTrash(t, 1735689600)
	renameCalled := false
	tr.rename = func(oldpath, newpath string) error {
		renameCalled = true
		return &os.LinkError{Op: "rename", Old: oldpath, New: newpath, Err: syscall.EXDEV}
	}

	if err := tr.Put(context.Background(), trashTestShareID, "big.bin", srcAbs); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if !renameCalled {
		t.Fatal("expected rename to be attempted first")
	}
	if _, err := os.Lstat(srcAbs); !os.IsNotExist(err) {
		t.Fatalf("source should be removed after copy+delete fallback, err = %v", err)
	}

	want := filepath.Join(trashRoot, trashTestShareID, "big.bin.1735689600")
	data, err := os.ReadFile(want)
	if err != nil {
		t.Fatalf("read trashed file: %v", err)
	}
	if string(data) != "cross-device content" {
		t.Fatalf("trashed content = %q", data)
	}
}

// Cross-device copy failing partway must not lose the original: the
// partial trash copy is cleaned up and the source is left alone.
func TestTrash_CrossDeviceFallback_PartialCopyLeavesSourceIntact(t *testing.T) {
	shareDir := t.TempDir()
	srcAbs := writeShareFile(t, shareDir, "big.bin", "content that will fail to copy")

	// Point trash at a path that will fail to receive the copy (a file in
	// place of what should be the trash share directory), while still
	// reporting EXDEV from rename, so copyThenRemove's OpenFile fails.
	dir := t.TempDir()
	trashRoot := filepath.Join(dir, "trash")
	if err := os.MkdirAll(filepath.Join(trashRoot, trashTestShareID), 0o700); err != nil {
		t.Fatal(err)
	}
	// Make the destination's parent directory unwritable to force the
	// create to fail without depending on a second filesystem.
	blocked := filepath.Join(trashRoot, trashTestShareID, "blocked")
	if err := os.WriteFile(blocked, []byte("not a dir"), 0o644); err != nil {
		t.Fatal(err)
	}

	tr := NewTrash(trashRoot, fixedTrashClock(1735689600))
	tr.rename = func(oldpath, newpath string) error {
		return &os.LinkError{Op: "rename", Old: oldpath, New: newpath, Err: syscall.EXDEV}
	}

	// relpath "blocked/x" makes destPath's MkdirAll(filepath.Dir(...)) try
	// to MkdirAll through the "blocked" regular file, which fails cleanly.
	err := tr.Put(context.Background(), trashTestShareID, "blocked/x", srcAbs)
	if err == nil {
		t.Fatal("expected an error when the trash destination can't be created")
	}
	data, statErr := os.ReadFile(srcAbs)
	if statErr != nil {
		t.Fatalf("source must survive a failed trash attempt, stat err = %v", statErr)
	}
	if string(data) != "content that will fail to copy" {
		t.Fatalf("source content changed: %q", data)
	}
}

// 4. Collision: the same relpath trashed twice within the same unix
// second doesn't overwrite the first copy.
func TestTrash_CollisionKeepsBothCopies(t *testing.T) {
	shareDir := t.TempDir()
	tr, trashRoot := newTestTrash(t, 1735689600)

	src1 := writeShareFile(t, shareDir, "notes.txt", "first version")
	if err := tr.Put(context.Background(), trashTestShareID, "notes.txt", src1); err != nil {
		t.Fatalf("Put 1: %v", err)
	}

	src2 := writeShareFile(t, shareDir, "notes.txt", "second version")
	if err := tr.Put(context.Background(), trashTestShareID, "notes.txt", src2); err != nil {
		t.Fatalf("Put 2: %v", err)
	}

	first := filepath.Join(trashRoot, trashTestShareID, "notes.txt.1735689600")
	second := filepath.Join(trashRoot, trashTestShareID, "notes.txt.1735689600-1")

	d1, err := os.ReadFile(first)
	if err != nil {
		t.Fatalf("read first trash copy: %v", err)
	}
	d2, err := os.ReadFile(second)
	if err != nil {
		t.Fatalf("read second trash copy: %v", err)
	}
	if string(d1) != "first version" {
		t.Fatalf("first copy = %q, want %q", d1, "first version")
	}
	if string(d2) != "second version" {
		t.Fatalf("second copy = %q, want %q", d2, "second version")
	}
}

// 5. List returns relpath/trashed-at/size; Restore copies content back and
// bumps the local version so the file dominates whatever the index held
// before (i.e. it will propagate).
func TestTrash_ListAndRestore(t *testing.T) {
	ctx := context.Background()
	shareDir := t.TempDir()
	tr, _ := newTestTrash(t, 1735689600)

	src := writeShareFile(t, shareDir, "dir1/report.txt", "trashed content")
	if err := tr.Put(ctx, trashTestShareID, "dir1/report.txt", src); err != nil {
		t.Fatalf("Put: %v", err)
	}

	entries, err := tr.List(trashTestShareID)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("len(entries) = %d, want 1: %+v", len(entries), entries)
	}
	e := entries[0]
	if e.RelPath != "dir1/report.txt" {
		t.Fatalf("RelPath = %q, want %q", e.RelPath, "dir1/report.txt")
	}
	if !e.TrashedAt.Equal(time.Unix(1735689600, 0)) {
		t.Fatalf("TrashedAt = %v", e.TrashedAt)
	}
	if e.Size != int64(len("trashed content")) {
		t.Fatalf("Size = %d, want %d", e.Size, len("trashed content"))
	}

	store := openTestTrashStore(t)
	// Seed an existing index row with some version, as if this file had
	// been synced before at v{node: 3}.
	existing := index.FileRow{
		ShareID: trashTestShareID, RelPath: "dir1/report.txt", Type: protocol.FileTypeFile,
		Version: protocol.VersionVector{"nodeA": 3}, Deleted: true, UpdatedAt: time.Now(),
	}
	if err := store.PutFile(ctx, existing); err != nil {
		t.Fatalf("seed index: %v", err)
	}

	row, err := tr.Restore(ctx, store, "nodeA", shareDir, e)
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}

	restoredPath := filepath.Join(shareDir, "dir1", "report.txt")
	data, err := os.ReadFile(restoredPath)
	if err != nil {
		t.Fatalf("read restored file: %v", err)
	}
	if string(data) != "trashed content" {
		t.Fatalf("restored content = %q", data)
	}

	if !Dominates(row.Version, existing.Version) {
		t.Fatalf("restored version %v does not dominate previous %v", row.Version, existing.Version)
	}
	if row.Version["nodeA"] != 4 {
		t.Fatalf("restored version[nodeA] = %d, want 4", row.Version["nodeA"])
	}
	if row.Deleted {
		t.Fatal("restored row should not be a tombstone")
	}

	stored, err := store.GetFile(ctx, trashTestShareID, "dir1/report.txt")
	if err != nil {
		t.Fatalf("GetFile after restore: %v", err)
	}
	if !Equal(stored.Version, row.Version) {
		t.Fatalf("index not updated: stored=%v returned=%v", stored.Version, row.Version)
	}

	// The trash entry itself must still be there (restore copies, doesn't
	// move).
	if _, err := os.Stat(filepath.Join(tr.root, trashTestShareID, "dir1", "report.txt.1735689600")); err != nil {
		t.Fatalf("trash entry should survive a restore: %v", err)
	}
}

// Restore-collision policy: refuse rather than silently clobber a file
// that now occupies the destination path.
func TestTrash_RestoreRefusesToClobberExistingFile(t *testing.T) {
	ctx := context.Background()
	shareDir := t.TempDir()
	tr, _ := newTestTrash(t, 1735689600)

	src := writeShareFile(t, shareDir, "report.txt", "original trashed content")
	if err := tr.Put(ctx, trashTestShareID, "report.txt", src); err != nil {
		t.Fatalf("Put: %v", err)
	}
	entries, err := tr.List(trashTestShareID)
	if err != nil || len(entries) != 1 {
		t.Fatalf("List: %v, %+v", err, entries)
	}

	// A different file now occupies that path.
	writeShareFile(t, shareDir, "report.txt", "unrelated new content")

	store := openTestTrashStore(t)
	_, err = tr.Restore(ctx, store, "nodeA", shareDir, entries[0])
	if !errors.Is(err, ErrRestoreDestExists) {
		t.Fatalf("Restore error = %v, want wrapping ErrRestoreDestExists", err)
	}

	data, rerr := os.ReadFile(filepath.Join(shareDir, "report.txt"))
	if rerr != nil {
		t.Fatalf("read existing file: %v", rerr)
	}
	if string(data) != "unrelated new content" {
		t.Fatalf("existing file was clobbered: %q", data)
	}
}

// 6. Trash failure aborts the apply: trashHook (the exact call every
// destructive apply.go path makes immediately before its destructive step
// — see applyDelete and pullAndInstall) must return an error without
// touching the original file when the underlying Trash.Put fails, so the
// caller never reaches its rename/remove.
func TestTrash_FailureAbortsApply(t *testing.T) {
	ctx := context.Background()
	shareDir := t.TempDir()
	writeShareFile(t, shareDir, "notes.txt", "local original — must survive")
	store := openTestTrashStore(t)

	// Force every Put to fail: point the trash root at a path that is
	// actually a regular file, so Trash.Put's MkdirAll under it always
	// errors before anything is moved.
	blockedRoot := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(blockedRoot, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	failingTrash := NewTrash(blockedRoot, nil)

	sess := NewSession(nil, store, "nodeA", "nodeB", nil, nil)
	sess.SetTrash(failingTrash)

	destAbs := filepath.Join(shareDir, "notes.txt")
	if err := sess.trashHook(ctx, testShareID, "notes.txt", destAbs); err == nil {
		t.Fatal("expected trashHook to fail with a broken trash root")
	}

	data, rerr := os.ReadFile(destAbs)
	if rerr != nil {
		t.Fatalf("original file must survive a failed trash: %v", rerr)
	}
	if string(data) != "local original — must survive" {
		t.Fatalf("original file content changed: %q", data)
	}

	// And the concrete apply.go call sites honor that: applyDelete must
	// not remove the file either.
	cfg := ShareConfig{ShareID: testShareID, Root: shareDir}
	if _, err := sess.applyDelete(ctx, cfg, "notes.txt"); err == nil {
		t.Fatal("expected applyDelete to abort when trashing fails")
	}
	if _, err := os.Stat(destAbs); err != nil {
		t.Fatalf("applyDelete must leave the file in place on trash failure: %v", err)
	}
}

// 7. Structural local-vs-remote distinction: a local delete the user makes
// (observed by the scanner, applied via index.ApplyScanResult — a code
// path with zero dependency on this package) must never trash anything. A
// remote-initiated delete, going through applyDelete, must.
func TestTrash_LocalDeleteIsNeverTrashed_RemoteDeleteIs(t *testing.T) {
	ctx := context.Background()
	shareDir := t.TempDir()
	writeShareFile(t, shareDir, "keep-me-untrashed.txt", "local content")
	writeShareFile(t, shareDir, "remote-deleted.txt", "remote content")

	store := openTestTrashStore(t)
	sc := index.NewScanner(os.DirFS(shareDir), nil)
	result, err := sc.Scan(ctx, trashTestShareID, nil)
	if err != nil {
		t.Fatalf("initial scan: %v", err)
	}
	if err := store.ApplyScanResult(ctx, result); err != nil {
		t.Fatalf("apply initial scan: %v", err)
	}

	tr, trashRoot := newTestTrash(t, 1735689600)

	// --- local delete: the user removes the file directly; the scanner
	// just observes it's gone. This path never touches internal/sync.
	if err := os.Remove(filepath.Join(shareDir, "keep-me-untrashed.txt")); err != nil {
		t.Fatal(err)
	}
	existing, err := store.ListShareMap(ctx, trashTestShareID)
	if err != nil {
		t.Fatalf("ListShareMap: %v", err)
	}
	result2, err := sc.Scan(ctx, trashTestShareID, existing)
	if err != nil {
		t.Fatalf("rescan after local delete: %v", err)
	}
	if err := store.ApplyScanResult(ctx, result2); err != nil {
		t.Fatalf("apply rescan: %v", err)
	}
	if len(result2.Deleted) != 1 || result2.Deleted[0].RelPath != "keep-me-untrashed.txt" {
		t.Fatalf("expected local delete to be observed as a tombstone: %+v", result2.Deleted)
	}

	// --- remote-initiated delete: goes through Session.applyDelete, which
	// does call the trash hook.
	sess := NewSession(nil, store, "nodeA", "nodeB", nil, nil)
	sess.SetTrash(tr)
	cfg := ShareConfig{ShareID: trashTestShareID, Root: shareDir}
	deleted, err := sess.applyDelete(ctx, cfg, "remote-deleted.txt")
	if err != nil {
		t.Fatalf("applyDelete: %v", err)
	}
	if !deleted {
		t.Fatal("expected applyDelete to report the file as deleted")
	}

	// The local delete must have produced no trash entry anywhere.
	entries, err := tr.List(trashTestShareID)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected exactly one trash entry (the remote delete only), got %+v", entries)
	}
	if entries[0].RelPath != "remote-deleted.txt" {
		t.Fatalf("unexpected trash entry: %+v", entries[0])
	}
	if _, err := os.Stat(filepath.Join(trashRoot, trashTestShareID, "keep-me-untrashed.txt.1735689600")); !os.IsNotExist(err) {
		t.Fatalf("local delete must not appear in trash, stat err = %v", err)
	}
}

// 8. Very long relpath: nested-but-valid components succeed without
// panicking; Trash handles it gracefully end to end (Put, List, Restore).
func TestTrash_VeryLongRelPath(t *testing.T) {
	ctx := context.Background()
	shareDir := t.TempDir()

	// Build a deeply nested relpath comfortably under both the relpath
	// validator's 4096-byte cap and a single path component's typical
	// 255-byte filesystem limit, but still long enough to matter.
	comp := strings.Repeat("a", 200)
	relpath := strings.Join([]string{comp, comp, comp + ".txt"}, "/")

	src := writeShareFile(t, shareDir, relpath, "deep content")
	tr, _ := newTestTrash(t, 1735689600)

	if err := tr.Put(ctx, trashTestShareID, relpath, src); err != nil {
		t.Fatalf("Put with long relpath: %v", err)
	}
	entries, err := tr.List(trashTestShareID)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(entries) != 1 || entries[0].RelPath != relpath {
		t.Fatalf("entries = %+v, want relpath %q", entries, relpath)
	}

	store := openTestTrashStore(t)
	if _, err := tr.Restore(ctx, store, "nodeA", shareDir, entries[0]); err != nil {
		t.Fatalf("Restore with long relpath: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(shareDir, filepath.FromSlash(relpath)))
	if err != nil {
		t.Fatalf("read restored long-path file: %v", err)
	}
	if string(data) != "deep content" {
		t.Fatalf("restored content = %q", data)
	}
}

func openTestTrashStore(t *testing.T) *index.Store {
	t.Helper()
	dir := t.TempDir()
	store, err := index.Open(context.Background(), filepath.Join(dir, "index.db"))
	if err != nil {
		t.Fatalf("open index: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

// fakeJanitorClock is a manually-advanced JanitorClock: Now() returns
// whatever it's set to, and After(d) delivers on a channel only when the
// test explicitly Fires it — so the janitor's background loop never
// depends on a real timer or a real sleep.
type fakeJanitorClock struct {
	now   time.Time
	fired chan chan time.Time
}

func newFakeJanitorClock(now time.Time) *fakeJanitorClock {
	return &fakeJanitorClock{now: now, fired: make(chan chan time.Time, 16)}
}

func (c *fakeJanitorClock) Now() time.Time { return c.now }

func (c *fakeJanitorClock) After(time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	c.fired <- ch
	return ch
}

// tick advances the clock and fires the most recently requested timer
// (blocking until the loop has actually asked for one), synchronizing the
// test with the background goroutine without any sleep.
func (c *fakeJanitorClock) tick(t *testing.T, advance time.Duration) {
	t.Helper()
	c.now = c.now.Add(advance)
	select {
	case ch := <-c.fired:
		ch <- c.now
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for janitor loop to request a timer")
	}
}

// trashFileAt seeds a trash entry directly on disk, named exactly the way
// Trash.Put would (<relpath>.<unix-ts>), but at an explicit timestamp
// rather than whatever tr's configured clock reports — so a single Trash
// can be seeded with entries of several different ages for the janitor
// tests below, independent of Put's own clock.
func trashFileAt(t *testing.T, tr *Trash, shareID, relpath string, ts int64) {
	t.Helper()
	full := filepath.Join(tr.root, shareID, filepath.FromSlash(relpath)) + "." + itoa(ts)
	if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte("trashed"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// 1. Sweep purges entries older than retention and keeps fresher ones, on
// a fake "now".
func TestJanitor_SweepPurgesOldKeepsFresh(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	tr := NewTrash(filepath.Join(dir, "trash"), nil)

	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	retention := 30 * 24 * time.Hour

	oldTS := now.Add(-31 * 24 * time.Hour).Unix()  // older than retention: purge
	freshTS := now.Add(-1 * 24 * time.Hour).Unix() // within retention: keep
	edgeTS := now.Add(-30 * 24 * time.Hour).Unix() // exactly at retention: keep (Before(cutoff) is strict)

	trashFileAt(t, tr, trashTestShareID, "old.txt", oldTS)
	trashFileAt(t, tr, trashTestShareID, "fresh.txt", freshTS)
	trashFileAt(t, tr, trashTestShareID, "edge.txt", edgeTS)

	clock := newFakeJanitorClock(now)
	j := NewJanitor(tr, retention, clock, time.Hour, nil)

	purged, err := j.Sweep(ctx)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if purged != 1 {
		t.Fatalf("purged = %d, want 1", purged)
	}

	if _, err := os.Stat(filepath.Join(tr.root, trashTestShareID, "old.txt."+itoa(oldTS))); !os.IsNotExist(err) {
		t.Fatalf("old entry should be purged, stat err = %v", err)
	}
	if _, err := os.Stat(filepath.Join(tr.root, trashTestShareID, "fresh.txt."+itoa(freshTS))); err != nil {
		t.Fatalf("fresh entry should survive: %v", err)
	}
	if _, err := os.Stat(filepath.Join(tr.root, trashTestShareID, "edge.txt."+itoa(edgeTS))); err != nil {
		t.Fatalf("edge entry should survive: %v", err)
	}
}

// 2. A missing trash root is tolerated (no error, nothing purged).
func TestJanitor_MissingTrashDirTolerated(t *testing.T) {
	dir := t.TempDir()
	tr := NewTrash(filepath.Join(dir, "does-not-exist"), nil)
	j := NewJanitor(tr, 30*24*time.Hour, nil, time.Hour, nil)

	purged, err := j.Sweep(context.Background())
	if err != nil {
		t.Fatalf("Sweep on missing trash dir: %v", err)
	}
	if purged != 0 {
		t.Fatalf("purged = %d, want 0", purged)
	}
}

// 3. Sweep never touches anything outside the trash root: a sibling
// directory with an identically-named, identically-aged file is left
// alone.
func TestJanitor_NeverTouchesOutsideTrashRoot(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	tr := NewTrash(filepath.Join(dir, "trash"), nil)

	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	oldTS := now.Add(-60 * 24 * time.Hour).Unix()
	trashFileAt(t, tr, trashTestShareID, "old.txt", oldTS)

	// A sibling, outside the trash root, that happens to look exactly
	// like an expired trash entry.
	sibling := filepath.Join(dir, "not-trash", trashTestShareID, "old.txt."+itoa(oldTS))
	if err := os.MkdirAll(filepath.Dir(sibling), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(sibling, []byte("must survive"), 0o644); err != nil {
		t.Fatal(err)
	}

	clock := newFakeJanitorClock(now)
	j := NewJanitor(tr, 30*24*time.Hour, clock, time.Hour, nil)
	if _, err := j.Sweep(ctx); err != nil {
		t.Fatalf("Sweep: %v", err)
	}

	if _, err := os.Stat(sibling); err != nil {
		t.Fatalf("sibling outside trash root must survive: %v", err)
	}
}

// 4. The background loop, driven entirely by a fake clock, sweeps on
// schedule with no real sleeping, and Close stops it cleanly with no
// goroutine leak (verified by Close returning promptly and being safe to
// call once the loop has exited).
func TestJanitor_BackgroundLoopRunsOnFakeClockAndStopsCleanly(t *testing.T) {
	dir := t.TempDir()
	tr := NewTrash(filepath.Join(dir, "trash"), nil)

	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	oldTS := now.Add(-40 * 24 * time.Hour).Unix()
	trashFileAt(t, tr, trashTestShareID, "old.txt", oldTS)

	clock := newFakeJanitorClock(now)
	swept := make(chan int, 4)
	j := NewJanitor(tr, 30*24*time.Hour, clock, time.Hour, func(purged int, err error) {
		if err != nil {
			t.Errorf("unexpected sweep error: %v", err)
		}
		swept <- purged
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	j.Start(ctx)

	clock.tick(t, time.Hour) // fires the first sweep

	select {
	case n := <-swept:
		if n != 1 {
			t.Fatalf("first sweep purged = %d, want 1", n)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for first sweep")
	}

	clock.tick(t, time.Hour) // second sweep: nothing left to purge

	select {
	case n := <-swept:
		if n != 0 {
			t.Fatalf("second sweep purged = %d, want 0", n)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for second sweep")
	}

	if err := j.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Close is documented safe to call more than once.
	if err := j.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// TestSweepTempFiles covers the cleanup for staging files a killed daemon
// could not remove itself. Apply only renames a download into place once its
// content verifies, so an interrupted transfer leaves a full-size file behind
// — hidden, inside the user's own synced directory, one per crash.
func TestSweepTempFiles(t *testing.T) {
	root := t.TempDir()
	nested := filepath.Join(root, "sub", "deeper")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	// Two orphans, one of them in a subdirectory — staging files are written
	// beside their destination, so a nested tree leaves them nested too.
	orphans := []string{
		filepath.Join(root, TempFilePrefix+"123"),
		filepath.Join(nested, TempFilePrefix+"456"),
	}
	// ... and real files that must survive, including one whose name merely
	// resembles the prefix.
	keepers := []string{
		filepath.Join(root, "real.txt"),
		filepath.Join(nested, "also-real.txt"),
		filepath.Join(root, ".syncat.tmpsomething"),
	}
	for _, p := range append(append([]string{}, orphans...), keepers...) {
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatalf("seed %s: %v", p, err)
		}
	}

	removed, err := SweepTempFiles(root)
	if err != nil {
		t.Fatalf("SweepTempFiles: %v", err)
	}
	if removed != len(orphans) {
		t.Errorf("removed %d files, want %d", removed, len(orphans))
	}
	for _, p := range orphans {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("%s survived the sweep", filepath.Base(p))
		}
	}
	for _, p := range keepers {
		if _, err := os.Stat(p); err != nil {
			t.Errorf("%s was removed by the sweep, want it left alone", filepath.Base(p))
		}
	}

	// A root that does not exist is not an error: a subscription can be
	// configured for a directory that has not been created yet.
	if n, err := SweepTempFiles(filepath.Join(root, "nope")); err != nil || n != 0 {
		t.Errorf("SweepTempFiles on a missing dir = (%d, %v), want (0, nil)", n, err)
	}
}

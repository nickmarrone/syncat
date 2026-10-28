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
)

const trashTestShareID = "share-trash-1"

// trashTestTime is a fixed instant; event folder names are rendered in
// local time, so tests compute expected names with eventFolderName rather
// than spelling them out.
var trashTestTime = time.Date(2026, 10, 27, 10, 32, 5, 0, time.UTC)

func fixedTrashClock(at time.Time) Clock {
	return func() time.Time { return at }
}

func newTestTrash(at time.Time) *Trash { return NewTrash(fixedTrashClock(at)) }

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

func readFile(t *testing.T, p string) string {
	t.Helper()
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("read %s: %v", p, err)
	}
	return string(data)
}

func openTestTrashStore(t *testing.T) *index.Store {
	t.Helper()
	store, err := index.Open(context.Background(), filepath.Join(t.TempDir(), "index.db"))
	if err != nil {
		t.Fatalf("open index: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func mustList(t *testing.T, tr *Trash, root string) []TrashEventInfo {
	t.Helper()
	events, err := tr.List(root)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	return events
}

// The trash lives in the share, one folder per event, laid out like the
// share, so a file manager is enough to find and undo a change.
func TestTrash_PutMovesIntoEventFolderInsideShare(t *testing.T) {
	ctx := context.Background()
	share := t.TempDir()
	src := writeShareFile(t, share, "dir1/notes.txt", "old content")
	tr := newTestTrash(trashTestTime)

	ev := tr.BeginPeerEvent(share, "laptop")
	if err := ev.Put(ctx, "dir1/notes.txt", src, TrashDeleted); err != nil {
		t.Fatalf("Put: %v", err)
	}
	ev.Close()

	if _, err := os.Lstat(src); !os.IsNotExist(err) {
		t.Fatalf("source should be gone, stat err = %v", err)
	}
	id := eventFolderName(trashTestTime, "from laptop")
	want := filepath.Join(share, ".syncat", "trash", id, "dir1", "notes.txt")
	if got := readFile(t, want); got != "old content" {
		t.Errorf("trashed content = %q", got)
	}
	if _, err := os.Stat(filepath.Join(share, ".syncat", "README")); err != nil {
		t.Errorf("README marking .syncat as syncat's: %v", err)
	}

	events := mustList(t, tr, share)
	if len(events) != 1 {
		t.Fatalf("List = %+v, want one event", events)
	}
	e := events[0]
	if e.ID != id || e.Label != "from laptop" || e.Source != TrashSourcePeer || !e.At.Equal(trashTestTime) {
		t.Errorf("event = %+v", e)
	}
	if len(e.Items) != 1 || e.Items[0].RelPath != "dir1/notes.txt" || e.Items[0].Kind != TrashDeleted || e.Items[0].Size != 11 {
		t.Errorf("items = %+v", e.Items)
	}
}

func TestTrash_PutNoOpWhenSourceMissing(t *testing.T) {
	share := t.TempDir()
	tr := newTestTrash(trashTestTime)
	ev := tr.BeginPeerEvent(share, "laptop")
	if err := ev.Put(context.Background(), "nope.txt", filepath.Join(share, "nope.txt"), TrashOverwritten); err != nil {
		t.Fatalf("Put: %v", err)
	}
	ev.Close()
	if _, err := os.Stat(filepath.Join(share, ".syncat")); !os.IsNotExist(err) {
		t.Errorf("an event that trashed nothing created .syncat (stat err = %v)", err)
	}
}

func TestTrash_SamePathTwiceInOneEventKeepsBoth(t *testing.T) {
	ctx := context.Background()
	share := t.TempDir()
	tr := newTestTrash(trashTestTime)
	ev := tr.BeginPeerEvent(share, "laptop")
	defer ev.Close()

	if err := ev.Put(ctx, "a.txt", writeShareFile(t, share, "a.txt", "first"), TrashOverwritten); err != nil {
		t.Fatal(err)
	}
	if err := ev.Put(ctx, "a.txt", writeShareFile(t, share, "a.txt", "second"), TrashDeleted); err != nil {
		t.Fatal(err)
	}
	eventDir := filepath.Join(share, ".syncat", "trash", ev.id)
	if readFile(t, filepath.Join(eventDir, "a.txt")) != "first" || readFile(t, filepath.Join(eventDir, "a (2).txt")) != "second" {
		t.Error("both copies of a.txt should be kept")
	}
	if items := mustList(t, tr, share)[0].Items; len(items) != 2 || items[0].RelPath != "a.txt" || items[1].RelPath != "a.txt" {
		t.Errorf("items = %+v, want a.txt twice", items)
	}
}

func TestTrash_TwoEventsInOneSecondGetDistinctFolders(t *testing.T) {
	ctx := context.Background()
	share := t.TempDir()
	tr := newTestTrash(trashTestTime)
	for i := 0; i < 2; i++ {
		ev := tr.BeginPeerEvent(share, "laptop")
		if err := ev.Put(ctx, "x.txt", writeShareFile(t, share, "x.txt", "x"), TrashDeleted); err != nil {
			t.Fatal(err)
		}
		ev.Close()
	}
	if events := mustList(t, tr, share); len(events) != 2 || events[0].ID == events[1].ID {
		t.Errorf("events = %+v, want two distinct", events)
	}
}

func TestTrash_PeerNameIsSanitizedInFolderName(t *testing.T) {
	name := eventFolderName(trashTestTime, "from ../evil/peer:name")
	if strings.ContainsAny(name, `/\:`) {
		t.Errorf("event folder name %q contains a path separator or colon", name)
	}
}

func TestTrash_CrossDeviceFallback(t *testing.T) {
	ctx := context.Background()
	share := t.TempDir()
	src := writeShareFile(t, share, "big.bin", "cross-device content")
	tr := newTestTrash(trashTestTime)
	tr.rename = func(oldpath, newpath string) error {
		return &os.LinkError{Op: "rename", Old: oldpath, New: newpath, Err: syscall.EXDEV}
	}

	ev := tr.BeginPeerEvent(share, "laptop")
	defer ev.Close()
	if err := ev.Put(ctx, "big.bin", src, TrashDeleted); err != nil {
		t.Fatalf("Put with cross-device fallback: %v", err)
	}
	if _, err := os.Lstat(src); !os.IsNotExist(err) {
		t.Fatalf("source should be removed after a successful copy, err = %v", err)
	}
	if got := readFile(t, filepath.Join(share, ".syncat", "trash", ev.id, "big.bin")); got != "cross-device content" {
		t.Errorf("copied content = %q", got)
	}
}

// A file the trash can't take must not be deleted or overwritten: the
// destructive step aborts and the file stays.
func TestTrash_FailureAbortsApply(t *testing.T) {
	ctx := context.Background()
	share := t.TempDir()
	dest := writeShareFile(t, share, "notes.txt", "local original, must survive")
	// A regular file where .syncat should be: the trash can't be created.
	writeShareFile(t, share, ".syncat", "in the way")

	tr := newTestTrash(trashTestTime)
	sess := NewSession(nil, openTestTrashStore(t), "nodeA", "nodeB", SessionOptions{Trash: tr})
	ctx = withTrashEvent(ctx, tr.BeginPeerEvent(share, "nodeB"))

	if err := sess.trashHook(ctx, "notes.txt", dest, TrashOverwritten); err == nil {
		t.Fatal("expected trashHook to fail")
	}
	cfg := ShareConfig{ShareID: testShareID, Root: share}
	if _, err := sess.applyDelete(ctx, cfg, "notes.txt"); err == nil {
		t.Fatal("expected applyDelete to abort when trashing fails")
	}
	if got := readFile(t, dest); got != "local original, must survive" {
		t.Errorf("file content = %q", got)
	}
}

// Every apply runs inside a reconcile pass that started a trash event. One
// that somehow doesn't must fail closed, not discard the file untrashed.
func TestTrash_HookWithoutEventFailsClosed(t *testing.T) {
	share := t.TempDir()
	dest := writeShareFile(t, share, "notes.txt", "keep")
	sess := NewSession(nil, openTestTrashStore(t), "nodeA", "nodeB", SessionOptions{Trash: newTestTrash(trashTestTime)})
	if _, err := sess.applyDelete(context.Background(), ShareConfig{ShareID: testShareID, Root: share}, "notes.txt"); err == nil {
		t.Fatal("applyDelete without a trash event succeeded")
	}
	if _, err := os.Stat(dest); err != nil {
		t.Errorf("file removed: %v", err)
	}
}

// A local delete the user makes (observed by the scanner, a code path with
// no dependency on this package) never reaches the trash; a peer's delete,
// through applyDelete, does.
func TestTrash_LocalDeleteIsNeverTrashed_RemoteDeleteIs(t *testing.T) {
	ctx := context.Background()
	share := t.TempDir()
	writeShareFile(t, share, "local.txt", "local content")
	writeShareFile(t, share, "remote.txt", "remote content")

	store := openTestTrashStore(t)
	sc := index.NewScanner(os.DirFS(share), nil)
	result, err := sc.Scan(ctx, trashTestShareID, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ApplyScanResult(ctx, result); err != nil {
		t.Fatal(err)
	}

	if err := os.Remove(filepath.Join(share, "local.txt")); err != nil {
		t.Fatal(err)
	}
	existing, err := store.ListShareMap(ctx, trashTestShareID)
	if err != nil {
		t.Fatal(err)
	}
	result2, err := sc.Scan(ctx, trashTestShareID, existing)
	if err != nil {
		t.Fatal(err)
	}
	if len(result2.Deleted) != 1 || result2.Deleted[0].RelPath != "local.txt" {
		t.Fatalf("local delete not observed: %+v", result2.Deleted)
	}

	tr := newTestTrash(trashTestTime)
	sess := NewSession(nil, store, "nodeA", "nodeB", SessionOptions{Trash: tr})
	ev := tr.BeginPeerEvent(share, "nodeB")
	if _, err := sess.applyDelete(withTrashEvent(ctx, ev), ShareConfig{ShareID: trashTestShareID, Root: share}, "remote.txt"); err != nil {
		t.Fatal(err)
	}
	ev.Close()

	events := mustList(t, tr, share)
	if len(events) != 1 || len(events[0].Items) != 1 || events[0].Items[0].RelPath != "remote.txt" {
		t.Fatalf("trash = %+v, want only remote.txt", events)
	}
}

// The scanner never indexes syncat's own directory, so the trash is never
// synced and never counts towards a hold.
func TestTrash_ReservedDirIsNotScanned(t *testing.T) {
	share := t.TempDir()
	tr := newTestTrash(trashTestTime)
	ev := tr.BeginPeerEvent(share, "laptop")
	if err := ev.Put(context.Background(), "a.txt", writeShareFile(t, share, "a.txt", "a"), TrashDeleted); err != nil {
		t.Fatal(err)
	}
	ev.Close()
	writeShareFile(t, share, "b.txt", "b")

	result, err := index.NewScanner(os.DirFS(share), nil).Scan(context.Background(), trashTestShareID, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range result.Added {
		if strings.HasPrefix(r.RelPath, ".syncat") {
			t.Errorf("scanner indexed %s", r.RelPath)
		}
	}
	if len(result.Added) != 1 {
		t.Errorf("Added = %v, want only b.txt", result.Added)
	}
}

func TestValidateRelPathRejectsReservedDir(t *testing.T) {
	for _, p := range []string{".syncat", ".syncat/trash/x", ".syncat/README"} {
		if ValidateRelPath(p) == nil {
			t.Errorf("ValidateRelPath(%q) accepted a path into the reserved dir", p)
		}
	}
	for _, p := range []string{"sub/.syncat", ".syncatx", "a/.syncat/b"} {
		if err := ValidateRelPath(p); err != nil {
			t.Errorf("ValidateRelPath(%q) = %v, want ok: only the root's .syncat is reserved", p, err)
		}
	}
}

func TestEnsureReservedDirRefusesSomeoneElses(t *testing.T) {
	share := t.TempDir()
	if err := os.Mkdir(filepath.Join(share, ".syncat"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := EnsureReservedDir(share); !errors.Is(err, ErrReservedDirTaken) {
		t.Errorf("EnsureReservedDir over a foreign .syncat = %v, want ErrReservedDirTaken", err)
	}

	ours := t.TempDir()
	if err := EnsureReservedDir(ours); err != nil {
		t.Fatalf("first EnsureReservedDir: %v", err)
	}
	if err := EnsureReservedDir(ours); err != nil {
		t.Errorf("EnsureReservedDir on its own dir: %v", err)
	}
}

// --- restoring -------------------------------------------------------------

func trashOne(t *testing.T, tr *Trash, share, relpath, content string) TrashEventInfo {
	t.Helper()
	ev := tr.BeginPeerEvent(share, "laptop")
	if err := ev.Put(context.Background(), relpath, writeShareFile(t, share, relpath, content), TrashDeleted); err != nil {
		t.Fatal(err)
	}
	ev.Close()
	for _, e := range mustList(t, tr, share) {
		if e.ID == ev.id {
			return e
		}
	}
	t.Fatalf("event %s not listed", ev.id)
	return TrashEventInfo{}
}

func TestTrash_RestoreWholeEventBumpsVersion(t *testing.T) {
	ctx := context.Background()
	share := t.TempDir()
	tr := newTestTrash(trashTestTime)
	ev := tr.BeginPeerEvent(share, "laptop")
	for _, p := range []string{"a.txt", "sub/b.txt"} {
		if err := ev.Put(ctx, p, writeShareFile(t, share, p, "content of "+p), TrashDeleted); err != nil {
			t.Fatal(err)
		}
	}
	ev.Close()

	store := openTestTrashStore(t)
	rows, err := tr.Restore(ctx, store, "nodeA", trashTestShareID, share, ev.id, nil)
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("rows = %+v, want 2", rows)
	}
	for _, p := range []string{"a.txt", "sub/b.txt"} {
		if got := readFile(t, filepath.Join(share, p)); got != "content of "+p {
			t.Errorf("%s = %q", p, got)
		}
		row, err := store.GetFile(ctx, trashTestShareID, p)
		if err != nil || row.Deleted || row.Version["nodeA"] != 1 {
			t.Errorf("row for %s = %+v, %v; want live with nodeA bumped", p, row, err)
		}
	}
	if events := mustList(t, tr, share); len(events) != 0 {
		t.Errorf("trash after restoring everything = %+v, want empty", events)
	}
	if _, err := os.Stat(filepath.Join(share, ".syncat", "trash", ev.id)); !os.IsNotExist(err) {
		t.Errorf("emptied event folder left behind (stat err = %v)", err)
	}
}

func TestTrash_RestoreOneFileLeavesTheRest(t *testing.T) {
	ctx := context.Background()
	share := t.TempDir()
	tr := newTestTrash(trashTestTime)
	ev := tr.BeginPeerEvent(share, "laptop")
	for _, p := range []string{"a.txt", "b.txt"} {
		if err := ev.Put(ctx, p, writeShareFile(t, share, p, p), TrashDeleted); err != nil {
			t.Fatal(err)
		}
	}
	ev.Close()

	if _, err := tr.Restore(ctx, openTestTrashStore(t), "nodeA", trashTestShareID, share, ev.id, []string{"b.txt"}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(share, "a.txt")); !os.IsNotExist(err) {
		t.Error("a.txt was restored too")
	}
	if items := mustList(t, tr, share)[0].Items; len(items) != 1 || items[0].RelPath != "a.txt" {
		t.Errorf("remaining items = %+v, want a.txt", items)
	}
}

// Restoring over a file moves that file into a new event first, so the
// restore can itself be undone.
func TestTrash_RestoreOverAFileTrashesIt(t *testing.T) {
	ctx := context.Background()
	share := t.TempDir()
	tr := newTestTrash(trashTestTime)
	old := trashOne(t, tr, share, "todo.md", "old version")
	writeShareFile(t, share, "todo.md", "current version")

	if _, err := tr.Restore(ctx, openTestTrashStore(t), "nodeA", trashTestShareID, share, old.ID, nil); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if got := readFile(t, filepath.Join(share, "todo.md")); got != "old version" {
		t.Errorf("todo.md = %q, want the restored version", got)
	}
	events := mustList(t, tr, share)
	if len(events) != 1 || events[0].Source != TrashSourceRestore || events[0].Items[0].Kind != TrashOverwritten {
		t.Fatalf("trash = %+v, want one restore event holding the replaced file", events)
	}
	replaced := filepath.Join(share, ".syncat", "trash", events[0].ID, "todo.md")
	if got := readFile(t, replaced); got != "current version" {
		t.Errorf("replaced copy = %q", got)
	}
}

func TestTrash_RestoreRefusesDirectoryInTheWay(t *testing.T) {
	ctx := context.Background()
	share := t.TempDir()
	tr := newTestTrash(trashTestTime)
	ev := trashOne(t, tr, share, "thing", "file")
	if err := os.Mkdir(filepath.Join(share, "thing"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := tr.Restore(ctx, openTestTrashStore(t), "nodeA", trashTestShareID, share, ev.ID, nil); !errors.Is(err, ErrRestoreDestExists) {
		t.Errorf("Restore = %v, want ErrRestoreDestExists", err)
	}
}

func TestTrash_RestoreUnknownEventOrPath(t *testing.T) {
	ctx := context.Background()
	share := t.TempDir()
	tr := newTestTrash(trashTestTime)
	ev := trashOne(t, tr, share, "a.txt", "a")
	store := openTestTrashStore(t)
	for _, tc := range []struct {
		id    string
		paths []string
	}{{"nope", nil}, {"../..", nil}, {ev.ID, []string{"missing.txt"}}} {
		if _, err := tr.Restore(ctx, store, "nodeA", trashTestShareID, share, tc.id, tc.paths); !errors.Is(err, ErrTrashEntryNotFound) {
			t.Errorf("Restore(%q, %v) = %v, want ErrTrashEntryNotFound", tc.id, tc.paths, err)
		}
	}
}

// Files dropped into an event folder by hand, or whose log was lost, are
// still listed and restorable.
func TestTrash_ListFindsFilesTheLogDoesNotMention(t *testing.T) {
	share := t.TempDir()
	tr := newTestTrash(trashTestTime)
	ev := trashOne(t, tr, share, "a.txt", "a")
	writeShareFile(t, filepath.Join(share, ".syncat", "trash", ev.ID), "extra.txt", "extra")
	if err := os.Remove(filepath.Join(share, ".syncat", "trash-events", ev.ID+".jsonl")); err != nil {
		t.Fatal(err)
	}
	items := mustList(t, tr, share)[0].Items
	if len(items) != 2 || items[0].RelPath != "a.txt" || items[1].RelPath != "extra.txt" {
		t.Errorf("items = %+v, want a.txt and extra.txt", items)
	}
}

func TestTrash_VeryLongRelPath(t *testing.T) {
	ctx := context.Background()
	share := t.TempDir()
	comp := strings.Repeat("a", 200)
	relpath := strings.Join([]string{comp, comp, comp + ".txt"}, "/")
	tr := newTestTrash(trashTestTime)
	ev := trashOne(t, tr, share, relpath, "deep content")
	if ev.Items[0].RelPath != relpath {
		t.Fatalf("items = %+v", ev.Items)
	}
	if _, err := tr.Restore(ctx, openTestTrashStore(t), "nodeA", trashTestShareID, share, ev.ID, nil); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if got := readFile(t, filepath.Join(share, filepath.FromSlash(relpath))); got != "deep content" {
		t.Errorf("restored = %q", got)
	}
}

// --- expiry ------------------------------------------------------------

func trashAt(t *testing.T, share string, at time.Time, relpath string, size int) string {
	t.Helper()
	tr := newTestTrash(at)
	ev := tr.BeginPeerEvent(share, "laptop")
	if err := ev.Put(context.Background(), relpath, writeShareFile(t, share, relpath, strings.Repeat("x", size)), TrashDeleted); err != nil {
		t.Fatal(err)
	}
	ev.Close()
	return ev.id
}

func eventIDs(events []TrashEventInfo) []string {
	ids := make([]string, len(events))
	for i, e := range events {
		ids[i] = e.ID
	}
	return ids
}

func TestTrash_SweepExpiresOldEvents(t *testing.T) {
	share := t.TempDir()
	now := trashTestTime
	trashAt(t, share, now.Add(-40*24*time.Hour), "old.txt", 1)
	fresh := trashAt(t, share, now.Add(-time.Hour), "fresh.txt", 1)

	res, err := newTestTrash(now).Sweep(share, 30*24*time.Hour, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	if res.Purged != 1 {
		t.Errorf("purged %d, want 1", res.Purged)
	}
	if ids := eventIDs(mustList(t, newTestTrash(now), share)); len(ids) != 1 || ids[0] != fresh {
		t.Errorf("left = %v, want only %s", ids, fresh)
	}
	if _, err := os.Stat(filepath.Join(share, ".syncat", "trash-events")); err != nil {
		t.Fatal(err)
	}
}

// Over the size limit, oldest events go first and whole; the newest is
// never removed for size, even when it alone is over the limit.
func TestTrash_SweepEnforcesSizeOldestFirstKeepingNewest(t *testing.T) {
	share := t.TempDir()
	now := trashTestTime
	trashAt(t, share, now.Add(-3*time.Hour), "a.txt", 100)
	b := trashAt(t, share, now.Add(-2*time.Hour), "b.txt", 100)
	c := trashAt(t, share, now.Add(-1*time.Hour), "c.txt", 100)

	res, err := newTestTrash(now).Sweep(share, 30*24*time.Hour, 250)
	if err != nil {
		t.Fatal(err)
	}
	if res.Purged != 1 || res.Size != 200 {
		t.Errorf("sweep = %+v, want 1 purged leaving 200 bytes", res)
	}
	if ids := eventIDs(mustList(t, newTestTrash(now), share)); len(ids) != 2 || ids[0] != c || ids[1] != b {
		t.Errorf("left = %v, want [%s %s]", ids, c, b)
	}

	res, err = newTestTrash(now).Sweep(share, 30*24*time.Hour, 10)
	if err != nil {
		t.Fatal(err)
	}
	if ids := eventIDs(mustList(t, newTestTrash(now), share)); len(ids) != 1 || ids[0] != c {
		t.Errorf("left = %v, want only the newest %s", ids, c)
	}
	if res.Size != 100 {
		t.Errorf("size after = %d, want 100 (over the limit, but the newest stays)", res.Size)
	}
}

func TestTrash_SweepOfShareWithNoTrash(t *testing.T) {
	if res, err := newTestTrash(trashTestTime).Sweep(t.TempDir(), time.Hour, 1); err != nil || res.Purged != 0 {
		t.Errorf("Sweep = %+v, %v", res, err)
	}
}

// --- migration ---------------------------------------------------------

func TestTrash_MigrateLegacyTrashGroupsBySecond(t *testing.T) {
	ctx := context.Background()
	legacy := filepath.Join(t.TempDir(), "trash", trashTestShareID)
	share := t.TempDir()
	writeShareFile(t, legacy, "notes/a.txt.1700000000", "a")
	writeShareFile(t, legacy, "b.txt.1700000000-1", "b")
	writeShareFile(t, legacy, "c.txt.1700000500", "c")
	writeShareFile(t, legacy, "not-ours.txt", "left alone")

	tr := newTestTrash(trashTestTime)
	moved, err := tr.MigrateLegacyTrash(ctx, legacy, share)
	if err != nil {
		t.Fatalf("MigrateLegacyTrash: %v", err)
	}
	if moved != 3 {
		t.Errorf("moved %d, want 3", moved)
	}
	events := mustList(t, tr, share)
	if len(events) != 2 {
		t.Fatalf("events = %+v, want 2 (one per second)", events)
	}
	newer, older := events[0], events[1]
	if !older.At.Equal(time.Unix(1700000000, 0)) || len(older.Items) != 2 || older.Source != TrashSourceMigrated {
		t.Errorf("older event = %+v", older)
	}
	if len(newer.Items) != 1 || newer.Items[0].RelPath != "c.txt" || newer.Items[0].Kind != TrashMigrated {
		t.Errorf("newer event = %+v", newer)
	}
	if got := readFile(t, filepath.Join(share, ".syncat", "trash", older.ID, "notes", "a.txt")); got != "a" {
		t.Errorf("migrated a.txt = %q", got)
	}
	if _, err := os.Stat(filepath.Join(legacy, "not-ours.txt")); err != nil {
		t.Errorf("a file the old trash didn't name was touched: %v", err)
	}
	if _, err := os.Stat(filepath.Join(legacy, "notes")); !os.IsNotExist(err) {
		t.Errorf("emptied old directory left behind (stat err = %v)", err)
	}
}

func TestTrash_MigrateMissingLegacyDir(t *testing.T) {
	moved, err := newTestTrash(trashTestTime).MigrateLegacyTrash(context.Background(), filepath.Join(t.TempDir(), "nope"), t.TempDir())
	if err != nil || moved != 0 {
		t.Errorf("= %d, %v; want 0, nil", moved, err)
	}
}

// TestSweepTempFiles covers the cleanup for staging files a killed daemon
// could not remove itself. Apply only renames a download into place once its
// content verifies, so an interrupted transfer leaves a full-size file behind,
// hidden, inside the user's own synced directory, one per crash.
func TestSweepTempFiles(t *testing.T) {
	root := t.TempDir()
	nested := filepath.Join(root, "sub", "deeper")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	// Two orphans, one of them in a subdirectory, staging files are written
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

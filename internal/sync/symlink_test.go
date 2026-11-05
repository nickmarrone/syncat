package sync

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nickmarrone/syncat/internal/index"
	"github.com/nickmarrone/syncat/internal/protocol"
)

// Symlinks are never synced (SPEC.md §5): the scanner skips them locally,
// and everything here pins the receiving side to the same rule, plus the
// share boundary that a symlink on disk must not let a write cross.

func symlinkOrSkip(t *testing.T, target, link string) {
	t.Helper()
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlinks not supported on this platform: %v", err)
	}
}

func TestReconcile_RemoteSymlinkEntryRejected(t *testing.T) {
	r := fi("link", 100, "target", vv("peer", 1), false)
	r.Type = protocol.FileTypeSymlink
	for _, dir := range []Direction{mirrorDir(), receiveOnlyDir()} {
		a := findAction(t, Reconcile(nil, []protocol.FileInfo{r}, nodeLocal, dir, fixedClock, nil), "link")
		if a.Kind != ActionNone {
			t.Errorf("dir %+v: Kind = %v, want ActionNone for a remote symlink", dir, a.Kind)
		}
	}

	// A symlink entry must not delete a local file of the same name
	// either, whether it arrives live or as a tombstone.
	local := fi("link", 50, "data", vv("peer", 1), false)
	for _, deleted := range []bool{false, true} {
		r := fi("link", 100, "target", vv("peer", 2), deleted)
		r.Type = protocol.FileTypeSymlink
		a := findAction(t, Reconcile([]protocol.FileInfo{local}, []protocol.FileInfo{r}, nodeLocal, mirrorDir(), fixedClock, nil), "link")
		if a.Kind != ActionNone {
			t.Errorf("deleted=%v: Kind = %v, want ActionNone", deleted, a.Kind)
		}
	}
}

func TestJoinSharePathRejectsSymlinkedParent(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	symlinkOrSkip(t, outside, filepath.Join(root, "out"))
	if err := os.MkdirAll(filepath.Join(root, "real", "deeper"), 0o700); err != nil {
		t.Fatal(err)
	}
	symlinkOrSkip(t, filepath.Join(root, "real"), filepath.Join(root, "real", "deeper", "loop"))

	for _, rel := range []string{"out/x.txt", "out/a/b.txt", "real/deeper/loop/x.txt"} {
		if _, err := JoinSharePath(root, rel); !errors.Is(err, ErrSymlinkInPath) {
			t.Errorf("JoinSharePath(%q) err = %v, want ErrSymlinkInPath", rel, err)
		}
	}
	// The leaf itself may be a symlink: callers Lstat, rename over, or
	// remove it, none of which follow it.
	for _, rel := range []string{"out", "real/deeper/loop", "real/deeper/new.txt", "missing/dir/x.txt"} {
		if _, err := JoinSharePath(root, rel); err != nil {
			t.Errorf("JoinSharePath(%q) = %v, want ok", rel, err)
		}
	}
}

// A peer advertising, and able to serve, a symlink entry gets nothing
// created and nothing recorded. syncat's own scanner never indexes one,
// so the peer here stands in for a hostile or older node.
func TestIntegration_PeerSymlinkEntryRejected(t *testing.T) {
	a := newTestNode(t, "aaaaaaaaaaaaaaaa")
	b := newTestNode(t, "bbbbbbbbbbbbbbbb")
	target := t.TempDir()
	symlinkOrSkip(t, target, filepath.Join(a.root, "evil"))
	putIndexRow(t, a.store, a.id, "evil", protocol.FileTypeSymlink, int64(len(target)), time.Now().UnixNano(), sha256Of(target))
	writeFile(t, a.root, "tracer.txt", "tracer")
	indexFile(t, a.store, a.id, "tracer.txt", a.root)

	sa, sb := connectSessions(t, a, b, Direction{}, Direction{})
	mustSync(t, sa, testShareID)
	mustSync(t, sb, testShareID)
	waitForFile(t, 5*time.Second, b.store, testShareID, b.root, "tracer.txt", "tracer")

	if _, err := os.Lstat(filepath.Join(b.root, "evil")); !os.IsNotExist(err) {
		t.Fatalf("peer's symlink entry was installed (lstat err = %v)", err)
	}
	if _, err := b.store.GetFile(context.Background(), testShareID, "evil"); !errors.Is(err, index.ErrNotFound) {
		t.Fatalf("peer's symlink entry was recorded in the index: err=%v", err)
	}
}

// A symlinked directory the user made inside their own share must not let
// a peer's file land outside the share.
func TestIntegration_PullDoesNotWriteThroughSymlinkedDir(t *testing.T) {
	a := newTestNode(t, "aaaaaaaaaaaaaaaa")
	b := newTestNode(t, "bbbbbbbbbbbbbbbb")
	outside := t.TempDir()
	symlinkOrSkip(t, outside, filepath.Join(b.root, "out"))

	writeFile(t, a.root, "out/x.txt", "from a")
	indexFile(t, a.store, a.id, "out/x.txt", a.root)
	writeFile(t, a.root, "tracer.txt", "tracer")
	indexFile(t, a.store, a.id, "tracer.txt", a.root)

	sa, sb := connectSessions(t, a, b, Direction{}, Direction{})
	mustSync(t, sa, testShareID)
	mustSync(t, sb, testShareID)
	waitForFile(t, 5*time.Second, b.store, testShareID, b.root, "tracer.txt", "tracer")

	if _, err := os.Lstat(filepath.Join(outside, "x.txt")); !os.IsNotExist(err) {
		t.Fatalf("a peer's file was written through the symlink, outside the share (lstat err = %v)", err)
	}
	if fi, err := os.Lstat(filepath.Join(b.root, "out")); err != nil || fi.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("the user's symlink was disturbed: fi=%v err=%v", fi, err)
	}
}

// A symlink the trash kept (it sat where a peer's file landed) comes back
// on disk when restored, but is not indexed, so it is never offered to a
// peer.
func TestTrash_RestoredSymlinkIsNotIndexed(t *testing.T) {
	ctx := context.Background()
	share := t.TempDir()
	symlinkOrSkip(t, "somewhere", filepath.Join(share, "link"))
	tr := newTestTrash(trashTestTime)
	ev := tr.BeginPeerEvent(share, "laptop")
	if err := ev.Put(ctx, "link", filepath.Join(share, "link"), TrashOverwritten); err != nil {
		t.Fatal(err)
	}
	ev.Close()

	store := openTestTrashStore(t)
	rows, err := tr.Restore(ctx, store, "nodeA", trashTestShareID, share, ev.id, nil)
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if got, err := os.Readlink(filepath.Join(share, "link")); err != nil || got != "somewhere" {
		t.Fatalf("restored link = %q, %v; want the symlink back", got, err)
	}
	if len(rows) != 0 {
		t.Errorf("Restore returned rows %+v, want none for a symlink", rows)
	}
	if _, err := store.GetFile(ctx, trashTestShareID, "link"); !errors.Is(err, index.ErrNotFound) {
		t.Errorf("restored symlink was indexed: err=%v", err)
	}
}

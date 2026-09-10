package sync

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nickmarrone/syncat/internal/config"
	"github.com/nickmarrone/syncat/internal/index"
	"github.com/nickmarrone/syncat/internal/protocol"
	"github.com/nickmarrone/syncat/internal/transport"
)

// --- test harness -----------------------------------------------------
//
// Two in-process nodes over transport.PipeTransport (SPEC.md §10's
// loopback stand-in for the real tailcat carrier), each with its own
// temp dir, index, and identity, wired into a pair of Sessions that talk
// to each other exactly the way two peers would once internal/protocol's
// handshake and the share negotiation already happened. Rather than
// reaching for the real fsnotify/Scanner pipeline (already covered by its
// own tests), local "edits" here go straight through the index Store:
// write the bytes, then record a hashed, version-bumped FileRow for them.
// That keeps these tests focused on what 5b actually owns — reconcile
// execution, transfer, and atomic apply — without re-testing scanning.

const testShareID = "share1"

type testNode struct {
	id    string
	root  string
	store *index.Store
}

func newTestNode(t *testing.T, id string) *testNode {
	t.Helper()
	if len(id) != 16 {
		sum := sha256.Sum256([]byte(id))
		id = fmt.Sprintf("%x", sum[:8])
	}
	dir := t.TempDir()
	root := filepath.Join(dir, "share")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatalf("mkdir share root: %v", err)
	}
	store, err := index.Open(context.Background(), filepath.Join(dir, "index.db"))
	if err != nil {
		t.Fatalf("open index: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return &testNode{id: id, root: root, store: store}
}

// writeFile writes content at root/relpath, creating parent directories as
// needed.
func writeFile(t *testing.T, root, relpath, content string) string {
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

// indexFile stats+hashes root/relpath (already written via writeFile) and
// upserts a FileRow for it, bumping nodeID's counter on top of whatever
// version the store already has for that relpath (0 if none) — standing
// in for what internal/index's scanner+ApplyScanResult would have
// recorded after a real local edit.
func indexFile(t *testing.T, store *index.Store, nodeID, relpath string, root string) index.FileRow {
	t.Helper()
	full := filepath.Join(root, filepath.FromSlash(relpath))
	data, err := os.ReadFile(full)
	if err != nil {
		t.Fatalf("read %s: %v", full, err)
	}
	fi, err := os.Stat(full)
	if err != nil {
		t.Fatalf("stat %s: %v", full, err)
	}
	sum := sha256.Sum256(data)
	return putIndexRow(t, store, nodeID, relpath, protocol.FileTypeFile, int64(len(data)), fi.ModTime().UnixNano(), sum[:])
}

// indexFileWithMTime is indexFile but with an explicit mtime, used only to
// make conflict tie-breaks (SPEC.md §5: larger mtime_ns wins) deterministic
// in tests instead of racing on wall-clock write order.
func indexFileWithMTime(t *testing.T, store *index.Store, nodeID, relpath, root string, mtimeNS int64) index.FileRow {
	t.Helper()
	full := filepath.Join(root, filepath.FromSlash(relpath))
	data, err := os.ReadFile(full)
	if err != nil {
		t.Fatalf("read %s: %v", full, err)
	}
	mtime := time.Unix(0, mtimeNS)
	if err := os.Chtimes(full, mtime, mtime); err != nil {
		t.Fatalf("set mtime %s: %v", full, err)
	}
	sum := sha256.Sum256(data)
	return putIndexRow(t, store, nodeID, relpath, protocol.FileTypeFile, int64(len(data)), mtimeNS, sum[:])
}

func putIndexRow(t *testing.T, store *index.Store, nodeID, relpath string, typ protocol.FileType, size, mtimeNS int64, sum []byte) index.FileRow {
	t.Helper()
	ctx := context.Background()
	var version protocol.VersionVector
	if existing, err := store.GetFile(ctx, testShareID, relpath); err == nil {
		version = existing.Version
	} else if !errors.Is(err, index.ErrNotFound) {
		t.Fatalf("get file %s: %v", relpath, err)
	}
	row := index.FileRow{
		ShareID: testShareID, RelPath: relpath, Type: typ, Size: size,
		MTimeNS: mtimeNS, Mode: 0o644, SHA256: sum,
		Version: Bump(version, nodeID), Deleted: false, UpdatedAt: time.Now(),
	}
	if err := store.PutFile(ctx, row); err != nil {
		t.Fatalf("put file %s: %v", relpath, err)
	}
	return row
}

// indexDelete records a tombstone for relpath, bumping nodeID's counter on
// top of the row's current version (as ApplyScanResult's Deleted bucket
// would after a real local delete).
func indexDelete(t *testing.T, store *index.Store, nodeID, relpath string) {
	t.Helper()
	ctx := context.Background()
	existing, err := store.GetFile(ctx, testShareID, relpath)
	if err != nil {
		t.Fatalf("get file %s: %v", relpath, err)
	}
	existing.Deleted = true
	existing.Version = Bump(existing.Version, nodeID)
	existing.UpdatedAt = time.Now()
	if err := store.PutFile(ctx, existing); err != nil {
		t.Fatalf("put tombstone %s: %v", relpath, err)
	}
}

var pipeAddrCounter int64

// connectSessions wires a and b together over a fresh PipeTransport,
// registering testShareID with the given per-side Directions, and starts
// both Sessions' read loops. Both are closed via t.Cleanup.
func connectSessions(t *testing.T, a, b *testNode, dirA, dirB Direction) (sa, sb *Session) {
	t.Helper()
	return connectSessionsWithIgnore(t, a, b, dirA, dirB, nil, nil)
}

// connectSessionsWithIgnore is connectSessions plus a per-side ignore
// predicate, standing in for each node's own .syncatignore (which is never
// synced, so the two sides genuinely can differ).
func connectSessionsWithIgnore(t *testing.T, a, b *testNode, dirA, dirB Direction, ignoreA, ignoreB func(string, bool) bool) (sa, sb *Session) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	addr := fmt.Sprintf("pipe-%s-%d", t.Name(), atomic.AddInt64(&pipeAddrCounter, 1))
	tr := transport.NewPipeTransport(addr)
	t.Cleanup(func() { _ = tr.Close() })

	logger := log.New(io.Discard, "", 0)

	var sess1 *Session
	ready := make(chan struct{})
	if err := tr.Start(ctx, func(conn net.Conn) {
		sess1 = NewSession(conn, a.store, a.id, b.id, nil, logger)
		sess1.AddShare(ShareConfig{ShareID: testShareID, Root: a.root, Direction: dirA, Ignore: ignoreA})
		sess1.Start(ctx)
		close(ready)
	}); err != nil {
		t.Fatalf("transport start: %v", err)
	}

	conn, err := tr.Dial(ctx, addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	sess2 := NewSession(conn, b.store, b.id, a.id, nil, logger)
	sess2.AddShare(ShareConfig{ShareID: testShareID, Root: b.root, Direction: dirB, Ignore: ignoreB})
	sess2.Start(ctx)

	<-ready
	t.Cleanup(func() {
		_ = sess1.Close()
		_ = sess2.Close()
	})
	return sess1, sess2
}

func mustSync(t *testing.T, s *Session, shareID string) {
	t.Helper()
	if err := s.SyncShare(context.Background(), shareID); err != nil {
		t.Fatalf("sync share: %v", err)
	}
}

// snapshotTree walks root and returns relpath -> sha256 for every regular
// file (symlinks/dirs excluded — none of these tests need them).
func snapshotTree(t *testing.T, root string) map[string][32]byte {
	t.Helper()
	out := map[string][32]byte{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasPrefix(d.Name(), ".syncat.tmp.") {
			// In-flight downloads: never count as converged content.
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		out[filepath.ToSlash(rel)] = sha256.Sum256(data)
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	return out
}

func treeKeys(m map[string][32]byte) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func treesEqual(a, b map[string][32]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for k, va := range a {
		vb, ok := b[k]
		if !ok || va != vb {
			return false
		}
	}
	return true
}

// integrationWaitTimeout bounds every waitFor/waitForFile call in this
// file. It was raised from an original 5s (10s for the one test that
// deliberately serializes 10 files through a 4-slot pull semaphore) after
// two rounds of diagnosis on a flake that only ever showed up under
// `go test ./...` for the whole repo — never in isolation, never with
// `-race` on this package alone — because the whole-repo run puts every
// package's tests (including internal/transport's real network I/O) on CPU
// at once:
//
//  1. A genuine production bug (fixed in reconcile.go/apply.go):
//     resolving a delete-vs-modify or concurrent-conflict case whose winner
//     needed fetching from the peer sent the FileRequest carrying the
//     *merged, locally-bumped* version instead of the version the peer
//     actually advertised, so the peer's freshness check (session.go's
//     handleFileRequest) could never match. This wasted one full request/
//     error round trip on *every* such resolution, unconditionally — not a
//     race, reproduced deterministically pre-fix. See Action.SourceVersion.
//  2. A genuine test-harness synchronization gap (fixed via waitForFile):
//     several tests wait only for a file's *content* to land, then
//     immediately read or Bump that same node's *index row* for it
//     (indexFile/indexDelete/a direct assertion) — but the row is written
//     by a separate store.PutFile call strictly after the content lands
//     (session.go's handleIndexUpdate), so under scheduling pressure the
//     row can still lag behind what's already on disk. Reproduced under
//     synthetic CPU load as both silent version-vector corruption
//     (indexFile bumping on a stale/absent row) and hard "not found"
//     failures — not merely as slow convergence.
//
// With both fixed, dozens of `go test -race -count=1 ./...` repeats under
// synthetic whole-machine CPU load (many more concurrent busy processes
// than a real `go test ./...` run creates) still occasionally needed more
// than 5s of *genuine* wall-clock convergence time — no crashes, no wrong
// state, just scheduling delay — which is what this constant now budgets
// for. It's a deliberately generous, still-bounded ceiling (a real hang
// fails a CI run in 20s, not silently); waitFor's polling loop itself
// already reacts within 10ms of the real condition, so raising this number
// costs nothing on a healthy run and only buys headroom on a loaded one.
const integrationWaitTimeout = 20 * time.Second

// waitFor polls cond until it returns true or timeout elapses, failing the
// test on timeout.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("condition not met within %s", timeout)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// waitForFile polls until relpath's on-disk content under root equals want
// AND the corresponding row in store already reflects that same content
// (present, not a tombstone, SHA256 matching want).
//
// A plain "is it on disk yet" check (what the bare waitFor calls elsewhere
// in this file use) is enough on its own only when nothing afterward reads
// that node's *store* — e.g. a final assertion at the end of a test. But
// several tests immediately follow such a wait with indexFile/indexDelete
// on the very same store, to simulate the node's own next local edit; those
// helpers (see putIndexRow) read the store's *current* row for the path
// and Bump on top of it. The corresponding index row is written by a
// separate call — session.go's handleIndexUpdate persists it via
// store.PutFile only after applyAction (which does the actual rename) has
// already returned — so under load the row can still lag behind what's
// already readable on disk. A test that doesn't also wait for the row can
// have indexFile silently Bump on top of a stale or absent row (mistaken
// for "no prior version" — see putIndexRow's ErrNotFound handling) instead
// of the version it just received, corrupting the version vector for the
// rest of that test with no visible error at the call site itself — it
// only surfaces later as a mysteriously-unconverged waitFor, or as a
// "not found" a few lines on. This showed up exactly that way under a
// loaded machine (many packages' tests running in parallel): the two
// writes (file content, index row) that are simultaneous on a quiet box
// pull apart under scheduling pressure often enough to matter.
func waitForFile(t *testing.T, timeout time.Duration, store *index.Store, shareID, root, relpath, want string) {
	t.Helper()
	wantSum := sha256.Sum256([]byte(want))
	waitFor(t, timeout, func() bool {
		data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(relpath)))
		if err != nil || string(data) != want {
			return false
		}
		row, err := store.GetFile(context.Background(), shareID, relpath)
		if err != nil || row.Deleted {
			return false
		}
		return bytes.Equal(row.SHA256, wantSum[:])
	})
}

// --- tests --------------------------------------------------------------

// 1. Bidirectional sync of a nested tree converges: both sides end with
// identical trees (compared by walked file list + hash).
func TestIntegration_BidirectionalNestedTreeConverges(t *testing.T) {
	a := newTestNode(t, "aaaaaaaaaaaaaaaa")
	b := newTestNode(t, "bbbbbbbbbbbbbbbb")

	files := map[string]string{
		"top.txt":             "top level",
		"dir1/mid.txt":        "mid level",
		"dir1/dir2/deep.txt":  "deep level",
		"dir1/dir2/deep2.txt": "another deep file",
		"other/sibling.txt":   "sibling",
	}
	for rel, content := range files {
		writeFile(t, a.root, rel, content)
		indexFile(t, a.store, a.id, rel, a.root)
	}

	sa, sb := connectSessions(t, a, b, Direction{}, Direction{})
	mustSync(t, sa, testShareID)
	mustSync(t, sb, testShareID)

	waitFor(t, integrationWaitTimeout, func() bool {
		ta, tb := snapshotTree(t, a.root), snapshotTree(t, b.root)
		return len(ta) == len(files) && treesEqual(ta, tb)
	})

	ta, tb := snapshotTree(t, a.root), snapshotTree(t, b.root)
	if !treesEqual(ta, tb) {
		t.Fatalf("trees differ: A=%v B=%v", treeKeys(ta), treeKeys(tb))
	}
	if len(ta) != len(files) {
		t.Fatalf("expected %d files, got %d: %v", len(files), len(ta), treeKeys(ta))
	}
}

// 2. Edit on A propagates to B; then edit on B propagates back.
func TestIntegration_EditPropagatesBothWays(t *testing.T) {
	a := newTestNode(t, "aaaaaaaaaaaaaaaa")
	b := newTestNode(t, "bbbbbbbbbbbbbbbb")

	writeFile(t, a.root, "notes.txt", "version 1")
	indexFile(t, a.store, a.id, "notes.txt", a.root)

	sa, sb := connectSessions(t, a, b, Direction{}, Direction{})
	mustSync(t, sa, testShareID)
	mustSync(t, sb, testShareID)

	waitFor(t, integrationWaitTimeout, func() bool {
		data, err := os.ReadFile(filepath.Join(b.root, "notes.txt"))
		return err == nil && string(data) == "version 1"
	})

	// Edit on A.
	writeFile(t, a.root, "notes.txt", "version 2 from A")
	indexFile(t, a.store, a.id, "notes.txt", a.root)
	mustSync(t, sa, testShareID)

	// Also wait for B's index row, not just the file content: B's next
	// edit (below) reads and bumps B's own current row for this path, and
	// that row is what B just received from A via the pull above — see
	// waitForFile's doc comment for why the plain content-only wait isn't
	// enough here.
	waitForFile(t, integrationWaitTimeout, b.store, testShareID, b.root, "notes.txt", "version 2 from A")

	// Edit on B.
	writeFile(t, b.root, "notes.txt", "version 3 from B")
	indexFile(t, b.store, b.id, "notes.txt", b.root)
	mustSync(t, sb, testShareID)

	waitFor(t, integrationWaitTimeout, func() bool {
		data, err := os.ReadFile(filepath.Join(a.root, "notes.txt"))
		return err == nil && string(data) == "version 3 from B"
	})
}

// 3. Concurrent edit on both sides while disconnected => conflict copy on
// both sides, both agree on the winner, both end with the same file set.
func TestIntegration_ConcurrentEditConflict(t *testing.T) {
	a := newTestNode(t, "aaaaaaaaaaaaaaaa")
	b := newTestNode(t, "bbbbbbbbbbbbbbbb")

	// Independent, concurrent edits while "disconnected" (no session
	// exists yet): each side creates the same relpath from scratch with
	// different content and an explicit, deterministic mtime so the
	// SPEC.md §5 tie-break (larger mtime_ns wins) has a known answer: B's
	// content should win.
	writeFile(t, a.root, "shared.txt", "content from A")
	indexFileWithMTime(t, a.store, a.id, "shared.txt", a.root, 1000)
	writeFile(t, b.root, "shared.txt", "content from B")
	indexFileWithMTime(t, b.store, b.id, "shared.txt", b.root, 2000)

	sa, sb := connectSessions(t, a, b, Direction{}, Direction{})
	mustSync(t, sa, testShareID)
	mustSync(t, sb, testShareID)

	conflictRe := regexp.MustCompile(`^shared\.sync-conflict-\d{8}-\d{6}-[0-9a-f]+\.txt$`)

	waitFor(t, integrationWaitTimeout, func() bool {
		ta, tb := snapshotTree(t, a.root), snapshotTree(t, b.root)
		if !treesEqual(ta, tb) {
			return false
		}
		winner, ok := ta["shared.txt"]
		if !ok || winner != sha256.Sum256([]byte("content from B")) {
			return false
		}
		for rel := range ta {
			if conflictRe.MatchString(rel) {
				return true
			}
		}
		return false
	})

	ta, tb := snapshotTree(t, a.root), snapshotTree(t, b.root)
	if !treesEqual(ta, tb) {
		t.Fatalf("trees differ after convergence: A=%v B=%v", treeKeys(ta), treeKeys(tb))
	}
	wantWinner := sha256.Sum256([]byte("content from B"))
	if ta["shared.txt"] != wantWinner {
		t.Fatalf("winner mismatch: shared.txt does not contain B's content on both sides")
	}
	wantLoser := sha256.Sum256([]byte("content from A"))
	foundLoser := false
	for rel, sum := range ta {
		if conflictRe.MatchString(rel) {
			foundLoser = true
			if sum != wantLoser {
				t.Errorf("conflict copy %s does not contain A's (losing) content", rel)
			}
		}
	}
	if !foundLoser {
		t.Fatalf("no conflict copy found in converged tree: %v", treeKeys(ta))
	}
}

// 4. Delete propagates.
func TestIntegration_DeletePropagates(t *testing.T) {
	a := newTestNode(t, "aaaaaaaaaaaaaaaa")
	b := newTestNode(t, "bbbbbbbbbbbbbbbb")

	writeFile(t, a.root, "gone.txt", "will be deleted")
	indexFile(t, a.store, a.id, "gone.txt", a.root)

	sa, sb := connectSessions(t, a, b, Direction{}, Direction{})
	mustSync(t, sa, testShareID)
	mustSync(t, sb, testShareID)

	waitFor(t, integrationWaitTimeout, func() bool {
		_, err := os.Stat(filepath.Join(b.root, "gone.txt"))
		return err == nil
	})

	if err := os.Remove(filepath.Join(a.root, "gone.txt")); err != nil {
		t.Fatalf("remove: %v", err)
	}
	indexDelete(t, a.store, a.id, "gone.txt")
	mustSync(t, sa, testShareID)

	waitFor(t, integrationWaitTimeout, func() bool {
		_, err := os.Stat(filepath.Join(b.root, "gone.txt"))
		return os.IsNotExist(err)
	})
}

// 5. Delete-vs-modify => file resurrected, modify wins.
func TestIntegration_DeleteVsModifyResurrects(t *testing.T) {
	a := newTestNode(t, "aaaaaaaaaaaaaaaa")
	b := newTestNode(t, "bbbbbbbbbbbbbbbb")

	writeFile(t, a.root, "fought-over.txt", "original")
	indexFile(t, a.store, a.id, "fought-over.txt", a.root)

	sa, sb := connectSessions(t, a, b, Direction{}, Direction{})
	mustSync(t, sa, testShareID)
	mustSync(t, sb, testShareID)

	// See waitForFile's doc comment: B's upcoming indexFile call below reads
	// and bumps B's own row for this path, which is what B just received
	// from A, so the wait must cover the index row too, not just the file.
	waitForFile(t, integrationWaitTimeout, b.store, testShareID, b.root, "fought-over.txt", "original")

	// Disconnect (simulated: just stop syncing for a moment) and diverge:
	// A deletes, B modifies, before either learns of the other's change.
	if err := os.Remove(filepath.Join(a.root, "fought-over.txt")); err != nil {
		t.Fatalf("remove: %v", err)
	}
	indexDelete(t, a.store, a.id, "fought-over.txt")

	writeFile(t, b.root, "fought-over.txt", "modified by B")
	indexFile(t, b.store, b.id, "fought-over.txt", b.root)

	mustSync(t, sa, testShareID)
	mustSync(t, sb, testShareID)

	waitFor(t, integrationWaitTimeout, func() bool {
		da, errA := os.ReadFile(filepath.Join(a.root, "fought-over.txt"))
		db, errB := os.ReadFile(filepath.Join(b.root, "fought-over.txt"))
		return errA == nil && errB == nil && string(da) == "modified by B" && string(db) == "modified by B"
	})
}

// 6. One-way push: a read-only share ignores the subscriber's changes; the
// subscriber's local edit does not reach the offerer.
func TestIntegration_ReadOnlyShareIsOneWay(t *testing.T) {
	offerer := newTestNode(t, "aaaaaaaaaaaaaaaa")
	subscriber := newTestNode(t, "bbbbbbbbbbbbbbbb")

	writeFile(t, offerer.root, "readonly.txt", "from offerer")
	indexFile(t, offerer.store, offerer.id, "readonly.txt", offerer.root)

	dirOfferer := DirectionFor(config.PermissionReadOnly, "")
	dirSubscriber := DirectionFor("", config.ModeMirror)

	sOfferer, sSubscriber := connectSessions(t, offerer, subscriber, dirOfferer, dirSubscriber)
	mustSync(t, sOfferer, testShareID)
	mustSync(t, sSubscriber, testShareID)

	// See waitForFile's doc comment: the subscriber's edit below reads and
	// bumps the subscriber's own row for this path, which is what it just
	// received from the offerer.
	waitForFile(t, integrationWaitTimeout, subscriber.store, testShareID, subscriber.root, "readonly.txt", "from offerer")

	// Subscriber edits its local copy.
	writeFile(t, subscriber.root, "readonly.txt", "edited by subscriber")
	indexFile(t, subscriber.store, subscriber.id, "readonly.txt", subscriber.root)
	mustSync(t, sSubscriber, testShareID)

	// Give the (blocked) update a moment to have been ignored, then assert
	// the offerer's file never changed.
	time.Sleep(200 * time.Millisecond)
	data, err := os.ReadFile(filepath.Join(offerer.root, "readonly.txt"))
	if err != nil {
		t.Fatalf("read offerer file: %v", err)
	}
	if string(data) != "from offerer" {
		t.Fatalf("offerer's file was overwritten by a read-only subscriber's edit: got %q", string(data))
	}
}

// 7. Hostile relpath ("../../etc/passwd") from a peer is rejected, and
// nothing is written outside the share root — asserted on the filesystem.
func TestIntegration_HostileRelPathRejected(t *testing.T) {
	a := newTestNode(t, "aaaaaaaaaaaaaaaa")
	b := newTestNode(t, "bbbbbbbbbbbbbbbb")

	sa, sb := connectSessions(t, a, b, Direction{}, Direction{})
	mustSync(t, sa, testShareID)
	mustSync(t, sb, testShareID)

	// Directly inject a malicious IndexUpdate as if it came from a hostile
	// peer, bypassing the local-write helpers entirely.
	sum := sha256.Sum256([]byte("pwned"))
	malicious := protocol.IndexUpdate{
		ShareID: testShareID,
		Full:    false,
		Files: []protocol.FileInfo{
			{
				RelPath: "../../etc/passwd",
				Type:    protocol.FileTypeFile,
				Size:    5,
				MTimeNS: time.Now().UnixNano(),
				Mode:    0o644,
				SHA256:  sum[:],
				Version: protocol.VersionVector{"evil": 1},
			},
		},
	}
	if err := sa.writer.WriteMessage(protocol.MsgIndexUpdate, malicious); err != nil {
		t.Fatalf("send malicious index update: %v", err)
	}

	// Give it time to (not) be applied.
	time.Sleep(300 * time.Millisecond)

	// Nothing should have appeared inside b's share root under that name,
	// and nothing should exist outside the share root either (the whole
	// point of ValidateRelPath/JoinSharePath).
	outside := filepath.Join(filepath.Dir(filepath.Dir(b.root)), "etc", "passwd")
	if _, err := os.Stat(outside); err == nil {
		t.Fatalf("hostile relpath escaped the share root: %s exists", outside)
	}
	tb := snapshotTree(t, b.root)
	if len(tb) != 0 {
		t.Fatalf("expected b's share root to remain empty, got: %v", treeKeys(tb))
	}
	// And the malicious relpath must never have made it into the index
	// either (5a's Reconcile drops it before producing any Action).
	if _, err := b.store.GetFile(context.Background(), testShareID, "../../etc/passwd"); !errors.Is(err, index.ErrNotFound) {
		t.Fatalf("hostile relpath was recorded in the index: err=%v", err)
	}
}

// 8. sha256 mismatch on a transferred file leaves the destination
// untouched, with no stray .syncat.tmp.* file left behind.
func TestIntegration_SHA256MismatchLeavesDestinationUntouched(t *testing.T) {
	a := newTestNode(t, "aaaaaaaaaaaaaaaa")
	b := newTestNode(t, "bbbbbbbbbbbbbbbb")

	writeFile(t, a.root, "corrupt.txt", "real content")
	row := indexFile(t, a.store, a.id, "corrupt.txt", a.root)

	// Make the index lie about the hash: A will serve the real bytes, but
	// advertise (and B will expect) a different sha256, simulating a
	// corrupted/lying advertisement without touching the transport layer.
	lyingRow := row
	lyingRow.SHA256 = sha256Of("not the real content")
	if err := a.store.PutFile(context.Background(), lyingRow); err != nil {
		t.Fatalf("put lying row: %v", err)
	}

	sa, sb := connectSessions(t, a, b, Direction{}, Direction{})
	mustSync(t, sa, testShareID)
	mustSync(t, sb, testShareID)

	// Give the doomed pull time to run and fail.
	time.Sleep(300 * time.Millisecond)

	if _, err := os.Stat(filepath.Join(b.root, "corrupt.txt")); !os.IsNotExist(err) {
		t.Fatalf("destination file should not exist after a sha256 mismatch, stat err=%v", err)
	}
	matches, err := filepath.Glob(filepath.Join(b.root, ".syncat.tmp.*"))
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	if len(matches) != 0 {
		t.Fatalf("stray temp file(s) left behind: %v", matches)
	}
	// The index must not have been updated to claim we have the file.
	if _, err := b.store.GetFile(context.Background(), testShareID, "corrupt.txt"); !errors.Is(err, index.ErrNotFound) {
		t.Fatalf("index should have no row for corrupt.txt, got err=%v", err)
	}
}

func sha256Of(s string) []byte {
	sum := sha256.Sum256([]byte(s))
	return sum[:]
}

// 9. Several files transferring at once exercise the 4-concurrent-pull
// limit and interleaving.
func TestIntegration_ConcurrentPullLimit(t *testing.T) {
	a := newTestNode(t, "aaaaaaaaaaaaaaaa")
	b := newTestNode(t, "bbbbbbbbbbbbbbbb")

	const numFiles = 10
	for i := 0; i < numFiles; i++ {
		rel := fmt.Sprintf("file%02d.txt", i)
		writeFile(t, a.root, rel, fmt.Sprintf("content of file %d", i))
		indexFile(t, a.store, a.id, rel, a.root)
	}

	sa, sb := connectSessions(t, a, b, Direction{}, Direction{})
	// Slow down serving just enough that, with 10 files racing for a
	// 4-slot semaphore, concurrency is actually observable rather than
	// completing serially before the next pull even starts.
	sa.testServeDelay = 20 * time.Millisecond

	mustSync(t, sa, testShareID)
	mustSync(t, sb, testShareID)

	waitFor(t, integrationWaitTimeout, func() bool {
		tb := snapshotTree(t, b.root)
		return len(tb) == numFiles
	})

	ta, tb := snapshotTree(t, a.root), snapshotTree(t, b.root)
	if !treesEqual(ta, tb) {
		t.Fatalf("trees differ: A=%v B=%v", treeKeys(ta), treeKeys(tb))
	}

	peak := atomic.LoadInt32(&sb.pullPeak)
	if peak == 0 {
		t.Fatalf("no concurrent pulls were observed at all")
	}
	if peak > maxConcurrentPulls {
		t.Fatalf("observed peak concurrency %d exceeds the %d-pull limit", peak, maxConcurrentPulls)
	}
	if peak < 2 {
		t.Errorf("expected to observe meaningful interleaving (peak >= 2), got peak=%d", peak)
	}
	t.Logf("peak concurrent pulls observed: %d (limit %d)", peak, maxConcurrentPulls)
}

// 10. Receive-only revert (SPEC.md §1, §5, §7): a subscriber's local edit
// to a receive-only share is trashed and overwritten the next time the
// offerer's own copy changes, and the revert is recorded as a warning.
func TestIntegration_ReceiveOnlyRevertsLocalEditViaTrash(t *testing.T) {
	offerer := newTestNode(t, "aaaaaaaaaaaaaaaa")
	subscriber := newTestNode(t, "bbbbbbbbbbbbbbbb")

	writeFile(t, offerer.root, "notes.txt", "v1 from offerer")
	indexFile(t, offerer.store, offerer.id, "notes.txt", offerer.root)

	dirOfferer := DirectionFor(config.PermissionReadWrite, "")
	dirSubscriber := DirectionFor("", config.ModeReceiveOnly)

	sOfferer, sSubscriber := connectSessions(t, offerer, subscriber, dirOfferer, dirSubscriber)
	tr := NewTrash(filepath.Join(t.TempDir(), "trash"), nil)
	sSubscriber.SetTrash(tr)

	mustSync(t, sOfferer, testShareID)
	mustSync(t, sSubscriber, testShareID)

	// See waitForFile's doc comment: the subscriber's edit below reads and
	// bumps the subscriber's own row for this path, which is what it just
	// received from the offerer.
	waitForFile(t, integrationWaitTimeout, subscriber.store, testShareID, subscriber.root, "notes.txt", "v1 from offerer")

	// Subscriber edits its local copy. Under receive-only this is never
	// sent outward (mustSync/SyncShare is a no-op for an OutboundBlocked
	// share; see cfg.Direction.OutboundBlocked in SyncShare), so it just
	// sits locally, diverging from the offerer's version.
	writeFile(t, subscriber.root, "notes.txt", "edited locally by subscriber")
	indexFile(t, subscriber.store, subscriber.id, "notes.txt", subscriber.root)
	mustSync(t, sSubscriber, testShareID) // no-op under receive-only; asserts nothing breaks calling it anyway

	// The offerer's own copy now changes — this is the trigger SPEC.md §1
	// promises the revert on. Sending it reaches the subscriber as a
	// concurrent divergence (offerer's new version doesn't know about the
	// subscriber's local bump, and vice versa).
	writeFile(t, offerer.root, "notes.txt", "v2 from offerer")
	indexFile(t, offerer.store, offerer.id, "notes.txt", offerer.root)
	mustSync(t, sOfferer, testShareID)

	waitFor(t, integrationWaitTimeout, func() bool {
		data, err := os.ReadFile(filepath.Join(subscriber.root, "notes.txt"))
		return err == nil && string(data) == "v2 from offerer"
	})

	// The subscriber's locally-modified content must have been trashed,
	// not just discarded.
	entries, err := tr.List(testShareID)
	if err != nil {
		t.Fatalf("trash List: %v", err)
	}
	if len(entries) != 1 || entries[0].RelPath != "notes.txt" {
		t.Fatalf("expected exactly one trashed notes.txt entry, got %+v", entries)
	}
	trashedData, err := os.ReadFile(entries[0].trashAbs)
	if err != nil {
		t.Fatalf("read trashed copy: %v", err)
	}
	if string(trashedData) != "edited locally by subscriber" {
		t.Fatalf("trashed content = %q, want the subscriber's local edit", trashedData)
	}

	// And a warning was recorded for the UI to surface.
	warnings := sSubscriber.LocallyModifiedWarnings()
	if len(warnings) == 0 {
		t.Fatal("expected at least one LocallyModifiedWarning to be recorded")
	}
	found := false
	for _, w := range warnings {
		if w.RelPath == "notes.txt" && w.Reverted {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected a Reverted warning for notes.txt, got %+v", warnings)
	}
}

// 11. Restore propagates as a new change (SPEC.md §7): a file trashed by a
// remote-initiated delete is restored, and the restore reaches the peer
// through the normal sync pipeline because restoring bumps the local
// version.
func TestIntegration_RestorePropagatesAsNewChange(t *testing.T) {
	a := newTestNode(t, "aaaaaaaaaaaaaaaa")
	b := newTestNode(t, "bbbbbbbbbbbbbbbb")

	writeFile(t, a.root, "keepme.txt", "original content")
	indexFile(t, a.store, a.id, "keepme.txt", a.root)

	sa, sb := connectSessions(t, a, b, Direction{}, Direction{})
	tr := NewTrash(filepath.Join(t.TempDir(), "trash"), nil)
	sa.SetTrash(tr)

	mustSync(t, sa, testShareID)
	mustSync(t, sb, testShareID)

	// See waitForFile's doc comment: the indexDelete call below reads and
	// bumps B's own row for this path, which is what B just received from
	// A.
	waitForFile(t, integrationWaitTimeout, b.store, testShareID, b.root, "keepme.txt", "original content")

	// B deletes the file and that delete propagates to A, which — being a
	// remote-initiated delete — trashes A's copy first.
	if err := os.Remove(filepath.Join(b.root, "keepme.txt")); err != nil {
		t.Fatal(err)
	}
	indexDelete(t, b.store, b.id, "keepme.txt")
	mustSync(t, sb, testShareID)

	waitFor(t, integrationWaitTimeout, func() bool {
		_, err := os.Stat(filepath.Join(a.root, "keepme.txt"))
		return os.IsNotExist(err)
	})
	entries, err := tr.List(testShareID)
	if err != nil {
		t.Fatalf("trash List: %v", err)
	}
	if len(entries) != 1 || entries[0].RelPath != "keepme.txt" {
		t.Fatalf("expected keepme.txt to be trashed on A, got %+v", entries)
	}

	// Restore on A: content comes back, and the version is bumped so it
	// dominates the tombstone currently in A's index.
	row, err := tr.Restore(context.Background(), a.store, a.id, a.root, entries[0])
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if row.Deleted {
		t.Fatal("restored row must not be a tombstone")
	}
	data, err := os.ReadFile(filepath.Join(a.root, "keepme.txt"))
	if err != nil {
		t.Fatalf("read restored file on A: %v", err)
	}
	if string(data) != "original content" {
		t.Fatalf("restored content = %q", data)
	}

	// Propagate the restore, exactly as a real daemon's own
	// change-detection (internal/index's watcher/scanner, not wired into this
	// package's own tests — see indexFile's doc comment) would trigger a
	// SyncShare after noticing the local change.
	mustSync(t, sa, testShareID)

	// See waitForFile's doc comment: the assertions right below read B's
	// row directly, so the wait must cover it too, not just the file.
	waitForFile(t, integrationWaitTimeout, b.store, testShareID, b.root, "keepme.txt", "original content")

	bRow, err := b.store.GetFile(context.Background(), testShareID, "keepme.txt")
	if err != nil {
		t.Fatalf("get b's row: %v", err)
	}
	if bRow.Deleted {
		t.Fatal("B's row should no longer be a tombstone after the restore propagated")
	}
	if !Equal(bRow.Version, row.Version) {
		t.Fatalf("B's version %v does not match A's restored version %v", bRow.Version, row.Version)
	}
}

// TestReconcileShareRecoversAfterAFailedPull covers the gap that made an
// interrupted transfer permanent.
//
// handleIndexUpdate was the only thing that reconciled, so "the peer sent us
// rows" was the only trigger for noticing a file is missing. A pull that
// fails leaves the peer's rows already recorded in peer_files and the file
// still absent locally — and on reconnect the peer sends nothing, because its
// index sync is incremental and its cursor says we already have those rows.
// Nothing revisited it, so the file stayed missing indefinitely.
//
// The setup is that state exactly: the subscriber knows what the offerer has,
// because a previous session recorded it in peer_files, but has neither the
// file nor a local index row, because apply is atomic and the interrupted
// pull never got as far as installing anything. No index update is sent here
// at all — that is the point, and it is what the reconnected peer would
// (not) send.
func TestReconcileShareRecoversAfterAFailedPull(t *testing.T) {
	ctx := context.Background()
	a := newTestNode(t, "offerer")
	b := newTestNode(t, "subscriber")

	writeFile(t, a.root, "big.bin", "the payload")
	row := indexFile(t, a.store, a.id, "big.bin", a.root)

	sa, sb := connectSessions(t, a, b, Direction{}, Direction{})
	_ = sa

	// What the interrupted session left behind on the subscriber.
	if err := b.store.UpsertPeerFiles(ctx, a.id, []index.FileRow{row}); err != nil {
		t.Fatalf("record what the peer has: %v", err)
	}
	if _, err := os.Stat(filepath.Join(b.root, "big.bin")); !os.IsNotExist(err) {
		t.Fatalf("the subscriber should not have the file yet")
	}

	// Deliberately no mustSync: the offerer has nothing new to say, exactly
	// as after a reconnect. Without ReconcileShare nothing pulls, ever.
	if err := sb.ReconcileShare(ctx, testShareID); err != nil {
		t.Fatalf("ReconcileShare: %v", err)
	}

	waitForFile(t, 10*time.Second, b.store, testShareID, b.root, "big.bin", "the payload")
}

// --- .syncatignore (SPEC.md §5) ---------------------------------------

// ignoreMatching builds an ignore predicate matching exactly the given
// share-relative paths, standing in for a compiled .syncatignore.
func ignoreMatching(paths ...string) func(string, bool) bool {
	set := make(map[string]bool, len(paths))
	for _, p := range paths {
		set[p] = true
	}
	return func(relpath string, _ bool) bool { return set[relpath] }
}

// TestIntegration_IgnoredFileIsNeitherSentNorRequested is the headline
// bidirectional case: a file A ignores never reaches B, while a
// non-ignored sibling does — the sibling being what stops this test from
// passing trivially if sync were broken outright.
func TestIntegration_IgnoredFileIsNeitherSentNorRequested(t *testing.T) {
	a := newTestNode(t, "aaaaaaaaaaaaaaaa")
	b := newTestNode(t, "bbbbbbbbbbbbbbbb")

	// A has both files indexed — the ignore rule arrived after they were
	// scanned, which is exactly when the wire-level filters have to work.
	writeFile(t, a.root, "secret.log", "do not share")
	indexFile(t, a.store, a.id, "secret.log", a.root)
	writeFile(t, a.root, "notes.txt", "please share")
	indexFile(t, a.store, a.id, "notes.txt", a.root)

	sa, sb := connectSessionsWithIgnore(t, a, b, Direction{}, Direction{},
		ignoreMatching("secret.log"), nil)
	mustSync(t, sa, testShareID)
	mustSync(t, sb, testShareID)

	// The positive control: the sibling must actually replicate.
	waitForFile(t, integrationWaitTimeout, b.store, testShareID, b.root, "notes.txt", "please share")

	if _, err := os.Stat(filepath.Join(b.root, "secret.log")); !os.IsNotExist(err) {
		t.Errorf("ignored file reached the peer's disk (stat err = %v)", err)
	}
	if _, err := b.store.GetFile(context.Background(), testShareID, "secret.log"); err == nil {
		t.Error("ignored file was advertised into the peer's index")
	}
}

// TestIntegration_IgnoredRemoteFileIsNotPulled is the receive side, and
// the flap loop: B ignores a path that A is legitimately sharing, so B
// must never fetch it — not on the first sync, and not on a second one.
func TestIntegration_IgnoredRemoteFileIsNotPulled(t *testing.T) {
	a := newTestNode(t, "aaaaaaaaaaaaaaaa")
	b := newTestNode(t, "bbbbbbbbbbbbbbbb")

	writeFile(t, a.root, "build/out.o", "artifact")
	indexFile(t, a.store, a.id, "build/out.o", a.root)
	writeFile(t, a.root, "notes.txt", "real content")
	indexFile(t, a.store, a.id, "notes.txt", a.root)

	sa, sb := connectSessionsWithIgnore(t, a, b, Direction{}, Direction{},
		nil, ignoreMatching("build/out.o"))
	mustSync(t, sa, testShareID)
	mustSync(t, sb, testShareID)

	waitForFile(t, integrationWaitTimeout, b.store, testShareID, b.root, "notes.txt", "real content")

	// A second sync is the flap check: if the first pass had pulled and
	// indexed the ignored path, the second would keep churning on it.
	mustSync(t, sb, testShareID)
	waitFor(t, time.Second, func() bool { return true })

	if _, err := os.Stat(filepath.Join(b.root, "build/out.o")); !os.IsNotExist(err) {
		t.Errorf("ignored remote file was pulled to disk (stat err = %v)", err)
	}
	// A's copy must be untouched — the flap loop's final act was to
	// propagate a tombstone back and delete it here.
	if data, err := os.ReadFile(filepath.Join(a.root, "build/out.o")); err != nil || string(data) != "artifact" {
		t.Errorf("the sharing node's own copy was disturbed: data=%q err=%v", data, err)
	}
}

// TestIntegration_IgnoredPathIsNotServedToPeer pins the handleFileRequest
// backstop: even a peer that already knows the path (it learned it before
// the rule existed) cannot fetch its bytes.
func TestIntegration_IgnoredPathIsNotServedToPeer(t *testing.T) {
	a := newTestNode(t, "aaaaaaaaaaaaaaaa")
	b := newTestNode(t, "bbbbbbbbbbbbbbbb")

	writeFile(t, a.root, "secret.txt", "classified")
	row := indexFile(t, a.store, a.id, "secret.txt", a.root)

	// Plant the row in B's view of A, as a pre-rule sync would have.
	if err := b.store.UpsertPeerFiles(context.Background(), a.id, []index.FileRow{row}); err != nil {
		t.Fatalf("seed peer_files: %v", err)
	}

	_, sb := connectSessionsWithIgnore(t, a, b, Direction{}, Direction{},
		ignoreMatching("secret.txt"), nil)
	mustSync(t, sb, testShareID)

	// B believes the file exists and will request it; A must refuse. Hold
	// the assertion long enough for a successful transfer to have landed.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(filepath.Join(b.root, "secret.txt")); !os.IsNotExist(err) {
			t.Fatalf("an ignored file was served to a peer that asked for it (stat err = %v)", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestIntegration_NewlyIgnoredFileSurvivesOnPeer is the regression that
// protects user data: once a file has synced, adding an ignore rule for it
// on A must leave B's copy exactly where it is. Ignoring is not deleting.
func TestIntegration_NewlyIgnoredFileSurvivesOnPeer(t *testing.T) {
	a := newTestNode(t, "aaaaaaaaaaaaaaaa")
	b := newTestNode(t, "bbbbbbbbbbbbbbbb")

	writeFile(t, a.root, "shared.txt", "both have this")
	indexFile(t, a.store, a.id, "shared.txt", a.root)

	sa, sb := connectSessions(t, a, b, Direction{}, Direction{})
	mustSync(t, sa, testShareID)
	waitForFile(t, integrationWaitTimeout, b.store, testShareID, b.root, "shared.txt", "both have this")

	// A now ignores it. The index row goes away WITHOUT a tombstone,
	// which is what index.Store.ApplyScanResult's Ignored bucket does.
	if err := a.store.ApplyScanResult(context.Background(), &index.ScanResult{
		ShareID: testShareID,
		Ignored: []index.FileRow{{ShareID: testShareID, RelPath: "shared.txt"}},
	}); err != nil {
		t.Fatalf("apply ignored: %v", err)
	}

	sa.AddShare(ShareConfig{ShareID: testShareID, Root: a.root, Direction: Direction{},
		Ignore: ignoreMatching("shared.txt")})
	mustSync(t, sa, testShareID)
	mustSync(t, sb, testShareID)

	// Hold: assert the file is still there after sync has had time to act.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(filepath.Join(b.root, "shared.txt")); err != nil {
			t.Fatalf("peer's copy of a newly-ignored file was deleted: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestIntegration_DeltaFallsBackToSnapshotWhenJournalHoldsIgnoredPath pins
// the one place ignoring interacts with the journal. change_journal keeps
// the entry written while a path was still syncing, and a delta batch
// cannot omit it (the receiver enforces contiguous sequence numbers, and
// Store.ApplyPeerDelta re-checks the range), so answerSyncRequest must
// answer with a full snapshot instead. Getting this wrong leaves the peer
// rejecting every batch and resyncing forever.
//
// Reaching the delta path at all takes a first completed exchange: A only
// sends deltas once its *outgoing* cursor for this peer holds the current
// epoch, which happens when B acks a snapshot.
func TestIntegration_DeltaFallsBackToSnapshotWhenJournalHoldsIgnoredPath(t *testing.T) {
	a := newTestNode(t, "aaaaaaaaaaaaaaaa")
	b := newTestNode(t, "bbbbbbbbbbbbbbbb")
	ctx := context.Background()

	writeFile(t, a.root, "notes.txt", "keep me")
	indexFile(t, a.store, a.id, "notes.txt", a.root)

	sa, _ := connectSessions(t, a, b, Direction{}, Direction{})
	mustSync(t, sa, testShareID)
	waitForFile(t, integrationWaitTimeout, b.store, testShareID, b.root, "notes.txt", "keep me")

	// Wait for B's ack to advance A's outgoing cursor onto the current
	// epoch; until it does, the next SyncShare would send a snapshot and
	// this test would prove nothing.
	epoch, _, err := a.store.ShareState(ctx, testShareID)
	if err != nil {
		t.Fatalf("share state: %v", err)
	}
	waitFor(t, integrationWaitTimeout, func() bool {
		c, err := a.store.Cursor(ctx, b.id, testShareID, "outgoing")
		return err == nil && c.Epoch == epoch
	})

	// Journal a new path while it is still syncing normally...
	writeFile(t, a.root, "stale.log", "journalled before the rule existed")
	indexFile(t, a.store, a.id, "stale.log", a.root)

	// ...then start ignoring it, leaving its journal entry ahead of B's
	// cursor. This is the exact state the fallback exists for.
	sa.AddShare(ShareConfig{ShareID: testShareID, Root: a.root, Direction: Direction{},
		Ignore: ignoreMatching("stale.log")})

	writeFile(t, a.root, "second.txt", "after the rule")
	indexFile(t, a.store, a.id, "second.txt", a.root)

	mustSync(t, sa, testShareID)

	// Convergence on a file journalled *after* the rule proves the
	// exchange completed rather than dead-ending in a resync loop.
	waitForFile(t, integrationWaitTimeout, b.store, testShareID, b.root, "second.txt", "after the rule")

	// The assertion that matters is on what the wire actually carried:
	// B's view of A's index. Checking only B's own files table would pass
	// even when the delta leaked the path, because the FileRequest
	// backstop would separately stop the download.
	peerRows, err := b.store.ListPeerFiles(ctx, a.id, testShareID)
	if err != nil {
		t.Fatalf("list peer files: %v", err)
	}
	for _, r := range peerRows {
		if r.RelPath == "stale.log" {
			t.Error("a now-ignored journal entry was advertised to the peer")
		}
	}
	if _, err := b.store.GetFile(ctx, testShareID, "stale.log"); err == nil {
		t.Error("a now-ignored path was indexed by the peer")
	}
	if _, err := os.Stat(filepath.Join(b.root, "stale.log")); !os.IsNotExist(err) {
		t.Errorf("a now-ignored path reached the peer's disk (stat err = %v)", err)
	}

	// The fallback must fire once, not on every sync forever. The snapshot
	// re-anchors this peer's cursor at HighSeq, past the stale journal
	// entry, so the next exchange is a delta again.
	_, high, err := a.store.ShareState(ctx, testShareID)
	if err != nil {
		t.Fatalf("share state: %v", err)
	}
	waitFor(t, integrationWaitTimeout, func() bool {
		c, err := a.store.Cursor(ctx, b.id, testShareID, "outgoing")
		return err == nil && c.AppliedSeq == high
	})
}

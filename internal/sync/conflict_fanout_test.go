package sync

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"sort"
	"testing"
	"time"

	"github.com/nickmarrone/syncat/internal/protocol"
)

// Issue #54: in a hub-and-spoke share, one conflict between two spokes can
// be resolved independently by more than one node. The hub (alice)
// resolves bob-vs-carol, and carol, having received bob's version through
// the hub before the hub's own resolution reaches her, resolves the same
// conflict herself. SPEC.md §5 wants one losing version kept as one
// conflict copy, so both resolvers must pick the same name for it.

// TestReconcile_ConflictCopyNameIndependentOfResolver is the pure,
// Reconcile-level form of the race: the hub and carol each reconcile the
// same pair of versions, with different node ids and different clocks.
func TestReconcile_ConflictCopyNameIndependentOfResolver(t *testing.T) {
	const (
		hubID   = "1111111111111111"
		bobID   = "2222222222222222"
		carolID = "3333333333333333"
	)
	bobSum := sha256.Sum256([]byte("bob's edit"))
	carolSum := sha256.Sum256([]byte("carol's edit"))
	base := time.Date(2026, 11, 6, 0, 58, 16, 0, time.UTC)
	bob := protocol.FileInfo{RelPath: "contested.txt", Type: protocol.FileTypeFile, Size: 10,
		MTimeNS: base.Add(2 * time.Second).UnixNano(), Mode: 0o644, SHA256: bobSum[:],
		Version: vv(hubID, 1, bobID, 1)}
	carol := protocol.FileInfo{RelPath: "contested.txt", Type: protocol.FileTypeFile, Size: 12,
		MTimeNS: base.UnixNano(), Mode: 0o644, SHA256: carolSum[:],
		Version: vv(hubID, 1, carolID, 1)}

	hubClock := func() time.Time { return base.Add(time.Hour) }
	carolClock := func() time.Time { return base.Add(3 * time.Hour) }

	// The hub already holds bob's version and receives carol's.
	atHub := findAction(t, Reconcile([]protocol.FileInfo{bob}, []protocol.FileInfo{carol}, hubID, mirrorDir(), hubClock, nil), "contested.txt")
	// Carol holds her own version and receives bob's, forwarded by the hub.
	atCarol := findAction(t, Reconcile([]protocol.FileInfo{carol}, []protocol.FileInfo{bob}, carolID, mirrorDir(), carolClock, nil), "contested.txt")

	for name, a := range map[string]Action{"hub": atHub, "carol": atCarol} {
		if a.Kind != ActionConflictCopy {
			t.Fatalf("%s: Kind = %v, want ActionConflictCopy", name, a.Kind)
		}
		if !bytes.Equal(a.ConflictInfo.SHA256, carolSum[:]) {
			t.Fatalf("%s: loser is not carol's edit (bob's mtime is newer)", name)
		}
	}
	if atHub.ConflictRelPath != atCarol.ConflictRelPath {
		t.Fatalf("hub and carol named the same losing version differently: hub=%q carol=%q",
			atHub.ConflictRelPath, atCarol.ConflictRelPath)
	}
	if want := "contested.sync-conflict-20261106-005816-" + hexPrefix(carolSum[:]) + ".txt"; atHub.ConflictRelPath != want {
		t.Errorf("ConflictRelPath = %q, want %q (loser's mtime and digest)", atHub.ConflictRelPath, want)
	}

	// The reverse: a different losing content, even with the same mtime and
	// version vector, must get a different name, or one copy would
	// overwrite the other.
	other := carol
	otherSum := sha256.Sum256([]byte("carol's other edit"))
	other.SHA256 = otherSum[:]
	atHub2 := findAction(t, Reconcile([]protocol.FileInfo{bob}, []protocol.FileInfo{other}, hubID, mirrorDir(), hubClock, nil), "contested.txt")
	if atHub2.Kind != ActionConflictCopy {
		t.Fatalf("other loser: Kind = %v, want ActionConflictCopy", atHub2.Kind)
	}
	if atHub2.ConflictRelPath == atHub.ConflictRelPath {
		t.Fatalf("two different losing contents share the conflict name %q", atHub.ConflictRelPath)
	}
}

// TestLoserConflictRelPathFallback covers the two cases where the loser
// cannot name its own copy: a digest too short to take an id from, and a
// derived name equal to the winner's own path.
func TestLoserConflictRelPathFallback(t *testing.T) {
	const node = "deadbeefcafef00d"
	stamp := time.Date(2026, 8, 30, 14, 5, 9, 0, time.UTC)
	clock := func() time.Time { return stamp }
	const fallback = "f.sync-conflict-20260830-140509-deadbeefcafef00d.txt"

	short := protocol.FileInfo{SHA256: []byte("short"), MTimeNS: 0}
	if got := loserConflictRelPath("f.txt", short, node, clock); got != fallback {
		t.Errorf("short digest: got %q, want %q", got, fallback)
	}

	sum := sha256.Sum256([]byte("x"))
	mtime := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	self := "f.sync-conflict-20260102-030405-" + hexPrefix(sum[:]) + ".txt"
	loser := protocol.FileInfo{SHA256: sum[:], MTimeNS: mtime.UnixNano()}
	if got := loserConflictRelPath(self, loser, node, clock); got != fallback {
		t.Errorf("name equal to relpath: got %q, want %q", got, fallback)
	}
	if got := loserConflictRelPath("f.txt", loser, node, clock); got != self {
		t.Errorf("normal case: got %q, want %q", got, self)
	}
}

func hexPrefix(sum []byte) string { return hex.EncodeToString(sum[:8]) }

// TestIntegration_ThreeNodeConflictResolvedTwiceLeavesOneCopy drives the
// #54 race end to end with real Sessions, in a fixed order so it fails
// every time rather than only under unlucky timing:
//
//  1. bob and carol edit contested.txt independently; the hub (alice)
//     syncs with bob first and so holds bob's version.
//  2. The hub resolves bob-vs-carol (carol's side ignores the hub for this
//     step, so carol is left unresolved).
//  3. Carol resolves the same conflict herself against bob's version as the
//     hub first forwarded it (bob, who still holds exactly that version and
//     ignores carol for this step, stands in for the hub before its
//     resolution reached her).
//  4. All links come back; everything converges.
//
// The converged trees must hold exactly one conflict copy, the same one on
// every node.
func TestIntegration_ThreeNodeConflictResolvedTwiceLeavesOneCopy(t *testing.T) {
	alice := newTestNode(t, "alice")
	bob := newTestNode(t, "bob")
	carol := newTestNode(t, "carol")

	base := time.Date(2026, 11, 6, 0, 58, 16, 0, time.UTC).UnixNano()
	writeFile(t, bob.root, "contested.txt", "bob's edit")
	indexFileWithMTime(t, bob.store, bob.id, "contested.txt", bob.root, base+2*int64(time.Second))
	writeFile(t, carol.root, "contested.txt", "carol's edit")
	indexFileWithMTime(t, carol.store, carol.id, "contested.txt", carol.root, base)

	wantWinner := sha256.Sum256([]byte("bob's edit"))
	wantLoser := sha256.Sum256([]byte("carol's edit"))
	blocked := Direction{InboundBlocked: true}

	// Each phase waits until the resolving node has persisted its index
	// rows, not only written the files: closing a Session cancels row
	// writes still in flight, and this harness has no scanner to index a
	// file left on disk without one.

	// 1. The hub receives bob's version.
	sa, sb := connectSessions(t, alice, bob, Direction{}, Direction{})
	mustSync(t, sa, testShareID)
	mustSync(t, sb, testShareID)
	waitFor(t, integrationWaitTimeout, func() bool {
		tree, ok := settledTree(t, alice)
		return ok && tree["contested.txt"] == wantWinner
	})
	_ = sa.Close()
	_ = sb.Close()

	// 2. The hub resolves the conflict; carol does not.
	sa, sc := connectSessions(t, alice, carol, Direction{}, blocked)
	mustSync(t, sa, testShareID)
	mustSync(t, sc, testShareID)
	waitFor(t, integrationWaitTimeout, func() bool {
		tree, ok := settledTree(t, alice)
		return ok && len(conflictCopies(tree, "contested")) == 1 && rowVersion(t, alice, "contested.txt")[alice.id] == 1
	})
	_ = sa.Close()
	_ = sc.Close()

	// 3. Carol resolves it too, against bob's unresolved version.
	sb, sc = connectSessions(t, bob, carol, blocked, Direction{})
	mustSync(t, sb, testShareID)
	mustSync(t, sc, testShareID)
	waitFor(t, integrationWaitTimeout, func() bool {
		tree, ok := settledTree(t, carol)
		return ok && tree["contested.txt"] == wantWinner && len(conflictCopies(tree, "contested")) == 1 &&
			rowVersion(t, carol, "contested.txt")[carol.id] == 2
	})
	_ = sb.Close()
	_ = sc.Close()

	hubTree, _ := settledTree(t, alice)
	carolTree, _ := settledTree(t, carol)
	hubCopy, carolCopy := conflictCopies(hubTree, "contested"), conflictCopies(carolTree, "contested")

	// 4. Hub and spokes reconnect and converge.
	sab, sba := connectSessions(t, alice, bob, Direction{}, Direction{})
	sac, sca := connectSessions(t, alice, carol, Direction{}, Direction{})
	all := []*Session{sab, sba, sac, sca}
	deadline := time.Now().Add(integrationWaitTimeout)
	for {
		for _, s := range all {
			mustSync(t, s, testShareID)
		}
		ta, okA := settledTree(t, alice)
		tb, okB := settledTree(t, bob)
		tc, okC := settledTree(t, carol)
		if okA && okB && okC && treesEqual(ta, tb) && treesEqual(ta, tc) && ta["contested.txt"] == wantWinner {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no convergence: alice=%v bob=%v carol=%v", treeKeys(ta), treeKeys(tb), treeKeys(tc))
		}
		time.Sleep(20 * time.Millisecond)
	}

	for name, n := range map[string]*testNode{"alice": alice, "bob": bob, "carol": carol} {
		tree := snapshotTree(t, n.root)
		copies := conflictCopies(tree, "contested")
		if len(copies) != 1 {
			t.Errorf("%s holds %d conflict copies, want exactly 1: %v (hub named its copy %v, carol named hers %v)",
				name, len(copies), copies, hubCopy, carolCopy)
			continue
		}
		if tree[copies[0]] != wantLoser {
			t.Errorf("%s: conflict copy %s does not hold carol's losing edit", name, copies[0])
		}
	}
}

// settledTree snapshots n's share and reports whether its index agrees with
// the disk: every file has a live row with the same digest, and every live
// row has its file.
func settledTree(t *testing.T, n *testNode) (map[string][32]byte, bool) {
	t.Helper()
	tree := snapshotTree(t, n.root)
	rows, err := n.store.ListShare(context.Background(), testShareID, false)
	if err != nil {
		t.Fatalf("list share: %v", err)
	}
	live := 0
	for _, r := range rows {
		if r.Deleted || r.Type != protocol.FileTypeFile {
			continue
		}
		live++
		sum, ok := tree[r.RelPath]
		if !ok || !bytes.Equal(sum[:], r.SHA256) {
			return tree, false
		}
	}
	return tree, live == len(tree)
}

// rowVersion returns relpath's indexed version on n, or nil.
func rowVersion(t *testing.T, n *testNode, relpath string) protocol.VersionVector {
	t.Helper()
	row, err := n.store.GetFile(context.Background(), testShareID, relpath)
	if err != nil {
		return nil
	}
	return row.Version
}

// conflictCopies lists the conflict copies of <stem>.txt in tree, sorted.
func conflictCopies(tree map[string][32]byte, stem string) []string {
	re := regexp.MustCompile(`^` + regexp.QuoteMeta(stem) + `\.sync-conflict-\d{8}-\d{6}-[0-9a-f]+\.txt$`)
	var out []string
	for rel := range tree {
		if re.MatchString(rel) {
			out = append(out, rel)
		}
	}
	sort.Strings(out)
	return out
}

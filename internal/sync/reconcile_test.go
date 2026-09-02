package sync

import (
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/nickmarrone/syncat/internal/protocol"
)

const nodeLocal = "aaaaaaaaaaaaaaaa"

func fixedClock() time.Time { return time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC) }

func fi(relpath string, mtime int64, sha string, version protocol.VersionVector, deleted bool) protocol.FileInfo {
	return protocol.FileInfo{
		RelPath: relpath,
		Type:    protocol.FileTypeFile,
		Size:    int64(len(sha)),
		MTimeNS: mtime,
		Mode:    0o644,
		SHA256:  []byte(sha),
		Version: version,
		Deleted: deleted,
	}
}

func mirrorDir() Direction      { return Direction{} }
func readOnlyDir() Direction    { return Direction{InboundBlocked: true} }
func receiveOnlyDir() Direction { return Direction{OutboundBlocked: true} }

// findAction returns the action for relpath, failing the test if absent.
func findAction(t *testing.T, actions []Action, relpath string) Action {
	t.Helper()
	for _, a := range actions {
		if a.RelPath == relpath {
			return a
		}
	}
	t.Fatalf("no action for relpath %q in %+v", relpath, actions)
	return Action{}
}

func TestReconcile_OnlyLocal(t *testing.T) {
	local := []protocol.FileInfo{fi("a.txt", 100, "x", vv("local", 1), false)}
	actions := Reconcile(local, nil, nodeLocal, mirrorDir(), fixedClock)
	a := findAction(t, actions, "a.txt")
	if a.Kind != ActionNone {
		t.Errorf("Kind = %v, want ActionNone", a.Kind)
	}
}

func TestReconcile_OnlyLocal_ReceiveOnlyFlagged(t *testing.T) {
	local := []protocol.FileInfo{fi("a.txt", 100, "x", vv("local", 1), false)}
	actions := Reconcile(local, nil, nodeLocal, receiveOnlyDir(), fixedClock)
	a := findAction(t, actions, "a.txt")
	if a.Kind != ActionLocallyModified || !a.LocallyModified {
		t.Errorf("got %+v, want ActionLocallyModified", a)
	}
}

func TestReconcile_OnlyRemote_Pull(t *testing.T) {
	remote := []protocol.FileInfo{fi("a.txt", 100, "x", vv("remote", 1), false)}
	actions := Reconcile(nil, remote, nodeLocal, mirrorDir(), fixedClock)
	a := findAction(t, actions, "a.txt")
	if a.Kind != ActionPull || a.Source != SourceRemote {
		t.Errorf("got %+v, want ActionPull/SourceRemote", a)
	}
	if !Equal(a.Resolved.Version, vv("remote", 1)) {
		t.Errorf("Resolved.Version = %v, want remote's version as-is", a.Resolved.Version)
	}
}

func TestReconcile_OnlyRemote_Tombstone(t *testing.T) {
	remote := []protocol.FileInfo{fi("a.txt", 100, "x", vv("remote", 1), true)}
	actions := Reconcile(nil, remote, nodeLocal, mirrorDir(), fixedClock)
	a := findAction(t, actions, "a.txt")
	if a.Kind != ActionDelete || a.Source != SourceNone {
		t.Errorf("got %+v, want ActionDelete/SourceNone", a)
	}
}

func TestReconcile_RemoteDominates_Pull(t *testing.T) {
	local := []protocol.FileInfo{fi("a.txt", 100, "x", vv("local", 1), false)}
	remote := []protocol.FileInfo{fi("a.txt", 200, "y", vv("local", 1, "remote", 1), false)}
	actions := Reconcile(local, remote, nodeLocal, mirrorDir(), fixedClock)
	a := findAction(t, actions, "a.txt")
	if a.Kind != ActionPull || a.Source != SourceRemote {
		t.Errorf("got %+v, want ActionPull/SourceRemote", a)
	}
}

func TestReconcile_RemoteDominates_Delete(t *testing.T) {
	local := []protocol.FileInfo{fi("a.txt", 100, "x", vv("local", 1), false)}
	remote := []protocol.FileInfo{fi("a.txt", 200, "y", vv("local", 1, "remote", 1), true)}
	actions := Reconcile(local, remote, nodeLocal, mirrorDir(), fixedClock)
	a := findAction(t, actions, "a.txt")
	if a.Kind != ActionDelete || a.Source != SourceNone {
		t.Errorf("got %+v, want ActionDelete/SourceNone", a)
	}
	if !a.Resolved.Deleted {
		t.Errorf("Resolved.Deleted = false, want true")
	}
}

func TestReconcile_LocalDominates_Skip(t *testing.T) {
	local := []protocol.FileInfo{fi("a.txt", 200, "y", vv("local", 1, "remote", 1), false)}
	remote := []protocol.FileInfo{fi("a.txt", 100, "x", vv("remote", 1), false)}
	actions := Reconcile(local, remote, nodeLocal, mirrorDir(), fixedClock)
	a := findAction(t, actions, "a.txt")
	if a.Kind != ActionNone {
		t.Errorf("got %+v, want ActionNone (peer will pull from us)", a)
	}
}

func TestReconcile_LocalDominates_ReceiveOnlyFlagged(t *testing.T) {
	local := []protocol.FileInfo{fi("a.txt", 200, "y", vv("local", 1, "remote", 1), false)}
	remote := []protocol.FileInfo{fi("a.txt", 100, "x", vv("remote", 1), false)}
	actions := Reconcile(local, remote, nodeLocal, receiveOnlyDir(), fixedClock)
	a := findAction(t, actions, "a.txt")
	if a.Kind != ActionLocallyModified {
		t.Errorf("got %+v, want ActionLocallyModified", a)
	}
}

func TestReconcile_EqualVersions_None(t *testing.T) {
	v := vv("local", 1, "remote", 1)
	local := []protocol.FileInfo{fi("a.txt", 100, "x", v, false)}
	remote := []protocol.FileInfo{fi("a.txt", 100, "x", v, false)}
	actions := Reconcile(local, remote, nodeLocal, mirrorDir(), fixedClock)
	a := findAction(t, actions, "a.txt")
	if a.Kind != ActionNone {
		t.Errorf("got %+v, want ActionNone", a)
	}
}

func TestReconcile_InboundBlocked_ReadOnlyShare(t *testing.T) {
	local := []protocol.FileInfo{fi("a.txt", 100, "x", vv("local", 1), false)}
	remote := []protocol.FileInfo{fi("a.txt", 999, "z", vv("local", 1, "remote", 5), false)}
	actions := Reconcile(local, remote, nodeLocal, readOnlyDir(), fixedClock)
	a := findAction(t, actions, "a.txt")
	if a.Kind != ActionNone {
		t.Errorf("got %+v, want ActionNone: inbound must be ignored entirely on a read-only share", a)
	}
}

func TestReconcile_Concurrent_ConflictCopy_RemoteWinsByMtime(t *testing.T) {
	local := []protocol.FileInfo{fi("a.txt", 100, "local-content", vv("local", 1), false)}
	remote := []protocol.FileInfo{fi("a.txt", 200, "remote-content", vv("remote", 1), false)}
	actions := Reconcile(local, remote, nodeLocal, mirrorDir(), fixedClock)
	a := findAction(t, actions, "a.txt")

	if a.Kind != ActionConflictCopy {
		t.Fatalf("Kind = %v, want ActionConflictCopy", a.Kind)
	}
	if a.Source != SourceRemote {
		t.Errorf("Source = %v, want SourceRemote (remote has the newer mtime)", a.Source)
	}
	if string(a.Resolved.SHA256) != "remote-content" {
		t.Errorf("Resolved is not the remote (winning) content: %+v", a.Resolved)
	}
	if a.ConflictSource != SourceLocal {
		t.Errorf("ConflictSource = %v, want SourceLocal", a.ConflictSource)
	}
	if string(a.ConflictInfo.SHA256) != "local-content" {
		t.Errorf("ConflictInfo is not the local (losing) content: %+v", a.ConflictInfo)
	}
	wantConflictPath := "a.sync-conflict-20260830-120000-aaaaaaaaaaaaaaaa.txt"
	if a.ConflictRelPath != wantConflictPath {
		t.Errorf("ConflictRelPath = %q, want %q", a.ConflictRelPath, wantConflictPath)
	}
	wantVersion := Bump(Merge(vv("local", 1), vv("remote", 1)), nodeLocal)
	if !Equal(a.Resolved.Version, wantVersion) {
		t.Errorf("Resolved.Version = %v, want merge+bump %v", a.Resolved.Version, wantVersion)
	}
}

func TestReconcile_Concurrent_ConflictCopy_LocalWinsByMtime(t *testing.T) {
	local := []protocol.FileInfo{fi("a.txt", 500, "local-content", vv("local", 1), false)}
	remote := []protocol.FileInfo{fi("a.txt", 200, "remote-content", vv("remote", 1), false)}
	actions := Reconcile(local, remote, nodeLocal, mirrorDir(), fixedClock)
	a := findAction(t, actions, "a.txt")

	if a.Kind != ActionConflictCopy {
		t.Fatalf("Kind = %v, want ActionConflictCopy", a.Kind)
	}
	if a.Source != SourceLocal {
		t.Errorf("Source = %v, want SourceLocal (local has the newer mtime)", a.Source)
	}
	if string(a.Resolved.SHA256) != "local-content" {
		t.Errorf("Resolved is not the local (winning) content: %+v", a.Resolved)
	}
	if a.ConflictSource != SourceRemote {
		t.Errorf("ConflictSource = %v, want SourceRemote", a.ConflictSource)
	}
	if string(a.ConflictInfo.SHA256) != "remote-content" {
		t.Errorf("ConflictInfo is not the remote (losing) content: %+v", a.ConflictInfo)
	}
}

func TestReconcile_Concurrent_MtimeTie_SHA256TieBreak(t *testing.T) {
	// Same mtime on both sides; sha256 "remote-hi" > "local-lo" lexically,
	// so remote must win via the tie-break, not local (which would win if
	// the tie-break were ignored and mtime-equal defaulted to local).
	local := []protocol.FileInfo{fi("a.txt", 100, "local-lo", vv("local", 1), false)}
	remote := []protocol.FileInfo{fi("a.txt", 100, "remote-hi", vv("remote", 1), false)}
	actions := Reconcile(local, remote, nodeLocal, mirrorDir(), fixedClock)
	a := findAction(t, actions, "a.txt")

	if a.Kind != ActionConflictCopy {
		t.Fatalf("Kind = %v, want ActionConflictCopy", a.Kind)
	}
	if a.Source != SourceRemote {
		t.Errorf("Source = %v, want SourceRemote: sha256(%q) > sha256(%q) lexically", a.Source, "remote-hi", "local-lo")
	}
}

func TestReconcile_DeleteVsModify_LocalDeletedRemoteModified_Resurrect(t *testing.T) {
	local := []protocol.FileInfo{fi("a.txt", 100, "old", vv("local", 1), true)}
	remote := []protocol.FileInfo{fi("a.txt", 200, "new", vv("remote", 1), false)}
	actions := Reconcile(local, remote, nodeLocal, mirrorDir(), fixedClock)
	a := findAction(t, actions, "a.txt")

	if a.Kind != ActionResurrect {
		t.Fatalf("Kind = %v, want ActionResurrect", a.Kind)
	}
	if a.Source != SourceRemote {
		t.Errorf("Source = %v, want SourceRemote (remote holds the surviving modify)", a.Source)
	}
	if a.Resolved.Deleted {
		t.Errorf("Resolved.Deleted = true, want false: modify must win over delete")
	}
}

func TestReconcile_DeleteVsModify_RemoteDeletedLocalModified_Resurrect(t *testing.T) {
	// Even though remote's mtime is far newer, delete-vs-modify overrides
	// the mtime/sha256 tie-break entirely: modify always wins.
	local := []protocol.FileInfo{fi("a.txt", 100, "new", vv("local", 1), false)}
	remote := []protocol.FileInfo{fi("a.txt", 99999, "gone", vv("remote", 1), true)}
	actions := Reconcile(local, remote, nodeLocal, mirrorDir(), fixedClock)
	a := findAction(t, actions, "a.txt")

	if a.Kind != ActionResurrect {
		t.Fatalf("Kind = %v, want ActionResurrect", a.Kind)
	}
	if a.Source != SourceLocal {
		t.Errorf("Source = %v, want SourceLocal (local holds the surviving modify)", a.Source)
	}
	if a.Resolved.Deleted {
		t.Errorf("Resolved.Deleted = true, want false")
	}
}

func TestReconcile_TombstoneVsTombstone_ConcurrentVersions(t *testing.T) {
	local := []protocol.FileInfo{fi("a.txt", 100, "old", vv("local", 1), true)}
	remote := []protocol.FileInfo{fi("a.txt", 200, "old2", vv("remote", 1), true)}
	actions := Reconcile(local, remote, nodeLocal, mirrorDir(), fixedClock)
	a := findAction(t, actions, "a.txt")

	if a.Kind != ActionDelete {
		t.Fatalf("Kind = %v, want ActionDelete", a.Kind)
	}
	if a.Source != SourceNone {
		t.Errorf("Source = %v, want SourceNone: nothing to transfer for a tombstone", a.Source)
	}
	if !a.Resolved.Deleted {
		t.Errorf("Resolved.Deleted = false, want true")
	}
	wantVersion := Bump(Merge(vv("local", 1), vv("remote", 1)), nodeLocal)
	if !Equal(a.Resolved.Version, wantVersion) {
		t.Errorf("Resolved.Version = %v, want merge+bump %v", a.Resolved.Version, wantVersion)
	}
}

func TestReconcile_ReceiveOnly_ConcurrentContentConflict_FlaggedNotCopied(t *testing.T) {
	local := []protocol.FileInfo{fi("a.txt", 500, "local-content", vv("local", 1), false)}
	remote := []protocol.FileInfo{fi("a.txt", 200, "remote-content", vv("remote", 1), false)}
	actions := Reconcile(local, remote, nodeLocal, receiveOnlyDir(), fixedClock)
	a := findAction(t, actions, "a.txt")

	if a.Kind != ActionLocallyModified {
		t.Fatalf("Kind = %v, want ActionLocallyModified (receive-only never keeps both)", a.Kind)
	}
	if a.ConflictRelPath != "" {
		t.Errorf("ConflictRelPath = %q, want empty: no conflict copy under receive-only", a.ConflictRelPath)
	}
}

func TestReconcile_ReceiveOnly_DeleteVsModify_StillResurrects(t *testing.T) {
	// Local deleted, remote modified: this is a legitimate incoming
	// resurrect driven by the peer, not an unauthorized local change, so it
	// must proceed as ActionResurrect even under receive-only.
	local := []protocol.FileInfo{fi("a.txt", 100, "old", vv("local", 1), true)}
	remote := []protocol.FileInfo{fi("a.txt", 200, "new", vv("remote", 1), false)}
	actions := Reconcile(local, remote, nodeLocal, receiveOnlyDir(), fixedClock)
	a := findAction(t, actions, "a.txt")

	if a.Kind != ActionResurrect {
		t.Fatalf("Kind = %v, want ActionResurrect", a.Kind)
	}
}

func TestReconcile_InvalidRemoteRelPath_Rejected(t *testing.T) {
	remote := []protocol.FileInfo{fi("../../etc/passwd", 100, "x", vv("remote", 1), false)}
	actions := Reconcile(nil, remote, nodeLocal, mirrorDir(), fixedClock)
	a := findAction(t, actions, "../../etc/passwd")
	if a.Kind != ActionNone {
		t.Errorf("Kind = %v, want ActionNone: a hostile remote relpath must never produce a real action", a.Kind)
	}
}

func TestReconcile_DeterministicOrdering(t *testing.T) {
	local := []protocol.FileInfo{
		fi("z.txt", 1, "z", vv("local", 1), false),
		fi("a.txt", 1, "a", vv("local", 1), false),
		fi("m.txt", 1, "m", vv("local", 1), false),
	}
	actions := Reconcile(local, nil, nodeLocal, mirrorDir(), fixedClock)
	if len(actions) != 3 {
		t.Fatalf("got %d actions, want 3", len(actions))
	}
	want := []string{"a.txt", "m.txt", "z.txt"}
	for i, w := range want {
		if actions[i].RelPath != w {
			t.Errorf("actions[%d].RelPath = %q, want %q", i, actions[i].RelPath, w)
		}
	}
}

func TestDirectionFor(t *testing.T) {
	d := DirectionFor("read-only", "")
	if !d.InboundBlocked || d.OutboundBlocked {
		t.Errorf("read-only offerer: got %+v", d)
	}
	d = DirectionFor("read-write", "")
	if d.InboundBlocked || d.OutboundBlocked {
		t.Errorf("read-write offerer: got %+v", d)
	}
	d = DirectionFor("", "receive-only")
	if d.InboundBlocked || !d.OutboundBlocked {
		t.Errorf("receive-only subscriber: got %+v", d)
	}
	d = DirectionFor("", "mirror")
	if d.InboundBlocked || d.OutboundBlocked {
		t.Errorf("mirror subscriber: got %+v", d)
	}
}

// vv is a small test helper for building a VersionVector literal:
// vv("a", 1, "b", 2) == protocol.VersionVector{"a": 1, "b": 2}.
func vv(pairs ...any) protocol.VersionVector {
	v := protocol.VersionVector{}
	for i := 0; i < len(pairs); i += 2 {
		v[pairs[i].(string)] = uint64(pairs[i+1].(int))
	}
	return v
}

func TestEqual(t *testing.T) {
	cases := []struct {
		name string
		a, b protocol.VersionVector
		want bool
	}{
		{"both empty", vv(), vv(), true},
		{"nil vs empty", nil, vv(), true},
		{"nil vs nil", nil, nil, true},
		{"identical single key", vv("a", 1), vv("a", 1), true},
		{"identical multi key", vv("a", 1, "b", 2), vv("a", 1, "b", 2), true},
		{"different value", vv("a", 1), vv("a", 2), false},
		{"missing key as zero, equal", vv("a", 0), vv(), true},
		{"missing key as zero, not equal", vv("a", 1), vv(), false},
		{"disjoint keys, both nonzero", vv("a", 1), vv("b", 1), false},
		{"extra zero key still equal", vv("a", 1, "b", 0), vv("a", 1), true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Equal(c.a, c.b); got != c.want {
				t.Errorf("Equal(a, b) = %v, want %v", got, c.want)
			}
			if got := Equal(c.b, c.a); got != c.want {
				t.Errorf("Equal(b, a) = %v, want %v", got, c.want)
			}
		})
	}
}

func TestDominates(t *testing.T) {
	cases := []struct {
		name       string
		a, b       protocol.VersionVector
		wantAOverB bool
		wantBOverA bool
	}{
		{"equal vectors", vv("a", 1), vv("a", 1), false, false},
		{"equal empty", vv(), vv(), false, false},
		{"strict single key a>b", vv("a", 2), vv("a", 1), true, false},
		{"strict multi key a>b all keys ahead", vv("a", 2, "b", 3), vv("a", 1, "b", 2), true, false},
		{"a dominates via missing key treated as zero", vv("a", 1), vv(), true, false},
		{"empty dominated by nonzero", vv(), vv("a", 1), false, true},
		{"disjoint keys: neither dominates", vv("a", 1), vv("b", 1), false, false},
		{"partial overlap, mixed: neither dominates", vv("a", 2, "b", 1), vv("a", 1, "c", 1), false, false},
		{"a ahead on shared key, b has no extra keys", vv("a", 2, "b", 1), vv("a", 1, "b", 1), true, false},
		{"equal on shared key, a has an extra nonzero key", vv("a", 1, "b", 1), vv("a", 1), true, false},
		{"equal on shared key, a has an extra zero key -> equal not dominates", vv("a", 1, "b", 0), vv("a", 1), false, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Dominates(c.a, c.b); got != c.wantAOverB {
				t.Errorf("Dominates(a, b) = %v, want %v", got, c.wantAOverB)
			}
			if got := Dominates(c.b, c.a); got != c.wantBOverA {
				t.Errorf("Dominates(b, a) = %v, want %v", got, c.wantBOverA)
			}
		})
	}
}

func TestConcurrent(t *testing.T) {
	cases := []struct {
		name string
		a, b protocol.VersionVector
		want bool
	}{
		{"equal", vv("a", 1), vv("a", 1), false},
		{"a dominates b", vv("a", 2), vv("a", 1), false},
		{"b dominates a", vv("a", 1), vv("a", 2), false},
		{"disjoint keys, both nonzero", vv("a", 1), vv("b", 1), true},
		{"partial overlap, mixed direction", vv("a", 2, "b", 1), vv("a", 1, "b", 2), true},
		{"empty vs empty", vv(), vv(), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Concurrent(c.a, c.b); got != c.want {
				t.Errorf("Concurrent(a, b) = %v, want %v", got, c.want)
			}
			if got := Concurrent(c.b, c.a); got != c.want {
				t.Errorf("Concurrent(b, a) = %v, want %v", got, c.want)
			}
		})
	}
}

func TestMergeBasic(t *testing.T) {
	got := Merge(vv("a", 1, "b", 5), vv("a", 3, "c", 2))
	want := vv("a", 3, "b", 5, "c", 2)
	if !Equal(got, want) {
		t.Errorf("Merge = %v, want %v", got, want)
	}
}

func TestMergeDoesNotMutateInputs(t *testing.T) {
	a := vv("a", 1)
	b := vv("a", 2)
	_ = Merge(a, b)
	if a["a"] != 1 || b["a"] != 2 {
		t.Fatalf("Merge mutated an input: a=%v b=%v", a, b)
	}
}

func TestBump(t *testing.T) {
	v := vv("a", 1)
	got := Bump(v, "a")
	if got["a"] != 2 {
		t.Errorf("Bump existing key: got %v, want counter 2", got)
	}
	if v["a"] != 1 {
		t.Fatalf("Bump mutated its input: %v", v)
	}

	got2 := Bump(v, "b")
	if got2["a"] != 1 || got2["b"] != 1 {
		t.Errorf("Bump new key: got %v, want {a:1, b:1}", got2)
	}

	gotNil := Bump(nil, "a")
	if gotNil["a"] != 1 {
		t.Errorf("Bump(nil, ...) = %v, want {a:1}", gotNil)
	}
}

// --- Randomized property checks -------------------------------------------

func randVector(r *rand.Rand, nodes []string) protocol.VersionVector {
	v := protocol.VersionVector{}
	for _, n := range nodes {
		if r.Intn(3) == 0 {
			continue // leave the key absent sometimes, to exercise implicit-zero
		}
		v[n] = uint64(r.Intn(5))
	}
	return v
}

func TestVectorProperties(t *testing.T) {
	nodes := []string{"n1", "n2", "n3", "n4"}
	r := rand.New(rand.NewSource(42))

	const iterations = 5000
	for i := 0; i < iterations; i++ {
		a := randVector(r, nodes)
		b := randVector(r, nodes)
		c := randVector(r, nodes)

		// Merge is commutative.
		if !Equal(Merge(a, b), Merge(b, a)) {
			t.Fatalf("Merge not commutative for a=%v b=%v", a, b)
		}

		// Merge is associative.
		lhs := Merge(Merge(a, b), c)
		rhs := Merge(a, Merge(b, c))
		if !Equal(lhs, rhs) {
			t.Fatalf("Merge not associative for a=%v b=%v c=%v: (a∘b)∘c=%v a∘(b∘c)=%v", a, b, c, lhs, rhs)
		}

		// Merge(a, b) dominates-or-equals both inputs.
		m := Merge(a, b)
		if !(Equal(m, a) || Dominates(m, a)) {
			t.Fatalf("Merge(a,b)=%v does not dominate-or-equal a=%v", m, a)
		}
		if !(Equal(m, b) || Dominates(m, b)) {
			t.Fatalf("Merge(a,b)=%v does not dominate-or-equal b=%v", m, b)
		}

		// Exactly one of Equal(a,b) / Dominates(a,b) / Dominates(b,a) /
		// Concurrent(a,b) holds.
		flags := 0
		if Equal(a, b) {
			flags++
		}
		if Dominates(a, b) {
			flags++
		}
		if Dominates(b, a) {
			flags++
		}
		if Concurrent(a, b) {
			flags++
		}
		if flags != 1 {
			t.Fatalf("expected exactly one relation to hold for a=%v b=%v, got %d: eq=%v a>b=%v b>a=%v conc=%v",
				a, b, flags, Equal(a, b), Dominates(a, b), Dominates(b, a), Concurrent(a, b))
		}
	}
}

func TestConflictRelPath(t *testing.T) {
	fixedTime := time.Date(2026, 8, 30, 14, 5, 9, 0, time.UTC)
	const node = "deadbeefcafef00d"

	cases := []struct {
		name    string
		relpath string
		want    string
	}{
		{
			name:    "simple extension",
			relpath: "notes.txt",
			want:    "notes.sync-conflict-20260830-140509-deadbeefcafef00d.txt",
		},
		{
			name:    "multi-part extension keeps only the last part appended",
			relpath: "notes.tar.gz",
			want:    "notes.tar.sync-conflict-20260830-140509-deadbeefcafef00d.gz",
		},
		{
			name:    "extensionless file",
			relpath: "README",
			want:    "README.sync-conflict-20260830-140509-deadbeefcafef00d",
		},
		{
			name:    "dotfile with no extension",
			relpath: ".bashrc",
			want:    ".bashrc.sync-conflict-20260830-140509-deadbeefcafef00d",
		},
		{
			name:    "dotfile with an extension",
			relpath: ".config.json",
			want:    ".config.sync-conflict-20260830-140509-deadbeefcafef00d.json",
		},
		{
			name:    "many dots",
			relpath: "a.b.c.d.txt",
			want:    "a.b.c.d.sync-conflict-20260830-140509-deadbeefcafef00d.txt",
		},
		{
			name:    "nested path keeps directory intact",
			relpath: "dir/sub/notes.txt",
			want:    "dir/sub/notes.sync-conflict-20260830-140509-deadbeefcafef00d.txt",
		},
		{
			name:    "already-conflicted file gets a fresh marker, not a second one",
			relpath: "notes.sync-conflict-20200101-000000-aaaaaaaaaaaaaaaa.txt",
			want:    "notes.sync-conflict-20260830-140509-deadbeefcafef00d.txt",
		},
		{
			name:    "already-conflicted multi-part extension file",
			relpath: "notes.tar.sync-conflict-20200101-000000-aaaaaaaaaaaaaaaa.gz",
			want:    "notes.tar.sync-conflict-20260830-140509-deadbeefcafef00d.gz",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := ConflictRelPath(c.relpath, node, fixedTime)
			if got != c.want {
				t.Errorf("ConflictRelPath(%q) = %q, want %q", c.relpath, got, c.want)
			}
		})
	}
}

// TestConflictRelPathBoundedGrowth ensures repeatedly conflicting the same
// file never grows the filename without bound: each round strips the prior
// marker before adding a new one, so the length converges instead of
// growing linearly with the number of conflicts.
func TestConflictRelPathBoundedGrowth(t *testing.T) {
	relpath := "notes.txt"
	baseLen := len(ConflictRelPath(relpath, "deadbeefcafef00d", time.Now()))

	for i := 0; i < 25; i++ {
		relpath = ConflictRelPath(relpath, "deadbeefcafef00d", time.Now())
		if len(relpath) > baseLen+1 { // allow for timestamp/second jitter, not accumulation
			t.Fatalf("round %d: relpath grew unboundedly: %q (len %d, base %d)", i, relpath, len(relpath), baseLen)
		}
	}
}

func TestConflictRelPathTimestampFormat(t *testing.T) {
	got := ConflictRelPath("f.txt", "abc", time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))
	if !strings.Contains(got, ".sync-conflict-20260102-030405-abc") {
		t.Errorf("unexpected timestamp formatting: %q", got)
	}
}

func TestConflictRelPathUsesUTC(t *testing.T) {
	// A time expressed in a non-UTC zone must format identically to its UTC
	// equivalent, so two nodes in different timezones agree on the name.
	loc := time.FixedZone("test", -5*60*60) // UTC-5
	local := time.Date(2026, 8, 30, 9, 5, 9, 0, loc)
	utc := local.UTC()

	got := ConflictRelPath("f.txt", "abc", local)
	want := ConflictRelPath("f.txt", "abc", utc)
	if got != want {
		t.Errorf("timezone-dependent output: local=%q utc=%q", got, want)
	}
}

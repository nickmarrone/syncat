package sync

import (
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

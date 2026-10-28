package core

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nickmarrone/syncat/internal/config"
)

// A folder that already has a .syncat/ syncat didn't make can't be shared,
// and one that gains one later goes on hold until it is moved away.
func TestForeignReservedDirBlocksShare(t *testing.T) {
	ctx := context.Background()
	node := newTestNode(t, "node")

	taken := t.TempDir()
	if err := os.Mkdir(filepath.Join(taken, ".syncat"), 0o755); err != nil {
		t.Fatal(err)
	}
	_, err := node.AddShare(taken, "taken", config.PermissionReadWrite, false)
	if kind, ok := ClassifyMutationError(err); !ok || kind != MutationConflict {
		t.Fatalf("AddShare over a foreign .syncat = %v, want a conflict", err)
	}

	dir := t.TempDir()
	writeNumberedFiles(t, dir, 3)
	shareID, err := node.AddShare(dir, "docs", config.PermissionReadWrite, false)
	if err != nil {
		t.Fatal(err)
	}
	// Someone replaces syncat's .syncat with their own.
	if err := os.RemoveAll(filepath.Join(dir, ".syncat")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, ".syncat"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := node.RescanShare(ctx, shareID); err == nil {
		t.Fatal("RescanShare succeeded with a foreign .syncat")
	}
	h := onlyHold(t, node)
	if h.Reason != HoldReservedDirTaken || h.Confirmable {
		t.Errorf("hold = %+v, want an unconfirmable reserved-dir-taken hold", h)
	}

	if err := os.Remove(filepath.Join(dir, ".syncat")); err != nil {
		t.Fatal(err)
	}
	if err := node.RescanShare(ctx, shareID); err != nil {
		t.Fatalf("RescanShare after moving it away: %v", err)
	}
	if holds := node.Holds(); len(holds) != 0 {
		t.Errorf("Holds() = %+v, want none once the folder is fixed", holds)
	}
}

// A peer's delete lands in this node's in-share trash as one event named
// after the peer, and undoing it brings the file back on both sides.
func TestPeerDeleteIsTrashedAndUndoPropagates(t *testing.T) {
	ctx := context.Background()
	nodeA := newTestNode(t, "nodeA")
	nodeB := newTestNode(t, "nodeB")

	shareDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(shareDir, "keep.txt"), []byte("precious"), 0o644); err != nil {
		t.Fatal(err)
	}
	shareID, err := nodeA.AddShare(shareDir, "docs", config.PermissionReadWrite, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := nodeA.AddPeer("nodeB", peerToken(t, nodeB)); err != nil {
		t.Fatal(err)
	}
	if _, err := nodeB.AddPeer("nodeA", peerToken(t, nodeA)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool { return peerConnected(nodeA) && peerConnected(nodeB) })
	localDir := t.TempDir()
	if err := nodeB.AddSubscription(nodeA.PeerKey(), shareID, localDir, config.ModeMirror); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool { return fileHas(filepath.Join(localDir, "keep.txt"), "precious") })

	if err := os.Remove(filepath.Join(shareDir, "keep.txt")); err != nil {
		t.Fatal(err)
	}
	if err := nodeA.RescanShare(ctx, shareID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool {
		_, err := os.Stat(filepath.Join(localDir, "keep.txt"))
		return errors.Is(err, os.ErrNotExist)
	})

	st, err := nodeB.ListTrash(ctx, "docs")
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Events) != 1 || st.Events[0].Label != "from nodeA" || len(st.Events[0].Items) != 1 || st.Events[0].Items[0].RelPath != "keep.txt" {
		t.Fatalf("B's trash = %+v, want one event from nodeA holding keep.txt", st.Events)
	}
	if !strings.HasPrefix(st.Events[0].ID, st.Events[0].At.Local().Format("2006-01-02")) {
		t.Errorf("event folder %q doesn't start with its date", st.Events[0].ID)
	}

	if _, err := nodeB.UndoTrash(ctx, "docs", st.Events[0].ID, nil); err != nil {
		t.Fatalf("UndoTrash: %v", err)
	}
	if !fileHas(filepath.Join(localDir, "keep.txt"), "precious") {
		t.Fatal("keep.txt not restored on B")
	}
	waitFor(t, 5*time.Second, func() bool { return fileHas(filepath.Join(shareDir, "keep.txt"), "precious") })
}

package core

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nickmarrone/syncat/internal/config"
	"github.com/nickmarrone/syncat/internal/hold"
)

// writeNumberedFiles writes count small files named f00.txt, f01.txt, ...
// into dir.
func writeNumberedFiles(t *testing.T, dir string, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("f%02d.txt", i)), []byte(fmt.Sprintf("file %d", i)), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func removeNumberedFiles(t *testing.T, dir string, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		if err := os.Remove(filepath.Join(dir, fmt.Sprintf("f%02d.txt", i))); err != nil {
			t.Fatal(err)
		}
	}
}

func liveIndexed(t *testing.T, n *Node, shareID string) int {
	t.Helper()
	rows, err := n.store.ListShare(context.Background(), shareID, false)
	if err != nil {
		t.Fatal(err)
	}
	return len(rows)
}

func onlyHold(t *testing.T, n *Node) HoldStatus {
	t.Helper()
	holds := n.Holds()
	if len(holds) != 1 {
		t.Fatalf("Holds() = %+v, want exactly one", holds)
	}
	return holds[0]
}

func TestLocalBulkDeleteIsHeldUntilFolderIsFixed(t *testing.T) {
	ctx := context.Background()
	node := newTestNode(t, "node")
	dir := t.TempDir()
	writeNumberedFiles(t, dir, 30)
	shareID, err := node.AddShare(dir, "docs", config.PermissionReadWrite, false)
	if err != nil {
		t.Fatal(err)
	}

	removeNumberedFiles(t, dir, 20)
	if err := node.RescanShare(ctx, shareID); err != nil {
		t.Fatalf("RescanShare: %v", err)
	}
	h := onlyHold(t, node)
	if h.Reason != hold.BulkDelete || h.Deletes != 20 || h.Live != 30 || h.PeerKey != "" {
		t.Errorf("hold = %+v, want a local bulk-delete of 20 of 30", h)
	}
	if got := liveIndexed(t, node, shareID); got != 30 {
		t.Errorf("index has %d live files while held, want all 30 kept", got)
	}

	// Putting some back leaves 5 deletes, under the thresholds, but still
	// held.
	writeNumberedFiles(t, dir, 15)
	if err := node.RescanShare(ctx, shareID); err != nil {
		t.Fatal(err)
	}
	if h := onlyHold(t, node); h.Reason != hold.BulkDelete || h.Deletes != 5 {
		t.Errorf("hold after a partial fix = %+v, want bulk-delete of 5", h)
	}

	// Putting them all back clears the hold by itself.
	writeNumberedFiles(t, dir, 30)
	if err := node.RescanShare(ctx, shareID); err != nil {
		t.Fatal(err)
	}
	if holds := node.Holds(); len(holds) != 0 {
		t.Errorf("Holds() after fixing the folder = %+v, want none", holds)
	}
}

func TestConfirmLetsLocalBulkDeleteThrough(t *testing.T) {
	ctx := context.Background()
	node := newTestNode(t, "node")
	dir := t.TempDir()
	writeNumberedFiles(t, dir, 30)
	shareID, err := node.AddShare(dir, "docs", config.PermissionReadWrite, false)
	if err != nil {
		t.Fatal(err)
	}
	removeNumberedFiles(t, dir, 20)
	if err := node.RescanShare(ctx, shareID); err != nil {
		t.Fatal(err)
	}
	onlyHold(t, node)

	if _, err := node.ConfirmHold(ctx, "docs", ""); err != nil {
		t.Fatalf("ConfirmHold: %v", err)
	}
	if holds := node.Holds(); len(holds) != 0 {
		t.Errorf("Holds() after confirming = %+v, want none", holds)
	}
	if got := liveIndexed(t, node, shareID); got != 10 {
		t.Errorf("index has %d live files after confirming, want 10", got)
	}
}

func TestMissingRootIsHeldAndCannotBeConfirmed(t *testing.T) {
	ctx := context.Background()
	node := newTestNode(t, "node")
	dir := filepath.Join(t.TempDir(), "share")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	writeNumberedFiles(t, dir, 5)
	shareID, err := node.AddShare(dir, "docs", config.PermissionReadWrite, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := node.RescanShare(ctx, shareID); err == nil {
		t.Fatal("RescanShare of a missing root succeeded, want an error")
	}
	h := onlyHold(t, node)
	if h.Reason != hold.RootUnavailable || h.Confirmable() {
		t.Errorf("hold = %+v, want an unconfirmable root-unavailable hold", h)
	}
	if _, err := node.ConfirmHold(ctx, "docs", ""); err == nil {
		t.Error("ConfirmHold on a missing root succeeded, want an error")
	}
	if got := liveIndexed(t, node, shareID); got != 5 {
		t.Errorf("index has %d live files, want all 5 kept", got)
	}
}

func TestHoldThresholdsAreConfigurable(t *testing.T) {
	ctx := context.Background()
	node := newTestNode(t, "node")
	dir := t.TempDir()
	writeNumberedFiles(t, dir, 10)
	shareID, err := node.AddShare(dir, "docs", config.PermissionReadWrite, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := node.SetSafety("docs", config.Safety{HoldDeletePercent: 30, HoldDeleteMin: 3}); err != nil {
		t.Fatalf("SetSafety: %v", err)
	}
	removeNumberedFiles(t, dir, 4)
	if err := node.RescanShare(ctx, shareID); err != nil {
		t.Fatal(err)
	}
	if h := onlyHold(t, node); h.Reason != hold.BulkDelete || h.Deletes != 4 {
		t.Errorf("hold = %+v, want bulk-delete of 4", h)
	}
}

// heldPeerDelete sets up nodeB subscribed to nodeA's 25-file share "docs"
// and has nodeA delete 22 of them, which nodeA lets through and nodeB holds.
// It returns once nodeB holds it.
func heldPeerDelete(t *testing.T) (nodeA, nodeB *Node, shareID, localDir string) {
	t.Helper()
	nodeA = newTestNode(t, "nodeA")
	nodeB = newTestNode(t, "nodeB")

	shareDir := t.TempDir()
	writeNumberedFiles(t, shareDir, 25)
	shareID, err := nodeA.AddShare(shareDir, "docs", config.PermissionReadWrite, false)
	if err != nil {
		t.Fatal(err)
	}
	// The offerer's own guard is loosened, so the delete leaves it.
	if _, err := nodeA.SetSafety("docs", config.Safety{HoldDeletePercent: 100, HoldDeleteMin: 1000}); err != nil {
		t.Fatal(err)
	}
	if _, err := nodeA.AddPeer("nodeB", peerToken(t, nodeB)); err != nil {
		t.Fatal(err)
	}
	if _, err := nodeB.AddPeer("nodeA", peerToken(t, nodeA)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool { return peerConnected(nodeA) && peerConnected(nodeB) })

	localDir = t.TempDir()
	if err := nodeB.AddSubscription(nodeA.PeerKey(), shareID, localDir, config.ModeMirror); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool { return countFiles(localDir) == 25 })

	removeNumberedFiles(t, shareDir, 22)
	if err := nodeA.RescanShare(context.Background(), shareID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool { return len(nodeB.Holds()) == 1 })
	return nodeA, nodeB, shareID, localDir
}

// A peer's bulk delete is held on the receiving side, even when the
// sending side's own guard let it through, and confirming applies it.
func TestPeerBulkDeleteIsHeldOnReceiver(t *testing.T) {
	nodeA, nodeB, _, localDir := heldPeerDelete(t)
	h := onlyHold(t, nodeB)
	if h.PeerKey != nodeA.PeerKey() || h.Reason != hold.BulkDelete || h.Deletes != 22 {
		t.Errorf("hold on B = %+v, want bulk-delete of 22 from nodeA", h)
	}
	if got := countFiles(localDir); got != 25 {
		t.Errorf("B has %d files while held, want all 25", got)
	}

	if _, err := nodeB.ConfirmHold(context.Background(), "docs", "nodeA"); err != nil {
		t.Fatalf("ConfirmHold: %v", err)
	}
	waitFor(t, 5*time.Second, func() bool { return countFiles(localDir) == 3 })
	if holds := nodeB.Holds(); len(holds) != 0 {
		t.Errorf("Holds() on B after confirming = %+v, want none", holds)
	}
}

// countFiles counts the regular files directly in dir, ignoring syncat's
// own hidden entries.
func countFiles(dir string) int {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return -1
	}
	n := 0
	for _, e := range entries {
		if e.Type().IsRegular() && e.Name()[0] != '.' {
			n++
		}
	}
	return n
}

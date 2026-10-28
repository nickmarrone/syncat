package core

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nickmarrone/syncat/internal/config"
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
	if h.Reason != HoldBulkDelete || h.Deletes != 20 || h.Live != 30 || h.PeerKey != "" {
		t.Errorf("hold = %+v, want a local bulk-delete of 20 of 30", h)
	}
	if got := liveIndexed(t, node, shareID); got != 30 {
		t.Errorf("index has %d live files while held, want all 30 kept", got)
	}

	// Putting the files back clears the hold by itself.
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

func TestSmallDeletesAreNotHeld(t *testing.T) {
	ctx := context.Background()
	node := newTestNode(t, "node")
	dir := t.TempDir()
	writeNumberedFiles(t, dir, 10)
	shareID, err := node.AddShare(dir, "docs", config.PermissionReadWrite, false)
	if err != nil {
		t.Fatal(err)
	}
	// 8 of 10 is over the percentage but under the default minimum of 20.
	removeNumberedFiles(t, dir, 8)
	if err := node.RescanShare(ctx, shareID); err != nil {
		t.Fatal(err)
	}
	if holds := node.Holds(); len(holds) != 0 {
		t.Errorf("Holds() = %+v, want none", holds)
	}
	if got := liveIndexed(t, node, shareID); got != 2 {
		t.Errorf("index has %d live files, want 2", got)
	}
}

func TestEverythingVanishingIsHeldInASmallShare(t *testing.T) {
	ctx := context.Background()
	node := newTestNode(t, "node")
	dir := t.TempDir()
	writeNumberedFiles(t, dir, 3)
	shareID, err := node.AddShare(dir, "docs", config.PermissionReadWrite, false)
	if err != nil {
		t.Fatal(err)
	}
	removeNumberedFiles(t, dir, 3)
	if err := node.RescanShare(ctx, shareID); err != nil {
		t.Fatal(err)
	}
	if h := onlyHold(t, node); h.Reason != HoldEverythingVanished {
		t.Errorf("reason = %q, want %q", h.Reason, HoldEverythingVanished)
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
	if h.Reason != HoldRootUnavailable || h.Confirmable {
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
	if h := onlyHold(t, node); h.Reason != HoldBulkDelete || h.Deletes != 4 {
		t.Errorf("hold = %+v, want bulk-delete of 4", h)
	}
}

func TestHoldSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	node := newTestNode(t, "node")
	dir := t.TempDir()
	writeNumberedFiles(t, dir, 30)
	shareID, err := node.AddShare(dir, "docs", config.PermissionReadWrite, false)
	if err != nil {
		t.Fatal(err)
	}
	removeNumberedFiles(t, dir, 25)
	if err := node.RescanShare(ctx, shareID); err != nil {
		t.Fatal(err)
	}
	onlyHold(t, node)

	reloaded, err := loadHolds(ctx, node.store)
	if err != nil {
		t.Fatal(err)
	}
	if h, ok := reloaded[holdKey{shareID, ""}]; !ok || h.Reason != HoldBulkDelete || h.Deletes != 25 {
		t.Errorf("hold read back from the store = %+v (found %v), want the bulk-delete", h, ok)
	}
}

// A peer's bulk delete is held on the receiving side, even when the
// sending side's own guard let it through, and confirming applies it.
func TestPeerBulkDeleteIsHeldOnReceiver(t *testing.T) {
	ctx := context.Background()
	nodeA := newTestNode(t, "nodeA")
	nodeB := newTestNode(t, "nodeB")

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

	localDir := t.TempDir()
	if err := nodeB.AddSubscription(nodeA.PeerKey(), shareID, localDir, config.ModeMirror); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool { return countFiles(localDir) == 25 })

	removeNumberedFiles(t, shareDir, 22)
	if err := nodeA.RescanShare(ctx, shareID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool { return len(nodeB.Holds()) == 1 })
	h := onlyHold(t, nodeB)
	if h.PeerKey != nodeA.PeerKey() || h.Reason != HoldBulkDelete || h.Deletes != 22 {
		t.Errorf("hold on B = %+v, want bulk-delete of 22 from nodeA", h)
	}
	if got := countFiles(localDir); got != 25 {
		t.Errorf("B has %d files while held, want all 25", got)
	}

	if _, err := nodeB.ConfirmHold(ctx, "docs", "nodeA"); err != nil {
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

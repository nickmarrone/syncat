package core

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nickmarrone/syncat/internal/config"
)

// resubscribePair connects two nodes, has A offer a share of n numbered
// files, and has B subscribe into a folder and receive them all.
func resubscribePair(t *testing.T, n int) (nodeA, nodeB *Node, shareID, shareDir, firstDir string) {
	t.Helper()
	nodeA = newTestNode(t, "nodeA")
	nodeB = newTestNode(t, "nodeB")
	shareDir = t.TempDir()
	writeNumberedFiles(t, shareDir, n)
	var err error
	shareID, err = nodeA.AddShare(shareDir, "docs", config.PermissionReadWrite, false)
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

	firstDir = t.TempDir()
	if err := nodeB.AddSubscription(nodeA.PeerKey(), shareID, firstDir, config.ModeMirror); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool { return countFiles(firstDir) == n })
	return
}

// Issue #44: removing a subscription has to forget its index state, or a
// re-subscription into a new, empty folder reads as the old copy with every
// file deleted, and is held with the deletes bound for the offerer.
func TestResubscribeIntoFreshFolderStartsClean(t *testing.T) {
	const n = 25
	nodeA, nodeB, shareID, shareDir, firstDir := resubscribePair(t, n)

	if err := nodeB.RemoveSubscription(nodeA.PeerKey(), shareID); err != nil {
		t.Fatal(err)
	}
	newDir := filepath.Join(t.TempDir(), "again")
	if err := nodeB.AddSubscription(nodeA.PeerKey(), shareID, newDir, config.ModeMirror); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, func() bool { return countFiles(newDir) == n || len(nodeB.Holds()) > 0 })
	if holds := nodeB.Holds(); len(holds) != 0 {
		t.Fatalf("re-subscribing into a fresh folder put the share on hold: %+v", holds)
	}
	if got := countFiles(newDir); got != n {
		t.Fatalf("fresh folder has %d files, want %d", got, n)
	}
	if got := countFiles(shareDir); got != n {
		t.Errorf("offerer has %d files, want %d", got, n)
	}
	if got := countFiles(firstDir); got != n {
		t.Errorf("old folder has %d files, want %d", got, n)
	}

	// The new copy is live in both directions.
	if err := os.WriteFile(filepath.Join(newDir, "new.txt"), []byte("hi"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, func() bool {
		b, err := os.ReadFile(filepath.Join(shareDir, "new.txt"))
		return err == nil && string(b) == "hi"
	})
	if got := countFiles(shareDir); got != n+1 {
		t.Errorf("offerer has %d files, want %d", got, n+1)
	}
}

// Re-subscribing into the old folder, which still has its files, must not
// lose or duplicate anything or send deletes to the offerer.
func TestResubscribeIntoOldFolderKeepsEverything(t *testing.T) {
	const n = 25
	nodeA, nodeB, shareID, shareDir, firstDir := resubscribePair(t, n)

	if err := nodeB.RemoveSubscription(nodeA.PeerKey(), shareID); err != nil {
		t.Fatal(err)
	}
	if err := nodeB.AddSubscription(nodeA.PeerKey(), shareID, firstDir, config.ModeMirror); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(shareDir, "later.txt"), []byte("later"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, func() bool {
		_, err := os.Stat(filepath.Join(firstDir, "later.txt"))
		return err == nil
	})
	if holds := nodeB.Holds(); len(holds) != 0 {
		t.Fatalf("holds after re-subscribing into the old folder: %+v", holds)
	}
	for _, dir := range []string{shareDir, firstDir} {
		if got := countFiles(dir); got != n+1 {
			t.Errorf("%s has %d files, want %d (no losses, no conflict copies)", dir, got, n+1)
		}
	}
}

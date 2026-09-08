package core

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nickmarrone/syncat/internal/config"
	"github.com/nickmarrone/syncat/internal/index"
	syncsvc "github.com/nickmarrone/syncat/internal/sync"
)

// indexHas reports whether shareID's index holds a live row for relpath.
func indexHas(t *testing.T, n *Node, shareID, relpath string) bool {
	t.Helper()
	rows, err := n.store.ListShare(context.Background(), shareID, false)
	if err != nil {
		t.Fatalf("ListShare: %v", err)
	}
	for _, r := range rows {
		if r.RelPath == relpath {
			return true
		}
	}
	return false
}

func writeShareFile(t *testing.T, dir, relpath, content string) {
	t.Helper()
	full := filepath.Join(dir, filepath.FromSlash(relpath))
	if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", relpath, err)
	}
}

// TestSyncatignoreExcludesFromIndex is the basic contract: a path matching
// a share-root .syncatignore never enters the index, so it can never reach
// a peer.
func TestSyncatignoreExcludesFromIndex(t *testing.T) {
	n := newTestNode(t, "node")
	shareDir := t.TempDir()
	writeShareFile(t, shareDir, index.IgnoreFileName, "*.log\nbuild/\n")
	writeShareFile(t, shareDir, "notes.txt", "keep")
	writeShareFile(t, shareDir, "debug.log", "noisy")
	writeShareFile(t, shareDir, "build/out.o", "artifact")

	shareID, err := n.AddShare(shareDir, "docs", config.PermissionReadWrite, false)
	if err != nil {
		t.Fatalf("AddShare: %v", err)
	}
	if err := n.rescanShare(context.Background(), shareID); err != nil {
		t.Fatalf("rescanShare: %v", err)
	}

	if !indexHas(t, n, shareID, "notes.txt") {
		t.Error("notes.txt should be indexed")
	}
	for _, p := range []string{"debug.log", "build/out.o", "build", index.IgnoreFileName} {
		if indexHas(t, n, shareID, p) {
			t.Errorf("%s should not be indexed", p)
		}
	}
}

// TestSyncatignoreReloadsBetweenScans pins requirement 6: editing the
// ignore file takes effect on the next scan, with no restart and no
// re-adding of the share.
func TestSyncatignoreReloadsBetweenScans(t *testing.T) {
	n := newTestNode(t, "node")
	ctx := context.Background()
	shareDir := t.TempDir()
	writeShareFile(t, shareDir, "debug.log", "noisy")

	shareID, err := n.AddShare(shareDir, "docs", config.PermissionReadWrite, false)
	if err != nil {
		t.Fatalf("AddShare: %v", err)
	}
	if err := n.rescanShare(ctx, shareID); err != nil {
		t.Fatalf("first rescan: %v", err)
	}
	if !indexHas(t, n, shareID, "debug.log") {
		t.Fatal("debug.log should be indexed before the rule exists")
	}

	// The file's mtime has to move for the reload stamp to notice it, and
	// a freshly-created file can share a timestamp with the scan above.
	writeShareFile(t, shareDir, index.IgnoreFileName, "*.log\n")
	future := time.Now().Add(2 * time.Second)
	if err := os.Chtimes(filepath.Join(shareDir, index.IgnoreFileName), future, future); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	if err := n.rescanShare(ctx, shareID); err != nil {
		t.Fatalf("second rescan: %v", err)
	}
	if indexHas(t, n, shareID, "debug.log") {
		t.Error("debug.log should have been dropped after the rule was added")
	}
}

// TestSyncatignoreNewlyIgnoredLeavesNoTombstone is the core-level guard on
// the data-loss path: dropping a now-ignored row must not journal a
// tombstone, because a tombstone propagates and deletes the peer's copy.
func TestSyncatignoreNewlyIgnoredLeavesNoTombstone(t *testing.T) {
	n := newTestNode(t, "node")
	ctx := context.Background()
	shareDir := t.TempDir()
	writeShareFile(t, shareDir, "debug.log", "noisy")

	shareID, err := n.AddShare(shareDir, "docs", config.PermissionReadWrite, false)
	if err != nil {
		t.Fatalf("AddShare: %v", err)
	}
	if err := n.rescanShare(ctx, shareID); err != nil {
		t.Fatalf("first rescan: %v", err)
	}

	writeShareFile(t, shareDir, index.IgnoreFileName, "*.log\n")
	future := time.Now().Add(2 * time.Second)
	if err := os.Chtimes(filepath.Join(shareDir, index.IgnoreFileName), future, future); err != nil {
		t.Fatalf("chtimes: %v", err)
	}
	if err := n.rescanShare(ctx, shareID); err != nil {
		t.Fatalf("second rescan: %v", err)
	}

	rows, err := n.store.ListShare(ctx, shareID, true)
	if err != nil {
		t.Fatalf("ListShare: %v", err)
	}
	for _, r := range rows {
		if r.RelPath == "debug.log" {
			t.Errorf("debug.log left a row behind (Deleted=%v); a tombstone here deletes it on every peer", r.Deleted)
		}
	}
}

// TestSyncatignoreUnreadableKeepsPreviousRules: the safe direction for an
// ignore file we cannot read is to keep excluding what the user excluded,
// never to silently start indexing and publishing all of it.
func TestSyncatignoreUnreadableKeepsPreviousRules(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root: an unreadable file is still readable")
	}
	n := newTestNode(t, "node")
	ctx := context.Background()
	shareDir := t.TempDir()
	writeShareFile(t, shareDir, "debug.log", "noisy")
	writeShareFile(t, shareDir, index.IgnoreFileName, "*.log\n")

	shareID, err := n.AddShare(shareDir, "docs", config.PermissionReadWrite, false)
	if err != nil {
		t.Fatalf("AddShare: %v", err)
	}
	if err := n.rescanShare(ctx, shareID); err != nil {
		t.Fatalf("first rescan: %v", err)
	}
	if indexHas(t, n, shareID, "debug.log") {
		t.Fatal("debug.log should be ignored to begin with")
	}

	ignorePath := filepath.Join(shareDir, index.IgnoreFileName)
	if err := os.Chmod(ignorePath, 0o000); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(ignorePath, 0o644) })
	future := time.Now().Add(2 * time.Second)
	if err := os.Chtimes(ignorePath, future, future); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	if err := n.rescanShare(ctx, shareID); err != nil {
		t.Fatalf("second rescan: %v", err)
	}
	if indexHas(t, n, shareID, "debug.log") {
		t.Error("an unreadable .syncatignore caused previously-excluded content to be indexed")
	}
}

// TestSyncatignoreUnignoreDominatesPeerVersion is the regression for the
// bug the end-to-end scenario caught. Removing a rule has to produce a
// version vector that still beats the copy the peer kept while the file
// was ignored. If the row's vector were discarded when the rule was added,
// the resurrected file would restart at the peer's own version, the two
// sides would compare equal with different contents, and they would never
// converge again.
func TestSyncatignoreUnignoreDominatesPeerVersion(t *testing.T) {
	n := newTestNode(t, "node")
	ctx := context.Background()
	shareDir := t.TempDir()
	writeShareFile(t, shareDir, "shared.txt", "v1")

	shareID, err := n.AddShare(shareDir, "docs", config.PermissionReadWrite, false)
	if err != nil {
		t.Fatalf("AddShare: %v", err)
	}
	if err := n.rescanShare(ctx, shareID); err != nil {
		t.Fatalf("first rescan: %v", err)
	}
	before, err := n.store.GetFile(ctx, shareID, "shared.txt")
	if err != nil {
		t.Fatalf("GetFile: %v", err)
	}

	bump := func(rel string) {
		t.Helper()
		future := time.Now().Add(2 * time.Second)
		if err := os.Chtimes(filepath.Join(shareDir, rel), future, future); err != nil {
			t.Fatalf("chtimes %s: %v", rel, err)
		}
	}

	// Ignore it, then un-ignore it.
	writeShareFile(t, shareDir, index.IgnoreFileName, "shared.txt\n")
	bump(index.IgnoreFileName)
	if err := n.rescanShare(ctx, shareID); err != nil {
		t.Fatalf("rescan with rule: %v", err)
	}
	if _, err := n.store.GetFile(ctx, shareID, "shared.txt"); err == nil {
		t.Fatal("shared.txt should be excluded while the rule is present")
	}

	writeShareFile(t, shareDir, index.IgnoreFileName, "# no rules now\n")
	bump(index.IgnoreFileName)
	if err := n.rescanShare(ctx, shareID); err != nil {
		t.Fatalf("rescan without rule: %v", err)
	}

	after, err := n.store.GetFile(ctx, shareID, "shared.txt")
	if err != nil {
		t.Fatalf("shared.txt did not come back after the rule was removed: %v", err)
	}
	if !syncsvc.Dominates(after.Version, before.Version) {
		t.Errorf("resurrected version %v does not dominate the pre-ignore version %v; "+
			"a peer holding the old copy would compare equal and never converge",
			after.Version, before.Version)
	}
}

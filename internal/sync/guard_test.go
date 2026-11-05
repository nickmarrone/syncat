package sync

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/nickmarrone/syncat/internal/index"
)

// ShareConfig.Guard is how internal/hold stops a pass. A pass it refuses
// applies nothing, and the peer's rows stay recorded, so the same change
// applies once a later pass is allowed, without the peer sending it again.
func TestIntegration_GuardRefusalHoldsThePassUntilAllowed(t *testing.T) {
	a := newTestNode(t, "aaaaaaaaaaaaaaaa")
	b := newTestNode(t, "bbbbbbbbbbbbbbbb")

	writeFile(t, a.root, "keep.txt", "precious")
	indexFile(t, a.store, a.id, "keep.txt", a.root)
	sa, sb := connectSessions(t, a, b, Direction{}, Direction{})
	mustSync(t, sa, testShareID)
	waitForFile(t, integrationWaitTimeout, b.store, testShareID, b.root, "keep.txt", "precious")

	var allow, askedToDelete atomic.Bool
	sb.AddShare(ShareConfig{ShareID: testShareID, Root: b.root, Guard: func(local []index.FileRow, deleting []string) bool {
		if slices.Contains(deleting, "keep.txt") && slices.ContainsFunc(local, func(r index.FileRow) bool { return r.RelPath == "keep.txt" && !r.Deleted }) {
			askedToDelete.Store(true)
		}
		return allow.Load()
	}})

	if err := os.Remove(filepath.Join(a.root, "keep.txt")); err != nil {
		t.Fatal(err)
	}
	indexDelete(t, a.store, a.id, "keep.txt")
	mustSync(t, sa, testShareID)
	waitFor(t, integrationWaitTimeout, askedToDelete.Load)
	if _, err := os.Stat(filepath.Join(b.root, "keep.txt")); err != nil {
		t.Fatalf("a refused pass deleted the file: %v", err)
	}

	allow.Store(true)
	if err := sb.ReconcileShare(context.Background(), testShareID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, integrationWaitTimeout, func() bool {
		_, err := os.Stat(filepath.Join(b.root, "keep.txt"))
		return errors.Is(err, fs.ErrNotExist)
	})
}

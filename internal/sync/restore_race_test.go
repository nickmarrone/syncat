package sync

import (
	"context"
	"os"
	"path/filepath"
	stdsync "sync"
	"testing"
	"time"
)

// TestRestoreDuringApplyIsNotOverwrittenByTombstone pins #62: a restore
// that runs after a pass has trashed a file but before it has written the
// tombstone row must not be undone by that row. The restore waits for the
// pass's write, then bumps over the tombstone, so A's index ends up holding
// the restored file and the restore reaches B.
func TestRestoreDuringApplyIsNotOverwrittenByTombstone(t *testing.T) {
	a := newTestNode(t, "aaaaaaaaaaaaaaaa")
	b := newTestNode(t, "bbbbbbbbbbbbbbbb")

	writeFile(t, a.root, "keep.txt", "original content")
	indexFile(t, a.store, a.id, "keep.txt", a.root)

	inGap := make(chan struct{})
	release := make(chan struct{})
	releaseOnce := stdsync.OnceFunc(func() { close(release) })
	var gapOnce stdsync.Once
	tr := NewTrash(nil)
	optsA := SessionOptions{
		Trash: tr,
		testAfterApply: func(act Action) {
			if act.Kind != ActionDelete || act.RelPath != "keep.txt" {
				return
			}
			gapOnce.Do(func() {
				close(inGap)
				<-release
			})
		},
	}
	sa, sb := connectSessionsWithOptions(t, a, b, Direction{}, Direction{}, optsA, SessionOptions{})
	// Registered after the sessions' own cleanup so it runs first: a failure
	// with the pass still held would otherwise deadlock their teardown.
	t.Cleanup(releaseOnce)
	mustSync(t, sa, testShareID)
	mustSync(t, sb, testShareID)
	waitForFile(t, integrationWaitTimeout, b.store, testShareID, b.root, "keep.txt", "original content")

	// B deletes; A's pass trashes its copy and stops before the tombstone.
	if err := os.Remove(filepath.Join(b.root, "keep.txt")); err != nil {
		t.Fatal(err)
	}
	indexDelete(t, b.store, b.id, "keep.txt")
	mustSync(t, sb, testShareID)
	select {
	case <-inGap:
	case <-time.After(integrationWaitTimeout):
		t.Fatal("A's pass never applied the delete")
	}
	if _, err := os.Stat(filepath.Join(a.root, "keep.txt")); !os.IsNotExist(err) {
		t.Fatalf("keep.txt should be gone from A's disk in the gap, stat err = %v", err)
	}
	events, err := tr.List(a.root)
	if err != nil || len(events) != 1 {
		t.Fatalf("trash List: %v (events %+v)", err, events)
	}

	// The undo, issued in the gap.
	type result struct {
		n   int
		err error
	}
	done := make(chan result, 1)
	go func() {
		rows, err := tr.Restore(context.Background(), a.store, a.id, testShareID, a.root, events[0].ID, nil)
		done <- result{len(rows), err}
	}()
	select {
	case r := <-done:
		t.Fatalf("Restore finished (%d rows, %v) while the pass was between apply and index write", r.n, r.err)
	case <-time.After(200 * time.Millisecond):
	}
	releaseOnce()
	select {
	case r := <-done:
		if r.err != nil || r.n != 1 {
			t.Fatalf("Restore: %v (%d rows)", r.err, r.n)
		}
	case <-time.After(integrationWaitTimeout):
		t.Fatal("Restore never finished")
	}

	row, err := a.store.GetFile(context.Background(), testShareID, "keep.txt")
	if err != nil {
		t.Fatalf("get A's row: %v", err)
	}
	if row.Deleted {
		t.Fatal("A's index holds the pass's tombstone over the restored file")
	}
	data, err := os.ReadFile(filepath.Join(a.root, "keep.txt"))
	if err != nil || string(data) != "original content" {
		t.Fatalf("restored file on A: %q, %v", data, err)
	}

	// The restore reaches B as a new change.
	mustSync(t, sa, testShareID)
	waitForFile(t, integrationWaitTimeout, b.store, testShareID, b.root, "keep.txt", "original content")
}

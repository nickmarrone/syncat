package core

import (
	"bytes"
	"context"
	"crypto/sha256"
	"sync"
	"testing"
	"time"

	"github.com/nickmarrone/syncat/internal/config"
	"github.com/nickmarrone/syncat/internal/index"
)

// quietShareWatcher swaps shareID's fsnotify watcher for one on an empty
// directory, so the test alone decides when the share is rescanned.
func quietShareWatcher(t *testing.T, n *Node, shareID string) {
	t.Helper()
	n.sharesMu.Lock()
	sw := n.shareWatches[shareID]
	n.sharesMu.Unlock()
	if sw == nil {
		t.Fatalf("share %s has no watch", shareID)
	}
	if err := sw.watcher.Close(); err != nil {
		t.Fatalf("close watcher: %v", err)
	}
	idle, err := index.NewWatcher(index.WatcherOptions{Root: t.TempDir(), RescanInterval: time.Hour})
	if err != nil {
		t.Fatalf("idle watcher: %v", err)
	}
	sw.watcher = idle
}

// TestBackgroundRescansAreSerializedAndCoalesced pins #61: a trigger that
// lands while a scan of the same share is running does not start a second
// scan alongside it. However many arrive, they cost exactly one follow-up,
// run after the first scan has applied, and the index then matches disk.
func TestBackgroundRescansAreSerializedAndCoalesced(t *testing.T) {
	n := newTestNode(t, "node")
	shareDir := t.TempDir()
	writeShareFile(t, shareDir, "notes.txt", "v1")

	shareID, err := n.AddShare(shareDir, "docs", config.PermissionReadWrite, false)
	if err != nil {
		t.Fatalf("AddShare: %v", err)
	}
	quietShareWatcher(t, n, shareID)

	var mu sync.Mutex
	scans := 0
	entered := make(chan struct{}, 16)
	release := make(chan struct{})
	releaseScan := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseScan) // before the node closes, or a failure deadlocks Close
	n.testBeforeScan = func(id string) {
		if id != shareID {
			return
		}
		mu.Lock()
		scans++
		first := scans == 1
		mu.Unlock()
		entered <- struct{}{}
		if first {
			<-release
		}
	}
	scanCount := func() int {
		mu.Lock()
		defer mu.Unlock()
		return scans
	}

	writeShareFile(t, shareDir, "notes.txt", "v2")
	n.rescanShareAsync(shareID)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first scan never started")
	}

	// Edits and triggers that land while the first scan is held open.
	writeShareFile(t, shareDir, "notes.txt", "v3-final")
	for range 3 {
		n.rescanShareAsync(shareID)
	}
	select {
	case <-entered:
		t.Fatal("a second scan started while the first was still running")
	case <-time.After(200 * time.Millisecond):
	}

	releaseScan()
	sc := n.shareScanFor(shareID)
	waitFor(t, 5*time.Second, func() bool {
		sc.stateMu.Lock()
		defer sc.stateMu.Unlock()
		return !sc.running
	})
	if got := scanCount(); got != 2 {
		t.Fatalf("scans = %d, want 2 (the held one plus one coalesced follow-up)", got)
	}

	rows, err := n.store.ListShare(context.Background(), shareID, false)
	if err != nil {
		t.Fatalf("ListShare: %v", err)
	}
	want := sha256.Sum256([]byte("v3-final"))
	for _, r := range rows {
		if r.RelPath == "notes.txt" {
			if !bytes.Equal(r.SHA256, want[:]) {
				t.Fatalf("index hash for notes.txt does not match disk")
			}
			return
		}
	}
	t.Fatal("notes.txt missing from the index")
}

// TestSynchronousRescanWaitsForInFlightScan: rescanShare called while a
// background scan is running waits for it rather than overlapping it.
func TestSynchronousRescanWaitsForInFlightScan(t *testing.T) {
	n := newTestNode(t, "node")
	shareDir := t.TempDir()
	writeShareFile(t, shareDir, "a.txt", "a")

	shareID, err := n.AddShare(shareDir, "docs", config.PermissionReadWrite, false)
	if err != nil {
		t.Fatalf("AddShare: %v", err)
	}
	quietShareWatcher(t, n, shareID)

	var mu sync.Mutex
	inFlight, maxInFlight := 0, 0
	held := make(chan struct{})
	release := make(chan struct{})
	releaseScan := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseScan) // before the node closes, or a failure deadlocks Close
	var once sync.Once
	n.testBeforeScan = func(id string) {
		mu.Lock()
		inFlight++
		maxInFlight = max(maxInFlight, inFlight)
		mu.Unlock()
		once.Do(func() {
			close(held)
			<-release
		})
		mu.Lock()
		inFlight--
		mu.Unlock()
	}

	n.rescanShareAsync(shareID)
	<-held
	done := make(chan error, 1)
	go func() { done <- n.rescanShare(context.Background(), shareID) }()
	select {
	case err := <-done:
		t.Fatalf("rescanShare returned (%v) while another scan held the share", err)
	case <-time.After(200 * time.Millisecond):
	}
	releaseScan()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("rescanShare: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("rescanShare never finished")
	}
	mu.Lock()
	defer mu.Unlock()
	if maxInFlight != 1 {
		t.Fatalf("max concurrent scans = %d, want 1", maxInFlight)
	}
}

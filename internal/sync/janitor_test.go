package sync

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// fakeJanitorClock is a manually-advanced JanitorClock: Now() returns
// whatever it's set to, and After(d) delivers on a channel only when the
// test explicitly Fires it — so the janitor's background loop never
// depends on a real timer or a real sleep.
type fakeJanitorClock struct {
	now   time.Time
	fired chan chan time.Time
}

func newFakeJanitorClock(now time.Time) *fakeJanitorClock {
	return &fakeJanitorClock{now: now, fired: make(chan chan time.Time, 16)}
}

func (c *fakeJanitorClock) Now() time.Time { return c.now }

func (c *fakeJanitorClock) After(time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	c.fired <- ch
	return ch
}

// tick advances the clock and fires the most recently requested timer
// (blocking until the loop has actually asked for one), synchronizing the
// test with the background goroutine without any sleep.
func (c *fakeJanitorClock) tick(t *testing.T, advance time.Duration) {
	t.Helper()
	c.now = c.now.Add(advance)
	select {
	case ch := <-c.fired:
		ch <- c.now
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for janitor loop to request a timer")
	}
}

// trashFileAt seeds a trash entry directly on disk, named exactly the way
// Trash.Put would (<relpath>.<unix-ts>), but at an explicit timestamp
// rather than whatever tr's configured clock reports — so a single Trash
// can be seeded with entries of several different ages for the janitor
// tests below, independent of Put's own clock.
func trashFileAt(t *testing.T, tr *Trash, shareID, relpath string, ts int64) {
	t.Helper()
	full := filepath.Join(tr.root, shareID, filepath.FromSlash(relpath)) + "." + itoa(ts)
	if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte("trashed"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// 1. Sweep purges entries older than retention and keeps fresher ones, on
// a fake "now".
func TestJanitor_SweepPurgesOldKeepsFresh(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	tr := NewTrash(filepath.Join(dir, "trash"), nil)

	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	retention := 30 * 24 * time.Hour

	oldTS := now.Add(-31 * 24 * time.Hour).Unix()  // older than retention: purge
	freshTS := now.Add(-1 * 24 * time.Hour).Unix() // within retention: keep
	edgeTS := now.Add(-30 * 24 * time.Hour).Unix() // exactly at retention: keep (Before(cutoff) is strict)

	trashFileAt(t, tr, trashTestShareID, "old.txt", oldTS)
	trashFileAt(t, tr, trashTestShareID, "fresh.txt", freshTS)
	trashFileAt(t, tr, trashTestShareID, "edge.txt", edgeTS)

	clock := newFakeJanitorClock(now)
	j := NewJanitor(tr, retention, clock, time.Hour, nil)

	purged, err := j.Sweep(ctx)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if purged != 1 {
		t.Fatalf("purged = %d, want 1", purged)
	}

	if _, err := os.Stat(filepath.Join(tr.root, trashTestShareID, "old.txt."+itoa(oldTS))); !os.IsNotExist(err) {
		t.Fatalf("old entry should be purged, stat err = %v", err)
	}
	if _, err := os.Stat(filepath.Join(tr.root, trashTestShareID, "fresh.txt."+itoa(freshTS))); err != nil {
		t.Fatalf("fresh entry should survive: %v", err)
	}
	if _, err := os.Stat(filepath.Join(tr.root, trashTestShareID, "edge.txt."+itoa(edgeTS))); err != nil {
		t.Fatalf("edge entry should survive: %v", err)
	}
}

// 2. A missing trash root is tolerated (no error, nothing purged).
func TestJanitor_MissingTrashDirTolerated(t *testing.T) {
	dir := t.TempDir()
	tr := NewTrash(filepath.Join(dir, "does-not-exist"), nil)
	j := NewJanitor(tr, 30*24*time.Hour, nil, time.Hour, nil)

	purged, err := j.Sweep(context.Background())
	if err != nil {
		t.Fatalf("Sweep on missing trash dir: %v", err)
	}
	if purged != 0 {
		t.Fatalf("purged = %d, want 0", purged)
	}
}

// 3. Sweep never touches anything outside the trash root: a sibling
// directory with an identically-named, identically-aged file is left
// alone.
func TestJanitor_NeverTouchesOutsideTrashRoot(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	tr := NewTrash(filepath.Join(dir, "trash"), nil)

	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	oldTS := now.Add(-60 * 24 * time.Hour).Unix()
	trashFileAt(t, tr, trashTestShareID, "old.txt", oldTS)

	// A sibling, outside the trash root, that happens to look exactly
	// like an expired trash entry.
	sibling := filepath.Join(dir, "not-trash", trashTestShareID, "old.txt."+itoa(oldTS))
	if err := os.MkdirAll(filepath.Dir(sibling), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(sibling, []byte("must survive"), 0o644); err != nil {
		t.Fatal(err)
	}

	clock := newFakeJanitorClock(now)
	j := NewJanitor(tr, 30*24*time.Hour, clock, time.Hour, nil)
	if _, err := j.Sweep(ctx); err != nil {
		t.Fatalf("Sweep: %v", err)
	}

	if _, err := os.Stat(sibling); err != nil {
		t.Fatalf("sibling outside trash root must survive: %v", err)
	}
}

// 4. The background loop, driven entirely by a fake clock, sweeps on
// schedule with no real sleeping, and Close stops it cleanly with no
// goroutine leak (verified by Close returning promptly and being safe to
// call once the loop has exited).
func TestJanitor_BackgroundLoopRunsOnFakeClockAndStopsCleanly(t *testing.T) {
	dir := t.TempDir()
	tr := NewTrash(filepath.Join(dir, "trash"), nil)

	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	oldTS := now.Add(-40 * 24 * time.Hour).Unix()
	trashFileAt(t, tr, trashTestShareID, "old.txt", oldTS)

	clock := newFakeJanitorClock(now)
	swept := make(chan int, 4)
	j := NewJanitor(tr, 30*24*time.Hour, clock, time.Hour, func(purged int, err error) {
		if err != nil {
			t.Errorf("unexpected sweep error: %v", err)
		}
		swept <- purged
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	j.Start(ctx)

	clock.tick(t, time.Hour) // fires the first sweep

	select {
	case n := <-swept:
		if n != 1 {
			t.Fatalf("first sweep purged = %d, want 1", n)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for first sweep")
	}

	clock.tick(t, time.Hour) // second sweep: nothing left to purge

	select {
	case n := <-swept:
		if n != 0 {
			t.Fatalf("second sweep purged = %d, want 0", n)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for second sweep")
	}

	if err := j.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Close is documented safe to call more than once.
	if err := j.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

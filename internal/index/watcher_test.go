package index

import (
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

// fakeClock is a manually-driven Clock for deterministic debounce tests:
// nothing here ever sleeps on a wall clock to produce a debounce flush.
// Mirrors internal/protocol's keepalive_test.go fakeClock (see its doc
// comment for the full rationale); duplicated rather than imported
// because internal/index defines its own local Clock interface for the
// same layering reason protocol.Clock is local to internal/protocol.
type fakeClock struct {
	mu      sync.Mutex
	now     time.Time
	waiters []fakeWaiter
	notify  chan struct{}
}

type fakeWaiter struct {
	deadline time.Time
	ch       chan time.Time
}

func newFakeClock(start time.Time) *fakeClock {
	return &fakeClock{now: start, notify: make(chan struct{})}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) After(d time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	c.mu.Lock()
	deadline := c.now.Add(d)
	if d <= 0 {
		c.mu.Unlock()
		ch <- deadline
		return ch
	}
	c.waiters = append(c.waiters, fakeWaiter{deadline: deadline, ch: ch})
	old := c.notify
	c.notify = make(chan struct{})
	c.mu.Unlock()
	close(old)
	return ch
}

// Advance moves the fake clock forward by d, firing (synchronously) every
// waiter whose deadline has now been reached.
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	now := c.now
	var fired []fakeWaiter
	remaining := c.waiters[:0]
	for _, w := range c.waiters {
		if !w.deadline.After(now) {
			fired = append(fired, w)
		} else {
			remaining = append(remaining, w)
		}
	}
	c.waiters = remaining
	c.mu.Unlock()

	for _, w := range fired {
		w.ch <- now
	}
}

// waiterCount reports how many goroutines are currently parked on
// clock.After.
func (c *fakeClock) waiterCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.waiters)
}

// waitForWaiters blocks until at least min goroutines are parked on
// clock.After, or timeout elapses. See keepalive_test.go's identical
// helper for why this matters: without it, an Advance can race a
// not-yet-registered timer.
func (c *fakeClock) waitForWaiters(min int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		c.mu.Lock()
		n := len(c.waiters)
		ch := c.notify
		c.mu.Unlock()
		if n >= min {
			return true
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return false
		}
		select {
		case <-ch:
		case <-time.After(remaining):
			return false
		}
	}
}

// --- debouncer: pure, deterministic timing tests ---

func TestDebouncerCollapsesBurstIntoOneFlush(t *testing.T) {
	clock := newFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	flushed := make(chan []string, 10)
	d := newDebouncer(clock, time.Second, func(paths []string) { flushed <- paths })

	var wg sync.WaitGroup
	wg.Add(1)
	go d.run(&wg)
	defer d.close()

	burst := []string{"a", "b", "c", "a"} // "a" repeated: dedup within one batch
	for _, p := range burst {
		d.markDirty(p)
	}
	if !clock.waitForWaiters(len(burst), 5*time.Second) {
		t.Fatal("timed out waiting for the debouncer to register all timers from the burst")
	}
	clock.Advance(time.Second)

	select {
	case got := <-flushed:
		set := map[string]bool{}
		for _, p := range got {
			set[p] = true
		}
		if len(set) != 3 || !set["a"] || !set["b"] || !set["c"] {
			t.Errorf("flush set = %v, want {a,b,c}", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for flush after burst")
	}

	// Exactly one flush: nothing else should arrive without new marks.
	select {
	case extra := <-flushed:
		t.Errorf("unexpected second flush with no new marks: %v", extra)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestDebouncerEventsAfterWindowProduceSecondFlush(t *testing.T) {
	clock := newFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	flushed := make(chan []string, 10)
	d := newDebouncer(clock, time.Second, func(paths []string) { flushed <- paths })

	var wg sync.WaitGroup
	wg.Add(1)
	go d.run(&wg)
	defer d.close()

	d.markDirty("first")
	if !clock.waitForWaiters(1, 5*time.Second) {
		t.Fatal("timed out waiting for first timer registration")
	}
	clock.Advance(time.Second)
	select {
	case got := <-flushed:
		if len(got) != 1 || got[0] != "first" {
			t.Errorf("first flush = %v, want [first]", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for first flush")
	}

	d.markDirty("second")
	if !clock.waitForWaiters(1, 5*time.Second) {
		t.Fatal("timed out waiting for second timer registration")
	}
	clock.Advance(time.Second)
	select {
	case got := <-flushed:
		if len(got) != 1 || got[0] != "second" {
			t.Errorf("second flush = %v, want [second]", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for second flush")
	}
}

func TestDebouncerCloseStopsRun(t *testing.T) {
	clock := newFakeClock(time.Now())
	d := newDebouncer(clock, time.Second, nil)
	var wg sync.WaitGroup
	wg.Add(1)
	go d.run(&wg)

	done := make(chan struct{})
	go func() {
		d.close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("debouncer.close() did not return promptly")
	}
}

// --- Watcher: real fsnotify wiring over a real temp dir, driven by a
// fake clock so the debounce/rescan timing stays deterministic even
// though event delivery itself is real OS async I/O (waited for via
// channel receive + bounded timeout, never a blind time.Sleep). ---

func newTestWatcher(t *testing.T, root string, clock Clock, onDirty func([]string)) *Watcher {
	t.Helper()
	w, err := NewWatcher(WatcherOptions{
		Root:           root,
		Clock:          clock,
		Debounce:       time.Second,
		RescanInterval: time.Hour, // effectively disabled for these tests
		OnDirty:        onDirty,
		OnWarn: func(format string, args ...any) {
			t.Logf("watcher warning: "+format, args...)
		},
	})
	if err != nil {
		t.Fatalf("NewWatcher: %v", err)
	}
	t.Cleanup(func() {
		if err := w.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return w
}

func TestWatcherAddsWatchesForNewSubdirectories(t *testing.T) {
	root := t.TempDir()
	clock := newFakeClock(time.Now())
	dirty := make(chan []string, 10)
	w := newTestWatcher(t, root, clock, func(paths []string) { dirty <- paths })
	if w.fsw == nil {
		t.Skip("fsnotify watcher unavailable in this environment; skipping real-event test")
	}

	// The periodic-rescan loop registers (and, with RescanInterval set to
	// an hour, keeps) its own waiter on this same clock the whole time.
	// Wait for that registration to land deterministically before reading
	// it as our baseline, so "wait for a new registration" below means
	// "wait for one more than whatever's already there" rather than a
	// magic absolute count racing periodicLoop's own startup.
	if !clock.waitForWaiters(1, 5*time.Second) {
		t.Fatal("timed out waiting for the watcher's periodic-rescan loop to register its timer")
	}
	baseline := clock.waiterCount()

	// Create a new subdirectory. The watcher must both report it dirty
	// and add a live watch for it, so...
	if err := os.Mkdir(filepath.Join(root, "sub1"), 0755); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	waitForDirtyFlush(t, clock, baseline, dirty, "sub1", 5*time.Second)

	// ...a file created inside sub1 afterwards is also seen, which only
	// happens if sub1 itself got a live fsnotify watch. The debouncer's
	// own waiter was consumed by the flush above, so the registration
	// count is back at baseline (just the periodic loop) until this
	// event lands.
	if err := os.WriteFile(filepath.Join(root, "sub1", "inside.txt"), []byte("x"), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	waitForDirtyFlush(t, clock, baseline, dirty, "sub1/inside.txt", 5*time.Second)
}

// waitForDirtyFlush advances the fake clock until a debounce flush
// containing want arrives on dirty, and returns it.
//
// A single filesystem operation can legitimately produce more than one
// fsnotify event — os.WriteFile, for instance, is open(O_CREATE)+write+
// close, which is commonly delivered as separate CREATE and WRITE events
// for the same path. That means a "new timer registered" signal on the
// fake clock isn't necessarily the one Advance is meant to fire: if the
// second event's markDirty call reaches the debouncer's select in the same
// instant Advance fires the current timer, Go's select is free to pick
// either ready case. Picking the incoming mark over the already-fired
// timer is not a bug — the debouncer is meant to treat that as "activity
// within the window" and extend it (see debouncer.go's re-registration
// comment, and SPEC.md §5: "batches of events collapse into one scan") —
// but it means the fired timer's flush is superseded rather than
// delivered, and waiterCount is right back at baseline+1 with a *new*
// timer to wait for. So instead of assuming one Advance always yields one
// flush, keep registering-and-advancing (bounded by overall, never a
// sleep) until the flush we're after actually shows up.
func waitForDirtyFlush(t *testing.T, clock *fakeClock, baseline int, dirty chan []string, want string, overall time.Duration) []string {
	t.Helper()
	deadline := time.Now().Add(overall)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("timed out waiting for a dirty flush containing %s", want)
		}
		if !clock.waitForWaiters(baseline+1, remaining) {
			t.Fatalf("timed out waiting for a new debounce timer registration (want flush containing %s)", want)
		}
		clock.Advance(time.Second)
		select {
		case got := <-dirty:
			if containsPath(got, want) {
				return got
			}
			t.Fatalf("flush = %v, want to contain %s", got, want)
		case <-time.After(200 * time.Millisecond):
			// The fired timer lost the select race to a fresh mark for the
			// same underlying filesystem operation (see doc comment above);
			// a new timer is now registered in its place. Loop and try
			// advancing past that one instead.
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for a dirty flush containing %s", want)
		}
	}
}

func containsPath(paths []string, want string) bool {
	for _, p := range paths {
		if p == want {
			return true
		}
	}
	return false
}

func TestWatcherPeriodicRescanFires(t *testing.T) {
	root := t.TempDir()
	clock := newFakeClock(time.Now())
	periodic := make(chan struct{}, 10)

	w, err := NewWatcher(WatcherOptions{
		Root:           root,
		Clock:          clock,
		Debounce:       time.Second,
		RescanInterval: 10 * time.Second,
		OnPeriodic:     func() { periodic <- struct{}{} },
	})
	if err != nil {
		t.Fatalf("NewWatcher: %v", err)
	}
	defer w.Close()

	if !clock.waitForWaiters(1, 5*time.Second) {
		t.Fatal("timed out waiting for periodic timer registration")
	}
	clock.Advance(10 * time.Second)
	select {
	case <-periodic:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for periodic rescan")
	}
}

func TestWatcherCloseStopsAllGoroutines(t *testing.T) {
	root := t.TempDir()

	before := goroutineCountSettled()

	for i := 0; i < 5; i++ {
		w, err := NewWatcher(WatcherOptions{
			Root:           root,
			Clock:          RealClock,
			Debounce:       10 * time.Millisecond,
			RescanInterval: time.Hour,
		})
		if err != nil {
			t.Fatalf("NewWatcher: %v", err)
		}
		done := make(chan struct{})
		go func() {
			if err := w.Close(); err != nil {
				t.Errorf("Close: %v", err)
			}
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("Close() did not return promptly (possible goroutine deadlock)")
		}
	}

	after := goroutineCountSettled()
	// Allow a little slack: unrelated background goroutines (GC, test
	// framework) can come and go. What we're guarding against is Watcher
	// leaking one or more goroutines per Close, which after 5 cycles
	// would show up as a clear, sustained increase, not noise.
	if after > before+2 {
		t.Errorf("goroutine count grew from %d to %d after 5 NewWatcher/Close cycles; possible leak", before, after)
	}
}

// goroutineCountSettled samples runtime.NumGoroutine() after giving the
// runtime a brief, bounded chance to finish tearing down goroutines that
// already logically exited (e.g. a Close that returned but whose deferred
// wg.Done() hasn't been scheduled yet). This is a leak-detection heuristic,
// not a debounce-timing mechanism — the debounce behavior itself is tested
// exclusively via fakeClock above, with no sleeping.
func goroutineCountSettled() int {
	runtime.GC()
	var n int
	for i := 0; i < 10; i++ {
		n = runtime.NumGoroutine()
		time.Sleep(10 * time.Millisecond)
	}
	return n
}

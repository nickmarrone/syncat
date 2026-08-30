package protocol

import (
	"context"
	"sync"
	"testing"
	"time"
)

// fakeClock is a manually-driven [Clock] for deterministic keepalive
// tests: nothing here ever sleeps on a wall clock. Advance fires any
// pending After channels whose deadline it reaches, synchronously with
// respect to the caller (though the goroutines woken by those channels
// still run concurrently, same as with a real timer) — tests synchronize
// with those goroutines via their own channels, never by sleeping.
type fakeClock struct {
	mu      sync.Mutex
	now     time.Time
	waiters []fakeWaiter
	// notify is closed and replaced (the same channel-swap idiom used by
	// transport's bufConn) every time a new waiter registers, so
	// waitForWaiters can block on it instead of polling/sleeping.
	notify chan struct{}
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

// Advance moves the fake clock forward by d, firing (synchronously, in
// deadline order) every waiter whose deadline has now been reached.
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

// waitForWaiters blocks until at least min goroutines are parked on
// clock.After (i.e. Keepalive.Run has reached its next select and is
// ready to observe an Advance), or timeout elapses. This is the
// synchronization primitive that keeps the Run-driving tests below
// deterministic: without it, a test's Advance could race Run's goroutine
// not yet having re-registered its next timer, silently attaching that
// registration to a clock value that already moved past when the test
// expected it to fire. The bounded time.After here is only a test
// safety-net deadline, not a polling interval — waking happens
// event-driven via c.notify.
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

func TestKeepaliveNeedsPingAndDead(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := newFakeClock(start)
	k := NewKeepalive(clock, 30*time.Second, 90*time.Second)

	if k.NeedsPing() {
		t.Error("NeedsPing() immediately after construction: want false")
	}
	if k.Dead() {
		t.Error("Dead() immediately after construction: want false")
	}

	clock.Advance(29 * time.Second)
	if k.NeedsPing() {
		t.Error("NeedsPing() at 29s: want false")
	}

	clock.Advance(1 * time.Second) // now at 30s
	if !k.NeedsPing() {
		t.Error("NeedsPing() at 30s: want true")
	}
	if k.Dead() {
		t.Error("Dead() at 30s: want false")
	}

	k.RecordSent()
	if k.NeedsPing() {
		t.Error("NeedsPing() right after RecordSent: want false")
	}

	clock.Advance(60 * time.Second) // total 90s since start, but lastRecv never updated
	if !k.Dead() {
		t.Error("Dead() at 90s with nothing ever received since construction: want true")
	}

	k.RecordReceived()
	if k.Dead() {
		t.Error("Dead() right after RecordReceived: want false")
	}
}

func TestKeepaliveRunPingsAtIntervalAndPongResets(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := newFakeClock(start)
	k := NewKeepalive(clock, 30*time.Second, 90*time.Second)

	pings := make(chan struct{}, 10)
	deadCh := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go k.Run(ctx, 30*time.Second, func() {
		k.RecordSent()
		pings <- struct{}{}
	}, func() {
		deadCh <- struct{}{}
	})

	// First tick at +30s: idle since construction, so a ping is due. Wait
	// for Run to actually be parked on its timer before advancing past
	// it — see waitForWaiters' doc comment for why this matters.
	if !clock.waitForWaiters(1, 5*time.Second) {
		t.Fatal("timed out waiting for Run to register its first timer")
	}
	clock.Advance(30 * time.Second)
	select {
	case <-pings:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for first ping at 30s")
	}

	// Simulate the peer's Pong arriving, keeping the connection alive.
	k.RecordReceived()

	// Second tick at +60s: lastSent was reset by the ping callback at
	// +30s, so another ping is due.
	if !clock.waitForWaiters(1, 5*time.Second) {
		t.Fatal("timed out waiting for Run to register its second timer")
	}
	clock.Advance(30 * time.Second)
	select {
	case <-pings:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for second ping at 60s")
	}

	select {
	case <-deadCh:
		t.Fatal("Run reported dead while Pongs were arriving on schedule")
	default:
	}
}

func TestKeepaliveRunReportsDeadAfterSilence(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := newFakeClock(start)
	k := NewKeepalive(clock, 30*time.Second, 90*time.Second)

	pings := make(chan struct{}, 10)
	deadCh := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Note: onPing here does NOT call RecordReceived (no Pong ever
	// arrives), so lastRecv stays pinned at construction time and the
	// connection should be declared dead once 90s have elapsed.
	go k.Run(ctx, 30*time.Second, func() {
		k.RecordSent()
		pings <- struct{}{}
	}, func() {
		deadCh <- struct{}{}
	})

	for i := 0; i < 2; i++ {
		if !clock.waitForWaiters(1, 5*time.Second) {
			t.Fatalf("timed out waiting for Run to register timer #%d", i+1)
		}
		clock.Advance(30 * time.Second)
		select {
		case <-pings:
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for ping #%d", i+1)
		}
	}

	// Now at +60s. One more 30s tick reaches +90s of total silence on the
	// receive side.
	if !clock.waitForWaiters(1, 5*time.Second) {
		t.Fatal("timed out waiting for Run to register its third timer")
	}
	clock.Advance(30 * time.Second)
	select {
	case <-deadCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Run to report the connection dead at 90s")
	}

	// Run must have returned (not kept polling) once it declared the
	// connection dead.
	select {
	case <-pings:
		t.Error("Run kept sending pings after declaring the connection dead")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestKeepaliveRunStopsOnContextCancel(t *testing.T) {
	clock := newFakeClock(time.Now())
	k := NewKeepalive(clock, 30*time.Second, 90*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		k.Run(ctx, 30*time.Second, nil, nil)
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return promptly after ctx was canceled")
	}
}

func TestNewKeepaliveDefaults(t *testing.T) {
	k := NewKeepalive(nil, 0, 0)
	if k.clock == nil {
		t.Error("clock defaulted to nil, want RealClock")
	}
	if k.pingInterval != DefaultPingInterval {
		t.Errorf("pingInterval = %v, want %v", k.pingInterval, DefaultPingInterval)
	}
	if k.deadAfter != DefaultDeadAfter {
		t.Errorf("deadAfter = %v, want %v", k.deadAfter, DefaultDeadAfter)
	}
}

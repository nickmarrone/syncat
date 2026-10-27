package sync

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	stdsync "sync"
	"testing"
	"time"
)

// --- keepalive: SPEC.md §4's ping and dead rule, run by Session ---------

// fakeClock is a manually driven protocol.Clock, so keepalive tests never
// wait on the wall clock. Advance fires every After whose deadline it
// reaches. It mirrors internal/protocol's test clock, which is not exported.
type fakeClock struct {
	mu      stdsync.Mutex
	now     time.Time
	waiters []fakeWaiter
	notify  chan struct{} // closed and replaced whenever a waiter registers
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

// waitForWaiter blocks until something is parked on After, so an Advance
// that follows cannot race the keepalive registering its next timer.
func (c *fakeClock) waitForWaiter(t *testing.T) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		c.mu.Lock()
		n, ch := len(c.waiters), c.notify
		c.mu.Unlock()
		if n > 0 {
			return
		}
		select {
		case <-ch:
		case <-deadline:
			t.Fatal("keepalive never waited on the clock")
		}
	}
}

// TestSessionKeepalivePingsAndCountsPong checks the live half of the rule:
// an idle session pings, the peer's Session answers without any help from
// its owner, and the Pong is counted.
func TestSessionKeepalivePingsAndCountsPong(t *testing.T) {
	a, b := newTestNode(t, "aaaaaaaaaaaaaaaa"), newTestNode(t, "bbbbbbbbbbbbbbbb")
	clock := newFakeClock(time.Now())
	sa, _ := connectSessionsWithOptions(t, a, b, Direction{}, Direction{}, SessionOptions{Clock: clock}, SessionOptions{})

	clock.waitForWaiter(t)
	clock.Advance(30 * time.Second)

	waitFor(t, 5*time.Second, func() bool { return sa.Stats().PongsReceived == 1 })
	if got := sa.Stats().PingsSent; got != 1 {
		t.Fatalf("PingsSent = %d, want 1", got)
	}
	select {
	case <-sa.Done():
		t.Fatal("a session that got its Pong ended")
	default:
	}
}

// TestSessionKeepaliveDeadEndsSession checks the backstop: a peer that
// reads but never writes is declared dead after 90s, the session closes
// itself, and Err says why.
func TestSessionKeepaliveDeadEndsSession(t *testing.T) {
	n := newTestNode(t, "keepalive")
	near, far := net.Pipe()
	t.Cleanup(func() { _ = far.Close() })
	go func() { _, _ = io.Copy(io.Discard, far) }() // silent, but never blocks our writes

	clock := newFakeClock(time.Now())
	s := NewSession(near, n.store, n.id, "peer", SessionOptions{Clock: clock, Logger: log.New(io.Discard, "", 0)})
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	clock.waitForWaiter(t)
	clock.Advance(90 * time.Second)

	select {
	case <-s.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("session outlived the keepalive dead interval")
	}
	if err := s.Err(); !errors.Is(err, ErrKeepaliveDead) {
		t.Fatalf("Err() = %v, want ErrKeepaliveDead", err)
	}
	if got := s.Stats().DeadConnections; got != 1 {
		t.Fatalf("DeadConnections = %d, want 1", got)
	}
}

package transport

import (
	"bytes"
	"io"
	"net"
	"os"
	"sync"
	"time"
)

// bufConn is a net.Conn backed by an unbounded, in-memory byte queue in
// each direction.
//
// This is deliberately not the stdlib's net.Pipe: that pipe is synchronous
// and unbuffered, meaning a Write blocks until a concurrent Read on the
// other end drains it. SPEC.md §4's handshake has both sides write their
// Hello before either has read anything ("Both sides immediately send
// Hello..."); wired together with a bare net.Pipe, two goroutines doing
// that in the obvious write-then-read order deadlock, because each side's
// Write blocks waiting for a Read the peer hasn't reached yet. Buffering
// each direction avoids relying on the caller always having a concurrent
// reader in flight: Write returns as soon as the bytes are queued, the
// same way a real TCP socket's send buffer behaves. Deadlines are
// supported (Read waits on the queue, a timer, or the connection's own
// Close, whichever comes first); Write never blocks, so a write deadline
// is accepted but has nothing to enforce against.
type bufConn struct {
	local, remote net.Addr
	in            *byteQueue // bytes the peer wrote to us; we read from here
	out           *byteQueue // bytes we write; the peer reads them from here

	closeOnce sync.Once
	closed    chan struct{}

	deadlineMu   sync.Mutex
	readDeadline time.Time
}

// newBufConnPair returns two ends of an in-memory connection, as if `a`
// dialed `b`.
func newBufConnPair(aAddr, bAddr net.Addr) (a, b *bufConn) {
	aToB := newByteQueue()
	bToA := newByteQueue()
	a = &bufConn{local: aAddr, remote: bAddr, in: bToA, out: aToB, closed: make(chan struct{})}
	b = &bufConn{local: bAddr, remote: aAddr, in: aToB, out: bToA, closed: make(chan struct{})}
	return a, b
}

func (c *bufConn) Read(p []byte) (int, error) {
	select {
	case <-c.closed:
		return 0, net.ErrClosed
	default:
	}
	return c.in.read(p, c.readDeadlineChan(), c.closed)
}

func (c *bufConn) Write(p []byte) (int, error) {
	select {
	case <-c.closed:
		return 0, net.ErrClosed
	default:
	}
	return c.out.write(p)
}

// Close closes this end of the connection: further Reads and Writes on it
// fail, and the peer's Read observes io.EOF once it has drained whatever
// was already queued. It is idempotent.
func (c *bufConn) Close() error {
	c.closeOnce.Do(func() {
		close(c.closed)
		c.out.close()
	})
	return nil
}

func (c *bufConn) LocalAddr() net.Addr  { return c.local }
func (c *bufConn) RemoteAddr() net.Addr { return c.remote }

func (c *bufConn) SetDeadline(t time.Time) error {
	c.SetReadDeadline(t)
	return nil // no-op for writes; see the bufConn doc comment
}

func (c *bufConn) SetReadDeadline(t time.Time) error {
	c.deadlineMu.Lock()
	c.readDeadline = t
	c.deadlineMu.Unlock()
	return nil
}

func (c *bufConn) SetWriteDeadline(t time.Time) error {
	return nil // no-op; Write never blocks (unbounded queue)
}

func (c *bufConn) readDeadlineChan() <-chan time.Time {
	c.deadlineMu.Lock()
	d := c.readDeadline
	c.deadlineMu.Unlock()
	if d.IsZero() {
		return nil
	}
	return time.After(time.Until(d))
}

// byteQueue is an unbounded, closable, concurrency-safe byte buffer used
// to implement one direction of a bufConn. Waiting readers are woken via
// the channel-swap idiom: notifyCh is closed (broadcasting to everyone
// currently waiting) and replaced whenever new data arrives or the queue
// is closed.
type byteQueue struct {
	mu       sync.Mutex
	buf      bytes.Buffer
	closed   bool
	notifyCh chan struct{}
}

func newByteQueue() *byteQueue {
	return &byteQueue{notifyCh: make(chan struct{})}
}

func (q *byteQueue) write(p []byte) (int, error) {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return 0, net.ErrClosed
	}
	n, _ := q.buf.Write(p) // bytes.Buffer.Write never fails except on OOM
	old := q.notifyCh
	q.notifyCh = make(chan struct{})
	q.mu.Unlock()
	close(old)
	return n, nil
}

func (q *byteQueue) close() {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return
	}
	q.closed = true
	old := q.notifyCh
	q.notifyCh = make(chan struct{})
	q.mu.Unlock()
	close(old)
}

// read reads whatever is available into p, blocking until there is data,
// the queue is closed (io.EOF), deadlineCh fires (os.ErrDeadlineExceeded),
// or connClosed fires (net.ErrClosed).
func (q *byteQueue) read(p []byte, deadlineCh <-chan time.Time, connClosed <-chan struct{}) (int, error) {
	for {
		q.mu.Lock()
		if q.buf.Len() > 0 {
			n, _ := q.buf.Read(p)
			q.mu.Unlock()
			return n, nil
		}
		if q.closed {
			q.mu.Unlock()
			return 0, io.EOF
		}
		ch := q.notifyCh
		q.mu.Unlock()

		select {
		case <-ch:
			// New data or a close arrived; loop and re-check.
		case <-deadlineCh:
			return 0, os.ErrDeadlineExceeded
		case <-connClosed:
			return 0, net.ErrClosed
		}
	}
}

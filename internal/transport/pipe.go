package transport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"
)

// PipeTransport is an in-memory [Transport] for tests (SPEC.md §10): two
// PipeTransports sharing the same process find each other by address string
// through a package-level registry, the same way tailcat peers find each
// other by ConnBlob. See bufConn below for why connections are a buffered
// in-memory pipe rather than the stdlib's net.Pipe.
//
// The zero value is not usable; construct with [NewPipeTransport].
type PipeTransport struct {
	addr string

	mu      sync.Mutex
	onConn  func(net.Conn)
	started bool
	closed  bool
}

// pipeRegistry maps address string to the PipeTransport currently
// listening on it, so independently constructed transports can find each
// other. Entries are added by Start and removed by Close.
var pipeRegistry sync.Map // map[string]*PipeTransport

// NewPipeTransport returns a transport that will listen on, and be
// reachable at, addr once Start is called. addr must be unique among all
// PipeTransports concurrently registered in this process.
func NewPipeTransport(addr string) *PipeTransport {
	return &PipeTransport{addr: addr}
}

func (t *PipeTransport) Start(ctx context.Context, onConn func(net.Conn)) error {
	if onConn == nil {
		return errors.New("transport: pipe: Start: onConn must not be nil")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return errors.New("transport: pipe: Start called after Close")
	}
	if t.started {
		return errors.New("transport: pipe: Start called twice")
	}
	if t.addr == "" {
		return errors.New("transport: pipe: address must not be empty")
	}
	if _, loaded := pipeRegistry.LoadOrStore(t.addr, t); loaded {
		return fmt.Errorf("transport: pipe: address %q is already registered", t.addr)
	}
	t.onConn = onConn
	t.started = true
	return nil
}

// DiscardPeer implements Transport. PipeTransport caches nothing per peer
// — every Dial goes straight to the registry — so there is nothing to drop.
func (t *PipeTransport) DiscardPeer(string) {}

func (t *PipeTransport) Dial(ctx context.Context, addr string) (net.Conn, error) {
	v, ok := pipeRegistry.Load(addr)
	if !ok {
		return nil, fmt.Errorf("transport: pipe: dial %q: no such address", addr)
	}
	peer := v.(*PipeTransport)

	peer.mu.Lock()
	onConn := peer.onConn
	closed := peer.closed
	peer.mu.Unlock()
	if closed || onConn == nil {
		return nil, fmt.Errorf("transport: pipe: dial %q: peer is not accepting connections", addr)
	}

	t.mu.Lock()
	localAddr := t.addr
	t.mu.Unlock()

	ours, theirs := newBufConnPair(pipeAddr(localAddr), pipeAddr(addr))
	go onConn(theirs)
	return ours, nil
}

func (t *PipeTransport) LocalAddress() (string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.started {
		return "", errors.New("transport: pipe: LocalAddress called before Start")
	}
	return t.addr, nil
}

func (t *PipeTransport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	addr := t.addr
	started := t.started
	t.mu.Unlock()

	if started {
		// Only remove the registry entry if it's still ours: a fresh
		// PipeTransport could in principle have re-registered the same
		// address after we were replaced, though callers shouldn't do
		// that concurrently with our own Close.
		pipeRegistry.CompareAndDelete(addr, t)
	}
	return nil
}

// pipeAddr is a net.Addr for PipeTransport connections; its String is the
// registry address string.
type pipeAddr string

func (a pipeAddr) Network() string { return "pipe" }
func (a pipeAddr) String() string  { return string(a) }

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

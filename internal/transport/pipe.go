package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
)

// --- PipeTransport: the loopback Transport used by tests ---------------

// PipeTransport is the [Transport] tests use in place of
// [TailcatTransport] (SPEC.md §10). Two PipeTransports in the same process
// find each other by address string through a package-level registry, the
// same way tailcat peers find each other by ConnBlob; the connections
// themselves are ordinary TCP connections over the loopback interface.
//
// Loopback sockets rather than an in-process pipe, deliberately.
// Everything above this package is written against net.Conn's whole
// contract, and three parts of it carry real weight here:
//
//   - A deadline set on a Read that is *already blocked* must interrupt
//     it. internal/protocol's Handshake cancels a stalled handshake by
//     exactly that move — its ctx watcher calls conn.SetDeadline(now) on
//     a connection some other goroutine is blocked reading.
//   - A Write to a peer that has gone away must eventually fail, so the
//     error paths above this package are reachable at all.
//   - The send buffer must be finite, so backpressure exists and a test
//     can observe a stalled transfer instead of quietly growing a heap.
//
// The stdlib's net.Pipe honours deadlines but is synchronous and
// unbuffered — a Write blocks until the peer Reads, which deadlocks
// SPEC.md §4's "both sides immediately send Hello". A hand-rolled
// buffered net.Conn fixes that one problem and must then re-earn all
// three properties above; the ones it gets wrong fail silently, leaving a
// green test suite that never exercised the behaviour it was written to
// pin. A kernel socket costs a few microseconds per connection and gets
// every one of them right for free.
//
// The zero value is not usable; construct with [NewPipeTransport].
type PipeTransport struct {
	addr string

	mu      sync.Mutex
	ln      net.Listener
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

	ln, err := listenLoopback()
	if err != nil {
		pipeRegistry.CompareAndDelete(t.addr, t)
		return fmt.Errorf("transport: pipe: listen: %w", err)
	}

	t.ln = ln
	t.onConn = onConn
	t.started = true
	go acceptLoop(ln, onConn)
	return nil
}

// listenLoopback binds a listener on the loopback interface, on a port the
// kernel picks. IPv4 loopback is tried first and IPv6 is the fallback, so
// this works on hosts configured for either.
func listenLoopback() (net.Listener, error) {
	ln, err4 := net.Listen("tcp", "127.0.0.1:0")
	if err4 == nil {
		return ln, nil
	}
	ln, err6 := net.Listen("tcp", "[::1]:0")
	if err6 == nil {
		return ln, nil
	}
	return nil, fmt.Errorf("no loopback listener: IPv4: %v; IPv6: %w", err4, err6)
}

// acceptLoop hands every accepted connection to onConn on its own
// goroutine, per [Transport.Start]'s contract, until ln is closed (which
// is how [PipeTransport.Close] stops it).
func acceptLoop(ln net.Listener, onConn func(net.Conn)) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			// The only expected error is the listener being closed; any
			// other is equally terminal for a loopback listener, and this
			// is a test transport with nowhere useful to report it.
			return
		}
		go onConn(conn)
	}
}

func (t *PipeTransport) Dial(ctx context.Context, addr string) (net.Conn, error) {
	v, ok := pipeRegistry.Load(addr)
	if !ok {
		return nil, fmt.Errorf("transport: pipe: dial %q: no such address", addr)
	}
	peer := v.(*PipeTransport)

	peer.mu.Lock()
	ln, onConn, closed := peer.ln, peer.onConn, peer.closed
	peer.mu.Unlock()
	if closed || ln == nil || onConn == nil {
		return nil, fmt.Errorf("transport: pipe: dial %q: peer is not accepting connections", addr)
	}

	// Dialling the peer's real listener address means ctx bounds the dial
	// itself, the same way it does for TailcatTransport.Dial.
	var d net.Dialer
	conn, err := d.DialContext(ctx, ln.Addr().Network(), ln.Addr().String())
	if err != nil {
		return nil, fmt.Errorf("transport: pipe: dial %q: %w", addr, err)
	}
	return conn, nil
}

// DiscardPeer implements Transport. PipeTransport caches nothing per peer
// — every Dial goes straight to the registry — so there is nothing to drop.
func (t *PipeTransport) DiscardPeer(string) {}

// LocalAddress implements Transport. It returns the registry key — the
// address peers dial, and the tailcat ConnBlob's stand-in inside a node
// token — not the loopback host:port the listener happens to hold.
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
	addr, ln, started := t.addr, t.ln, t.started
	t.ln = nil
	t.mu.Unlock()

	if started {
		// Only remove the registry entry if it's still ours: a fresh
		// PipeTransport could in principle have re-registered the same
		// address after we were replaced, though callers shouldn't do
		// that concurrently with our own Close.
		pipeRegistry.CompareAndDelete(addr, t)
	}
	if ln != nil {
		// Closing the listener ends acceptLoop. Connections already
		// accepted, or already returned by Dial, are the caller's to close
		// — see Transport.Close.
		if err := ln.Close(); err != nil {
			return fmt.Errorf("transport: pipe: close listener: %w", err)
		}
	}
	return nil
}

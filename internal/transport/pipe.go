package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
)

// PipeTransport is an in-memory [Transport] for tests (SPEC.md §10): two
// PipeTransports sharing the same process find each other by address string
// through a package-level registry, the same way tailcat peers find each
// other by ConnBlob. See bufconn.go for why connections are a buffered
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

package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
)

// PipeTransport is the in-process Transport used by tests. Instances find
// each other by address through a registry; each connection is a stdlib
// net.Pipe. Synchronous writes expose invalid protocol ordering instead of
// hiding it behind a socket buffer.
type PipeTransport struct {
	addr    string
	mu      sync.Mutex
	onConn  func(net.Conn)
	started bool
	closed  bool
}

var pipeRegistry sync.Map // map[string]*PipeTransport

func NewPipeTransport(addr string) *PipeTransport { return &PipeTransport{addr: addr} }

func (t *PipeTransport) Start(_ context.Context, onConn func(net.Conn)) error {
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
	t.onConn, t.started = onConn, true
	return nil
}

func (t *PipeTransport) Dial(ctx context.Context, addr string) (net.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("transport: pipe: dial %q: %w", addr, err)
	}
	v, ok := pipeRegistry.Load(addr)
	if !ok {
		return nil, fmt.Errorf("transport: pipe: dial %q: no such address", addr)
	}
	peer := v.(*PipeTransport)
	peer.mu.Lock()
	onConn, started, closed := peer.onConn, peer.started, peer.closed
	peer.mu.Unlock()
	if closed || !started || onConn == nil {
		return nil, fmt.Errorf("transport: pipe: dial %q: peer is not accepting connections", addr)
	}
	dialed, accepted := net.Pipe()
	go onConn(accepted)
	return dialed, nil
}

func (t *PipeTransport) DiscardPeer(string) {}

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
	addr, started := t.addr, t.started
	t.mu.Unlock()
	if started {
		pipeRegistry.CompareAndDelete(addr, t)
	}
	return nil
}

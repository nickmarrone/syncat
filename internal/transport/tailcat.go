package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tailscale/tailcat"
	"tailscale.com/wgengine/filter"
)

// SyncatPort is the TCP port the syncat protocol runs on, inside the
// tailcat tunnel (SPEC.md §4).
const SyncatPort uint16 = 4197

// drainTimeout bounds how long Close waits for one of tailcat's userspace
// TCP stacks to flush final FINs before tearing it down. Close spends it
// twice: once as a single budget shared by every cached client, then once
// on the server.
const drainTimeout = 5 * time.Second

// TailcatTransport is the production [Transport], carrying the syncat
// protocol stream over a tailcat WireGuard tunnel relayed through DERP.
//
// Only port [SyncatPort] is ever served: Start listens on that port alone
// with [tailcat.Server.Listen], and [tailcat.Server.ServedTCPPorts] is set
// to an empty, non-nil list, so the packet filter admits only listened-on
// ports and drops everything else before it reaches the TCP stack. OnTCP is
// left nil, so even a packet the filter let through to another port would
// get a RST.
type TailcatTransport struct {
	key  *tailcat.PrivateKey // node identity; Public.RegionID is baked in (see config.LoadOrCreateTailcatKey)
	logf func(string, ...any)

	mu       sync.Mutex
	server   tailcatServer
	listener net.Listener  // set once started; closed by Close
	accepted chan struct{} // closed when the accept loop exits
	state    tailcatTransportState
	// newServer is replaceable in tests so startup can be paused at the
	// exact lifecycle boundaries without contacting DERP.
	newServer func() tailcatServer

	clientsMu sync.Mutex
	clients   map[string]*peerClient // keyed by peer addr (tailcat.Addr string); reused across dials
}

// peerClient pairs a cached [tailcat.Client] with whether a dial on it has
// ever succeeded — the same question as whether it has a netstack yet.
//
// Close needs that answer. tailcat.Client.DrainTCP dereferences the netstack
// unconditionally, unlike tailcat.Client.Close, which returns nil for a
// client that was never used; and Dial caches a Client *before* the dial
// that starts it, so a Close racing that window would otherwise drain a
// client no dial has yet touched and panic.
type peerClient struct {
	c      *tailcat.Client
	dialed atomic.Bool
}

type tailcatServer interface {
	Listen(ctx context.Context, network, address string) (net.Listener, error)
	DrainTCP(context.Context) error
	Close() error
	TailcatAddr() tailcat.Addr
}

type tailcatTransportState uint8

const (
	tailcatStateNew tailcatTransportState = iota
	tailcatStateStarting
	tailcatStateStarted
	tailcatStateFailed
	tailcatStateClosed
)

// OpError keeps the internal cause available to errors.Is/errors.As without
// copying a credential-bearing tailcat error into Error(), logs, or status.
type OpError struct {
	Op  string
	Err error
}

func (e *OpError) Error() string { return "transport: tailcat: " + e.Op + " failed" }
func (e *OpError) Unwrap() error { return e.Err }

// NewTailcatTransport returns a transport using key as this node's tailcat
// identity (see config.LoadOrCreateTailcatKey, which bakes in a concrete
// DERP RegionID so the resulting address is stable across runs). logf, if
// non-nil, receives tailcat's debug logging.
func NewTailcatTransport(key *tailcat.PrivateKey, logf func(string, ...any)) *TailcatTransport {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	logf = redactingLogf(logf, string(key.Public.Addr()), fmt.Sprint(key.Public.PresharedKey))
	t := &TailcatTransport{key: key, logf: logf}
	t.newServer = func() tailcatServer {
		return &tailcat.Server{
			Key:      t.key.Private,
			RegionID: t.key.Public.RegionID,
			Logf:     t.logf,
			// A zero value makes tailcat generate a new PSK, changing this
			// node's address and invalidating persisted pairing tokens.
			PresharedKey: t.key.Public.PresharedKey,
			// Non-nil but empty: the packet filter then admits only ports
			// with an active Listen listener, i.e. SyncatPort alone.
			ServedTCPPorts: []filter.PortRange{},
		}
	}
	return t
}

func (t *TailcatTransport) Start(ctx context.Context, onConn func(net.Conn)) error {
	if onConn == nil {
		return errors.New("transport: tailcat: Start: onConn must not be nil")
	}

	t.mu.Lock()
	if t.state != tailcatStateNew {
		state := t.state
		t.mu.Unlock()
		if state == tailcatStateClosed {
			return errors.New("transport: tailcat: Start called after Close")
		}
		return errors.New("transport: tailcat: Start called more than once")
	}
	srv := t.newServer()
	t.server = srv
	t.state = tailcatStateStarting
	t.mu.Unlock()

	// Listen starts the server, with ctx bounding the startup network work
	// (fetching the DERP map and picking a region), and cleans up whatever
	// a failed startup created. What follows it is local setup, so Start
	// returns promptly on cancellation without a goroutine racing ctx.
	ln, err := srv.Listen(ctx, "tcp", fmt.Sprintf(":%d", SyncatPort))

	t.mu.Lock()
	// Close may have run while Listen was in flight. It leaves the server
	// alone in that window (tailcat's Server.Close must not race its own
	// startup), so whatever Listen produced is ours to close here.
	abandoned := t.state != tailcatStateStarting
	switch {
	case abandoned:
	case err != nil:
		t.state = tailcatStateFailed
	default:
		t.state = tailcatStateStarted
		t.listener = ln
		t.accepted = make(chan struct{})
	}
	accepted := t.accepted
	t.mu.Unlock()

	if err != nil || abandoned {
		if ln != nil {
			_ = ln.Close()
		}
		_ = srv.Close()
		if err == nil {
			err = errors.New("transport closed during startup")
		}
		return fmt.Errorf("transport: tailcat: start: %w", err)
	}

	go t.acceptLoop(ln, onConn, accepted)
	return nil
}

// acceptLoop hands each inbound connection to onConn on its own goroutine,
// as [Transport.Start] promises, until the listener is closed.
func (t *TailcatTransport) acceptLoop(ln net.Listener, onConn func(net.Conn), done chan<- struct{}) {
	defer close(done)
	for {
		conn, err := ln.Accept()
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				t.logf("transport: tailcat: accept: %v", err)
			}
			return
		}
		go onConn(conn)
	}
}

// clientFor returns the [tailcat.Client] for addr, creating and caching it
// on first use. Reusing one Client per peer (rather than building one per
// dial attempt) avoids repeating its WireGuard/DERP setup and lets it keep
// its connection to the peer warm across repeated dials.
//
// Deliberately leaves Client.Key unset rather than reusing t.key.Private
// (this node's own server identity): tailcat.Client's doc comment says an
// unset Key gets "a new ephemeral key ... generated at first use", and
// that's required here, not optional. DERP routes packets to a node key by
// delivering to whichever local connection most recently registered under
// it; every locoBackend that needs to receive unsolicited inbound traffic
// (this transport's own Server, plus one per-peer Client per configured
// peer, since each independently dials and keeps a "home" DERP connection
// alive) needs a *distinct* key, or their DERP registrations keep
// stealing each other's routing slot. Concretely: reusing the server's
// key here meant that the moment any client (i.e. any dial, including
// one started well after Start, like Node.AddPeer's) registered with
// DERP, it silently superseded the server's own registration, so a
// MeowPing another peer sent us landed on a Client's locoBackend instead
// of the Server's, and Client.onDERPRecv explicitly ignores MeowPing
// ("client ignores MeowPing"). The sender then just sits on
// Client.Ping's unconditional 10s internal timeout, forever, with no
// error surfaced anywhere below dialAttempt. See internal/core/peer.go's
// dialAttempt and the regression test for the full failure mode.
func (t *TailcatTransport) clientFor(addr string) (*peerClient, error) {
	if addr == "" {
		return nil, errors.New("transport: tailcat: dial: address must not be empty")
	}

	t.clientsMu.Lock()
	defer t.clientsMu.Unlock()
	return t.clientForLocked(addr), nil
}

// clientForLocked is clientFor with clientsMu already held. Dial uses it
// while transitioning from the lifecycle lock to the client-cache lock so
// Close cannot miss a newly created client.
func (t *TailcatTransport) clientForLocked(addr string) *peerClient {
	if pc, ok := t.clients[addr]; ok {
		return pc
	}
	pc := &peerClient{c: &tailcat.Client{
		Server: tailcat.Addr(addr),
		Logf:   redactingLogf(t.logf, addr),
	}}
	if t.clients == nil {
		t.clients = make(map[string]*peerClient)
	}
	t.clients[addr] = pc
	return pc
}

func (t *TailcatTransport) Dial(ctx context.Context, addr string) (net.Conn, error) {
	t.mu.Lock()
	if t.state != tailcatStateStarted {
		t.mu.Unlock()
		return nil, errors.New("transport: tailcat: Dial called while transport is not started")
	}
	t.clientsMu.Lock()
	t.mu.Unlock()
	// Holding clientsMu across lookup/create closes the race where Close
	// snapshots the cache immediately before a dial adds a new client.
	if addr == "" {
		t.clientsMu.Unlock()
		return nil, errors.New("transport: tailcat: dial: address must not be empty")
	}
	pc := t.clientForLocked(addr)
	t.clientsMu.Unlock()
	// DialTCPPort blocks internally (via Client.up) until the server has
	// acked us as a peer, resending the meow ping while it waits.
	conn, err := pc.c.DialTCPPort(ctx, SyncatPort)
	if err != nil {
		t.discardClient(addr, pc)
		return nil, &OpError{Op: "dial", Err: err}
	}
	// The dial brought this client's netstack up, so Close may drain it.
	pc.dialed.Store(true)
	return conn, nil
}

// discardClient drops a cached Client after a failed dial and closes it, so
// the next dial to addr builds a fresh one.
//
// This is what makes a peer restart recoverable. tailcat.Client.up latches:
//
//	func (c *Client) up(ctx context.Context) error {
//		if c.upDone.Load() { return nil }  // set once, never cleared
//		_, err := c.Ping(ctx)
//		return err
//	}
//
// and that Ping is the meow that tells the *server* to add us as a
// WireGuard peer. So a cached Client which has meowed once never meows
// again. When the peer restarts, its new process has no WireGuard peers at
// all, and every later dial on the cached Client sends TCP into a tunnel
// the far side will not accept, forever, because each retry reuses the
// same latched Client.
//
// Observed as a striking asymmetry: restarting the *peer* stalled the link
// permanently, while restarting *this* node fixed it (Close clears the
// whole map). Nothing on this side said anything; the only visible symptom
// was on the peer, which kept dialing, kept authenticating, and kept losing
// SPEC.md §2.4's dedup rule with nothing ever adopted, because the dial
// that was supposed to win was this one.
//
// Only failed dials discard. A healthy peer keeps its warm Client across
// dials, which is the whole reason for caching.
//
// Closing matters as much as evicting: each Client owns a locoBackend with
// its own WireGuard engine and DERP connections, so dropping one without
// closing it would leak an engine per failed dial attempt.
func (t *TailcatTransport) discardClient(addr string, pc *peerClient) {
	t.clientsMu.Lock()
	cached, ok := t.clients[addr]
	if ok && cached == pc {
		delete(t.clients, addr)
	} else {
		// Another dial already discarded this one and installed a
		// replacement; it owns closing what it removed.
		pc = nil
	}
	t.clientsMu.Unlock()

	if pc != nil {
		_ = pc.c.Close()
	}
}

// DiscardPeer implements Transport. Unlike discardClient it is
// unconditional: the caller is telling us the peer's connection has ended,
// so whatever we hold for it is stale regardless of which Client it is.
//
// It deliberately does not drain the way Close does. This runs on every
// disconnect, ahead of a redial the peer is waiting on, and the connection
// whose bytes might still be queued is one the caller has already closed
// and stopped reading. Spending up to drainTimeout here would delay every
// reconnect to protect a stream nobody is listening to.
func (t *TailcatTransport) DiscardPeer(addr string) {
	t.clientsMu.Lock()
	pc := t.clients[addr]
	delete(t.clients, addr)
	t.clientsMu.Unlock()

	if pc != nil {
		_ = pc.c.Close()
	}
}

// ValidateAddr implements Transport, rejecting an address tailcat could
// never dial so that a token carrying one is refused where it was pasted.
//
// The case that matters is an address predating tailcat 0.3.0, which split
// path discovery from node identity: it has no disco key, and tailcat
// rejects it deep inside Client.up, below dialTimeout and below anything
// that reports an error. All the operator would see is a peer that never
// connects while the supervisor redials it on backoff forever.
//
// Only the missing disco key is fatal. A zero pre-shared key is a supported
// configuration on tailcat's side (`--psk=false`) and clients accept an
// address without one, so it is no reason to refuse a token.
//
// Errors deliberately do not wrap tailcat's parse error: like OpError, they
// must not copy a credential-bearing address into logs or API responses.
func (t *TailcatTransport) ValidateAddr(addr string) error {
	ci, err := tailcat.ParseAddr(tailcat.Addr(addr))
	if err != nil {
		return &OpError{Op: "parse address", Err: err}
	}
	if ci.ServerDiscoPublic.IsZero() {
		return errors.New("transport: tailcat: address predates tailcat 0.3.0 (it carries no disco key) and cannot be dialed; " +
			"the other node must upgrade syncat and send a token from a fresh `syncat token`")
	}
	return nil
}

func (t *TailcatTransport) LocalAddress() (string, error) {
	t.mu.Lock()
	srv, state := t.server, t.state
	t.mu.Unlock()
	if state != tailcatStateStarted || srv == nil {
		return "", errors.New("transport: tailcat: LocalAddress called before Start completed")
	}
	return string(srv.TailcatAddr()), nil
}

func (t *TailcatTransport) Close() error {
	t.mu.Lock()
	if t.state == tailcatStateClosed {
		t.mu.Unlock()
		return nil
	}
	state := t.state
	t.state = tailcatStateClosed
	srv, ln, accepted := t.server, t.listener, t.accepted
	t.mu.Unlock()

	var errs []error

	// Stop accepting first, so no connection arrives after the drains below
	// have started, and wait for the accept loop so it never outlives Close.
	if ln != nil {
		_ = ln.Close()
		<-accepted
	}

	t.clientsMu.Lock()
	clients := t.clients
	t.clients = nil
	t.clientsMu.Unlock()
	// Each client owns its own userspace TCP stack, so closing one outright
	// drops whatever it still had queued to send — the outbound half of what
	// the server drain below guards against. One budget covers all of them:
	// by the time Close runs, the peer connections are themselves closed
	// (core.Node.Close waits out the goroutines that own them), so each
	// drain should return as soon as its endpoints have quiesced rather than
	// spending the budget.
	clientDrainCtx, cancelClientDrain := context.WithTimeout(context.Background(), drainTimeout)
	for _, pc := range clients {
		if pc.dialed.Load() {
			if err := pc.c.DrainTCP(clientDrainCtx); err != nil {
				t.logf("transport: tailcat: drain peer client: %v", err)
			}
		}
		if err := pc.c.Close(); err != nil {
			errs = append(errs, &OpError{Op: "close peer client", Err: err})
		}
	}
	cancelClientDrain()

	if srv != nil && state == tailcatStateStarted {
		// The same for the inbound half, on its own budget: exiting right
		// after closing a net.Conn can lose its final FIN before it's ever
		// transmitted. Bounded, because the peer may simply be gone.
		drainCtx, cancel := context.WithTimeout(context.Background(), drainTimeout)
		if err := srv.DrainTCP(drainCtx); err != nil {
			t.logf("transport: tailcat: drain: %v", err)
		}
		cancel()
		if err := srv.Close(); err != nil {
			errs = append(errs, fmt.Errorf("transport: tailcat: close server: %w", err))
		}
	}
	return errors.Join(errs...)
}

func redactingLogf(next func(string, ...any), secrets ...string) func(string, ...any) {
	return func(format string, args ...any) {
		msg := fmt.Sprintf(format, args...)
		for _, secret := range secrets {
			if secret != "" {
				msg = strings.ReplaceAll(msg, secret, "[redacted]")
			}
		}
		msg = strings.Map(func(r rune) rune {
			if r < 0x20 || r == 0x7f {
				return ' '
			}
			return r
		}, msg)
		const maxLogRunes = 1024
		runes := []rune(msg)
		if len(runes) > maxLogRunes {
			msg = string(runes[:maxLogRunes]) + "…"
		}
		next("%s", msg)
	}
}

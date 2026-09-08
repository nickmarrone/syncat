package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/tailscale/tailcat"
	"tailscale.com/wgengine/filter"
)

// SyncatPort is the TCP port the syncat protocol runs on, inside the
// tailcat tunnel (SPEC.md §4).
const SyncatPort uint16 = 4197

// drainTimeout bounds how long Close waits for the tailcat server's
// userspace TCP stack to flush final FINs before tearing it down.
const drainTimeout = 5 * time.Second

// TailcatTransport is the production [Transport], carrying the syncat
// protocol stream over a tailcat WireGuard tunnel relayed through DERP.
//
// Only port [SyncatPort] is ever served: [tailcat.Server.OnTCP] returns a
// handler for that port alone (everything else gets a RST), and
// [tailcat.Server.ServedTCPPorts] independently restricts the packet
// filter to the same port for defense in depth. Both are set before Start,
// as tailcat requires.
type TailcatTransport struct {
	key  *tailcat.PrivateKey // node identity; Public.RegionID is baked in (see config.LoadOrCreateTailcatKey)
	logf func(string, ...any)

	mu      sync.Mutex
	server  *tailcat.Server
	started bool
	closed  bool

	clientsMu sync.Mutex
	clients   map[string]*tailcat.Client // keyed by peer addr (tailcat.Addr string); reused across dials
}

// NewTailcatTransport returns a transport using key as this node's tailcat
// identity (see config.LoadOrCreateTailcatKey, which bakes in a concrete
// DERP RegionID so the resulting address is stable across runs). logf, if
// non-nil, receives tailcat's debug logging.
func NewTailcatTransport(key *tailcat.PrivateKey, logf func(string, ...any)) *TailcatTransport {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	return &TailcatTransport{key: key, logf: logf}
}

func (t *TailcatTransport) Start(ctx context.Context, onConn func(net.Conn)) error {
	if onConn == nil {
		return errors.New("transport: tailcat: Start: onConn must not be nil")
	}

	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return errors.New("transport: tailcat: Start called after Close")
	}
	if t.started {
		t.mu.Unlock()
		return errors.New("transport: tailcat: Start called twice")
	}

	srv := &tailcat.Server{
		Key:      t.key.Private,
		RegionID: t.key.Public.RegionID,
		Logf:     t.logf,
		// The pre-shared key is part of our address, so it has to be
		// restored from disk alongside Key: tailcat generates a fresh
		// random one whenever Server.PresharedKey is zero, which would
		// mint a different address on every daemon start and silently
		// invalidate every `sc1` token we have ever handed out. See
		// config.LoadOrCreateTailcatKey, which is what guarantees the
		// field is non-zero by the time we get here.
		PresharedKey: t.key.Public.PresharedKey,
	}
	srv.OnTCP = func(port uint16) func(net.Conn) {
		if port != SyncatPort {
			return nil // RST
		}
		return onConn
	}
	srv.ServedTCPPorts = []filter.PortRange{{First: SyncatPort, Last: SyncatPort}}
	t.server = srv
	t.mu.Unlock()

	// Server.Start does its own DERP-map fetch/region resolution using
	// context.Background() internally (it takes no context), so it isn't
	// directly cancelable. Run it in a goroutine and race it against ctx:
	// on ctx expiry we return promptly, but a Start that later succeeds in
	// the background is still reachable via t.server (already assigned
	// above) — a subsequent Close will shut it down rather than leaking it.
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Start() }()

	select {
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("transport: tailcat: start: %w", err)
		}
		t.mu.Lock()
		t.started = true
		t.mu.Unlock()
		return nil
	case <-ctx.Done():
		return fmt.Errorf("transport: tailcat: start: %w", ctx.Err())
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
// key here meant that the moment any client (i.e. any dial — including
// one started well after Start, like Node.AddPeer's) registered with
// DERP, it silently superseded the server's own registration, so a
// MeowPing another peer sent us landed on a Client's locoBackend instead
// of the Server's — and Client.onDERPRecv explicitly ignores MeowPing
// ("client ignores MeowPing"). The sender then just sits on
// Client.Ping's unconditional 10s internal timeout, forever, with no
// error surfaced anywhere below dialAttempt. See internal/core/peer.go's
// dialAttempt and the regression test for the full failure mode.
func (t *TailcatTransport) clientFor(addr string) (*tailcat.Client, error) {
	if addr == "" {
		return nil, errors.New("transport: tailcat: dial: address must not be empty")
	}

	t.clientsMu.Lock()
	defer t.clientsMu.Unlock()
	if c, ok := t.clients[addr]; ok {
		return c, nil
	}
	c := &tailcat.Client{
		Server: tailcat.Addr(addr),
		Logf:   t.logf,
	}
	if t.clients == nil {
		t.clients = make(map[string]*tailcat.Client)
	}
	t.clients[addr] = c
	return c, nil
}

func (t *TailcatTransport) Dial(ctx context.Context, addr string) (net.Conn, error) {
	c, err := t.clientFor(addr)
	if err != nil {
		return nil, err
	}
	// No sleep is needed before dialing: DialTCPPort blocks internally
	// (via Client.up) until the server has acked us as a peer.
	conn, err := c.DialTCPPort(ctx, SyncatPort)
	if err != nil {
		t.discardClient(addr, c)
		return nil, fmt.Errorf("transport: tailcat: dial %s: %w", addr, err)
	}
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
// the far side will not accept — forever, because each retry reuses the
// same latched Client.
//
// Observed as a striking asymmetry: restarting the *peer* wedged the link
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
func (t *TailcatTransport) discardClient(addr string, c *tailcat.Client) {
	t.clientsMu.Lock()
	cached, ok := t.clients[addr]
	if ok && cached == c {
		delete(t.clients, addr)
	} else {
		// Another dial already discarded this one and installed a
		// replacement; it owns closing what it removed.
		c = nil
	}
	t.clientsMu.Unlock()

	if c != nil {
		_ = c.Close()
	}
}

// DiscardPeer implements Transport. Unlike discardClient it is
// unconditional: the caller is telling us the peer's connection has ended,
// so whatever we hold for it is stale regardless of which Client it is.
func (t *TailcatTransport) DiscardPeer(addr string) {
	t.clientsMu.Lock()
	c := t.clients[addr]
	delete(t.clients, addr)
	t.clientsMu.Unlock()

	if c != nil {
		_ = c.Close()
	}
}

func (t *TailcatTransport) LocalAddress() (string, error) {
	t.mu.Lock()
	srv, started := t.server, t.started
	t.mu.Unlock()
	if !started || srv == nil {
		return "", errors.New("transport: tailcat: LocalAddress called before Start completed")
	}
	return string(srv.TailcatAddr()), nil
}

func (t *TailcatTransport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	srv := t.server
	t.mu.Unlock()

	var errs []error

	t.clientsMu.Lock()
	clients := t.clients
	t.clients = nil
	t.clientsMu.Unlock()
	for addr, c := range clients {
		if err := c.Close(); err != nil {
			errs = append(errs, fmt.Errorf("transport: tailcat: close client %s: %w", addr, err))
		}
	}

	if srv != nil {
		// tailcat's TCP stack is userspace: exiting right after closing a
		// net.Conn can lose its final FIN before it's ever transmitted, so
		// drain first (bounded — the peer may simply be gone).
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

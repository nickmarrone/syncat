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

	mu     sync.Mutex
	server tailcatServer
	state  tailcatTransportState
	// newServer is replaceable in tests so startup can be paused at the
	// exact lifecycle boundaries without contacting DERP.
	newServer func(func(net.Conn)) tailcatServer

	clientsMu sync.Mutex
	clients   map[string]*tailcat.Client // keyed by peer addr (tailcat.Addr string); reused across dials
}

type tailcatServer interface {
	Start() error
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

// NewTailcatTransport returns a transport using key as this node's tailcat
// identity (see config.LoadOrCreateTailcatKey, which bakes in a concrete
// DERP RegionID so the resulting address is stable across runs). logf, if
// non-nil, receives tailcat's debug logging.
func NewTailcatTransport(key *tailcat.PrivateKey, logf func(string, ...any)) *TailcatTransport {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	t := &TailcatTransport{key: key, logf: logf}
	t.newServer = func(onConn func(net.Conn)) tailcatServer {
		srv := &tailcat.Server{
			Key:      t.key.Private,
			RegionID: t.key.Public.RegionID,
			Logf:     t.logf,
			// A zero value makes tailcat generate a new PSK, changing this
			// node's address and invalidating persisted pairing tokens.
			PresharedKey: t.key.Public.PresharedKey,
		}
		srv.OnTCP = func(port uint16) func(net.Conn) {
			if port != SyncatPort {
				return nil
			}
			return onConn
		}
		srv.ServedTCPPorts = []filter.PortRange{{First: SyncatPort, Last: SyncatPort}}
		return srv
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
	srv := t.newServer(onConn)
	t.server = srv
	t.state = tailcatStateStarting
	t.mu.Unlock()

	// Server.Start does its own DERP-map fetch/region resolution using
	// context.Background() internally (it takes no context), so it isn't
	// directly cancelable. Run it in a goroutine and race it against ctx:
	// on ctx expiry we return promptly. The startup goroutine observes the
	// abandoned lifecycle state when it eventually returns and closes every
	// resource that late startup created.
	errCh := make(chan error, 1)
	go func() {
		err := srv.Start()
		t.mu.Lock()
		abandoned := t.state != tailcatStateStarting
		if !abandoned {
			if err == nil {
				t.state = tailcatStateStarted
			} else {
				t.state = tailcatStateFailed
			}
		}
		t.mu.Unlock()

		// Start may allocate resources before returning an error, and a
		// canceled/closed transport may finish successfully much later. The
		// startup goroutine is the only code that knows Start has returned,
		// so it owns cleanup in both cases.
		if err != nil || abandoned {
			_ = srv.Close()
		}
		if err == nil && abandoned {
			err = errors.New("transport closed during startup")
		}
		errCh <- err
	}()

	select {
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("transport: tailcat: start: %w", err)
		}
		return nil
	case <-ctx.Done():
		t.mu.Lock()
		state := t.state
		if state == tailcatStateStarting || state == tailcatStateStarted {
			t.state = tailcatStateClosed
		}
		t.mu.Unlock()
		// If startup won the mutex race and fully completed just before this
		// select chose cancellation, its goroutine no longer considers itself
		// abandoned, so this path owns the close.
		if state == tailcatStateStarted {
			_ = srv.Close()
		}
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
	return t.clientForLocked(addr), nil
}

// clientForLocked is clientFor with clientsMu already held. Dial uses it
// while transitioning from the lifecycle lock to the client-cache lock so
// Close cannot miss a newly created client.
func (t *TailcatTransport) clientForLocked(addr string) *tailcat.Client {
	if c, ok := t.clients[addr]; ok {
		return c
	}
	c := &tailcat.Client{
		Server: tailcat.Addr(addr),
		Logf:   t.logf,
	}
	if t.clients == nil {
		t.clients = make(map[string]*tailcat.Client)
	}
	t.clients[addr] = c
	return c
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
	c := t.clientForLocked(addr)
	t.clientsMu.Unlock()
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

	if srv != nil && state == tailcatStateStarted {
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

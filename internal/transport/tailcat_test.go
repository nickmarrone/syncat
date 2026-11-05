package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tailscale/tailcat"
	"tailscale.com/ipn/ipnstate"
	"tailscale.com/types/key"
)

// fakeTailcatServer stands in for tailcat.Server so startup can be paused at
// exact lifecycle boundaries without contacting DERP. Its Listen behaves like
// tailcat's: it blocks for startup, honors ctx while doing so (unless
// ignoreCtx models the window after the DERP-map fetch, where tailcat no
// longer consults ctx), and returns a listener for the one port.
type fakeTailcatServer struct {
	started   chan struct{}
	release   chan struct{}
	ignoreCtx bool
	startOnce sync.Once
	startErr  error
	closed    atomic.Int32
	ln        *fakeListener
	status    *ipnstate.Status // what Status reports; nil means no status
}

func (s *fakeTailcatServer) Listen(ctx context.Context, network, address string) (net.Listener, error) {
	if network != "tcp" || address != fmt.Sprintf(":%d", SyncatPort) {
		return nil, fmt.Errorf("fake: Listen(%q, %q), want tcp on SyncatPort only", network, address)
	}
	s.startOnce.Do(func() { close(s.started) })
	if s.release != nil {
		if s.ignoreCtx {
			<-s.release
		} else {
			select {
			case <-s.release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	if s.startErr != nil {
		return nil, s.startErr
	}
	s.ln = newFakeListener()
	return s.ln, nil
}
func (s *fakeTailcatServer) DrainTCP(context.Context) error { return nil }
func (s *fakeTailcatServer) Close() error {
	s.closed.Add(1)
	if s.ln != nil {
		_ = s.ln.Close()
	}
	return nil
}
func (s *fakeTailcatServer) TailcatAddr() tailcat.Addr { return "fake" }
func (s *fakeTailcatServer) Status() *ipnstate.Status  { return s.status }

// fakeListener is a net.Listener whose connections the test hands in.
type fakeListener struct {
	conns     chan net.Conn
	closed    chan struct{}
	closeOnce sync.Once
}

func newFakeListener() *fakeListener {
	return &fakeListener{conns: make(chan net.Conn), closed: make(chan struct{})}
}

func (l *fakeListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}
func (l *fakeListener) Close() error {
	l.closeOnce.Do(func() { close(l.closed) })
	return nil
}
func (l *fakeListener) Addr() net.Addr { return &net.TCPAddr{Port: int(SyncatPort)} }

func (l *fakeListener) isClosed() bool {
	select {
	case <-l.closed:
		return true
	default:
		return false
	}
}

func transportWithFakeServer(s *fakeTailcatServer) *TailcatTransport {
	t := NewTailcatTransport(tailcat.NewPrivateKey(), nil)
	t.newServer = func() tailcatServer { return s }
	return t
}

// TestTailcatStartCancellationReturnsPromptly pins that cancelling ctx during
// startup ends Start with the context error and leaves nothing running,
// synchronously: Listen honors ctx, so there is no late server to clean up
// after Start returns.
func TestTailcatStartCancellationReturnsPromptly(t *testing.T) {
	srv := &fakeTailcatServer{started: make(chan struct{}), release: make(chan struct{})}
	tr := transportWithFakeServer(srv)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- tr.Start(ctx, func(net.Conn) {}) }()
	<-srv.started
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("Start error = %v, want context cancellation", err)
	}
	if got := srv.closed.Load(); got != 1 {
		t.Fatalf("server closed %d times by the time Start returned, want once", got)
	}
	if _, err := tr.LocalAddress(); err == nil {
		t.Fatal("LocalAddress succeeded after canceled startup")
	}
	if err := tr.Start(context.Background(), func(net.Conn) {}); err == nil {
		t.Fatal("second Start succeeded after canceled startup")
	}
}

// TestTailcatCloseDuringStartCleansUpAfterStartReturns covers Close landing
// while Listen is in a stretch that no longer consults ctx. Close must not
// touch the server mid-startup, so Start, once Listen returns, owns closing
// both the listener and the server, and must not report success.
func TestTailcatCloseDuringStartCleansUpAfterStartReturns(t *testing.T) {
	srv := &fakeTailcatServer{started: make(chan struct{}), release: make(chan struct{}), ignoreCtx: true}
	tr := transportWithFakeServer(srv)
	result := make(chan error, 1)
	go func() { result <- tr.Start(context.Background(), func(net.Conn) {}) }()
	<-srv.started
	if err := tr.Close(); err != nil {
		t.Fatalf("Close during Start: %v", err)
	}
	if got := srv.closed.Load(); got != 0 {
		t.Fatalf("Close touched the server %d times while it was still starting, want 0", got)
	}
	close(srv.release)
	if err := <-result; err == nil {
		t.Fatal("Start succeeded after Close won the lifecycle race")
	}
	if got := srv.closed.Load(); got != 1 {
		t.Fatalf("server closed %d times, want once", got)
	}
	if !srv.ln.isClosed() {
		t.Fatal("listener from the abandoned startup was left open")
	}
}

func TestTailcatStartFailureClosesPartialServerAndStaysFailed(t *testing.T) {
	srv := &fakeTailcatServer{started: make(chan struct{}), startErr: errors.New("startup exploded")}
	tr := transportWithFakeServer(srv)
	if err := tr.Start(context.Background(), func(net.Conn) {}); err == nil {
		t.Fatal("Start succeeded despite server failure")
	}
	if got := srv.closed.Load(); got != 1 {
		t.Fatalf("partially started server closed %d times, want once", got)
	}
	if _, err := tr.LocalAddress(); err == nil {
		t.Fatal("LocalAddress succeeded after failed startup")
	}
	if _, err := tr.Dial(context.Background(), "fake"); err == nil {
		t.Fatal("Dial succeeded after failed startup")
	}
	if err := tr.Start(context.Background(), func(net.Conn) {}); err == nil {
		t.Fatal("second Start succeeded after failed startup")
	}
}

// TestTailcatAcceptLoopDeliversUntilClose pins the accept loop that replaced
// tailcat's OnTCP hook: each accepted connection reaches onConn, on its own
// goroutine so a slow handler cannot hold up the next one, and Close stops
// the loop before returning.
func TestTailcatAcceptLoopDeliversUntilClose(t *testing.T) {
	srv := &fakeTailcatServer{started: make(chan struct{})}
	tr := transportWithFakeServer(srv)

	block := make(chan struct{})
	got := make(chan net.Conn, 2)
	if err := tr.Start(context.Background(), func(c net.Conn) {
		got <- c
		<-block // a handler that never returns on its own
	}); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer close(block)

	for i := range 2 {
		a, b := net.Pipe()
		defer a.Close()
		defer b.Close()
		select {
		case srv.ln.conns <- a:
		case <-time.After(time.Second):
			t.Fatalf("accept loop did not take connection %d (blocked behind a running handler?)", i)
		}
		select {
		case c := <-got:
			if c != a {
				t.Fatalf("onConn got %v, want the accepted connection", c)
			}
		case <-time.After(time.Second):
			t.Fatalf("connection %d never reached onConn", i)
		}
	}

	if err := tr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !srv.ln.isClosed() {
		t.Fatal("Close left the listener open")
	}
	if got := srv.closed.Load(); got != 1 {
		t.Fatalf("server closed %d times, want once", got)
	}
}

func TestTailcatDiagnosticsDoNotExposeCredentialsOrControls(t *testing.T) {
	const credential = "tc-distinctive-private-credential"
	cause := fmt.Errorf("dial %s failed", credential)
	err := &OpError{Op: "dial", Err: cause}
	if strings.Contains(err.Error(), credential) {
		t.Fatalf("typed transport error exposed credential: %q", err)
	}
	if !errors.Is(err, cause) {
		t.Fatal("typed transport error did not retain its internal cause")
	}

	var logged string
	logf := redactingLogf(func(format string, args ...any) { logged = fmt.Sprintf(format, args...) }, credential)
	logf("failure at %s\nforged\x1b[31m", credential)
	if strings.Contains(logged, credential) || strings.ContainsAny(logged, "\r\n\x1b") {
		t.Fatalf("tailcat log was not safely redacted: %q", logged)
	}
}

// TestTailcatTransportEndToEnd exercises the production transport against
// the real Tailscale DERP relays. Reachability was confirmed from this
// sandbox by hand: all four *.ipn.dev relays respond, and Server.Start
// comes up with a live home region. It's guarded by -short: DERP setup plus a
// netcheck-based region pick takes a few real seconds, and this
// environment's netcheck reports udp=false, so every byte here is relayed
// rather than direct, hence the generous timeout.
func TestTailcatTransportEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("uses real DERP relays; skipped under -short")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	server := NewTailcatTransport(tailcat.NewPrivateKey(), t.Logf)
	client := NewTailcatTransport(tailcat.NewPrivateKey(), t.Logf)

	accepted := make(chan net.Conn, 1)
	if err := server.Start(ctx, func(c net.Conn) { accepted <- c }); err != nil {
		t.Fatalf("server Start: %v", err)
	}
	if err := client.Start(ctx, func(net.Conn) {}); err != nil {
		t.Fatalf("client Start: %v", err)
	}

	addr, err := server.LocalAddress()
	if err != nil {
		t.Fatalf("server LocalAddress: %v", err)
	}

	dialConn, err := client.Dial(ctx, addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}

	var acceptedConn net.Conn
	select {
	case acceptedConn = <-accepted:
	case <-ctx.Done():
		t.Fatal("timed out waiting for the server to accept the connection")
	}

	const toServer = "hello over tailcat"
	go func() {
		if _, err := io.WriteString(dialConn, toServer); err != nil {
			t.Errorf("client write: %v", err)
		}
	}()
	buf := make([]byte, len(toServer))
	if _, err := io.ReadFull(acceptedConn, buf); err != nil {
		t.Fatalf("server read: %v", err)
	}
	if string(buf) != toServer {
		t.Fatalf("server got %q, want %q", buf, toServer)
	}

	const toClient = "hello back over tailcat"
	go func() {
		if _, err := io.WriteString(acceptedConn, toClient); err != nil {
			t.Errorf("server write: %v", err)
		}
	}()
	buf2 := make([]byte, len(toClient))
	if _, err := io.ReadFull(dialConn, buf2); err != nil {
		t.Fatalf("client read: %v", err)
	}
	if string(buf2) != toClient {
		t.Fatalf("client got %q, want %q", buf2, toClient)
	}

	if err := dialConn.Close(); err != nil {
		t.Fatalf("dialConn Close: %v", err)
	}
	if err := acceptedConn.Close(); err != nil {
		t.Fatalf("acceptedConn Close: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("client transport Close: %v", err)
	}
	if err := server.Close(); err != nil {
		t.Fatalf("server transport Close: %v", err)
	}
}

func TestTailcatTransportLocalAddressBeforeStartFails(t *testing.T) {
	tr := NewTailcatTransport(tailcat.NewPrivateKey(), nil)
	if _, err := tr.LocalAddress(); err == nil {
		t.Fatal("LocalAddress before Start succeeded, want error")
	}
}

func TestTailcatTransportDialRejectsEmptyAddress(t *testing.T) {
	tr := NewTailcatTransport(tailcat.NewPrivateKey(), nil)
	if _, err := tr.Dial(context.Background(), ""); err == nil {
		t.Fatal("Dial(\"\") succeeded, want error")
	}
}

// TestTailcatTransportStaysReachableAfterDialing is the regression test for a
// bug that left every peer added at runtime stuck in "connecting" forever,
// while peers already in config.json at daemon startup connected fine.
//
// A syncat node runs a server continuously and also dials each configured
// peer (SPEC.md §2.2), so one TailcatTransport owns a tailcat.Server plus a
// tailcat.Client per peer. Each is a separate locoBackend holding its own
// DERP connection, and DERP routes to a node key by delivering to whichever
// local connection registered under it most recently. Setting Client.Key to
// this node's own server key therefore made our first outbound dial silently
// supersede our own server's DERP registration: inbound MeowPings landed on a
// Client's backend, which ignores them outright, and the peer dialing us sat
// on an internal timeout with no error surfaced anywhere.
//
// The invariant under test is exactly that: *after* this transport has dialed
// out, it must still be reachable by an inbound dial. A third transport does
// the inbound dial rather than the one we dialed, deliberately, SPEC.md
// §2.4's dedup rule exists because two nodes holding simultaneous tunnels to
// each other is a state the design avoids, so asserting on it here would test
// something syncat never relies on.
//
// TestTailcatTransportEndToEnd above cannot catch this: there the server never
// dials, so it never builds a Client to collide with itself.
func TestTailcatTransportStaysReachableAfterDialing(t *testing.T) {
	if testing.Short() {
		t.Skip("uses real DERP relays; skipped under -short")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	// subject is the node whose reachability the bug destroyed.
	subject := NewTailcatTransport(tailcat.NewPrivateKey(), t.Logf)
	target := NewTailcatTransport(tailcat.NewPrivateKey(), t.Logf)
	inbound := NewTailcatTransport(tailcat.NewPrivateKey(), t.Logf)

	subjectAccepted := make(chan net.Conn, 1)
	if err := subject.Start(ctx, func(c net.Conn) { subjectAccepted <- c }); err != nil {
		t.Fatalf("subject Start: %v", err)
	}
	defer subject.Close()
	if err := target.Start(ctx, func(net.Conn) {}); err != nil {
		t.Fatalf("target Start: %v", err)
	}
	defer target.Close()
	if err := inbound.Start(ctx, func(net.Conn) {}); err != nil {
		t.Fatalf("inbound Start: %v", err)
	}
	defer inbound.Close()

	subjectAddr, err := subject.LocalAddress()
	if err != nil {
		t.Fatalf("subject LocalAddress: %v", err)
	}
	targetAddr, err := target.LocalAddress()
	if err != nil {
		t.Fatalf("target LocalAddress: %v", err)
	}

	// Give the subject a Client of its own. Before the fix, this is the step
	// that cost the subject its inbound reachability.
	outConn, err := subject.Dial(ctx, targetAddr)
	if err != nil {
		t.Fatalf("subject dial target: %v", err)
	}
	defer outConn.Close()

	// The subject must still be reachable from outside.
	conn, err := inbound.Dial(ctx, subjectAddr)
	if err != nil {
		t.Fatalf("inbound dial subject after subject had dialled out (the regression): %v", err)
	}
	defer conn.Close()

	var accepted net.Conn
	select {
	case accepted = <-subjectAccepted:
	case <-ctx.Done():
		t.Fatal("subject never accepted an inbound connection after dialling out: its server is no longer reachable")
	}
	defer accepted.Close()

	want := []byte("syncat")
	go func() { conn.Write(want) }()
	got := make([]byte, len(want))
	if _, err := io.ReadFull(accepted, got); err != nil {
		t.Fatalf("read from accepted conn: %v", err)
	}
	if string(got) != string(want) {
		t.Errorf("got %q, want %q", got, want)
	}
}

// TestClientCacheDiscardsFailedDials pins the eviction that makes a peer
// restart recoverable.
//
// tailcat.Client.up latches on upDone: the meow that tells the *server* to
// add us as a WireGuard peer is sent once and never again. A Client cached
// across a peer restart therefore dials into a tunnel whose far side has
// forgotten us, forever, because every retry reuses the same latched
// Client. Discarding on failure is what forces the next dial to build a
// fresh Client and meow again.
func TestClientCacheDiscardsFailedDials(t *testing.T) {
	tr := NewTailcatTransport(tailcat.NewPrivateKey(), nil)
	const addr = "tc-cache-test"

	// Healthy peers must keep their warm Client, that is the point of
	// caching, and only failed dials are supposed to discard.
	first, err := tr.clientFor(addr)
	if err != nil {
		t.Fatalf("clientFor: %v", err)
	}
	again, err := tr.clientFor(addr)
	if err != nil {
		t.Fatalf("clientFor (second): %v", err)
	}
	if again != first {
		t.Fatal("clientFor built a second Client for the same address; the cache is not reusing")
	}

	tr.discardClient(addr, first)
	replacement, err := tr.clientFor(addr)
	if err != nil {
		t.Fatalf("clientFor after discard: %v", err)
	}
	if replacement == first {
		t.Fatal("clientFor returned the discarded Client; a peer restart would stay wedged")
	}

	// A stale pointer must not evict whatever replaced it, or one slow
	// failing dial could throw away a Client another dial just built.
	tr.discardClient(addr, first)
	still, err := tr.clientFor(addr)
	if err != nil {
		t.Fatalf("clientFor after stale discard: %v", err)
	}
	if still != replacement {
		t.Fatal("a stale discard evicted the replacement Client")
	}
}

// TestCloseDrainsOnlyDialedClients pins the guard behind peerClient.dialed.
//
// Close drains each cached client before closing it, but tailcat's
// Client.DrainTCP dereferences a netstack that only exists once a dial has
// started the client, unlike Client.Close, which tolerates a client that
// was never used. Dial caches a Client before the dial that starts it, so
// without the flag a Close landing in that window panics on a nil backend.
// No DERP is needed to hit it: an undialed client is undialed whether or
// not a relay exists.
func TestCloseDrainsOnlyDialedClients(t *testing.T) {
	tr := NewTailcatTransport(tailcat.NewPrivateKey(), nil)
	if _, err := tr.clientFor("tc-never-dialed"); err != nil {
		t.Fatalf("clientFor: %v", err)
	}
	if err := tr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestValidateAddr covers what a pasted token is refused for, and, just as
// importantly, what it is not. The rejected case is the one that used to be
// invisible: a pre-0.3.0 address is added without complaint and then never
// connects, because tailcat only refuses it inside Client.up where nothing
// surfaces the reason.
func TestValidateAddr(t *testing.T) {
	tr := NewTailcatTransport(tailcat.NewPrivateKey(), nil)

	current := tailcat.NewPrivateKey()
	current.Public.RegionID = 1

	// A pre-0.3.0 address: node key only, no disco key.
	legacy := *current
	legacy.Public.ServerDiscoPublic = tailcat.DiscoPublic{}

	// A current address that simply opted out of the pre-shared key, which
	// tailcat supports (`--psk=false`) and clients accept.
	noPSK := *current
	noPSK.Public.PresharedKey = tailcat.PresharedKey{}

	for _, tc := range []struct {
		name    string
		addr    string
		wantErr bool
	}{
		{"current", string(current.Public.Addr()), false},
		{"no preshared key", string(noPSK.Public.Addr()), false},
		{"legacy, no disco key", string(legacy.Public.Addr()), true},
		{"empty", "", true},
		{"not a tailcat address", "sc1AAAA", true},
		{"corrupt payload", "tc!!!not-base64!!!", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tr.ValidateAddr(tc.addr)
			if tc.wantErr && err == nil {
				t.Fatal("ValidateAddr accepted an address that cannot be dialed")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("ValidateAddr rejected a dialable address: %v", err)
			}
			// The error reaches the CLI and the API; like OpError, it must
			// not copy the credential-bearing address into either.
			if err != nil && tc.addr != "" && strings.Contains(err.Error(), tc.addr) {
				t.Errorf("ValidateAddr error %q leaks the address", err)
			}
		})
	}

	// The legacy rejection has to tell the operator what to do about it;
	// that is the whole reason for checking here rather than at dial time.
	err := tr.ValidateAddr(string(legacy.Public.Addr()))
	if err == nil {
		t.Fatal("legacy address accepted")
	}
	for _, want := range []string{"disco key", "syncat token"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("legacy address error %q does not mention %q", err, want)
		}
	}
}

// TestLocalAddressMatchesPersistedKeyAcrossRestarts is the regression test
// for the tailcat v0.6.0 upgrade: an address now carries a WireGuard
// pre-shared key, and tailcat.Server mints a throwaway one whenever
// Server.PresharedKey is left zero.
//
// syncat produces its address two different ways and both have to agree,
// the daemon serves LocalAddress() from a running Server, while `syncat
// token` derives it offline from the persisted PrivateKey.Public. Without
// feeding the persisted pre-shared key back into the Server, the daemon
// advertised a fresh random one on every start: `syncat token` printed a
// token that no longer reached the node, and every token already handed to
// a peer went stale at the next restart. Nothing failed loudly; peers just
// stopped connecting.
//
// The assertions deliberately compare the parsed key material rather than
// the address strings, which legitimately differ: LocalAddress() embeds the
// full DERP region while the offline form carries only a region id, and the
// region is re-picked by latency on each Start.
func TestLocalAddressMatchesPersistedKeyAcrossRestarts(t *testing.T) {
	if testing.Short() {
		t.Skip("starts a real tailcat server; skipped under -short")
	}

	key := tailcat.NewPrivateKey()
	if key.Public.PresharedKey.IsZero() {
		t.Fatal("tailcat.NewPrivateKey returned no pre-shared key")
	}

	// What `syncat token` would print, derived offline from the key alone.
	offline, err := tailcat.ParseAddr(key.Public.Addr())
	if err != nil {
		t.Fatalf("ParseAddr(persisted): %v", err)
	}

	// Two successive daemon lifetimes on the same persisted key.
	for run := 1; run <= 2; run++ {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)

		tr := NewTailcatTransport(key, t.Logf)
		if err := tr.Start(ctx, func(net.Conn) {}); err != nil {
			cancel()
			t.Fatalf("run %d: Start: %v", run, err)
		}

		addr, err := tr.LocalAddress()
		if err != nil {
			tr.Close()
			cancel()
			t.Fatalf("run %d: LocalAddress: %v", run, err)
		}
		served, err := tailcat.ParseAddr(tailcat.Addr(addr))
		if err != nil {
			tr.Close()
			cancel()
			t.Fatalf("run %d: ParseAddr(served): %v", run, err)
		}

		if !served.ServerPublic.Equal(offline.ServerPublic) {
			t.Errorf("run %d: served node key differs from the persisted one", run)
		}
		if !served.ServerDiscoPublic.Equal(offline.ServerDiscoPublic) {
			t.Errorf("run %d: served disco key differs from the persisted one", run)
		}
		if !served.PresharedKey.Equal(offline.PresharedKey) {
			t.Errorf("run %d: served pre-shared key differs from the persisted one; "+
				"the daemon is advertising an address `syncat token` cannot reproduce", run)
		}

		if err := tr.Close(); err != nil {
			t.Errorf("run %d: Close: %v", run, err)
		}
		cancel()
	}
}

// remoteConn is a net.Conn reporting a chosen remote address, standing in
// for a connection the server accepted from a client at that address.
type remoteConn struct {
	net.Conn
	remote net.Addr
}

func (c remoteConn) RemoteAddr() net.Addr { return c.remote }

// TestInboundPathMatchesConnectionToServerPeer pins how a connection a peer
// dialed to us is matched to its entry in Server.Status: by the tunnel
// address tailcat derives from the client's key, since the key itself is
// ephemeral and names no configured peer. With two clients connected, each
// connection must get its own client's path, not the other's.
func TestInboundPathMatchesConnectionToServerPeer(t *testing.T) {
	direct, relayed, stranger := key.NewNode().Public(), key.NewNode().Public(), key.NewNode().Public()
	srv := &fakeTailcatServer{started: make(chan struct{}), status: &ipnstate.Status{
		Peer: map[key.NodePublic]*ipnstate.PeerStatus{
			direct:  {CurAddr: "192.0.2.7:41641", Relay: "nyc"},
			relayed: {Relay: "fra"},
		},
	}}
	tr := transportWithFakeServer(srv)
	if err := tr.Start(context.Background(), func(net.Conn) {}); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { tr.Close() })

	connFrom := func(k key.NodePublic) net.Conn {
		return remoteConn{remote: &net.TCPAddr{IP: tailcatAddrForKey(k).AsSlice(), Port: 51000}}
	}

	got, err := tr.PeerPath(context.Background(), connFrom(direct), "unused", false)
	if err != nil {
		t.Fatalf("PeerPath(direct client): %v", err)
	}
	// A direct path reports the endpoint and not the relay, even though
	// magicsock always fills in the client's home region alongside it.
	if want := (PeerPath{Direct: true, Endpoint: "192.0.2.7:41641"}); got != want {
		t.Errorf("PeerPath(direct client) = %+v, want %+v", got, want)
	}

	got, err = tr.PeerPath(context.Background(), connFrom(relayed), "unused", false)
	if err != nil {
		t.Fatalf("PeerPath(relayed client): %v", err)
	}
	if want := (PeerPath{Relay: "fra"}); got != want {
		t.Errorf("PeerPath(relayed client) = %+v, want %+v", got, want)
	}

	if _, err := tr.PeerPath(context.Background(), connFrom(stranger), "unused", false); err == nil {
		t.Error("PeerPath reported a path for a connection no server peer carries")
	}
}

// TestPeerPathRefusesClientsThatNeverDialed pins the guard in front of
// DiscoPing, and pins that it is *fast*.
//
// DiscoPing calls Client.up, so handing it a client no dial has started
// would begin a whole connection attempt and block on Client.Ping's hard
// 10s internal timeout, which the caller's context cannot shorten. A sampler
// on a 2s budget would then be stuck for 10s. The elapsed-time assertion is
// the real one: an error alone would still pass after blocking for 10s.
func TestPeerPathRefusesClientsThatNeverDialed(t *testing.T) {
	tr := NewTailcatTransport(tailcat.NewPrivateKey(), nil)
	t.Cleanup(func() { tr.Close() })

	// Real, well-formed addresses that nothing is listening on. A bogus
	// string would prove nothing: tailcat rejects an unparseable address
	// immediately, so the test would pass with the guard removed.
	addrOf := func() string {
		k := tailcat.NewPrivateKey()
		k.Public.RegionID = 1
		return string(k.Public.Addr())
	}
	cached, unknown := addrOf(), addrOf()

	// A cached-but-undialed client is the case the dialed flag catches; an
	// unknown address is the case the nil check catches.
	if _, err := tr.clientFor(cached); err != nil {
		t.Fatalf("clientFor: %v", err)
	}
	for name, addr := range map[string]string{"cached, never dialed": cached, "never seen": unknown} {
		t.Run(name, func(t *testing.T) {
			start := time.Now()
			_, err := tr.PeerPath(context.Background(), nil, addr, true)
			elapsed := time.Since(start)
			if err == nil {
				t.Fatal("PeerPath reported a path for a client that never dialed")
			}
			if elapsed > 200*time.Millisecond {
				t.Errorf("PeerPath took %v to refuse; it reached tailcat instead of short-circuiting", elapsed)
			}
		})
	}
}

// TestPeerPathOverRealDERP checks that a live tunnel reports a coherent path
// in both directions: the dialer through DiscoPing, the acceptor through
// Server.Status.
//
// It deliberately does not assert *which* path. Whether two peers go direct
// or fall back to a relay is a property of the network the test runs on.
// What must hold either way is that each path describes itself completely:
// a direct one names the endpoint, a relayed one names the relay.
//
// The acceptor half is also what guards tailcatAddrForKey: if tailcat ever
// derives tunnel addresses differently, no server peer matches the
// accepted connection and PeerPath fails here.
func TestPeerPathOverRealDERP(t *testing.T) {
	if testing.Short() {
		t.Skip("uses real DERP relays; skipped under -short")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	server := NewTailcatTransport(tailcat.NewPrivateKey(), t.Logf)
	client := NewTailcatTransport(tailcat.NewPrivateKey(), t.Logf)
	t.Cleanup(func() { server.Close(); client.Close() })

	accepted := make(chan net.Conn, 1)
	if err := server.Start(ctx, func(c net.Conn) { accepted <- c }); err != nil {
		t.Fatalf("server Start: %v", err)
	}
	if err := client.Start(ctx, func(net.Conn) {}); err != nil {
		t.Fatalf("client Start: %v", err)
	}
	addr, err := server.LocalAddress()
	if err != nil {
		t.Fatalf("server LocalAddress: %v", err)
	}
	conn, err := client.Dial(ctx, addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()
	var inbound net.Conn
	select {
	case inbound = <-accepted:
		defer inbound.Close()
	case <-ctx.Done():
		t.Fatal("timed out waiting for the server to accept the connection")
	}

	checkCoherent := func(side string, path PeerPath) {
		t.Helper()
		switch {
		case path.Direct && path.Endpoint == "":
			t.Errorf("%s: direct path names no endpoint", side)
		case !path.Direct && path.Relay == "":
			t.Errorf("%s: relayed path names no relay", side)
		case path.Direct && path.Relay != "":
			t.Errorf("%s: direct path also names relay %q", side, path.Relay)
		}
		t.Logf("%s path: direct=%v endpoint=%q relay=%q", side, path.Direct, path.Endpoint, path.Relay)
	}

	// The production sampler runs on a 2s budget; hold this to the same
	// bound so a measurement slower than the sampler waits for shows up here.
	pathCtx, pathCancel := context.WithTimeout(ctx, 2*time.Second)
	defer pathCancel()
	out, err := client.PeerPath(pathCtx, conn, addr, true)
	if err != nil {
		t.Fatalf("dialer PeerPath over a live tunnel: %v", err)
	}
	checkCoherent("dialer", out)

	in, err := server.PeerPath(pathCtx, inbound, "", false)
	if err != nil {
		t.Fatalf("acceptor PeerPath over a live tunnel (tailcatAddrForKey out of step with tailcat?): %v", err)
	}
	checkCoherent("acceptor", in)
}

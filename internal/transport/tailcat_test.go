package transport

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/tailscale/tailcat"
)

// dialWithRetry dials addr, retrying until it succeeds or ctx expires.
//
// Transport.Start returns as soon as tailcat's Server.Start does, which is
// before that server has picked a home DERP region and connected to it —
// measured here at roughly 2.5–3s after Start returns. Until then the server
// is registered on no relay at all, and a dial's meow ping is a single DERP
// packet with no retry of its own: tailcat's Client.ping sends one and then
// waits on a hard internal 10s timeout, documented as applying "regardless
// of ctx". Dialing straight after Start therefore fires that one packet into
// the void and fails, deterministically and with a context-deadline error
// that says nothing about the real cause.
//
// Production never sees this, which is why it went unnoticed: syncat dials
// peers through transport.Supervisor (internal/core's runSupervisor), which
// retries with backoff, so a meow lost in the startup window costs one
// retry. These tests dial the transport directly, so they have to do the
// same thing themselves rather than assert on a window production never
// depends on.
func dialWithRetry(ctx context.Context, t *testing.T, tr *TailcatTransport, addr string) (net.Conn, error) {
	t.Helper()
	var lastErr error
	for attempt := 1; ctx.Err() == nil; attempt++ {
		conn, err := tr.Dial(ctx, addr)
		if err == nil {
			return conn, nil
		}
		lastErr = err
		t.Logf("dial attempt %d failed (retrying): %v", attempt, err)
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
		}
	}
	return nil, fmt.Errorf("no dial succeeded before the deadline: %w", lastErr)
}

// TestTailcatTransportEndToEnd exercises the production transport against
// the real Tailscale DERP relays. Reachability was confirmed from this
// sandbox by hand: all four *.ipn.dev relays respond, and Server.Start
// comes up with a live home region. It's guarded by -short: DERP setup plus a
// netcheck-based region pick takes a few real seconds, and this
// environment's netcheck reports udp=false, so every byte here is relayed
// rather than direct — hence the generous timeout.
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

	dialConn, err := dialWithRetry(ctx, t, client, addr)
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
// the inbound dial rather than the one we dialed, deliberately — SPEC.md
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
	outConn, err := dialWithRetry(ctx, t, subject, targetAddr)
	if err != nil {
		t.Fatalf("subject dial target: %v", err)
	}
	defer outConn.Close()

	// The subject must still be reachable from outside.
	conn, err := dialWithRetry(ctx, t, inbound, subjectAddr)
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

	// Healthy peers must keep their warm Client — that is the point of
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

// TestLocalAddressMatchesPersistedKeyAcrossRestarts is the regression test
// for the tailcat v0.6.0 upgrade: an address now carries a WireGuard
// pre-shared key, and tailcat.Server mints a throwaway one whenever
// Server.PresharedKey is left zero.
//
// syncat produces its address two different ways and both have to agree —
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

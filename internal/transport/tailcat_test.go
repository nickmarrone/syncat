package transport

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/tailscale/tailcat"
)

// TestTailcatTransportEndToEnd exercises the production transport against
// the real Tailscale DERP relays (confirmed reachable from this sandbox in
// Phase 0: all four *.ipn.dev relays reachable, Server.Start comes up with
// a live home region). It's guarded by -short: DERP setup plus a
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

	// No sleep before dialing: DialTCPPort blocks internally until the
	// server has acked the client as a peer.
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

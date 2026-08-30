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

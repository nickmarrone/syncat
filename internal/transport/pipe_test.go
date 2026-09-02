package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPipeTransportBidirectional(t *testing.T) {
	ctx := context.Background()

	const serverAddr = "pipe-bidi-server"
	server := NewPipeTransport(serverAddr)
	accepted := make(chan net.Conn, 1)
	if err := server.Start(ctx, func(c net.Conn) { accepted <- c }); err != nil {
		t.Fatalf("server Start: %v", err)
	}
	t.Cleanup(func() { server.Close() })

	client := NewPipeTransport("pipe-bidi-client")
	if err := client.Start(ctx, func(net.Conn) {}); err != nil {
		t.Fatalf("client Start: %v", err)
	}
	t.Cleanup(func() { client.Close() })

	dialConn, err := client.Dial(ctx, serverAddr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer dialConn.Close()

	var acceptedConn net.Conn
	select {
	case acceptedConn = <-accepted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for accept")
	}
	defer acceptedConn.Close()

	const clientMsg = "hello from client"
	if _, err := dialConn.Write([]byte(clientMsg)); err != nil {
		t.Fatalf("client write: %v", err)
	}
	buf := make([]byte, len(clientMsg))
	if _, err := io.ReadFull(acceptedConn, buf); err != nil {
		t.Fatalf("server read: %v", err)
	}
	if string(buf) != clientMsg {
		t.Fatalf("server got %q, want %q", buf, clientMsg)
	}

	const serverMsg = "hello from server, a longer reply"
	if _, err := acceptedConn.Write([]byte(serverMsg)); err != nil {
		t.Fatalf("server write: %v", err)
	}
	buf2 := make([]byte, len(serverMsg))
	if _, err := io.ReadFull(dialConn, buf2); err != nil {
		t.Fatalf("client read: %v", err)
	}
	if string(buf2) != serverMsg {
		t.Fatalf("client got %q, want %q", buf2, serverMsg)
	}
}

func TestPipeTransportConcurrentConnections(t *testing.T) {
	ctx := context.Background()

	const n = 20
	const serverAddr = "pipe-concurrent-server"
	server := NewPipeTransport(serverAddr)
	accepted := make(chan net.Conn, n)
	if err := server.Start(ctx, func(c net.Conn) { accepted <- c }); err != nil {
		t.Fatalf("server Start: %v", err)
	}
	t.Cleanup(func() { server.Close() })

	var dialWG sync.WaitGroup
	for i := 0; i < n; i++ {
		dialWG.Add(1)
		go func(i int) {
			defer dialWG.Done()
			client := NewPipeTransport(fmt.Sprintf("pipe-concurrent-client-%d", i))
			if err := client.Start(ctx, func(net.Conn) {}); err != nil {
				t.Errorf("client %d Start: %v", i, err)
				return
			}
			defer client.Close()

			conn, err := client.Dial(ctx, serverAddr)
			if err != nil {
				t.Errorf("client %d Dial: %v", i, err)
				return
			}
			defer conn.Close()

			if _, err := conn.Write([]byte(fmt.Sprintf("msg-%d", i))); err != nil {
				t.Errorf("client %d write: %v", i, err)
			}
		}(i)
	}

	var mu sync.Mutex
	got := make(map[string]bool)
	var readWG sync.WaitGroup
	for i := 0; i < n; i++ {
		readWG.Add(1)
		go func() {
			defer readWG.Done()
			select {
			case c := <-accepted:
				defer c.Close()
				buf := make([]byte, 64)
				readN, err := c.Read(buf)
				if err != nil {
					t.Errorf("server read: %v", err)
					return
				}
				mu.Lock()
				got[string(buf[:readN])] = true
				mu.Unlock()
			case <-time.After(5 * time.Second):
				t.Error("timed out waiting for an accepted connection")
			}
		}()
	}
	readWG.Wait()
	dialWG.Wait()

	if len(got) != n {
		t.Fatalf("got %d distinct messages, want %d: %v", len(got), n, got)
	}
}

func TestPipeTransportDialUnknownAddress(t *testing.T) {
	client := NewPipeTransport("pipe-unknown-dialer")
	if err := client.Start(context.Background(), func(net.Conn) {}); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { client.Close() })

	if _, err := client.Dial(context.Background(), "pipe-does-not-exist"); err == nil {
		t.Fatal("Dial to unknown address succeeded, want error")
	}
}

func TestPipeTransportDialAfterPeerClosed(t *testing.T) {
	const addr = "pipe-dial-after-close"
	server := NewPipeTransport(addr)
	if err := server.Start(context.Background(), func(net.Conn) {}); err != nil {
		t.Fatalf("server Start: %v", err)
	}
	if err := server.Close(); err != nil {
		t.Fatalf("server Close: %v", err)
	}

	client := NewPipeTransport("pipe-dial-after-close-client")
	if err := client.Start(context.Background(), func(net.Conn) {}); err != nil {
		t.Fatalf("client Start: %v", err)
	}
	t.Cleanup(func() { client.Close() })

	if _, err := client.Dial(context.Background(), addr); err == nil {
		t.Fatal("Dial to a closed (deregistered) peer succeeded, want error")
	}
}

func TestPipeTransportCloseIsCleanAndIdempotent(t *testing.T) {
	const addr = "pipe-close-idempotent"
	tr := NewPipeTransport(addr)
	if err := tr.Start(context.Background(), func(net.Conn) {}); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := tr.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := tr.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	// The address should be free for reuse by a fresh transport once the
	// original owner has closed.
	other := NewPipeTransport(addr)
	if err := other.Start(context.Background(), func(net.Conn) {}); err != nil {
		t.Fatalf("re-registering %q after Close: %v", addr, err)
	}
	t.Cleanup(func() { other.Close() })
}

func TestPipeTransportStartTwiceFails(t *testing.T) {
	tr := NewPipeTransport("pipe-start-twice")
	if err := tr.Start(context.Background(), func(net.Conn) {}); err != nil {
		t.Fatalf("first Start: %v", err)
	}
	t.Cleanup(func() { tr.Close() })
	if err := tr.Start(context.Background(), func(net.Conn) {}); err == nil {
		t.Fatal("second Start succeeded, want error")
	}
}

func TestPipeTransportLocalAddressBeforeStartFails(t *testing.T) {
	tr := NewPipeTransport("pipe-local-address-before-start")
	if _, err := tr.LocalAddress(); err == nil {
		t.Fatal("LocalAddress before Start succeeded, want error")
	}
}

// --- net.Conn contract: what a real socket gives us for free -----------

// TestPipeTransportReadDeadlineInterruptsBlockedRead pins the property
// internal/protocol's Handshake depends on: a deadline set on a Read that
// is *already* blocked interrupts it.
//
// This is a regression test for the pipe transport itself. The hand-rolled
// buffered net.Conn this transport used to be evaluated its read deadline
// once, at the top of Read, so a SetDeadline arriving afterwards could not
// unblock anything — and Handshake's ctx watcher does precisely that, on a
// connection another goroutine is already blocked reading. Nothing failed;
// the cancellation path simply never worked over the test transport, and
// no test could tell.
func TestPipeTransportReadDeadlineInterruptsBlockedRead(t *testing.T) {
	conn, peer := dialedPair(t)
	defer conn.Close()
	defer peer.Close()

	readErr := make(chan error, 1)
	go func() {
		buf := make([]byte, 1)
		_, err := conn.Read(buf) // the peer never writes; this blocks
		readErr <- err
	}()

	// Give the Read time to actually block before arming the deadline —
	// the point of the test is the deadline landing on an in-flight Read,
	// not on one that hasn't started.
	time.Sleep(50 * time.Millisecond)
	if err := conn.SetReadDeadline(time.Now()); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}

	select {
	case err := <-readErr:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("blocked Read returned %v, want os.ErrDeadlineExceeded", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SetReadDeadline did not interrupt a Read that was already blocked")
	}
}

// TestPipeTransportWriteFailsAfterPeerClose pins the other half of the
// contract: writing to a peer that has gone away eventually fails, so the
// error paths above this package are reachable from a test at all. The
// previous in-memory pipe queued such writes forever and reported success.
func TestPipeTransportWriteFailsAfterPeerClose(t *testing.T) {
	conn, peer := dialedPair(t)
	defer conn.Close()

	if err := peer.Close(); err != nil {
		t.Fatalf("peer Close: %v", err)
	}

	// The first write after the peer's close usually succeeds — it lands
	// in the send buffer before the RST arrives — so keep writing until
	// one fails. A write deadline keeps a wedged socket from hanging the
	// test instead of failing it.
	payload := make([]byte, 1024)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := conn.SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
			t.Fatalf("SetWriteDeadline: %v", err)
		}
		if _, err := conn.Write(payload); err != nil {
			return // expected: EPIPE/ECONNRESET, or the deadline
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("writes to a closed peer kept succeeding; a broken connection must eventually report itself")
}

// dialedPair returns the two ends of one connection between a pair of
// PipeTransports, as if `dialed` opened it and `accepted` received it.
func dialedPair(t *testing.T) (dialed, accepted net.Conn) {
	t.Helper()
	ctx := context.Background()
	n := atomic.AddInt64(&pairCounter, 1)

	serverAddr := fmt.Sprintf("pipe-pair-server-%d", n)
	server := NewPipeTransport(serverAddr)
	inbound := make(chan net.Conn, 1)
	if err := server.Start(ctx, func(c net.Conn) { inbound <- c }); err != nil {
		t.Fatalf("server Start: %v", err)
	}
	t.Cleanup(func() { server.Close() })

	client := NewPipeTransport(fmt.Sprintf("pipe-pair-client-%d", n))
	if err := client.Start(ctx, func(net.Conn) {}); err != nil {
		t.Fatalf("client Start: %v", err)
	}
	t.Cleanup(func() { client.Close() })

	dialed, err := client.Dial(ctx, serverAddr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	select {
	case accepted = <-inbound:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the accept")
	}
	return dialed, accepted
}

var pairCounter int64

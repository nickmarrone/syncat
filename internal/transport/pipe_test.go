package transport

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
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

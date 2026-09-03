package sync

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"net"
	"testing"
	"time"

	"github.com/nickmarrone/syncat/internal/protocol"
)

// --- Session.Done: noticing a connection ended ------------------------

// TestSessionDoneClosesWhenPeerDisconnects is the property Done exists for.
//
// Nothing else in a Session reacts to a disconnect, so before Done the only
// thing that noticed one was protocol.Keepalive's dead timer — SPEC.md §4's
// full 90s, even for a peer that closed cleanly and immediately. The
// timeout here is deliberately far below that: passing it on the dead
// timer alone is impossible.
func TestSessionDoneClosesWhenPeerDisconnects(t *testing.T) {
	a, b := newTestNode(t, "a"), newTestNode(t, "b")
	sa, sb := connectSessions(t, a, b, Direction{}, Direction{})

	// A graceful close, the case that used to cost 90s: the peer says
	// goodbye rather than the link breaking.
	if err := sb.Close(); err != nil {
		t.Fatalf("close peer session: %v", err)
	}

	select {
	case <-sa.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("Done stayed open after the peer disconnected; the owner would wait out the 90s dead rule instead")
	}
}

// TestSessionDoneClosesOnOwnClose covers the other way a read loop ends, so
// an owner selecting on Done can't be left waiting by its own teardown.
func TestSessionDoneClosesOnOwnClose(t *testing.T) {
	a, b := newTestNode(t, "a"), newTestNode(t, "b")
	sa, _ := connectSessions(t, a, b, Direction{}, Direction{})

	if err := sa.Close(); err != nil {
		t.Fatalf("close session: %v", err)
	}
	select {
	case <-sa.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("Done stayed open after Close")
	}
}

// --- pullStallTimeout: a peer that answers with silence ---------------

// silentPeerSession returns a started Session whose peer end is drained but
// never answered: every frame the session writes is read off the wire and
// dropped. That is the peer SPEC.md §4/§5 promise doesn't exist — one that
// replies to a FileRequest with neither a chunk nor an Error — and the only
// thing pullFile can do about it is give up.
func silentPeerSession(t *testing.T, stall time.Duration) *Session {
	t.Helper()
	near, far := net.Pipe()
	go func() { _, _ = io.Copy(io.Discard, far) }()

	n := newTestNode(t, "n")
	s := NewSession(near, n.store, n.id, "peer", nil, log.New(io.Discard, "", 0))
	s.testPullStallTimeout = stall
	ctx, cancel := context.WithCancel(context.Background())
	s.Start(ctx)
	t.Cleanup(func() {
		cancel()
		_ = s.Close()
		_ = far.Close()
	})
	return s
}

func TestPullFileStallsWhenPeerNeverAnswers(t *testing.T) {
	s := silentPeerSession(t, 300*time.Millisecond)

	var buf bytes.Buffer
	n, err := s.pullFile(context.Background(), testShareID, "a.txt", nil, &buf)
	if !errors.Is(err, ErrTransferStalled) {
		t.Fatalf("pullFile = (%d, %v), want ErrTransferStalled", n, err)
	}

	// The stuck pull was never the real harm: it held one of
	// maxConcurrentPulls slots for as long as the session lived, so four of
	// them wedged every transfer with that peer while the connection still
	// looked perfectly healthy — pings, index updates and share lists kept
	// flowing, so nothing declared it dead. Releasing the slot is the point.
	if held := len(s.pullSem); held != 0 {
		t.Errorf("%d of %d pull slots still held after the stall, want 0", held, maxConcurrentPulls)
	}
	s.pullMu.Lock()
	_, inFlight := s.pullTbl[transferKey{testShareID, "a.txt"}]
	s.pullMu.Unlock()
	if inFlight {
		t.Error("the transfer is still registered after the stall; a retry would be rejected as already in flight")
	}
}

// TestPullFileStallTimerResetsOnEachChunk pins the other half of the
// timeout: it bounds the gap between chunks, not the transfer. A large file
// over a slow link takes arbitrarily longer than pullStallTimeout to
// arrive, and must not be killed for it.
func TestPullFileStallTimerResetsOnEachChunk(t *testing.T) {
	const (
		stall = 1 * time.Second
		gap   = 200 * time.Millisecond
		semi  = 7 // chunks before EOF; 8 gaps total, so 1.6s > stall
	)
	s := silentPeerSession(t, stall)

	relpath := "big.bin"
	key := transferKey{testShareID, relpath}
	fed := make(chan struct{})
	go func() {
		defer close(fed)
		// pullFile registers its transfer before it can receive anything, so
		// wait for that rather than racing it. If it never appears, the pull
		// below simply stalls and fails the test with that.
		for i := 0; i < 400; i++ {
			s.pullMu.Lock()
			_, ok := s.pullTbl[key]
			s.pullMu.Unlock()
			if ok {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		for i := 0; i < semi; i++ {
			time.Sleep(gap)
			s.routeChunk(testShareID, relpath, pullChunk{data: []byte("x")})
		}
		time.Sleep(gap)
		s.routeChunk(testShareID, relpath, pullChunk{eof: true})
	}()

	var buf bytes.Buffer
	n, err := s.pullFile(context.Background(), testShareID, relpath, nil, &buf)
	<-fed
	if err != nil {
		t.Fatalf("pullFile = %v, want it to survive %d chunks %v apart with a %v timeout", err, semi, gap, stall)
	}
	if n != semi {
		t.Errorf("pulled %d bytes, want %d", n, semi)
	}
	if buf.Len() != semi {
		t.Errorf("wrote %d bytes to dst, want %d", buf.Len(), semi)
	}
}

// TestPullFileWriterNotStartedBeforeStart is the failure mode the
// StreamWriter's Start/enqueue split introduces: a pull on a Session whose
// writer was never started must report that plainly rather than blocking or
// silently dropping the request.
func TestPullFileWriterNotStartedBeforeStart(t *testing.T) {
	near, far := net.Pipe()
	t.Cleanup(func() { _ = near.Close(); _ = far.Close() })

	n := newTestNode(t, "n")
	s := NewSession(near, n.store, n.id, "peer", nil, log.New(io.Discard, "", 0))

	var buf bytes.Buffer
	_, err := s.pullFile(context.Background(), testShareID, "a.txt", nil, &buf)
	if !errors.Is(err, protocol.ErrWriterNotStarted) {
		t.Errorf("pullFile before Start = %v, want ErrWriterNotStarted", err)
	}
}

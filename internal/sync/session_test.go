package sync

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"net"
	"strings"
	stdsync "sync"
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

func TestSessionStartHasExplicitLifecycleErrors(t *testing.T) {
	near, far := net.Pipe()
	defer far.Close()
	n := newTestNode(t, "n")
	s := NewSession(near, n.store, n.id, "peer", nil, log.New(io.Discard, "", 0))
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("first Start = %v", err)
	}
	if err := s.Start(context.Background()); !errors.Is(err, ErrSessionAlreadyStarted) {
		t.Fatalf("second Start = %v, want ErrSessionAlreadyStarted", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}
	if err := s.Start(context.Background()); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("Start after Close = %v, want ErrSessionClosed", err)
	}
}

func TestSessionConcurrentStartsLaunchExactlyOnce(t *testing.T) {
	near, far := net.Pipe()
	defer far.Close()
	n := newTestNode(t, "n")
	s := NewSession(near, n.store, n.id, "peer", nil, log.New(io.Discard, "", 0))
	defer s.Close()
	const callers = 32
	results := make(chan error, callers)
	var wg stdsync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- s.Start(context.Background())
		}()
	}
	wg.Wait()
	close(results)
	successes := 0
	for err := range results {
		if err == nil {
			successes++
		} else if !errors.Is(err, ErrSessionAlreadyStarted) {
			t.Fatalf("Start = %v, want success or ErrSessionAlreadyStarted", err)
		}
	}
	if successes != 1 {
		t.Fatalf("successful starts = %d, want 1", successes)
	}
}

func TestSessionCloseBeforeStartPreventsLaterStart(t *testing.T) {
	n := newTestNode(t, "n")
	s := NewSession(nil, n.store, n.id, "peer", nil, log.New(io.Discard, "", 0))
	if err := s.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}
	if err := s.Start(context.Background()); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("Start after Close = %v, want ErrSessionClosed", err)
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
	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
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
	n, err := s.pullFile(context.Background(), testShareID, "a.txt", nil, -1, &buf)
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
	inFlight := len(s.pullTbl) != 0
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
	fed := make(chan struct{})
	go func() {
		defer close(fed)
		// pullFile registers its transfer before it can receive anything, so
		// wait for that rather than racing it. If it never appears, the pull
		// below simply stalls and fails the test with that.
		for i := 0; i < 400; i++ {
			s.pullMu.Lock()
			var id string
			for key := range s.pullTbl {
				id = key.id
			}
			s.pullMu.Unlock()
			if id != "" {
				for i := 0; i < semi; i++ {
					time.Sleep(gap)
					s.routeChunk(id, pullChunk{header: protocol.FileChunkHeader{TransferID: id, ShareID: testShareID, RelPath: relpath, Offset: int64(i)}, data: []byte("x")})
				}
				time.Sleep(gap)
				s.routeChunk(id, pullChunk{header: protocol.FileChunkHeader{TransferID: id, ShareID: testShareID, RelPath: relpath, Offset: semi}, eof: true})
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()

	var buf bytes.Buffer
	n, err := s.pullFile(context.Background(), testShareID, relpath, nil, -1, &buf)
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
	_, err := s.pullFile(context.Background(), testShareID, "a.txt", nil, -1, &buf)
	if !errors.Is(err, protocol.ErrWriterNotStarted) {
		t.Errorf("pullFile before Start = %v, want ErrWriterNotStarted", err)
	}
}

func TestPullFileRejectsStaleTransferAndWrongOffset(t *testing.T) {
	s := silentPeerSession(t, time.Second)
	result := make(chan error, 1)
	go func() {
		var dst bytes.Buffer
		_, err := s.pullFile(context.Background(), testShareID, "a.txt", nil, 1, &dst)
		result <- err
	}()

	var id string
	waitFor(t, time.Second, func() bool {
		s.pullMu.Lock()
		defer s.pullMu.Unlock()
		for key := range s.pullTbl {
			id = key.id
		}
		return id != ""
	})
	// A late chunk from an earlier request has a different ID and must not
	// enter the new request's queue.
	s.routeChunk("22222222222222222222222222222222", pullChunk{data: []byte("stale")})
	s.pullMu.Lock()
	queued := len(s.pullTbl[transferKey{id: id}].ch)
	s.pullMu.Unlock()
	if queued != 0 {
		t.Fatalf("stale transfer queued %d chunks", queued)
	}

	s.routeChunk(id, pullChunk{header: protocol.FileChunkHeader{TransferID: id, ShareID: testShareID, RelPath: "a.txt", Offset: 1}, data: []byte("x")})
	if err := <-result; err == nil || !strings.Contains(err.Error(), "offset") {
		t.Fatalf("wrong-offset pull error = %v", err)
	}
}

func TestCancelTransferCancelsActiveServe(t *testing.T) {
	s := &Session{serveCancel: make(map[string]context.CancelFunc)}
	ctx, cancel := context.WithCancel(context.Background())
	const id = "11111111111111111111111111111111"
	if !s.registerServe(id, cancel) {
		t.Fatal("registerServe rejected new transfer")
	}
	s.cancelServe(id)
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("CancelTransfer did not cancel active serve")
	}
}

func TestSessionStatsCountStaleTransferFrames(t *testing.T) {
	now := time.Now()
	var logs bytes.Buffer
	s := NewSession(nil, nil, "self", "peer", func() time.Time { return now }, log.New(&logs, "", 0))
	for range 100 {
		s.routeChunk("11111111111111111111111111111111", pullChunk{})
	}
	stats := s.Stats()
	if stats.StaleTransferFrames != 100 || stats.ProtocolViolations != 100 {
		t.Fatalf("stale transfer stats = %+v, want 100 stale violations", stats)
	}
	if got := strings.Count(logs.String(), "unknown or finished transfer"); got != 1 {
		t.Fatalf("rate-limited log count = %d, want 1; logs: %s", got, logs.String())
	}
	now = now.Add(staleTransferLogInterval)
	s.routeChunk("22222222222222222222222222222222", pullChunk{})
	if got := strings.Count(logs.String(), "unknown or finished transfer"); got != 2 {
		t.Fatalf("log count after interval = %d, want 2", got)
	}
}

// --- locally-modified warnings ----------------------------------------

// newWarningSession builds a bare Session for exercising the warning
// bookkeeping directly. Nothing here touches the connection, so a nil conn
// is fine — the same trick session_test's trash helpers use.
func newWarningSession(t *testing.T) *Session {
	t.Helper()
	return NewSession(nil, nil, "self", "peerid", nil, log.New(io.Discard, "", 0))
}

func warningPaths(s *Session) []string {
	out := []string{}
	for _, w := range s.LocallyModifiedWarnings() {
		out = append(out, w.ShareID+"/"+w.RelPath)
	}
	return out
}

// TestRecordWarningDedupesPerPath is the property that keeps a standing
// condition from being recorded as an unbounded stream of events. A
// locally-modified file is re-detected by every reconcile pass for as long
// as it stays diverged; appending each detection grew the warning slice
// without bound for the life of the connection and made the UI repeat the
// same file once per pass.
func TestRecordWarningDedupesPerPath(t *testing.T) {
	s := newWarningSession(t)

	for i := 0; i < 10; i++ {
		isNew := s.recordWarning(LocallyModifiedWarning{ShareID: "sh1", RelPath: "notes.txt", Reason: "diverged"})
		if want := i == 0; isNew != want {
			t.Errorf("pass %d: recordWarning new = %v, want %v", i, isNew, want)
		}
	}

	got := warningPaths(s)
	if len(got) != 1 || got[0] != "sh1/notes.txt" {
		t.Fatalf("warnings = %v, want exactly one entry for sh1/notes.txt", got)
	}
}

// TestRecordWarningKeepsLatestPerPath: the retained entry must be the most
// recent one, so a revert (Reverted: true) replaces the earlier "flagged
// only" record rather than being swallowed by it.
func TestRecordWarningKeepsLatestPerPath(t *testing.T) {
	s := newWarningSession(t)

	s.recordWarning(LocallyModifiedWarning{ShareID: "sh1", RelPath: "notes.txt", Reason: "flagged"})
	s.recordWarning(LocallyModifiedWarning{ShareID: "sh1", RelPath: "notes.txt", Reason: "reverted", Reverted: true})

	got := s.LocallyModifiedWarnings()
	if len(got) != 1 {
		t.Fatalf("warnings = %d entries, want 1", len(got))
	}
	if !got[0].Reverted || got[0].Reason != "reverted" {
		t.Errorf("warning = %+v, want the later Reverted record", got[0])
	}
}

// TestRetainWarningsDropsConverged covers the other end of a warning's
// life. A reconcile pass sees the whole share, so the paths it flags are
// the complete set of currently-diverged files; anything still holding a
// warning has converged and its warning is stale. Without this the UI's
// warning list only ever grew — a divergence the user resolved stayed on
// screen for the life of the connection.
func TestRetainWarningsDropsConverged(t *testing.T) {
	s := newWarningSession(t)

	s.recordWarning(LocallyModifiedWarning{ShareID: "sh1", RelPath: "a.txt"})
	s.recordWarning(LocallyModifiedWarning{ShareID: "sh1", RelPath: "b.txt"})
	s.recordWarning(LocallyModifiedWarning{ShareID: "sh2", RelPath: "c.txt"})

	// b.txt converged; a.txt is still diverged. sh2 was not reconciled by
	// this pass and must be left entirely alone.
	if dropped := s.retainWarnings("sh1", map[string]bool{"a.txt": true}); dropped != 1 {
		t.Errorf("retainWarnings dropped %d, want 1", dropped)
	}

	got := warningPaths(s)
	want := map[string]bool{"sh1/a.txt": true, "sh2/c.txt": true}
	if len(got) != len(want) {
		t.Fatalf("warnings = %v, want %v", got, want)
	}
	for _, p := range got {
		if !want[p] {
			t.Errorf("unexpected surviving warning %q (all: %v)", p, got)
		}
	}

	// The index must still line up with the slice after the compaction:
	// a re-flag of a surviving path is not new, and a re-flag of the
	// dropped one is.
	if s.recordWarning(LocallyModifiedWarning{ShareID: "sh1", RelPath: "a.txt"}) {
		t.Error("re-flagging a still-standing warning reported it as new")
	}
	if !s.recordWarning(LocallyModifiedWarning{ShareID: "sh1", RelPath: "b.txt"}) {
		t.Error("re-flagging a dropped warning did not report it as new")
	}
}

// TestRetainWarningsNoopWhenAllStillFlagged guards the early return: a pass
// where nothing converged must not rebuild the index or disturb ordering.
func TestRetainWarningsNoopWhenAllStillFlagged(t *testing.T) {
	s := newWarningSession(t)
	s.recordWarning(LocallyModifiedWarning{ShareID: "sh1", RelPath: "a.txt"})
	s.recordWarning(LocallyModifiedWarning{ShareID: "sh1", RelPath: "b.txt"})

	if dropped := s.retainWarnings("sh1", map[string]bool{"a.txt": true, "b.txt": true}); dropped != 0 {
		t.Errorf("retainWarnings dropped %d, want 0", dropped)
	}
	if got := warningPaths(s); len(got) != 2 {
		t.Fatalf("warnings = %v, want both retained", got)
	}
}

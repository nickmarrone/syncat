package protocol

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"
)

type firstWriteGate struct {
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (w *firstWriteGate) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.entered); <-w.release })
	return len(p), nil
}

// --- StreamWriter -------------------------------------------------------

// startedWriter returns a StreamWriter over one end of a net.Pipe, with
// the far end returned for the test to read (or not read) from. net.Pipe is
// synchronous and unbuffered, which is exactly what these tests need: not
// reading the far end is what parks the writer goroutine mid-Write, the
// state every property below is about.
func startedWriter(t *testing.T, timeout time.Duration) (*StreamWriter, net.Conn) {
	t.Helper()
	near, far := net.Pipe()
	sw := NewStreamWriter(near, timeout)
	if err := sw.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = near.Close()
		_ = far.Close()
		_ = sw.Close()
	})
	return sw, far
}

// blockWriter fills the data lane (and hands one frame to the writer
// goroutine, which parks writing it to an unread pipe) so that every
// subsequent bulk frame has to queue. It returns once the writer cannot
// make progress.
func blockWriter(t *testing.T, sw *StreamWriter) {
	t.Helper()
	// dataQueueDepth queued plus one in the goroutine's hand. Enqueueing
	// exactly this many can't block; one more would.
	for i := 0; i < dataQueueDepth+1; i++ {
		if err := sw.WriteFileChunk(FileChunkHeader{ShareID: "s", RelPath: "f", Offset: int64(i)}, []byte("chunk")); err != nil {
			t.Fatalf("queue chunk %d: %v", i, err)
		}
	}
}

// TestStreamWriterPrioritizesControlFrames is the property the type exists
// for: a control frame is never stuck behind queued file bytes. Only the
// single frame already handed to the writer goroutine can precede it, so
// with the data lane full a Ping must still be at most the second frame on
// the wire.
func TestStreamWriterPrioritizesControlFrames(t *testing.T) {
	sw, far := startedWriter(t, time.Minute)
	blockWriter(t, sw)

	if err := sw.WriteMessage(MsgPong, Pong{}); err != nil {
		t.Fatalf("queue pong: %v", err)
	}

	if err := far.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	fr := NewReader(far)

	var before int
	for {
		typ, _, err := fr.ReadFrame()
		if err != nil {
			t.Fatalf("read frame: %v (saw %d data frames, never the ping)", err, before)
		}
		if typ == MsgPong {
			break
		}
		if typ != MsgFileChunk {
			t.Fatalf("read %s, want FileChunk or Ping", typ)
		}
		before++
	}
	if before > 1 {
		t.Errorf("ping arrived behind %d file chunks, want at most 1 (the one already being written)", before)
	}
}

func TestStreamWriterControlBackpressureIsCancelable(t *testing.T) {
	sw, _ := startedWriter(t, time.Minute)
	blockWriter(t, sw)

	for i := 0; i < ctrlQueueDepth; i++ {
		if err := sw.WriteMessage(MsgPing, Ping{}); err != nil {
			t.Fatalf("control %d: %v", i, err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if err := sw.WriteMessageContext(ctx, MsgPing, Ping{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("full control queue error = %v, want context deadline", err)
	}
	if sw.Err() != nil {
		t.Fatalf("ordinary queue pressure poisoned connection: %v", sw.Err())
	}
}

func TestStreamWriterObserverFiresAfterWriteNotEnqueue(t *testing.T) {
	sw, far := startedWriter(t, time.Minute)
	written := make(chan MsgType, 1)
	sw.SetWriteObserver(func(typ MsgType) { written <- typ })
	if err := sw.WriteMessage(MsgPing, Ping{}); err != nil {
		t.Fatal(err)
	}
	select {
	case typ := <-written:
		t.Fatalf("observer fired before socket write: %s", typ)
	case <-time.After(25 * time.Millisecond):
	}
	if _, _, err := NewReader(far).ReadFrame(); err != nil {
		t.Fatal(err)
	}
	select {
	case typ := <-written:
		if typ != MsgPing {
			t.Fatalf("observer type = %s", typ)
		}
	case <-time.After(time.Second):
		t.Fatal("observer did not fire after write")
	}
}

func TestStreamWriterBoundsControlBurstSoBulkProgresses(t *testing.T) {
	gate := &firstWriteGate{entered: make(chan struct{}), release: make(chan struct{})}
	sw := NewStreamWriterTo(gate)
	order := make(chan MsgType, ctrlQueueDepth+4)
	sw.SetWriteObserver(func(typ MsgType) { order <- typ })
	if err := sw.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sw.Close() })
	if err := sw.WriteFrame(MsgFileChunk, []byte{0}); err != nil {
		t.Fatal(err)
	}
	<-gate.entered
	if err := sw.WriteFrame(MsgFileChunk, []byte{1}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < ctrlQueueDepth; i++ {
		if err := sw.WriteMessage(MsgPing, Ping{}); err != nil {
			t.Fatal(err)
		}
	}
	close(gate.release)
	if typ := <-order; typ != MsgFileChunk {
		t.Fatalf("first type = %s", typ)
	}
	controls := 0
	for {
		select {
		case typ := <-order:
			if typ == MsgFileChunk {
				if controls > maxControlBurst {
					t.Fatalf("bulk waited behind %d controls", controls)
				}
				return
			}
			controls++
		case <-time.After(time.Second):
			t.Fatal("bulk frame was starved")
		}
	}
}

// TestStreamWriterWriteErrorIsTerminal checks that a broken socket poisons
// the writer rather than being retried frame after frame.
func TestStreamWriterWriteErrorIsTerminal(t *testing.T) {
	near, far := net.Pipe()
	sw := NewStreamWriter(near, time.Minute)
	if err := sw.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sw.Close() })

	if err := far.Close(); err != nil {
		t.Fatalf("close far end: %v", err)
	}
	if err := sw.WriteMessage(MsgPing, Ping{}); err != nil {
		t.Fatalf("queue ping: %v", err)
	}

	waitForErr(t, sw, "a write to a closed pipe")
	if err := sw.WriteMessage(MsgPong, Pong{}); err == nil {
		t.Error("a write after the terminal failure succeeded")
	}
}

// TestStreamWriterWriteDeadline covers the deadline itself: before it, no
// write on a live session connection had one, so a peer that stopped
// draining could park a writer indefinitely.
func TestStreamWriterWriteDeadline(t *testing.T) {
	sw, _ := startedWriter(t, 50*time.Millisecond)

	// Not blockWriter: this needs the write to actually be attempted and
	// time out, which the first frame does on its own against an unread pipe.
	if err := sw.WriteMessage(MsgPing, Ping{}); err != nil {
		t.Fatalf("queue ping: %v", err)
	}

	err := waitForErr(t, sw, "a write deadline expiring")
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Errorf("Err() = %v, want it to wrap os.ErrDeadlineExceeded", err)
	}
}

func TestStreamWriterRejectsWritesBeforeStart(t *testing.T) {
	near, far := net.Pipe()
	t.Cleanup(func() { _ = near.Close(); _ = far.Close() })

	sw := NewStreamWriter(near, time.Minute)
	if err := sw.WriteMessage(MsgPing, Ping{}); !errors.Is(err, ErrWriterNotStarted) {
		t.Errorf("WriteMessage before Start = %v, want ErrWriterNotStarted", err)
	}
}

// TestStreamWriterCloseWithoutStart makes sure a Session built over a
// connection it never drives (internal/sync's trash tests do exactly that)
// can be closed without hanging on a goroutine that was never launched.
func TestStreamWriterCloseWithoutStart(t *testing.T) {
	sw := NewStreamWriter(nil, 0)
	done := make(chan error, 1)
	go func() { done <- sw.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Close() = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close hung on a writer that was never started")
	}
}

func TestStreamWriterStartHasExplicitLifecycleErrors(t *testing.T) {
	sw := NewStreamWriterTo(io.Discard)
	if err := sw.Start(); err != nil {
		t.Fatalf("first Start = %v", err)
	}
	if err := sw.Start(); !errors.Is(err, ErrWriterAlreadyStarted) {
		t.Fatalf("second Start = %v, want ErrWriterAlreadyStarted", err)
	}
	if err := sw.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}
	if err := sw.Start(); !errors.Is(err, ErrWriterClosed) {
		t.Fatalf("Start after Close = %v, want ErrWriterClosed", err)
	}
}

func TestStreamWriterConcurrentStartsLaunchExactlyOnce(t *testing.T) {
	sw := NewStreamWriterTo(io.Discard)
	defer sw.Close()
	const callers = 32
	results := make(chan error, callers)
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- sw.Start()
		}()
	}
	wg.Wait()
	close(results)
	successes := 0
	for err := range results {
		if err == nil {
			successes++
		} else if !errors.Is(err, ErrWriterAlreadyStarted) {
			t.Fatalf("Start = %v, want success or ErrWriterAlreadyStarted", err)
		}
	}
	if successes != 1 {
		t.Fatalf("successful starts = %d, want 1", successes)
	}
}

func TestStreamWriterConcurrentStartAndCloseAreOrdered(t *testing.T) {
	for range 100 {
		sw := NewStreamWriterTo(io.Discard)
		startResult := make(chan error, 1)
		closeResult := make(chan error, 1)
		gate := make(chan struct{})
		go func() { <-gate; startResult <- sw.Start() }()
		go func() { <-gate; closeResult <- sw.Close() }()
		close(gate)
		startErr, closeErr := <-startResult, <-closeResult
		if closeErr != nil {
			t.Fatalf("Close = %v", closeErr)
		}
		if startErr != nil && !errors.Is(startErr, ErrWriterClosed) {
			t.Fatalf("Start = %v, want success or ErrWriterClosed", startErr)
		}
		if err := sw.Start(); !errors.Is(err, ErrWriterClosed) {
			t.Fatalf("Start after concurrent Close = %v, want ErrWriterClosed", err)
		}
	}
}

func TestStreamWriterConcurrentClosesBothWaitForWriter(t *testing.T) {
	gateWriter := &firstWriteGate{entered: make(chan struct{}), release: make(chan struct{})}
	sw := NewStreamWriterTo(gateWriter)
	if err := sw.Start(); err != nil {
		t.Fatal(err)
	}
	if err := sw.WriteMessage(MsgPing, Ping{}); err != nil {
		t.Fatal(err)
	}
	<-gateWriter.entered
	closed := make(chan error, 2)
	go func() { closed <- sw.Close() }()
	go func() { closed <- sw.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("Close returned before the active write exited: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(gateWriter.release)
	for range 2 {
		if err := <-closed; err != nil {
			t.Fatalf("Close = %v", err)
		}
	}
}

func waitForErr(t *testing.T, sw *StreamWriter, what string) error {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if err := sw.Err(); err != nil {
			return err
		}
		if time.Now().After(deadline) {
			t.Fatalf("Err() stayed nil after %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

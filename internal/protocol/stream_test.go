package protocol

import (
	"errors"
	"net"
	"os"
	"testing"
	"time"
)

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
	sw.Start()
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

	if err := sw.WriteMessage(MsgPing, Ping{}); err != nil {
		t.Fatalf("queue ping: %v", err)
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
		if typ == MsgPing {
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

// TestStreamWriterControlOverflowClosesConnection covers the deliberate
// asymmetry between the lanes. Blocking on a full control lane would
// reintroduce the very stall StreamWriter exists to prevent, because
// internal/sync's read loop queues its Pong inline — so an overflowing
// control lane is treated as a dead connection instead.
func TestStreamWriterControlOverflowClosesConnection(t *testing.T) {
	sw, _ := startedWriter(t, time.Minute)
	blockWriter(t, sw)

	var err error
	for i := 0; i < ctrlQueueDepth*4; i++ {
		if err = sw.WriteMessage(MsgPing, Ping{}); err != nil {
			break
		}
	}
	if err == nil {
		t.Fatalf("queued %d control frames onto a stalled connection without an error", ctrlQueueDepth*4)
	}
	if sw.Err() == nil {
		t.Error("Err() is nil after the control lane overflowed; the failure should be terminal for the session")
	}
	// The connection must be closed, so internal/sync's read loop EOFs and
	// the session unwinds through its normal disconnect path.
	if _, writeErr := sw.conn.Write([]byte{0}); writeErr == nil {
		t.Error("connection is still writable; the overflow should have closed it")
	}
}

// TestStreamWriterWriteErrorIsTerminal checks that a broken socket poisons
// the writer rather than being retried frame after frame.
func TestStreamWriterWriteErrorIsTerminal(t *testing.T) {
	near, far := net.Pipe()
	sw := NewStreamWriter(near, time.Minute)
	sw.Start()
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

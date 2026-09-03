package protocol

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// --- the session writer: one goroutine, two lanes ----------------------

// DefaultWriteTimeout bounds one frame's write on a live session. It is
// deliberately generous: a full 1 MiB FileChunk still only needs ~17 KB/s
// to beat it, which a DERP-relayed tunnel comfortably clears. Its job is
// not to police throughput but to make sure a peer that has stopped
// draining altogether eventually breaks the connection instead of parking
// a writer forever — before this existed, no write on a live session
// connection had a deadline of any kind.
const DefaultWriteTimeout = 60 * time.Second

// Queue depths for [StreamWriter]'s two lanes.
//
// The data lane is deliberately shallow: it exists to keep the socket fed
// while a transfer reads its next chunk off disk, not to buffer a transfer.
// Blocking there is the backpressure that stops a serving goroutine from
// racing ahead of the network.
//
// The control lane is sized to hold every control frame a healthy session
// could plausibly have outstanding at once (a ShareList, one
// SubscribeRequest or AccessUpdate per share, a Ping, and a transfer error
// per in-flight pull), because enqueueing there must never block — see
// [StreamWriter.enqueue].
const (
	ctrlQueueDepth = 32
	dataQueueDepth = 4
)

// ErrWriterClosed is returned by [StreamWriter]'s write methods after
// Close, or after a terminal write failure whose own error has already
// been reported to an earlier caller.
var ErrWriterClosed = errors.New("protocol: stream writer is closed")

// ErrWriterNotStarted is returned by [StreamWriter]'s write methods before
// Start. Writing to a session that hasn't started its writer is a
// programming error, not a runtime condition.
var ErrWriterNotStarted = errors.New("protocol: stream writer has not been started")

// StreamWriter serializes every frame written to one live session
// connection onto a single goroutine, fed by two queues: a priority lane
// for control frames and a shallow lane for bulk ones (FileChunk,
// IndexUpdate). It replaces sharing a [Writer] directly across every
// goroutine that writes to a session.
//
// The point is that no caller ever waits on a bulk write. With a shared
// [Writer], a 1 MiB FileChunk to a peer that has stopped reading holds the
// writer's mutex for as long as the stall lasts, and everything else
// blocks behind it — including the Pong that internal/sync's read loop
// sends inline when it sees a Ping. A blocked read loop stops draining the
// socket, which is exactly what makes the *peer's* writes stall, so two
// nodes each transferring a large file could deadlock outright with
// nothing to break the tie. Draining the control lane to empty before
// touching the data lane means a Ping, Pong, AccessUpdate or
// transfer-scoped Error is never queued behind file bytes, and the read
// loop's inline writes can't block at all.
//
// A write failure is terminal for the whole session: StreamWriter records
// it (see [StreamWriter.Err]), closes the connection — which EOFs the read
// loop, and so unwinds the session through the same path a peer
// disconnecting takes — and fails every later write.
//
// Construct with [NewStreamWriter], start with [StreamWriter.Start], and
// release with [StreamWriter.Close].
type StreamWriter struct {
	conn    net.Conn // nil is tolerated (no deadlines, no close-on-failure)
	w       io.Writer
	timeout time.Duration

	ctrl chan []byte
	data chan []byte

	started   atomic.Bool
	startOnce sync.Once
	stopOnce  sync.Once
	stop      chan struct{} // closed by Close or by a terminal failure
	done      chan struct{} // closed when the writer goroutine exits

	errMu sync.Mutex
	err   error
}

// NewStreamWriter returns a StreamWriter writing frames to conn, arming
// conn's write deadline for timeout on each frame (DefaultWriteTimeout if
// <= 0, no deadline if conn is nil). No goroutine runs until Start is
// called, so it is safe to construct one for a connection that may never
// be used.
func NewStreamWriter(conn net.Conn, timeout time.Duration) *StreamWriter {
	if timeout <= 0 {
		timeout = DefaultWriteTimeout
	}
	s := &StreamWriter{
		conn:    conn,
		timeout: timeout,
		ctrl:    make(chan []byte, ctrlQueueDepth),
		data:    make(chan []byte, dataQueueDepth),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	if conn != nil {
		s.w = conn
	}
	return s
}

// NewStreamWriterTo is [NewStreamWriter] for an arbitrary io.Writer, with
// no connection to set deadlines on or close. Used by this package's own
// tests; production always has a net.Conn.
func NewStreamWriterTo(w io.Writer) *StreamWriter {
	s := NewStreamWriter(nil, 0)
	s.w = w
	return s
}

// Start launches the writer goroutine. Safe to call more than once; only
// the first call does anything.
func (s *StreamWriter) Start() {
	s.startOnce.Do(func() {
		s.started.Store(true)
		go s.run()
	})
}

// Close stops the writer. Frames still queued are discarded rather than
// flushed: Close only happens as part of tearing a session down, and its
// caller has either already closed the connection or is about to.
//
// It returns the terminal write error, if the writer failed, or nil for a
// clean shutdown. Close blocks until the writer goroutine has exited,
// which requires any in-progress write to have returned — so callers that
// want it prompt should close the connection first (internal/sync.Session
// does).
func (s *StreamWriter) Close() error {
	started := s.started.Load()
	s.stopOnce.Do(func() { close(s.stop) })
	if started {
		<-s.done
	}
	return s.Err()
}

// Err returns the terminal write error, if any. A clean Close leaves it
// nil.
func (s *StreamWriter) Err() error {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.err
}

// WriteMessage CBOR-encodes v and queues it as a frame of the given type,
// on the control lane unless typ is a bulk type (see isBulk).
func (s *StreamWriter) WriteMessage(typ MsgType, v any) error {
	payload, err := encodeMessage(typ, v)
	if err != nil {
		return err
	}
	return s.WriteFrame(typ, payload)
}

// WriteFileChunk queues a MsgFileChunk frame (hdr followed by data's raw
// bytes) on the data lane, blocking while that lane is full. data is
// copied into the queued frame, so the caller may reuse its buffer as soon
// as this returns.
func (s *StreamWriter) WriteFileChunk(hdr FileChunkHeader, data []byte) error {
	payload, err := encodeFileChunk(hdr, data)
	if err != nil {
		return err
	}
	return s.WriteFrame(MsgFileChunk, payload)
}

// WriteFrame queues one frame of the given type with payload as its body.
// See [Writer.WriteFrame] for when to prefer WriteMessage/WriteFileChunk.
func (s *StreamWriter) WriteFrame(typ MsgType, payload []byte) error {
	frame, err := encodeFrame(typ, payload)
	if err != nil {
		return err
	}
	return s.enqueue(typ, frame)
}

// enqueue hands a built frame to the lane its type belongs on.
//
// The two lanes differ in what a full queue means. A full data lane is
// normal — the network is slower than the goroutine feeding it — so the
// caller waits, and that wait is the transfer's backpressure. A full
// control lane is not normal: control frames are small, drained ahead of
// everything else, and never produced in bulk, so ctrlQueueDepth of them
// outstanding means the socket itself has stopped accepting bytes. Waiting
// there would reintroduce the very stall this type exists to prevent (the
// read loop enqueues its Pong inline), so instead the connection is
// declared dead and torn down.
func (s *StreamWriter) enqueue(typ MsgType, frame []byte) error {
	if err := s.Err(); err != nil {
		return err
	}
	if !s.started.Load() {
		return ErrWriterNotStarted
	}

	if isBulk(typ) {
		select {
		case s.data <- frame:
			return nil
		case <-s.stop:
			return s.errOrClosed()
		}
	}

	select {
	case s.ctrl <- frame:
		return nil
	case <-s.stop:
		return s.errOrClosed()
	default:
		err := fmt.Errorf("protocol: stream writer: %d control frames are queued and unsent; the connection is not draining", ctrlQueueDepth)
		s.fail(err)
		return err
	}
}

// isBulk reports whether typ is a bulk frame, i.e. one big enough that
// letting a control frame queue behind it would matter. Everything else —
// including a message type this build doesn't know about — takes the
// priority lane, which is the safe default: a frame wrongly treated as
// control costs a queue slot, while a frame wrongly treated as bulk can
// delay a keepalive.
func isBulk(typ MsgType) bool {
	return typ == MsgFileChunk || typ == MsgIndexUpdate || typ == MsgIndexSnapshotBegin || typ == MsgIndexSnapshotBatch || typ == MsgIndexSnapshotEnd || typ == MsgIndexDeltaBatch
}

// run is the single writer goroutine: drain ctrl to empty, then take one
// frame from either lane, repeat.
func (s *StreamWriter) run() {
	defer close(s.done)
	for {
		// Priority pass: never touch the data lane while a control frame
		// is waiting.
		select {
		case frame := <-s.ctrl:
			if !s.writeFrame(frame) {
				return
			}
			continue
		default:
		}

		select {
		case frame := <-s.ctrl:
			if !s.writeFrame(frame) {
				return
			}
		case frame := <-s.data:
			if !s.writeFrame(frame) {
				return
			}
		case <-s.stop:
			return
		}
	}
}

// writeFrame writes one built frame, reporting whether the writer should
// keep going. A failure is terminal (see fail).
func (s *StreamWriter) writeFrame(frame []byte) bool {
	if s.w == nil {
		s.fail(errors.New("protocol: stream writer: no underlying writer"))
		return false
	}
	if s.conn != nil {
		if err := s.conn.SetWriteDeadline(time.Now().Add(s.timeout)); err != nil {
			s.fail(fmt.Errorf("protocol: stream writer: set write deadline: %w", err))
			return false
		}
	}
	// io.Copy gives a frame the stdlib's full-write/error checking rather
	// than assuming one Writer.Write consumed the entire buffer.
	_, err := io.Copy(s.w, bytes.NewReader(frame))
	if s.conn != nil {
		// Clear it again: the deadline is an absolute time, and leaving one
		// armed would poison a connection that is merely idle.
		_ = s.conn.SetWriteDeadline(time.Time{})
	}
	if err != nil {
		s.fail(fmt.Errorf("protocol: stream writer: write frame: %w", err))
		return false
	}
	return true
}

// fail records a terminal error, stops the writer, and closes the
// connection so the session's read loop unblocks and the whole session
// unwinds through its normal disconnect path.
func (s *StreamWriter) fail(err error) {
	s.errMu.Lock()
	if s.err == nil {
		s.err = err
	}
	s.errMu.Unlock()

	s.stopOnce.Do(func() { close(s.stop) })
	if s.conn != nil {
		_ = s.conn.Close()
	}
}

func (s *StreamWriter) errOrClosed() error {
	if err := s.Err(); err != nil {
		return err
	}
	return ErrWriterClosed
}

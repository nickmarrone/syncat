package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/fxamacker/cbor/v2"
)

const (
	// frameLenSize is the width of the big-endian length prefix (SPEC.md
	// §4).
	frameLenSize = 4

	// MaxFrameSize is the largest a frame's length prefix may declare
	// (SPEC.md §4: "Max frame 4 MiB"). The length prefix covers the 1-byte
	// type plus the payload, so MaxFrameSize is also the ceiling on
	// type+payload combined. A peer that sends a bigger length is
	// rejected before we ever allocate a buffer for the claimed size —
	// see [Reader.ReadFrame].
	MaxFrameSize = 4 * 1024 * 1024

	// MaxFileChunkData bounds a single FileChunk's raw payload (SPEC.md
	// §4: "≤1 MiB raw bytes"), comfortably under MaxFrameSize once the
	// small FileChunkHeader is added.
	MaxFileChunkData = 1 * 1024 * 1024
)

// Writer encodes messages onto an underlying io.Writer as length-prefixed
// frames (SPEC.md §4).
//
// Concurrency: a *Writer is safe for concurrent use by multiple goroutines
// — each WriteFrame/WriteMessage/WriteFileChunk call builds its complete
// frame in memory and writes it in one Write call under an internal mutex,
// so frames from different goroutines (e.g. interleaved chunks from
// several concurrent transfers, SPEC.md §4) are never torn or interleaved
// on the wire.
type Writer struct {
	mu sync.Mutex
	w  io.Writer
}

// NewWriter returns a Writer that writes frames to w.
func NewWriter(w io.Writer) *Writer {
	return &Writer{w: w}
}

// WriteFrame writes one frame of the given type with payload as its body.
// Most callers want [Writer.WriteMessage] or [Writer.WriteFileChunk]
// instead; WriteFrame is exposed for tests and for message types this
// package doesn't know about (forward compatibility).
func (w *Writer) WriteFrame(typ MsgType, payload []byte) error {
	bodyLen := 1 + len(payload)
	if bodyLen > MaxFrameSize {
		return fmt.Errorf("protocol: frame: %s body is %d bytes, exceeds max %d", typ, bodyLen, MaxFrameSize)
	}

	buf := make([]byte, frameLenSize+bodyLen)
	binary.BigEndian.PutUint32(buf[:frameLenSize], uint32(bodyLen))
	buf[frameLenSize] = byte(typ)
	copy(buf[frameLenSize+1:], payload)

	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := w.w.Write(buf); err != nil {
		return fmt.Errorf("protocol: frame: write %s: %w", typ, err)
	}
	return nil
}

// WriteMessage CBOR-encodes v as a map (via its `cbor:"..."` struct tags)
// and writes it as a frame of the given type.
func (w *Writer) WriteMessage(typ MsgType, v any) error {
	payload, err := cbor.Marshal(v)
	if err != nil {
		return fmt.Errorf("protocol: frame: encode %s: %w", typ, err)
	}
	return w.WriteFrame(typ, payload)
}

// WriteFileChunk writes a MsgFileChunk frame: hdr CBOR-encoded, followed
// immediately by the raw bytes in data (SPEC.md §4). data must be at most
// MaxFileChunkData bytes.
func (w *Writer) WriteFileChunk(hdr FileChunkHeader, data []byte) error {
	if len(data) > MaxFileChunkData {
		return fmt.Errorf("protocol: frame: file chunk data is %d bytes, exceeds max %d", len(data), MaxFileChunkData)
	}
	hdrBytes, err := cbor.Marshal(hdr)
	if err != nil {
		return fmt.Errorf("protocol: frame: encode file chunk header: %w", err)
	}
	payload := make([]byte, 0, len(hdrBytes)+len(data))
	payload = append(payload, hdrBytes...)
	payload = append(payload, data...)
	return w.WriteFrame(MsgFileChunk, payload)
}

// Reader decodes length-prefixed frames from an underlying io.Reader
// (SPEC.md §4).
//
// Concurrency: a *Reader is NOT safe for concurrent use. Frames are
// inherently ordered on a single stream, so callers must serialize calls
// to ReadFrame — typically one read loop per connection, dispatching
// decoded messages to other goroutines as needed.
type Reader struct {
	r io.Reader
}

// NewReader returns a Reader that reads frames from r.
func NewReader(r io.Reader) *Reader {
	return &Reader{r: r}
}

// ReadFrame reads and returns the next frame's type and payload.
//
// A length prefix over MaxFrameSize is rejected immediately, without
// allocating a buffer of that size, so a hostile peer cannot use an
// oversized length header to force a large allocation — the only
// allocation ReadFrame ever performs is exactly the (bounded) body length
// it just validated.
//
// Errors are always returned, never panics, even for a truncated header,
// a truncated payload, or a zero-length body. Read errors from r
// (including io.EOF at a frame boundary) are wrapped and returned as-is
// where the caller benefits from checking with errors.Is(err, io.EOF); a
// clean close between frames yields io.EOF, a close mid-frame yields a
// wrapped io.ErrUnexpectedEOF.
func (r *Reader) ReadFrame() (MsgType, []byte, error) {
	var lenBuf [frameLenSize]byte
	if _, err := io.ReadFull(r.r, lenBuf[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return 0, nil, io.EOF
		}
		return 0, nil, fmt.Errorf("protocol: frame: read length header: %w", err)
	}

	bodyLen := binary.BigEndian.Uint32(lenBuf[:])
	if bodyLen == 0 {
		return 0, nil, errors.New("protocol: frame: zero-length frame (missing type byte)")
	}
	if bodyLen > MaxFrameSize {
		return 0, nil, fmt.Errorf("protocol: frame: declared body is %d bytes, exceeds max %d", bodyLen, MaxFrameSize)
	}

	body := make([]byte, bodyLen)
	if _, err := io.ReadFull(r.r, body); err != nil {
		return 0, nil, fmt.Errorf("protocol: frame: read %d byte body: %w", bodyLen, err)
	}

	return MsgType(body[0]), body[1:], nil
}

// DecodeMessage CBOR-decodes payload (as returned by [Reader.ReadFrame])
// into v. Fields present in payload but not in v's type are ignored
// (SPEC.md §11 forward compatibility) — this is fxamacker/cbor's default
// decode behavior, not an option enabled here.
func DecodeMessage(payload []byte, v any) error {
	if err := cbor.Unmarshal(payload, v); err != nil {
		return fmt.Errorf("protocol: frame: decode message: %w", err)
	}
	return nil
}

// DecodeFileChunk splits a MsgFileChunk frame's payload (as returned by
// [Reader.ReadFrame]) back into its CBOR header and raw data, using the
// CBOR decoder's own knowledge of how many bytes the header item occupied
// — the header is a self-delimiting CBOR value, so no separate length
// prefix between header and data is needed on the wire.
func DecodeFileChunk(payload []byte) (FileChunkHeader, []byte, error) {
	dec := cbor.NewDecoder(bytes.NewReader(payload))
	var hdr FileChunkHeader
	if err := dec.Decode(&hdr); err != nil {
		return FileChunkHeader{}, nil, fmt.Errorf("protocol: frame: decode file chunk header: %w", err)
	}
	n := dec.NumBytesRead()
	if n < 0 || n > len(payload) {
		return FileChunkHeader{}, nil, fmt.Errorf("protocol: frame: file chunk header consumed an invalid length %d", n)
	}
	return hdr, payload[n:], nil
}

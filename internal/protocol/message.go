// Package protocol implements the syncat wire format (SPEC.md §4):
// length-prefixed frames and CBOR message types (message.go), the
// mutually-authenticated Ed25519 handshake (handshake.go), and the
// Ping/Pong idle/dead-connection timing logic (keepalive.go).
//
// This package has no filesystem, UI, or HTTP dependencies (SPEC.md §12:
// it must stay gomobile-safe) and is testable entirely over a bare
// net.Conn; see internal/transport's PipeTransport for the in-process
// Transport used by this package's own tests.
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

// --- message types (SPEC.md §4) ----------------------------------------

// MsgType is the 1-byte frame type tag (SPEC.md §4).
type MsgType byte

// Message types, per SPEC.md §4's table. 0 is deliberately unused so a
// zero-valued MsgType (e.g. from a truncated or all-zero frame) is never
// mistaken for a real message.
const (
	MsgHello MsgType = 1 + iota
	MsgAuth
	MsgShareList
	MsgSubscribeRequest
	MsgAccessUpdate
	MsgIndexUpdate
	MsgFileRequest
	MsgFileChunk
	MsgPing
	MsgPong
	MsgError
	MsgFinished
	MsgIndexSyncRequest
	MsgIndexSnapshotBegin
	MsgIndexSnapshotBatch
	MsgIndexSnapshotEnd
	MsgIndexDeltaBatch
	MsgIndexAck
)

func (t MsgType) String() string {
	switch t {
	case MsgHello:
		return "Hello"
	case MsgAuth:
		return "Auth"
	case MsgShareList:
		return "ShareList"
	case MsgSubscribeRequest:
		return "SubscribeRequest"
	case MsgAccessUpdate:
		return "AccessUpdate"
	case MsgIndexUpdate:
		return "IndexUpdate"
	case MsgFileRequest:
		return "FileRequest"
	case MsgFileChunk:
		return "FileChunk"
	case MsgPing:
		return "Ping"
	case MsgPong:
		return "Pong"
	case MsgError:
		return "Error"
	case MsgFinished:
		return "Finished"
	case MsgIndexSyncRequest:
		return "IndexSyncRequest"
	case MsgIndexSnapshotBegin:
		return "IndexSnapshotBegin"
	case MsgIndexSnapshotBatch:
		return "IndexSnapshotBatch"
	case MsgIndexSnapshotEnd:
		return "IndexSnapshotEnd"
	case MsgIndexDeltaBatch:
		return "IndexDeltaBatch"
	case MsgIndexAck:
		return "IndexAck"
	default:
		return fmt.Sprintf("MsgType(%d)", byte(t))
	}
}

// Forward-compat note (SPEC.md §11): every message below is a Go struct
// with explicit `cbor:"..."` string-keyed tags, so it round-trips through
// fxamacker/cbor as a CBOR map rather than an array. Decoding a map into a
// struct silently ignores keys the struct doesn't declare (this is
// fxamacker/cbor's default behavior, not an option we had to opt into) —
// so a future v2 peer can add fields to any of these messages and v1 nodes
// keep working. Do not switch any of these to array-shaped encoding
// (e.g. via `cbor:",toarray"`), and do not add ExtraDecErrorUnknownField
// anywhere in this package, or that guarantee breaks.

// VersionVector is a per-file version vector: node-short-id (the first 8
// bytes of a node's Ed25519 public key, hex-encoded — see
// config.IdentityKey.ShortID) mapped to that node's local counter for the
// file (SPEC.md §5). This package defines only the wire shape; the
// dominance/merge algebra lives in internal/sync.
type VersionVector map[string]uint64

// FileType is FileInfo's `type` field.
type FileType string

const (
	FileTypeFile    FileType = "file"
	FileTypeDir     FileType = "dir"
	FileTypeSymlink FileType = "symlink"
)

// FileInfo describes one file/dir/symlink entry in an IndexUpdate
// (SPEC.md §4). Deleted entries are tombstones (Deleted=true) rather than
// being omitted.
//
// Forward-compat (SPEC.md §11): this is encoded as a CBOR map, so v2's
// optional `blocks` field (FastCDC block list, §11) can be added later
// without a wire break — v1 decoders will simply ignore it, and v2
// decoders talking to a v1 peer will see it absent. Do not add that field
// yet; it's noted here only so the shape isn't accidentally closed off
// (e.g. by switching to array encoding).
type FileInfo struct {
	RelPath string        `cbor:"relpath"`
	Type    FileType      `cbor:"type"`
	Size    int64         `cbor:"size"`
	MTimeNS int64         `cbor:"mtime_ns"`
	Mode    uint32        `cbor:"mode"`
	SHA256  []byte        `cbor:"sha256"`
	Version VersionVector `cbor:"version"`
	Deleted bool          `cbor:"deleted"`
}

// Hello carries one endpoint's identity and fresh handshake nonce.
type Hello struct {
	ProtoVersion int    `cbor:"proto_version"`
	NodeName     string `cbor:"node_name"`
	Ed25519Pub   []byte `cbor:"ed25519_pub"`
	// Token is the sender's own sc1 node token (SPEC.md §2.3): carried so
	// that, once the pending-peer approval queue exists, approving a
	// pending peer alone suffices to complete peering without a second
	// round trip.
	Token string `cbor:"token"`
	// Nonce is exactly 32 bytes of crypto/rand output, fresh per
	// connection.
	Nonce []byte `cbor:"nonce"`
}

// Auth is each side's reply to the peer's Hello: a signature binding both
// nonces (see [Handshake] for the exact transcript and why the nonce order
// is direction-dependent).
type Auth struct {
	Sig []byte `cbor:"sig"`
}

// HelloAuth is the responder's atomic handshake flight. Keeping its Hello
// and proof in one frame prevents it from beginning another write before
// the initiator has accepted the Hello.
type HelloAuth struct {
	Hello Hello  `cbor:"hello"`
	Sig   []byte `cbor:"sig"`
}

// Finished confirms that the responder accepted the initiator's proof.
// Without it the initiator could enter the session before mutual
// authentication had completed at the responder.
type Finished struct{}

// Share access states, as seen by a subscriber (ShareListEntry.Access) or
// pushed by an offerer (AccessUpdate.Access).
const (
	AccessNone    = "none"
	AccessPending = "pending"
	AccessGranted = "granted"
	AccessDenied  = "denied"
	AccessRevoked = "revoked"
)

// ShareListEntry is one share in a ShareList message.
type ShareListEntry struct {
	ShareID          string `cbor:"share_id"`
	Name             string `cbor:"name"`
	Permission       string `cbor:"permission"`
	ApprovalRequired bool   `cbor:"approval_required"`
	// Access is one of AccessNone, AccessPending, or AccessGranted, from
	// the recipient's point of view.
	Access string `cbor:"access"`
}

// ShareList announces the shares visible to the receiving peer, sent on
// connect and whenever the sender's share list changes (SPEC.md §4).
type ShareList struct {
	Shares []ShareListEntry `cbor:"shares"`
}

// SubscribeRequest asks the offerer for access to one of its shares
// (subscriber → offerer, SPEC.md §4).
type SubscribeRequest struct {
	ShareID string `cbor:"share_id"`
}

// AccessUpdate reports a share-access decision (offerer → subscriber,
// SPEC.md §4). Access is one of AccessGranted, AccessDenied, or
// AccessRevoked.
type AccessUpdate struct {
	ShareID string `cbor:"share_id"`
	Access  string `cbor:"access"`
}

// IndexUpdate carries a share's file list: a full snapshot on first
// sync/reconnect, deltas after (SPEC.md §4).
type IndexUpdate struct {
	ShareID string     `cbor:"share_id"`
	Files   []FileInfo `cbor:"files"`
	Full    bool       `cbor:"full"`
}

// Index synchronization is an acknowledged, ordered stream. Sequence zero
// means that no change in the epoch has been applied yet.
type IndexSyncRequest struct {
	ShareID       string `cbor:"share_id"`
	Epoch         string `cbor:"epoch"`
	AppliedSeq    uint64 `cbor:"applied_seq"`
	SnapshotID    string `cbor:"snapshot_id,omitempty"`
	SnapshotBatch uint64 `cbor:"snapshot_batch,omitempty"`
}
type IndexSnapshotBegin struct {
	ShareID    string `cbor:"share_id"`
	SnapshotID string `cbor:"snapshot_id"`
	Epoch      string `cbor:"epoch"`
	HighSeq    uint64 `cbor:"high_seq"`
}
type IndexSnapshotBatch struct {
	ShareID    string     `cbor:"share_id"`
	SnapshotID string     `cbor:"snapshot_id"`
	Batch      uint64     `cbor:"batch"`
	Files      []FileInfo `cbor:"files"`
}
type IndexSnapshotEnd struct {
	ShareID    string `cbor:"share_id"`
	SnapshotID string `cbor:"snapshot_id"`
	BatchCount uint64 `cbor:"batch_count"`
}
type IndexDeltaEntry struct {
	Seq  uint64   `cbor:"seq"`
	File FileInfo `cbor:"file"`
}
type IndexDeltaBatch struct {
	ShareID string            `cbor:"share_id"`
	Epoch   string            `cbor:"epoch"`
	FromSeq uint64            `cbor:"from_seq"`
	ToSeq   uint64            `cbor:"to_seq"`
	Entries []IndexDeltaEntry `cbor:"entries"`
}
type IndexAck struct {
	ShareID    string `cbor:"share_id"`
	Epoch      string `cbor:"epoch"`
	AppliedSeq uint64 `cbor:"applied_seq"`
	SnapshotID string `cbor:"snapshot_id,omitempty"`
	Replay     bool   `cbor:"replay,omitempty"`
}

// FileRequest asks the holder of a file for its bytes, optionally resuming
// from Offset (puller → holder, SPEC.md §4).
type FileRequest struct {
	ShareID string        `cbor:"share_id"`
	RelPath string        `cbor:"relpath"`
	Version VersionVector `cbor:"version"`
	Offset  int64         `cbor:"offset"`
}

// FileChunkHeader is the small CBOR header preceding a FileChunk's raw
// bytes (SPEC.md §4). It is encoded and decoded separately from the raw
// data — see [Writer.WriteFileChunk] and [DecodeFileChunk] — because
// FileChunk's payload is CBOR header + raw bytes concatenated, not a
// single CBOR value.
type FileChunkHeader struct {
	ShareID string        `cbor:"share_id"`
	RelPath string        `cbor:"relpath"`
	Version VersionVector `cbor:"version"`
	Offset  int64         `cbor:"offset"`
	EOF     bool          `cbor:"eof"`
}

// Ping and Pong are empty keepalive messages (SPEC.md §4); see keepalive.go
// for the reusable idle/dead-connection timing logic that decides when to
// send one.
type Ping struct{}
type Pong struct{}

// Error codes used during the handshake (SPEC.md §4). Other phases may
// define additional codes for their own failure modes.
const (
	ErrCodeUnsupportedVersion = "unsupported_proto_version"
	ErrCodeUnauthorized       = "unauthorized"
	ErrCodeBadHello           = "bad_hello"
	ErrCodeBadAuth            = "bad_auth"

	// ErrCodeFileNotFound and ErrCodeVersionChanged are the
	// file-transfer-level error codes (SPEC.md §4/§5: "a peer requesting a
	// file we no longer have, or whose version moved on, gets an Error —
	// never a hung stream"). Unlike the handshake codes above, an Error
	// carrying one of these also sets ShareID/RelPath so the requester can
	// route it back to the specific FileRequest it answers.
	ErrCodeFileNotFound   = "file_not_found"
	ErrCodeVersionChanged = "version_changed"
)

// Error reports a protocol-level failure to the peer before closing the
// connection (SPEC.md §4). ShareID and RelPath are optional (omitempty):
// unset for connection-level errors (e.g. the handshake codes above), set
// for a file-transfer-level error answering a specific FileRequest so the
// puller can demultiplex it back to the right in-flight transfer. Adding
// these two fields is safe under SPEC.md §11 forward compatibility (see
// the package doc comment above): Error is CBOR-map encoded, so older
// decoders that don't know about them simply ignore them.
type Error struct {
	Code    string `cbor:"code"`
	Msg     string `cbor:"msg"`
	ShareID string `cbor:"share_id,omitempty"`
	RelPath string `cbor:"relpath,omitempty"`
}

// --- framing: length-prefixed frames on the wire -----------------------

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
	// TargetIndexBatchSize leaves ample room below MaxFrameSize for framing
	// and future fields.
	TargetIndexBatchSize = 3 * 1024 * 1024
)

// EncodedMessageSize returns the CBOR payload size used for batch planning.
func EncodedMessageSize(typ MsgType, v any) (int, error) {
	b, err := encodeMessage(typ, v)
	return len(b), err
}

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
	buf, err := encodeFrame(typ, payload)
	if err != nil {
		return err
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := io.Copy(w.w, bytes.NewReader(buf)); err != nil {
		return fmt.Errorf("protocol: frame: write %s: %w", typ, err)
	}
	return nil
}

// encodeFrame builds one complete on-the-wire frame — length prefix, type
// byte, payload — as a single self-contained buffer.
//
// Returning bytes rather than writing them is what lets [StreamWriter]
// queue a frame: the buffer owns its copy of payload, so a caller that
// reuses its payload slice across calls (Session.streamFile reads every
// chunk into one buffer) can't mutate a frame already sitting in the
// queue.
func encodeFrame(typ MsgType, payload []byte) ([]byte, error) {
	bodyLen := 1 + len(payload)
	if bodyLen > MaxFrameSize {
		return nil, fmt.Errorf("protocol: frame: %s body is %d bytes, exceeds max %d", typ, bodyLen, MaxFrameSize)
	}

	buf := make([]byte, frameLenSize+bodyLen)
	binary.BigEndian.PutUint32(buf[:frameLenSize], uint32(bodyLen))
	buf[frameLenSize] = byte(typ)
	copy(buf[frameLenSize+1:], payload)
	return buf, nil
}

// WriteMessage CBOR-encodes v as a map (via its `cbor:"..."` struct tags)
// and writes it as a frame of the given type.
func (w *Writer) WriteMessage(typ MsgType, v any) error {
	payload, err := encodeMessage(typ, v)
	if err != nil {
		return err
	}
	return w.WriteFrame(typ, payload)
}

// encodeMessage CBOR-encodes v as a frame payload, naming typ in any
// error.
func encodeMessage(typ MsgType, v any) ([]byte, error) {
	payload, err := cbor.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("protocol: frame: encode %s: %w", typ, err)
	}
	return payload, nil
}

// WriteFileChunk writes a MsgFileChunk frame: hdr CBOR-encoded, followed
// immediately by the raw bytes in data (SPEC.md §4). data must be at most
// MaxFileChunkData bytes.
func (w *Writer) WriteFileChunk(hdr FileChunkHeader, data []byte) error {
	payload, err := encodeFileChunk(hdr, data)
	if err != nil {
		return err
	}
	return w.WriteFrame(MsgFileChunk, payload)
}

// encodeFileChunk builds a MsgFileChunk frame's payload: hdr's CBOR
// encoding followed immediately by the raw bytes in data.
func encodeFileChunk(hdr FileChunkHeader, data []byte) ([]byte, error) {
	if len(data) > MaxFileChunkData {
		return nil, fmt.Errorf("protocol: frame: file chunk data is %d bytes, exceeds max %d", len(data), MaxFileChunkData)
	}
	hdrBytes, err := cbor.Marshal(hdr)
	if err != nil {
		return nil, fmt.Errorf("protocol: frame: encode file chunk header: %w", err)
	}
	payload := make([]byte, 0, len(hdrBytes)+len(data))
	payload = append(payload, hdrBytes...)
	payload = append(payload, data...)
	return payload, nil
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

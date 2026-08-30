package protocol

import "fmt"

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
// file (SPEC.md §5). Phase 3 only defines the wire shape; the dominance/
// merge algebra belongs to Phase 5.
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
// in this phase; it's noted here only so the shape isn't accidentally
// closed off (e.g. by switching to array encoding).
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

// Hello is the first message each side sends, immediately and without
// waiting for the peer's Hello (SPEC.md §4).
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
)

// Error reports a protocol-level failure to the peer before closing the
// connection (SPEC.md §4).
type Error struct {
	Code string `cbor:"code"`
	Msg  string `cbor:"msg"`
}

// RemoteError wraps an Error message received from the peer, so callers
// can distinguish "the peer told us why it's closing" from a local
// decode/timeout/IO failure.
type RemoteError struct {
	Code string
	Msg  string
}

func (e *RemoteError) Error() string {
	return fmt.Sprintf("protocol: peer sent error %q: %s", e.Code, e.Msg)
}

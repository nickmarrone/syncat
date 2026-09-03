package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math/rand"
	"reflect"
	"runtime"
	"testing"

	"github.com/fxamacker/cbor/v2"
)

// TestUnknownFieldsIgnoredOnDecode proves the SPEC.md §11 forward-compat
// obligation: a message encoded with extra fields a v1 decoder doesn't
// know about must decode cleanly, ignoring them, rather than erroring or
// panicking. This simulates a v2 peer sending a Hello (and separately a
// FileInfo, since that one's future `blocks` field is called out
// explicitly in §11) with additional keys this build's structs don't
// declare.
func TestUnknownFieldsIgnoredOnDecode(t *testing.T) {
	type helloV2 struct {
		ProtoVersion int    `cbor:"proto_version"`
		NodeName     string `cbor:"node_name"`
		Ed25519Pub   []byte `cbor:"ed25519_pub"`
		Token        string `cbor:"token"`
		Nonce        []byte `cbor:"nonce"`
		// Fields a hypothetical v2 might add, unknown to this build.
		Capabilities []string `cbor:"capabilities"`
		ExtraFlag    bool     `cbor:"extra_flag"`
	}

	future := helloV2{
		ProtoVersion: 2,
		NodeName:     "alice",
		Ed25519Pub:   bytes.Repeat([]byte{1}, 32),
		Token:        "sc1AAAA",
		Nonce:        bytes.Repeat([]byte{2}, 32),
		Capabilities: []string{"block-transfer"},
		ExtraFlag:    true,
	}

	data, err := cbor.Marshal(future)
	if err != nil {
		t.Fatalf("cbor.Marshal(helloV2): %v", err)
	}

	var got Hello
	if err := DecodeMessage(data, &got); err != nil {
		t.Fatalf("DecodeMessage with unknown fields present: %v", err)
	}
	if got.NodeName != future.NodeName {
		t.Errorf("NodeName = %q, want %q", got.NodeName, future.NodeName)
	}
	if !bytes.Equal(got.Ed25519Pub, future.Ed25519Pub) {
		t.Errorf("Ed25519Pub mismatch")
	}
	if !bytes.Equal(got.Nonce, future.Nonce) {
		t.Errorf("Nonce mismatch")
	}
	if got.Token != future.Token {
		t.Errorf("Token = %q, want %q", got.Token, future.Token)
	}

	// Also round-trip through the real frame codec, not just raw
	// cbor.Unmarshal, so the guarantee holds through the path production
	// code actually uses.
	var buf bytes.Buffer
	if err := NewWriter(&buf).WriteMessage(MsgHello, future); err != nil {
		t.Fatalf("WriteMessage(helloV2): %v", err)
	}
	typ, payload, err := NewReader(&buf).ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if typ != MsgHello {
		t.Fatalf("type = %s, want %s", typ, MsgHello)
	}
	var got2 Hello
	if err := DecodeMessage(payload, &got2); err != nil {
		t.Fatalf("DecodeMessage via frame codec with unknown fields: %v", err)
	}
	if got2.NodeName != future.NodeName {
		t.Errorf("via frame codec: NodeName = %q, want %q", got2.NodeName, future.NodeName)
	}
}

// TestFileInfoForwardCompatBlocksField specifically exercises SPEC.md
// §11's called-out future extension: FileInfo gaining an optional
// `blocks` field in v2 must not break a v1 decoder.
func TestFileInfoForwardCompatBlocksField(t *testing.T) {
	type block struct {
		Offset int64  `cbor:"offset"`
		Size   int64  `cbor:"size"`
		SHA256 []byte `cbor:"sha256_16"`
	}
	type fileInfoV2 struct {
		RelPath string        `cbor:"relpath"`
		Type    FileType      `cbor:"type"`
		Size    int64         `cbor:"size"`
		MTimeNS int64         `cbor:"mtime_ns"`
		Mode    uint32        `cbor:"mode"`
		SHA256  []byte        `cbor:"sha256"`
		Version VersionVector `cbor:"version"`
		Deleted bool          `cbor:"deleted"`
		Blocks  []block       `cbor:"blocks"`
	}

	future := fileInfoV2{
		RelPath: "a/b.txt",
		Type:    FileTypeFile,
		Size:    100,
		MTimeNS: 42,
		Mode:    0644,
		SHA256:  bytes.Repeat([]byte{9}, 32),
		Version: VersionVector{"aaaa1111": 1},
		Blocks:  []block{{Offset: 0, Size: 64 * 1024, SHA256: bytes.Repeat([]byte{7}, 16)}},
	}

	data, err := cbor.Marshal(future)
	if err != nil {
		t.Fatalf("cbor.Marshal(fileInfoV2): %v", err)
	}

	var got FileInfo
	if err := DecodeMessage(data, &got); err != nil {
		t.Fatalf("DecodeMessage FileInfo with blocks field: %v", err)
	}
	if got.RelPath != future.RelPath || got.Size != future.Size || !bytes.Equal(got.SHA256, future.SHA256) {
		t.Errorf("FileInfo round-trip mismatch: got %+v", got)
	}
}

func TestMsgTypeStringUnknown(t *testing.T) {
	if s := MsgType(200).String(); s == "" {
		t.Error("MsgType.String() for an unknown type returned empty string")
	}
}

// roundTrip writes typ/v through a Writer, reads it back through a
// Reader, and decodes the payload into a fresh zero value of the same
// type as v, returning it for the caller to inspect.
func roundTripMessage[T any](t *testing.T, typ MsgType, v T) T {
	t.Helper()
	var buf bytes.Buffer
	if err := NewWriter(&buf).WriteMessage(typ, v); err != nil {
		t.Fatalf("WriteMessage(%s): %v", typ, err)
	}
	gotType, payload, err := NewReader(&buf).ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if gotType != typ {
		t.Fatalf("ReadFrame type = %s, want %s", gotType, typ)
	}
	var out T
	if err := DecodeMessage(payload, &out); err != nil {
		t.Fatalf("DecodeMessage: %v", err)
	}
	return out
}

func TestFrameRoundTripAllMessageTypes(t *testing.T) {
	hello := Hello{ProtoVersion: 1, NodeName: "alice", Ed25519Pub: bytes.Repeat([]byte{0x11}, 32), Token: "sc1abc", Nonce: bytes.Repeat([]byte{0x22}, 32)}
	if got := roundTripMessage(t, MsgHello, hello); got.NodeName != hello.NodeName || !bytes.Equal(got.Ed25519Pub, hello.Ed25519Pub) || !bytes.Equal(got.Nonce, hello.Nonce) || got.Token != hello.Token || got.ProtoVersion != hello.ProtoVersion {
		t.Errorf("Hello round-trip mismatch: got %+v, want %+v", got, hello)
	}

	auth := Auth{Sig: bytes.Repeat([]byte{0x33}, 64)}
	if got := roundTripMessage(t, MsgAuth, auth); !bytes.Equal(got.Sig, auth.Sig) {
		t.Errorf("Auth round-trip mismatch: got %+v, want %+v", got, auth)
	}

	shareList := ShareList{Shares: []ShareListEntry{
		{ShareID: "s1", Name: "docs", Permission: "read-only", ApprovalRequired: true, Access: AccessPending},
	}}
	if got := roundTripMessage(t, MsgShareList, shareList); len(got.Shares) != 1 || got.Shares[0] != shareList.Shares[0] {
		t.Errorf("ShareList round-trip mismatch: got %+v, want %+v", got, shareList)
	}

	sub := SubscribeRequest{ShareID: "s1"}
	if got := roundTripMessage(t, MsgSubscribeRequest, sub); got != sub {
		t.Errorf("SubscribeRequest round-trip mismatch: got %+v, want %+v", got, sub)
	}

	access := AccessUpdate{ShareID: "s1", Access: AccessGranted}
	if got := roundTripMessage(t, MsgAccessUpdate, access); got != access {
		t.Errorf("AccessUpdate round-trip mismatch: got %+v, want %+v", got, access)
	}

	idx := IndexUpdate{ShareID: "s1", Full: true, Files: []FileInfo{
		{RelPath: "a/b.txt", Type: FileTypeFile, Size: 42, MTimeNS: 123, Mode: 0644, SHA256: bytes.Repeat([]byte{0x44}, 32), Version: VersionVector{"aaaa1111": 3}, Deleted: false},
		{RelPath: "a", Type: FileTypeDir, Deleted: false},
		{RelPath: "gone.txt", Deleted: true},
	}}
	got := roundTripMessage(t, MsgIndexUpdate, idx)
	if got.ShareID != idx.ShareID || got.Full != idx.Full || len(got.Files) != len(idx.Files) {
		t.Fatalf("IndexUpdate round-trip mismatch: got %+v, want %+v", got, idx)
	}
	if !bytes.Equal(got.Files[0].SHA256, idx.Files[0].SHA256) || got.Files[0].Version["aaaa1111"] != 3 {
		t.Errorf("IndexUpdate.Files[0] round-trip mismatch: got %+v, want %+v", got.Files[0], idx.Files[0])
	}
	if !got.Files[2].Deleted {
		t.Errorf("IndexUpdate.Files[2] tombstone lost Deleted=true: got %+v", got.Files[2])
	}

	freq := FileRequest{ShareID: "s1", RelPath: "a/b.txt", Version: VersionVector{"aaaa1111": 3}, Offset: 100}
	if got := roundTripMessage(t, MsgFileRequest, freq); got.ShareID != freq.ShareID || got.RelPath != freq.RelPath || got.Offset != freq.Offset || got.Version["aaaa1111"] != 3 {
		t.Errorf("FileRequest round-trip mismatch: got %+v, want %+v", got, freq)
	}

	if got := roundTripMessage(t, MsgPing, Ping{}); got != (Ping{}) {
		t.Errorf("Ping round-trip mismatch: got %+v", got)
	}
	if got := roundTripMessage(t, MsgPong, Pong{}); got != (Pong{}) {
		t.Errorf("Pong round-trip mismatch: got %+v", got)
	}

	errMsg := Error{Code: ErrCodeBadAuth, Msg: "nope"}
	if got := roundTripMessage(t, MsgError, errMsg); got != errMsg {
		t.Errorf("Error round-trip mismatch: got %+v, want %+v", got, errMsg)
	}
}

func TestFrameRoundTripFileChunk(t *testing.T) {
	hdr := FileChunkHeader{ShareID: "s1", RelPath: "a/b.txt", Version: VersionVector{"aaaa1111": 3}, Offset: 256, EOF: true}
	data := bytes.Repeat([]byte{0xAB}, 1024)

	var buf bytes.Buffer
	if err := NewWriter(&buf).WriteFileChunk(hdr, data); err != nil {
		t.Fatalf("WriteFileChunk: %v", err)
	}
	typ, payload, err := NewReader(&buf).ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if typ != MsgFileChunk {
		t.Fatalf("type = %s, want %s", typ, MsgFileChunk)
	}
	gotHdr, gotData, err := DecodeFileChunk(payload)
	if err != nil {
		t.Fatalf("DecodeFileChunk: %v", err)
	}
	if !reflect.DeepEqual(gotHdr, hdr) {
		t.Errorf("header mismatch: got %+v, want %+v", gotHdr, hdr)
	}
	if !bytes.Equal(gotData, data) {
		t.Errorf("data mismatch: got %d bytes, want %d bytes", len(gotData), len(data))
	}
}

func TestFrameRoundTripFileChunkEmptyData(t *testing.T) {
	hdr := FileChunkHeader{ShareID: "s1", RelPath: "empty.txt", EOF: true}
	var buf bytes.Buffer
	if err := NewWriter(&buf).WriteFileChunk(hdr, nil); err != nil {
		t.Fatalf("WriteFileChunk: %v", err)
	}
	_, payload, err := NewReader(&buf).ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	gotHdr, gotData, err := DecodeFileChunk(payload)
	if err != nil {
		t.Fatalf("DecodeFileChunk: %v", err)
	}
	if !reflect.DeepEqual(gotHdr, hdr) {
		t.Errorf("header mismatch: got %+v, want %+v", gotHdr, hdr)
	}
	if len(gotData) != 0 {
		t.Errorf("data = %d bytes, want 0", len(gotData))
	}
}

func TestFrameWriteFileChunkTooLarge(t *testing.T) {
	var buf bytes.Buffer
	data := make([]byte, MaxFileChunkData+1)
	if err := NewWriter(&buf).WriteFileChunk(FileChunkHeader{}, data); err == nil {
		t.Fatal("WriteFileChunk with oversized data: want error, got nil")
	}
}

func TestFrameZeroLengthPayload(t *testing.T) {
	var buf bytes.Buffer
	if err := NewWriter(&buf).WriteFrame(MsgPing, nil); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	typ, payload, err := NewReader(&buf).ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if typ != MsgPing {
		t.Errorf("type = %s, want %s", typ, MsgPing)
	}
	if len(payload) != 0 {
		t.Errorf("payload = %d bytes, want 0", len(payload))
	}
}

// A broken io.Writer may accept fewer bytes than it was given without the
// required error. A frame writer must still report io.ErrShortWrite rather
// than claim it emitted a complete frame.
func TestFrameWriterRejectsSilentShortWrite(t *testing.T) {
	var dst bytes.Buffer
	w := &shortWriter{w: &dst, max: 3}
	if err := NewWriter(w).WriteFrame(MsgPing, []byte("payload")); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("WriteFrame error = %v, want io.ErrShortWrite", err)
	}
}

type shortWriter struct {
	w   io.Writer
	max int
}

func (w *shortWriter) Write(p []byte) (int, error) {
	if len(p) > w.max {
		p = p[:w.max]
	}
	return w.w.Write(p)
}

func TestFrameTruncatedHeader(t *testing.T) {
	// Only 2 of the 4 length-header bytes.
	r := NewReader(bytes.NewReader([]byte{0x00, 0x01}))
	_, _, err := r.ReadFrame()
	if err == nil {
		t.Fatal("ReadFrame on truncated header: want error, got nil")
	}
	if errors.Is(err, io.EOF) {
		t.Errorf("truncated header should not report clean io.EOF, got: %v", err)
	}
}

func TestFrameEmptyStreamIsEOF(t *testing.T) {
	r := NewReader(bytes.NewReader(nil))
	_, _, err := r.ReadFrame()
	if !errors.Is(err, io.EOF) {
		t.Fatalf("ReadFrame on empty stream: err = %v, want io.EOF", err)
	}
}

func TestFrameTruncatedPayload(t *testing.T) {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], 100) // claims 100 bytes, supplies far fewer
	var buf bytes.Buffer
	buf.Write(hdr[:])
	buf.Write([]byte{byte(MsgPing), 0x01, 0x02, 0x03})

	_, _, err := NewReader(&buf).ReadFrame()
	if err == nil {
		t.Fatal("ReadFrame on truncated payload: want error, got nil")
	}
}

func TestFrameZeroLengthBody(t *testing.T) {
	var hdr [4]byte // length 0: not even a type byte
	r := NewReader(bytes.NewReader(hdr[:]))
	_, _, err := r.ReadFrame()
	if err == nil {
		t.Fatal("ReadFrame on zero-length body: want error, got nil")
	}
}

func TestFrameOversizedLengthRejectedWithoutHugeAlloc(t *testing.T) {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], MaxFrameSize+1)
	r := NewReader(bytes.NewReader(hdr[:])) // no body at all follows

	before := allocatedBytes()
	_, _, err := r.ReadFrame()
	after := allocatedBytes()

	if err == nil {
		t.Fatal("ReadFrame on oversized length: want error, got nil")
	}
	// A buggy implementation that allocates the claimed size before
	// validating it would show up here as a huge jump in heap allocation
	// (MaxFrameSize+1 is 4MiB+1); a correct implementation rejects the
	// length before allocating anything of that scale, so the delta stays
	// small.
	if delta := after - before; delta > MaxFrameSize/2 {
		t.Errorf("ReadFrame allocated %d bytes rejecting an oversized length header (>%d) — looks like it allocated the claimed size before validating it", delta, MaxFrameSize/2)
	}
}

func TestFrameWriteOversizedPayloadRejected(t *testing.T) {
	var buf bytes.Buffer
	payload := make([]byte, MaxFrameSize) // +1 byte type = MaxFrameSize+1 body
	err := NewWriter(&buf).WriteFrame(MsgFileChunk, payload)
	if err == nil {
		t.Fatal("WriteFrame with oversized payload: want error, got nil")
	}
	if buf.Len() != 0 {
		t.Errorf("WriteFrame wrote %d bytes despite rejecting the frame", buf.Len())
	}
}

func TestFrameAtMaxSizeAccepted(t *testing.T) {
	var buf bytes.Buffer
	payload := make([]byte, MaxFrameSize-1) // + 1-byte type = exactly MaxFrameSize
	for i := range payload {
		payload[i] = byte(i)
	}
	if err := NewWriter(&buf).WriteFrame(MsgFileChunk, payload); err != nil {
		t.Fatalf("WriteFrame at exactly MaxFrameSize: %v", err)
	}
	typ, got, err := NewReader(&buf).ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame at exactly MaxFrameSize: %v", err)
	}
	if typ != MsgFileChunk || !bytes.Equal(got, payload) {
		t.Errorf("round-trip at MaxFrameSize mismatch (type=%s, len=%d)", typ, len(got))
	}
}

func TestFrameGarbageBytesNeverPanics(t *testing.T) {
	rnd := rand.New(rand.NewSource(1))
	for i := 0; i < 2000; i++ {
		n := rnd.Intn(64)
		data := make([]byte, n)
		rnd.Read(data)
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("ReadFrame panicked on garbage input %x: %v", data, r)
				}
			}()
			r := NewReader(bytes.NewReader(data))
			// Drain everything this "stream" will give us; garbage bytes
			// can legally decode as a well-formed (if meaningless) frame
			// header, so ReadFrame may succeed one or more times before
			// running out of input — the only requirement is: no panic.
			for {
				_, _, err := r.ReadFrame()
				if err != nil {
					break
				}
			}
		}()
	}
}

func TestFrameGarbagePayloadNeverPanicsCBORDecode(t *testing.T) {
	rnd := rand.New(rand.NewSource(2))
	for i := 0; i < 2000; i++ {
		n := rnd.Intn(256)
		data := make([]byte, n)
		rnd.Read(data)
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("decode panicked on garbage payload %x: %v", data, r)
				}
			}()
			var h Hello
			_ = DecodeMessage(data, &h)
			_, _, _ = DecodeFileChunk(data)
		}()
	}
}

// allocatedBytes is a coarse proxy for "how much has this process
// allocated so far", used only to sanity-check that rejecting an oversized
// frame length doesn't allocate a buffer anywhere near that size. It's
// intentionally simple (runtime.ReadMemStats is process-wide and includes
// noise from the test binary itself) — see the generous threshold at the
// call site.
func allocatedBytes() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.TotalAlloc
}

// FuzzDecode fuzzes the frame decoder ([Reader.ReadFrame]) and, on
// whatever payload it yields, the message decoders layered on top
// ([DecodeMessage] into every known struct type, [DecodeFileChunk]).
// Per SPEC.md §13.9/§14, none of this may ever panic on attacker-supplied
// bytes — the only acceptable outcomes are "returns a value" or "returns
// an error".
func FuzzDecode(f *testing.F) {
	// Seed with valid frames for every message type, so the fuzzer starts
	// from inputs the mutator can meaningfully perturb, plus a handful of
	// structurally-tricky byte sequences.
	seedMessage(f, MsgHello, Hello{ProtoVersion: 1, NodeName: "alice", Ed25519Pub: bytes.Repeat([]byte{1}, 32), Token: "sc1AAAA", Nonce: bytes.Repeat([]byte{2}, 32)})
	seedMessage(f, MsgAuth, Auth{Sig: bytes.Repeat([]byte{3}, 64)})
	seedMessage(f, MsgShareList, ShareList{Shares: []ShareListEntry{{ShareID: "s1", Name: "docs", Permission: "read-only", ApprovalRequired: true, Access: AccessPending}}})
	seedMessage(f, MsgSubscribeRequest, SubscribeRequest{ShareID: "s1"})
	seedMessage(f, MsgAccessUpdate, AccessUpdate{ShareID: "s1", Access: AccessGranted})
	seedMessage(f, MsgIndexUpdate, IndexUpdate{ShareID: "s1", Full: true, Files: []FileInfo{
		{RelPath: "a/b.txt", Type: FileTypeFile, Size: 42, MTimeNS: 123, Mode: 0644, SHA256: bytes.Repeat([]byte{4}, 32), Version: VersionVector{"aaaa1111": 3}},
	}})
	seedMessage(f, MsgFileRequest, FileRequest{ShareID: "s1", RelPath: "a/b.txt", Version: VersionVector{"aaaa1111": 3}, Offset: 100})
	seedMessage(f, MsgPing, Ping{})
	seedMessage(f, MsgPong, Pong{})
	seedMessage(f, MsgError, Error{Code: ErrCodeBadAuth, Msg: "nope"})

	// A valid FileChunk frame (header + raw bytes), built by hand since
	// it's not a single CBOR-marshaled struct.
	{
		var buf bytes.Buffer
		if err := NewWriter(&buf).WriteFileChunk(FileChunkHeader{ShareID: "s1", RelPath: "a/b.txt", Offset: 0, EOF: true}, []byte("hello world")); err != nil {
			f.Fatalf("seed: WriteFileChunk: %v", err)
		}
		f.Add(buf.Bytes())
	}

	// Structural edge cases: empty input, a truncated header, a header
	// claiming a body far larger than what follows, and a couple of
	// concatenated frames (exercises ReadFrame being called repeatedly on
	// one stream, the normal usage pattern).
	f.Add([]byte{})
	f.Add([]byte{0x00, 0x00})
	f.Add([]byte{0x00, 0x00, 0x00, 0x00})                          // zero-length body
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF, byte(MsgHello)})          // huge declared length, no body
	f.Add([]byte{0x00, 0x00, 0x00, 0x05, byte(MsgHello), 1, 2, 3}) // declares 5, only 4 follow

	f.Fuzz(func(t *testing.T, data []byte) {
		r := NewReader(bytes.NewReader(data))
		for {
			typ, payload, err := r.ReadFrame()
			if err != nil {
				return
			}
			// Whatever type came back, try decoding the payload as every
			// message shape we know about (not just the declared type —
			// a corrupted type byte should still never cause a panic
			// when something downstream naively decodes it) plus the
			// FileChunk header/data split.
			var hello Hello
			_ = DecodeMessage(payload, &hello)
			var auth Auth
			_ = DecodeMessage(payload, &auth)
			var shareList ShareList
			_ = DecodeMessage(payload, &shareList)
			var sub SubscribeRequest
			_ = DecodeMessage(payload, &sub)
			var access AccessUpdate
			_ = DecodeMessage(payload, &access)
			var idx IndexUpdate
			_ = DecodeMessage(payload, &idx)
			var freq FileRequest
			_ = DecodeMessage(payload, &freq)
			var errMsg Error
			_ = DecodeMessage(payload, &errMsg)
			_, _, _ = DecodeFileChunk(payload)

			_ = typ
		}
	})
}

func seedMessage[T any](f *testing.F, typ MsgType, v T) {
	f.Helper()
	var buf bytes.Buffer
	if err := NewWriter(&buf).WriteMessage(typ, v); err != nil {
		f.Fatalf("seed: WriteMessage(%s): %v", typ, err)
	}
	f.Add(buf.Bytes())
}

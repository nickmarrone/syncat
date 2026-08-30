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
)

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

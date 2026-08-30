package protocol

import (
	"bytes"
	"testing"
)

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

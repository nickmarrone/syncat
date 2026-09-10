package sync

import (
	"bytes"
	"context"
	"io"
	"log"
	"net"
	"testing"

	"github.com/nickmarrone/syncat/internal/protocol"
)

func TestSessionFrameHandlerTableCoversDataPlane(t *testing.T) {
	want := []protocol.MsgType{
		protocol.MsgIndexSyncRequest, protocol.MsgIndexSnapshotBegin,
		protocol.MsgIndexSnapshotBatch, protocol.MsgIndexSnapshotEnd,
		protocol.MsgIndexDeltaBatch, protocol.MsgIndexAck, protocol.MsgIndexUpdate,
		protocol.MsgFileRequest, protocol.MsgFileChunk, protocol.MsgError,
		protocol.MsgCancelTransfer,
	}
	if len(sessionFrameHandlers) != len(want) {
		t.Fatalf("handler table has %d entries, want %d", len(sessionFrameHandlers), len(want))
	}
	for _, typ := range want {
		if sessionFrameHandlers[typ] == nil {
			t.Errorf("handler table is missing %s", typ)
		}
	}
}

// FuzzSessionDecodeValidateDispatch drives attacker-controlled payloads
// through the live Session reader, semantic validators, admission checks, and
// dispatcher. Framing itself has a separate byte-stream fuzz target in the
// protocol package; building a valid envelope here lets mutations reach every
// post-handshake dispatch case instead of usually stopping at ReadFrame.
func FuzzSessionDecodeValidateDispatch(f *testing.F) {
	file := protocol.FileInfo{RelPath: "dir", Type: protocol.FileTypeDir, Version: protocol.VersionVector{"aaaaaaaaaaaaaaaa": 1}}
	seedDispatchMessage(f, protocol.MsgPing, protocol.Ping{})
	seedDispatchMessage(f, protocol.MsgIndexSyncRequest, protocol.IndexSyncRequest{ShareID: testShareID, Epoch: "epoch"})
	seedDispatchMessage(f, protocol.MsgIndexSnapshotBegin, protocol.IndexSnapshotBegin{ShareID: testShareID, SnapshotID: "snap", Epoch: "epoch", HighSeq: 1})
	seedDispatchMessage(f, protocol.MsgIndexSnapshotBatch, protocol.IndexSnapshotBatch{ShareID: testShareID, SnapshotID: "snap", Files: []protocol.FileInfo{file}})
	seedDispatchMessage(f, protocol.MsgIndexSnapshotEnd, protocol.IndexSnapshotEnd{ShareID: testShareID, SnapshotID: "snap", BatchCount: 1})
	seedDispatchMessage(f, protocol.MsgIndexDeltaBatch, protocol.IndexDeltaBatch{ShareID: testShareID, Epoch: "epoch", FromSeq: 1, ToSeq: 1, Entries: []protocol.IndexDeltaEntry{{Seq: 1, File: file}}})
	seedDispatchMessage(f, protocol.MsgIndexAck, protocol.IndexAck{ShareID: testShareID, Epoch: "epoch", AppliedSeq: 1})
	seedDispatchMessage(f, protocol.MsgIndexUpdate, protocol.IndexUpdate{ShareID: testShareID, Full: true, Files: []protocol.FileInfo{file}})
	seedDispatchMessage(f, protocol.MsgFileRequest, protocol.FileRequest{TransferID: "11111111111111111111111111111111", ShareID: testShareID, RelPath: "missing", Version: protocol.VersionVector{"aaaaaaaaaaaaaaaa": 1}})
	seedDispatchFileChunk(f, protocol.FileChunkHeader{TransferID: "11111111111111111111111111111111", ShareID: testShareID, RelPath: "missing", Version: protocol.VersionVector{"aaaaaaaaaaaaaaaa": 1}, EOF: true}, nil)
	seedDispatchMessage(f, protocol.MsgError, protocol.Error{Code: protocol.ErrCodeTransferFailed, Msg: "failed", TransferID: "11111111111111111111111111111111"})
	seedDispatchMessage(f, protocol.MsgCancelTransfer, protocol.CancelTransfer{TransferID: "11111111111111111111111111111111"})
	f.Add(uint8(255), []byte{0xff, 0x00, 0x7f})

	f.Fuzz(func(t *testing.T, typByte uint8, payload []byte) {
		if len(payload) > protocol.MaxFrameSize-1 {
			return
		}
		n := newTestNode(t, "dispatch-fuzz")
		near, far := net.Pipe()
		s := NewSession(near, n.store, n.id, "peer000000000000", nil, log.New(io.Discard, "", 0))
		s.AddShare(ShareConfig{ShareID: testShareID, Root: n.root, Direction: Direction{}})
		s.SetControlHandler(func(typ protocol.MsgType, payload []byte) {
			decodeControlForFuzz(typ, payload)
		})
		if err := s.Start(context.Background()); err != nil {
			t.Fatal(err)
		}

		err := protocol.NewWriter(far).WriteFrame(protocol.MsgType(typByte), payload)
		_ = far.Close()
		if err != nil {
			_ = s.Close()
			t.Fatalf("write fuzz frame: %v", err)
		}
		// Closing the peer can race a legitimate response already selected by
		// the stream writer; that expected broken-pipe result is not a decoder
		// or dispatcher failure. Close still waits for all session workers.
		_ = s.Close()
	})
}

func seedDispatchMessage[T any](f *testing.F, typ protocol.MsgType, msg T) {
	f.Helper()
	var frame bytes.Buffer
	if err := protocol.NewWriter(&frame).WriteMessage(typ, msg); err != nil {
		f.Fatal(err)
	}
	f.Add(uint8(typ), append([]byte(nil), frame.Bytes()[5:]...))
}

func seedDispatchFileChunk(f *testing.F, hdr protocol.FileChunkHeader, data []byte) {
	f.Helper()
	var frame bytes.Buffer
	if err := protocol.NewWriter(&frame).WriteFileChunk(hdr, data); err != nil {
		f.Fatal(err)
	}
	f.Add(uint8(protocol.MsgFileChunk), append([]byte(nil), frame.Bytes()[5:]...))
}

func decodeControlForFuzz(typ protocol.MsgType, payload []byte) {
	switch typ {
	case protocol.MsgShareList:
		var m protocol.ShareList
		_ = protocol.DecodeAndValidateMessage(payload, &m)
	case protocol.MsgSubscribeRequest:
		var m protocol.SubscribeRequest
		_ = protocol.DecodeAndValidateMessage(payload, &m)
	case protocol.MsgAccessUpdate:
		var m protocol.AccessUpdate
		_ = protocol.DecodeAndValidateMessage(payload, &m)
	case protocol.MsgPing:
		var m protocol.Ping
		_ = protocol.DecodeAndValidateMessage(payload, &m)
	case protocol.MsgPong:
		var m protocol.Pong
		_ = protocol.DecodeAndValidateMessage(payload, &m)
	case protocol.MsgHello:
		var m protocol.Hello
		_ = protocol.DecodeAndValidateMessage(payload, &m)
	case protocol.MsgAuth:
		var m protocol.Auth
		_ = protocol.DecodeAndValidateMessage(payload, &m)
	case protocol.MsgFinished:
		var m protocol.Finished
		_ = protocol.DecodeAndValidateMessage(payload, &m)
	}
}

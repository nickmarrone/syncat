package sync

import (
	"context"
	"crypto/sha256"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nickmarrone/syncat/internal/index"
	"github.com/nickmarrone/syncat/internal/protocol"
)

func symlinkServeSession(t *testing.T, target, indexedTarget string) (*Session, *protocol.Reader, protocol.FileRequest) {
	t.Helper()
	n := newTestNode(t, "symlink-serve")
	link := filepath.Join(n.root, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	info, err := os.Lstat(link)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(indexedTarget))
	row := index.FileRow{
		ShareID: testShareID, RelPath: "link", Type: protocol.FileTypeSymlink,
		Size: int64(len(indexedTarget)), MTimeNS: info.ModTime().UnixNano(), SHA256: sum[:],
		Version: protocol.VersionVector{"0123456789abcdef": 1}, UpdatedAt: time.Now(),
	}
	if err := n.store.PutFile(context.Background(), row); err != nil {
		t.Fatal(err)
	}
	near, far := net.Pipe()
	s := NewSession(near, n.store, n.id, "peer", SessionOptions{Logger: log.New(io.Discard, "", 0)})
	s.AddShare(ShareConfig{ShareID: testShareID, Root: n.root})
	if err := s.writer.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = near.Close()
		_ = far.Close()
		_ = s.writer.Close()
	})
	req := protocol.FileRequest{
		TransferID: "11111111111111111111111111111111", ShareID: testShareID,
		RelPath: "link", Version: row.Version,
	}
	return s, protocol.NewReader(far), req
}

// Nothing indexes a symlink any more (SPEC.md §5), but if a row claims
// one, the holder refuses it rather than sending the link's target.
func TestServeRefusesSymlinkRow(t *testing.T) {
	s, reader, req := symlinkServeSession(t, "same-target", "same-target")
	done := make(chan struct{})
	go func() {
		s.handleFileRequest(context.Background(), req)
		close(done)
	}()
	typ, _, err := reader.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	if typ != protocol.MsgError {
		t.Fatalf("response type = %s, want Error", typ)
	}
	<-done
}

func TestTransferResponseEmitsExactlyOneTerminalOutcome(t *testing.T) {
	n := newTestNode(t, "response-terminal")
	near, far := net.Pipe()
	s := NewSession(near, n.store, n.id, "peer", SessionOptions{Logger: log.New(io.Discard, "", 0)})
	if err := s.writer.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = near.Close()
		_ = far.Close()
		_ = s.writer.Close()
	})
	req := protocol.FileRequest{TransferID: "11111111111111111111111111111111", ShareID: testShareID, RelPath: "file"}
	response := newTransferResponse(s, context.Background(), req)
	response.setVersion(protocol.VersionVector{"0123456789abcdef": 1})
	if err := response.send([]byte("body")); err != nil {
		t.Fatal(err)
	}
	if err := response.finish(); err != nil {
		t.Fatal(err)
	}
	response.fail(protocol.ErrCodeTransferFailed, "must not follow EOF")
	if err := response.finish(); err == nil {
		t.Fatal("second terminal outcome succeeded")
	}
	if err := response.send([]byte("late")); err == nil {
		t.Fatal("data after terminal outcome succeeded")
	}

	reader := protocol.NewReader(far)
	for i, wantEOF := range []bool{false, true} {
		typ, payload, err := reader.ReadFrame()
		if err != nil {
			t.Fatal(err)
		}
		if typ != protocol.MsgFileChunk {
			t.Fatalf("frame %d type = %s, want FileChunk", i, typ)
		}
		hdr, _, err := protocol.DecodeFileChunk(payload)
		if err != nil {
			t.Fatal(err)
		}
		if hdr.EOF != wantEOF {
			t.Fatalf("frame %d EOF = %v, want %v", i, hdr.EOF, wantEOF)
		}
	}
	if err := far.SetReadDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := reader.ReadFrame(); err == nil {
		t.Fatal("received a second terminal frame")
	}
}

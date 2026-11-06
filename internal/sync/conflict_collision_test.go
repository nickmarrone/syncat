package sync

import (
	"context"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nickmarrone/syncat/internal/protocol"
)

// Conflict-copy names carry only an 8-hex-character digest prefix, so two
// different losers can, very rarely, map to the same name. Preserving a
// local loser is a rename onto that name, which would silently replace the
// file already there; it must go to the trash first instead.
func TestConflictCopyRenameTrashesWhatItWouldReplace(t *testing.T) {
	share := t.TempDir()
	writeShareFile(t, share, "notes.txt", "local loser")
	const conflictPath = "notes.sync-conflict-20261106-013715-2bd806c9.txt"
	writeShareFile(t, share, conflictPath, "an earlier, different loser")

	near, far := net.Pipe()
	t.Cleanup(func() { near.Close(); far.Close() })
	go io.Copy(io.Discard, far) // swallow the FileRequest for the winner
	tr := newTestTrash(trashTestTime)
	sess := NewSession(near, openTestTrashStore(t), "nodeA", "nodeB", SessionOptions{Trash: tr, Logger: log.New(io.Discard, "", 0)})
	if err := sess.writer.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sess.writer.Close() })

	ev := tr.BeginPeerEvent(share, "nodeB")
	// The winner's pull never gets an answer, so it times out; the
	// preserve-the-loser rename runs before it.
	ctx, cancel := context.WithTimeout(withTrashEvent(context.Background(), ev), 300*time.Millisecond)
	defer cancel()
	a := Action{
		Kind: ActionConflictCopy, RelPath: "notes.txt",
		Source: SourceRemote, Resolved: protocol.FileInfo{RelPath: "notes.txt", Type: protocol.FileTypeFile, Size: 6, SHA256: sha256Of("winner")},
		SourceVersion:   protocol.VersionVector{"nodeB": 1},
		ConflictSource:  SourceLocal,
		ConflictRelPath: conflictPath,
		ConflictInfo:    protocol.FileInfo{RelPath: "notes.txt", Type: protocol.FileTypeFile, Size: 11, SHA256: sha256Of("local loser")},
	}
	_, _ = sess.applyConflictCopy(ctx, ShareConfig{ShareID: trashTestShareID, Root: share}, a, time.Now())
	ev.Close()

	if got := readFile(t, filepath.Join(share, conflictPath)); got != "local loser" {
		t.Fatalf("conflict path holds %q, want the local loser", got)
	}
	events := mustList(t, tr, share)
	if len(events) != 1 || len(events[0].Items) != 1 || events[0].Items[0].RelPath != conflictPath || events[0].Items[0].Kind != TrashOverwritten {
		t.Fatalf("trash = %+v, want the replaced conflict copy as overwritten", events)
	}
	kept := filepath.Join(share, ".syncat", "trash", events[0].ID, conflictPath)
	if data, err := os.ReadFile(kept); err != nil || string(data) != "an earlier, different loser" {
		t.Fatalf("trashed copy = %q, %v; want the earlier loser's bytes", data, err)
	}
}

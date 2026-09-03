package sync

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/nickmarrone/syncat/internal/index"
	"github.com/nickmarrone/syncat/internal/protocol"
)

func TestSnapshotBatchingKeepsEveryFrameBelowLimit(t *testing.T) {
	// The aggregate CBOR inventory is deliberately far beyond one frame.
	rows := make([]index.FileRow, 9000)
	for i := range rows {
		rows[i] = index.FileRow{ShareID: "s", RelPath: fmt.Sprintf("dir/%06d-%s", i, bytes.Repeat([]byte{'x'}, 600)), Type: protocol.FileTypeFile, SHA256: bytes.Repeat([]byte{byte(i)}, 32)}
	}
	all := make([]protocol.FileInfo, len(rows))
	for i := range rows {
		all[i] = rows[i].Info()
	}
	total, err := protocol.EncodedMessageSize(protocol.MsgIndexSnapshotBatch, protocol.IndexSnapshotBatch{ShareID: "s", SnapshotID: "id", Files: all})
	if err != nil {
		t.Fatal(err)
	}
	if total <= protocol.MaxFrameSize {
		t.Fatalf("fixture is only %d bytes; must exceed one frame", total)
	}

	frames, delivered := 0, 0
	for len(rows) > 0 {
		n := snapshotBatchLen("s", "id", uint64(frames), rows)
		if n <= 0 {
			t.Fatal("batcher rejected an ordinary entry")
		}
		files := make([]protocol.FileInfo, n)
		for i := range files {
			files[i] = rows[i].Info()
		}
		sz, err := protocol.EncodedMessageSize(protocol.MsgIndexSnapshotBatch, protocol.IndexSnapshotBatch{ShareID: "s", SnapshotID: "id", Batch: uint64(frames), Files: files})
		if err != nil {
			t.Fatal(err)
		}
		if sz > protocol.TargetIndexBatchSize || sz+1 > protocol.MaxFrameSize {
			t.Fatalf("frame %d is %d bytes", frames, sz+1)
		}
		frames++
		delivered += n
		rows = rows[n:]
	}
	if frames < 2 || delivered != 9000 {
		t.Fatalf("frames/delivered = %d/%d", frames, delivered)
	}
}

func TestSnapshotBatchingReportsSingleOversizedEntry(t *testing.T) {
	row := index.FileRow{ShareID: "s", RelPath: string(bytes.Repeat([]byte{'x'}, protocol.TargetIndexBatchSize+1)), Type: protocol.FileTypeFile}
	if got := snapshotBatchLen("s", "id", 0, []index.FileRow{row}); got != 0 {
		t.Fatalf("batch length = %d, want 0", got)
	}
}

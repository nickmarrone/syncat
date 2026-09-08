package sync

import (
	"bytes"
	"fmt"
	"runtime"
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

func TestDeltaBatchingKeepsEveryFrameBelowLimit(t *testing.T) {
	// deltaBatchLen sizes its envelope from the last row's sequence number
	// (see its doc comment); start high enough that those numbers are
	// multi-byte in CBOR and the estimate has something to get wrong.
	rows := make([]index.JournalRow, 9000)
	for i := range rows {
		rows[i] = index.JournalRow{
			Seq: uint64(1_000_000 + i),
			Row: index.FileRow{ShareID: "s", RelPath: fmt.Sprintf("dir/%06d-%s", i, bytes.Repeat([]byte{'x'}, 600)), Type: protocol.FileTypeFile, SHA256: bytes.Repeat([]byte{byte(i)}, 32)},
		}
	}

	frames, delivered := 0, 0
	for len(rows) > 0 {
		n := deltaBatchLen("s", "epoch", rows)
		if n <= 0 {
			t.Fatal("batcher rejected an ordinary entry")
		}
		entries := make([]protocol.IndexDeltaEntry, n)
		for i := range entries {
			entries[i] = protocol.IndexDeltaEntry{Seq: rows[i].Seq, File: rows[i].Row.Info()}
		}
		sz, err := protocol.EncodedMessageSize(protocol.MsgIndexDeltaBatch, protocol.IndexDeltaBatch{
			ShareID: "s", Epoch: "epoch", FromSeq: entries[0].Seq, ToSeq: entries[n-1].Seq, Entries: entries,
		})
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

// TestBatchPlanningIsLinear guards the property that makes index sync
// affordable on a large share. Sizing a batch by re-encoding the whole
// growing message once per candidate entry is quadratic: planning this
// very fixture that way allocated 28 GB and took 77 seconds, which is what
// made a full snapshot of a large share spike the daemon's memory. The
// budget below is ~200x what linear planning actually uses and ~6000x
// below what quadratic planning would.
func TestBatchPlanningIsLinear(t *testing.T) {
	rows := make([]index.FileRow, 20000)
	for i := range rows {
		rows[i] = index.FileRow{
			ShareID: "s", RelPath: fmt.Sprintf("some/deep/directory/path/file-%06d.txt", i),
			Type: protocol.FileTypeFile, Size: int64(i), Mode: 0o644,
			SHA256:  bytes.Repeat([]byte{byte(i)}, 32),
			Version: protocol.VersionVector{"aaaaaaaaaaaaaaaa": uint64(i)},
		}
	}

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	if n := snapshotBatchLen("s", "id", 0, rows); n <= 0 {
		t.Fatalf("batch length = %d", n)
	}
	runtime.ReadMemStats(&after)

	const budget = 1 << 30 // 1 GiB
	if used := after.TotalAlloc - before.TotalAlloc; used > budget {
		t.Fatalf("planning one batch of %d rows allocated %d bytes, over the %d byte budget: batch sizing has gone quadratic again", len(rows), used, budget)
	}
}

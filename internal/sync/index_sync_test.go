package sync

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

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

func TestCanAnswerWithDeltas(t *testing.T) {
	req := func(epoch string, applied uint64) protocol.IndexSyncRequest {
		return protocol.IndexSyncRequest{ShareID: "s", Epoch: epoch, AppliedSeq: applied}
	}
	cases := []struct {
		name         string
		req          protocol.IndexSyncRequest
		epoch        string
		high, oldest uint64
		want         bool
	}{
		{"in range", req("e", 5), "e", 9, 3, true},
		{"exactly at the oldest surviving entry", req("e", 2), "e", 9, 3, true},
		{"needed entry has been pruned", req("e", 1), "e", 9, 3, false},
		{"journal fully pruned, peer behind", req("e", 4), "e", 9, 0, false},
		{"journal fully pruned, peer current", req("e", 9), "e", 9, 0, true},
		{"nothing ever journalled", req("e", 0), "e", 0, 0, true},
		{"different epoch", req("old", 5), "e", 9, 3, false},
		{"peer ahead of us", req("e", 12), "e", 9, 3, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := canAnswerWithDeltas(c.req, c.epoch, c.high, c.oldest); got != c.want {
				t.Fatalf("canAnswerWithDeltas = %v, want %v", got, c.want)
			}
		})
	}
}

// TestPrunedJournalFallsBackToSnapshot is the property that makes the
// change journal safe to prune at all: a peer whose cursor predates every
// surviving entry must be re-anchored with a full snapshot, not answered
// with silence. Answering an empty journal as if it meant "you are up to
// date" leaves that peer permanently behind — its next delta is rejected
// for a sequence gap, it asks again, and gets the same silence.
func TestPrunedJournalFallsBackToSnapshot(t *testing.T) {
	ctx := context.Background()
	a := newTestNode(t, "aaaaaaaaaaaaaaaa")
	b := newTestNode(t, "bbbbbbbbbbbbbbbb")
	sa, _ := connectSessions(t, a, b, Direction{}, Direction{})

	writeFile(t, a.root, "first.txt", "one")
	indexFile(t, a.store, a.id, "first.txt", a.root)
	mustSync(t, sa, testShareID)
	waitForFile(t, 10*time.Second, b.store, testShareID, b.root, "first.txt", "one")

	// A change B never hears about, whose journal entry is then aged out
	// from under it.
	writeFile(t, a.root, "second.txt", "two")
	indexFile(t, a.store, a.id, "second.txt", a.root)
	if err := a.store.PruneJournal(ctx, time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("prune journal: %v", err)
	}

	mustSync(t, sa, testShareID)
	waitForFile(t, 10*time.Second, b.store, testShareID, b.root, "second.txt", "two")
}

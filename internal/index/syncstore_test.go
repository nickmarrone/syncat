package index

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/nickmarrone/syncat/internal/protocol"
)

func syncRow(share, path string, size int64) FileRow {
	return FileRow{ShareID: share, RelPath: path, Type: protocol.FileTypeFile,
		Size: size, Version: protocol.VersionVector{"node": uint64(size + 1)}, UpdatedAt: time.Now().UTC()}
}

func TestPutFileAtomicallyAppendsJournal(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if err := s.PutFile(ctx, syncRow("s", "a.txt", 1)); err != nil {
		t.Fatal(err)
	}
	if err := s.PutFile(ctx, syncRow("s", "a.txt", 2)); err != nil {
		t.Fatal(err)
	}
	epoch, high, err := s.ShareState(ctx, "s")
	if err != nil {
		t.Fatal(err)
	}
	if epoch == "" || high != 2 {
		t.Fatalf("state = %q/%d, want non-empty/2", epoch, high)
	}
	journal, err := s.JournalSince(ctx, "s", epoch, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(journal) != 2 || journal[0].Seq != 1 || journal[1].Seq != 2 || journal[1].Row.Size != 2 {
		t.Fatalf("journal = %+v, want ordered versions 1 and 2", journal)
	}
}

func TestSnapshotStagingIsAtomicAndPrunesStaleRows(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if err := s.UpsertPeerFiles(ctx, "peer", []FileRow{syncRow("s", "stale.txt", 1)}); err != nil {
		t.Fatal(err)
	}
	if err := s.StageSnapshotBatch(ctx, "peer", "s", "snap", 0, []FileRow{syncRow("s", "new-a.txt", 2)}); err != nil {
		t.Fatal(err)
	}
	if err := s.StageSnapshotBatch(ctx, "peer", "s", "snap", 1, []FileRow{syncRow("s", "new-b.txt", 3)}); err != nil {
		t.Fatal(err)
	}
	before, _ := s.ListPeerFiles(ctx, "peer", "s")
	if len(before) != 1 || before[0].RelPath != "stale.txt" {
		t.Fatalf("partial snapshot became visible: %+v", before)
	}
	if err := s.CommitSnapshot(ctx, "peer", "s", "snap", "epoch", 17); err != nil {
		t.Fatal(err)
	}
	after, _ := s.ListPeerFiles(ctx, "peer", "s")
	if len(after) != 2 || after[0].RelPath != "new-a.txt" || after[1].RelPath != "new-b.txt" {
		t.Fatalf("committed snapshot = %+v", after)
	}
	c, _ := s.Cursor(ctx, "peer", "s", "incoming")
	if c.Epoch != "epoch" || c.AppliedSeq != 17 || c.SnapshotID != "" {
		t.Fatalf("cursor = %+v", c)
	}
}

func TestPeerDeltaRejectsGapsAndDuplicates(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if err := s.SetCursor(ctx, "p", "s", "incoming", Cursor{Epoch: "e", AppliedSeq: 4}); err != nil {
		t.Fatal(err)
	}
	rows := []FileRow{syncRow("s", "five", 5), syncRow("s", "six", 6)}
	if err := s.ApplyPeerDelta(ctx, "p", "s", "e", 5, 6, rows); err != nil {
		t.Fatalf("valid delta: %v", err)
	}
	for name, seqs := range map[string][2]uint64{"duplicate": {5, 6}, "gap": {8, 9}} {
		t.Run(name, func(t *testing.T) {
			err := s.ApplyPeerDelta(ctx, "p", "s", "e", seqs[0], seqs[1], rows)
			if err == nil || !strings.Contains(err.Error(), "delta gap") {
				t.Fatalf("error = %v, want delta gap", err)
			}
		})
	}
	c, _ := s.Cursor(ctx, "p", "s", "incoming")
	if c.AppliedSeq != 6 {
		t.Fatalf("cursor moved after rejected delta: %+v", c)
	}
}

func TestJournalRetentionMakesOldCursorUnavailable(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	for i := int64(0); i < 3; i++ {
		if err := s.PutFile(ctx, syncRow("s", string(rune('a'+i)), i)); err != nil {
			t.Fatal(err)
		}
	}
	epoch, _, _ := s.ShareState(ctx, "s")
	if _, err := s.db.Exec(`UPDATE change_journal SET created_at=? WHERE seq=1`, time.Now().Add(-8*24*time.Hour).UnixNano()); err != nil {
		t.Fatal(err)
	}
	if err := s.PruneJournal(ctx, time.Now().Add(-7*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	oldest, err := s.OldestJournalSeq(ctx, "s", epoch)
	if err != nil || oldest != 2 {
		t.Fatalf("oldest = %d, %v; want 2", oldest, err)
	}
}

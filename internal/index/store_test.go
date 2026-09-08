package index

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/nickmarrone/syncat/internal/protocol"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	s, err := Open(context.Background(), filepath.Join(dir, "index.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return s
}

func TestOpenCreatesFreshDatabase(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "index.db")

	s, err := Open(context.Background(), dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	var version int
	if err := s.db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		t.Fatalf("read user_version: %v", err)
	}
	if version != currentSchemaVersion {
		t.Errorf("user_version = %d, want %d", version, currentSchemaVersion)
	}

	for _, table := range []string{"files", "peer_files", "pending_transfers", "share_state", "change_journal", "peer_cursors", "snapshot_staging", "dirty_paths"} {
		var name string
		err := s.db.QueryRow(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&name)
		if err != nil {
			t.Errorf("table %s missing: %v", table, err)
		}
	}
}

func TestReopenIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "index.db")
	ctx := context.Background()

	s1, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	row := FileRow{ShareID: "s1", RelPath: "a.txt", Type: protocol.FileTypeFile, Size: 3, MTimeNS: 100, UpdatedAt: time.Now().UTC()}
	if err := s1.PutFile(ctx, row); err != nil {
		t.Fatalf("PutFile: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("second Open (reopen): %v", err)
	}
	defer s2.Close()

	got, err := s2.GetFile(ctx, "s1", "a.txt")
	if err != nil {
		t.Fatalf("GetFile after reopen: %v", err)
	}
	if got.Size != 3 {
		t.Errorf("Size = %d, want 3", got.Size)
	}

	// Reopening again (a third time) must not error or double-apply
	// migrations.
	s3, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("third Open: %v", err)
	}
	defer s3.Close()
}

func TestPutGetFileRoundTripsVersionVector(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	want := FileRow{
		ShareID:   "share1",
		RelPath:   "dir/file.txt",
		Type:      protocol.FileTypeFile,
		Size:      1234,
		MTimeNS:   987654321,
		Mode:      0644,
		SHA256:    []byte{1, 2, 3, 4},
		Version:   protocol.VersionVector{"aabbccdd": 5, "11223344": 2},
		Deleted:   false,
		UpdatedAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
	}
	if err := s.PutFile(ctx, want); err != nil {
		t.Fatalf("PutFile: %v", err)
	}

	got, err := s.GetFile(ctx, want.ShareID, want.RelPath)
	if err != nil {
		t.Fatalf("GetFile: %v", err)
	}

	if got.Size != want.Size || got.MTimeNS != want.MTimeNS || got.Mode != want.Mode {
		t.Errorf("basic fields mismatch: got %+v, want %+v", got, want)
	}
	if string(got.SHA256) != string(want.SHA256) {
		t.Errorf("SHA256 = %v, want %v", got.SHA256, want.SHA256)
	}
	if len(got.Version) != len(want.Version) {
		t.Fatalf("Version length = %d, want %d", len(got.Version), len(want.Version))
	}
	for k, v := range want.Version {
		if got.Version[k] != v {
			t.Errorf("Version[%q] = %d, want %d", k, got.Version[k], v)
		}
	}
	if !got.UpdatedAt.Equal(want.UpdatedAt) {
		t.Errorf("UpdatedAt = %v, want %v", got.UpdatedAt, want.UpdatedAt)
	}

	// Upsert: putting the same key again with different content replaces it.
	want.Size = 9999
	if err := s.PutFile(ctx, want); err != nil {
		t.Fatalf("PutFile (update): %v", err)
	}
	got, err = s.GetFile(ctx, want.ShareID, want.RelPath)
	if err != nil {
		t.Fatalf("GetFile after update: %v", err)
	}
	if got.Size != 9999 {
		t.Errorf("Size after update = %d, want 9999", got.Size)
	}
}

func TestGetFileNotFound(t *testing.T) {
	s := openTestStore(t)
	_, err := s.GetFile(context.Background(), "share1", "nope.txt")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("GetFile error = %v, want wrapping ErrNotFound", err)
	}
}

func TestTombstoneSurvives(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	row := FileRow{ShareID: "s1", RelPath: "gone.txt", Type: protocol.FileTypeFile, Size: 5, MTimeNS: 1, UpdatedAt: time.Now().UTC()}
	if err := s.PutFile(ctx, row); err != nil {
		t.Fatalf("PutFile: %v", err)
	}

	row.Deleted = true
	if err := s.PutFile(ctx, row); err != nil {
		t.Fatalf("PutFile (tombstone): %v", err)
	}

	got, err := s.GetFile(ctx, "s1", "gone.txt")
	if err != nil {
		t.Fatalf("GetFile: %v", err)
	}
	if !got.Deleted {
		t.Error("Deleted = false, want true (tombstone should survive as a row, not disappear)")
	}

	live, err := s.ListShare(ctx, "s1", false)
	if err != nil {
		t.Fatalf("ListShare(includeDeleted=false): %v", err)
	}
	if len(live) != 0 {
		t.Errorf("ListShare(includeDeleted=false) = %d rows, want 0 (tombstone should be excluded)", len(live))
	}

	all, err := s.ListShare(ctx, "s1", true)
	if err != nil {
		t.Fatalf("ListShare(includeDeleted=true): %v", err)
	}
	if len(all) != 1 {
		t.Fatalf("ListShare(includeDeleted=true) = %d rows, want 1", len(all))
	}

	if all[0].RelPath != "gone.txt" || !all[0].Deleted {
		t.Errorf("ListShare(includeDeleted=true) = %+v, want [gone.txt tombstone]", all)
	}
}

func TestListShareOrderingAndScoping(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	for _, sh := range []string{"share-a", "share-b"} {
		for _, p := range []string{"z.txt", "a.txt", "m/mid.txt"} {
			row := FileRow{ShareID: sh, RelPath: p, Type: protocol.FileTypeFile, Size: 1, MTimeNS: 1, UpdatedAt: time.Now().UTC()}
			if err := s.PutFile(ctx, row); err != nil {
				t.Fatalf("PutFile(%s,%s): %v", sh, p, err)
			}
		}
	}

	rows, err := s.ListShare(ctx, "share-a", false)
	if err != nil {
		t.Fatalf("ListShare: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("len(rows) = %d, want 3", len(rows))
	}
	wantOrder := []string{"a.txt", "m/mid.txt", "z.txt"}
	for i, w := range wantOrder {
		if rows[i].RelPath != w {
			t.Errorf("rows[%d].RelPath = %q, want %q", i, rows[i].RelPath, w)
		}
	}
	for _, r := range rows {
		if r.ShareID != "share-a" {
			t.Errorf("ListShare(share-a) leaked row from share %q", r.ShareID)
		}
	}
}

func TestApplyScanResultBulkTransaction(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	// Pre-seed an existing row that will be deleted by this apply.
	if err := s.PutFile(ctx, FileRow{ShareID: "s1", RelPath: "old.txt", Type: protocol.FileTypeFile, Size: 1, MTimeNS: 1, UpdatedAt: now}); err != nil {
		t.Fatalf("seed PutFile: %v", err)
	}

	result := &ScanResult{
		ShareID: "s1",
		Added: []FileRow{
			{ShareID: "s1", RelPath: "new.txt", Type: protocol.FileTypeFile, Size: 10, MTimeNS: 2, UpdatedAt: now},
		},
		ContentChanged: []FileRow{},
		MetadataOnly:   []FileRow{},
		Deleted: []FileRow{
			{ShareID: "s1", RelPath: "old.txt", Type: protocol.FileTypeFile, Size: 1, MTimeNS: 1, Deleted: true, UpdatedAt: now},
		},
	}
	if err := s.ApplyScanResult(ctx, result); err != nil {
		t.Fatalf("ApplyScanResult: %v", err)
	}

	all, err := s.ListShare(ctx, "s1", true)
	if err != nil {
		t.Fatalf("ListShare: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("len(all) = %d, want 2", len(all))
	}
	live, err := s.ListShare(ctx, "s1", false)
	if err != nil {
		t.Fatalf("ListShare(live): %v", err)
	}
	if len(live) != 1 || live[0].RelPath != "new.txt" {
		t.Errorf("live rows = %+v, want just new.txt", live)
	}
}

func TestUpsertAndListPeerFiles(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	rows := []FileRow{
		{ShareID: "share1", RelPath: "a.txt", Type: protocol.FileTypeFile, Size: 1, MTimeNS: 1, Version: protocol.VersionVector{"peer1": 1}, UpdatedAt: now},
		{ShareID: "share1", RelPath: "b.txt", Type: protocol.FileTypeFile, Size: 2, MTimeNS: 2, UpdatedAt: now},
	}
	if err := s.UpsertPeerFiles(ctx, "peer1", rows); err != nil {
		t.Fatalf("UpsertPeerFiles: %v", err)
	}

	got, err := s.ListPeerFiles(ctx, "peer1", "share1")
	if err != nil {
		t.Fatalf("ListPeerFiles: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len(got) = %d, want 2", len(got))
	}

	if got[0].RelPath != "a.txt" || got[0].Version["peer1"] != 1 {
		t.Errorf("ListPeerFiles[0] = %+v, want a.txt at peer1:1", got[0])
	}

	// Re-upserting one row updates it in place without touching the other.
	updated := rows[0]
	updated.Size = 999
	if err := s.UpsertPeerFiles(ctx, "peer1", []FileRow{updated}); err != nil {
		t.Fatalf("UpsertPeerFiles (update): %v", err)
	}
	got, err = s.ListPeerFiles(ctx, "peer1", "share1")
	if err != nil {
		t.Fatalf("ListPeerFiles after update: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len(got) after update = %d, want 2", len(got))
	}
}

// TestConcurrentReadersAndWriter exercises the store's stated concurrency
// model (single *sql.DB, WAL, busy_timeout) under real concurrent access:
// many goroutines reading while one goroutine writes, none of them should
// deadlock, error unexpectedly, or observe a corrupted row (a torn write
// would show up as an update whose fields don't agree with any version we
// actually wrote).
func TestConcurrentReadersAndWriter(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	const shareID = "concurrent-share"

	if err := s.PutFile(ctx, FileRow{ShareID: shareID, RelPath: "f.txt", Type: protocol.FileTypeFile, Size: 0, MTimeNS: 0, UpdatedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("seed PutFile: %v", err)
	}

	const iterations = 200
	var wg sync.WaitGroup
	errCh := make(chan error, iterations*2)

	// Writer: repeatedly upserts the same row with monotonically
	// increasing Size/MTimeNS, so a reader can sanity-check consistency
	// (Size == MTimeNS on every observed row, since the writer always
	// sets them equal).
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 1; i <= iterations; i++ {
			row := FileRow{ShareID: shareID, RelPath: "f.txt", Type: protocol.FileTypeFile, Size: int64(i), MTimeNS: int64(i), UpdatedAt: time.Now().UTC()}
			if err := s.PutFile(ctx, row); err != nil {
				errCh <- fmt.Errorf("writer PutFile #%d: %w", i, err)
				return
			}
		}
	}()

	// Readers.
	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				row, err := s.GetFile(ctx, shareID, "f.txt")
				if err != nil {
					errCh <- fmt.Errorf("reader GetFile: %w", err)
					return
				}
				if row.Size != row.MTimeNS {
					errCh <- fmt.Errorf("reader observed torn row: Size=%d MTimeNS=%d", row.Size, row.MTimeNS)
					return
				}
				if _, err := s.ListShare(ctx, shareID, true); err != nil {
					errCh <- fmt.Errorf("reader ListShare: %w", err)
					return
				}
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("concurrent readers/writer test timed out (possible deadlock)")
	}
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}

// TestApplyScanResultIgnoredParksWithoutJournal is the store half of the
// no-tombstone rule. A newly-ignored row must disappear from every listing
// that feeds the wire, and — the part that actually protects the peers —
// must leave change_journal and share_state.next_seq untouched, so nothing
// about it ever reaches a peer as a change.
func TestApplyScanResultIgnoredParksWithoutJournal(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	seed := FileRow{ShareID: "s1", RelPath: "debug.log", Type: protocol.FileTypeFile, Size: 5, MTimeNS: 1, UpdatedAt: now}
	if err := s.PutFile(ctx, seed); err != nil {
		t.Fatalf("seed PutFile: %v", err)
	}

	countJournal := func() int {
		t.Helper()
		var n int
		if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM change_journal WHERE share_id = ?`, "s1").Scan(&n); err != nil {
			t.Fatalf("count change_journal: %v", err)
		}
		return n
	}
	_, seqBefore, err := s.ShareState(ctx, "s1")
	if err != nil {
		t.Fatalf("ShareState: %v", err)
	}
	journalBefore := countJournal()

	if err := s.ApplyScanResult(ctx, &ScanResult{ShareID: "s1", Ignored: []FileRow{seed}}); err != nil {
		t.Fatalf("ApplyScanResult: %v", err)
	}

	// The row is gone outright — not left behind as a tombstone.
	all, err := s.ListShare(ctx, "s1", true)
	if err != nil {
		t.Fatalf("ListShare: %v", err)
	}
	if len(all) != 0 {
		t.Errorf("rows = %+v, want none (an ignored row is parked, and never tombstoned)", all)
	}

	if got := countJournal(); got != journalBefore {
		t.Errorf("change_journal grew from %d to %d: an ignored row was journalled and would propagate as a delete", journalBefore, got)
	}
	_, seqAfter, err := s.ShareState(ctx, "s1")
	if err != nil {
		t.Fatalf("ShareState after: %v", err)
	}
	if seqAfter != seqBefore {
		t.Errorf("share_state.next_seq moved %d -> %d; the ignored delete must not advance the journal cursor", seqBefore, seqAfter)
	}
}

// TestIgnoredRowIsParkedNotDeleted pins why an ignored row is kept at all.
// Deleting it would restart the file's version vector if the rule were
// ever removed, producing a version the peer already holds — the two sides
// would compare equal while their contents differed, and would never
// converge again. Parking keeps the vector; only the scanner can see it.
func TestIgnoredRowIsParkedNotDeleted(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	seed := FileRow{
		ShareID: "s1", RelPath: "shared.txt", Type: protocol.FileTypeFile,
		Size: 5, MTimeNS: 1, Version: protocol.VersionVector{"alice": 7}, UpdatedAt: now,
	}
	if err := s.PutFile(ctx, seed); err != nil {
		t.Fatalf("seed PutFile: %v", err)
	}
	if err := s.ApplyScanResult(ctx, &ScanResult{ShareID: "s1", Ignored: []FileRow{seed}}); err != nil {
		t.Fatalf("ApplyScanResult: %v", err)
	}

	// Invisible to everything that feeds the wire.
	rows, err := s.ListShare(ctx, "s1", true)
	if err != nil {
		t.Fatalf("ListShare: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("ListShare returned a parked row: %+v", rows)
	}
	if _, err := s.GetFile(ctx, "s1", "shared.txt"); !errors.Is(err, ErrNotFound) {
		t.Errorf("GetFile returned a parked row (err = %v); it could then be served to a peer", err)
	}

	// Visible to the scanner, with its version vector intact.
	m, err := s.ListShareMap(ctx, "s1")
	if err != nil {
		t.Fatalf("ListShareMap: %v", err)
	}
	parked, ok := m["shared.txt"]
	if !ok {
		t.Fatal("ListShareMap lost the parked row; the version vector is gone and un-ignoring would silently diverge")
	}
	if !parked.Ignored {
		t.Error("parked row not marked Ignored")
	}
	if parked.Version["alice"] != 7 {
		t.Errorf("parked version = %v, want alice:7 preserved", parked.Version)
	}
}

// TestUnparkOnNormalWrite: any ordinary upsert clears the parked flag, so
// removing an ignore rule needs no special un-park path.
func TestUnparkOnNormalWrite(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	seed := FileRow{ShareID: "s1", RelPath: "a.txt", Type: protocol.FileTypeFile, Size: 1, MTimeNS: 1, UpdatedAt: now}
	if err := s.PutFile(ctx, seed); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := s.ApplyScanResult(ctx, &ScanResult{ShareID: "s1", Ignored: []FileRow{seed}}); err != nil {
		t.Fatalf("park: %v", err)
	}
	revived := seed
	revived.Size = 2
	revived.Version = protocol.VersionVector{"alice": 8}
	if err := s.PutFile(ctx, revived); err != nil {
		t.Fatalf("revive: %v", err)
	}

	got, err := s.GetFile(ctx, "s1", "a.txt")
	if err != nil {
		t.Fatalf("GetFile after revive: %v", err)
	}
	if got.Size != 2 {
		t.Errorf("Size = %d, want 2", got.Size)
	}
}

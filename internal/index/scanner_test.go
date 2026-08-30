package index

import (
	"context"
	"crypto/sha256"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/nickmarrone/syncat/internal/protocol"
)

func sha256Of(t *testing.T, data []byte) []byte {
	t.Helper()
	h := sha256.Sum256(data)
	return h[:]
}

func writeFile(t *testing.T, root, relpath string, content []byte, mtime time.Time) {
	t.Helper()
	full := filepath.Join(root, filepath.FromSlash(relpath))
	if err := os.MkdirAll(filepath.Dir(full), 0755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(full, content, 0644); err != nil {
		t.Fatalf("WriteFile %s: %v", full, err)
	}
	if err := os.Chtimes(full, mtime, mtime); err != nil {
		t.Fatalf("Chtimes %s: %v", full, err)
	}
}

func findRow(t *testing.T, rows []FileRow, relpath string) FileRow {
	t.Helper()
	for _, r := range rows {
		if r.RelPath == relpath {
			return r
		}
	}
	t.Fatalf("no row for %q in %+v", relpath, rows)
	return FileRow{}
}

func TestScanAddsNewFile(t *testing.T) {
	root := t.TempDir()
	content := []byte("hello world")
	writeFile(t, root, "a.txt", content, time.Now())

	sc := NewScanner(os.DirFS(root), nil)
	result, err := sc.Scan(context.Background(), "share1", nil)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}

	if len(result.Added) != 1 {
		t.Fatalf("len(Added) = %d, want 1: %+v", len(result.Added), result.Added)
	}
	row := result.Added[0]
	if row.RelPath != "a.txt" {
		t.Errorf("RelPath = %q, want a.txt", row.RelPath)
	}
	if row.Type != protocol.FileTypeFile {
		t.Errorf("Type = %q, want file", row.Type)
	}
	if row.Size != int64(len(content)) {
		t.Errorf("Size = %d, want %d", row.Size, len(content))
	}
	want := sha256Of(t, content)
	if string(row.SHA256) != string(want) {
		t.Errorf("SHA256 = %x, want %x", row.SHA256, want)
	}
	if len(result.ContentChanged) != 0 || len(result.MetadataOnly) != 0 || len(result.Deleted) != 0 {
		t.Errorf("unexpected non-Added changes: %+v", result)
	}
}

func TestScanContentChangeDetectsHashDiff(t *testing.T) {
	root := t.TempDir()
	t1 := time.Now().Add(-time.Hour)
	writeFile(t, root, "a.txt", []byte("version one"), t1)

	sc := NewScanner(os.DirFS(root), nil)
	first, err := sc.Scan(context.Background(), "share1", nil)
	if err != nil {
		t.Fatalf("first Scan: %v", err)
	}
	existing := map[string]FileRow{"a.txt": first.Added[0]}

	// Change size AND mtime, and the actual bytes.
	t2 := time.Now()
	writeFile(t, root, "a.txt", []byte("version two, longer content"), t2)

	second, err := sc.Scan(context.Background(), "share1", existing)
	if err != nil {
		t.Fatalf("second Scan: %v", err)
	}
	if len(second.ContentChanged) != 1 {
		t.Fatalf("len(ContentChanged) = %d, want 1: %+v", len(second.ContentChanged), second)
	}
	if len(second.MetadataOnly) != 0 {
		t.Errorf("MetadataOnly should be empty, got %+v", second.MetadataOnly)
	}
	if len(second.Added) != 0 {
		t.Errorf("Added should be empty for an existing file, got %+v", second.Added)
	}
	row := second.ContentChanged[0]
	want := sha256Of(t, []byte("version two, longer content"))
	if string(row.SHA256) != string(want) {
		t.Errorf("SHA256 = %x, want %x", row.SHA256, want)
	}
}

// TestScanMetadataOnlyChangeIsNotContentChange is the load-bearing test the
// phase brief calls out explicitly: mtime differs from the index but the
// hash is identical (e.g. a `touch`, or a rewrite that produced identical
// bytes) must classify as MetadataOnly, never ContentChanged.
func TestScanMetadataOnlyChangeIsNotContentChange(t *testing.T) {
	root := t.TempDir()
	content := []byte("unchanged bytes")
	t1 := time.Now().Add(-time.Hour)
	writeFile(t, root, "a.txt", content, t1)

	sc := NewScanner(os.DirFS(root), nil)
	first, err := sc.Scan(context.Background(), "share1", nil)
	if err != nil {
		t.Fatalf("first Scan: %v", err)
	}
	existing := map[string]FileRow{"a.txt": first.Added[0]}

	// Rewrite with identical content but a new mtime (size stays the
	// same, but mtime differs, which is enough to trigger the hash check
	// per SPEC.md §5).
	t2 := time.Now()
	writeFile(t, root, "a.txt", content, t2)

	second, err := sc.Scan(context.Background(), "share1", existing)
	if err != nil {
		t.Fatalf("second Scan: %v", err)
	}
	if len(second.ContentChanged) != 0 {
		t.Fatalf("ContentChanged should be empty for a hash-identical rewrite, got %+v", second.ContentChanged)
	}
	if len(second.MetadataOnly) != 1 {
		t.Fatalf("len(MetadataOnly) = %d, want 1: %+v", len(second.MetadataOnly), second)
	}
	row := second.MetadataOnly[0]
	if row.MTimeNS != t2.UnixNano() {
		t.Errorf("MetadataOnly row MTimeNS not updated: got %d, want %d", row.MTimeNS, t2.UnixNano())
	}
	want := sha256Of(t, content)
	if string(row.SHA256) != string(want) {
		t.Errorf("SHA256 changed unexpectedly: %x vs %x", row.SHA256, want)
	}
}

func TestScanDeletionProducesTombstone(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "a.txt", []byte("bye"), time.Now())

	sc := NewScanner(os.DirFS(root), nil)
	first, err := sc.Scan(context.Background(), "share1", nil)
	if err != nil {
		t.Fatalf("first Scan: %v", err)
	}
	existing := map[string]FileRow{"a.txt": first.Added[0]}

	if err := os.Remove(filepath.Join(root, "a.txt")); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	second, err := sc.Scan(context.Background(), "share1", existing)
	if err != nil {
		t.Fatalf("second Scan: %v", err)
	}
	if len(second.Deleted) != 1 {
		t.Fatalf("len(Deleted) = %d, want 1: %+v", len(second.Deleted), second)
	}
	if !second.Deleted[0].Deleted {
		t.Error("tombstone row Deleted = false, want true")
	}
	if second.Deleted[0].RelPath != "a.txt" {
		t.Errorf("tombstone RelPath = %q, want a.txt", second.Deleted[0].RelPath)
	}

	// A file already tombstoned in the index and still absent shouldn't
	// be reported again.
	existing["a.txt"] = second.Deleted[0]
	third, err := sc.Scan(context.Background(), "share1", existing)
	if err != nil {
		t.Fatalf("third Scan: %v", err)
	}
	if len(third.Deleted) != 0 {
		t.Errorf("re-scanning an already-tombstoned deletion produced another tombstone: %+v", third.Deleted)
	}
}

func TestScanNestedAndEmptyDirectories(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "empty"), 0755); err != nil {
		t.Fatalf("MkdirAll empty: %v", err)
	}
	writeFile(t, root, "nested/deep/file.txt", []byte("x"), time.Now())

	sc := NewScanner(os.DirFS(root), nil)
	result, err := sc.Scan(context.Background(), "share1", nil)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}

	wantPaths := map[string]protocol.FileType{
		"empty":                protocol.FileTypeDir,
		"nested":               protocol.FileTypeDir,
		"nested/deep":          protocol.FileTypeDir,
		"nested/deep/file.txt": protocol.FileTypeFile,
	}
	got := make(map[string]protocol.FileType, len(result.Added))
	for _, r := range result.Added {
		got[r.RelPath] = r.Type
	}
	for p, typ := range wantPaths {
		gotTyp, ok := got[p]
		if !ok {
			t.Errorf("missing added entry for %q (got %+v)", p, got)
			continue
		}
		if gotTyp != typ {
			t.Errorf("%q Type = %q, want %q", p, gotTyp, typ)
		}
	}
}

func TestScanLargeFileStreamsWithoutLoadingWhole(t *testing.T) {
	root := t.TempDir()
	// 32MB is well beyond anything reasonable to buffer whole in a test
	// process without it showing up; the real assertion here is on
	// correctness of the streamed hash, with a heap-growth sanity check
	// as a secondary signal that hashFile isn't slurping the file into
	// one big []byte.
	const size = 32 * 1024 * 1024
	f, err := os.Create(filepath.Join(root, "big.bin"))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	h := sha256.New()
	buf := make([]byte, 1<<20)
	for i := range buf {
		buf[i] = byte(i)
	}
	written := 0
	for written < size {
		n := len(buf)
		if written+n > size {
			n = size - written
		}
		if _, err := f.Write(buf[:n]); err != nil {
			t.Fatalf("Write: %v", err)
		}
		h.Write(buf[:n])
		written += n
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	wantSHA := h.Sum(nil)

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)

	sc := NewScanner(os.DirFS(root), nil)
	result, err := sc.Scan(context.Background(), "share1", nil)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}

	runtime.ReadMemStats(&after)

	row := findRow(t, result.Added, "big.bin")
	if row.Size != size {
		t.Errorf("Size = %d, want %d", row.Size, size)
	}
	if string(row.SHA256) != string(wantSHA) {
		t.Error("SHA256 mismatch on large file")
	}

	// Streaming via io.Copy uses a small fixed buffer (32KB by default);
	// allocating the whole 32MB file would be a very visible, order of
	// magnitude larger jump in TotalAlloc.
	grew := after.TotalAlloc - before.TotalAlloc
	if grew > size/2 {
		t.Errorf("heap grew by %d bytes hashing a %d-byte file; looks like the file was buffered whole instead of streamed", grew, size)
	}
}

func TestScanFileVanishingMidScanDoesNotAbort(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "keep.txt", []byte("stays"), time.Now())
	if err := os.WriteFile(filepath.Join(root, "vanish.txt"), []byte("temporary"), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// A custom fs.FS wrapping os.DirFS(root) that deletes vanish.txt the
	// moment it's opened, simulating a concurrent delete racing the
	// scanner's hash pass (the file is still present, and its metadata
	// still readable, when the walk lists and stats it — it only
	// disappears when the scanner tries to read its bytes).
	fsys := &vanishingFS{FS: os.DirFS(root), root: root, vanish: "vanish.txt"}

	sc := NewScanner(fsys, nil)
	result, err := sc.Scan(context.Background(), "share1", nil)
	if err != nil {
		t.Fatalf("Scan returned a hard error instead of skip-and-continue: %v", err)
	}

	got := findRow(t, result.Added, "keep.txt")
	if got.RelPath != "keep.txt" {
		t.Errorf("keep.txt missing from Added: %+v", result.Added)
	}
	for _, r := range result.Added {
		if r.RelPath == "vanish.txt" {
			t.Errorf("vanish.txt should have been skipped, not added: %+v", r)
		}
	}
	if len(result.Warnings) == 0 {
		t.Error("expected a warning about the vanished file")
	}
}

// vanishingFS deletes the real underlying file the moment its relpath is
// Open()ed, then delegates to the wrapped FS (whose Open will now fail),
// to simulate a file disappearing between the scanner stat'ing it and
// hashing it.
type vanishingFS struct {
	fs.FS
	root   string
	vanish string
}

func (v *vanishingFS) Open(name string) (fs.File, error) {
	if name == v.vanish {
		os.Remove(filepath.Join(v.root, name))
	}
	return v.FS.Open(name)
}

func TestScanSkipsSymlinks(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "real.txt", []byte("data"), time.Now())
	if err := os.Symlink(filepath.Join(root, "real.txt"), filepath.Join(root, "link.txt")); err != nil {
		t.Skipf("symlinks not supported on this platform: %v", err)
	}

	var warnings []string
	sc := NewScanner(os.DirFS(root), nil)
	sc.SetWarnFunc(func(format string, args ...any) {
		warnings = append(warnings, format)
	})

	result, err := sc.Scan(context.Background(), "share1", nil)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	for _, r := range result.Added {
		if r.RelPath == "link.txt" {
			t.Errorf("symlink should not be added to the index: %+v", r)
		}
	}
	if len(result.Warnings) == 0 {
		t.Error("expected a warning about the skipped symlink")
	}
	found := false
	for _, w := range result.Warnings {
		if strings.Contains(w, "link.txt") {
			found = true
		}
	}
	if !found {
		t.Errorf("no warning mentioned link.txt: %+v", result.Warnings)
	}
}

func TestScanIgnoresMatchedPaths(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "keep.txt", []byte("a"), time.Now())
	writeFile(t, root, ".DS_Store", []byte("junk"), time.Now())
	writeFile(t, root, "build/output.log", []byte("log"), time.Now())

	m := NewMatcher([]string{"build"})
	sc := NewScanner(os.DirFS(root), m)
	result, err := sc.Scan(context.Background(), "share1", nil)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	for _, r := range result.Added {
		if r.RelPath == ".DS_Store" || strings.HasPrefix(r.RelPath, "build") {
			t.Errorf("ignored path %q was added: %+v", r.RelPath, r)
		}
	}
	got := findRow(t, result.Added, "keep.txt")
	if got.RelPath != "keep.txt" {
		t.Fatal("keep.txt missing")
	}
}

package index

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"strings"
	"time"

	"github.com/nickmarrone/syncat/internal/protocol"
)

// ScanResult is a Scanner.Scan diff against the index's current view of a
// share, bucketed by the classification SPEC.md §5 asks for. Scanning
// never mutates the index itself — see Scanner.Scan's doc comment for why
// applying is a separate step — so a ScanResult is inert data until a
// caller passes it to Store.ApplyScanResult.
//
// None of the rows here carry a bumped version vector: Scanner has no
// opinion on version-vector algebra (that's Phase 5). For an existing
// file, a row's Version is copied unchanged from the index's current
// entry; for a brand-new or resurrected path, Version is nil. Phase 5's
// apply step decides how to bump local counters before these rows become
// durable via ApplyScanResult (or is expected to bump them itself before
// storing, if it needs different behavior).
type ScanResult struct {
	ShareID string

	// Added holds paths with no corresponding live index entry: brand new
	// files/dirs, and paths whose only prior index entry was a tombstone
	// (Deleted=true) — i.e. resurrections.
	Added []FileRow

	// ContentChanged holds files whose size or mtime differ from the
	// index AND whose sha256 differs from the index's recorded hash —
	// i.e. the bytes actually changed.
	ContentChanged []FileRow

	// MetadataOnly holds files whose size or mtime differ from the index
	// but whose sha256 is unchanged (a touch, a chmod, or a rewrite that
	// produced identical bytes), plus files whose mode changed with size
	// and mtime unchanged. No transfer is needed for these; only the
	// index row's metadata advances.
	MetadataOnly []FileRow

	// Deleted holds tombstones for index entries that were live before
	// this scan but were not found on disk (or now resolve to an ignored
	// or symlink path). Directories are only tombstoned when they
	// disappear entirely; SPEC.md §5's "keep non-empty locally-modified
	// dirs" rule belongs to Phase 5's apply step, not here.
	Deleted []FileRow

	// Warnings collects human-readable descriptions of entries the scan
	// skipped rather than erroring out on: symlinks (not followed in the
	// MVP), and paths that vanished or changed underfoot mid-walk. A
	// non-empty Warnings never means the scan failed — see Scanner.Scan.
	Warnings []string
}

// Scanner walks one share root, classifying every entry against the
// index's current view of that share (SPEC.md §5 "Scanning"). It reads
// the tree exclusively through FS (an fs.FS), not os.* directly, per
// SPEC.md §12 — see fs.go.
//
// The zero value is not usable; construct with NewScanner.
type Scanner struct {
	fsys   FS
	ignore *Matcher // nil means nothing is ignored
}

// NewScanner returns a Scanner that reads share contents through fsys
// (typically os.DirFS(shareRoot) in production) and excludes paths
// matched by ignore (nil is fine — nothing is ignored).
func NewScanner(fsys FS, ignore *Matcher) *Scanner {
	return &Scanner{fsys: fsys, ignore: ignore}
}

// warnf formats one skipped-entry message for ScanResult.Warnings.
func (sc *Scanner) warnf(format string, args ...any) string {
	return fmt.Sprintf(format, args...)
}

// Scan walks the share and classifies every entry against existing (the
// index's current rows for this share, including tombstones — see
// Store.ListShareMap). It does not write anything to the index; the
// caller applies the result explicitly (e.g. via Store.ApplyScanResult)
// once it's ready to make the change durable. This split matters because
// Phase 5's reconciler needs to see a scan's outcome before it becomes the
// new source of truth (e.g. to fold in version-vector bumps).
//
// Scan is robust to the tree changing underneath it: a file or directory
// that vanishes, or a file that shrinks/errors out while being hashed,
// is recorded in ScanResult.Warnings and skipped, never treated as a
// fatal error that aborts the rest of the walk. Scan returns a non-nil
// error only for a failure that makes the walk itself impossible to
// continue (e.g. the share root itself is unreadable) or for ctx
// cancellation.
func (sc *Scanner) Scan(ctx context.Context, shareID string, existing map[string]FileRow) (*ScanResult, error) {
	result := &ScanResult{ShareID: shareID}
	seen := make(map[string]bool, len(existing))
	now := time.Now().UTC()

	walkErr := fs.WalkDir(sc.fsys, ".", func(p string, d fs.DirEntry, err error) error {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if p == "." {
			// The share root itself: never emit a row for it, but do
			// propagate a hard failure to read it (nothing to scan).
			return err
		}
		if err != nil {
			// A directory or file vanished, or became unreadable,
			// between being listed by its parent and being visited here.
			// Skip and continue (never abort the whole scan).
			result.Warnings = append(result.Warnings, sc.warnf("index: scan %s: skip %s (vanished or unreadable): %v", shareID, p, err))
			if d != nil && d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}

		relpath := path.Clean(p)
		if relpath == "." || relpath == ".." || strings.HasPrefix(relpath, "../") || path.IsAbs(relpath) {
			// Defensive only: fs.WalkDir's paths are already clean,
			// relative, and rooted at fsys — this should be unreachable
			// for any real fs.FS implementation, but we never trust a
			// path enough to let it name something outside the share.
			result.Warnings = append(result.Warnings, sc.warnf("index: scan %s: rejecting escaping path %q", shareID, p))
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}

		if sc.ignore.Match(relpath) {
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}

		if d.Type()&fs.ModeSymlink != 0 {
			// SPEC.md §5: symlinks are not followed; MVP scope skips them
			// entirely (with a warning) rather than syncing the link
			// itself. Not recursed into either way, since a symlink
			// dirent is never also a directory dirent.
			result.Warnings = append(result.Warnings, sc.warnf("index: scan %s: skipping symlink %s (not synced in this version)", shareID, relpath))
			return nil
		}

		info, err := d.Info()
		if err != nil {
			result.Warnings = append(result.Warnings, sc.warnf("index: scan %s: skip %s (stat failed, likely vanished): %v", shareID, relpath, err))
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}

		seen[relpath] = true
		old, existed := existing[relpath]

		if d.IsDir() {
			if !existed || old.Deleted || old.Type != protocol.FileTypeDir {
				result.Added = append(result.Added, FileRow{
					ShareID: shareID, RelPath: relpath, Type: protocol.FileTypeDir,
					MTimeNS: info.ModTime().UnixNano(), Mode: uint32(info.Mode().Perm()),
					Version: carriedVersion(old, existed), UpdatedAt: now,
				})
			}
			return nil
		}

		newSize := info.Size()
		newMTimeNS := info.ModTime().UnixNano()
		newMode := uint32(info.Mode().Perm())

		if !existed || old.Deleted || old.Type != protocol.FileTypeFile {
			sha, err := sc.hashFile(relpath)
			if err != nil {
				result.Warnings = append(result.Warnings, sc.warnf("index: scan %s: skip %s (read failed, likely vanished): %v", shareID, relpath, err))
				return nil
			}
			result.Added = append(result.Added, FileRow{
				ShareID: shareID, RelPath: relpath, Type: protocol.FileTypeFile,
				Size: newSize, MTimeNS: newMTimeNS, Mode: newMode, SHA256: sha,
				Version: carriedVersion(old, existed), UpdatedAt: now,
			})
			return nil
		}

		sizeOrMTimeChanged := newSize != old.Size || newMTimeNS != old.MTimeNS
		modeChanged := newMode != old.Mode

		if !sizeOrMTimeChanged {
			if modeChanged {
				row := old
				row.Mode = newMode
				row.Version = cloneVersion(old.Version)
				row.UpdatedAt = now
				result.MetadataOnly = append(result.MetadataOnly, row)
			}
			return nil
		}

		// SPEC.md §5: "A file is 'changed' when size or mtime differs
		// from the index; then hash (sha256) to confirm. Hash-equal =>
		// metadata-only update, no transfer." This is the load-bearing
		// distinction the phase brief calls out for explicit testing.
		sha, err := sc.hashFile(relpath)
		if err != nil {
			result.Warnings = append(result.Warnings, sc.warnf("index: scan %s: skip %s (read failed, likely changed/vanished mid-scan): %v", shareID, relpath, err))
			return nil
		}
		row := FileRow{
			ShareID: shareID, RelPath: relpath, Type: protocol.FileTypeFile,
			Size: newSize, MTimeNS: newMTimeNS, Mode: newMode, SHA256: sha,
			Version: cloneVersion(old.Version), UpdatedAt: now,
		}
		if bytes.Equal(sha, old.SHA256) {
			result.MetadataOnly = append(result.MetadataOnly, row)
		} else {
			result.ContentChanged = append(result.ContentChanged, row)
		}
		return nil
	})
	if walkErr != nil {
		if errors.Is(walkErr, context.Canceled) || errors.Is(walkErr, context.DeadlineExceeded) {
			return nil, walkErr
		}
		return nil, fmt.Errorf("index: scan share %s: %w", shareID, walkErr)
	}

	for relpath, old := range existing {
		if old.Deleted || seen[relpath] {
			continue
		}
		row := old
		row.Deleted = true
		row.Version = cloneVersion(old.Version)
		row.UpdatedAt = now
		result.Deleted = append(result.Deleted, row)
	}

	return result, nil
}

// carriedVersion returns the version vector a newly-classified "Added" row
// should carry: nil for a genuinely new path, or the prior tombstone's
// vector (unchanged — Phase 5 decides how to bump it) for a resurrection.
func carriedVersion(old FileRow, existed bool) protocol.VersionVector {
	if !existed {
		return nil
	}
	return cloneVersion(old.Version)
}

// hashFile streams relpath's contents through sha256 without reading the
// whole file into memory, so scanning handles arbitrarily large files.
func (sc *Scanner) hashFile(relpath string) ([]byte, error) {
	f, err := sc.fsys.Open(relpath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return nil, err
	}
	return h.Sum(nil), nil
}

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

// --- the scanner: classifying a share tree against the index -----------

// ScanResult is a Scanner.Scan diff against the index's current view of a
// share, bucketed by the classification SPEC.md §5 asks for. Scanning
// never mutates the index itself — see Scanner.Scan's doc comment for why
// applying is a separate step — so a ScanResult is inert data until a
// caller passes it to Store.ApplyScanResult.
//
// None of the rows here carry a bumped version vector: Scanner has no
// opinion on version-vector algebra — that lives in internal/sync. For an
// existing file, a row's Version is copied unchanged from the index's
// current entry; for a brand-new or resurrected path, Version is nil. The
// caller bumps local counters (internal/core's rescanShare does) before
// these rows become durable via ApplyScanResult.
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
	// this scan but were not found on disk (or now resolve to a symlink
	// path). Directories are only tombstoned when they disappear
	// entirely; SPEC.md §5's "keep non-empty locally-modified dirs" rule
	// belongs to internal/sync's apply step, not here.
	//
	// A path that is merely newly *ignored* never lands here — see
	// Ignored, and the comment there for why the distinction matters.
	Deleted []FileRow

	// Ignored holds entries that still have a live index row but now
	// match an ignore rule: a pattern was added to .syncatignore, or the
	// global ignore list grew.
	//
	// These are PARKED, never tombstoned. The distinction is the whole
	// point: a tombstone is a delete, and it propagates, so tombstoning a
	// newly-ignored file would move every peer's copy to the trash.
	// Ignoring a file says what this node syncs; it is not a request to
	// destroy it elsewhere. Store.ApplyScanResult is what makes that true:
	// it sets files.ignored on these rows and writes no change_journal
	// entry, so peers are told nothing and keep what they have.
	//
	// Parking keeps the row (and with it the version vector) rather than
	// deleting it — see FileRow.Ignored for why discarding the vector
	// would leave the two sides permanently diverged if the rule were
	// ever removed.
	Ignored []FileRow

	// Warnings collects human-readable descriptions of entries the scan
	// skipped rather than erroring out on: symlinks (not followed in the
	// MVP), and paths that vanished or changed underfoot mid-walk. A
	// non-empty Warnings never means the scan failed — see Scanner.Scan.
	Warnings []string
}

// Scanner walks one share root, classifying every entry against the
// index's current view of that share (SPEC.md §5 "Scanning").
//
// It reads the tree exclusively through an fs.FS, never os.* directly, so
// a mobile port can supply its own sandboxed filesystem (SPEC.md §12).
// fs.FS is all the scanner needs: Open covers streaming reads for
// hashing, and fs.WalkDir type-asserts to fs.ReadDirFS for efficient
// directory listing when the value supports it. os.DirFS(shareRoot)
// satisfies both, so production needs no adapter and tests can substitute
// fstest.MapFS. There is deliberately no write-side interface: nothing
// here writes into a share tree — that belongs to internal/sync.
//
// The zero value is not usable; construct with NewScanner.
type Scanner struct {
	fsys   fs.FS
	ignore *Matcher // nil means nothing is ignored
}

// NewScanner returns a Scanner that reads share contents through fsys
// (typically os.DirFS(shareRoot) in production) and excludes paths
// matched by ignore (nil is fine — nothing is ignored).
func NewScanner(fsys fs.FS, ignore *Matcher) *Scanner {
	return &Scanner{fsys: fsys, ignore: ignore}
}

// warnf formats one skipped-entry message and records it in r.Warnings.
func (r *ScanResult) warnf(format string, args ...any) {
	r.Warnings = append(r.Warnings, fmt.Sprintf(format, args...))
}

// Scan walks the share and classifies every entry against existing (the
// index's current rows for this share, including tombstones — see
// Store.ListShareMap). It does not write anything to the index; the
// caller applies the result explicitly (e.g. via Store.ApplyScanResult)
// once it's ready to make the change durable. This split matters because
// the caller needs to see a scan's outcome before it becomes the new
// source of truth (e.g. to fold in version-vector bumps).
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
		return sc.visit(result, existing, seen, now, p, d, err)
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
		if old.Ignored {
			// A parked row whose file is gone from disk. It is not on
			// disk and not synced, so there is nothing to report — and
			// emphatically nothing to tombstone, since a tombstone would
			// propagate and delete the peer's copy of a file we merely
			// stopped tracking.
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

// visit is Scan's fs.WalkDir callback body for one entry: it skips what
// SPEC.md §5 says to skip (recording a warning where the skip is
// noteworthy), marks the path seen, and classifies the entry against
// existing into one of result's buckets. It returns fs.SkipDir to prune a
// subtree, and a non-nil error only for the share root itself being
// unreadable — every other problem is a warning, never a walk failure.
func (sc *Scanner) visit(result *ScanResult, existing map[string]FileRow, seen map[string]bool, now time.Time, p string, d fs.DirEntry, err error) error {
	shareID := result.ShareID
	if p == "." {
		// The share root itself: never emit a row for it, but do
		// propagate a hard failure to read it (nothing to scan).
		return err
	}
	if err != nil {
		// A directory or file vanished, or became unreadable,
		// between being listed by its parent and being visited here.
		// Skip and continue (never abort the whole scan).
		result.warnf("index: scan %s: skip %s (vanished or unreadable): %v", shareID, p, err)
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
		result.warnf("index: scan %s: rejecting escaping path %q", shareID, p)
		if d.IsDir() {
			return fs.SkipDir
		}
		return nil
	}

	if sc.ignore.MatchPath(relpath, d.IsDir()) {
		sc.noteIgnored(result, existing, seen, relpath)
		if d.IsDir() && sc.ignore.PruneDirs() {
			// Pruning means the children are never visited, so they
			// would never be marked seen — and Scan's tail loop would
			// tombstone every one of them. Account for them here.
			sc.noteIgnoredSubtree(result, existing, seen, relpath)
			return fs.SkipDir
		}
		return nil
	}

	if d.Type()&fs.ModeSymlink != 0 {
		// SPEC.md §5: symlinks are not followed; MVP scope skips them
		// entirely (with a warning) rather than syncing the link
		// itself. Not recursed into either way, since a symlink
		// dirent is never also a directory dirent.
		result.warnf("index: scan %s: skipping symlink %s (not synced in this version)", shareID, relpath)
		return nil
	}

	info, err := d.Info()
	if err != nil {
		result.warnf("index: scan %s: skip %s (stat failed, likely vanished): %v", shareID, relpath, err)
		if d.IsDir() {
			return fs.SkipDir
		}
		return nil
	}

	seen[relpath] = true
	old, existed := existing[relpath]

	if d.IsDir() {
		if !existed || old.Deleted || old.Ignored || old.Type != protocol.FileTypeDir {
			result.Added = append(result.Added, FileRow{
				ShareID: shareID, RelPath: relpath, Type: protocol.FileTypeDir,
				MTimeNS: info.ModTime().UnixNano(), Mode: uint32(info.Mode().Perm()),
				Version: carriedVersion(old, existed), UpdatedAt: now,
			})
		}
		return nil
	}

	// A parked (previously ignored) row resurrects through Added rather
	// than the classify path below, so that un-ignoring always produces a
	// row the caller will version-bump — even when the file's bytes never
	// changed while it was ignored, which classifyExisting would other-
	// wise report as "unchanged" and leave parked forever. carriedVersion
	// keeps the old vector, so the bump dominates the peer's copy.
	if !existed || old.Deleted || old.Ignored || old.Type != protocol.FileTypeFile {
		sha, err := sc.hashFile(relpath)
		if err != nil {
			result.warnf("index: scan %s: skip %s (read failed, likely vanished): %v", shareID, relpath, err)
			return nil
		}
		result.Added = append(result.Added, FileRow{
			ShareID: shareID, RelPath: relpath, Type: protocol.FileTypeFile,
			Size: info.Size(), MTimeNS: info.ModTime().UnixNano(), Mode: uint32(info.Mode().Perm()), SHA256: sha,
			Version: carriedVersion(old, existed), UpdatedAt: now,
		})
		return nil
	}

	// SPEC.md §5: "A file is 'changed' when size or mtime differs
	// from the index; then hash (sha256) to confirm. Hash-equal =>
	// metadata-only update, no transfer." This is the load-bearing
	// distinction; see scanner_test.go for the case that pins it.
	var sha []byte
	if needsHash(old, info) {
		sha, err = sc.hashFile(relpath)
		if err != nil {
			result.warnf("index: scan %s: skip %s (read failed, likely changed/vanished mid-scan): %v", shareID, relpath, err)
			return nil
		}
	}
	row, change := classifyExisting(shareID, relpath, old, info, sha, now)
	switch change {
	case fileMetadataOnly:
		result.MetadataOnly = append(result.MetadataOnly, row)
	case fileContentChanged:
		result.ContentChanged = append(result.ContentChanged, row)
	}
	return nil
}

// noteIgnored records that relpath is ignored: it marks the path seen, so
// Scan's tail loop does not mistake "ignored" for "deleted from disk", and
// — if the index still holds a live row for it — queues that row for
// removal in ScanResult.Ignored.
//
// Marking seen is the load-bearing half. Without it, adding one pattern to
// a .syncatignore tombstones every path it matches, and those tombstones
// propagate and delete the files on every peer.
func (sc *Scanner) noteIgnored(result *ScanResult, existing map[string]FileRow, seen map[string]bool, relpath string) {
	seen[relpath] = true
	// Only rows that are live and not already parked: re-reporting an
	// already-parked path every scan would make the "newly ignored" count
	// meaningless and rewrite rows that have not changed.
	if old, ok := existing[relpath]; ok && !old.Deleted && !old.Ignored {
		result.Ignored = append(result.Ignored, old)
	}
}

// noteIgnoredSubtree does the same for every indexed path beneath dir,
// which is what a pruned directory's children need: the walk is about to
// skip them, so nothing else will mark them seen.
func (sc *Scanner) noteIgnoredSubtree(result *ScanResult, existing map[string]FileRow, seen map[string]bool, dir string) {
	prefix := dir + "/"
	for relpath := range existing {
		if strings.HasPrefix(relpath, prefix) {
			sc.noteIgnored(result, existing, seen, relpath)
		}
	}
}

// fileChange is classifyExisting's verdict on a file the index already
// holds a live row for.
type fileChange int

const (
	fileUnchanged      fileChange = iota // nothing to record
	fileMetadataOnly                     // row goes in ScanResult.MetadataOnly
	fileContentChanged                   // row goes in ScanResult.ContentChanged
)

// needsHash reports whether info's size or mtime differ from old's — the
// cheap pre-check SPEC.md §5 uses to decide a file must be re-hashed
// before it can be classified. Files that pass it are never re-read.
func needsHash(old FileRow, info fs.FileInfo) bool {
	return info.Size() != old.Size || info.ModTime().UnixNano() != old.MTimeNS
}

// classifyExisting decides how a live, indexed file (old, stored under
// shareID/relpath) relates to what is on disk now (info), and builds the
// replacement row. sha is the file's current hash, and is only consulted
// when needsHash(old, info) is true — callers pass nil otherwise, having
// skipped the read. It is a pure function of its inputs: no I/O, no
// mutation of old (the returned row carries a fresh copy of old's version
// vector, unchanged).
//
// With size and mtime unchanged, only a mode change is reportable, and
// that is metadata-only. With size or mtime changed, the hash is the
// tiebreaker: equal means metadata-only, different means content changed.
func classifyExisting(shareID, relpath string, old FileRow, info fs.FileInfo, sha []byte, now time.Time) (FileRow, fileChange) {
	newMode := uint32(info.Mode().Perm())

	if !needsHash(old, info) {
		if newMode == old.Mode {
			return FileRow{}, fileUnchanged
		}
		row := old
		row.Mode = newMode
		row.Version = cloneVersion(old.Version)
		row.UpdatedAt = now
		return row, fileMetadataOnly
	}

	row := FileRow{
		ShareID: shareID, RelPath: relpath, Type: protocol.FileTypeFile,
		Size: info.Size(), MTimeNS: info.ModTime().UnixNano(), Mode: newMode, SHA256: sha,
		Version: cloneVersion(old.Version), UpdatedAt: now,
	}
	if bytes.Equal(sha, old.SHA256) {
		return row, fileMetadataOnly
	}
	return row, fileContentChanged
}

// carriedVersion returns the version vector a newly-classified "Added" row
// should carry: nil for a genuinely new path, or the prior tombstone's
// vector (unchanged — the caller decides how to bump it) for a resurrection.
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

// --- ignore matching (SPEC.md §5) --------------------------------------

// Matcher decides whether a share-relative path should be excluded from
// scanning (SPEC.md §5). It layers three sources, in this order:
//
//   - Built-in always-ignored names: our own temp-file prefix
//     (.syncat.tmp.*), the ignore file itself (.syncatignore, which is
//     per-node and never synced), and common OS junk files (.DS_Store,
//     Thumbs.db, desktop.ini).
//   - config.Config.GlobalIgnores: user-supplied glob patterns.
//   - The share's own .syncatignore, as parsed IgnoreRules (see ignore.go).
//
// The layering is deliberately one-way: a '!' negation in a .syncatignore
// can re-include a path the file's own patterns excluded, but it can never
// re-include one of the built-ins. Keeping .syncat.tmp.* ignored is a
// correctness invariant (those are our in-flight downloads), not a
// preference a user should be able to override by accident.
//
// Glob semantics for the first two layers, stated precisely because the web
// UI and the README both have to describe them exactly (the .syncatignore
// layer has its own, richer syntax — see ParseIgnore):
//
//   - Every pattern is a path.Match pattern: '*' matches any sequence of
//     non-'/' characters, '?' matches any single non-'/' character, and
//     '[...]' is a character class. There is no '**' — a pattern cannot
//     cross a '/' boundary by itself (path.Match's ErrBadPattern aside,
//     unmatched pattern syntax is treated as "does not match", not an
//     error — see path.Match's own doc comment).
//   - A path is ignored if the pattern matches EITHER the entry's base
//     name (path.Base(relpath)) OR the full share-relative path
//     (forward-slash separated, no leading '/'), tried independently. A
//     pattern with no '/' in it (e.g. "*.log") therefore matches a file
//     of that name at any depth, because it always matches the base-name
//     comparison; a pattern containing '/' (e.g. "build/output") only
//     matches when compared against the full relpath.
//   - A pattern that matches a directory also excludes everything beneath
//     it: every ancestor of a path is tested before the path itself. The
//     scanner gets this for free by pruning the walk, but internal/sync's
//     reconciler asks about one path at a time and never walks, so the
//     rule has to live here for the two to agree.
//   - relpath is always expected in forward-slash form (as produced by
//     the scanner — see scanner.go), regardless of host OS.
type Matcher struct {
	patterns []string
	rules    *IgnoreRules // the share's .syncatignore; nil when it has none
}

// builtinIgnorePatterns are always excluded, regardless of config
// (SPEC.md §5). ".syncat.tmp.*" is our own in-flight-download naming
// scheme (SPEC.md §5 "Applying remote changes"), and IgnoreFileName is the
// ignore file itself, which is per-node and deliberately not synced; the
// rest are common OS metadata files that should never be synced.
var builtinIgnorePatterns = []string{
	".syncat.tmp.*",
	IgnoreFileName,
	".DS_Store",
	"Thumbs.db",
	"desktop.ini",
}

// NewMatcher builds a Matcher from a share's global ignore list (typically
// config.Config.GlobalIgnores). The built-ins are always included in
// addition to globalIgnores.
func NewMatcher(globalIgnores []string) *Matcher {
	return NewMatcherWithRules(globalIgnores, nil)
}

// NewMatcherWithRules is NewMatcher plus the share's own parsed
// .syncatignore. rules may be nil, which is the common case of a share
// that has no ignore file.
func NewMatcherWithRules(globalIgnores []string, rules *IgnoreRules) *Matcher {
	patterns := make([]string, 0, len(builtinIgnorePatterns)+len(globalIgnores))
	patterns = append(patterns, builtinIgnorePatterns...)
	patterns = append(patterns, globalIgnores...)
	return &Matcher{patterns: patterns, rules: rules}
}

// Match reports whether relpath (forward-slash separated, share-relative,
// no leading '/') should be excluded from scanning, treating it as a
// non-directory. Prefer MatchPath, which can honour directory-only
// .syncatignore patterns; this exists for callers that have only a path.
func (m *Matcher) Match(relpath string) bool {
	return m.MatchPath(relpath, false)
}

// MatchPath reports whether relpath should be excluded from scanning.
// isDir says whether relpath names a directory, which directory-only
// patterns ("build/") need in order to match.
func (m *Matcher) MatchPath(relpath string, isDir bool) bool {
	if m == nil {
		return false
	}
	// Ancestors first: a glob naming a directory excludes everything
	// under it, the same way the scanner's fs.SkipDir does. Without this
	// the two disagree — the scanner would prune "build/" while the
	// reconciler happily pulled "build/out.o" back from a peer, indexed
	// it, pruned it again, and pulled it again.
	for i := 0; i < len(relpath); i++ {
		if relpath[i] == '/' && m.matchGlobs(relpath[:i]) {
			return true
		}
	}
	if m.matchGlobs(relpath) {
		return true
	}
	return m.rules.Match(relpath, isDir)
}

// matchGlobs tests p against the built-in and global patterns, by base
// name and by full path independently (see the type comment).
func (m *Matcher) matchGlobs(p string) bool {
	base := path.Base(p)
	for _, pat := range m.patterns {
		if ok, _ := path.Match(pat, base); ok {
			return true
		}
		if ok, _ := path.Match(pat, p); ok {
			return true
		}
	}
	return false
}

// PruneDirs reports whether the scanner may skip an ignored directory
// without descending into it. That is the cheap path — it is what keeps a
// node_modules/ rule from costing a walk of node_modules — but it is only
// sound when no rule can re-include something underneath. A '!' pattern
// anywhere in the share's .syncatignore turns it off, and the scanner
// tests every entry individually instead.
func (m *Matcher) PruneDirs() bool {
	if m == nil {
		return true
	}
	return !m.rules.HasNegations()
}

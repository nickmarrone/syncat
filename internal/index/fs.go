package index

import "io/fs"

// FS is the scanner's narrow read-side filesystem interface (SPEC.md §12:
// mobile filesystem sandboxes differ, so file I/O goes through an
// fs.FS-ish read side rather than calling os.* directly). It is exactly
// the standard library's fs.FS: Open is all the scanner strictly needs
// (streaming reads for hashing, via the returned fs.File), and fs.WalkDir
// — which the scanner uses to walk a share tree — optionally type-asserts
// its argument to fs.ReadDirFS for efficient directory listing, falling
// back to Open+ReadDir otherwise. os.DirFS(shareRoot) satisfies both, so
// production needs no adapter; tests can substitute fstest.MapFS or any
// other fs.FS.
//
// A corresponding write-side interface (SPEC.md §12's "small write
// interface") is deliberately not defined in this phase: nothing here
// writes into a share tree. Downloading, atomic rename-into-place, and
// trash relocation are Phase 5/6 concerns (SPEC.md §5 "Applying remote
// changes", §7 "Trash can") — that's where a write interface belongs, and
// defining one now, with no caller, would be guessing at its shape.
type FS = fs.FS

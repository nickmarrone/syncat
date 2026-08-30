// Package index maintains the local SQLite file index, the filesystem
// scanner, the fsnotify watcher, and ignore matching (SPEC.md §5).
//
// This package is one of the gomobile-safe leaves called out in SPEC.md
// §12: it imports no UI/CLI/HTTP packages, builds with CGO_ENABLED=0 (the
// SQLite driver is modernc.org/sqlite, a pure-Go implementation), and the
// scanner reads share contents through the narrow [FS] interface rather
// than calling os.* directly, so a future mobile port can supply its own
// sandboxed filesystem.
//
// Phase 4 (this phase) delivers durable storage ([Store]), change
// detection ([Scanner]), ignore matching ([Matcher]), and a debounced
// fsnotify [Watcher]. It deliberately does NOT implement version-vector
// dominance/merge algebra, the reconciler, transfers, or trash — those
// belong to Phases 5 and 6. Where this package needs to touch a per-file
// version vector, it treats it as an opaque protocol.VersionVector and
// leaves it unchanged; bumping counters is Phase 5's job.
package index

// Package version records this build's identity: the release version baked
// into the source, plus whatever the Go toolchain stamped in about the
// commit it was built from.
package version

import (
	"runtime/debug"
	"strings"
	"sync"
)

// Version is this build's release version, and the one place the number
// lives. Everything that reports a version — `syncat version`, GET
// /api/status's "version" field, the web UI's footer — reads it from here.
//
// It is deliberately not a protocol or format version. syncat already has
// three of those, each independent of this one and of each other:
// `proto_version` in the handshake (internal/protocol), the `sc1` token
// prefix (SPEC.md §2), and the index's `PRAGMA user_version`
// (internal/index). Bumping this number says nothing about any of them.
const Version = "0.4.0"

// String returns [Version] with the commit it was built from appended as
// semver build metadata, with a `.dirty` suffix when the working tree had
// uncommitted changes.
//
// The commit comes from the Go toolchain, which stamps it into the binary
// automatically when building from a git checkout, so there is nothing to
// pass at build time and no linker flags to keep in sync. It is genuinely
// absent from some builds, though — `go run`, `go test`, and
// `-buildvcs=false` all produce binaries with no VCS stamp — so a bare
// release version is a normal result here, not a failure to report.
var String = sync.OnceValue(func() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return Version
	}

	var revision, modified string
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			revision = s.Value
		case "vcs.modified":
			modified = s.Value
		}
	}
	if revision == "" {
		return Version
	}

	// Seven characters to match what `git log --oneline` prints, so a
	// reported version can be pasted straight into a git command.
	var b strings.Builder
	b.WriteString(Version)
	b.WriteString("+")
	b.WriteString(revision[:min(len(revision), 7)])
	if modified == "true" {
		b.WriteString(".dirty")
	}
	return b.String()
})

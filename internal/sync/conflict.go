package sync

import (
	"path"
	"regexp"
	"strings"
	"time"
)

// conflictMarkerRe matches an existing ".sync-conflict-YYYYMMDD-HHMMSS-<hex>"
// marker anywhere it appears in a filename, so ConflictRelPath can strip a
// prior marker before appending a fresh one. See ConflictRelPath's doc
// comment for why this bounds filename growth across repeated conflicts.
var conflictMarkerRe = regexp.MustCompile(`\.sync-conflict-\d{8}-\d{6}-[0-9a-f]+`)

// conflictTimeLayout is YYYYMMDD-HHMMSS, per SPEC.md §5.
const conflictTimeLayout = "20060102-150405"

// ConflictRelPath returns the relpath a conflict "loser" file is written to,
// beside the winner at relpath, per SPEC.md §5:
//
//	<name>.sync-conflict-YYYYMMDD-HHMMSS-<node-short-id><ext>
//
// The timestamp is always taken in UTC (now.UTC()), so the same instant
// produces the same filename regardless of which node's local timezone
// generated it, and so filenames sort consistently across nodes.
//
// Extension handling: <ext> is the file's *last* dot-suffix only, not every
// dot-separated segment. "notes.tar.gz" therefore becomes
// "notes.tar.sync-conflict-<ts>-<id>.gz", keeping the final ".gz" intact.
// This is deliberate: most OS file-type detection, double-click handlers,
// and archive tools key off the last extension, and burying it as
// "notes.sync-conflict-<ts>-<id>.tar.gz" would hide the archive's real type
// behind an arbitrary node id inserted in the middle of the name. It also
// matches the prior art this naming scheme is modeled on (Syncthing's
// ".sync-conflict-" files apply the same last-extension-only rule).
//
// Dotfiles are handled specially so this doesn't misfire on them: a name
// whose only dot is a single leading one (".bashrc") is treated as having
// no extension at all, producing ".bashrc.sync-conflict-<ts>-<id>" — naively
// using the last-dot rule on the whole string would treat ".bashrc" itself
// as the "extension" and leave an empty base. ".config.json" (a dotfile
// *with* a real extension) still splits into ".config" + ".json" as
// expected. Extensionless files (e.g. "README") simply get no trailing
// <ext>.
//
// If relpath already carries a ".sync-conflict-...-<id>" marker — a
// conflict copy that itself conflicted again — that marker is stripped
// before the new one is appended, so repeated conflicts on the same file
// never grow the filename without bound: the result always carries at most
// one marker, always the most recent.
func ConflictRelPath(relpath, nodeShortID string, now time.Time) string {
	dir, base := path.Split(relpath)
	base = conflictMarkerRe.ReplaceAllString(base, "")

	name, ext := splitExt(base)
	stamp := now.UTC().Format(conflictTimeLayout)
	return dir + name + ".sync-conflict-" + stamp + "-" + nodeShortID + ext
}

// splitExt splits a filename into (name, ext) using the usual
// last-dot-in-the-name rule, except a filename whose only dot is a single
// leading one (a dotfile with no further dot, e.g. ".bashrc") is treated as
// having no extension: splitExt returns (".bashrc", "") rather than
// treating the whole name as its own extension (which is what
// path/filepath.Ext would do).
func splitExt(base string) (name, ext string) {
	i := strings.LastIndexByte(base, '.')
	if i <= 0 {
		return base, ""
	}
	return base[:i], base[i:]
}

// Package sync implements SPEC.md §5 and §7.
//
// reconcile.go is the pure decision layer: version-vector algebra, the
// per-relpath [Reconcile] pass, and the [Action] values it produces. It
// does no I/O. session.go executes those decisions over one peer
// connection (lifecycle, read loop, index exchange), transfer.go moves the
// bytes (pulling files from the peer and serving them), apply.go writes
// the results to disk, path.go validates every peer-supplied relpath
// before it ever becomes a filesystem path, and trash.go implements the
// trash can and its janitor.
package sync

import (
	"bytes"
	"path"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/nickmarrone/syncat/internal/config"
	"github.com/nickmarrone/syncat/internal/protocol"
)

// The version-vector algebra SPEC.md §5 rests on: Dominates, Concurrent,
// Equal, Merge, and Bump over protocol.VersionVector (map[string]uint64,
// keyed by node-short-id).
//
// Missing-key convention: a key absent from a VersionVector is an implicit
// zero counter, exactly as Go's map indexing already treats it (v[k] on a
// missing k yields the zero value with no separate "ok" check needed). Every
// function below relies on this directly rather than special-casing missing
// keys, so a nil VersionVector behaves identically to an empty one, and a
// vector that happens to store an explicit 0 for some key is indistinguishable
// from that key being absent. Bump never removes keys and Merge never invents
// zero entries, so in practice explicit zeros never appear — but the
// functions here are correct either way.

// --- version vectors (SPEC.md §5) --------------------------------------

// Equal reports whether a and b represent the same version vector, treating
// a missing key in either as an implicit zero.
func Equal(a, b protocol.VersionVector) bool {
	for k, av := range a {
		if b[k] != av {
			return false
		}
	}
	for k, bv := range b {
		if a[k] != bv {
			return false
		}
	}
	return true
}

// Dominates reports whether a strictly dominates b: for every node key that
// appears in either vector, a's counter is >= b's, and at least one is
// strictly greater. The empty vector is dominated by any non-empty vector
// and dominates nothing (Dominates(empty, empty) is false, matching Equal).
func Dominates(a, b protocol.VersionVector) bool {
	strict := false
	for k := range a {
		av, bv := a[k], b[k]
		if av < bv {
			return false
		}
		if av > bv {
			strict = true
		}
	}
	for k := range b {
		if _, ok := a[k]; ok {
			continue // already compared above
		}
		if a[k] < b[k] { // a[k] is the implicit zero here
			return false
		}
	}
	return strict
}

// Concurrent reports whether a and b are neither equal nor ordered by
// Dominates in either direction. For any pair of vectors, exactly one of
// Equal(a,b), Dominates(a,b), Dominates(b,a), Concurrent(a,b) holds — see
// reconcile_test.go's property check.
func Concurrent(a, b protocol.VersionVector) bool {
	return !Equal(a, b) && !Dominates(a, b) && !Dominates(b, a)
}

// Merge returns the element-wise maximum of a and b over the union of their
// keys. Merge is commutative and associative, and Merge(a, b) always
// dominates-or-equals both a and b. Merge never mutates a or b.
func Merge(a, b protocol.VersionVector) protocol.VersionVector {
	out := make(protocol.VersionVector, len(a)+len(b))
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		if v > out[k] {
			out[k] = v
		}
	}
	return out
}

// Bump returns a copy of v with nodeID's counter incremented by one,
// recording a local modification by nodeID (SPEC.md §5). v itself is never
// mutated, since callers may still hold a reference to it (e.g. as a row's
// stored version) that must not change out from under them.
func Bump(v protocol.VersionVector, nodeID string) protocol.VersionVector {
	out := make(protocol.VersionVector, len(v)+1)
	for k, val := range v {
		out[k] = val
	}
	out[nodeID] = out[nodeID] + 1
	return out
}

// --- the decisions Reconcile emits -------------------------------------

// ActionKind identifies what a reconciled Action instructs the caller
// (session.go/apply.go) to do.
type ActionKind int

const (
	// ActionNone means no local filesystem change is needed for this
	// relpath. This is a real, deliberate outcome (equal versions, local
	// already dominates and the peer will pull from us, or the share
	// direction blocks inbound changes entirely) — Reconcile returns one
	// Action per relpath it considered, including these no-ops, so callers
	// and tests never have to distinguish "considered and skipped" from
	// "never looked at".
	ActionNone ActionKind = iota
	// ActionPull means fetch Resolved's content from the peer and write it
	// to RelPath, then store Resolved.Version as RelPath's version.
	ActionPull
	// ActionDelete means RelPath is deleted (or already absent) locally;
	// store Resolved.Version (a tombstone: Resolved.Deleted is true) as its
	// version. Idempotent: applying it when the file is already absent
	// locally is just a version-bookkeeping update.
	ActionDelete
	// ActionResurrect means a delete-vs-modify conflict resolved in favor
	// of the modification (SPEC.md §5: "modify wins"). Resolved is the
	// surviving, non-deleted content; Source says whether it must be
	// fetched from the peer or is already the current local file.
	ActionResurrect
	// ActionConflictCopy means a genuine concurrent content conflict:
	// Resolved (the winner, chosen by SPEC.md §5's mtime/sha256 rule) is
	// written to RelPath, and the loser is written beside it, unmodified,
	// at ConflictRelPath. Both are real files afterward; the conflict copy
	// syncs onward like any other new file.
	ActionConflictCopy
	// ActionLocallyModified flags a local change that a receive-only
	// subscription must not propagate (SPEC.md §1, §5). Resolved carries
	// what the peer currently holds, for a future revert to apply.
	// applyLocallyModified in apply.go does the reverting.
	ActionLocallyModified
)

func (k ActionKind) String() string {
	switch k {
	case ActionNone:
		return "None"
	case ActionPull:
		return "Pull"
	case ActionDelete:
		return "Delete"
	case ActionResurrect:
		return "Resurrect"
	case ActionConflictCopy:
		return "ConflictCopy"
	case ActionLocallyModified:
		return "LocallyModified"
	default:
		return "Unknown"
	}
}

// ActionSource says which side's bytes an Action's Resolved (or
// ConflictInfo) content must come from.
type ActionSource int

const (
	// SourceNone means no content transfer is needed: either there is no
	// content at all (a delete/tombstone), or the version vector alone was
	// reconciled without any material change.
	SourceNone ActionSource = iota
	// SourceLocal means the content is already present in the local
	// filesystem/index; no fetch from the peer is required.
	SourceLocal
	// SourceRemote means the content must be fetched from the peer (a
	// FileRequest/FileChunk exchange in transfer.go).
	SourceRemote
)

func (s ActionSource) String() string {
	switch s {
	case SourceNone:
		return "None"
	case SourceLocal:
		return "Local"
	case SourceRemote:
		return "Remote"
	default:
		return "Unknown"
	}
}

// Action is one reconciled decision for a single relpath: everything
// session.go/apply.go need to execute it without re-deriving any of the
// comparison logic above.
type Action struct {
	Kind ActionKind

	// RelPath is the file this action concerns — the "winner" path, i.e.
	// the one that keeps the original name. Always set.
	RelPath string

	// Resolved is the state RelPath must end up holding once this action
	// is applied: for ActionPull/ActionDelete/ActionResurrect it is the
	// surviving side's FileInfo (Deleted=true for ActionDelete); for
	// ActionConflictCopy it is the winning side's FileInfo; for
	// ActionLocallyModified it is what the peer currently holds (for a
	// future revert). Resolved.Version is already the correct version to
	// persist — for ActionPull/ActionDelete this is simply the peer's
	// version as received; for ActionResurrect/ActionConflictCopy it is
	// Merge(local, remote) plus a local Bump, per SPEC.md §5. Zero value
	// (unused) for ActionNone.
	Resolved protocol.FileInfo

	// Source says whether Resolved's bytes must be fetched from the peer
	// (SourceRemote), are already the current local file (SourceLocal), or
	// aren't needed at all (SourceNone: a delete, or a pure version-vector
	// reconciliation with no content change).
	Source ActionSource

	// SourceVersion is the version to actually request from the peer when
	// Source == SourceRemote: the version the peer's own index advertised
	// for this content, before any local merge/bump.
	//
	// It matters because Resolved.Version is not always that same value:
	// for ActionPull/ActionDelete (a plain dominance, no conflict)
	// Resolved.Version *is* the peer's version as received, so
	// SourceVersion is identical to it. But for
	// ActionResurrect/ActionConflictCopy, Resolved.Version has been
	// rewritten to Merge(local, remote) plus a local Bump (per SPEC.md §5)
	// — the version we're about to persist locally, reflecting that *we*
	// have now resolved the conflict. The peer never has that value; they
	// only ever have what they last told us about. A FileRequest must
	// carry SourceVersion, or the peer's Equal(row.Version, req.Version)
	// freshness check (handleFileRequest in transfer.go) can never match,
	// and the pull always fails with "version_changed" on its first
	// attempt — see apply.go's pullAndInstall, the only consumer of this
	// field. Zero/unused when Source != SourceRemote.
	SourceVersion protocol.VersionVector

	// The following three fields are set only when Kind == ActionConflictCopy:
	// the losing side, preserved unmodified beside the winner rather than
	// being discarded.

	// ConflictRelPath is the loser's new relpath, computed by
	// ConflictRelPath (SPEC.md §5 conflict-filename format).
	ConflictRelPath string
	// ConflictInfo is the losing side's FileInfo, verbatim (its own
	// version, size, sha256, mtime — not merged or bumped: the conflict
	// copy is a brand-new file as far as the index is concerned, and syncs
	// onward from there like any other file).
	ConflictInfo protocol.FileInfo
	// ConflictSource says whether ConflictInfo's bytes must be fetched from
	// the peer or are already the current local file.
	ConflictSource ActionSource

	// LocallyModified mirrors Kind == ActionLocallyModified as a plain
	// bool, so callers that only care about "does this need a
	// locally-modified warning" don't need to switch on Kind.
	LocallyModified bool

	// Reason is a short, human-readable explanation of why this action was
	// chosen — useful for logs, the UI, and test assertions.
	Reason string
}

// Direction captures how a share/subscription pairing constrains which way
// changes may flow for one reconciliation, per SPEC.md §5 "Direction
// rules". Compute it with DirectionFor rather than constructing it by hand.
type Direction struct {
	// InboundBlocked is true when incoming changes for this share must be
	// ignored outright, regardless of version-vector comparison: an
	// offerer's read-only share never accepts remote-dominant updates,
	// tombstones, or conflicts from a peer (SPEC.md §5: "ignores incoming
	// IndexUpdate for that share").
	InboundBlocked bool
	// OutboundBlocked is true when we must never propose our own changes
	// for this share: a receive-only subscription. Local changes that
	// diverge from the peer are still detected, but instead of the normal
	// "skip, the peer will pull from us" or "keep both as a conflict copy"
	// handling, they are reported via ActionLocallyModified. Reverting them
	// via trash happens in apply.go's applyLocallyModified (SPEC.md §1).
	OutboundBlocked bool
}

// DirectionFor derives a share's inbound/outbound constraints from its
// offerer permission and our subscription mode (SPEC.md §5). Pass
// permission == "" when we are not the offerer in this pairing (we are
// purely a subscriber), and subscriptionMode == "" when we are not a
// subscriber (we are purely the offerer). In practice exactly one of the
// two is non-empty for any single reconciliation, since a share is offered
// by one node and subscribed to by others (SPEC.md §5 "Fan-out").
func DirectionFor(permission, subscriptionMode string) Direction {
	return Direction{
		InboundBlocked:  permission == config.PermissionReadOnly,
		OutboundBlocked: subscriptionMode == config.ModeReceiveOnly,
	}
}

// --- conflict-copy filenames (SPEC.md §5) ------------------------------

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

// --- the reconciler ----------------------------------------------------

// Clock returns the current time. Production callers pass time.Now; tests
// inject a fixed or stepped function so conflict-filename timestamps (and
// any other time-dependent behavior added later) are deterministic.
type Clock func() time.Time

// Reconcile compares this node's index rows for a share (local) against a
// peer's rows for the same share (remote), and returns one Action per
// relpath that appears in either side, ordered deterministically by
// relpath. It performs no I/O: applying the returned actions to the
// filesystem and to the index is entirely the caller's job (apply.go).
//
// nodeID is this node's ShortID (config.IdentityKey.ShortID), used to bump
// the local counter when a conflict is resolved. dir reflects the share's
// permission (as offerer) or subscription mode (as subscriber) — build it
// with DirectionFor. clock supplies the timestamp for any conflict
// filenames generated; if nil, time.Now is used.
//
// Every remote FileInfo is validated with ValidateRelPath before use; an
// entry with an invalid relpath is dropped (reported as ActionNone with an
// explanatory Reason) rather than acted on, so a hostile or buggy peer's
// IndexUpdate can never smuggle a traversal attempt into an Action for a
// caller to (mis)trust. Local rows come from this node's own index and are
// not re-validated here.
//
// ignore, when non-nil, reports whether a share-relative path is excluded
// by the share's ignore rules (SPEC.md §5). Ignored paths are decided
// before any version algebra runs and always yield ActionNone, which is
// what makes ignoring bidirectional: we neither pull the peer's copy nor
// delete our own on their tombstone. It is a required parameter rather
// than a second entry point so that no caller can quietly skip it.
func Reconcile(local, remote []protocol.FileInfo, nodeID string, dir Direction, clock Clock, ignore func(relpath string, isDir bool) bool) []Action {
	if clock == nil {
		clock = time.Now
	}

	localByPath := indexByPath(local)
	remoteByPath := indexByPath(remote)

	paths := make(map[string]struct{}, len(localByPath)+len(remoteByPath))
	for p := range localByPath {
		paths[p] = struct{}{}
	}
	for p := range remoteByPath {
		paths[p] = struct{}{}
	}
	sorted := make([]string, 0, len(paths))
	for p := range paths {
		sorted = append(sorted, p)
	}
	sort.Strings(sorted)

	actions := make([]Action, 0, len(sorted))
	for _, p := range sorted {
		l, hasLocal := localByPath[p]
		r, hasRemote := remoteByPath[p]
		// A malformed remote path is reported as such by reconcileOne,
		// whose ValidateRelPath check runs first. Never mask that signal
		// with "ignored" just because the path also matched a rule: a
		// traversal attempt should be visible as a traversal attempt.
		malformed := hasRemote && ValidateRelPath(r.RelPath) != nil
		if !malformed && ignore != nil && ignore(p, isDirEntry(l, hasLocal, r, hasRemote)) {
			// Decided here rather than inside reconcileOne so that a
			// single branch covers every outcome the algebra could have
			// reached: no pull of a remote-only file (which would flap,
			// since the next scan re-ignores it), no delete of our copy
			// on the peer's tombstone, no conflict copy, and no
			// locally-modified warning.
			actions = append(actions, Action{Kind: ActionNone, RelPath: p, Reason: "ignored: .syncatignore"})
			continue
		}
		actions = append(actions, reconcileOne(p, l, hasLocal, r, hasRemote, nodeID, dir, clock))
	}
	return actions
}

// isDirEntry reports whether a reconciled path names a directory,
// preferring our own view of it and falling back to the peer's. Ignore
// rules need it because a directory-only pattern ("build/") must not match
// a file of the same name.
func isDirEntry(l protocol.FileInfo, hasLocal bool, r protocol.FileInfo, hasRemote bool) bool {
	if hasLocal {
		return l.Type == protocol.FileTypeDir
	}
	if hasRemote {
		return r.Type == protocol.FileTypeDir
	}
	return false
}

// indexByPath keys files by RelPath. If the input slice has duplicate
// relpaths (it shouldn't — an index has at most one row per relpath), the
// last occurrence silently wins.
func indexByPath(files []protocol.FileInfo) map[string]protocol.FileInfo {
	m := make(map[string]protocol.FileInfo, len(files))
	for _, f := range files {
		m[f.RelPath] = f
	}
	return m
}

func reconcileOne(relpath string, l protocol.FileInfo, hasLocal bool, r protocol.FileInfo, hasRemote bool, nodeID string, dir Direction, clock Clock) Action {
	if hasRemote {
		if err := ValidateRelPath(r.RelPath); err != nil {
			return Action{Kind: ActionNone, RelPath: relpath, Reason: "rejected: invalid remote relpath: " + err.Error()}
		}
	}

	if dir.InboundBlocked {
		// Offerer of a read-only share: incoming changes are ignored
		// outright, regardless of what the vectors say (SPEC.md §5).
		return Action{Kind: ActionNone, RelPath: relpath, Reason: "inbound blocked: offering read-only share"}
	}

	if !hasRemote {
		// Local-only: the peer has never told us about this path. Treat it
		// like local strictly dominating an (implicit) empty remote vector:
		// nothing to pull, the peer will pull from us — unless we must
		// never propose outbound changes, in which case this is exactly
		// the kind of local content a receive-only subscription must flag.
		if dir.OutboundBlocked {
			return Action{Kind: ActionLocallyModified, RelPath: relpath, LocallyModified: true,
				Reason: "local-only file under receive-only subscription"}
		}
		return Action{Kind: ActionNone, RelPath: relpath, Reason: "local-only, peer will pull from us"}
	}

	if !hasLocal {
		// Remote-only: new information we don't have yet, so adopt it.
		if r.Deleted {
			return Action{Kind: ActionDelete, RelPath: relpath, Resolved: r, Source: SourceNone,
				Reason: "remote-only tombstone"}
		}
		return Action{Kind: ActionPull, RelPath: relpath, Resolved: r, Source: SourceRemote,
			SourceVersion: r.Version, Reason: "remote-only file"}
	}

	switch {
	case Equal(l.Version, r.Version):
		return Action{Kind: ActionNone, RelPath: relpath, Reason: "versions equal"}

	case Dominates(r.Version, l.Version):
		if r.Deleted {
			return Action{Kind: ActionDelete, RelPath: relpath, Resolved: r, Source: SourceNone,
				Reason: "remote dominates: delete"}
		}
		return Action{Kind: ActionPull, RelPath: relpath, Resolved: r, Source: SourceRemote,
			SourceVersion: r.Version, Reason: "remote dominates"}

	case Dominates(l.Version, r.Version):
		if dir.OutboundBlocked {
			return Action{Kind: ActionLocallyModified, RelPath: relpath, LocallyModified: true,
				Reason: "local dominates under receive-only subscription"}
		}
		return Action{Kind: ActionNone, RelPath: relpath, Reason: "local dominates: peer will pull from us"}

	default:
		return reconcileConcurrent(relpath, l, r, nodeID, dir, clock)
	}
}

// reconcileConcurrent handles the Concurrent(l.Version, r.Version) case:
// delete-vs-modify, tombstone-vs-tombstone, receive-only local changes, and
// genuine content conflicts.
func reconcileConcurrent(relpath string, l, r protocol.FileInfo, nodeID string, dir Direction, clock Clock) Action {
	// Under a receive-only subscription, a concurrent divergence caused by
	// *our own* live content (l.Deleted == false) is never resolved via a
	// conflict copy — SPEC.md §1: local modifications on a receive-only
	// copy are "detected, logged as warnings... and overwritten... after a
	// trash copy is taken" the next time the offerer changes the file. The
	// actual revert-via-trash happens in apply.go; here we only raise the
	// flag, carrying what the peer currently holds for it to apply. (If
	// l.Deleted is true instead, the peer holding live content is a
	// legitimate incoming resurrect, not a local misdeed — that falls
	// through to the normal handling below.)
	if dir.OutboundBlocked && !l.Deleted {
		return Action{Kind: ActionLocallyModified, RelPath: relpath, Resolved: r, Source: SourceRemote,
			SourceVersion: r.Version, LocallyModified: true, Reason: "concurrent local change under receive-only subscription"}
	}

	merged := Bump(Merge(l.Version, r.Version), nodeID)

	if l.Deleted != r.Deleted {
		// Delete-vs-modify: modify wins, unconditionally (SPEC.md §5),
		// regardless of what the mtime/sha256 tie-break below would say.
		winner, source := r, SourceRemote
		if !l.Deleted {
			winner, source = l, SourceLocal
		}
		// Capture the winner's real, as-advertised version before
		// overwriting Resolved.Version with the merged+bumped value below
		// — SourceVersion (used only when source == SourceRemote) must stay
		// the version the peer actually has, or the FileRequest this
		// resurrect issues can never match on the peer's side. See
		// SourceVersion's doc comment above.
		sourceVersion := winner.Version
		winner.Version = merged
		winner.RelPath = relpath
		return Action{Kind: ActionResurrect, RelPath: relpath, Resolved: winner, Source: source,
			SourceVersion: sourceVersion, Reason: "delete-vs-modify: modify wins"}
	}

	if l.Deleted { // && r.Deleted, by the check above
		// Both sides already agree the file is gone; only the version
		// vectors disagree (e.g. two nodes each deleted it independently
		// before ever syncing). Nothing to move — just reconcile the
		// vector.
		resolved := r
		resolved.Version = merged
		resolved.RelPath = relpath
		return Action{Kind: ActionDelete, RelPath: relpath, Resolved: resolved, Source: SourceNone,
			Reason: "tombstone vs tombstone, concurrent: merge versions"}
	}

	if bytes.Equal(l.SHA256, r.SHA256) {
		return reconcileIdenticalContent(relpath, l, r)
	}

	// Genuine content conflict: both sides have live, differing content.
	// Winner keeps relpath; loser is written beside it as a new file.
	winner, loser := l, r
	winnerSource, loserSource := SourceLocal, SourceRemote
	if conflictWinnerIsRemote(l, r) {
		winner, loser = r, l
		winnerSource, loserSource = SourceRemote, SourceLocal
	}
	// Same reasoning as reconcileConcurrent's delete-vs-modify branch above:
	// capture the winner's real version before it's overwritten with the
	// merged+bumped value, so a SourceRemote winner's FileRequest still asks
	// for what the peer actually has.
	winnerSourceVersion := winner.Version
	winner.Version = merged
	winner.RelPath = relpath

	conflictPath := ConflictRelPath(relpath, nodeID, clock())
	loser.RelPath = conflictPath

	return Action{
		Kind:            ActionConflictCopy,
		RelPath:         relpath,
		Resolved:        winner,
		Source:          winnerSource,
		SourceVersion:   winnerSourceVersion,
		ConflictRelPath: conflictPath,
		ConflictInfo:    loser,
		ConflictSource:  loserSource,
		Reason:          "concurrent modification",
	}
}

// reconcileIdenticalContent handles the concurrent-but-same-bytes case
// (bytes.Equal(l.SHA256, r.SHA256)). This concurrency isn't a real
// divergence to preserve, only version-vector bookkeeping that hasn't
// caught up yet — overwhelmingly because both peers just independently
// resolved this very conflict on their own (each bumping its own counter
// per SPEC.md §5's "element-wise max + local bump"), which leaves their two
// results *mutually concurrent with each other* even though the bytes they
// each landed on are identical (see conflictWinnerIsRemote's doc comment
// on the same equal-content case for the tie-break itself).
//
// Two things make it essential to short-circuit here rather than fall
// through to reconcileConcurrent's generic conflict-copy handling:
//
//  1. Correctness: a conflict copy of content that's identical to the
//     winner is a no-op at best. At worst it's actively destructive —
//     ConflictRelPath is a deterministic function of (relpath, nodeID,
//     second), so a second pass through this branch within the same
//     second computes the *same* path as this node's own earlier,
//     genuinely-different conflict copy and silently overwrites it,
//     destroying the very losing content that copy existed to
//     preserve.
//  2. Liveness: without this, each side keeps re-bumping its own
//     counter every time it reconciles the other's already-resolved
//     result, which (unlike the merged+bump growing past the *other*
//     side's last-known value and settling into a normal dominates
//     relationship) has no guarantee of ever converging — under
//     symmetric timing both sides can keep leapfrogging each other's
//     bump indefinitely. Folding the vectors with a plain Merge (no
//     extra local Bump) instead is deterministic and commutative: both
//     sides compute the exact same union regardless of processing
//     order, so they land on one *equal* vector after this single
//     round and the whole thing stops for good, rather than each side
//     manufacturing a fresh "local modification" out of applying no
//     actual local modification at all.
func reconcileIdenticalContent(relpath string, l, r protocol.FileInfo) Action {
	resolved := l
	resolved.Version = Merge(l.Version, r.Version)
	resolved.RelPath = relpath
	return Action{Kind: ActionPull, RelPath: relpath, Resolved: resolved, Source: SourceNone,
		Reason: "concurrent but content identical: reconcile version only, no conflict copy"}
}

// conflictWinnerIsRemote applies SPEC.md §5's conflict tie-break: the
// larger mtime_ns wins; if mtimes are equal, the larger sha256 wins,
// compared with bytes.Compare over the raw digest bytes (an unsigned,
// left-to-right byte comparison — there is no numeric interpretation of the
// digest implied, it's simply a total order that both sides can compute
// identically). If both mtime and sha256 are exactly equal — the content is
// byte-identical, so the vectors' concurrency reflects two independent
// no-op edits rather than any real divergence — there is nothing left to
// break the tie on; we deterministically prefer local. This never arises
// for genuinely different content, since sha256 collisions aren't a
// practical concern.
func conflictWinnerIsRemote(l, r protocol.FileInfo) bool {
	if r.MTimeNS != l.MTimeNS {
		return r.MTimeNS > l.MTimeNS
	}
	return bytes.Compare(r.SHA256, l.SHA256) > 0
}

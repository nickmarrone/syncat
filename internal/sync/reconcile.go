package sync

import (
	"bytes"
	"sort"
	"time"

	"github.com/nickmarrone/syncat/internal/config"
	"github.com/nickmarrone/syncat/internal/protocol"
)

// Clock returns the current time. Production callers pass time.Now; tests
// inject a fixed or stepped function so conflict-filename timestamps (and
// any other time-dependent behavior added later) are deterministic.
type Clock func() time.Time

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

// Reconcile compares this node's index rows for a share (local) against a
// peer's rows for the same share (remote), and returns one Action per
// relpath that appears in either side, ordered deterministically by
// relpath. It performs no I/O: applying the returned actions to the
// filesystem and to the index is entirely the caller's (5b's) job.
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
func Reconcile(local, remote []protocol.FileInfo, nodeID string, dir Direction, clock Clock) []Action {
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
		actions = append(actions, reconcileOne(p, l, hasLocal, r, hasRemote, nodeID, dir, clock))
	}
	return actions
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
	// actual revert-via-trash is Phase 6; here we only raise the flag,
	// carrying what the peer currently holds so a future revert has
	// something to apply. (If l.Deleted is true instead, the peer holding
	// live content is a legitimate incoming resurrect, not a local
	// misdeed — that falls through to the normal handling below.)
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
		// SourceVersion's doc comment in action.go.
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

	// Same bytes on both sides: this concurrency isn't a real divergence to
	// preserve, only version-vector bookkeeping that hasn't caught up yet —
	// overwhelmingly because both peers just independently resolved this
	// very conflict on their own (each bumping its own counter per
	// SPEC.md §5's "element-wise max + local bump"), which leaves their two
	// results *mutually concurrent with each other* even though the bytes
	// they each landed on are identical (see conflictWinnerIsRemote's doc
	// comment on the same equal-content case for the tie-break itself).
	//
	// Two things make it essential to short-circuit here rather than fall
	// through to the generic conflict-copy handling below:
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
	if bytes.Equal(l.SHA256, r.SHA256) {
		resolved := l
		resolved.Version = Merge(l.Version, r.Version)
		resolved.RelPath = relpath
		return Action{Kind: ActionPull, RelPath: relpath, Resolved: resolved, Source: SourceNone,
			Reason: "concurrent but content identical: reconcile version only, no conflict copy"}
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

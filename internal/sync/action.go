package sync

import (
	"time"

	"github.com/nickmarrone/syncat/internal/protocol"
)

// LocallyModifiedWarning records one receive-only "locally modified" event
// (SPEC.md §1, §5): a subscriber's local edit that diverged from the
// offerer under a receive-only subscription. Reverted reports whether the
// offerer already had content to revert to (a trash copy was taken and the
// offerer's version installed) or the divergence was only flagged because
// there was nothing yet to revert to (a local-only file the offerer
// doesn't have — see applyLocallyModified in apply.go). Session keeps a
// log of these (Session.LocallyModifiedWarnings) for Phase 8's API/UI to
// surface.
type LocallyModifiedWarning struct {
	ShareID  string
	RelPath  string
	At       time.Time
	Reverted bool
	Reason   string
}

// ActionKind identifies what a reconciled Action instructs the caller (5b's
// transfer/apply layer) to do.
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
	// Reverting via trash is Phase 6; this phase only raises the flag.
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
	// FileRequest/FileChunk exchange in 5b).
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

// Action is one reconciled decision for a single relpath: everything 5b
// needs to execute it without re-deriving any of the comparison logic in
// this package.
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
	// via trash is Phase 6 (SPEC.md §1); this phase only raises the flag.
	OutboundBlocked bool
}

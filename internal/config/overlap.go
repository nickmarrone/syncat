package config

import (
	"fmt"
	"path/filepath"
	"strings"
)

// A directory that this node syncs down from a peer must not also be offered
// back out as one of this node's own shares.
//
// SPEC.md §5 makes fan-out hub-and-spoke: "a share offered to multiple peers
// propagates through the offerer (hub). Peers of the same share do not talk to
// each other in v1." Re-offering a subscribed directory would quietly build a
// chain A -> B -> C out of two distinct share IDs covering the same files on
// B. The index is keyed by (share_id, relpath), so B would track each file
// twice under independent version vectors, and a change arriving on one share
// would look like a fresh local edit to the other — the two shares would
// bounce edits between themselves indefinitely.
//
// The rule is symmetric and covers nesting in both directions, because sharing
// a parent of a subscribed directory re-exports its contents just as surely as
// sharing the directory itself.

// pathContains reports whether child is parent or lies beneath it. The
// comparison is lexical after cleaning, so it is not fooled by "/a/code"
// against "/a/codebase" the way a string-prefix test would be.
//
// It deliberately does no filesystem I/O: Validate runs on every load and save,
// including for paths that do not exist yet. That means a symlink pointing from
// a share into a subscribed tree is not caught here; resolving symlinks belongs
// with the scanner, which already has to walk real directories.
func pathContains(parent, child string) bool {
	rel, err := filepath.Rel(filepath.Clean(parent), filepath.Clean(child))
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}

// pathsOverlap reports whether two directory trees intersect: either path is
// the other, or one contains the other.
func pathsOverlap(a, b string) bool {
	return pathContains(a, b) || pathContains(b, a)
}

// CheckSharePath reports whether path may be offered as a local share, given
// the subscriptions already in c. skipShareID, when non-empty, excludes one
// existing share from consideration so a share can be edited in place.
func (c *Config) CheckSharePath(path string) error {
	if !filepath.IsAbs(path) {
		return fmt.Errorf("share path %q must be absolute", path)
	}
	for _, sub := range c.Subscriptions {
		if sub.LocalPath != "" && pathsOverlap(path, sub.LocalPath) {
			return fmt.Errorf(
				"cannot share %q: it overlaps %q, which is synced down from peer %q (share %s); "+
					"a directory received from another node cannot be offered back out",
				path, sub.LocalPath, sub.Peer, sub.ShareID)
		}
	}
	return nil
}

// CheckSubscriptionPath reports whether localPath may be used as the local
// destination for a subscription, given the shares already in c. It is the
// mirror of CheckSharePath: the rule has to hold whichever side is added
// second.
func (c *Config) CheckSubscriptionPath(localPath string) error {
	if !filepath.IsAbs(localPath) {
		return fmt.Errorf("subscription local path %q must be absolute", localPath)
	}
	for _, s := range c.Shares {
		if s.Path != "" && pathsOverlap(localPath, s.Path) {
			return fmt.Errorf(
				"cannot sync into %q: it overlaps local share %q (%s), which this node offers to peers; "+
					"a directory received from another node cannot be offered back out",
				localPath, s.Path, s.Name)
		}
	}
	return nil
}

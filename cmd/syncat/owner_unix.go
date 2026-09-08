//go:build unix

package main

import (
	"os"
	"syscall"
)

// currentUID returns the effective uid of this process.
func currentUID() int { return os.Geteuid() }

// fileOwner returns the uid owning fi. The bool reports whether the
// platform gave us an owner at all; see owner_other.go for where it
// doesn't.
func fileOwner(fi os.FileInfo) (int, bool) {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}
	return int(st.Uid), true
}

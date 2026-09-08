//go:build !unix

package main

import "os"

// File ownership is a uid comparison this package only makes on Unix,
// where syncat's service story (a dedicated user, mode 0700 state dirs)
// lives. Elsewhere currentUID never matches, fileOwner never answers, and
// the reset's ownership check stands down rather than guessing.

func currentUID() int { return -1 }

func fileOwner(os.FileInfo) (int, bool) { return 0, false }

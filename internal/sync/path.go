package sync

import (
	"fmt"
	"path/filepath"
	"strings"
)

// This file is the security boundary for every relpath a peer sends us over
// the wire (SPEC.md §13.9): FileInfo.RelPath, FileRequest.RelPath, and
// FileChunkHeader.RelPath. ValidateRelPath is the single function that
// decides whether such a string is safe to ever use in building a
// filesystem path. JoinSharePath is the only function in this package that
// actually builds one, and it always calls ValidateRelPath itself first —
// so a caller cannot construct a share-relative filesystem path through this
// package without going through validation, even if it forgets to call
// ValidateRelPath explicitly.
//
// Auditing call sites: JoinSharePath should be the *only* place in the
// codebase that joins a peer-supplied relpath onto a share root. To check
// that invariant, search for every read of a RelPath field (FileInfo,
// FileRequest, FileChunkHeader) that flows into a filesystem call
// (os.Open, os.Create, os.Rename, os.MkdirAll, filepath.Join, ...) and
// confirm each one is downstream of a sync.JoinSharePath call rather than
// building the path by hand — e.g. `grep -rn 'RelPath' --include=*.go .`
// cross-checked against `grep -rn 'JoinSharePath\|ValidateRelPath'`. No
// other function in internal/sync (or elsewhere) should call filepath.Join
// with a share root and a wire-sourced relpath.

// maxRelPathLen bounds the total byte length of a relpath. This is a
// robustness/hardening limit (SPEC.md §13.9), not a filesystem requirement:
// it exists so a hostile or buggy peer can't hand us an absurdly long
// string to build paths, log lines, or filenames out of. 4096 matches the
// common PATH_MAX ceiling on Unix.
const maxRelPathLen = 4096

// windowsReservedNames are the device names Windows treats as reserved
// regardless of extension (e.g. "CON.txt" is just as reserved as "CON").
// v1 targets Unix (SPEC.md §0), but relpaths travel over the wire to
// whatever node receives them, so a hostile peer must not be able to smuggle
// one of these through to a future/mobile Windows-hosted node.
var windowsReservedNames = map[string]bool{
	"CON": true, "PRN": true, "AUX": true, "NUL": true,
	"COM1": true, "COM2": true, "COM3": true, "COM4": true, "COM5": true,
	"COM6": true, "COM7": true, "COM8": true, "COM9": true,
	"LPT1": true, "LPT2": true, "LPT3": true, "LPT4": true, "LPT5": true,
	"LPT6": true, "LPT7": true, "LPT8": true, "LPT9": true,
}

// ValidateRelPath reports whether relpath is safe to treat as a
// share-relative path: forward-slash separated (SPEC.md §4's wire format,
// independent of host OS), containing no element that could escape the
// share root, no reserved or hostile names, and no control characters.
//
// Rejected:
//   - the empty string, ".", ".."
//   - absolute paths (leading "/")
//   - any path containing a ".." element, checked by splitting on "/" and
//     comparing whole elements (never a substring/Contains search, which a
//     name like "a..b" or "..hidden" would false-positive on, or which a
//     cleverly-encoded traversal could false-negative past)
//   - any empty element (e.g. "a//b", a leading or trailing "/")
//   - backslashes and colons anywhere (Windows path separators / drive
//     letters; also blocks NTFS alternate-data-stream syntax)
//   - NUL bytes and other ASCII control characters (<0x20, 0x7F)
//   - any element that is (ignoring its own extension) a Windows reserved
//     device name: CON, PRN, AUX, NUL, COM1-9, LPT1-9
//   - any element starting with our own temp-file prefix ".syncat.tmp."
//   - paths longer than maxRelPathLen bytes
//
// A relpath that passes ValidateRelPath, joined element-by-element onto any
// absolute share root, is guaranteed to resolve to a path inside that root.
func ValidateRelPath(relpath string) error {
	if relpath == "" {
		return fmt.Errorf("sync: relpath is empty")
	}
	if len(relpath) > maxRelPathLen {
		return fmt.Errorf("sync: relpath exceeds %d bytes", maxRelPathLen)
	}
	if relpath == "." || relpath == ".." {
		return fmt.Errorf("sync: relpath %q is not a valid file path", relpath)
	}
	if strings.HasPrefix(relpath, "/") {
		return fmt.Errorf("sync: relpath %q must not be absolute", relpath)
	}
	if strings.ContainsRune(relpath, '\\') {
		return fmt.Errorf("sync: relpath %q must not contain a backslash", relpath)
	}
	if strings.ContainsRune(relpath, ':') {
		return fmt.Errorf("sync: relpath %q must not contain a colon", relpath)
	}
	for _, r := range relpath {
		if r == 0 || r < 0x20 || r == 0x7f {
			return fmt.Errorf("sync: relpath %q contains a control character", relpath)
		}
	}

	for _, elem := range strings.Split(relpath, "/") {
		switch elem {
		case "":
			return fmt.Errorf("sync: relpath %q contains an empty path element", relpath)
		case ".":
			return fmt.Errorf("sync: relpath %q contains a %q element", relpath, ".")
		case "..":
			return fmt.Errorf("sync: relpath %q contains a %q element", relpath, "..")
		}
		if strings.HasPrefix(elem, TempFilePrefix) {
			return fmt.Errorf("sync: relpath %q uses the reserved .syncat.tmp. prefix", relpath)
		}
		name := elem
		if i := strings.IndexByte(name, '.'); i >= 0 {
			name = name[:i]
		}
		if windowsReservedNames[strings.ToUpper(name)] {
			return fmt.Errorf("sync: relpath %q uses reserved name %q", relpath, elem)
		}
	}
	return nil
}

// JoinSharePath validates relpath and joins it onto shareRoot (an absolute
// local filesystem path), returning the resulting absolute path. It always
// validates relpath itself — see the package-level doc comment above for
// why callers should use this instead of validating and joining separately.
func JoinSharePath(shareRoot, relpath string) (string, error) {
	if err := ValidateRelPath(relpath); err != nil {
		return "", err
	}
	if !filepath.IsAbs(shareRoot) {
		return "", fmt.Errorf("sync: share root %q must be absolute", shareRoot)
	}

	cleanRoot := filepath.Clean(shareRoot)
	elems := strings.Split(relpath, "/")
	full := filepath.Join(append([]string{cleanRoot}, elems...)...)

	// Defense in depth: ValidateRelPath already guarantees this can't
	// happen, but confirm the joined path is still inside the root before
	// handing it back, in case that guarantee is ever weakened.
	if full != cleanRoot && !strings.HasPrefix(full, cleanRoot+string(filepath.Separator)) {
		return "", fmt.Errorf("sync: relpath %q escapes share root %q", relpath, shareRoot)
	}
	return full, nil
}

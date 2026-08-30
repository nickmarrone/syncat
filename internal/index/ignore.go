package index

import (
	"path"
)

// Matcher decides whether a share-relative path should be excluded from
// scanning, per SPEC.md §5's ignore-rules MVP subset. Full gitignore-style
// `.syncatignore` matching (also described in SPEC.md §5) is explicitly
// deferred — implementing it would pull in a gitignore-syntax library,
// which is outside this phase's dependency budget. Matcher covers only:
//
//   - Built-in always-ignored names: our own temp-file prefix
//     (.syncat.tmp.*) and common OS junk files (.DS_Store, Thumbs.db,
//     desktop.ini).
//   - config.Config.GlobalIgnores: user-supplied glob patterns.
//
// Matching semantics (document this precisely — the phase brief calls out
// that Phase 9's UI and the README need to describe it exactly):
//
//   - Every pattern is a path.Match pattern: '*' matches any sequence of
//     non-'/' characters, '?' matches any single non-'/' character, and
//     '[...]' is a character class. There is no '**' — a pattern cannot
//     cross a '/' boundary by itself (path.Match's ErrBadPattern aside,
//     unmatched pattern syntax is treated as "does not match", not an
//     error — see path.Match's own doc comment).
//   - A path is ignored if the pattern matches EITHER the entry's base
//     name (path.Base(relpath)) OR the full share-relative path
//     (forward-slash separated, no leading '/'), tried independently. A
//     pattern with no '/' in it (e.g. "*.log") therefore matches a file
//     of that name at any depth, because it always matches the base-name
//     comparison; a pattern containing '/' (e.g. "build/output") only
//     matches when compared against the full relpath.
//   - relpath is always expected in forward-slash form (as produced by
//     the scanner — see scanner.go), regardless of host OS.
type Matcher struct {
	patterns []string
}

// builtinIgnorePatterns are always excluded, regardless of config
// (SPEC.md §5). ".syncat.tmp.*" is our own in-flight-download naming
// scheme (SPEC.md §5 "Applying remote changes"); the rest are common OS
// metadata files that should never be synced.
var builtinIgnorePatterns = []string{
	".syncat.tmp.*",
	".DS_Store",
	"Thumbs.db",
	"desktop.ini",
}

// NewMatcher builds a Matcher from a share's global ignore list (typically
// config.Config.GlobalIgnores). The built-ins are always included in
// addition to globalIgnores.
func NewMatcher(globalIgnores []string) *Matcher {
	patterns := make([]string, 0, len(builtinIgnorePatterns)+len(globalIgnores))
	patterns = append(patterns, builtinIgnorePatterns...)
	patterns = append(patterns, globalIgnores...)
	return &Matcher{patterns: patterns}
}

// Match reports whether relpath (forward-slash separated, share-relative,
// no leading '/') should be excluded from scanning.
func (m *Matcher) Match(relpath string) bool {
	if m == nil {
		return false
	}
	base := path.Base(relpath)
	for _, pat := range m.patterns {
		if ok, _ := path.Match(pat, base); ok {
			return true
		}
		if ok, _ := path.Match(pat, relpath); ok {
			return true
		}
	}
	return false
}

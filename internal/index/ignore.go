package index

import (
	"bufio"
	"fmt"
	"io"
	"regexp"
	"strings"
)

// --- .syncatignore: gitignore-syntax ignore rules (SPEC.md §5) ----------

// IgnoreFileName is the file, at a share root, holding this node's
// gitignore-syntax ignore patterns. It is deliberately a single exported
// constant rather than a string literal at each use, for the same reason
// sync.TempFilePrefix is: the scanner, the built-in ignore list, and
// internal/core's loader all have to agree on one spelling.
//
// The file is NOT synced between peers — each node keeps its own. It is in
// builtinIgnorePatterns, so it never enters the index and therefore never
// reaches the wire.
const IgnoreFileName = ".syncatignore"

// ignorePattern is one compiled, non-comment line of a .syncatignore.
//
// re matches against a whole share-relative path (forward slashes, no
// leading '/'), so all of the anchoring decisions are already baked into
// it by compileGlob; negate and dirOnly are the two things matching still
// has to consult at match time.
type ignorePattern struct {
	source  string // the original line, for warnings
	negate  bool   // leading '!': re-includes an otherwise-ignored path
	dirOnly bool   // trailing '/': matches directories only
	re      *regexp.Regexp
}

// IgnoreRules is a parsed .syncatignore: an ordered list of patterns, of
// which the LAST one to match a path decides that path's fate (this is
// what makes '!' re-inclusion work, and it is gitignore's rule).
//
// The zero value matches nothing, and a nil *IgnoreRules is usable.
type IgnoreRules struct {
	patterns []ignorePattern

	// hasNegations records whether any pattern is a '!' re-inclusion. It
	// exists so the scanner can decide whether pruning an ignored
	// directory is safe — see PruneDirs and Matcher.PruneDirs.
	hasNegations bool
}

// ParseIgnore reads gitignore-syntax patterns from r.
//
// It returns rules, a slice of human-readable warnings for lines it could
// not compile, and an error only for a failure to read r at all. A line
// that cannot be compiled is skipped and warned about rather than failing
// the parse: a typo in one pattern must never take a whole share offline,
// and the safe direction for an unparseable line is "ignores nothing".
//
// Supported syntax (a subset of gitignore, minus the parts that only make
// sense with an index and a `git add -f` escape hatch):
//
//   - Blank lines are skipped; '#' starts a comment. Use '\#' for a
//     literal leading '#'.
//   - Trailing whitespace is trimmed unless escaped with a backslash.
//   - A leading '!' negates (re-includes). Use '\!' for a literal '!'.
//   - A trailing '/' matches directories only.
//   - A leading '/' anchors to the share root. So does a '/' anywhere else
//     in the pattern (gitignore's rule): "docs/x" is anchored, "x" is not
//     and therefore matches at any depth.
//   - '*' matches any run of non-'/' characters, '?' any single one,
//     '[...]' is a character class ('[!...]' negates), and '**' crosses
//     '/' boundaries.
func ParseIgnore(r io.Reader) (*IgnoreRules, []string, error) {
	g := &IgnoreRules{}
	var warnings []string

	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	lineNo := 0
	for sc.Scan() {
		lineNo++
		pat, ok, err := parseIgnoreLine(sc.Text())
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("line %d (%q): %v", lineNo, sc.Text(), err))
			continue
		}
		if !ok {
			continue // blank or comment
		}
		if pat.negate {
			g.hasNegations = true
		}
		g.patterns = append(g.patterns, pat)
	}
	if err := sc.Err(); err != nil {
		return nil, warnings, fmt.Errorf("index: read %s: %w", IgnoreFileName, err)
	}
	return g, warnings, nil
}

// parseIgnoreLine turns one raw line into a compiled pattern. ok is false
// for a line that carries no pattern at all (blank, or a comment).
func parseIgnoreLine(line string) (ignorePattern, bool, error) {
	line = strings.TrimSuffix(line, "\r")
	if strings.TrimSpace(line) == "" {
		return ignorePattern{}, false, nil
	}
	if strings.HasPrefix(line, "#") {
		return ignorePattern{}, false, nil
	}
	source := line

	line = trimUnescapedTrailingSpace(line)
	if line == "" {
		return ignorePattern{}, false, nil
	}

	var pat ignorePattern
	pat.source = source

	if strings.HasPrefix(line, "!") {
		pat.negate = true
		line = line[1:]
		if line == "" {
			return ignorePattern{}, false, fmt.Errorf("%q negates nothing", source)
		}
	}

	if strings.HasSuffix(line, "/") {
		pat.dirOnly = true
		line = strings.TrimSuffix(line, "/")
		if line == "" {
			return ignorePattern{}, false, fmt.Errorf("%q names no path", source)
		}
	}

	// Anchoring: a leading '/' anchors and is stripped; any other '/' left
	// in the pattern anchors it too, without being stripped.
	anchored := false
	if strings.HasPrefix(line, "/") {
		anchored = true
		line = strings.TrimPrefix(line, "/")
		if line == "" {
			return ignorePattern{}, false, fmt.Errorf("%q names no path", source)
		}
	}
	if strings.Contains(line, "/") {
		anchored = true
	}

	re, err := compileGlob(line, anchored)
	if err != nil {
		return ignorePattern{}, false, err
	}
	pat.re = re
	return pat, true, nil
}

// trimUnescapedTrailingSpace drops trailing spaces and tabs, but keeps one
// that a backslash escapes (gitignore allows "foo\ " to mean a name ending
// in a space).
func trimUnescapedTrailingSpace(s string) string {
	end := len(s)
	for end > 0 && (s[end-1] == ' ' || s[end-1] == '\t') {
		backslashes := 0
		for j := end - 2; j >= 0 && s[j] == '\\'; j-- {
			backslashes++
		}
		if backslashes%2 == 1 {
			break // escaped: this space is part of the name
		}
		end--
	}
	return s[:end]
}

// compileGlob translates one gitignore glob into a regexp matching a whole
// share-relative path. An unanchored pattern gets a "(?:.*/)?" prefix so it
// matches at any depth; an anchored one matches from the share root only.
//
// Note what is deliberately NOT handled here: a pattern naming a directory
// does not, by itself, match that directory's children. Ancestor
// containment is IgnoreRules.Match's job, because only it can evaluate the
// ancestors in order and let a later '!' re-include a child.
func compileGlob(pat string, anchored bool) (*regexp.Regexp, error) {
	var b strings.Builder
	b.WriteString(`\A`)
	if !anchored {
		b.WriteString(`(?:.*/)?`)
	}

	i, n := 0, len(pat)
	for i < n {
		switch c := pat[i]; c {
		case '*':
			j := i
			for j < n && pat[j] == '*' {
				j++
			}
			if j-i >= 2 {
				// '**' crosses '/' boundaries.
				prevSlash := i == 0 || pat[i-1] == '/'
				if prevSlash && j < n && pat[j] == '/' {
					// "**/" — match zero or more leading segments.
					b.WriteString(`(?:.*/)?`)
					j++ // consume the '/'
				} else {
					b.WriteString(`.*`)
				}
			} else {
				b.WriteString(`[^/]*`)
			}
			i = j
		case '?':
			b.WriteString(`[^/]`)
			i++
		case '[':
			cls, next, ok := scanCharClass(pat, i)
			if !ok {
				b.WriteString(regexp.QuoteMeta("["))
				i++
				continue
			}
			b.WriteString(cls)
			i = next
		case '\\':
			if i+1 < n {
				b.WriteString(regexp.QuoteMeta(string(pat[i+1])))
				i += 2
			} else {
				b.WriteString(regexp.QuoteMeta(`\`))
				i++
			}
		default:
			b.WriteString(regexp.QuoteMeta(string(c)))
			i++
		}
	}

	b.WriteString(`\z`)
	return regexp.Compile(b.String())
}

// scanCharClass reads a '[...]' class starting at pat[i], returning its
// regexp spelling and the index just past it. ok is false for an
// unterminated '[', which callers treat as a literal bracket.
func scanCharClass(pat string, i int) (cls string, next int, ok bool) {
	n := len(pat)
	j := i + 1
	negated := false
	if j < n && (pat[j] == '!' || pat[j] == '^') {
		negated = true
		j++
	}
	if j < n && pat[j] == ']' {
		j++ // a ']' first in the class is a literal ']'
	}
	for j < n && pat[j] != ']' {
		if pat[j] == '\\' {
			j++
		}
		j++
	}
	if j >= n {
		return "", 0, false
	}
	body := pat[i+1 : j]
	if negated {
		body = body[1:] // drop the '!' or '^'
		return "[^" + body + "]", j + 1, true
	}
	return "[" + body + "]", j + 1, true
}

// Match reports whether relpath is excluded by these rules.
//
// It evaluates every ancestor directory of relpath before relpath itself,
// keeping the last verdict. That ordering is what makes a directory
// pattern cover its contents ("build/" ignores "build/out.o") while still
// letting a later negation re-include one of them ("!build/keep.txt"),
// which is why the scanner is free to stop pruning ignored directories
// when negations are present without changing any outcome.
//
// relpath must be share-relative and forward-slash separated, as the
// scanner produces it. isDir says whether relpath itself is a directory;
// ancestors are always treated as directories, because they are.
func (g *IgnoreRules) Match(relpath string, isDir bool) bool {
	if g == nil || len(g.patterns) == 0 || relpath == "" || relpath == "." {
		return false
	}

	ignored := false
	start := 0
	for {
		slash := strings.IndexByte(relpath[start:], '/')
		if slash < 0 {
			break
		}
		start += slash
		if negate, matched := g.matchOne(relpath[:start], true); matched {
			ignored = !negate
		}
		start++
	}
	if negate, matched := g.matchOne(relpath, isDir); matched {
		ignored = !negate
	}
	return ignored
}

// matchOne finds the last pattern matching p, which is the one that
// decides p's fate. Iterating backwards and stopping at the first hit is
// the same thing, done once.
func (g *IgnoreRules) matchOne(p string, isDir bool) (negate, matched bool) {
	for i := len(g.patterns) - 1; i >= 0; i-- {
		pat := g.patterns[i]
		if pat.dirOnly && !isDir {
			continue
		}
		if pat.re.MatchString(p) {
			return pat.negate, true
		}
	}
	return false, false
}

// HasNegations reports whether any rule re-includes a path with '!'. The
// scanner uses it to decide whether it may prune an ignored directory
// without descending — see Matcher.PruneDirs.
func (g *IgnoreRules) HasNegations() bool {
	return g != nil && g.hasNegations
}

package index

import (
	"strings"
	"testing"
)

// mustRules parses content, failing the test on a read error and on any
// per-line warning the caller did not expect.
func mustRules(t *testing.T, content string) *IgnoreRules {
	t.Helper()
	g, warnings, err := ParseIgnore(strings.NewReader(content))
	if err != nil {
		t.Fatalf("ParseIgnore(%q): %v", content, err)
	}
	if len(warnings) != 0 {
		t.Fatalf("ParseIgnore(%q): unexpected warnings: %v", content, warnings)
	}
	return g
}

// TestIgnoreSyntax is the specification of .syncatignore's pattern
// language, written as the cases a user would actually reach for. Each row
// is one ignore file plus the paths it should and should not exclude.
func TestIgnoreSyntax(t *testing.T) {
	type pathCase struct {
		path  string
		isDir bool
		want  bool
	}
	tests := []struct {
		name    string
		content string
		paths   []pathCase
	}{
		{
			name:    "blank lines and comments carry no patterns",
			content: "\n# a comment\n\n   \n*.log\n",
			paths: []pathCase{
				{path: "a.log", want: true},
				{path: "comment", want: false},
				{path: "a.txt", want: false},
			},
		},
		{
			name:    "an escaped hash is a literal name",
			content: "\\#notacomment\n",
			paths: []pathCase{
				{path: "#notacomment", want: true},
				{path: "notacomment", want: false},
			},
		},
		{
			name:    "an unanchored pattern matches at any depth",
			content: "*.log\n",
			paths: []pathCase{
				{path: "a.log", want: true},
				{path: "deep/nested/a.log", want: true},
				{path: "a.log.txt", want: false},
			},
		},
		{
			name:    "a leading slash anchors to the share root",
			content: "/build\n",
			paths: []pathCase{
				{path: "build", isDir: true, want: true},
				{path: "build/out.o", want: true},
				{path: "src/build", isDir: true, want: false},
			},
		},
		{
			name:    "an interior slash anchors too",
			content: "docs/draft\n",
			paths: []pathCase{
				{path: "docs/draft", want: true},
				{path: "other/docs/draft", want: false},
			},
		},
		{
			name:    "a bare name is unanchored and matches at any depth",
			content: "build\n",
			paths: []pathCase{
				{path: "build", isDir: true, want: true},
				{path: "src/build", isDir: true, want: true},
				{path: "src/build/out.o", want: true},
			},
		},
		{
			name:    "a trailing slash matches directories only",
			content: "node_modules/\n",
			paths: []pathCase{
				{path: "node_modules", isDir: true, want: true},
				{path: "node_modules/pkg/index.js", want: true},
				{path: "app/node_modules", isDir: true, want: true},
				{path: "node_modules", isDir: false, want: false},
			},
		},
		{
			name:    "double star crosses slash boundaries",
			content: "docs/**/*.tmp\n",
			paths: []pathCase{
				{path: "docs/a.tmp", want: true},
				{path: "docs/x/y/a.tmp", want: true},
				{path: "other/a.tmp", want: false},
			},
		},
		{
			name:    "a leading double star matches at any depth",
			content: "**/target\n",
			paths: []pathCase{
				{path: "target", isDir: true, want: true},
				{path: "a/b/target", isDir: true, want: true},
			},
		},
		{
			name:    "a trailing double star matches everything beneath",
			content: "logs/**\n",
			paths: []pathCase{
				{path: "logs/a.txt", want: true},
				{path: "logs/x/y.txt", want: true},
				{path: "logs", isDir: true, want: false},
			},
		},
		{
			name:    "a later negation re-includes",
			content: "*.log\n!keep.log\n",
			paths: []pathCase{
				{path: "a.log", want: true},
				{path: "keep.log", want: false},
			},
		},
		{
			name:    "order decides: the last matching pattern wins",
			content: "!keep.log\n*.log\n",
			paths: []pathCase{
				{path: "keep.log", want: true},
			},
		},
		{
			name:    "a negation can re-include a file under an ignored directory",
			content: "build/\n!build/keep.txt\n",
			paths: []pathCase{
				{path: "build", isDir: true, want: true},
				{path: "build/out.o", want: true},
				{path: "build/keep.txt", want: false},
			},
		},
		{
			name:    "an escaped bang is a literal name",
			content: "\\!important\n",
			paths: []pathCase{
				{path: "!important", want: true},
			},
		},
		{
			name:    "character classes",
			content: "[abc].txt\nq[!xy].txt\n",
			paths: []pathCase{
				{path: "a.txt", want: true},
				{path: "c.txt", want: true},
				{path: "d.txt", want: false},
				{path: "qz.txt", want: true},
				{path: "qx.txt", want: false},
			},
		},
		{
			name:    "a single question mark matches one non-slash character",
			content: "?.txt\n",
			paths: []pathCase{
				{path: "a.txt", want: true},
				{path: "ab.txt", want: false},
			},
		},
		{
			name:    "a star does not cross a slash",
			content: "*.txt\n",
			paths: []pathCase{
				{path: "a.txt", want: true},
				{path: "dir/a.txt", want: true},
				{path: "a/b.txt", want: true},
			},
		},
		{
			name:    "unescaped trailing whitespace is trimmed",
			content: "foo   \n",
			paths: []pathCase{
				{path: "foo", want: true},
				{path: "foo   ", want: false},
			},
		},
		{
			name:    "escaped trailing whitespace is kept",
			content: "foo\\ \n",
			paths: []pathCase{
				{path: "foo ", want: true},
				{path: "foo", want: false},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := mustRules(t, tt.content)
			for _, p := range tt.paths {
				if got := g.Match(p.path, p.isDir); got != p.want {
					t.Errorf("Match(%q, isDir=%v) = %v, want %v", p.path, p.isDir, got, p.want)
				}
			}
		})
	}
}

// TestIgnoreMalformedPatternWarnsAndSkips pins the failure mode that keeps
// a share online: one bad line is reported and dropped, and every other
// line in the file still takes effect.
func TestIgnoreMalformedPatternWarnsAndSkips(t *testing.T) {
	g, warnings, err := ParseIgnore(strings.NewReader("*.log\n[z-a]\n*.tmp\n"))
	if err != nil {
		t.Fatalf("ParseIgnore: %v", err)
	}
	if len(warnings) != 1 {
		t.Fatalf("warnings = %v, want exactly 1", warnings)
	}
	if !strings.Contains(warnings[0], "[z-a]") {
		t.Errorf("warning %q does not name the offending line", warnings[0])
	}
	if !g.Match("a.log", false) || !g.Match("a.tmp", false) {
		t.Error("a malformed line suppressed the surrounding valid patterns")
	}
}

// TestIgnoreNilAndEmptyMatchNothing: the zero value and a nil *IgnoreRules
// are both usable, because a share with no .syncatignore is the norm.
func TestIgnoreNilAndEmptyMatchNothing(t *testing.T) {
	var nilRules *IgnoreRules
	if nilRules.Match("anything", false) {
		t.Error("nil IgnoreRules matched")
	}
	if nilRules.HasNegations() {
		t.Error("nil IgnoreRules reported negations")
	}
	empty := mustRules(t, "# only a comment\n")
	if empty.Match("anything", false) {
		t.Error("empty IgnoreRules matched")
	}
}

// TestIgnoreHasNegations drives Matcher.PruneDirs, which is what decides
// whether the scanner may skip an ignored directory without descending.
func TestIgnoreHasNegations(t *testing.T) {
	if mustRules(t, "*.log\n").HasNegations() {
		t.Error("rules without '!' reported negations")
	}
	if !mustRules(t, "*.log\n!keep.log\n").HasNegations() {
		t.Error("rules with '!' did not report negations")
	}
}

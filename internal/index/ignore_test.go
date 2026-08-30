package index

import "testing"

func TestMatcherBuiltins(t *testing.T) {
	m := NewMatcher(nil)
	cases := []struct {
		relpath string
		want    bool
	}{
		{".syncat.tmp.abc123", true},
		{"dir/.syncat.tmp.xyz", true},
		{".DS_Store", true},
		{"nested/deep/.DS_Store", true},
		{"Thumbs.db", true},
		{"desktop.ini", true},
		{"normal.txt", false},
		{"dir/normal.txt", false},
	}
	for _, c := range cases {
		if got := m.Match(c.relpath); got != c.want {
			t.Errorf("Match(%q) = %v, want %v", c.relpath, got, c.want)
		}
	}
}

func TestMatcherGlobalIgnores(t *testing.T) {
	m := NewMatcher([]string{"*.log", "build/output.txt"})

	cases := []struct {
		relpath string
		want    bool
	}{
		// "*.log" has no '/', so it matches the base name at any depth.
		{"app.log", true},
		{"nested/deep/app.log", true},
		{"app.logx", false},
		// "build/output.txt" contains '/', so it only matches the full
		// relpath, not the base name alone.
		{"build/output.txt", true},
		{"other/build/output.txt", false},
		{"output.txt", false},
	}
	for _, c := range cases {
		if got := m.Match(c.relpath); got != c.want {
			t.Errorf("Match(%q) = %v, want %v", c.relpath, got, c.want)
		}
	}
}

func TestMatcherBaseNameVsFullPathSemantics(t *testing.T) {
	// A pattern with a '/' should NOT match just because the base name
	// happens to match part of it; it only matches the full relpath.
	m := NewMatcher([]string{"a/b.txt"})
	if m.Match("b.txt") {
		t.Error(`Match("b.txt") with pattern "a/b.txt": want false (base name alone must not satisfy a slash pattern)`)
	}
	if !m.Match("a/b.txt") {
		t.Error(`Match("a/b.txt") with pattern "a/b.txt": want true`)
	}

	// A pattern with no '/' matches the base name regardless of
	// directory depth, but does NOT match a full relpath containing
	// slashes unless path.Match itself would (it won't, since '*' does
	// not cross '/').
	m2 := NewMatcher([]string{"*.tmp"})
	if !m2.Match("x.tmp") {
		t.Error(`Match("x.tmp") with pattern "*.tmp": want true`)
	}
	if !m2.Match("a/b/x.tmp") {
		t.Error(`Match("a/b/x.tmp") with pattern "*.tmp": want true (matches base name)`)
	}
}

func TestMatcherNilIsNoop(t *testing.T) {
	var m *Matcher
	if m.Match("anything") {
		t.Error("nil *Matcher should never match")
	}
}

func TestMatcherAlwaysIncludesBuiltinsAlongsideGlobal(t *testing.T) {
	m := NewMatcher([]string{"*.custom"})
	if !m.Match(".DS_Store") {
		t.Error("built-ins must still apply even when GlobalIgnores is non-empty")
	}
	if !m.Match("foo.custom") {
		t.Error("global ignore pattern should also apply")
	}
}

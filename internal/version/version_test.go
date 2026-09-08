package version

import (
	"regexp"
	"testing"
)

func TestVersionIsSet(t *testing.T) {
	if Version == "" {
		t.Fatal("Version is empty")
	}
}

// TestStringStartsWithVersion pins the shape String() promises: Version,
// optionally followed by "+" and build metadata. Anything reading the
// version — a human, a bug report, a `syncat status --json` consumer —
// relies on the release number being the leading, unadorned part.
//
// The commit suffix is deliberately not asserted on: `go test` builds
// carry no VCS stamp, so under test String() correctly returns a bare
// Version. Asserting it were present would only pin the test harness.
func TestStringStartsWithVersion(t *testing.T) {
	got := String()
	if got == Version {
		return // no VCS stamp in this build; the documented fallback
	}
	want := regexp.MustCompile(`^` + regexp.QuoteMeta(Version) + `\+[0-9a-f]{7}(\.dirty)?$`)
	if !want.MatchString(got) {
		t.Errorf("String() = %q, want %q or %q+<7 hex chars>[.dirty]", got, Version, Version)
	}
}

// TestStringIsStable guards the memoization: the version must not change
// between calls, since it is read on every /api/status poll.
func TestStringIsStable(t *testing.T) {
	if a, b := String(), String(); a != b {
		t.Errorf("String() is not stable: %q then %q", a, b)
	}
}

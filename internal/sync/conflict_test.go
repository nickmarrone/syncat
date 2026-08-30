package sync

import (
	"strings"
	"testing"
	"time"
)

func TestConflictRelPath(t *testing.T) {
	fixedTime := time.Date(2026, 8, 30, 14, 5, 9, 0, time.UTC)
	const node = "deadbeefcafef00d"

	cases := []struct {
		name    string
		relpath string
		want    string
	}{
		{
			name:    "simple extension",
			relpath: "notes.txt",
			want:    "notes.sync-conflict-20260830-140509-deadbeefcafef00d.txt",
		},
		{
			name:    "multi-part extension keeps only the last part appended",
			relpath: "notes.tar.gz",
			want:    "notes.tar.sync-conflict-20260830-140509-deadbeefcafef00d.gz",
		},
		{
			name:    "extensionless file",
			relpath: "README",
			want:    "README.sync-conflict-20260830-140509-deadbeefcafef00d",
		},
		{
			name:    "dotfile with no extension",
			relpath: ".bashrc",
			want:    ".bashrc.sync-conflict-20260830-140509-deadbeefcafef00d",
		},
		{
			name:    "dotfile with an extension",
			relpath: ".config.json",
			want:    ".config.sync-conflict-20260830-140509-deadbeefcafef00d.json",
		},
		{
			name:    "many dots",
			relpath: "a.b.c.d.txt",
			want:    "a.b.c.d.sync-conflict-20260830-140509-deadbeefcafef00d.txt",
		},
		{
			name:    "nested path keeps directory intact",
			relpath: "dir/sub/notes.txt",
			want:    "dir/sub/notes.sync-conflict-20260830-140509-deadbeefcafef00d.txt",
		},
		{
			name:    "already-conflicted file gets a fresh marker, not a second one",
			relpath: "notes.sync-conflict-20200101-000000-aaaaaaaaaaaaaaaa.txt",
			want:    "notes.sync-conflict-20260830-140509-deadbeefcafef00d.txt",
		},
		{
			name:    "already-conflicted multi-part extension file",
			relpath: "notes.tar.sync-conflict-20200101-000000-aaaaaaaaaaaaaaaa.gz",
			want:    "notes.tar.sync-conflict-20260830-140509-deadbeefcafef00d.gz",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := ConflictRelPath(c.relpath, node, fixedTime)
			if got != c.want {
				t.Errorf("ConflictRelPath(%q) = %q, want %q", c.relpath, got, c.want)
			}
		})
	}
}

// TestConflictRelPathBoundedGrowth ensures repeatedly conflicting the same
// file never grows the filename without bound: each round strips the prior
// marker before adding a new one, so the length converges instead of
// growing linearly with the number of conflicts.
func TestConflictRelPathBoundedGrowth(t *testing.T) {
	relpath := "notes.txt"
	baseLen := len(ConflictRelPath(relpath, "deadbeefcafef00d", time.Now()))

	for i := 0; i < 25; i++ {
		relpath = ConflictRelPath(relpath, "deadbeefcafef00d", time.Now())
		if len(relpath) > baseLen+1 { // allow for timestamp/second jitter, not accumulation
			t.Fatalf("round %d: relpath grew unboundedly: %q (len %d, base %d)", i, relpath, len(relpath), baseLen)
		}
	}
}

func TestConflictRelPathTimestampFormat(t *testing.T) {
	got := ConflictRelPath("f.txt", "abc", time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))
	if !strings.Contains(got, ".sync-conflict-20260102-030405-abc") {
		t.Errorf("unexpected timestamp formatting: %q", got)
	}
}

func TestConflictRelPathUsesUTC(t *testing.T) {
	// A time expressed in a non-UTC zone must format identically to its UTC
	// equivalent, so two nodes in different timezones agree on the name.
	loc := time.FixedZone("test", -5*60*60) // UTC-5
	local := time.Date(2026, 8, 30, 9, 5, 9, 0, loc)
	utc := local.UTC()

	got := ConflictRelPath("f.txt", "abc", local)
	want := ConflictRelPath("f.txt", "abc", utc)
	if got != want {
		t.Errorf("timezone-dependent output: local=%q utc=%q", got, want)
	}
}

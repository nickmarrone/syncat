package sync

import (
	"strings"
	"testing"
)

func TestValidateRelPathRejects(t *testing.T) {
	longPath := strings.Repeat("a", maxRelPathLen+1)

	cases := []struct {
		name    string
		relpath string
	}{
		{"parent traversal at root", "../../etc/passwd"},
		{"absolute unix path", "/etc/passwd"},
		{"traversal after a real element", "a/../../b"},
		{"dot then traversal", "./../x"},
		{"double slash then traversal", "a//../..//b"},
		{"dots-only element via empty split", "....//"},
		{"nul byte", "a\x00b"},
		{"empty string", ""},
		{"single dot", "."},
		{"double dot", ".."},
		{"too long", longPath},
		{"trailing slash", "a/b/"},
		{"windows drive letter and backslash", `C:\Windows\system32`},
		{"backslash traversal", `a\..\..\b`},
		{"reserved device name", "CON"},
		{"reserved device name with extension", "CON.txt"},
		{"reserved device name in subdirectory", "dir/NUL"},
		{"lowercase reserved device name", "con"},
		{"leading slash with elements", "/a/b"},
		{"embedded colon (ADS-style)", "file.txt:hidden"},
		{"our own temp file pattern", ".syncat.tmp.abc123"},
		{"our own temp file pattern in a subdir", "dir/.syncat.tmp.xyz"},
		{"control character (tab)", "a\tb"},
		{"control character (DEL)", "a\x7fb"},
		{"empty element in the middle", "a//b"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if err := ValidateRelPath(c.relpath); err == nil {
				t.Errorf("ValidateRelPath(%q) = nil, want an error", c.relpath)
			}
		})
	}
}

func TestValidateRelPathAccepts(t *testing.T) {
	cases := []string{
		"a/b/c.txt",
		"file.txt",
		"dir/.hidden",
		"name with spaces.txt",
		"café-unicode-文件.txt",
		"a.b.c.d",
		"deeply/nested/path/to/a/file.bin",
		".bashrc",
		"no_extension_at_all",
	}
	for _, relpath := range cases {
		t.Run(relpath, func(t *testing.T) {
			if err := ValidateRelPath(relpath); err != nil {
				t.Errorf("ValidateRelPath(%q) = %v, want nil", relpath, err)
			}
		})
	}
}

func TestJoinSharePath(t *testing.T) {
	root := "/data/share"

	t.Run("valid path stays inside root", func(t *testing.T) {
		got, err := JoinSharePath(root, "a/b/c.txt")
		if err != nil {
			t.Fatalf("JoinSharePath: %v", err)
		}
		want := "/data/share/a/b/c.txt"
		if got != want {
			t.Errorf("JoinSharePath = %q, want %q", got, want)
		}
	})

	t.Run("hostile relpath is rejected without ever building a path", func(t *testing.T) {
		_, err := JoinSharePath(root, "../../etc/passwd")
		if err == nil {
			t.Fatal("JoinSharePath = nil error, want rejection")
		}
	})

	t.Run("relative share root is rejected", func(t *testing.T) {
		_, err := JoinSharePath("relative/root", "a.txt")
		if err == nil {
			t.Fatal("JoinSharePath = nil error, want rejection of non-absolute root")
		}
	})

	t.Run("every ValidateRelPath rejection is also a JoinSharePath rejection", func(t *testing.T) {
		hostile := []string{"../x", "/abs", "a/../b", "CON", `a\b`, "a:b", ""}
		for _, relpath := range hostile {
			if _, err := JoinSharePath(root, relpath); err == nil {
				t.Errorf("JoinSharePath(%q) = nil error, want rejection", relpath)
			}
		}
	})
}

package core

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/nickmarrone/syncat/internal/config"
)

// TestRejectedSubscriptionLeavesNothingOnDisk pins #78: a subscription the
// node refuses must not create its folder (or a .syncat/ in it) on the way
// to refusing it.
func TestRejectedSubscriptionLeavesNothingOnDisk(t *testing.T) {
	_, nodeB, shareID := connectedPairWithShare(t, "docs")

	assertAbsent := func(t *testing.T, path string) {
		t.Helper()
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			t.Errorf("%s exists after a rejected subscription (stat err = %v)", path, err)
		}
	}

	t.Run("relative path", func(t *testing.T) {
		cwd := t.TempDir()
		t.Chdir(cwd)
		if err := nodeB.AddSubscription("nodeA", shareID, "relative/sub", config.ModeMirror); err == nil {
			t.Fatal("AddSubscription with a relative path succeeded; want an error")
		}
		assertAbsent(t, filepath.Join(cwd, "relative"))
	})

	t.Run("inside a local share", func(t *testing.T) {
		own := t.TempDir()
		if _, err := nodeB.AddShare(own, "own", config.PermissionReadWrite, false); err != nil {
			t.Fatalf("AddShare: %v", err)
		}
		inner := filepath.Join(own, "inner")
		if err := nodeB.AddSubscription("nodeA", shareID, inner, config.ModeMirror); err == nil {
			t.Fatal("AddSubscription inside a local share succeeded; want an error")
		}
		assertAbsent(t, inner)
	})

	t.Run("duplicate subscription", func(t *testing.T) {
		first := filepath.Join(t.TempDir(), "first")
		if err := nodeB.AddSubscription("nodeA", shareID, first, config.ModeMirror); err != nil {
			t.Fatalf("first AddSubscription: %v", err)
		}
		if _, err := os.Stat(filepath.Join(first, ".syncat")); err != nil {
			t.Fatalf("an accepted subscription should create its folder and .syncat/: %v", err)
		}
		second := filepath.Join(t.TempDir(), "second")
		if err := nodeB.AddSubscription("nodeA", shareID, second, config.ModeMirror); err == nil {
			t.Fatal("a second subscription to the same share succeeded; want an error")
		}
		assertAbsent(t, second)
	})

	if subs := nodeB.Status().Subscriptions; len(subs) != 1 {
		t.Errorf("Subscriptions = %+v, want only the one accepted subscription", subs)
	}
}

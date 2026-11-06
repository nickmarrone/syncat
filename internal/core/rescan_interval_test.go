package core

import (
	"context"
	"io"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nickmarrone/syncat/internal/config"
	"github.com/nickmarrone/syncat/internal/transport"
)

// TestRescanIntervalFromConfig opens a node with rescan_interval_seconds
// set well below the 300s default and checks that a share's periodic full
// rescan (SPEC.md §5) runs on that interval. The change it picks up is
// written so that the fsnotify debounce, which shares the fake clock, has
// not yet fired when the interval elapses: only the periodic scan can have
// indexed it.
func TestRescanIntervalFromConfig(t *testing.T) {
	const interval = 2 * time.Second

	dir := t.TempDir()
	paths, err := config.ResolvePaths(filepath.Join(dir, "config"), filepath.Join(dir, "data"))
	if err != nil {
		t.Fatal(err)
	}
	if err := paths.EnsureDirs(); err != nil {
		t.Fatal(err)
	}
	identity, _, err := config.LoadOrCreateIdentityKey(paths.IdentityKeyFile())
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Default()
	cfg.NodeName = "rescanner"
	cfg.RescanIntervalSeconds = int(interval / time.Second)

	clock := newFakeClock(time.Unix(0, 0))
	n, err := Open(context.Background(), Options{
		Paths:         paths,
		Config:        cfg,
		Identity:      identity,
		Transport:     transport.NewPipeTransport(newPipeAddr(t)),
		Logger:        log.New(io.Discard, "", 0),
		Clock:         clock,
		DisableJitter: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { n.Close() })

	shareDir := t.TempDir()
	shareID, err := n.AddShare(shareDir, "docs", config.PermissionReadWrite, false)
	if err != nil {
		t.Fatal(err)
	}
	isInterval := func(d time.Duration) bool { return d == interval }
	if !clock.waitForWaiterMatching(isInterval, 5*time.Second) {
		t.Fatalf("no timer armed for the configured %s rescan interval; armed delays: %v",
			interval, clock.snapshotDelays(func(time.Duration) bool { return true }))
	}

	// Write the file three quarters of the way through the interval, so
	// its debounce window (index.DefaultDebounce, 1s) ends after the
	// periodic scan is due.
	clock.Advance(interval * 3 / 4)
	if err := os.WriteFile(filepath.Join(shareDir, "late.txt"), []byte("hi"), 0o644); err != nil {
		t.Fatal(err)
	}
	clock.Advance(interval / 4)

	ctx := context.Background()
	waitFor(t, 5*time.Second, func() bool {
		_, err := n.store.GetFile(ctx, shareID, "late.txt")
		return err == nil
	})
}

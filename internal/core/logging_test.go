package core

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nickmarrone/syncat/internal/config"
	syncsvc "github.com/nickmarrone/syncat/internal/sync"
	"github.com/nickmarrone/syncat/internal/transport"
)

// safeBuffer is a bytes.Buffer usable as a log sink from the many
// goroutines a Node runs (peer supervisors, session read loops, the trash
// janitor) without tripping the race detector.
type safeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *safeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *safeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// newLoggingTestNode is newTestNode with the logger captured (and,
// optionally, a caller-supplied clock and config tweaks), so a test can
// assert on what the node actually wrote.
func newLoggingTestNode(t *testing.T, name string, clock Clock, tweak func(*config.Config)) (*Node, *safeBuffer) {
	t.Helper()
	dir := t.TempDir()
	paths, err := config.ResolvePaths(filepath.Join(dir, "config"), filepath.Join(dir, "data"))
	if err != nil {
		t.Fatalf("resolve paths: %v", err)
	}
	if err := paths.EnsureDirs(); err != nil {
		t.Fatalf("ensure dirs: %v", err)
	}
	identity, _, err := config.LoadOrCreateIdentityKey(paths.IdentityKeyFile())
	if err != nil {
		t.Fatalf("load identity: %v", err)
	}
	cfg := config.Default()
	cfg.NodeName = name
	if tweak != nil {
		tweak(cfg)
	}

	logs := &safeBuffer{}
	n, err := Open(context.Background(), Options{
		Paths:         paths,
		Config:        cfg,
		Identity:      identity,
		Transport:     transport.NewPipeTransport(newPipeAddr(t)),
		Logger:        log.New(logs, "", 0),
		Clock:         clock,
		DisableJitter: true,
	})
	if err != nil {
		t.Fatalf("open node %s: %v", name, err)
	}
	t.Cleanup(func() { n.Close() })
	return n, logs
}

// --- connection lifecycle -------------------------------------------------

func TestDialFailureRedactsAddressAndSanitizesStatusAndLogs(t *testing.T) {
	const credential = "tc-distinctive-private-credential"
	logs := &safeBuffer{}
	pc := &peerConn{
		node:      &Node{logger: log.New(logs, "", 0)},
		name:      "peer\nforged-name",
		peerShort: "01234567",
		addr:      credential,
	}
	pc.noteDialFailure("dial", fmt.Errorf("cannot reach %s\nforged-error\x1b[31m", credential))

	if strings.Contains(pc.lastErr, credential) || strings.ContainsAny(pc.lastErr, "\r\n\x1b") {
		t.Fatalf("LastError was not sanitized: %q", pc.lastErr)
	}
	out := logs.String()
	if strings.Contains(out, credential) || strings.ContainsAny(strings.TrimSuffix(out, "\n"), "\r\n\x1b") {
		t.Fatalf("dial log was not sanitized: %q", out)
	}
}

// TestConnectionLifecycleIsLogged is the gap this logging exists to close.
// Every other line in peer.go is an error path, so a node whose peer flaps
// every thirty seconds and one that has been stably connected for a week
// produced near-identical logs — the flapping one simply had less in it.
// Connect and disconnect must each say so, and the disconnect must carry
// how long the connection lasted, since that is the only thing that
// distinguishes a flap from an orderly shutdown.
func TestConnectionLifecycleIsLogged(t *testing.T) {
	nodeA, logsA := newLoggingTestNode(t, "nodeA", nil, nil)
	nodeB, _ := newLoggingTestNode(t, "nodeB", nil, nil) // dial target only; its own log is not inspected

	tokenA, tokenB := peerToken(t, nodeA), peerToken(t, nodeB)
	if _, err := nodeA.AddPeer("nodeB", tokenB); err != nil {
		t.Fatalf("nodeA AddPeer: %v", err)
	}
	if _, err := nodeB.AddPeer("nodeA", tokenA); err != nil {
		t.Fatalf("nodeB AddPeer: %v", err)
	}
	// Wait on the log line itself rather than on Status() reporting both
	// sides connected: the line is the thing under test, and requiring
	// nodeB to converge too would make this test fail for reasons that
	// have nothing to do with logging.
	waitFor(t, 20*time.Second, func() bool {
		out := logsA.String()
		return strings.Contains(out, "peer nodeB") && strings.Contains(out, "connected")
	})
	out := logsA.String()
	if !strings.Contains(out, "remote name") {
		t.Errorf("connect line = %q, want it to carry the peer's remote name", out)
	}
	// Whichever side won the dedup rule, the line must say how the
	// connection arrived so a one-way-reachability problem is legible.
	if !strings.Contains(out, "dialed") && !strings.Contains(out, "accepted from") {
		t.Errorf("connect line = %q, want it to say whether we dialed or accepted", out)
	}

	// Tearing the peer down must log the disconnect, with a duration.
	if err := nodeA.RemovePeer(nodeB.PeerKey()); err != nil {
		t.Fatalf("RemovePeer: %v", err)
	}
	waitFor(t, 20*time.Second, func() bool {
		return strings.Contains(logsA.String(), "disconnected after")
	})
}

// --- trash janitor --------------------------------------------------------

// TestTrashSweepIsLogged covers a callback that was previously wired to
// nil, which made the janitor completely silent: a sweep that failed was
// discarded with nothing recorded anywhere, and a successful purge deleted
// the user's last remaining copy of a file with no record it had happened.
func TestTrashSweepIsLogged(t *testing.T) {
	clock := newFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	n, logs := newLoggingTestNode(t, "nodeA", clock, func(cfg *config.Config) {
		cfg.TrashRetentionDays = 1
	})

	// Trash a file, then move the clock past both the retention window and
	// the janitor's sweep interval so the background loop purges it.
	src := filepath.Join(t.TempDir(), "doomed.txt")
	if err := os.WriteFile(src, []byte("bye"), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}
	if err := n.trash.Put(context.Background(), "share1", "doomed.txt", src); err != nil {
		t.Fatalf("trash put: %v", err)
	}

	// Wait until the janitor is actually parked on its interval before
	// advancing, so the tick is never missed.
	if !clock.waitForWaiterMatching(func(d time.Duration) bool { return d == syncsvc.DefaultJanitorInterval }, 5*time.Second) {
		t.Fatal("janitor never parked on its sweep interval")
	}
	clock.Advance(48 * time.Hour)

	waitFor(t, 5*time.Second, func() bool {
		return strings.Contains(logs.String(), "trash sweep purged")
	})
	if out := logs.String(); !strings.Contains(out, "purged 1 expired entry ") {
		t.Errorf("sweep line = %q, want it to report exactly 1 entry purged (singular)", out)
	}
}

// TestTrashSweepQuietWhenNothingPurged: retention deletion is worth a line,
// but the overwhelmingly common sweep purges nothing, and a daily "purged
// 0" is exactly the kind of line that trains people to ignore the log.
func TestTrashSweepQuietWhenNothingPurged(t *testing.T) {
	n, logs := newLoggingTestNode(t, "nodeA", nil, nil)

	n.onTrashSweep(0, nil)
	if out := logs.String(); out != "" {
		t.Errorf("empty sweep logged %q, want nothing", out)
	}

	// A failure is never silent, even with nothing purged.
	n.onTrashSweep(0, os.ErrPermission)
	if out := logs.String(); !strings.Contains(out, "trash sweep failed") {
		t.Errorf("failed sweep logged %q, want a failure line", out)
	}
}

// --- debug gating ---------------------------------------------------------

// TestDebugLoggingIsGated: the config-mutation trail and the scan/sync
// activity summaries are useful when reconstructing what a node did and
// far too chatty for a daemon at rest, so they must be silent by default
// and present when the debug flag is set.
func TestDebugLoggingIsGated(t *testing.T) {
	quiet, quietLogs := newLoggingTestNode(t, "quiet", nil, nil)
	if _, err := quiet.AddShare(t.TempDir(), "docs", config.PermissionReadWrite, false); err != nil {
		t.Fatalf("AddShare: %v", err)
	}
	if out := quietLogs.String(); strings.Contains(out, "added share") {
		t.Errorf("debug=false logged %q, want the mutation trail suppressed", out)
	}

	loud, loudLogs := newLoggingTestNode(t, "loud", nil, func(cfg *config.Config) { cfg.Debug = true })
	if _, err := loud.AddShare(t.TempDir(), "docs", config.PermissionReadWrite, false); err != nil {
		t.Fatalf("AddShare: %v", err)
	}
	if out := loudLogs.String(); !strings.Contains(out, `added share "docs"`) {
		t.Errorf("debug=true logged %q, want an \"added share\" line", out)
	}
}

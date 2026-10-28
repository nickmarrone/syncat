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
// sweeper) without tripping the race detector.
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
// produced near-identical logs, the flapping one simply had less in it.
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

// --- trash sweeps --------------------------------------------------------

// trashEventAt puts one file into root's trash as an event at at.
func trashEventAt(t *testing.T, root string, at time.Time, name string) {
	t.Helper()
	src := filepath.Join(root, name)
	if err := os.WriteFile(src, []byte("bye"), 0o644); err != nil {
		t.Fatal(err)
	}
	ev := syncsvc.NewTrash(func() time.Time { return at }).BeginPeerEvent(root, "peer")
	if err := ev.Put(context.Background(), name, src, syncsvc.TrashDeleted); err != nil {
		t.Fatalf("trash put: %v", err)
	}
	ev.Close()
}

// Expiry deletes the user's last copy of a file, so every removal is
// logged; the common pass that removes nothing stays quiet.
func TestTrashSweepIsLogged(t *testing.T) {
	now := time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC)
	clock := newFakeClock(now)
	n, logs := newLoggingTestNode(t, "nodeA", clock, func(cfg *config.Config) {
		cfg.TrashRetentionDays = 1
	})
	root := t.TempDir()
	if _, err := n.AddShare(root, "docs", config.PermissionReadWrite, false); err != nil {
		t.Fatal(err)
	}

	trashEventAt(t, root, now.Add(-time.Hour), "fresh.txt")
	n.sweepTrash()
	if out := logs.String(); strings.Contains(out, "trash sweep") {
		t.Errorf("a sweep that removed nothing logged %q", out)
	}

	trashEventAt(t, root, now.Add(-48*time.Hour), "doomed.txt")
	n.sweepTrash()
	if out := logs.String(); !strings.Contains(out, "trash sweep removed 1 event from") {
		t.Errorf("sweep logged %q, want it to report exactly 1 event removed", out)
	}
}

// The background loop runs the sweep on its interval.
func TestTrashSweeperRunsOnInterval(t *testing.T) {
	now := time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC)
	clock := newFakeClock(now)
	n, logs := newLoggingTestNode(t, "nodeA", clock, func(cfg *config.Config) {
		cfg.TrashRetentionDays = 1
	})
	root := t.TempDir()
	if _, err := n.AddShare(root, "docs", config.PermissionReadWrite, false); err != nil {
		t.Fatal(err)
	}
	trashEventAt(t, root, now.Add(-48*time.Hour), "doomed.txt")

	if !clock.waitForWaiterMatching(func(d time.Duration) bool { return d == trashSweepInterval }, 5*time.Second) {
		t.Fatal("trash sweeper never parked on its interval")
	}
	clock.Advance(trashSweepInterval)
	waitFor(t, 5*time.Second, func() bool { return strings.Contains(logs.String(), "trash sweep removed 1 event") })
}

// Old data-dir trash is moved into the share's own trash at startup.
func TestOpenMigratesLegacyTrash(t *testing.T) {
	root := t.TempDir()
	var legacy string
	n, logs := newLoggingTestNode(t, "nodeA", nil, func(cfg *config.Config) {
		cfg.Shares = []config.Share{{ID: "0123456789abcdef", Name: "docs", Path: root, Permission: config.PermissionReadWrite}}
	})
	// The node is already open; seed the old trash and run the migration
	// the way Open does.
	legacy = filepath.Join(n.paths.LegacyTrashDir(), "0123456789abcdef")
	if err := os.MkdirAll(filepath.Join(legacy, "sub"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(legacy, "sub", "old.txt.1700000000"), []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	n.cfgMu.RLock()
	cfg := n.cfg
	n.cfgMu.RUnlock()
	n.migrateLegacyTrash(cfg)

	st, err := n.ListTrash(context.Background(), "docs")
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Events) != 1 || st.Events[0].Items[0].RelPath != "sub/old.txt" {
		t.Fatalf("trash after migration = %+v, want sub/old.txt", st.Events)
	}
	if _, err := os.Stat(n.paths.LegacyTrashDir()); !os.IsNotExist(err) {
		t.Errorf("old trash dir left behind after everything moved (stat err = %v)", err)
	}
	if !strings.Contains(logs.String(), "moved 1 file(s) of old trash") {
		t.Errorf("migration not logged: %q", logs.String())
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

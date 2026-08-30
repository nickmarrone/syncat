package core

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nickmarrone/syncat/internal/config"
	"github.com/nickmarrone/syncat/internal/transport"
)

// --- test harness ---------------------------------------------------------

var pipeAddrCounter int64

func newPipeAddr(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("pipe-%s-%d", t.Name(), atomic.AddInt64(&pipeAddrCounter, 1))
}

// newTestNode builds and opens a fully running Node over a fresh
// PipeTransport (SPEC.md §10's in-memory stand-in for tailcat), with its
// own temp config/data dirs and identity, closed automatically via
// t.Cleanup. Backoff jitter is disabled so timing-sensitive tests (see
// TestBackoffRetryAndRecover) are deterministic.
func newTestNode(t *testing.T, name string) *Node {
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

	tr := transport.NewPipeTransport(newPipeAddr(t))

	n, err := Open(context.Background(), Options{
		Paths:         paths,
		Config:        cfg,
		Identity:      identity,
		Transport:     tr,
		Logger:        log.New(io.Discard, "", 0),
		DisableJitter: true,
	})
	if err != nil {
		t.Fatalf("open node %s: %v", name, err)
	}
	t.Cleanup(func() { n.Close() })
	return n
}

// waitFor polls cond until it returns true or timeout elapses, failing the
// test on timeout. Matches the identical helper already used by
// internal/sync's and internal/index's integration-style tests.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("condition not met within %s", timeout)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func peerConnected(n *Node) bool {
	for _, p := range n.Status().Peers {
		if p.State == ConnStateConnected {
			return true
		}
	}
	return false
}

func peerToken(t *testing.T, n *Node) string {
	t.Helper()
	tok := n.Status().NodeToken
	if tok == "" {
		t.Fatalf("node %s has no token", n.Status().NodeName)
	}
	return tok
}

// --- fake clock for deterministic backoff timing --------------------------

// fakeClock is a manually-driven Clock for the backoff test: nothing here
// ever sleeps on a wall clock. This mirrors internal/protocol's and
// internal/index's own fakeClock test helpers exactly (each package
// defines its own rather than sharing one — see protocol.Clock's doc
// comment for why); core needs the same pattern for the same reason.
//
// A single Options.Clock drives every timer Node starts (see clock.go's
// doc comment), so the test's own peer-dial backoff waits share this
// clock with, at minimum, the trash janitor's 24h sweep interval
// (Node.Open starts the janitor before anything else). isBackoffDelay
// filters that (and any other coarse, non-backoff interval) out wherever
// the test inspects recorded/pending delays, so it only ever reasons
// about the peer supervisor's own schedule.
type fakeClock struct {
	mu      sync.Mutex
	now     time.Time
	waiters []fakeWaiter
	notify  chan struct{}

	// delays records every duration passed to After, in call order — the
	// backoff test asserts this sequence grows (SPEC.md §2.2).
	delays []time.Duration
}

type fakeWaiter struct {
	d        time.Duration
	deadline time.Time
	ch       chan time.Time
}

// isBackoffDelay reports whether d could plausibly be one of
// transport.Backoff's own delays (capped at 5 minutes — SPEC.md §2.2),
// as opposed to some other subsystem's much coarser timer sharing the
// same fake clock (e.g. the trash janitor's 24h sweep interval).
func isBackoffDelay(d time.Duration) bool { return d < time.Hour }

func newFakeClock(start time.Time) *fakeClock {
	return &fakeClock{now: start, notify: make(chan struct{})}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) After(d time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	c.mu.Lock()
	c.delays = append(c.delays, d)
	deadline := c.now.Add(d)
	if d <= 0 {
		c.mu.Unlock()
		ch <- deadline
		return ch
	}
	c.waiters = append(c.waiters, fakeWaiter{d: d, deadline: deadline, ch: ch})
	old := c.notify
	c.notify = make(chan struct{})
	c.mu.Unlock()
	close(old)
	return ch
}

// Advance moves the fake clock forward by d, firing (synchronously, in
// deadline order) every waiter whose deadline has now been reached.
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	now := c.now
	var fired []fakeWaiter
	var remaining []fakeWaiter
	for _, w := range c.waiters {
		if !w.deadline.After(now) {
			fired = append(fired, w)
		} else {
			remaining = append(remaining, w)
		}
	}
	c.waiters = remaining
	c.mu.Unlock()

	for _, w := range fired {
		w.ch <- now
	}
}

// waitForWaiterMatching blocks until at least one goroutine is parked on
// clock.After with a duration matching pred, or timeout elapses — the
// event-driven synchronization primitive that keeps the backoff test
// deterministic instead of racing a blind sleep against Supervisor.Run's
// next iteration.
func (c *fakeClock) waitForWaiterMatching(pred func(time.Duration) bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		c.mu.Lock()
		found := false
		for _, w := range c.waiters {
			if pred(w.d) {
				found = true
				break
			}
		}
		ch := c.notify
		c.mu.Unlock()
		if found {
			return true
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return false
		}
		select {
		case <-ch:
		case <-time.After(remaining):
			return false
		}
	}
}

// snapshotDelays returns every duration passed to After so far, in call
// order, restricted to those matching pred.
func (c *fakeClock) snapshotDelays(pred func(time.Duration) bool) []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []time.Duration
	for _, d := range c.delays {
		if pred(d) {
			out = append(out, d)
		}
	}
	return out
}

// --- tests ------------------------------------------------------------

// TestEndToEndSync peers two in-process nodes over PipeTransport, has one
// subscribe to the other's share, and confirms a file present at
// subscribe time (initial full sync), a file added afterward on the
// offerer (propagated via RescanShare, standing in for the real
// fsnotify/periodic trigger already covered by internal/index's own
// tests), and — since the subscription is a mirror — a file added on the
// subscriber's side all converge, driven entirely through the core layer
// (Node.AddShare/AddPeer/AddSubscription/RescanShare), never by calling
// internal/sync.Session directly.
func TestEndToEndSync(t *testing.T) {
	ctx := context.Background()
	nodeA := newTestNode(t, "nodeA")
	nodeB := newTestNode(t, "nodeB")

	shareDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(shareDir, "hello.txt"), []byte("hello from A"), 0o644); err != nil {
		t.Fatalf("seed share file: %v", err)
	}
	shareID, err := nodeA.AddShare(shareDir, "docs", config.PermissionReadWrite, false)
	if err != nil {
		t.Fatalf("AddShare: %v", err)
	}

	tokenA, tokenB := peerToken(t, nodeA), peerToken(t, nodeB)
	if _, err := nodeA.AddPeer("nodeB", tokenB); err != nil {
		t.Fatalf("nodeA AddPeer: %v", err)
	}
	if _, err := nodeB.AddPeer("nodeA", tokenA); err != nil {
		t.Fatalf("nodeB AddPeer: %v", err)
	}

	waitFor(t, 5*time.Second, func() bool { return peerConnected(nodeA) && peerConnected(nodeB) })

	localDir := t.TempDir()
	if err := nodeB.AddSubscription(nodeA.PeerKey(), shareID, localDir, config.ModeMirror); err != nil {
		t.Fatalf("AddSubscription: %v", err)
	}

	waitFor(t, 5*time.Second, func() bool {
		data, err := os.ReadFile(filepath.Join(localDir, "hello.txt"))
		return err == nil && string(data) == "hello from A"
	})

	// A new file on the offerer side, after the fact.
	if err := os.WriteFile(filepath.Join(shareDir, "second.txt"), []byte("second"), 0o644); err != nil {
		t.Fatalf("write second file: %v", err)
	}
	if err := nodeA.RescanShare(ctx, shareID); err != nil {
		t.Fatalf("RescanShare on nodeA: %v", err)
	}
	waitFor(t, 5*time.Second, func() bool {
		data, err := os.ReadFile(filepath.Join(localDir, "second.txt"))
		return err == nil && string(data) == "second"
	})

	// Mirror mode: the subscriber's own local edit propagates back.
	if err := os.WriteFile(filepath.Join(localDir, "from_b.txt"), []byte("from B"), 0o644); err != nil {
		t.Fatalf("write from_b file: %v", err)
	}
	if err := nodeB.RescanShare(ctx, shareID); err != nil {
		t.Fatalf("RescanShare on nodeB: %v", err)
	}
	waitFor(t, 5*time.Second, func() bool {
		data, err := os.ReadFile(filepath.Join(shareDir, "from_b.txt"))
		return err == nil && string(data) == "from B"
	})
}

// TestDedupConvergence peers two nodes that both dial each other
// simultaneously (each Node.AddPeer starts its own dial supervisor right
// away) and asserts they converge on a single, stable connection on both
// sides (SPEC.md §2.4). Run with -count=20 to shake out ordering
// flakiness; transport.KeepConnection's own outcome is a pure function of
// the two keys and who dialed (see peer.go's offer doc comment), so this
// is expected to converge identically every run.
func TestDedupConvergence(t *testing.T) {
	nodeA := newTestNode(t, "dedupA")
	nodeB := newTestNode(t, "dedupB")

	tokenA, tokenB := peerToken(t, nodeA), peerToken(t, nodeB)
	if _, err := nodeA.AddPeer("B", tokenB); err != nil {
		t.Fatalf("nodeA AddPeer: %v", err)
	}
	if _, err := nodeB.AddPeer("A", tokenA); err != nil {
		t.Fatalf("nodeB AddPeer: %v", err)
	}

	waitFor(t, 5*time.Second, func() bool { return peerConnected(nodeA) && peerConnected(nodeB) })

	pcOnA := nodeA.lookupPeer(nodeB.PeerKey())
	pcOnB := nodeB.lookupPeer(nodeA.PeerKey())
	if pcOnA == nil || pcOnB == nil {
		t.Fatalf("peerConn missing: onA=%v onB=%v", pcOnA, pcOnB)
	}

	snapshot := func(pc *peerConn) (connected bool, state ConnState, since time.Time) {
		pc.mu.Lock()
		defer pc.mu.Unlock()
		return pc.session != nil, pc.state, pc.connectedSince
	}

	aConnected, aState, aSince := snapshot(pcOnA)
	bConnected, bState, bSince := snapshot(pcOnB)
	if !aConnected || aState != ConnStateConnected {
		t.Fatalf("nodeA's view of the peer is not connected: connected=%v state=%v", aConnected, aState)
	}
	if !bConnected || bState != ConnStateConnected {
		t.Fatalf("nodeB's view of the peer is not connected: connected=%v state=%v", bConnected, bState)
	}

	// Stability: a lingering duplicate connection being independently
	// negotiated and then dedup-closed would perturb connectedSince or
	// drop the state away from Connected. Re-checking after a short
	// interval confirms exactly one connection is in play on each side,
	// not just that one happened to be up at the first check.
	time.Sleep(150 * time.Millisecond)
	aConnected2, aState2, aSince2 := snapshot(pcOnA)
	bConnected2, bState2, bSince2 := snapshot(pcOnB)
	if !aConnected2 || aState2 != ConnStateConnected || !aSince.Equal(aSince2) {
		t.Fatalf("nodeA's connection was not stable: (%v,%v,%v) -> (%v,%v,%v)", aConnected, aState, aSince, aConnected2, aState2, aSince2)
	}
	if !bConnected2 || bState2 != ConnStateConnected || !bSince.Equal(bSince2) {
		t.Fatalf("nodeB's connection was not stable: (%v,%v,%v) -> (%v,%v,%v)", bConnected, bState, bSince, bConnected2, bState2, bSince2)
	}
}

// TestUnknownPeerRejected has nodeB dial nodeA without nodeA ever
// configuring nodeB as a peer. nodeA's handshake rejects the unknown key
// (SPEC.md §2.3's pending-peer queue is deferred — see the package doc
// comment) and records it as observable via Status().RejectedConnections.
func TestUnknownPeerRejected(t *testing.T) {
	nodeA := newTestNode(t, "unknownA")
	nodeB := newTestNode(t, "unknownB")

	tokenA := peerToken(t, nodeA)
	if _, err := nodeB.AddPeer("A", tokenA); err != nil {
		t.Fatalf("nodeB AddPeer: %v", err)
	}

	waitFor(t, 5*time.Second, func() bool { return len(nodeA.Status().RejectedConnections) > 0 })

	rej := nodeA.Status().RejectedConnections[0]
	if rej.PeerKey != nodeB.PeerKey() {
		t.Fatalf("rejected connection key = %s, want %s", rej.PeerKey, nodeB.PeerKey())
	}
	if len(nodeA.Status().Peers) != 0 {
		t.Fatalf("nodeA should have no configured peers, got %d", len(nodeA.Status().Peers))
	}
}

// TestBackoffRetryAndRecover drives a peer that's initially unreachable
// (its token's transport address has no listener) through several failed
// dial attempts on a fake clock — asserting the requested delay grows
// each time, with no real sleeping — then brings the peer online and
// confirms the connection recovers.
func TestBackoffRetryAndRecover(t *testing.T) {
	dir := t.TempDir()
	pathsB, err := config.ResolvePaths(filepath.Join(dir, "b-config"), filepath.Join(dir, "b-data"))
	if err != nil {
		t.Fatalf("resolve paths: %v", err)
	}
	if err := pathsB.EnsureDirs(); err != nil {
		t.Fatalf("ensure dirs: %v", err)
	}
	identityB, _, err := config.LoadOrCreateIdentityKey(pathsB.IdentityKeyFile())
	if err != nil {
		t.Fatalf("load identity B: %v", err)
	}

	pathsA, err := config.ResolvePaths(filepath.Join(dir, "a-config"), filepath.Join(dir, "a-data"))
	if err != nil {
		t.Fatalf("resolve paths: %v", err)
	}
	if err := pathsA.EnsureDirs(); err != nil {
		t.Fatalf("ensure dirs: %v", err)
	}
	identityA, _, err := config.LoadOrCreateIdentityKey(pathsA.IdentityKeyFile())
	if err != nil {
		t.Fatalf("load identity A: %v", err)
	}

	addrA := newPipeAddr(t)
	tokenA, err := config.EncodeToken(addrA, identityA.Public(), "ghostA")
	if err != nil {
		t.Fatalf("encode token A: %v", err)
	}
	addrB := newPipeAddr(t)
	tokenB, err := config.EncodeToken(addrB, identityB.Public(), "nodeB")
	if err != nil {
		t.Fatalf("encode token B: %v", err)
	}

	clock := newFakeClock(time.Unix(0, 0))

	cfgB := config.Default()
	cfgB.NodeName = "nodeB"
	cfgB.Peers = []config.Peer{{Name: "ghostA", Token: tokenA, Enabled: true}}

	nodeB, err := Open(context.Background(), Options{
		Paths:     pathsB,
		Config:    cfgB,
		Identity:  identityB,
		Transport: transport.NewPipeTransport(addrB),
		Logger:    log.New(io.Discard, "", 0),
		Clock:     clock,
	})
	if err != nil {
		t.Fatalf("open nodeB: %v", err)
	}
	t.Cleanup(func() { nodeB.Close() })

	// First attempt happens immediately (no delay before the very first
	// dial): wait for it to fail and for Supervisor to register its first
	// backoff wait (isBackoffDelay excludes the trash janitor's own,
	// much coarser, wait on this same shared clock — see fakeClock's doc
	// comment).
	if !clock.waitForWaiterMatching(isBackoffDelay, 5*time.Second) {
		t.Fatalf("timed out waiting for the first backoff wait")
	}
	waitFor(t, 5*time.Second, func() bool {
		peers := nodeB.Status().Peers
		return len(peers) == 1 && peers[0].LastError != ""
	})

	// Advance through two more failed attempts, confirming the delay
	// grows each time (SPEC.md §2.2: 1s -> 2s -> 4s, unjittered here).
	for i := 0; i < 2; i++ {
		delays := clock.snapshotDelays(isBackoffDelay)
		last := delays[len(delays)-1]
		clock.Advance(last)
		if !clock.waitForWaiterMatching(isBackoffDelay, 5*time.Second) {
			t.Fatalf("timed out waiting for backoff wait #%d", i+2)
		}
	}
	delays := clock.snapshotDelays(isBackoffDelay)
	if len(delays) < 3 {
		t.Fatalf("expected at least 3 recorded backoff delays, got %d: %v", len(delays), delays)
	}
	for i := 1; i < len(delays); i++ {
		if delays[i] <= delays[i-1] {
			t.Fatalf("backoff delay did not grow: %v", delays)
		}
	}
	if state := nodeB.Status().Peers[0].State; state != ConnStateBackingOff {
		t.Fatalf("expected backing_off, got %s", state)
	}

	// Bring the peer online: nodeA listens at the address nodeB's peer
	// token already points to, and configures nodeB as its own peer so
	// the handshake succeeds regardless of which side's dial wins dedup.
	cfgA := config.Default()
	cfgA.NodeName = "nodeA"
	cfgA.Peers = []config.Peer{{Name: "nodeB", Token: tokenB, Enabled: true}}
	nodeA, err := Open(context.Background(), Options{
		Paths:         pathsA,
		Config:        cfgA,
		Identity:      identityA,
		Transport:     transport.NewPipeTransport(addrA),
		Logger:        log.New(io.Discard, "", 0),
		DisableJitter: true,
	})
	if err != nil {
		t.Fatalf("open nodeA: %v", err)
	}
	t.Cleanup(func() { nodeA.Close() })

	// Release nodeB's pending wait so its next dial attempt (now that
	// nodeA is listening) can succeed.
	pending := clock.snapshotDelays(isBackoffDelay)
	clock.Advance(pending[len(pending)-1])

	waitFor(t, 5*time.Second, func() bool { return peerConnected(nodeB) })
	if lastErr := nodeB.Status().Peers[0].LastError; lastErr != "" {
		t.Fatalf("expected LastError to be cleared on reconnect, got %q", lastErr)
	}
}

// TestConfigMutationLive exercises Node's live config mutation entry
// points against a running pair of nodes: AddShare starts watching (and
// indexing) a share immediately, RemoveSubscription stops that
// subscription's sync (a later offerer-side change no longer
// propagates), and RenameNode both persists to config.json and is
// reflected in Status.
func TestConfigMutationLive(t *testing.T) {
	ctx := context.Background()
	nodeA := newTestNode(t, "mutA")
	nodeB := newTestNode(t, "mutB")

	tokenA, tokenB := peerToken(t, nodeA), peerToken(t, nodeB)
	if _, err := nodeA.AddPeer("B", tokenB); err != nil {
		t.Fatalf("nodeA AddPeer: %v", err)
	}
	if _, err := nodeB.AddPeer("A", tokenA); err != nil {
		t.Fatalf("nodeB AddPeer: %v", err)
	}
	waitFor(t, 5*time.Second, func() bool { return peerConnected(nodeA) && peerConnected(nodeB) })

	// AddShare starts watching it: a pre-existing file is indexed by the
	// automatic initial scan Open/AddShare perform, with no explicit
	// RescanShare call from the test.
	shareDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(shareDir, "a.txt"), []byte("a"), 0o644); err != nil {
		t.Fatalf("seed share file: %v", err)
	}
	shareID, err := nodeA.AddShare(shareDir, "s1", config.PermissionReadWrite, false)
	if err != nil {
		t.Fatalf("AddShare: %v", err)
	}
	waitFor(t, 5*time.Second, func() bool {
		rows, err := nodeA.store.ListShare(ctx, shareID, false)
		return err == nil && len(rows) > 0
	})

	localDir := t.TempDir()
	if err := nodeB.AddSubscription(nodeA.PeerKey(), shareID, localDir, config.ModeMirror); err != nil {
		t.Fatalf("AddSubscription: %v", err)
	}
	waitFor(t, 5*time.Second, func() bool {
		_, err := os.Stat(filepath.Join(localDir, "a.txt"))
		return err == nil
	})

	// RemoveSubscription stops its session: a subsequent offerer-side
	// change must not reach the (now unsubscribed) local copy.
	if err := nodeB.RemoveSubscription(nodeA.PeerKey(), shareID); err != nil {
		t.Fatalf("RemoveSubscription: %v", err)
	}
	if err := os.WriteFile(filepath.Join(shareDir, "b.txt"), []byte("b"), 0o644); err != nil {
		t.Fatalf("write b.txt: %v", err)
	}
	if err := nodeA.RescanShare(ctx, shareID); err != nil {
		t.Fatalf("RescanShare: %v", err)
	}
	time.Sleep(200 * time.Millisecond) // give any incorrect propagation a chance to arrive
	if _, err := os.Stat(filepath.Join(localDir, "b.txt")); err == nil {
		t.Fatalf("file propagated to a removed subscription")
	}

	// RenameNode persists and is reflected in Status.
	if err := nodeA.RenameNode("renamed-A"); err != nil {
		t.Fatalf("RenameNode: %v", err)
	}
	if got := nodeA.Status().NodeName; got != "renamed-A" {
		t.Fatalf("Status().NodeName = %q, want %q", got, "renamed-A")
	}
	onDisk, err := config.Load(nodeA.paths.ConfigFile())
	if err != nil {
		t.Fatalf("reload config: %v", err)
	}
	if onDisk.NodeName != "renamed-A" {
		t.Fatalf("persisted NodeName = %q, want %q", onDisk.NodeName, "renamed-A")
	}
}

// TestLifecycleNoGoroutineLeak starts a node with an active share watcher
// and a peer that's actively (and, in this test, perpetually)
// dial-retrying, then closes it, and confirms goroutine count settles
// back to (approximately) where it started — Close leaves nothing
// running.
func TestLifecycleNoGoroutineLeak(t *testing.T) {
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	before := runtime.NumGoroutine()

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
	cfg.NodeName = "leak-test"

	n, err := Open(context.Background(), Options{
		Paths:         paths,
		Config:        cfg,
		Identity:      identity,
		Transport:     transport.NewPipeTransport(newPipeAddr(t)),
		Logger:        log.New(io.Discard, "", 0),
		DisableJitter: true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	shareDir := t.TempDir()
	if _, err := n.AddShare(shareDir, "s", config.PermissionReadWrite, false); err != nil {
		t.Fatalf("AddShare: %v", err)
	}

	ghostPub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate ghost key: %v", err)
	}
	ghostToken, err := config.EncodeToken("pipe-nowhere", ghostPub, "ghost")
	if err != nil {
		t.Fatalf("encode ghost token: %v", err)
	}
	if _, err := n.AddPeer("ghost", ghostToken); err != nil {
		t.Fatalf("AddPeer: %v", err)
	}

	time.Sleep(20 * time.Millisecond) // let the watcher/supervisor goroutines actually start

	if err := n.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	var after int
	ok := false
	for i := 0; i < 100; i++ {
		runtime.GC()
		after = runtime.NumGoroutine()
		if after <= before+1 {
			ok = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !ok {
		t.Fatalf("goroutine leak: before=%d after=%d", before, after)
	}
}

package core

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nickmarrone/syncat/internal/config"
	"github.com/nickmarrone/syncat/internal/protocol"
	"github.com/nickmarrone/syncat/internal/transport"
)

// --- test harness ---------------------------------------------------------

var pipeAddrCounter int64

func newPipeAddr(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("pipe-%s-%d", t.Name(), atomic.AddInt64(&pipeAddrCounter, 1))
}

// newTestNode builds and opens a fully running Node over a fresh
// PipeTransport (SPEC.md §10's loopback stand-in for tailcat), with its
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

type startFailureTransport struct {
	transport.Transport
	closed atomic.Bool
}

func (t *startFailureTransport) Start(context.Context, func(net.Conn)) error {
	return errors.New("injected start failure")
}
func (t *startFailureTransport) Close() error {
	t.closed.Store(true)
	return nil
}

func TestOpenClosesTransportAfterStartFailure(t *testing.T) {
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
	tr := &startFailureTransport{}
	_, err = Open(context.Background(), Options{
		Paths: paths, Config: config.Default(), Identity: identity,
		Transport: tr, Logger: log.New(io.Discard, "", 0),
	})
	if err == nil {
		t.Fatal("Open succeeded despite transport startup failure")
	}
	if !tr.closed.Load() {
		t.Fatal("Open did not close transport after its Start returned an error")
	}
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
// A single Options.Clock drives every timer Node starts (see core.Clock's
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

func TestInboundHandshakeLimitRejectsExcessConnection(t *testing.T) {
	node := newTestNode(t, "handshake-limit")
	clients := make([]net.Conn, 0, maxInboundHandshakes)
	for i := 0; i < maxInboundHandshakes; i++ {
		server, client := net.Pipe()
		clients = append(clients, client)
		node.onAccept(server)
	}
	t.Cleanup(func() {
		for _, conn := range clients {
			_ = conn.Close()
		}
	})
	waitFor(t, 2*time.Second, func() bool { return len(node.inboundHandshakes) == maxInboundHandshakes })

	server, client := net.Pipe()
	defer client.Close()
	node.onAccept(server)
	_ = client.SetReadDeadline(time.Now().Add(time.Second))
	var one [1]byte
	if _, err := client.Read(one[:]); !errors.Is(err, io.EOF) {
		t.Fatalf("excess inbound connection read error = %v, want immediate EOF", err)
	}
}

func TestInboundHandshakeLimitIsFairPerPeer(t *testing.T) {
	n := &Node{inboundPeerCounts: make(map[string]int)}
	for i := 0; i < maxInboundHandshakesPerPeer; i++ {
		if !n.acquirePeerHandshake("peer-a") {
			t.Fatalf("peer A handshake %d was rejected before its limit", i)
		}
	}
	if n.acquirePeerHandshake("peer-a") {
		t.Fatal("peer A exceeded its per-peer handshake limit")
	}
	if !n.acquirePeerHandshake("peer-b") {
		t.Fatal("peer A exhausted peer B's independent handshake budget")
	}
	for i := 0; i < maxInboundHandshakesPerPeer; i++ {
		n.releasePeerHandshake("peer-a")
	}
	if !n.acquirePeerHandshake("peer-a") {
		t.Fatal("released peer handshake capacity was not reusable")
	}
}

// TestApprovalRequiredFailsClosed verifies that requesting a protected share
// records a pending decision without provisioning or syncing it, and that an
// explicit grant activates the already-open connection.
func TestApprovalRequiredFailsClosed(t *testing.T) {
	nodeA := newTestNode(t, "approvalA")
	nodeB := newTestNode(t, "approvalB")

	shareDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(shareDir, "secret.txt"), []byte("classified"), 0o644); err != nil {
		t.Fatalf("seed protected share: %v", err)
	}
	shareID, err := nodeA.AddShare(shareDir, "protected", config.PermissionReadOnly, true)
	if err != nil {
		t.Fatalf("AddShare: %v", err)
	}
	if _, err := nodeA.AddPeer("approvalB", peerToken(t, nodeB)); err != nil {
		t.Fatalf("nodeA AddPeer: %v", err)
	}
	if _, err := nodeB.AddPeer("approvalA", peerToken(t, nodeA)); err != nil {
		t.Fatalf("nodeB AddPeer: %v", err)
	}
	waitFor(t, 5*time.Second, func() bool { return peerConnected(nodeA) && peerConnected(nodeB) })

	localDir := t.TempDir()
	if err := nodeB.AddSubscription(nodeA.PeerKey(), shareID, localDir, config.ModeReceiveOnly); err != nil {
		t.Fatalf("AddSubscription: %v", err)
	}
	waitFor(t, 5*time.Second, func() bool {
		for _, sub := range nodeB.Status().Subscriptions {
			if sub.ShareID == shareID && sub.Access == protocol.AccessPending {
				return true
			}
		}
		return false
	})
	if _, err := os.Stat(filepath.Join(localDir, "secret.txt")); !os.IsNotExist(err) {
		t.Fatalf("protected file was available before approval; stat error = %v", err)
	}
	shares := nodeA.Status().Shares
	if len(shares) != 1 || len(shares[0].Access) != 1 || shares[0].Access[0].Access != protocol.AccessPending {
		t.Fatalf("offerer access = %+v, want one pending request", shares)
	}

	if err := nodeA.SetShareAccess(shareID, nodeB.PeerKey(), protocol.AccessGranted); err != nil {
		t.Fatalf("approve share: %v", err)
	}
	waitFor(t, 5*time.Second, func() bool {
		data, err := os.ReadFile(filepath.Join(localDir, "secret.txt"))
		return err == nil && string(data) == "classified"
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

// TestAddPeerRejectsOwnToken covers the mistake that motivated the check:
// the peering flow is symmetric ("run `syncat token` on each node, paste
// each into the other"), so pasting a node's own token back into itself is
// easy to do and used to be accepted silently. The resulting peer could
// never connect — the node dialed its own tailcat server, handshook with
// itself, and then lost dedup forever because KeepConnection compared its
// key against itself — while reporting only "connecting", with no error.
func TestAddPeerRejectsOwnToken(t *testing.T) {
	n := newTestNode(t, "solo")

	if _, err := n.AddPeer("myself", peerToken(t, n)); err == nil {
		t.Fatal("AddPeer accepted this node's own token; want an error")
	} else if !strings.Contains(err.Error(), "own token") {
		t.Errorf("AddPeer error = %q, want it to mention the token being this node's own", err)
	}

	if peers := n.Status().Peers; len(peers) != 0 {
		t.Errorf("Status().Peers = %d entries after the rejected AddPeer, want 0", len(peers))
	}
	if subs := n.Status().Subscriptions; len(subs) != 0 {
		t.Errorf("Status().Subscriptions = %d entries, want 0", len(subs))
	}
}

// TestOpenDoesNotDialOwnToken covers the same misconfiguration arriving
// from disk rather than through AddPeer — a config.json written before that
// check existed, or edited by hand. The peer must stay listed (it is still
// in config.json, and hiding it would make it unremovable through the UI)
// but must never be dialed, and must say why.
func TestOpenDoesNotDialOwnToken(t *testing.T) {
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

	addr := newPipeAddr(t)
	ownToken, err := config.EncodeToken(addr, identity.Public(), "self")
	if err != nil {
		t.Fatalf("encode own token: %v", err)
	}

	cfg := config.Default()
	cfg.NodeName = "self"
	cfg.Peers = []config.Peer{{Name: "self", Token: ownToken, Enabled: true}}

	n, err := Open(context.Background(), Options{
		Paths:     paths,
		Config:    cfg,
		Identity:  identity,
		Transport: transport.NewPipeTransport(addr),
		Logger:    log.New(io.Discard, "", 0),
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { n.Close() })

	peers := n.Status().Peers
	if len(peers) != 1 {
		t.Fatalf("Status().Peers = %d entries, want the self-peer to stay listed", len(peers))
	}
	if peers[0].State == ConnStateConnected {
		t.Errorf("self-peer state = %q, want it never to connect", peers[0].State)
	}
	if !strings.Contains(peers[0].LastError, "own token") {
		t.Errorf("self-peer LastError = %q, want it to explain the own-token misconfiguration", peers[0].LastError)
	}

	// It must remain removable through the normal path.
	if err := n.RemovePeer(n.PeerKey()); err != nil {
		t.Fatalf("RemovePeer on the self-peer: %v", err)
	}
	if peers := n.Status().Peers; len(peers) != 0 {
		t.Errorf("Status().Peers = %d entries after removal, want 0", len(peers))
	}
}

// TestRemovePeerDropsSubscriptions confirms that removing a peer also
// removes every subscription to that peer's shares. A subscription names
// its offering peer, so one left behind could never sync again — it would
// just sit in `syncat status` forever with a watcher still running. The
// local copy of the files must survive, matching RemoveSubscription:
// removal unsubscribes, it does not delete the user's data.
func TestRemovePeerDropsSubscriptions(t *testing.T) {
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

	// A second subscription, to a share offered by a *different* peer,
	// must be left completely alone. The other peer has to be genuinely
	// configured (rather than a made-up key) because AddSubscription now
	// rejects a peer it doesn't have — see TestAddSubscriptionRejectsUnknownPeer.
	nodeC := newTestNode(t, "nodeC")
	if _, err := nodeB.AddPeer("nodeC", peerToken(t, nodeC)); err != nil {
		t.Fatalf("nodeB AddPeer nodeC: %v", err)
	}
	otherDir := t.TempDir()
	if err := nodeB.AddSubscription(nodeC.PeerKey(), "othershare", otherDir, config.ModeMirror); err != nil {
		t.Fatalf("AddSubscription for the unrelated peer: %v", err)
	}

	if err := nodeB.RemovePeer(nodeA.PeerKey()); err != nil {
		t.Fatalf("RemovePeer: %v", err)
	}

	subs := nodeB.Status().Subscriptions
	if len(subs) != 1 {
		t.Fatalf("Subscriptions after RemovePeer = %d, want only the unrelated peer's to remain: %+v", len(subs), subs)
	}
	if subs[0].ShareID != "othershare" {
		t.Errorf("surviving subscription = %q, want the unrelated peer's %q", subs[0].ShareID, "othershare")
	}

	// The removed subscription's watcher is gone...
	nodeB.sharesMu.Lock()
	_, stillWatched := nodeB.shareWatches[shareID]
	nodeB.sharesMu.Unlock()
	if stillWatched {
		t.Errorf("share watch for %s still running after its peer was removed", shareID)
	}

	// ...but the files it had already synced are still on disk.
	data, err := os.ReadFile(filepath.Join(localDir, "hello.txt"))
	if err != nil {
		t.Fatalf("synced file was removed along with the subscription: %v", err)
	}
	if string(data) != "hello from A" {
		t.Errorf("synced file contents = %q, want it left untouched", data)
	}
}

// TestDedupLossIsReported covers the second silent failure mode from the
// same investigation: a peer whose dial authenticates but always loses
// SPEC.md §2.4's dedup rule, with the peer's own (winning) dial never
// arriving. Losing dedup is deliberately not an error — Supervisor retries
// with no backoff — so this used to look exactly like a peer that was
// simply offline: state "connecting", LastError empty, nothing logged.
//
// The setup pins the outcome without depending on timing: `low` (the
// lower-keyed node, so its own dial always loses) is given `high`'s real
// address, while `high` is given `low`'s identity paired with a dead
// address. So `high` accepts and authenticates `low`'s dial — then drops
// it per dedup — but can never dial back.
func TestDedupLossIsReported(t *testing.T) {
	dir := t.TempDir()
	logs := &lockedBuffer{}

	open := func(name, addr string, peers []config.Peer, identity *config.IdentityKey, clock Clock) *Node {
		t.Helper()
		paths, err := config.ResolvePaths(filepath.Join(dir, name+"-config"), filepath.Join(dir, name+"-data"))
		if err != nil {
			t.Fatalf("resolve paths for %s: %v", name, err)
		}
		if err := paths.EnsureDirs(); err != nil {
			t.Fatalf("ensure dirs for %s: %v", name, err)
		}
		cfg := config.Default()
		cfg.NodeName = name
		cfg.Peers = peers
		n, err := Open(context.Background(), Options{
			Paths:     paths,
			Config:    cfg,
			Identity:  identity,
			Transport: transport.NewPipeTransport(addr),
			Logger:    log.New(logs, "", 0),
			Clock:     clock,
		})
		if err != nil {
			t.Fatalf("open %s: %v", name, err)
		}
		t.Cleanup(func() { n.Close() })
		return n
	}

	// Two identities, sorted so we know which side loses its own dial.
	id1, _, err := config.LoadOrCreateIdentityKey(filepath.Join(t.TempDir(), "id1"))
	if err != nil {
		t.Fatalf("identity 1: %v", err)
	}
	id2, _, err := config.LoadOrCreateIdentityKey(filepath.Join(t.TempDir(), "id2"))
	if err != nil {
		t.Fatalf("identity 2: %v", err)
	}
	lowID, highID := id1, id2
	if bytes.Compare(lowID.Public(), highID.Public()) > 0 {
		lowID, highID = highID, lowID
	}

	lowAddr, highAddr := newPipeAddr(t), newPipeAddr(t)
	highToken, err := config.EncodeToken(highAddr, highID.Public(), "high")
	if err != nil {
		t.Fatalf("encode high token: %v", err)
	}
	// low's identity, but an address nothing is listening on: high knows
	// low well enough to authenticate it, and can never dial it.
	lowTokenDeadAddr, err := config.EncodeToken(newPipeAddr(t), lowID.Public(), "low")
	if err != nil {
		t.Fatalf("encode low token: %v", err)
	}

	clock := newFakeClock(time.Unix(0, 0))
	open("high", highAddr, []config.Peer{{Name: "low", Token: lowTokenDeadAddr, Enabled: true}}, highID, clock)
	low := open("low", lowAddr, []config.Peer{{Name: "high", Token: highToken, Enabled: true}}, lowID, clock)

	pc := low.lookupPeer(hex.EncodeToString(highID.Public()))
	if pc == nil {
		t.Fatal("low has no peerConn for high")
	}

	// Drive the clock until the losses pile up. Advancing also fires
	// high's own backoff waits, which is fine — those dials fail and
	// change nothing here.
	deadline := time.Now().Add(15 * time.Second)
	for {
		pc.mu.Lock()
		losses, lastErr := pc.dedupLosses, pc.lastErr
		pc.mu.Unlock()
		if losses >= dedupLossWarnAfter && lastErr != "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d dedup losses recorded (want >=%d), lastErr=%q", losses, dedupLossWarnAfter, lastErr)
		}
		clock.Advance(dedupLossPause)
		time.Sleep(5 * time.Millisecond)
	}

	peers := low.Status().Peers
	if len(peers) != 1 {
		t.Fatalf("low Status().Peers = %d, want 1", len(peers))
	}
	if !strings.Contains(peers[0].LastError, "deduplication rule") {
		t.Errorf("LastError = %q, want it to explain the repeated dedup losses", peers[0].LastError)
	}
	// The state is still "connecting": the node genuinely is still trying,
	// and should be. Only the reason it never finishes is now visible.
	if peers[0].State == ConnStateConnected {
		t.Errorf("peer state = %q, want it not to be connected", peers[0].State)
	}
	if got := logs.String(); !strings.Contains(got, "without a connection being adopted") {
		t.Errorf("no diagnostic logged for the stuck peer; log was:\n%s", got)
	}
}

// lockedBuffer is a bytes.Buffer safe to read while a log.Logger writes to
// it from the node's background goroutines.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestRemovePeerRevokesShareAccess covers the other half of removing a
// peer: its entries in our own shares' access lists. config.Share.Access is
// keyed by raw public key and buildShareList reads it without checking that
// the key still belongs to a configured peer, so a leftover "granted" would
// silently restore that peer's access to every share the moment the same
// key was re-added — no approval, nothing surfaced. Other peers' grants on
// the same share must survive.
func TestRemovePeerRevokesShareAccess(t *testing.T) {
	nodeA := newTestNode(t, "nodeA")
	nodeB := newTestNode(t, "nodeB")

	shareID, err := nodeA.AddShare(t.TempDir(), "docs", config.PermissionReadWrite, false)
	if err != nil {
		t.Fatalf("AddShare: %v", err)
	}
	if _, err := nodeA.AddPeer("nodeB", peerToken(t, nodeB)); err != nil {
		t.Fatalf("AddPeer: %v", err)
	}

	otherPeerKey := "ff" + strings.Repeat("00", 31)
	if err := nodeA.SetShareAccess(shareID, nodeB.PeerKey(), protocol.AccessGranted); err != nil {
		t.Fatalf("SetShareAccess for nodeB: %v", err)
	}
	if err := nodeA.SetShareAccess(shareID, otherPeerKey, protocol.AccessGranted); err != nil {
		t.Fatalf("SetShareAccess for the unrelated peer: %v", err)
	}

	accessFor := func(peerKey string) (string, bool) {
		t.Helper()
		for _, s := range nodeA.Status().Shares {
			if s.ShareID != shareID {
				continue
			}
			for _, a := range s.Access {
				if a.PeerKey == peerKey {
					return a.Access, true
				}
			}
		}
		return "", false
	}

	if got, ok := accessFor(nodeB.PeerKey()); !ok || got != protocol.AccessGranted {
		t.Fatalf("nodeB access before removal = %q (present=%v), want %q", got, ok, protocol.AccessGranted)
	}

	if err := nodeA.RemovePeer(nodeB.PeerKey()); err != nil {
		t.Fatalf("RemovePeer: %v", err)
	}

	if got, ok := accessFor(nodeB.PeerKey()); ok {
		t.Errorf("removed peer still has %q access to share %s; want the entry gone", got, shareID)
	}
	if got, ok := accessFor(otherPeerKey); !ok || got != protocol.AccessGranted {
		t.Errorf("unrelated peer's access = %q (present=%v), want it untouched at %q", got, ok, protocol.AccessGranted)
	}

	// The grant must not come back if the same key is re-added — the whole
	// point of clearing it.
	if _, err := nodeA.AddPeer("nodeB-again", peerToken(t, nodeB)); err != nil {
		t.Fatalf("re-AddPeer: %v", err)
	}
	if got, ok := accessFor(nodeB.PeerKey()); ok {
		t.Errorf("re-added peer regained %q access without a new grant", got)
	}
}

// TestAddSubscriptionRejectsUnknownPeer covers the failure that motivated
// resolve.go's ref resolvers. The subscribe command used to take only a hex
// peer key while reading as though it took a display name, so "subscribe
// nishinomiya test ./test/" is the natural thing to type — and it was
// accepted verbatim. The resulting subscription was inert and silently so:
// lookupPeer found nothing, so no SubscribeRequest was ever sent, and
// requestSubscriptions never matched it on any later reconnect either.
// `syncat status` listed it with a blank peer and share name while no bytes
// moved, and nothing was ever logged. Names now resolve
// (TestAddSubscriptionResolvesNames); a name that matches nothing is an
// error rather than a silent write.
func TestAddSubscriptionRejectsUnknownPeer(t *testing.T) {
	node := newTestNode(t, "node")

	err := node.AddSubscription("nishinomiya", "test", t.TempDir(), config.ModeMirror)
	if err == nil {
		t.Fatal("AddSubscription naming a peer that does not exist succeeded; want an error")
	}
	if !strings.Contains(err.Error(), "peer") {
		t.Errorf("error = %v, want it to be about the peer", err)
	}
	if subs := node.Status().Subscriptions; len(subs) != 0 {
		t.Errorf("Subscriptions = %+v, want the rejected subscription not to be persisted", subs)
	}
}

// TestAddSubscriptionResolvesNames is the payoff: the two identifiers a
// user actually reads off `syncat peer ls` and `syncat remote ls` work
// directly, and are stored as their canonical ids.
func TestAddSubscriptionResolvesNames(t *testing.T) {
	nodeA, nodeB, shareID := connectedPairWithShare(t, "docs")

	if err := nodeB.AddSubscription("nodeA", "docs", t.TempDir(), config.ModeMirror); err != nil {
		t.Fatalf("AddSubscription by display name: %v", err)
	}

	subs := nodeB.Status().Subscriptions
	if len(subs) != 1 {
		t.Fatalf("Subscriptions = %+v, want exactly one", subs)
	}
	if subs[0].PeerKey != nodeA.PeerKey() {
		t.Errorf("PeerKey = %q, want the resolved key %q", subs[0].PeerKey, nodeA.PeerKey())
	}
	if subs[0].ShareID != shareID {
		t.Errorf("ShareID = %q, want the resolved id %q", subs[0].ShareID, shareID)
	}
}

// TestAddSubscriptionResolvesIDPrefix covers the git-style shorthand, which
// is the only ergonomic way to name a share whose display name is ambiguous
// or absent.
func TestAddSubscriptionResolvesIDPrefix(t *testing.T) {
	nodeA, nodeB, shareID := connectedPairWithShare(t, "docs")

	if err := nodeB.AddSubscription(nodeA.PeerKey()[:8], shareID[:6], t.TempDir(), config.ModeMirror); err != nil {
		t.Fatalf("AddSubscription by id prefix: %v", err)
	}
	subs := nodeB.Status().Subscriptions
	if len(subs) != 1 || subs[0].PeerKey != nodeA.PeerKey() || subs[0].ShareID != shareID {
		t.Fatalf("Subscriptions = %+v, want the prefixes resolved to full ids", subs)
	}
}

// TestAddSubscriptionRejectsUnknownShare is the peer-resolves-share-doesn't
// case. Only enforced once the peer has actually sent a ShareList; see
// resolveOfferedShareRef.
func TestAddSubscriptionRejectsUnknownShare(t *testing.T) {
	_, nodeB, shareID := connectedPairWithShare(t, "docs")

	err := nodeB.AddSubscription("nodeA", "nosuchshare", t.TempDir(), config.ModeMirror)
	if err == nil {
		t.Fatal("AddSubscription naming a share the peer does not offer succeeded; want an error")
	}
	// The error has to be actionable, so it names what is on offer.
	if !strings.Contains(err.Error(), shareID) || !strings.Contains(err.Error(), "docs") {
		t.Errorf("error = %v, want it to list the offered share %s (docs)", err, shareID)
	}
	if subs := nodeB.Status().Subscriptions; len(subs) != 0 {
		t.Errorf("Subscriptions = %+v, want the rejected subscription not to be persisted", subs)
	}
}

// TestAddSubscriptionRejectsAmbiguousName pins that a name matching two
// peers is refused rather than silently resolved to whichever happened to
// come first out of the map.
func TestAddSubscriptionRejectsAmbiguousName(t *testing.T) {
	nodeB := newTestNode(t, "nodeB")
	first, second := newTestNode(t, "first"), newTestNode(t, "second")
	if _, err := nodeB.AddPeer("twin", peerToken(t, first)); err != nil {
		t.Fatalf("AddPeer first: %v", err)
	}
	if _, err := nodeB.AddPeer("twin", peerToken(t, second)); err != nil {
		t.Fatalf("AddPeer second: %v", err)
	}

	err := nodeB.AddSubscription("twin", "whatever", t.TempDir(), config.ModeMirror)
	if err == nil {
		t.Fatal("AddSubscription with an ambiguous peer name succeeded; want an error")
	}
	if !strings.Contains(err.Error(), "ambiguous") {
		t.Errorf("error = %v, want it to say the name is ambiguous", err)
	}
	if !strings.Contains(err.Error(), first.PeerKey()) || !strings.Contains(err.Error(), second.PeerKey()) {
		t.Errorf("error = %v, want it to name both candidate keys", err)
	}
}

// TestRemoveSubscriptionOfOrphanedPeer is the constraint that keeps ref
// resolution from making a mess unfixable. A subscription whose peer is no
// longer configured cannot resolve — there is nothing to resolve against —
// and if that were an error it would be permanently stuck in config with no
// CLI able to remove it. resolveSubscriptionRef passes unresolvable refs
// through untouched precisely so these stay removable by their literal
// stored values.
func TestRemoveSubscriptionOfOrphanedPeer(t *testing.T) {
	nodeA, nodeB, shareID := connectedPairWithShare(t, "docs")

	if err := nodeB.AddSubscription("nodeA", "docs", t.TempDir(), config.ModeMirror); err != nil {
		t.Fatalf("AddSubscription: %v", err)
	}
	peerKey := nodeA.PeerKey()
	// Orphan it: drop the peer, keeping the subscription behind.
	nodeB.peersMu.Lock()
	delete(nodeB.peers, peerKey)
	nodeB.peersMu.Unlock()

	if err := nodeB.RemoveSubscription(peerKey, shareID); err != nil {
		t.Fatalf("RemoveSubscription for an orphaned peer: %v", err)
	}
	if subs := nodeB.Status().Subscriptions; len(subs) != 0 {
		t.Fatalf("Subscriptions = %+v, want the orphan removed", subs)
	}
}

// TestRemoveSubscriptionResolvesNames: unsubscribing takes the same refs
// subscribing does, or the pair would be unusable together.
func TestRemoveSubscriptionResolvesNames(t *testing.T) {
	_, nodeB, _ := connectedPairWithShare(t, "docs")

	if err := nodeB.AddSubscription("nodeA", "docs", t.TempDir(), config.ModeMirror); err != nil {
		t.Fatalf("AddSubscription: %v", err)
	}
	if err := nodeB.RemoveSubscription("nodeA", "docs"); err != nil {
		t.Fatalf("RemoveSubscription by display name: %v", err)
	}
	if subs := nodeB.Status().Subscriptions; len(subs) != 0 {
		t.Fatalf("Subscriptions = %+v, want none", subs)
	}
}

// TestGrantReannouncesShareList is the bug where `syncat subscription ls`
// said "granted" and files were flowing, while the UI's peer page still
// showed the same share as "no access". The two read different fields:
// a subscription's access comes from the AccessUpdate the offerer pushed,
// but a remote share's comes from the offerer's ShareList and nowhere
// else — and an access decision used to send only the AccessUpdate, so
// the ShareList the subscriber held stayed at its connect-time "none"
// until the next reconnect. A grant to a peer that has not subscribed at
// all is the same gap with no AccessUpdate to mask it: nothing reached
// the peer until it reconnected.
func TestGrantReannouncesShareList(t *testing.T) {
	nodeA, nodeB, shareID := connectedPairWithShare(t, "docs")

	remoteAccess := func() string {
		for _, rs := range nodeB.Status().RemoteShares {
			if rs.ShareID == shareID {
				return rs.Access
			}
		}
		return ""
	}
	if got := remoteAccess(); got != protocol.AccessNone {
		t.Fatalf("remote share access before any grant = %q, want %q", got, protocol.AccessNone)
	}

	// A grant with no subscription behind it: the ShareList is the only
	// way nodeB can hear about it.
	if err := nodeA.SetShareAccess(shareID, nodeB.PeerKey(), protocol.AccessGranted); err != nil {
		t.Fatalf("SetShareAccess: %v", err)
	}
	waitFor(t, 5*time.Second, func() bool { return remoteAccess() == protocol.AccessGranted })

	if err := nodeA.SetShareAccess(shareID, nodeB.PeerKey(), protocol.AccessRevoked); err != nil {
		t.Fatalf("SetShareAccess revoke: %v", err)
	}
	waitFor(t, 5*time.Second, func() bool { return remoteAccess() == protocol.AccessRevoked })

	// And the reported case: subscribing auto-grants, and the peer page
	// must follow along without waiting for a reconnect.
	if err := nodeB.AddSubscription("nodeA", "docs", t.TempDir(), config.ModeMirror); err != nil {
		t.Fatalf("AddSubscription: %v", err)
	}
	waitFor(t, 5*time.Second, func() bool { return remoteAccess() == protocol.AccessGranted })
}

// connectedPairWithShare returns two connected nodes, where nodeA offers a
// read-only share by the given name and nodeB has already received nodeA's
// ShareList — the precondition for resolving share names.
func connectedPairWithShare(t *testing.T, shareName string) (nodeA, nodeB *Node, shareID string) {
	t.Helper()
	nodeA = newTestNode(t, "nodeA")
	nodeB = newTestNode(t, "nodeB")

	shareID, err := nodeA.AddShare(t.TempDir(), shareName, config.PermissionReadOnly, false)
	if err != nil {
		t.Fatalf("AddShare: %v", err)
	}
	if _, err := nodeA.AddPeer("nodeB", peerToken(t, nodeB)); err != nil {
		t.Fatalf("nodeA AddPeer: %v", err)
	}
	if _, err := nodeB.AddPeer("nodeA", peerToken(t, nodeA)); err != nil {
		t.Fatalf("nodeB AddPeer: %v", err)
	}
	waitFor(t, 5*time.Second, func() bool {
		for _, rs := range nodeB.Status().RemoteShares {
			if rs.ShareID == shareID {
				return true
			}
		}
		return false
	})
	return nodeA, nodeB, shareID
}

// TestTrashResolvesShareRef covers the case that exposed how narrowly
// the ref resolvers had been wired in: `syncat share ls` prints a share's name
// right next to its id, but `syncat trash ls <name>` answered "share test is
// not a local share or subscription" — because trash resolution was exact-id
// only. Every ref form the rest of the CLI accepts has to work here too.
func TestTrashResolvesShareRef(t *testing.T) {
	node := newTestNode(t, "node")
	shareID, err := node.AddShare(t.TempDir(), "test", config.PermissionReadOnly, false)
	if err != nil {
		t.Fatalf("AddShare: %v", err)
	}

	for _, ref := range []string{"test", shareID, shareID[:6]} {
		if _, err := node.ListTrash(ref); err != nil {
			t.Errorf("ListTrash(%q): %v", ref, err)
		}
	}
	if _, err := node.ListTrash("nosuchshare"); err == nil {
		t.Error("ListTrash with an unknown ref succeeded; want an error")
	}
}

// TestTrashResolvesSubscriptionRef: the trash holds entries for subscribed
// shares too (files are trashed on whichever side deleted them), so trash
// refs resolve against the union of offered and subscribed shares — not just
// the ones this node offers.
func TestTrashResolvesSubscriptionRef(t *testing.T) {
	_, nodeB, shareID := connectedPairWithShare(t, "docs")
	if err := nodeB.AddSubscription("nodeA", "docs", t.TempDir(), config.ModeMirror); err != nil {
		t.Fatalf("AddSubscription: %v", err)
	}

	// nodeB offers nothing; "docs" is only reachable as a subscription, and
	// its name is only known because nodeA sent a ShareList.
	for _, ref := range []string{"docs", shareID, shareID[:6]} {
		if _, err := nodeB.ListTrash(ref); err != nil {
			t.Errorf("ListTrash(%q) on a subscribed share: %v", ref, err)
		}
	}
}

// TestShareMutatorsResolveRefs: every share-addressed mutator takes the same
// refs, or the CLI is inconsistent again in a different place.
func TestShareMutatorsResolveRefs(t *testing.T) {
	node := newTestNode(t, "node")
	shareID, err := node.AddShare(t.TempDir(), "docs", config.PermissionReadOnly, false)
	if err != nil {
		t.Fatalf("AddShare: %v", err)
	}

	if err := node.RenameShare("docs", "papers"); err != nil {
		t.Fatalf("RenameShare by name: %v", err)
	}
	if err := node.SetSharePermission("papers", config.PermissionReadWrite); err != nil {
		t.Fatalf("SetSharePermission by the new name: %v", err)
	}
	if err := node.SetShareApprovalRequired(shareID[:6], true); err != nil {
		t.Fatalf("SetShareApprovalRequired by id prefix: %v", err)
	}

	shares := node.Status().Shares
	if len(shares) != 1 || shares[0].Name != "papers" ||
		shares[0].Permission != config.PermissionReadWrite || !shares[0].ApprovalRequired {
		t.Fatalf("Shares = %+v, want one renamed rw share requiring approval", shares)
	}

	if err := node.RemoveShare("papers"); err != nil {
		t.Fatalf("RemoveShare by name: %v", err)
	}
	if shares := node.Status().Shares; len(shares) != 0 {
		t.Fatalf("Shares = %+v, want none", shares)
	}
}

// TestRemovePeerResolvesRef: peer removal takes a name too, and — because it
// resolves best-effort — an exact key still works even when resolution has
// nothing to work with.
func TestRemovePeerResolvesRef(t *testing.T) {
	nodeA := newTestNode(t, "nodeA")
	other := newTestNode(t, "other")
	if _, err := nodeA.AddPeer("bob", peerToken(t, other)); err != nil {
		t.Fatalf("AddPeer: %v", err)
	}
	if err := nodeA.RemovePeer("bob"); err != nil {
		t.Fatalf("RemovePeer by display name: %v", err)
	}
	if peers := nodeA.Status().Peers; len(peers) != 0 {
		t.Fatalf("Peers = %+v, want none", peers)
	}

	// An unresolvable ref must still produce the plain "not configured"
	// error rather than a resolver error, so the API keeps answering 404.
	if err := nodeA.RemovePeer("bob"); err == nil {
		t.Fatal("RemovePeer of an already-removed peer succeeded; want an error")
	} else if !strings.Contains(err.Error(), "is not configured") {
		t.Errorf("error = %v, want it to say the peer is not configured", err)
	}
}

// TestDialFailureIsLogged covers the silence that made a wedged peer so
// hard to diagnose. transport.Supervisor.Run does no logging of its own, so
// a peer that could not dial out wrote nothing to any log — its error
// reached `syncat status` as lastErr and nowhere else. In practice that
// meant the node at fault said nothing at all, while the *other* node
// filled its log with dedup-loss warnings about a dial that was never
// coming.
func TestDialFailureIsLogged(t *testing.T) {
	dir := t.TempDir()
	logs := &lockedBuffer{}

	identity, _, err := config.LoadOrCreateIdentityKey(filepath.Join(dir, "id"))
	if err != nil {
		t.Fatalf("identity: %v", err)
	}
	peerIdentity, _, err := config.LoadOrCreateIdentityKey(filepath.Join(dir, "peer-id"))
	if err != nil {
		t.Fatalf("peer identity: %v", err)
	}
	// A well-formed token for an address nothing is listening on: the peer
	// is configured and dialable in principle, and every dial fails.
	token, err := config.EncodeToken(newPipeAddr(t), peerIdentity.Public(), "ghost")
	if err != nil {
		t.Fatalf("encode token: %v", err)
	}

	paths, err := config.ResolvePaths(filepath.Join(dir, "config"), filepath.Join(dir, "data"))
	if err != nil {
		t.Fatalf("resolve paths: %v", err)
	}
	if err := paths.EnsureDirs(); err != nil {
		t.Fatalf("ensure dirs: %v", err)
	}
	cfg := config.Default()
	cfg.NodeName = "node"
	cfg.Peers = []config.Peer{{Name: "ghost", Token: token, Enabled: true}}

	node, err := Open(context.Background(), Options{
		Paths: paths, Config: cfg, Identity: identity,
		Transport: transport.NewPipeTransport(newPipeAddr(t)),
		Logger:    log.New(logs, "", 0), DisableJitter: true,
	})
	if err != nil {
		t.Fatalf("open node: %v", err)
	}
	t.Cleanup(func() { node.Close() })

	// The very first failure logs; no need to wait out a backoff schedule.
	waitFor(t, 5*time.Second, func() bool {
		return strings.Contains(logs.String(), "dial failed (1 in a row)")
	})

	peers := node.Status().Peers
	if len(peers) != 1 {
		t.Fatalf("Peers = %d, want 1", len(peers))
	}
	if peers[0].LastError == "" {
		t.Error("LastError is empty; the failure must still reach `syncat status` too")
	}
	if peers[0].State != ConnStateBackingOff {
		t.Errorf("state = %q, want %q", peers[0].State, ConnStateBackingOff)
	}
}

// deadlineProbeTransport records whether Dial was handed a context with a
// deadline, then fails the dial.
type deadlineProbeTransport struct {
	transport.Transport
	mu       sync.Mutex
	dialed   bool
	deadline time.Duration
	hadOne   bool
}

func (t *deadlineProbeTransport) Dial(ctx context.Context, addr string) (net.Conn, error) {
	deadline, ok := ctx.Deadline()
	t.mu.Lock()
	t.dialed = true
	t.hadOne = ok
	if ok {
		t.deadline = time.Until(deadline)
	}
	t.mu.Unlock()
	return nil, errors.New("probe: dial refused")
}

// TestDialIsBounded pins that a dial attempt carries a deadline.
//
// Supervisor.Run hands dialAttempt the peer's own long-lived context, so
// before dialTimeout existed a dial had no deadline at all — and a dial into
// a half-dead tailcat tunnel does not fail, it hangs: WireGuard retries its
// handshake indefinitely while netstack retransmits the SYN behind it.
//
// That stalls every recovery path, because all of them are driven by a dial
// *failing*. A hung dial never reaches the backoff schedule, never counts a
// failure, and never reaches TailcatTransport.discardClient — the thing that
// rebuilds the stale per-peer Client after a peer restarts. Observed as a
// peer that stayed disconnected indefinitely after the *other* node
// restarted, with its WireGuard handshake retrying forever in the log.
//
// Asserting on the deadline rather than on elapsed time keeps this a
// millisecond test instead of a 30-second one.
func TestDialIsBounded(t *testing.T) {
	dir := t.TempDir()
	identity, _, err := config.LoadOrCreateIdentityKey(filepath.Join(dir, "id"))
	if err != nil {
		t.Fatalf("identity: %v", err)
	}
	peerIdentity, _, err := config.LoadOrCreateIdentityKey(filepath.Join(dir, "peer-id"))
	if err != nil {
		t.Fatalf("peer identity: %v", err)
	}
	token, err := config.EncodeToken(newPipeAddr(t), peerIdentity.Public(), "ghost")
	if err != nil {
		t.Fatalf("encode token: %v", err)
	}

	paths, err := config.ResolvePaths(filepath.Join(dir, "config"), filepath.Join(dir, "data"))
	if err != nil {
		t.Fatalf("resolve paths: %v", err)
	}
	if err := paths.EnsureDirs(); err != nil {
		t.Fatalf("ensure dirs: %v", err)
	}
	cfg := config.Default()
	cfg.NodeName = "node"
	cfg.Peers = []config.Peer{{Name: "ghost", Token: token, Enabled: true}}

	probe := &deadlineProbeTransport{Transport: transport.NewPipeTransport(newPipeAddr(t))}
	node, err := Open(context.Background(), Options{
		Paths: paths, Config: cfg, Identity: identity, Transport: probe,
		Logger: log.New(io.Discard, "", 0), DisableJitter: true,
	})
	if err != nil {
		t.Fatalf("open node: %v", err)
	}
	t.Cleanup(func() { node.Close() })

	waitFor(t, 5*time.Second, func() bool {
		probe.mu.Lock()
		defer probe.mu.Unlock()
		return probe.dialed
	})

	probe.mu.Lock()
	hadOne, remaining := probe.hadOne, probe.deadline
	probe.mu.Unlock()
	if !hadOne {
		t.Fatal("Dial got a context with no deadline; a dial that hangs would wedge the peer forever")
	}
	if remaining <= 0 || remaining > dialTimeout {
		t.Errorf("dial deadline is %v away, want (0, %v]", remaining, dialTimeout)
	}
}

// discardSpyTransport records DiscardPeer calls, then delegates.
type discardSpyTransport struct {
	transport.Transport
	mu        sync.Mutex
	discarded []string
}

func (t *discardSpyTransport) DiscardPeer(addr string) {
	t.mu.Lock()
	t.discarded = append(t.discarded, addr)
	t.mu.Unlock()
	t.Transport.DiscardPeer(addr)
}

func (t *discardSpyTransport) sawDiscard(addr string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, a := range t.discarded {
		if a == addr {
			return true
		}
	}
	return false
}

// TestPeerRedialDropsStaleSession covers how a node learns its connection
// died without waiting out SPEC.md §4's 90s dead rule.
//
// A peer only dials when it holds no session (dialAttempt short-circuits
// otherwise), so an authenticated inbound connection from a peer we believe
// we are already connected to means our side is a corpse — and the peer
// just proved it is alive by completing the handshake. The cost of not
// noticing falls entirely on whichever node the dedup rule made responsible
// for dialling the pairing, since the other node's dials are rejected here
// by design and can never repair it alone.
//
// The tear-down must also discard the transport's cached per-peer state, or
// the redial reuses a tailcat Client that announces itself to the peer's
// server only once and hangs dialling into a tunnel that has forgotten it.
func TestPeerRedialDropsStaleSession(t *testing.T) {
	dir := t.TempDir()
	clock := newFakeClock(time.Unix(0, 0))

	id1, _, err := config.LoadOrCreateIdentityKey(filepath.Join(dir, "id1"))
	if err != nil {
		t.Fatalf("identity 1: %v", err)
	}
	id2, _, err := config.LoadOrCreateIdentityKey(filepath.Join(dir, "id2"))
	if err != nil {
		t.Fatalf("identity 2: %v", err)
	}
	lowID, highID := id1, id2
	if bytes.Compare(lowID.Public(), highID.Public()) > 0 {
		lowID, highID = highID, lowID
	}
	lowAddr, highAddr := newPipeAddr(t), newPipeAddr(t)
	lowToken, err := config.EncodeToken(lowAddr, lowID.Public(), "low")
	if err != nil {
		t.Fatalf("encode low token: %v", err)
	}
	highToken, err := config.EncodeToken(highAddr, highID.Public(), "high")
	if err != nil {
		t.Fatalf("encode high token: %v", err)
	}

	logs := &lockedBuffer{}
	open := func(name, addr string, peers []config.Peer, identity *config.IdentityKey, tr transport.Transport) *Node {
		t.Helper()
		paths, err := config.ResolvePaths(filepath.Join(dir, name+"-config"), filepath.Join(dir, name+"-data"))
		if err != nil {
			t.Fatalf("resolve paths for %s: %v", name, err)
		}
		if err := paths.EnsureDirs(); err != nil {
			t.Fatalf("ensure dirs for %s: %v", name, err)
		}
		cfg := config.Default()
		cfg.NodeName = name
		cfg.Peers = peers
		n, err := Open(context.Background(), Options{
			Paths: paths, Config: cfg, Identity: identity, Transport: tr,
			Logger: log.New(logs, "", 0), Clock: clock,
		})
		if err != nil {
			t.Fatalf("open %s: %v", name, err)
		}
		t.Cleanup(func() { n.Close() })
		return n
	}

	// "high" is the node the dedup rule makes responsible for dialling, so
	// it is the one that pays for a slow recovery in production. Open
	// "low" first so high's very first dial finds something listening: a
	// failed dial would back off on the fake clock, which this test never
	// advances until it means to.
	open("low", lowAddr, []config.Peer{{Name: "high", Token: highToken, Enabled: true}}, lowID, transport.NewPipeTransport(lowAddr))
	spy := &discardSpyTransport{Transport: transport.NewPipeTransport(highAddr)}
	high := open("high", highAddr, []config.Peer{{Name: "low", Token: lowToken, Enabled: true}}, highID, spy)

	pc := high.lookupPeer(hex.EncodeToString(lowID.Public()))
	if pc == nil {
		t.Fatal("high has no peerConn for low")
	}
	waitFor(t, 5*time.Second, func() bool {
		pc.mu.Lock()
		defer pc.mu.Unlock()
		return pc.session != nil
	})

	// Drive the real path: an inbound connection from low, authenticated,
	// which the dedup rule rejects here because high owns this pairing's
	// dial. Calling offer rather than notePeerRedialed directly is what
	// makes this cover the wiring too.
	redial := func() {
		inbound, far := net.Pipe()
		defer far.Close()
		if outcome, err := pc.offer(context.Background(), inbound, &protocol.HandshakeResult{
			PeerPub: lowID.Public(), PeerName: "low",
		}, false); outcome == offerAdopted || err != nil {
			t.Fatalf("offer(inbound) = (%v, %v), want the dedup rule to reject it", outcome, err)
		}
	}

	// A connection this young is the simultaneous-start race, not a
	// restart: the peer's dial simply lost. It must be left alone.
	redial()
	pc.mu.Lock()
	original, addr := pc.session, pc.addr
	pc.mu.Unlock()
	if original == nil {
		t.Fatal("a redial inside the grace window tore down a healthy new connection")
	}

	// Older than the grace, the same signal means the peer restarted.
	clock.Advance(staleInboundGrace + time.Second)
	redial()

	// Assert on identity, not on nil: the supervisor redials the moment the
	// stale session is torn down, so over a pipe the nil window is far too
	// short to observe. What matters is that the corpse was replaced.
	waitFor(t, 5*time.Second, func() bool {
		pc.mu.Lock()
		defer pc.mu.Unlock()
		return pc.session != original
	})
	if !spy.sawDiscard(addr) {
		t.Error("the transport's cached state for the peer was not discarded; a redial would reuse a stale client")
	}
	if got := logs.String(); !strings.Contains(got, "so that connection is dead") {
		t.Errorf("nothing logged about dropping the dead connection; log was:\n%s", got)
	}
}

// TestConnectedPeerNeverReportsDialFailure pins the invariant that an
// adopted connection is the authoritative state of a peer: while a session
// is live, nothing the dial loop does may report that peer as anything
// other than connected, and nothing may leave a stale error on it.
//
// The bug this covers was a race, not a logic error, so it needs the
// repeats: two nodes that peer at the same instant both dial, and SPEC.md
// §2.4 keeps only one of the two connections. The winner closes the loser
// as soon as its own handshake completes — and when that close lands while
// the loser is still inside InitiateHandshake (its final SetDeadline is a
// favourite), the loser saw a hard error rather than a handshake it could
// then lose dedup on cleanly, and reported it via noteDialFailure. That
// overwrote the ConnStateConnected which offer had *already* set from the
// inbound connection that won.
//
// The result was sticky, which is what made it worth a test: the next
// dialAttempt pass sees a session and parks on connDone without ever
// touching state again, so a node that was connected and actively syncing
// reported "backing_off", with the teardown's error as its last_error, for
// the entire life of that healthy connection. It reproduced in roughly a
// third of setups, and was the reason TestRemovePeerDropsSubscriptions
// failed most of the time.
func TestConnectedPeerNeverReportsDialFailure(t *testing.T) {
	// Each round is a fresh pair racing to connect. One round proves
	// nothing; the loop is what makes the race show up.
	for round := range 15 {
		nodeA := newTestNode(t, "raceA")
		nodeB := newTestNode(t, "raceB")

		tokenA, tokenB := peerToken(t, nodeA), peerToken(t, nodeB)
		if _, err := nodeA.AddPeer("B", tokenB); err != nil {
			t.Fatalf("round %d: nodeA AddPeer: %v", round, err)
		}
		if _, err := nodeB.AddPeer("A", tokenA); err != nil {
			t.Fatalf("round %d: nodeB AddPeer: %v", round, err)
		}

		// Both sides must reach "connected" — with the bug, the side whose
		// own dial lost the race never leaves "backing_off", so this is
		// where the failure surfaces first.
		waitFor(t, 10*time.Second, func() bool { return peerConnected(nodeA) && peerConnected(nodeB) })

		for _, n := range []struct {
			name string
			node *Node
		}{{"nodeA", nodeA}, {"nodeB", nodeB}} {
			peers := n.node.Status().Peers
			if len(peers) != 1 {
				t.Fatalf("round %d: %s reports %d peers, want 1", round, n.name, len(peers))
			}
			p := peers[0]
			if p.State != ConnStateConnected {
				t.Errorf("round %d: %s reports peer state %q, want %q (last_error: %q)",
					round, n.name, p.State, ConnStateConnected, p.LastError)
			}
			// A connected peer with an error attached is the other half of
			// the same bug: the state can be repaired by a later transition
			// while the error it came with stays on display.
			if p.LastError != "" {
				t.Errorf("round %d: %s reports a connected peer carrying last_error %q, want it cleared",
					round, n.name, p.LastError)
			}
		}

		nodeA.Close()
		nodeB.Close()
	}
}

// TestDisabledPeerReportsDisconnected covers a peer that is configured but
// switched off. Its supervisor is never started, so nothing in the connect
// path ever runs for it — and nothing else set an initial state, so it
// reached `syncat peer ls` and the REST API as an empty string rather than
// as any of the four documented ConnStates.
func TestDisabledPeerReportsDisconnected(t *testing.T) {
	other := newTestNode(t, "disabledPeerRemote")
	token := peerToken(t, other)

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
	cfg.NodeName = "disabledPeerLocal"
	cfg.Peers = []config.Peer{{Name: "off", Token: token, Enabled: false}}

	n, err := Open(context.Background(), Options{
		Paths: paths, Config: cfg, Identity: identity,
		Transport: transport.NewPipeTransport(newPipeAddr(t)),
		Logger:    log.New(io.Discard, "", 0), DisableJitter: true,
	})
	if err != nil {
		t.Fatalf("open node: %v", err)
	}
	t.Cleanup(func() { n.Close() })

	peers := n.Status().Peers
	if len(peers) != 1 {
		t.Fatalf("Status().Peers = %d, want the disabled peer to still be listed", len(peers))
	}
	if peers[0].Enabled {
		t.Errorf("peer reports Enabled = true, want false")
	}
	if peers[0].State != ConnStateDisconnected {
		t.Errorf("disabled peer reports state %q, want %q", peers[0].State, ConnStateDisconnected)
	}
}

// TestConnectedSinceClearedOnDisconnect pins PeerStatus.ConnectedSince to
// its own documented contract — "when the *current* connection was
// established; zero otherwise". It used to keep the dead connection's
// timestamp after teardown, so anything rendering "connected for X" from it
// showed a duration that kept climbing for a peer that was gone.
// LastConnectedAt is the field that is meant to survive, and this checks it
// still does.
func TestConnectedSinceClearedOnDisconnect(t *testing.T) {
	nodeA := newTestNode(t, "sinceA")
	nodeB := newTestNode(t, "sinceB")

	tokenA, tokenB := peerToken(t, nodeA), peerToken(t, nodeB)
	if _, err := nodeA.AddPeer("B", tokenB); err != nil {
		t.Fatalf("nodeA AddPeer: %v", err)
	}
	if _, err := nodeB.AddPeer("A", tokenA); err != nil {
		t.Fatalf("nodeB AddPeer: %v", err)
	}
	waitFor(t, 10*time.Second, func() bool { return peerConnected(nodeA) && peerConnected(nodeB) })

	if got := nodeA.Status().Peers[0].ConnectedSince; got.IsZero() {
		t.Fatalf("ConnectedSince is zero while connected, want the connection's start time")
	}

	// Take nodeB away entirely, so nodeA's connection ends for a reason it
	// cannot immediately redial around.
	nodeB.Close()

	waitFor(t, 10*time.Second, func() bool {
		return nodeA.Status().Peers[0].State != ConnStateConnected
	})
	waitFor(t, 10*time.Second, func() bool {
		return nodeA.Status().Peers[0].ConnectedSince.IsZero()
	})
	if got := nodeA.Status().Peers[0].LastConnectedAt; got.IsZero() {
		t.Errorf("LastConnectedAt was cleared along with ConnectedSince, want it to survive the disconnect")
	}
}

// TestFanOutThroughHub covers SPEC.md §5's fan-out rule with the smallest
// topology that can show it: one share offered by a hub to two spokes.
//
// Two nodes are not enough. With a single peer, a change pulled from it is
// answered by the delta Session sends back at the end of handleIndexUpdate,
// and everything converges. Add a second peer and that delta reaches only
// the node the change came from. Nothing else was telling the other one:
// internal/core propagates a share when a *rescan* finds the working tree
// and the index disagree, and applying a pulled change updates both, so the
// scan that follows finds nothing and returns before it would propagate.
// The periodic rescan finds the same nothing, so it never healed either —
// a write on one spoke simply never reached the other, indefinitely.
func TestFanOutThroughHub(t *testing.T) {
	ctx := context.Background()
	hub := newTestNode(t, "hub")
	spokeA := newTestNode(t, "spokeA")
	spokeB := newTestNode(t, "spokeB")

	shareDir := t.TempDir()
	shareID, err := hub.AddShare(shareDir, "docs", config.PermissionReadWrite, false)
	if err != nil {
		t.Fatalf("AddShare: %v", err)
	}

	// Both spokes peer with the hub, and only with the hub — peers of the
	// same share never talk to each other.
	hubToken := peerToken(t, hub)
	for _, s := range []struct {
		name string
		node *Node
	}{{"spokeA", spokeA}, {"spokeB", spokeB}} {
		if _, err := hub.AddPeer(s.name, peerToken(t, s.node)); err != nil {
			t.Fatalf("hub AddPeer %s: %v", s.name, err)
		}
		if _, err := s.node.AddPeer("hub", hubToken); err != nil {
			t.Fatalf("%s AddPeer hub: %v", s.name, err)
		}
	}
	waitFor(t, 10*time.Second, func() bool {
		return len(hub.Status().Peers) == 2 &&
			hub.Status().Peers[0].State == ConnStateConnected &&
			hub.Status().Peers[1].State == ConnStateConnected &&
			peerConnected(spokeA) && peerConnected(spokeB)
	})

	dirA, dirB := t.TempDir(), t.TempDir()
	if err := spokeA.AddSubscription(hub.PeerKey(), shareID, dirA, config.ModeMirror); err != nil {
		t.Fatalf("spokeA AddSubscription: %v", err)
	}
	if err := spokeB.AddSubscription(hub.PeerKey(), shareID, dirB, config.ModeMirror); err != nil {
		t.Fatalf("spokeB AddSubscription: %v", err)
	}

	// A change at the hub reaches both spokes — this much always worked,
	// and is here so a failure below can't be blamed on the topology never
	// having come up.
	if err := os.WriteFile(filepath.Join(shareDir, "hub.txt"), []byte("from the hub"), 0o644); err != nil {
		t.Fatalf("write hub file: %v", err)
	}
	if err := hub.RescanShare(ctx, shareID); err != nil {
		t.Fatalf("RescanShare on hub: %v", err)
	}
	waitFor(t, 10*time.Second, func() bool {
		return fileHas(filepath.Join(dirA, "hub.txt"), "from the hub") &&
			fileHas(filepath.Join(dirB, "hub.txt"), "from the hub")
	})

	// The actual subject: a change on one spoke has to reach the other, and
	// the only path there is through the hub.
	if err := os.WriteFile(filepath.Join(dirA, "spoke.txt"), []byte("from spoke A"), 0o644); err != nil {
		t.Fatalf("write spoke file: %v", err)
	}
	if err := spokeA.RescanShare(ctx, shareID); err != nil {
		t.Fatalf("RescanShare on spokeA: %v", err)
	}
	waitFor(t, 10*time.Second, func() bool {
		return fileHas(filepath.Join(shareDir, "spoke.txt"), "from spoke A")
	})
	waitFor(t, 10*time.Second, func() bool {
		return fileHas(filepath.Join(dirB, "spoke.txt"), "from spoke A")
	})
}

func fileHas(path, want string) bool {
	data, err := os.ReadFile(path)
	return err == nil && string(data) == want
}

// Package core wires the syncat node together: lifecycle, peer manager,
// share registry and subscriptions. It must not import any UI, CLI or HTTP
// packages (SPEC.md §12).
//
// [Node] (node.go) owns startup/shutdown ordering and the accept path.
// mutations.go is the config-mutation API the REST layer and CLI call;
// resolve.go is the reference resolution that lets those callers pass
// names and id prefixes instead of raw hex. peer.go implements the
// per-peer dial/accept/dedup/keepalive state machine (SPEC.md §2, §4), and
// access.go the ShareList/SubscribeRequest/AccessUpdate negotiation that
// runs over an adopted connection (SPEC.md §6). shares.go wires local
// share/subscription directories to internal/index's Scanner/Watcher and
// drives internal/sync.Session on every local or remote change. status.go
// defines the read-only snapshot the API and UI render.
//
// Known MVP gaps, called out where they bite rather than left implicit:
//   - SPEC.md §2.3's pending-peer approval queue is deferred: an unknown
//     inbound key is rejected outright (see onAccept/handleAccept), though
//     the rejection is recorded (RejectedConnection) so the API and UI can
//     surface it.
//   - SPEC.md §6's share-access approval queue is deferred: every
//     SubscribeRequest is auto-granted regardless of ApprovalRequired (see
//     provisionShareForRequest's TODO in access.go) — ApprovalRequired is
//     still persisted in config for when that queue exists.
//   - Once granted, a share/subscription can only be "neutered" on an
//     already-open connection (both Direction flags set, see
//     peerConn.neuterShare in peer.go and its callers in mutations.go and
//     access.go) rather than fully removed, since internal/sync.Session
//     exposes no share-removal call. A fresh connection never re-adds a
//     removed/revoked share, so this converges on reconnect either way.
package core

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"sync"
	"time"

	"github.com/nickmarrone/syncat/internal/config"
	"github.com/nickmarrone/syncat/internal/index"
	"github.com/nickmarrone/syncat/internal/protocol"
	syncsvc "github.com/nickmarrone/syncat/internal/sync"
	"github.com/nickmarrone/syncat/internal/transport"
)

// Options configures Open. Paths and Transport are required; everything
// else has a documented default.
type Options struct {
	// Paths resolves config.json, the keys/db/trash directories (SPEC.md
	// §3). Required.
	Paths *config.Paths

	// Config, if non-nil, is used as the node's starting configuration
	// instead of loading (or defaulting) from Paths.ConfigFile(). Open
	// still immediately persists it to Paths.ConfigFile() (via
	// config.Save), so the on-disk file and the running node always agree
	// from the moment Open returns. Tests build a Config in memory and
	// pass it here rather than pre-seeding a config.json on disk.
	Config *config.Config

	// Identity, if non-nil, is used as the node's Ed25519 application
	// identity instead of loading-or-creating one at
	// Paths.IdentityKeyFile().
	Identity *config.IdentityKey

	// Transport carries the syncat protocol (SPEC.md §10): a
	// transport.TailcatTransport in production, a transport.PipeTransport
	// in tests. Required. Open calls Transport.Start; Close calls
	// Transport.Close.
	Transport transport.Transport

	// Logger receives Node's diagnostic logging; log.Default() if nil.
	Logger *log.Logger

	// Clock drives every timer Node starts (dial backoff, handshake
	// keepalive, share watchers, the trash janitor) — see Clock below.
	// RealClock if nil.
	Clock Clock

	// Rand supplies jitter for dial backoff (transport.Backoff.NextDelay).
	// If nil and DisableJitter is false, a time-seeded Rand is used
	// (production default). If nil and DisableJitter is true, backoff is
	// unjittered (deterministic — for tests that assert exact delays).
	Rand          *rand.Rand
	DisableJitter bool
}

// Node owns one running syncat instance end to end: config, identity,
// index store, transport, peer connections, share/subscription watchers,
// and the trash janitor. Construct with Open; shut down with Close.
//
// Every exported method is safe for concurrent use, including concurrently
// with the peer manager's own background activity — see mutations.go's
// file comment on lock ordering (config mutations always clone-mutate-
// swap under cfgMu; live effects are applied after cfgMu is released).
type Node struct {
	paths     *config.Paths
	identity  *config.IdentityKey
	transport transport.Transport
	store     *index.Store
	logger    *log.Logger
	clock     Clock
	rand      *rand.Rand
	trash     *syncsvc.Trash
	janitor   *syncsvc.Janitor

	startTime time.Time

	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	closeOnce sync.Once

	// closeMu/closing order every wg.Add strictly before Close's
	// wg.Wait — see goTracked.
	closeMu sync.RWMutex
	closing bool

	cfgMu sync.RWMutex
	cfg   *config.Config

	tokenMu sync.RWMutex
	token   string

	peersMu sync.Mutex
	peers   map[string]*peerConn // key: full Ed25519 pubkey, hex

	sharesMu     sync.Mutex
	shareWatches map[string]*shareWatch // key: share id

	rejectedMu sync.Mutex
	rejected   []RejectedConnection
}

// maxRejectedConnections bounds RejectedConnections' memory: only the most
// recent attempts are kept.
const maxRejectedConnections = 50

// Open loads (or accepts, via Options) config and identity, opens the
// index store, starts the transport, share watchers, trash janitor, and
// peer manager, and returns a fully running Node. The returned Node must
// be shut down with Close.
//
// Startup order matches the SPEC.md §1/§10 lifecycle: config+keys+paths,
// index store, transport (accepting connections from the moment Start
// returns), an initial synchronous scan of every configured share and
// non-paused subscription, the trash janitor, then the peer manager's
// dial supervisors.
func Open(ctx context.Context, opts Options) (*Node, error) {
	if opts.Paths == nil {
		return nil, errors.New("core: open: Paths is required")
	}
	if opts.Transport == nil {
		return nil, errors.New("core: open: Transport is required")
	}
	if err := opts.Paths.EnsureDirs(); err != nil {
		return nil, fmt.Errorf("core: open: %w", err)
	}
	in, err := loadOpenInputs(opts)
	if err != nil {
		return nil, err
	}

	store, err := index.Open(ctx, opts.Paths.DBFile())
	if err != nil {
		return nil, fmt.Errorf("core: open: %w", err)
	}

	nodeCtx, cancel := context.WithCancel(context.Background())

	n := &Node{
		paths:        opts.Paths,
		identity:     in.identity,
		transport:    opts.Transport,
		store:        store,
		logger:       in.logger,
		clock:        in.clock,
		rand:         in.rand,
		cfg:          in.cfg,
		peers:        map[string]*peerConn{},
		shareWatches: map[string]*shareWatch{},
		ctx:          nodeCtx,
		cancel:       cancel,
		startTime:    in.clock.Now(),
	}
	n.trash = syncsvc.NewTrash(opts.Paths.TrashDir(), asSyncClock(in.clock))
	n.janitor = syncsvc.NewJanitor(n.trash, time.Duration(in.cfg.TrashRetentionDays)*24*time.Hour, asJanitorClock(in.clock), 0, nil)
	n.janitor.Start(nodeCtx)

	// From here on a failure has to unwind what has already been started,
	// in this order (the transport only once Start has succeeded).
	ok, transportStarted := false, false
	cleanup := func() {
		n.janitor.Close()
		store.Close()
		if transportStarted {
			opts.Transport.Close()
		}
		cancel()
	}
	defer func() {
		if !ok {
			cleanup()
		}
	}()

	if err := opts.Transport.Start(nodeCtx, n.onAccept); err != nil {
		return nil, fmt.Errorf("core: open: start transport: %w", err)
	}
	transportStarted = true

	addr, err := opts.Transport.LocalAddress()
	if err != nil {
		return nil, fmt.Errorf("core: open: %w", err)
	}
	tok, err := config.EncodeToken(addr, in.identity.Public(), in.cfg.NodeName)
	if err != nil {
		return nil, fmt.Errorf("core: open: %w", err)
	}
	// Under tokenMu, not because anything else has started yet in Open,
	// but because Transport.Start above is already accepting: an inbound
	// connection landing in this window runs handleAccept, which reads
	// this field via localToken. (Such a connection sees an empty token
	// and is rejected by the peer's Hello validation; the peer's backoff
	// redials into a fully-initialised node. The token can't be built any
	// earlier — it embeds Transport.LocalAddress, which is only valid
	// once Start has returned.)
	n.tokenMu.Lock()
	n.token = tok
	n.tokenMu.Unlock()

	n.startConfiguredWatches(in.cfg)
	n.startConfiguredPeers(in.cfg)

	ok = true
	return n, nil
}

// openInputs is everything Open resolves from Options before it starts
// anything: the config (loaded or defaulted, then persisted), the
// identity, and the clock/rand/logger with their documented defaults
// applied.
type openInputs struct {
	cfg      *config.Config
	identity *config.IdentityKey
	clock    Clock
	rand     *rand.Rand
	logger   *log.Logger
}

// loadOpenInputs applies Options' documented defaults: it loads (or
// defaults) and persists the config, loads or creates the identity key,
// and falls back to RealClock, a time-seeded Rand (unless DisableJitter),
// and log.Default(). opts.Paths must already be validated and its
// directories ensured.
func loadOpenInputs(opts Options) (openInputs, error) {
	cfg := opts.Config
	if cfg == nil {
		loaded, err := config.Load(opts.Paths.ConfigFile())
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return openInputs{}, fmt.Errorf("core: open: %w", err)
			}
			loaded = config.Default()
		}
		cfg = loaded
	}
	cfg.ApplyDefaults()
	if err := config.Save(opts.Paths.ConfigFile(), cfg); err != nil {
		return openInputs{}, fmt.Errorf("core: open: persist config: %w", err)
	}

	identity := opts.Identity
	if identity == nil {
		id, _, err := config.LoadOrCreateIdentityKey(opts.Paths.IdentityKeyFile())
		if err != nil {
			return openInputs{}, fmt.Errorf("core: open: %w", err)
		}
		identity = id
	}

	clock := opts.Clock
	if clock == nil {
		clock = RealClock
	}
	rnd := opts.Rand
	if rnd == nil && !opts.DisableJitter {
		rnd = defaultRand()
	}
	logger := opts.Logger
	if logger == nil {
		logger = log.Default()
	}
	return openInputs{cfg: cfg, identity: identity, clock: clock, rand: rnd, logger: logger}, nil
}

// startConfiguredWatches starts a watcher and runs the initial synchronous
// scan for every share and every non-paused subscription in cfg. Failures
// are logged per entry rather than failing Open: one bad directory should
// not keep the rest of the node from starting.
func (n *Node) startConfiguredWatches(cfg *config.Config) {
	for _, s := range cfg.Shares {
		if _, err := n.startShareWatch(s.ID, s.Path); err != nil {
			n.logger.Printf("core: open: start watcher for share %s: %v", s.ID, err)
			continue
		}
		if err := n.rescanShare(n.ctx, s.ID); err != nil {
			n.logger.Printf("core: open: initial scan for share %s: %v", s.ID, err)
		}
	}
	for _, sub := range cfg.Subscriptions {
		if sub.Paused {
			continue
		}
		if _, err := n.startShareWatch(sub.ShareID, sub.LocalPath); err != nil {
			n.logger.Printf("core: open: start watcher for subscription %s: %v", sub.ShareID, err)
			continue
		}
		if err := n.rescanShare(n.ctx, sub.ShareID); err != nil {
			n.logger.Printf("core: open: initial scan for subscription %s: %v", sub.ShareID, err)
		}
	}
}

// startConfiguredPeers builds a peerConn for every peer in cfg, registers
// it, and starts the dial supervisor for each enabled one. A peer whose
// token cannot be parsed is logged and skipped.
func (n *Node) startConfiguredPeers(cfg *config.Config) {
	for _, p := range cfg.Peers {
		pc, err := newPeerConn(n, p)
		if err != nil {
			n.logger.Printf("core: open: configure peer %s: %v", p.Name, err)
			continue
		}
		n.peers[pc.peerKeyHex] = pc
		// AddPeer rejects our own token, but a config.json written before
		// that check existed (or edited by hand) can still carry one, and
		// dialing ourselves fails in a uniquely silent way: the handshake
		// succeeds against our own server, then loses dedup forever
		// because KeepConnection compares our key against itself. Register
		// the peer anyway — so it stays visible in `syncat peer ls` rather
		// than vanishing while it's still in config.json — but never dial.
		if pc.peerKeyHex == n.PeerKey() {
			n.logger.Printf("core: open: peer %q is configured with this node's own token; not dialing it. Run `syncat peer rm %s`, then add the other node's token.", p.Name, pc.peerKeyHex)
			pc.disable("configured with this node's own token; remove it and add the other node's token")
			continue
		}
		if p.Enabled {
			n.goTracked(pc.runSupervisor)
		}
	}
}

// Close shuts the node down in order: stop dialing/accepting new work
// (cancel the root context), wait for every peer connection and
// background goroutine Node started to finish tearing itself down, stop
// the share watchers and trash janitor, close the index store, and
// finally close the transport. Safe to call more than once; only the
// first call does anything.
func (n *Node) Close() error {
	var closeErr error
	n.closeOnce.Do(func() {
		n.cancel()
		// Refuse new tracked goroutines before waiting for the running
		// ones, so no wg.Add can race this Wait (see goTracked).
		n.closeMu.Lock()
		n.closing = true
		n.closeMu.Unlock()
		n.wg.Wait()

		n.sharesMu.Lock()
		watches := n.shareWatches
		n.shareWatches = map[string]*shareWatch{}
		n.sharesMu.Unlock()

		var errs []error
		for id, sw := range watches {
			if err := sw.watcher.Close(); err != nil {
				errs = append(errs, fmt.Errorf("close watcher %s: %w", id, err))
			}
		}
		if err := n.janitor.Close(); err != nil {
			errs = append(errs, err)
		}
		if err := n.store.Close(); err != nil {
			errs = append(errs, err)
		}
		if err := n.transport.Close(); err != nil {
			errs = append(errs, err)
		}
		closeErr = errors.Join(errs...)
	})
	return closeErr
}

// goTracked runs fn on a goroutine counted by n.wg, reporting false
// without running it if the node is already closing.
//
// The guard is what makes n.wg safe to Add to from goroutines the
// WaitGroup is not already counting. Three callers are like that:
// onAccept runs on a transport goroutine, and AddPeer and startShareWatch
// on API goroutines. (The rest are called from already-tracked goroutines,
// which keep the counter above zero and so cannot race a Wait.) Without
// the guard, such an Add can land after Close's wg.Wait has begun — the
// documented "Add that starts when the counter is zero must happen before
// Wait" misuse, which the race detector flags, and which really can leak a
// goroutine past Close and on into a closed store or transport.
//
// Holding closeMu for read across the Add, and taking it for write in
// Close before waiting, orders every Add strictly before the Wait.
// Deliberately *not* done by closing the transport before waiting, which
// would be simpler but would abandon in-flight transfers that SPEC.md §8's
// graceful shutdown exists to let finish.
func (n *Node) goTracked(fn func()) bool {
	n.closeMu.RLock()
	defer n.closeMu.RUnlock()
	if n.closing {
		return false
	}
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		fn()
	}()
	return true
}

// nodeName returns the current configured node display name.
func (n *Node) nodeName() string {
	n.cfgMu.RLock()
	defer n.cfgMu.RUnlock()
	return n.cfg.NodeName
}

// localToken returns this node's current sc1 token.
func (n *Node) localToken() string {
	n.tokenMu.RLock()
	defer n.tokenMu.RUnlock()
	return n.token
}

// PeerKey returns this node's own full Ed25519 public key, hex-encoded —
// the same identifier format used for every peer throughout Node's API.
func (n *Node) PeerKey() string {
	return hex.EncodeToString(n.identity.Public())
}

// --- inbound connections -------------------------------------------------

// onAccept is passed to Transport.Start; it's invoked in a new goroutine
// per inbound connection.
func (n *Node) onAccept(conn net.Conn) {
	if n.ctx.Err() != nil {
		conn.Close()
		return
	}
	// Unlike the other callers, this one owns a conn, so a refused
	// goTracked has to close it rather than drop it on the floor.
	if !n.goTracked(func() { n.handleAccept(conn) }) {
		conn.Close()
	}
}

// handleAccept authenticates one inbound connection and, on success, hands
// it to the matching peerConn's dedup/adopt logic (SPEC.md §2.4). An
// unknown or disabled peer key is rejected and recorded (SPEC.md §2.3's
// pending-peer queue is deferred — see the package doc comment).
func (n *Node) handleAccept(conn net.Conn) {
	var sawPub ed25519.PublicKey
	result, err := protocol.Handshake(n.ctx, conn, protocol.HandshakeConfig{
		IdentityKey: n.identity.Private,
		NodeName:    n.nodeName(),
		Token:       n.localToken(),
		IsKnownPeer: func(pub ed25519.PublicKey) bool {
			sawPub = append(ed25519.PublicKey(nil), pub...)
			return n.isKnownPeer(pub)
		},
	})
	if err != nil {
		if len(sawPub) > 0 {
			n.recordRejected(hex.EncodeToString(sawPub), "", err.Error())
		}
		conn.Close()
		return
	}

	pc := n.lookupPeer(hex.EncodeToString(result.PeerPub))
	if pc == nil {
		// IsKnownPeer said yes but the peer vanished from the map between
		// then and now (e.g. RemovePeer raced this handshake) — treat as
		// a rejection rather than panicking on a nil peerConn.
		n.recordRejected(hex.EncodeToString(result.PeerPub), result.PeerName, "peer removed during handshake")
		conn.Close()
		return
	}
	if _, err := pc.offer(n.ctx, conn, result, false); err != nil {
		n.logger.Printf("core: accept from %s: %v", pc.name, err)
	}
}

func (n *Node) isKnownPeer(pub ed25519.PublicKey) bool {
	pc := n.lookupPeer(hex.EncodeToString(pub))
	if pc == nil {
		return false
	}
	pc.mu.Lock()
	enabled := pc.enabled
	pc.mu.Unlock()
	return enabled
}

func (n *Node) lookupPeer(peerKeyHex string) *peerConn {
	n.peersMu.Lock()
	defer n.peersMu.Unlock()
	return n.peers[peerKeyHex]
}

func (n *Node) snapshotPeers() []*peerConn {
	n.peersMu.Lock()
	defer n.peersMu.Unlock()
	out := make([]*peerConn, 0, len(n.peers))
	for _, pc := range n.peers {
		out = append(out, pc)
	}
	return out
}

func (n *Node) recordRejected(peerKeyHex, peerName, reason string) {
	n.rejectedMu.Lock()
	n.rejected = append(n.rejected, RejectedConnection{
		PeerKey: peerKeyHex, PeerName: peerName, At: n.clock.Now(), Reason: reason,
	})
	if len(n.rejected) > maxRejectedConnections {
		n.rejected = n.rejected[len(n.rejected)-maxRejectedConnections:]
	}
	n.rejectedMu.Unlock()
}

// Clock abstracts wall-clock time for every timing-dependent piece Node
// wires together: dial backoff (transport.Backoff/Supervisor), handshake
// keepalive (protocol.Keepalive), share watchers (index.Watcher), and the
// trash janitor (sync.Janitor). Each of those packages defines its own
// narrow Clock interface (Now/After, or just After for transport.Clock) for
// leaf-package independence — Clock here has both methods, so a single
// value satisfies all of them structurally, letting a test drive every
// timer Node starts from one fake clock with no real sleeping.
//
// The zero value is not usable; use RealClock in production or a fake in
// tests.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

type realClock struct{}

func (realClock) Now() time.Time                         { return time.Now() }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// RealClock is the production Clock, backed by the time package.
var RealClock Clock = realClock{}

// asTransportClock adapts a Clock to transport.Clock (After only).
func asTransportClock(c Clock) transport.Clock { return transportClockAdapter{c} }

type transportClockAdapter struct{ c Clock }

func (a transportClockAdapter) After(d time.Duration) <-chan time.Time { return a.c.After(d) }

// asProtocolClock adapts a Clock to protocol.Clock (Now+After) — Clock
// already satisfies this interface structurally, but the helper keeps call
// sites self-documenting about which package's Clock is being supplied.
func asProtocolClock(c Clock) protocol.Clock { return c }

// asIndexClock adapts a Clock to index.Clock (Now+After).
func asIndexClock(c Clock) index.Clock { return c }

// asJanitorClock adapts a Clock to syncsvc.JanitorClock (Now+After).
func asJanitorClock(c Clock) syncsvc.JanitorClock { return c }

// asSyncClock adapts a Clock to syncsvc.Clock (a plain func() time.Time used
// only to timestamp conflict copies and trash entries).
func asSyncClock(c Clock) syncsvc.Clock { return c.Now }

// defaultRand returns a Rand seeded from the current time, for production
// backoff jitter. Tests pass their own *rand.Rand (or nil, for a
// deterministic un-jittered schedule) via Options.Rand.
func defaultRand() *rand.Rand {
	return rand.New(rand.NewSource(time.Now().UnixNano()))
}

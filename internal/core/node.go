// Package core wires the syncat node together: lifecycle, peer manager, share registry and subscriptions. It must not import any UI, CLI or HTTP packages (SPEC.md §12).
//
// Node (this file) owns startup/shutdown ordering and the config mutation
// entry points Phase 8's REST API calls. peer.go implements the per-peer
// dial/accept/dedup/keepalive state machine (SPEC.md §2, §4). shares.go
// wires local share/subscription directories to internal/index's
// Scanner/Watcher and drives internal/sync.Session on every local or
// remote change. status.go defines the read-only snapshot Phase 8/9
// render.
//
// Known MVP gaps, called out where they bite rather than left implicit:
//   - SPEC.md §2.3's pending-peer approval queue is deferred: an unknown
//     inbound key is rejected outright (see onAccept/handleAccept), though
//     the rejection is recorded (RejectedConnection) so Phase 8/9 can
//     surface it.
//   - SPEC.md §6's share-access approval queue is deferred: every
//     SubscribeRequest is auto-granted regardless of ApprovalRequired (see
//     handleSubscribeRequest's TODO) — ApprovalRequired is still persisted
//     in config for when that queue exists.
//   - Once granted, a share/subscription can only be "neutered" on an
//     already-open connection (both Direction flags set, see
//     neuterShareOnSessions and its call sites) rather than fully removed,
//     since internal/sync.Session exposes no share-removal call. A fresh
//     connection never re-adds a removed/revoked share, so this converges
//     on reconnect either way.
//   - Status.Transfers is always empty: internal/sync.Session doesn't
//     expose per-transfer byte-progress introspection yet (see status.go).
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
	// keepalive, share watchers, the trash janitor) — see clock.go.
	// RealClock if nil.
	Clock Clock

	// Rand supplies jitter for dial backoff (transport.Backoff.NextDelay).
	// If nil and DisableJitter is false, a time-seeded Rand is used
	// (production default). If nil and DisableJitter is true, backoff is
	// unjittered (deterministic — for tests that assert exact delays).
	Rand          *rand.Rand
	DisableJitter bool
}

// shareRole distinguishes a share we offer from a peer's share we
// subscribe to, for logging only — the sync mechanics (Direction, watcher,
// scanner) are identical either way (see shares.go).
type shareRole int

const (
	roleOffered shareRole = iota
	roleSubscription
)

// shareWatch is one local directory Node keeps indexed: either a share we
// offer (root = config.Share.Path) or a subscription's local copy (root =
// config.Subscription.LocalPath). See shares.go.
type shareWatch struct {
	shareID string
	root    string
	role    shareRole
	scanner *index.Scanner
	watcher *index.Watcher
}

// Node owns one running syncat instance end to end: config, identity,
// index store, transport, peer connections, share/subscription watchers,
// and the trash janitor. Construct with Open; shut down with Close.
//
// Every exported method is safe for concurrent use, including concurrently
// with the peer manager's own background activity — see the package doc
// comment's note on lock ordering (config mutations always clone-mutate-
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

	cfg := opts.Config
	if cfg == nil {
		loaded, err := config.Load(opts.Paths.ConfigFile())
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return nil, fmt.Errorf("core: open: %w", err)
			}
			loaded = config.Default()
		}
		cfg = loaded
	}
	cfg.ApplyDefaults()
	if err := config.Save(opts.Paths.ConfigFile(), cfg); err != nil {
		return nil, fmt.Errorf("core: open: persist config: %w", err)
	}

	identity := opts.Identity
	if identity == nil {
		id, _, err := config.LoadOrCreateIdentityKey(opts.Paths.IdentityKeyFile())
		if err != nil {
			return nil, fmt.Errorf("core: open: %w", err)
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

	store, err := index.Open(ctx, opts.Paths.DBFile())
	if err != nil {
		return nil, fmt.Errorf("core: open: %w", err)
	}

	nodeCtx, cancel := context.WithCancel(context.Background())

	n := &Node{
		paths:        opts.Paths,
		identity:     identity,
		transport:    opts.Transport,
		store:        store,
		logger:       logger,
		clock:        clock,
		rand:         rnd,
		cfg:          cfg,
		peers:        map[string]*peerConn{},
		shareWatches: map[string]*shareWatch{},
		ctx:          nodeCtx,
		cancel:       cancel,
		startTime:    clock.Now(),
	}
	n.trash = syncsvc.NewTrash(opts.Paths.TrashDir(), asSyncClock(clock))
	n.janitor = syncsvc.NewJanitor(n.trash, time.Duration(cfg.TrashRetentionDays)*24*time.Hour, asJanitorClock(clock), 0, nil)
	n.janitor.Start(nodeCtx)

	if err := opts.Transport.Start(nodeCtx, n.onAccept); err != nil {
		n.janitor.Close()
		store.Close()
		cancel()
		return nil, fmt.Errorf("core: open: start transport: %w", err)
	}

	addr, err := opts.Transport.LocalAddress()
	if err != nil {
		n.janitor.Close()
		store.Close()
		opts.Transport.Close()
		cancel()
		return nil, fmt.Errorf("core: open: %w", err)
	}
	tok, err := config.EncodeToken(addr, identity.Public(), cfg.NodeName)
	if err != nil {
		n.janitor.Close()
		store.Close()
		opts.Transport.Close()
		cancel()
		return nil, fmt.Errorf("core: open: %w", err)
	}
	n.token = tok

	for _, s := range cfg.Shares {
		if _, err := n.startShareWatch(s.ID, s.Path, roleOffered); err != nil {
			logger.Printf("core: open: start watcher for share %s: %v", s.ID, err)
			continue
		}
		if err := n.rescanShare(nodeCtx, s.ID); err != nil {
			logger.Printf("core: open: initial scan for share %s: %v", s.ID, err)
		}
	}
	for _, sub := range cfg.Subscriptions {
		if sub.Paused {
			continue
		}
		if _, err := n.startShareWatch(sub.ShareID, sub.LocalPath, roleSubscription); err != nil {
			logger.Printf("core: open: start watcher for subscription %s: %v", sub.ShareID, err)
			continue
		}
		if err := n.rescanShare(nodeCtx, sub.ShareID); err != nil {
			logger.Printf("core: open: initial scan for subscription %s: %v", sub.ShareID, err)
		}
	}

	for _, p := range cfg.Peers {
		pc, err := newPeerConn(n, p)
		if err != nil {
			logger.Printf("core: open: configure peer %s: %v", p.Name, err)
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
			logger.Printf("core: open: peer %q is configured with this node's own token; not dialing it. Run `syncat peer rm %s`, then add the other node's token.", p.Name, pc.peerKeyHex)
			pc.disable("configured with this node's own token; remove it and add the other node's token")
			continue
		}
		if p.Enabled {
			n.goTracked(pc.runSupervisor)
		}
	}

	return n, nil
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

// goTracked runs fn in a new goroutine tracked by n.wg, so Close waits for
// it.
func (n *Node) goTracked(fn func()) {
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		fn()
	}()
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
	n.goTracked(func() { n.handleAccept(conn) })
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

// --- config mutation helpers ---------------------------------------------

// cloneConfig returns a deep-enough copy of cfg for mutateConfig's
// clone-mutate-swap pattern: every slice/map mutation methods touch is
// copied, so a failed mutation (validation or save error) never leaves
// n.cfg's live, in-use structures partially edited.
func cloneConfig(cfg *config.Config) *config.Config {
	out := *cfg
	out.GlobalIgnores = append([]string(nil), cfg.GlobalIgnores...)
	out.Peers = append([]config.Peer(nil), cfg.Peers...)
	out.Shares = make([]config.Share, len(cfg.Shares))
	for i, s := range cfg.Shares {
		out.Shares[i] = s
		out.Shares[i].Access = make(map[string]string, len(s.Access))
		for k, v := range s.Access {
			out.Shares[i].Access[k] = v
		}
	}
	out.Subscriptions = append([]config.Subscription(nil), cfg.Subscriptions...)
	return &out
}

// mutateConfig applies fn to a clone of the current config, persists the
// clone (config.Save, which also validates), and — only on success —
// swaps it in as the live config. Callers apply live effects (starting/
// stopping watchers and sessions) after mutateConfig returns successfully,
// outside cfgMu, matching every mutation method in this file.
func (n *Node) mutateConfig(fn func(cfg *config.Config) error) (*config.Config, error) {
	n.cfgMu.Lock()
	defer n.cfgMu.Unlock()
	newCfg := cloneConfig(n.cfg)
	if err := fn(newCfg); err != nil {
		return nil, err
	}
	if err := config.Save(n.paths.ConfigFile(), newCfg); err != nil {
		return nil, fmt.Errorf("save config: %w", err)
	}
	n.cfg = newCfg
	return newCfg, nil
}

func findPeerIndex(cfg *config.Config, peerKeyHex string) int {
	for i, p := range cfg.Peers {
		if pt, err := config.ParseToken(p.Token); err == nil && hex.EncodeToString(pt.ID) == peerKeyHex {
			return i
		}
	}
	return -1
}

func findShareIndex(cfg *config.Config, shareID string) int {
	for i, s := range cfg.Shares {
		if s.ID == shareID {
			return i
		}
	}
	return -1
}

func peerNameLocked(cfg *config.Config, peerKeyHex string) string {
	if i := findPeerIndex(cfg, peerKeyHex); i >= 0 {
		return cfg.Peers[i].Name
	}
	return ""
}

// --- peer mutations --------------------------------------------------------

// AddPeer adds a peer by its pasted sc1 token (SPEC.md §2), persists it,
// and starts dialing it immediately. It returns the peer's canonical
// identifier (its full Ed25519 public key, hex-encoded) — every other
// peer-scoped method in this package's API takes that same identifier.
func (n *Node) AddPeer(name, token string) (string, error) {
	tok, err := config.ParseToken(token)
	if err != nil {
		return "", fmt.Errorf("core: add peer: %w", err)
	}
	peerKeyHex := hex.EncodeToString(tok.ID)
	if peerKeyHex == n.PeerKey() {
		return "", fmt.Errorf("core: add peer: this is this node's own token — paste the token printed by `syncat token` on the *other* node")
	}
	if name == "" {
		name = tok.Name
	}

	p := config.Peer{Name: name, Token: token, Enabled: true}
	if _, err := n.mutateConfig(func(cfg *config.Config) error {
		if findPeerIndex(cfg, peerKeyHex) >= 0 {
			return fmt.Errorf("peer %s is already configured", peerKeyHex)
		}
		cfg.Peers = append(cfg.Peers, p)
		return nil
	}); err != nil {
		return "", fmt.Errorf("core: add peer: %w", err)
	}

	pc, err := newPeerConn(n, p)
	if err != nil {
		return "", fmt.Errorf("core: add peer: %w", err)
	}
	n.peersMu.Lock()
	n.peers[peerKeyHex] = pc
	n.peersMu.Unlock()
	n.goTracked(pc.runSupervisor)
	return peerKeyHex, nil
}

// RemovePeer removes a configured peer and every trace of it from the rest
// of the config: subscriptions to its shares, and its entries in our own
// shares' access lists. It also closes any active connection to it.
//
// The subscriptions go because a subscription names the peer that offers
// the share (config.Subscription.Peer): once that peer is gone there is no
// node left to sync it with, so leaving it configured would keep a watcher
// running and keep it listed by `syncat status` forever, permanently
// stuck. Removal is config-only, matching RemoveSubscription — the local
// copy of the files stays on disk, it just stops being synced.
//
// The access entries go because config.Share.Access is keyed by the peer's
// Ed25519 public key, and buildShareList reads that map directly with no
// reference to whether the key still belongs to a configured peer. A
// leftover "granted" is therefore not just clutter: re-adding that same
// key later — the obvious thing to do after removing a peer by mistake, or
// to re-pair after one side is rebuilt — would silently restore its
// previous access to every share, with no new approval and nothing shown
// to the user. Removing a peer should mean it starts from nothing if it
// ever comes back.
func (n *Node) RemovePeer(peerKeyHex string) error {
	var droppedSubs []config.Subscription
	var droppedAccess []string // share ids we revoked this peer's access to
	if _, err := n.mutateConfig(func(cfg *config.Config) error {
		i := findPeerIndex(cfg, peerKeyHex)
		if i < 0 {
			return fmt.Errorf("peer %s is not configured", peerKeyHex)
		}
		cfg.Peers = append(cfg.Peers[:i], cfg.Peers[i+1:]...)

		kept := make([]config.Subscription, 0, len(cfg.Subscriptions))
		for _, s := range cfg.Subscriptions {
			if s.Peer == peerKeyHex {
				droppedSubs = append(droppedSubs, s)
				continue
			}
			kept = append(kept, s)
		}
		cfg.Subscriptions = kept

		// Safe to mutate in place: mutateConfig hands us a clone whose
		// Access maps are themselves freshly built (see cloneConfig).
		for j := range cfg.Shares {
			if _, ok := cfg.Shares[j].Access[peerKeyHex]; !ok {
				continue
			}
			delete(cfg.Shares[j].Access, peerKeyHex)
			droppedAccess = append(droppedAccess, cfg.Shares[j].ID)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("core: remove peer: %w", err)
	}

	// No need to neuter these on the peer's live session the way
	// RemoveSubscription does: cancelling pc below tears the whole session
	// down anyway.
	for _, s := range droppedSubs {
		n.stopShareWatch(s.ShareID)
		n.logger.Printf("core: remove peer %s: dropped subscription to share %s (local copy left at %s)", peerKeyHex, s.ShareID, s.LocalPath)
	}
	for _, shareID := range droppedAccess {
		n.logger.Printf("core: remove peer %s: revoked its access to share %s", peerKeyHex, shareID)
	}

	n.peersMu.Lock()
	pc := n.peers[peerKeyHex]
	delete(n.peers, peerKeyHex)
	n.peersMu.Unlock()
	if pc != nil {
		pc.cancel()
	}
	return nil
}

// --- share mutations --------------------------------------------------------

// AddShare offers a new local directory as a share (SPEC.md §3/§6):
// validates path (including the CheckSharePath overlap rule), generates a
// share id, persists it, starts watching it, and announces it to every
// connected peer.
func (n *Node) AddShare(path, name, permission string, approvalRequired bool) (string, error) {
	if permission != config.PermissionReadOnly && permission != config.PermissionReadWrite {
		return "", fmt.Errorf("core: add share: invalid permission %q", permission)
	}
	id, err := config.NewShareID()
	if err != nil {
		return "", fmt.Errorf("core: add share: %w", err)
	}

	if _, err := n.mutateConfig(func(cfg *config.Config) error {
		if err := cfg.CheckSharePath(path); err != nil {
			return err
		}
		cfg.Shares = append(cfg.Shares, config.Share{
			ID: id, Name: name, Path: path, Permission: permission,
			ApprovalRequired: approvalRequired, Access: map[string]string{},
		})
		return nil
	}); err != nil {
		return "", fmt.Errorf("core: add share: %w", err)
	}

	if _, err := n.startShareWatch(id, path, roleOffered); err != nil {
		n.logger.Printf("core: add share: start watcher for %s: %v", id, err)
	} else if err := n.rescanShare(n.ctx, id); err != nil {
		n.logger.Printf("core: add share: initial scan for %s: %v", id, err)
	}
	n.broadcastShareList()
	return id, nil
}

// RemoveShare stops offering a share: persists the removal, stops its
// watcher, neuters it on any live sessions (see the package doc comment),
// and announces the updated share list.
func (n *Node) RemoveShare(shareID string) error {
	if _, err := n.mutateConfig(func(cfg *config.Config) error {
		i := findShareIndex(cfg, shareID)
		if i < 0 {
			return fmt.Errorf("share %s is not configured", shareID)
		}
		cfg.Shares = append(cfg.Shares[:i], cfg.Shares[i+1:]...)
		return nil
	}); err != nil {
		return fmt.Errorf("core: remove share: %w", err)
	}

	n.stopShareWatch(shareID)
	n.neuterShareOnSessions(shareID)
	n.broadcastShareList()
	return nil
}

// RenameShare updates a share's display name.
func (n *Node) RenameShare(shareID, name string) error {
	if _, err := n.mutateConfig(func(cfg *config.Config) error {
		i := findShareIndex(cfg, shareID)
		if i < 0 {
			return fmt.Errorf("share %s is not configured", shareID)
		}
		cfg.Shares[i].Name = name
		return nil
	}); err != nil {
		return fmt.Errorf("core: rename share: %w", err)
	}
	n.broadcastShareList()
	return nil
}

// SetSharePermission changes a share's permission (SPEC.md §6), updating
// the Direction every currently-connected, currently-granted session for
// this share uses from this point on.
func (n *Node) SetSharePermission(shareID, permission string) error {
	if permission != config.PermissionReadOnly && permission != config.PermissionReadWrite {
		return fmt.Errorf("core: set share permission: invalid permission %q", permission)
	}
	if _, err := n.mutateConfig(func(cfg *config.Config) error {
		i := findShareIndex(cfg, shareID)
		if i < 0 {
			return fmt.Errorf("share %s is not configured", shareID)
		}
		cfg.Shares[i].Permission = permission
		return nil
	}); err != nil {
		return fmt.Errorf("core: set share permission: %w", err)
	}

	n.sharesMu.Lock()
	sw := n.shareWatches[shareID]
	n.sharesMu.Unlock()
	root := ""
	if sw != nil {
		root = sw.root
	}
	direction := syncsvc.DirectionFor(permission, "")
	for _, pc := range n.snapshotPeers() {
		pc.mu.Lock()
		sess := pc.session
		active := pc.activeShares != nil && pc.activeShares[shareID]
		pc.mu.Unlock()
		if sess != nil && active {
			sess.AddShare(syncsvc.ShareConfig{ShareID: shareID, Root: root, Direction: direction})
		}
	}
	n.broadcastShareList()
	return nil
}

// SetShareApprovalRequired toggles a share's approval_required flag.
// SPEC.md §6's approval queue is deferred (see the package doc comment),
// so this is persisted but has no live effect yet — every SubscribeRequest
// is still auto-granted.
func (n *Node) SetShareApprovalRequired(shareID string, required bool) error {
	if _, err := n.mutateConfig(func(cfg *config.Config) error {
		i := findShareIndex(cfg, shareID)
		if i < 0 {
			return fmt.Errorf("share %s is not configured", shareID)
		}
		cfg.Shares[i].ApprovalRequired = required
		return nil
	}); err != nil {
		return fmt.Errorf("core: set share approval required: %w", err)
	}
	n.broadcastShareList()
	return nil
}

// SetShareAccess grants, denies, or revokes one peer's access to a share
// (SPEC.md §6), persisting the decision and, if the peer is connected,
// pushing AccessUpdate and adding/neutering the share on that session
// immediately.
func (n *Node) SetShareAccess(shareID, peerKeyHex, access string) error {
	switch access {
	case protocol.AccessGranted, protocol.AccessDenied, protocol.AccessRevoked:
	default:
		return fmt.Errorf("core: set share access: invalid access %q", access)
	}

	var share config.Share
	if _, err := n.mutateConfig(func(cfg *config.Config) error {
		i := findShareIndex(cfg, shareID)
		if i < 0 {
			return fmt.Errorf("share %s is not configured", shareID)
		}
		if cfg.Shares[i].Access == nil {
			cfg.Shares[i].Access = map[string]string{}
		}
		cfg.Shares[i].Access[peerKeyHex] = access
		share = cfg.Shares[i]
		return nil
	}); err != nil {
		return fmt.Errorf("core: set share access: %w", err)
	}

	pc := n.lookupPeer(peerKeyHex)
	if pc == nil {
		return nil
	}
	pc.mu.Lock()
	sess := pc.session
	pc.mu.Unlock()
	if sess == nil {
		return nil
	}

	if err := sess.Writer().WriteMessage(protocol.MsgAccessUpdate, protocol.AccessUpdate{ShareID: shareID, Access: access}); err != nil {
		n.logger.Printf("core: send access update for %s to %s: %v", shareID, peerKeyHex, err)
	}
	if access == protocol.AccessGranted {
		sess.AddShare(syncsvc.ShareConfig{ShareID: shareID, Root: share.Path, Direction: syncsvc.DirectionFor(share.Permission, "")})
		pc.markShareActive(shareID)
		if err := sess.SyncShare(n.ctx, shareID); err != nil {
			n.logger.Printf("core: sync share %s to %s: %v", shareID, peerKeyHex, err)
		}
	} else {
		sess.AddShare(syncsvc.ShareConfig{ShareID: shareID, Direction: syncsvc.Direction{InboundBlocked: true, OutboundBlocked: true}})
		pc.clearShareActive(shareID)
	}
	return nil
}

// neuterShareOnSessions blocks a share on every currently-connected
// session (both Direction flags set — see the package doc comment) and
// stops counting it as active for propagation. Root is irrelevant once
// both flags are set: Reconcile/SyncShare both check InboundBlocked/
// OutboundBlocked before ever touching cfg.Root (see reconcile.go and
// session.go), so an empty Root here is safe.
func (n *Node) neuterShareOnSessions(shareID string) {
	blocked := syncsvc.Direction{InboundBlocked: true, OutboundBlocked: true}
	for _, pc := range n.snapshotPeers() {
		pc.mu.Lock()
		sess := pc.session
		if pc.activeShares != nil {
			delete(pc.activeShares, shareID)
		}
		pc.mu.Unlock()
		if sess != nil {
			sess.AddShare(syncsvc.ShareConfig{ShareID: shareID, Direction: blocked})
		}
	}
}

// --- subscription mutations -------------------------------------------------

// AddSubscription subscribes to a peer's share (SPEC.md §1/§6): validates
// the local path (CheckSubscriptionPath), creates it if needed, persists
// the subscription, starts watching the local copy, and — if already
// connected to the peer — sends SubscribeRequest immediately.
func (n *Node) AddSubscription(peerKeyHex, shareID, localPath, mode string) error {
	if mode != config.ModeMirror && mode != config.ModeReceiveOnly {
		return fmt.Errorf("core: add subscription: invalid mode %q", mode)
	}
	if err := os.MkdirAll(localPath, 0o700); err != nil {
		return fmt.Errorf("core: add subscription: create local path: %w", err)
	}

	if _, err := n.mutateConfig(func(cfg *config.Config) error {
		if err := cfg.CheckSubscriptionPath(localPath); err != nil {
			return err
		}
		for _, s := range cfg.Subscriptions {
			if s.Peer == peerKeyHex && s.ShareID == shareID {
				return fmt.Errorf("subscription to peer %s share %s already exists", peerKeyHex, shareID)
			}
		}
		cfg.Subscriptions = append(cfg.Subscriptions, config.Subscription{
			Peer: peerKeyHex, ShareID: shareID, LocalPath: localPath, Mode: mode,
		})
		return nil
	}); err != nil {
		return fmt.Errorf("core: add subscription: %w", err)
	}

	if _, err := n.startShareWatch(shareID, localPath, roleSubscription); err != nil {
		n.logger.Printf("core: add subscription: start watcher for %s: %v", shareID, err)
	}

	if pc := n.lookupPeer(peerKeyHex); pc != nil {
		pc.mu.Lock()
		sess := pc.session
		pc.mu.Unlock()
		if sess != nil {
			if err := sess.Writer().WriteMessage(protocol.MsgSubscribeRequest, protocol.SubscribeRequest{ShareID: shareID}); err != nil {
				n.logger.Printf("core: send subscribe request %s to %s: %v", shareID, peerKeyHex, err)
			}
		}
	}
	return nil
}

// RemoveSubscription stops syncing a subscription: persists the removal,
// stops its watcher, and neuters it on the offering peer's live session
// (see the package doc comment).
func (n *Node) RemoveSubscription(peerKeyHex, shareID string) error {
	if _, err := n.mutateConfig(func(cfg *config.Config) error {
		idx := -1
		for i, s := range cfg.Subscriptions {
			if s.Peer == peerKeyHex && s.ShareID == shareID {
				idx = i
				break
			}
		}
		if idx < 0 {
			return fmt.Errorf("subscription to peer %s share %s is not configured", peerKeyHex, shareID)
		}
		cfg.Subscriptions = append(cfg.Subscriptions[:idx], cfg.Subscriptions[idx+1:]...)
		return nil
	}); err != nil {
		return fmt.Errorf("core: remove subscription: %w", err)
	}

	n.stopShareWatch(shareID)
	if pc := n.lookupPeer(peerKeyHex); pc != nil {
		pc.mu.Lock()
		sess := pc.session
		if pc.activeShares != nil {
			delete(pc.activeShares, shareID)
		}
		pc.mu.Unlock()
		if sess != nil {
			sess.AddShare(syncsvc.ShareConfig{ShareID: shareID, Direction: syncsvc.Direction{InboundBlocked: true, OutboundBlocked: true}})
		}
	}
	return nil
}

// PauseSubscription pauses or resumes a subscription: paused stops the
// local watcher and neuters the share on the live session; resuming
// restarts the watcher and re-requests access.
func (n *Node) PauseSubscription(peerKeyHex, shareID string, paused bool) error {
	var sub config.Subscription
	if _, err := n.mutateConfig(func(cfg *config.Config) error {
		idx := -1
		for i, s := range cfg.Subscriptions {
			if s.Peer == peerKeyHex && s.ShareID == shareID {
				idx = i
				break
			}
		}
		if idx < 0 {
			return fmt.Errorf("subscription to peer %s share %s is not configured", peerKeyHex, shareID)
		}
		cfg.Subscriptions[idx].Paused = paused
		sub = cfg.Subscriptions[idx]
		return nil
	}); err != nil {
		return fmt.Errorf("core: pause subscription: %w", err)
	}

	pc := n.lookupPeer(peerKeyHex)

	if paused {
		n.stopShareWatch(shareID)
		if pc != nil {
			pc.mu.Lock()
			sess := pc.session
			if pc.activeShares != nil {
				delete(pc.activeShares, shareID)
			}
			pc.mu.Unlock()
			if sess != nil {
				sess.AddShare(syncsvc.ShareConfig{ShareID: shareID, Direction: syncsvc.Direction{InboundBlocked: true, OutboundBlocked: true}})
			}
		}
		return nil
	}

	if _, err := n.startShareWatch(shareID, sub.LocalPath, roleSubscription); err != nil {
		n.logger.Printf("core: resume subscription: start watcher for %s: %v", shareID, err)
	}
	if pc != nil {
		pc.mu.Lock()
		sess := pc.session
		pc.mu.Unlock()
		if sess != nil {
			if err := sess.Writer().WriteMessage(protocol.MsgSubscribeRequest, protocol.SubscribeRequest{ShareID: shareID}); err != nil {
				n.logger.Printf("core: send subscribe request %s to %s: %v", shareID, peerKeyHex, err)
			}
		}
	}
	return nil
}

// --- node mutation -----------------------------------------------------------

// RenameNode changes this node's display name, persisting it and
// recomputing the cached sc1 token (which embeds the name as a
// "suggested display name" — SPEC.md §2).
func (n *Node) RenameNode(name string) error {
	if _, err := n.mutateConfig(func(cfg *config.Config) error {
		cfg.NodeName = name
		return nil
	}); err != nil {
		return fmt.Errorf("core: rename node: %w", err)
	}

	if addr, err := n.transport.LocalAddress(); err == nil {
		if tok, err := config.EncodeToken(addr, n.identity.Public(), name); err == nil {
			n.tokenMu.Lock()
			n.token = tok
			n.tokenMu.Unlock()
		}
	}
	return nil
}

// --- trash (SPEC.md §7) -----------------------------------------------------

// shareOrSubscriptionRoot resolves shareID to the local directory its
// trash entries live under: the share's own path if this node offers it,
// or the local subscription path if this node subscribes to it from a
// peer. Mirrors cmd/syncat's offline shareRootFor helper, but reads the
// live in-memory config under cfgMu instead of a freshly loaded file.
func (n *Node) shareOrSubscriptionRoot(shareID string) (string, error) {
	n.cfgMu.RLock()
	defer n.cfgMu.RUnlock()
	for _, s := range n.cfg.Shares {
		if s.ID == shareID {
			return s.Path, nil
		}
	}
	for _, sub := range n.cfg.Subscriptions {
		if sub.ShareID == shareID {
			return sub.LocalPath, nil
		}
	}
	return "", fmt.Errorf("share %s is not a local share or subscription", shareID)
}

// ListTrash returns every trashed entry for shareID (SPEC.md §7),
// most-recently-trashed first. shareID must be a locally offered share or
// subscription; an empty result (not an error) means nothing has been
// trashed for it yet.
func (n *Node) ListTrash(shareID string) ([]syncsvc.Entry, error) {
	if _, err := n.shareOrSubscriptionRoot(shareID); err != nil {
		return nil, fmt.Errorf("core: list trash: %w", err)
	}
	entries, err := n.trash.List(shareID)
	if err != nil {
		return nil, fmt.Errorf("core: list trash: %w", err)
	}
	return entries, nil
}

// RestoreTrash restores the most-recently-trashed entry for shareID/relPath
// (SPEC.md §7): copies the trashed content back into the share's live root
// with a bumped version vector, then triggers an immediate rescan so the
// restore propagates to peers right away (unlike the offline `syncat
// trash restore` path, which requires a later rescan since no daemon is
// running to trigger one). Returns an error wrapping
// syncsvc.ErrRestoreDestExists if the destination is already occupied.
func (n *Node) RestoreTrash(ctx context.Context, shareID, relPath string) (index.FileRow, error) {
	root, err := n.shareOrSubscriptionRoot(shareID)
	if err != nil {
		return index.FileRow{}, fmt.Errorf("core: restore trash: %w", err)
	}

	entries, err := n.trash.List(shareID)
	if err != nil {
		return index.FileRow{}, fmt.Errorf("core: restore trash: %w", err)
	}
	var match *syncsvc.Entry
	for i := range entries {
		if entries[i].RelPath == relPath {
			match = &entries[i]
			break
		}
	}
	if match == nil {
		return index.FileRow{}, fmt.Errorf("core: restore trash: no trashed entry %q for share %s", relPath, shareID)
	}

	row, err := n.trash.Restore(ctx, n.store, n.identity.ShortID(), root, *match)
	if err != nil {
		return index.FileRow{}, fmt.Errorf("core: restore trash: %w", err)
	}
	if err := n.rescanShare(ctx, shareID); err != nil {
		n.logger.Printf("core: restore trash: rescan %s after restore: %v", shareID, err)
	}
	return row, nil
}

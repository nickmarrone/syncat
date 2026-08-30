package core

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/nickmarrone/syncat/internal/config"
	"github.com/nickmarrone/syncat/internal/protocol"
	syncsvc "github.com/nickmarrone/syncat/internal/sync"
	"github.com/nickmarrone/syncat/internal/transport"
)

// dedupLossPause bounds how long dialAttempt waits before redialing after
// losing SPEC.md §2.4's dedup rule on our own outbound dial. See
// dialAttempt's "not adopted" branch for why this exists: without it, the
// losing side's dial loop can redial fast enough to starve the peer's own
// (winning) dial of the time it needs to complete its handshake.
const dedupLossPause = 2 * time.Second

// peerConn is one configured peer's connection state machine (SPEC.md
// §2/§4): it drives the dial-with-backoff loop, adopts whichever
// connection (dialed or accepted) wins SPEC.md §2.4's dedup rule, and
// wires that connection's syncsvc.Session together with a
// protocol.Keepalive and Phase 7's own control-message handling for
// ShareList/SubscribeRequest/AccessUpdate.
//
// At most one connection is ever active at a time, guarded by mu.
// Reaching that invariant doesn't require coordinating the two sides of a
// race: KeepConnection's outcome depends only on the two peers' keys and
// which side dialed, not on timing (see offer's doc comment), so a
// connection that loses dedup is simply closed without ever being
// adopted, whichever order the dial and accept happen to complete in.
type peerConn struct {
	node *Node

	peerKeyHex string // full Ed25519 public key, hex — canonical id
	peerShort  string // first 8 bytes, hex — matches config.IdentityKey.ShortID
	connBlob   string // transport address to dial (the token's "tc" field)
	peerPub    ed25519.PublicKey

	ctx    context.Context
	cancel context.CancelFunc

	mu              sync.Mutex
	name            string // configured display name
	enabled         bool
	state           ConnState
	lastErr         string
	lastConnectedAt time.Time
	connectedSince  time.Time
	remoteName      string
	remoteShares    []protocol.ShareListEntry
	subAccess       map[string]string // shareID -> our access state, as offerer's peer reports it

	session      *syncsvc.Session
	conn         net.Conn
	activeShares map[string]bool // shareIDs currently added on session, for propagateShare
	connDone     chan struct{}   // closed when the current connection ends
}

func newPeerConn(n *Node, p config.Peer) (*peerConn, error) {
	tok, err := config.ParseToken(p.Token)
	if err != nil {
		return nil, fmt.Errorf("peer %q: invalid token: %w", p.Name, err)
	}
	ctx, cancel := context.WithCancel(n.ctx)
	return &peerConn{
		node:       n,
		peerKeyHex: hex.EncodeToString(tok.ID),
		peerShort:  hex.EncodeToString(tok.ID[:8]),
		connBlob:   tok.ConnBlob,
		peerPub:    tok.ID,
		ctx:        ctx,
		cancel:     cancel,
		name:       p.Name,
		enabled:    p.Enabled,
		subAccess:  map[string]string{},
	}, nil
}

// runSupervisor drives the dial-with-backoff loop for this peer for as
// long as pc.ctx is alive (SPEC.md §2.2: "dials every configured peer as
// a client with exponential backoff, 1s -> 5min cap, jittered").
func (pc *peerConn) runSupervisor() {
	sup := transport.Supervisor{
		Schedule: transport.Backoff{},
		Clock:    asTransportClock(pc.node.clock),
		Rand:     pc.node.rand,
	}
	sup.Run(pc.ctx, pc.dialAttempt)
}

// dialAttempt is transport.Supervisor's dial func for this peer. If a
// connection is already active, it just waits for that connection to end
// (or ctx to be canceled) rather than opening a second, redundant one —
// SPEC.md §2.2 says dial continuously, but there's nothing to gain from
// actually holding two tunnels to the same peer open at once when dedup
// would just close one of them anyway.
//
// A successful dial+handshake that loses dedup (see offer) returns nil,
// not an error: per transport.Supervisor's doc comment, "the losing side
// of dedup closing voluntarily" isn't a failure worth backing off from —
// the peer was reachable, we just aren't the side responsible for this
// pairing's connection.
func (pc *peerConn) dialAttempt(ctx context.Context) error {
	pc.mu.Lock()
	if pc.session != nil {
		done := pc.connDone
		pc.mu.Unlock()
		select {
		case <-done:
		case <-ctx.Done():
		}
		return nil
	}
	connBlob, peerPub := pc.connBlob, pc.peerPub
	pc.mu.Unlock()

	pc.setState(ConnStateConnecting)
	conn, err := pc.node.transport.Dial(ctx, connBlob)
	if err != nil {
		pc.setErr(err)
		return fmt.Errorf("dial: %w", err)
	}

	result, err := protocol.Handshake(ctx, conn, protocol.HandshakeConfig{
		IdentityKey: pc.node.identity.Private,
		NodeName:    pc.node.nodeName(),
		Token:       pc.node.localToken(),
		IsKnownPeer: func(pub ed25519.PublicKey) bool { return bytes.Equal(pub, peerPub) },
	})
	if err != nil {
		conn.Close()
		pc.setErr(err)
		return fmt.Errorf("handshake: %w", err)
	}

	adopted, err := pc.offer(ctx, conn, result, true)
	if err != nil {
		return err
	}
	if !adopted {
		// We lost SPEC.md §2.4's dedup rule on our own dial: the peer has
		// the higher key, so it's expected to complete this pairing's
		// connection via its own dial to us instead. Supervisor.Run
		// treats a nil error as success and retries with no backoff at
		// all (deliberately — see this func's doc comment, "isn't a
		// failure worth backing off from") — but redialing instantly,
		// over and over, tears down and rebuilds the underlying
		// tailcat/WireGuard session far faster than the peer's own dial
		// can complete its handshake, so in practice the peer's dial
		// never gets a clear window and both sides spin forever without
		// ever converging. A short pause here — still not counted as a
		// failure, still not subject to the growing backoff schedule —
		// is enough to let the peer's dial land.
		select {
		case <-pc.node.clock.After(dedupLossPause):
		case <-ctx.Done():
		}
		return nil
	}

	pc.mu.Lock()
	done := pc.connDone
	pc.mu.Unlock()
	select {
	case <-done:
	case <-ctx.Done():
	}
	return nil
}

// offer applies SPEC.md §2.4's dedup rule to one authenticated connection
// (dialed=true for our own outbound dial, false for an inbound accept)
// and, if it wins, adopts it as this peer's active connection: wires a
// syncsvc.Session with Phase 7's control-message and keepalive hooks,
// starts it, and sends our initial ShareList/SubscribeRequests. Returns
// whether this connection was adopted; a losing connection is closed here
// and never touches pc's state.
//
// transport.KeepConnection's outcome depends only on the two keys and
// dialed, so this never needs to coordinate with a concurrent offer for
// the same peer: at most one of {our dial, their dial} can ever compute
// keep=true for a given key pair, so the defensive "already have a
// session" check below is a correctness backstop (e.g. a stray duplicate
// connection attempt), not something expected to fire in normal
// operation.
func (pc *peerConn) offer(ctx context.Context, conn net.Conn, result *protocol.HandshakeResult, dialed bool) (bool, error) {
	keep := transport.KeepConnection(pc.node.identity.Public(), result.PeerPub, dialed)
	if !keep {
		conn.Close()
		return false, nil
	}

	pc.mu.Lock()
	if pc.session != nil {
		pc.mu.Unlock()
		conn.Close()
		return false, nil
	}

	node := pc.node
	sessCtx, cancel := context.WithCancel(pc.ctx)
	sess := syncsvc.NewSession(conn, node.store, node.identity.ShortID(), pc.peerShort, asSyncClock(node.clock), node.logger)
	sess.SetTrash(node.trash)

	ka := protocol.NewKeepalive(asProtocolClock(node.clock), 0, 0)
	sess.SetFrameObserver(func(protocol.MsgType) { ka.RecordReceived() })
	sess.SetControlHandler(func(typ protocol.MsgType, payload []byte) {
		pc.handleControl(sessCtx, sess, typ, payload)
	})

	pc.session = sess
	pc.conn = conn
	pc.activeShares = map[string]bool{}
	pc.connDone = make(chan struct{})
	pc.state = ConnStateConnected
	pc.connectedSince = node.clock.Now()
	pc.lastConnectedAt = pc.connectedSince
	pc.remoteName = result.PeerName
	pc.lastErr = ""
	done := pc.connDone
	pc.mu.Unlock()

	sess.Start(sessCtx)
	node.sendShareList(sess, pc.peerKeyHex)
	node.requestSubscriptions(sess, pc.peerKeyHex)

	node.goTracked(func() {
		ka.Run(sessCtx, 0, func() {
			if err := sess.Writer().WriteMessage(protocol.MsgPing, protocol.Ping{}); err != nil {
				node.logger.Printf("core: peer %s: send ping: %v", pc.name, err)
				return
			}
			ka.RecordSent()
		}, func() {
			conn.Close() // dead per SPEC.md §4's 90s rule; unblocks the session's read loop
		})
		cancel()
		sess.Close()
		conn.Close()

		pc.mu.Lock()
		pc.session = nil
		pc.conn = nil
		pc.activeShares = nil
		if pc.state == ConnStateConnected {
			pc.state = ConnStateDisconnected
		}
		close(done)
		pc.mu.Unlock()
	})

	return true, nil
}

// handleControl dispatches one frame Session's own read loop doesn't
// handle (see Session.SetControlHandler's doc comment): ShareList,
// SubscribeRequest, AccessUpdate, and Ping/Pong. Runs synchronously on
// the session's read-loop goroutine; anything that does I/O (config
// persistence, a store-backed rescan) is handed off via node.goTracked so
// the read loop is never blocked by it.
func (pc *peerConn) handleControl(ctx context.Context, sess *syncsvc.Session, typ protocol.MsgType, payload []byte) {
	node := pc.node
	switch typ {
	case protocol.MsgPing:
		if err := sess.Writer().WriteMessage(protocol.MsgPong, protocol.Pong{}); err != nil {
			node.logger.Printf("core: peer %s: reply pong: %v", pc.name, err)
		}
	case protocol.MsgPong:
		// No action beyond the frame observer's RecordReceived.

	case protocol.MsgShareList:
		var msg protocol.ShareList
		if err := protocol.DecodeMessage(payload, &msg); err != nil {
			node.logger.Printf("core: peer %s: decode ShareList: %v", pc.name, err)
			return
		}
		pc.setRemoteShares(msg.Shares)

	case protocol.MsgSubscribeRequest:
		var msg protocol.SubscribeRequest
		if err := protocol.DecodeMessage(payload, &msg); err != nil {
			node.logger.Printf("core: peer %s: decode SubscribeRequest: %v", pc.name, err)
			return
		}
		// provisionShareForRequest's sess.AddShare must happen here, on
		// the read-loop goroutine, not inside the goTracked closure below
		// — see its doc comment for why: it establishes the local
		// happens-before ordering that the async work here relies on.
		share, ok := node.provisionShareForRequest(pc, sess, msg.ShareID)
		if !ok {
			return
		}
		node.goTracked(func() { node.finishSubscribeRequest(pc, share) })

	case protocol.MsgAccessUpdate:
		var msg protocol.AccessUpdate
		if err := protocol.DecodeMessage(payload, &msg); err != nil {
			node.logger.Printf("core: peer %s: decode AccessUpdate: %v", pc.name, err)
			return
		}
		if needsFinish := node.provisionAccessUpdate(pc, sess, msg); needsFinish {
			node.goTracked(func() { node.finishAccessUpdate(ctx, sess, msg.ShareID) })
		}

	default:
		// Hello/Auth: shouldn't arrive post-handshake from a well-behaved
		// peer; ignore defensively rather than treating it as fatal.
	}
}

func (pc *peerConn) setState(s ConnState) {
	pc.mu.Lock()
	pc.state = s
	pc.mu.Unlock()
}

func (pc *peerConn) setErr(err error) {
	pc.mu.Lock()
	pc.state = ConnStateBackingOff
	pc.lastErr = err.Error()
	pc.mu.Unlock()
}

func (pc *peerConn) markShareActive(shareID string) {
	pc.mu.Lock()
	if pc.activeShares == nil {
		pc.activeShares = map[string]bool{}
	}
	pc.activeShares[shareID] = true
	pc.mu.Unlock()
}

func (pc *peerConn) clearShareActive(shareID string) {
	pc.mu.Lock()
	if pc.activeShares != nil {
		delete(pc.activeShares, shareID)
	}
	pc.mu.Unlock()
}

func (pc *peerConn) setRemoteShares(entries []protocol.ShareListEntry) {
	pc.mu.Lock()
	pc.remoteShares = entries
	pc.mu.Unlock()
}

func (pc *peerConn) setSubscriptionAccess(shareID, access string) {
	pc.mu.Lock()
	if pc.subAccess == nil {
		pc.subAccess = map[string]string{}
	}
	pc.subAccess[shareID] = access
	pc.mu.Unlock()
}

// --- offerer side: a peer wants access to a share we offer ----------------

// provisionShareForRequest is SPEC.md §6's grant decision point (the MVP
// auto-grants unconditionally — see the package doc comment) and the
// synchronous half of handling a SubscribeRequest: it must run on the
// session's read-loop goroutine (called directly from handleControl,
// never from a spawned goroutine), because it establishes sess's local
// share config — via sess.AddShare — before the read loop advances to
// read whatever frame the peer sends next. Once granted, this node itself
// sends an IndexUpdate for this share moments later (finishSubscribeRequest,
// via SetShareAccess -> Session.SyncShare) on the SAME connection, and if
// the peer's own inbound traffic interleaves with that, Session's read
// loop must already know about this share by the time it dispatches
// anything referencing it — a race that showed up in practice as
// Session logging "index update for unknown share" when the two halves of
// this handling ran on independent, unordered goroutines.
//
// Returns the share's config (for finishSubscribeRequest) and whether a
// matching share was found at all.
func (n *Node) provisionShareForRequest(pc *peerConn, sess *syncsvc.Session, shareID string) (config.Share, bool) {
	n.cfgMu.RLock()
	i := findShareIndex(n.cfg, shareID)
	var share config.Share
	if i >= 0 {
		share = n.cfg.Shares[i]
	}
	n.cfgMu.RUnlock()
	if i < 0 {
		return config.Share{}, false // unknown share; no Error code defined for this in SPEC.md §4
	}

	// TODO(approvals): SPEC.md §6 says a share with ApprovalRequired=true
	// should instead queue this for UI/CLI approval and only grant (and
	// only then provision the session) on an explicit decision. The MVP
	// auto-grants regardless — see the package doc comment — so the
	// share's ApprovalRequired flag is intentionally unused here; it's
	// still persisted in config for that future queue. Enforcing it will
	// need to reconcile with this function's "must run synchronously"
	// requirement above, since an approval decision can't be synchronous
	// with an inbound frame that arrived before a human ever acts on it.
	sess.AddShare(syncsvc.ShareConfig{ShareID: shareID, Root: share.Path, Direction: syncsvc.DirectionFor(share.Permission, "")})
	pc.markShareActive(shareID)
	return share, true
}

// finishSubscribeRequest is the asynchronous remainder of handling a
// SubscribeRequest: persisting the grant and notifying the peer
// (SetShareAccess also re-applies the same sess.AddShare provisionShareForRequest
// already did — a harmless idempotent overwrite — since SetShareAccess is
// also Phase 8's direct API entry point and shouldn't have a
// provision-already-done special case).
func (n *Node) finishSubscribeRequest(pc *peerConn, share config.Share) {
	if err := n.SetShareAccess(share.ID, pc.peerKeyHex, protocol.AccessGranted); err != nil {
		n.logger.Printf("core: grant %s to %s: %v", share.ID, pc.peerKeyHex, err)
	}
}

// --- subscriber side: the offerer answered our SubscribeRequest -----------

// provisionAccessUpdate is AccessUpdate's synchronous half — see
// provisionShareForRequest's doc comment for why sess.AddShare must run
// here, on the read-loop goroutine, rather than in a spawned goroutine:
// the offerer sends its own IndexUpdate for this share immediately after
// granting, and this session must already know the share by the time
// that frame (or any later one) is dispatched.
//
// Returns whether there's asynchronous follow-up work to do (the initial
// SyncShare and local rescan, only for a fresh, unpaused grant).
func (n *Node) provisionAccessUpdate(pc *peerConn, sess *syncsvc.Session, msg protocol.AccessUpdate) bool {
	n.cfgMu.RLock()
	var subCopy config.Subscription
	found := false
	for _, s := range n.cfg.Subscriptions {
		if s.Peer == pc.peerKeyHex && s.ShareID == msg.ShareID {
			subCopy, found = s, true
			break
		}
	}
	n.cfgMu.RUnlock()
	if !found {
		return false
	}

	pc.setSubscriptionAccess(msg.ShareID, msg.Access)

	switch msg.Access {
	case protocol.AccessGranted:
		if subCopy.Paused {
			return false
		}
		direction := syncsvc.DirectionFor("", subCopy.Mode)
		sess.AddShare(syncsvc.ShareConfig{ShareID: msg.ShareID, Root: subCopy.LocalPath, Direction: direction})
		pc.markShareActive(msg.ShareID)
		return true

	case protocol.AccessDenied, protocol.AccessRevoked:
		sess.AddShare(syncsvc.ShareConfig{ShareID: msg.ShareID, Direction: syncsvc.Direction{InboundBlocked: true, OutboundBlocked: true}})
		pc.clearShareActive(msg.ShareID)
	}
	return false
}

// finishAccessUpdate is AccessUpdate's asynchronous half: the initial
// full sync in both directions (SyncShare pushes our current state;
// rescanShare picks up any pre-existing local content in the subscription
// directory and pushes that too).
func (n *Node) finishAccessUpdate(ctx context.Context, sess *syncsvc.Session, shareID string) {
	if err := sess.SyncShare(ctx, shareID); err != nil {
		n.logger.Printf("core: initial sync for subscription %s: %v", shareID, err)
	}
	if err := n.rescanShare(n.ctx, shareID); err != nil {
		n.logger.Printf("core: initial local scan for subscription %s: %v", shareID, err)
	}
}

// --- share list / subscribe-request announcements --------------------------

func (n *Node) sendShareList(sess *syncsvc.Session, peerKeyHex string) {
	entries := n.buildShareList(peerKeyHex)
	if err := sess.Writer().WriteMessage(protocol.MsgShareList, protocol.ShareList{Shares: entries}); err != nil {
		n.logger.Printf("core: send share list to %s: %v", peerKeyHex, err)
	}
}

func (n *Node) buildShareList(peerKeyHex string) []protocol.ShareListEntry {
	n.cfgMu.RLock()
	defer n.cfgMu.RUnlock()
	entries := make([]protocol.ShareListEntry, 0, len(n.cfg.Shares))
	for _, s := range n.cfg.Shares {
		access := s.Access[peerKeyHex]
		if access == "" {
			access = protocol.AccessNone
		}
		entries = append(entries, protocol.ShareListEntry{
			ShareID: s.ID, Name: s.Name, Permission: s.Permission,
			ApprovalRequired: s.ApprovalRequired, Access: access,
		})
	}
	return entries
}

func (n *Node) requestSubscriptions(sess *syncsvc.Session, peerKeyHex string) {
	n.cfgMu.RLock()
	var toRequest []string
	for _, sub := range n.cfg.Subscriptions {
		if sub.Peer == peerKeyHex && !sub.Paused {
			toRequest = append(toRequest, sub.ShareID)
		}
	}
	n.cfgMu.RUnlock()
	for _, shareID := range toRequest {
		if err := sess.Writer().WriteMessage(protocol.MsgSubscribeRequest, protocol.SubscribeRequest{ShareID: shareID}); err != nil {
			n.logger.Printf("core: send subscribe request %s to %s: %v", shareID, peerKeyHex, err)
		}
	}
}

// broadcastShareList re-announces our share list to every currently
// connected peer (SPEC.md §4: ShareList is sent "on connect + on
// change").
func (n *Node) broadcastShareList() {
	for _, pc := range n.snapshotPeers() {
		pc.mu.Lock()
		sess := pc.session
		pc.mu.Unlock()
		if sess != nil {
			n.sendShareList(sess, pc.peerKeyHex)
		}
	}
}

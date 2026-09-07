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

// dedupLossWarnAfter is how many consecutive dedup losses with nothing
// ever adopted it takes before noteDedupLoss reports the condition, and
// dedupLossLogEvery how many further losses between repeat log lines
// (~1 minute apart, at dedupLossPause). The threshold is set well above
// the one-or-two losses that normal startup racing produces.
const (
	dedupLossWarnAfter = 5
	dedupLossLogEvery  = 30
)

// dialFailureLogEvery is how many consecutive failed dials between repeat
// log lines from noteDialFailure. The first failure always logs.
const dialFailureLogEvery = 30

// staleInboundGrace is how long a freshly adopted connection is immune to
// notePeerRedialed's staleness inference. It exists only for the
// simultaneous-start race: when both nodes boot at once, the loser's dial
// can land a few seconds after the winner adopted its own, and that inbound
// is not evidence of anything. A connection older than this did not come
// from that race.
const staleInboundGrace = 15 * time.Second

// dialTimeout bounds one call to Transport.Dial.
//
// Supervisor.Run hands dialAttempt the peer's own long-lived context,
// which is cancelled only on shutdown or peer removal, so without this a
// dial has no deadline whatsoever — and a dial into a half-dead tunnel does
// not fail, it *hangs*. tailcat's WireGuard session retries its handshake
// forever ("Handshake did not complete after 5 seconds, retrying (try N)")
// while netstack keeps retransmitting the SYN behind it, so Dial simply
// never returns.
//
// That is worse than slow: every recovery path here is driven by a dial
// *failing*. A dial that hangs never reaches the backoff schedule, never
// increments the failure count, and never reaches
// TailcatTransport.discardClient — which is what rebuilds the stale
// per-peer Client after the peer restarts. One wedged dial pins the peer in
// that state indefinitely.
//
// 30s is comfortably above a legitimate cold dial (a fresh Client needs a
// DERP connection, ~3s, plus tailcat's own hard 10s meow-ping timeout)
// while still giving up fast enough to retry with a fresh Client promptly.
const dialTimeout = 30 * time.Second

// ConnState is a peer connection's current state, per SPEC.md §2's dial/
// backoff/handshake flow.
type ConnState string

const (
	// ConnStateDisconnected means no connection attempt is currently in
	// flight or established, and none has ever succeeded (or the peer is
	// disabled).
	ConnStateDisconnected ConnState = "disconnected"
	// ConnStateConnecting means a dial or handshake is currently in
	// progress.
	ConnStateConnecting ConnState = "connecting"
	// ConnStateConnected means an authenticated connection is up (having
	// won SPEC.md §2.4's dedup, if applicable).
	ConnStateConnected ConnState = "connected"
	// ConnStateBackingOff means the last attempt failed and the next is
	// scheduled after transport.Backoff's delay.
	ConnStateBackingOff ConnState = "backing_off"
)

// peerConn is one configured peer's connection state machine (SPEC.md
// §2/§4): it drives the dial-with-backoff loop, adopts whichever
// connection (dialed or accepted) wins SPEC.md §2.4's dedup rule, and
// wires that connection's syncsvc.Session together with a
// protocol.Keepalive and handleControl, which dispatches
// ShareList/SubscribeRequest/AccessUpdate to access.go.
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
	dedupLosses     int // consecutive dedup losses with nothing adopted; see noteDedupLoss
	dialFailures    int // consecutive failed dials/handshakes; see noteDialFailure
	lastConnectedAt time.Time
	connectedSince  time.Time
	remoteName      string
	remoteShares    []protocol.ShareListEntry
	subAccess       map[string]string // shareID -> our access state, as offerer's peer reports it

	sessCancel   context.CancelFunc // ends the current session; see notePeerRedialed
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
	// cancel() as soon as Dial returns, not on function exit: this ctx
	// bounds the dial alone, and dialAttempt goes on to block for the whole
	// life of the connection below.
	// Real time, not node.clock: this bounds a network call inside
	// Transport, which has no notion of the injected clock. No test dials
	// slowly enough to reach it (PipeTransport fails an unroutable address
	// immediately), so nothing here waits on wall-clock time.
	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	conn, err := pc.node.transport.Dial(dialCtx, connBlob)
	cancel()
	if err != nil {
		pc.noteDialFailure("dial", err)
		return fmt.Errorf("dial: %w", err)
	}

	result, err := protocol.InitiateHandshake(ctx, conn, protocol.HandshakeConfig{
		IdentityKey: pc.node.identity.Private,
		NodeName:    pc.node.nodeName(),
		Token:       pc.node.localToken(),
		IsKnownPeer: func(pub ed25519.PublicKey) bool { return bytes.Equal(pub, peerPub) },
	})
	if err != nil {
		conn.Close()
		pc.noteDialFailure("handshake", err)
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
		//
		// That pause assumes the peer's dial *does* eventually land. When
		// it never does, this branch is the one place a peer can spin
		// indefinitely while looking perfectly healthy: state stays
		// "connecting", lastErr stays empty, and nothing is logged,
		// because losing dedup is deliberately not an error. noteDedupLoss
		// is what makes that state say so out loud.
		pc.noteDedupLoss()
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
// syncsvc.Session with this file's control-message and keepalive hooks,
// starts it, and sends our initial ShareList/SubscribeRequests. Returns
// whether this connection was adopted; a losing connection is closed here
// and never touches pc's state.
//
// transport.KeepConnection's outcome depends only on the two keys and
// dialed, so this never needs to coordinate with a concurrent offer for
// the same peer: at most one of {our dial, their dial} can ever compute
// keep=true for a given key pair, whichever order the dial and accept
// complete in.
//
// That does not make the "already have a session" check below dead code.
// It is where a winning connection lands when this side is still holding
// a stale session for the same peer — see the comment at that branch.
func (pc *peerConn) offer(ctx context.Context, conn net.Conn, result *protocol.HandshakeResult, dialed bool) (bool, error) {
	keep := transport.KeepConnection(pc.node.identity.Public(), result.PeerPub, dialed)
	if !keep {
		conn.Close()
		if !dialed {
			pc.notePeerRedialed()
		}
		return false, nil
	}

	pc.mu.Lock()
	if pc.session != nil {
		pc.mu.Unlock()
		conn.Close()
		// Same inference as the !keep branch above, and for a case that is
		// anything but hypothetical. When the *higher*-keyed peer restarts,
		// its fresh dial wins the rule on both ends, so on this (lower-keyed)
		// side it sails past !keep and lands right here — where, if we are
		// still holding a session from before its restart, we would silently
		// drop the winning connection and then wait out the dead timer for a
		// connection whose peer no longer exists. The peer, having adopted
		// its own side, sees our close and waits out its timer too.
		if !dialed {
			pc.notePeerRedialed()
		}
		return false, nil
	}

	node := pc.node
	sessCtx, cancel := context.WithCancel(pc.ctx)
	sess := syncsvc.NewSession(conn, node.store, node.identity.ShortID(), pc.peerShort, asSyncClock(node.clock), node.logger)
	sess.SetTrash(node.trash)
	// So sync's lines say "peer laptop" like core's, not a bare hex ShortID
	// the reader has to look up in `syncat peer ls`.
	sess.SetPeerLabel(pc.name)
	sess.SetDebug(node.debugEnabled())

	ka := protocol.NewKeepalive(asProtocolClock(node.clock), 0, 0)
	sess.SetFrameObserver(func(protocol.MsgType) { ka.RecordReceived() })
	sess.SetControlHandler(func(typ protocol.MsgType, payload []byte) {
		pc.handleControl(sessCtx, sess, typ, payload)
	})

	pc.sessCancel = cancel
	pc.session = sess
	pc.conn = conn
	pc.activeShares = map[string]bool{}
	pc.connDone = make(chan struct{})
	pc.state = ConnStateConnected
	pc.connectedSince = node.clock.Now()
	pc.lastConnectedAt = pc.connectedSince
	pc.remoteName = result.PeerName
	pc.lastErr = ""
	pc.dedupLosses = 0  // a connection was adopted (either direction) — see noteDedupLoss
	pc.dialFailures = 0 // ... and getting here means dialling works — see noteDialFailure
	done := pc.connDone
	pc.mu.Unlock()

	sess.Start(sessCtx)

	// The one line that says this pairing is actually up. Without it a
	// healthy node and one whose peer flaps every thirty seconds produce
	// almost identical logs — the flapping one simply has less in it,
	// since every existing line here is an error path. Pairs with
	// runConnection's teardown line, which reports how long this lasted.
	direction := "accepted from"
	if dialed {
		direction = "dialed"
	}
	node.logger.Printf("core: peer %s (%s): connected (%s, remote name %q)", pc.name, pc.peerShort, direction, result.PeerName)

	node.sendShareList(sess, pc.peerKeyHex)
	node.requestSubscriptions(sess, pc.peerKeyHex)

	node.goTracked(func() { pc.runConnection(sessCtx, cancel, sess, ka, conn, done) })
	return true, nil
}

// runConnection is the adopted connection's lifetime, on its own tracked
// goroutine: it runs the keepalive alongside the session, waits for
// whichever ends first, then tears everything down and releases
// dialAttempt (by closing done) to redial.
//
// The keepalive runs alongside the wait rather than being the wait itself.
// Its dead timer is the *backstop* for noticing this connection has ended,
// not the mechanism: a peer that closes cleanly, or a socket that breaks
// under a write, ends the session's read loop immediately, and waiting
// out SPEC.md §4's 90s instead (at the poll granularity, up to 120s) is
// 90s of a node reporting itself connected to something that is gone, and
// 90s before dialAttempt is released to redial. The check interval comes
// from protocol.DefaultKeepaliveCheckInterval.
//
// The teardown order is load-bearing: cancel unblocks ka.Run; sess.Close
// must precede conn.Close (see notePeerRedialed); the transport's cached
// per-peer state must be discarded before close(done) lets dialAttempt
// redial; and pc's fields are reset under mu in the same critical section
// that closes done.
func (pc *peerConn) runConnection(sessCtx context.Context, cancel context.CancelFunc, sess *syncsvc.Session, ka *protocol.Keepalive, conn net.Conn, done chan struct{}) {
	kaDone := make(chan struct{})
	go func() {
		defer close(kaDone)
		pc.runKeepalive(sessCtx, ka, sess, conn)
	}()

	var why string
	select {
	case <-sess.Done(): // the read loop ended: peer closed, or the link broke
		why = "peer closed the connection or the link broke"
	case <-kaDone: // the dead rule fired, or the session context was cancelled
		// runKeepalive distinguishes these two for us; a cancelled session
		// context is an orderly local teardown (shutdown, peer removal,
		// notePeerRedialed), not the 90s dead rule.
		why = "keepalive ended the connection"
		if sessCtx.Err() != nil {
			why = "shut down locally"
		}
	}

	cancel()
	<-kaDone // cancel() above is what unblocks ka.Run
	sess.Close()
	conn.Close()

	// Before close(done) releases dialAttempt to redial: whatever the
	// transport cached for this peer belongs to the connection that
	// just died. tailcat's per-peer Client is the case that matters —
	// it announces itself to the peer's server exactly once, so a
	// Client reused across the peer's restart dials into a tunnel the
	// far side has forgotten, and hangs there.
	pc.node.transport.DiscardPeer(pc.connBlob)

	pc.mu.Lock()
	pc.sessCancel = nil
	pc.session = nil
	pc.conn = nil
	pc.activeShares = nil
	if pc.state == ConnStateConnected {
		pc.state = ConnStateDisconnected
	}
	uptime := pc.node.clock.Now().Sub(pc.connectedSince)
	name := pc.name
	close(done)
	pc.mu.Unlock()

	// Uptime is what makes this line worth having: a peer reconnecting
	// every few seconds and one that has been up for a week both log a
	// single "disconnected", and only the duration tells them apart.
	pc.node.logger.Printf("core: peer %s (%s): disconnected after %s (%s)", name, pc.peerShort, uptime.Truncate(time.Second), why)
}

// runKeepalive drives SPEC.md §4's ping/dead rule for one connection until
// ctx is cancelled: it sends a Ping on ka's schedule and closes conn when
// the dead timer fires (which unblocks the session's read loop and so ends
// runConnection's wait).
func (pc *peerConn) runKeepalive(ctx context.Context, ka *protocol.Keepalive, sess *syncsvc.Session, conn net.Conn) {
	ka.Run(ctx, 0, func() {
		if err := sess.Writer().WriteMessage(protocol.MsgPing, protocol.Ping{}); err != nil {
			// Not a transient condition to log and retry in 30s: a
			// ping is one small frame on the writer's priority lane,
			// so a failure to even queue it means the connection is
			// finished. Close it and let the read loop's EOF unwind
			// the session, the same way a peer disconnecting does.
			pc.node.logger.Printf("core: peer %s: send ping, dropping connection: %v", pc.name, err)
			conn.Close()
			return
		}
		ka.RecordSent()
	}, func() {
		// The single most diagnostic event the transport has: the peer
		// stopped answering entirely. Silently closing here made it
		// indistinguishable from the peer hanging up cleanly, which is
		// the difference between "the other node was restarted" and "the
		// network between us is black-holing traffic".
		pc.node.logger.Printf("core: peer %s (%s): no traffic for the keepalive dead interval; treating the connection as dead", pc.name, pc.peerShort)
		conn.Close() // dead per SPEC.md §4's 90s rule; unblocks the session's read loop
	})
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

// notePeerRedialed reacts to an authenticated inbound connection that the
// dedup rule told us to reject while we still hold a session for that peer.
//
// That combination is evidence, not noise. dialAttempt does not dial while
// it holds a session, so a peer only dials us when it has none — and if it
// has none while we believe we have one, ours is a corpse. The peer just
// proved it is alive and reachable by completing a full handshake over it.
//
// Without this the corpse is only noticed by SPEC.md §4's 90s dead rule,
// and the wait is paid by whichever node the dedup rule made responsible
// for dialling this pairing: the *other* node can redial all it likes, but
// its connections are rejected here by design, so they can never repair the
// pairing on their own. Restarting the dialling node therefore looked
// instant while restarting its peer took minutes — an asymmetry with no
// cause beyond how the two keys happened to compare.
//
// Both the close and the cancel are needed, in that order. Closing the conn
// is what unblocks the session's read loop — cancelling alone would deadlock
// the teardown, which calls sess.Close() before conn.Close() and so waits on
// a reader still parked in conn.Read. Cancelling is what makes it prompt:
// closing alone would leave the keepalive's Run loop waiting out the full
// dead timer before the teardown that redials could run. The dead-rule path
// closes the conn for exactly the first reason.
func (pc *peerConn) notePeerRedialed() {
	pc.mu.Lock()
	cancel, sess, conn := pc.sessCancel, pc.session, pc.conn
	age := pc.node.clock.Now().Sub(pc.connectedSince)
	name := pc.name
	pc.mu.Unlock()

	if sess == nil || cancel == nil || conn == nil || age < staleInboundGrace {
		return
	}
	pc.node.logger.Printf("core: peer %s (%s): it dialled us while we still held a connection to it %s old, so that connection is dead; dropping it and redialling", name, pc.peerShort, age.Truncate(time.Second))
	conn.Close()
	cancel()
}

// noteDialFailure records one failed outbound dial or handshake: it moves
// the peer to backing-off with err as its last error, and logs the failure.
//
// Rate-limited rather than silent or spammy. transport.Supervisor.Run
// deliberately does no logging of its own, so before this existed a peer
// that could not dial out produced *no log line at all* — the error reached
// `syncat status` as lastErr and nowhere else. That is a bad way to find
// out a link is down: this node looks like it is merely backing off, and
// the only thing written to any log is on the *other* node, which keeps
// dialing, authenticating, and losing SPEC.md §2.4's dedup rule while it
// waits for the dial this node is failing to make.
func (pc *peerConn) noteDialFailure(stage string, err error) {
	pc.mu.Lock()
	pc.state = ConnStateBackingOff
	pc.lastErr = err.Error()
	pc.dialFailures++
	failures := pc.dialFailures
	name := pc.name
	pc.mu.Unlock()

	if failures == 1 || failures%dialFailureLogEvery == 0 {
		pc.node.logger.Printf("core: peer %s (%s): %s failed (%d in a row): %v", name, pc.peerShort, stage, failures, err)
	}
}

// noteDedupLoss records one outbound dial that authenticated but lost
// SPEC.md §2.4's dedup rule, and reports the condition once it has
// repeated enough times to mean something is wrong.
//
// A single loss — even a few — is entirely normal for the lower-keyed side
// of a pairing: it means the peer is the one responsible for this
// connection, and its own dial is expected to land within a beat, at which
// point offer adopts it and resets this counter. What is not normal is
// losing over and over with nothing ever adopted from either direction.
// That is the signature of a peer we can reach but that cannot reach us
// (so its winning dial never arrives), or of two nodes that disagree about
// who the other is. Both used to present identically to a peer that was
// simply offline: state "connecting", no error, no log line.
//
// This deliberately sets lastErr without touching state. The node really
// is still connecting, and it is still correct for it to keep trying — the
// only thing missing was a way to see why it never finishes.
func (pc *peerConn) noteDedupLoss() {
	pc.mu.Lock()
	pc.dedupLosses++
	losses := pc.dedupLosses
	if losses >= dedupLossWarnAfter {
		pc.lastErr = fmt.Sprintf("handshake succeeded but this connection lost the deduplication rule %d times in a row and the peer's own dial never arrived: the peer can be reached from here but may not be able to reach us, or may not have us configured as a peer", losses)
	}
	name := pc.name
	pc.mu.Unlock()

	// Log at the threshold, then only occasionally: this runs every
	// dedupLossPause for as long as the condition lasts, so logging each
	// time would bury everything else in a long-running daemon.
	if losses == dedupLossWarnAfter || (losses > dedupLossWarnAfter && (losses-dedupLossWarnAfter)%dedupLossLogEvery == 0) {
		pc.node.logger.Printf("core: peer %s (%s): dialed and authenticated %d times in a row without a connection being adopted; check that this node's token is configured on the peer", name, pc.peerShort, losses)
	}
}

// disable stops pc from ever dialing and records why, for a peer that is
// misconfigured badly enough that dialing it could not possibly succeed
// (see Open's own-token check). The peerConn stays registered so the peer
// remains visible — with reason attached — in `syncat peer ls` and the web
// UI for as long as it is still in config.json.
func (pc *peerConn) disable(reason string) {
	pc.cancel()
	pc.mu.Lock()
	pc.state = ConnStateDisconnected
	pc.lastErr = reason
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

// currentSession returns the session for this peer's active connection,
// or nil if it is not connected right now.
func (pc *peerConn) currentSession() *syncsvc.Session {
	pc.mu.Lock()
	s := pc.session
	pc.mu.Unlock()
	return s
}

// neuterShare blocks a share on this peer's current session, if any (both
// Direction flags set — see the package doc comment) and stops counting it
// as active for propagation. Root is irrelevant once both flags are set:
// Reconcile/SyncShare both check InboundBlocked/OutboundBlocked before
// ever touching cfg.Root (see reconcile.go and session.go), so an empty
// Root here is safe.
func (pc *peerConn) neuterShare(shareID string) {
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

func (pc *peerConn) setRemoteShares(entries []protocol.ShareListEntry) {
	pc.mu.Lock()
	pc.remoteShares = entries
	pc.mu.Unlock()
}

// offeredShares returns a copy of the share list this peer most recently
// sent us, or nil if it has never sent one.
func (pc *peerConn) offeredShares() []protocol.ShareListEntry {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if len(pc.remoteShares) == 0 {
		return nil
	}
	return append([]protocol.ShareListEntry(nil), pc.remoteShares...)
}

func (pc *peerConn) setSubscriptionAccess(shareID, access string) {
	pc.mu.Lock()
	if pc.subAccess == nil {
		pc.subAccess = map[string]string{}
	}
	pc.subAccess[shareID] = access
	pc.mu.Unlock()
}

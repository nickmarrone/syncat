package sync

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nickmarrone/syncat/internal/index"
	"github.com/nickmarrone/syncat/internal/protocol"
)

// maxConcurrentPulls bounds the number of FileRequests this Session keeps
// outstanding to one peer at once (SPEC.md §4: "max 4 concurrent pulls per
// peer"). Additional pulls queue on pullSem until a slot frees. Chunks for
// every in-flight pull are interleaved on the one underlying stream
// (protocol.Writer is safe for concurrent writers, see message.go), so
// several transfers make progress at once without separate connections.
const maxConcurrentPulls = 4

// maxConcurrentServes bounds how many FileRequests from the peer we answer
// at once. SPEC.md §4 only limits the puller side, but capping serving
// concurrency too bounds open file descriptors and goroutines, which would
// otherwise grow with however many requests the peer sends.
const maxConcurrentServes = 4

// maxConcurrentIndexJobs bounds independently requested index work. Index
// updates themselves are serialized by reconcileMu, so a larger number only
// creates waiters and memory pressure without increasing useful throughput.
const maxConcurrentIndexJobs = 4

// maxPendingIndexKeys bounds distinct share/operation pairs waiting for an
// index worker. Repeated work for the same key is replaced in place.
const maxPendingIndexKeys = 64

// --- the session: one authenticated peer connection --------------------

// ShareConfig is one share this Session keeps in sync with the peer at the
// other end of its connection: the local directory it maps to, and the
// Direction (SPEC.md §5) constraining which way changes may flow. Build
// Direction with DirectionFor rather than by hand.
type ShareConfig struct {
	ShareID   string
	Root      string // absolute local directory this share's files live under
	Direction Direction

	// Ignore reports whether a share-relative path is excluded from syncing
	// by this share's ignore rules: the built-ins, the global ignore list, and
	// the share's own .syncatignore (SPEC.md §5). nil means nothing is
	// ignored, so the zero value is usable.
	//
	// It is a closure rather than an *index.Matcher because .syncatignore is
	// re-read on every rescan. A snapshotted matcher would go stale when the
	// user edits the file, and nothing re-adds a share when that happens.
	// internal/core resolves the closure against the share's current matcher
	// on every call.
	Ignore func(relpath string, isDir bool) bool
}

// ignored calls Ignore after checking it for nil.
func (c ShareConfig) ignored(relpath string, isDir bool) bool {
	return c.Ignore != nil && c.Ignore(relpath, isDir)
}

// Session drives sync for one authenticated peer connection. It exchanges
// IndexUpdate for each configured share, pulls and serves file content, and
// applies the actions from [Reconcile] to the local filesystem and index.
// One Session wraps one net.Conn (SPEC.md §4: a single stream carries every
// share synced with that peer) and can hold any number of shares.
//
// Session assumes the handshake and the share/access negotiation in
// internal/protocol already happened. From then on it handles only
// IndexUpdate, FileRequest, FileChunk, and file-transfer-scoped Error. It
// passes other message types to the control handler that internal/core
// registers (see SessionOptions.OnControl), because the peer manager drives those
// exchanges. A test can therefore drive a Session with a bare net.Conn
// (e.g. transport.PipeTransport) and no peer manager.
//
// Every background goroutine Session starts is tied to the context passed
// to Start. Close cancels that context, closes the connection (which
// unblocks the read loop's pending Read), and waits for every goroutine to
// exit, so no goroutine outlives the Session.
type Session struct {
	conn   net.Conn
	reader *protocol.Reader
	writer *protocol.StreamWriter

	store  *index.Store
	nodeID string // this node's ShortID
	peerID string // the peer's ShortID
	clock  Clock

	// trash is this Session's trash can (SPEC.md §7), from
	// SessionOptions.Trash. If it is nil, trashHook does nothing (see
	// apply.go), so tests that ignore trash can use a Session without one.
	trash *Trash

	// warningsMu/warnings record every LocallyModifiedWarning this Session
	// has raised (the receive-only "flagged as locally modified" UI warning in
	// SPEC.md §1/§5). The API layer reads them back. warnIndex keys warnings by
	// share+relpath, so a file that stays diverged across many reconcile
	// passes occupies one entry instead of one per pass. See recordWarning.
	warningsMu sync.Mutex
	warnings   []LocallyModifiedWarning
	warnIndex  map[string]int

	logger *log.Logger

	// peerLabel prefixes every logf line for this session. See
	// SessionOptions.PeerLabel.
	peerLabel string

	// debug enables debugf. See SessionOptions.Debug.
	debug bool

	// The hooks from SessionOptions. Each may be nil. They are fixed at
	// construction, so the read loop reads them without a lock.
	onControl func(typ protocol.MsgType, payload []byte)
	onFrame   func(typ protocol.MsgType)
	onApplied func(shareID string)

	sharesMu   sync.RWMutex
	shares     map[string]*ShareConfig
	snapshotMu sync.Mutex
	snapshots  map[string]protocol.IndexSnapshotBegin
	sentMu     sync.Mutex
	sentIndex  map[string]sentIndexState

	// reconcileMu serializes Reconcile+apply+index-update passes across the
	// whole session (all shares). The read loop handles IndexUpdates in their
	// own goroutines (handleIndexUpdate), because a pull would otherwise
	// deadlock against the read loop that delivers its chunks. Two reconcile
	// passes that race could double-apply or interleave badly, for example
	// when our reply to peer A triggers a fresh IndexUpdate from peer A while a
	// second one is still in flight. Locking the whole pass is the simplest
	// correct fix. It does not block transfers: a pass holds the lock only
	// long enough to hand off to pullFile, and the byte transfer waits on the
	// read loop independently.
	reconcileMu sync.Mutex

	pullMu       sync.Mutex
	pullSem      chan struct{}
	pullTbl      map[transferKey]*pullEntry
	serveSem     chan struct{}
	indexSem     chan struct{}
	indexMu      sync.Mutex
	indexJobs    chan string
	indexPending map[string]func()
	indexRunning map[string]bool
	serveMu      sync.Mutex
	serveCancel  map[string]context.CancelFunc

	// pullActive/pullPeak count concurrent pulls for tests (see
	// integration_test.go's interleaving test). In production they cost a
	// couple of atomic operations.
	pullActive      int32
	pullPeak        int32
	stats           sessionCounters
	staleLogMu      sync.Mutex
	lastStaleLog    time.Time
	staleSuppressed uint64

	// testServeDelay, if non-zero, is slept before serving each FileRequest.
	// A test sets it to slow transfers enough to observe real concurrency (see
	// integration_test.go). Only this package's tests set it.
	testServeDelay time.Duration

	// testPullStallTimeout, if non-zero, replaces pullStallTimeout, so a
	// test need not wait a real minute for a silent peer to time out.
	testPullStallTimeout time.Duration

	// testJournalPageSize, if non-zero, replaces journalPageSize, so a test
	// need not journal 20,000 changes to see a second page.
	testJournalPageSize int

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// done is closed when the read loop exits, which is when this session
	// stops being usable: the peer closed the connection, the socket broke, or
	// a frame could not be read. See [Session.Done]. The owner (internal/core's
	// peer manager) watches it to act on a disconnect when it happens, not
	// when the keepalive's dead timer next notices.
	done        chan struct{}
	lifecycleMu sync.Mutex
	lifecycle   sessionLifecycle
}

type sessionLifecycle uint8

// sentIndexState is the highest index position this live session has put
// on the connection for one share. Store high-water marks are not enough,
// because they can include a local change queued after the last frame the
// peer received.
type sentIndexState struct {
	epoch      string
	high       uint64
	snapshotID string
}

type sessionCounters struct {
	protocolViolations, rejectedShareOperations, rejectedWork atomic.Uint64
	staleTransferFrames, pullsStarted, servesStarted          atomic.Uint64
	bytesReceived, bytesSent, transferStalls                  atomic.Uint64
	transferCancellations, hashFailures                       atomic.Uint64
	snapshotsSent, snapshotEntriesSent                        atomic.Uint64
	deltaBatchesSent, deltaEntriesSent, reconciliations       atomic.Uint64
	reconciliationsCoalesced                                  atomic.Uint64
}

// SessionStats is a low-cardinality snapshot suitable for status and metrics.
// Counters contain no share IDs, paths, or peer-provided labels.
type SessionStats struct {
	IndexWorkersActive, IndexQueueDepth, IndexQueueCapacity int
	ServeWorkersActive, PullsActive                         int
	ProtocolViolations, RejectedShareOperations             uint64
	RejectedWork, StaleTransferFrames                       uint64
	PullsStarted, ServesStarted                             uint64
	BytesReceived, BytesSent                                uint64
	TransferStalls, TransferCancellations, HashFailures     uint64
	SnapshotsSent, SnapshotEntriesSent                      uint64
	DeltaBatchesSent, DeltaEntriesSent, Reconciliations     uint64
	ReconciliationsCoalesced                                uint64
	Writer                                                  protocol.StreamWriterStats
}

// Stats returns a race-safe point-in-time session snapshot.
func (s *Session) Stats() SessionStats {
	s.indexMu.Lock()
	indexQueueDepth := len(s.indexPending)
	s.indexMu.Unlock()
	return SessionStats{
		IndexWorkersActive: len(s.indexSem), IndexQueueDepth: indexQueueDepth, IndexQueueCapacity: maxPendingIndexKeys,
		ServeWorkersActive: len(s.serveSem), PullsActive: len(s.pullSem),
		ProtocolViolations: s.stats.protocolViolations.Load(), RejectedShareOperations: s.stats.rejectedShareOperations.Load(),
		RejectedWork: s.stats.rejectedWork.Load(), StaleTransferFrames: s.stats.staleTransferFrames.Load(),
		PullsStarted: s.stats.pullsStarted.Load(), ServesStarted: s.stats.servesStarted.Load(),
		BytesReceived: s.stats.bytesReceived.Load(), BytesSent: s.stats.bytesSent.Load(),
		TransferStalls: s.stats.transferStalls.Load(), TransferCancellations: s.stats.transferCancellations.Load(), HashFailures: s.stats.hashFailures.Load(),
		SnapshotsSent: s.stats.snapshotsSent.Load(), SnapshotEntriesSent: s.stats.snapshotEntriesSent.Load(),
		DeltaBatchesSent: s.stats.deltaBatchesSent.Load(), DeltaEntriesSent: s.stats.deltaEntriesSent.Load(), Reconciliations: s.stats.reconciliations.Load(),
		ReconciliationsCoalesced: s.stats.reconciliationsCoalesced.Load(),
		Writer:                   s.writer.Stats(),
	}
}

func (s *Session) protocolViolation() { s.stats.protocolViolations.Add(1) }

// RecordProtocolViolation lets the core control-message dispatcher account
// for invalid post-handshake frames handled outside Session's data-plane table.
func (s *Session) RecordProtocolViolation() { s.protocolViolation() }

func (s *Session) rejectShareOperation() {
	s.stats.rejectedShareOperations.Add(1)
}

// RecordRejectedShareOperation accounts for a control-plane share request
// rejected before it becomes active on this session.
func (s *Session) RecordRejectedShareOperation() { s.rejectShareOperation() }
func (s *Session) rejectWork()                   { s.stats.rejectedWork.Add(1) }

func (s *Session) decodeWire(payload []byte, dst any) bool {
	if err := protocol.DecodeAndValidateMessage(payload, dst); err != nil {
		s.protocolViolation()
		return false
	}
	return true
}

const (
	sessionNew sessionLifecycle = iota
	sessionRunning
	sessionClosed
)

var (
	ErrSessionAlreadyStarted = errors.New("sync: session has already been started")
	ErrSessionClosed         = errors.New("sync: session is closed")
)

// SessionOptions configures a Session. Every field is optional, and the
// zero value gives a Session that logs to log.Default(), uses time.Now, and
// has no trash and no hooks.
type SessionOptions struct {
	// Clock is the time source (see reconcile.go's Clock). nil means time.Now.
	Clock Clock

	// Logger receives this session's log lines. nil means log.Default().
	Logger *log.Logger

	// PeerLabel prefixes every log line in place of the peer's ShortID.
	// internal/core passes the peer's configured name, so sync's log lines
	// match core's without cross-referencing `syncat peer ls`.
	PeerLabel string

	// Debug enables debugf, the routine-activity log (per-pass reconcile
	// summaries). These lines fire on every index update from every peer,
	// which helps when diagnosing a share that will not converge and is too
	// noisy for an idle daemon.
	Debug bool

	// Trash is where this session moves files it deletes or overwrites
	// (SPEC.md §7). nil makes trashHook do nothing.
	Trash *Trash

	// OnControl is called, synchronously from the read loop, for every frame
	// type this Session does not handle itself: Hello, Auth, ShareList,
	// SubscribeRequest, AccessUpdate, Ping, and Pong (see readLoop).
	// internal/core uses it to drive the peering flow (SPEC.md §2, §4, §6)
	// on the connection Session reads exclusively. It runs on the read loop
	// goroutine like every other dispatch here, so it should not block for
	// long. Start a goroutine for anything that does I/O.
	OnControl func(typ protocol.MsgType, payload []byte)

	// OnFrame is called once per frame read, of any type, before dispatch.
	// internal/core uses it to drive protocol.Keepalive.RecordReceived.
	// SPEC.md §4's "90s without traffic" dead-connection rule counts every
	// message, not just Ping/Pong, and only Session's read loop sees the
	// IndexUpdate/FileRequest/FileChunk/Error traffic that OnControl never
	// receives.
	OnFrame func(typ protocol.MsgType)

	// OnApplied is called after a reconcile pass that changed something on
	// disk, naming the share it changed. internal/core uses it to fan that
	// change out to the share's *other* peers.
	//
	// Session cannot do that itself, because it owns one connection and
	// knows nothing about the others. At the end of handleIndexUpdate it
	// sends a delta back to the peer it just heard from, which is enough for
	// a two-node pairing. It is not enough for a share offered to more than
	// one peer. SPEC.md §5's fan-out rule routes those through the offerer,
	// so when bob's change reaches alice, alice has to pass it to carol and
	// nothing else will. The rescan that internal/core triggers from
	// fsnotify compares the working tree against the index. Applying a
	// pulled change updates both, so that scan finds no difference and stops
	// before it would propagate. The periodic rescan finds the same nothing,
	// so it never self-heals either.
	//
	// It is called from the goroutine handling the peer's update, not the
	// read loop, but it should still hand off anything slow, because it runs
	// while reconcileMu is held for that share.
	OnApplied func(shareID string)
}

// NewSession constructs a Session over conn. nodeID is this node's own
// ShortID, used to bump version vectors on conflict resolution. peerID is
// the ShortID of the node at the other end of conn, and keys its mirrored
// index rows (index.Store.UpsertPeerFiles/ListPeerFiles).
func NewSession(conn net.Conn, store *index.Store, nodeID, peerID string, opts SessionOptions) *Session {
	clock := opts.Clock
	if clock == nil {
		clock = time.Now
	}
	logger := opts.Logger
	if logger == nil {
		logger = log.Default()
	}
	peerLabel := peerID
	if opts.PeerLabel != "" {
		peerLabel = protocol.SanitizeDiagnostic(opts.PeerLabel)
	}
	return &Session{
		conn:         conn,
		reader:       protocol.NewReader(conn),
		writer:       protocol.NewStreamWriter(conn, 0),
		store:        store,
		nodeID:       nodeID,
		peerID:       peerID,
		peerLabel:    peerLabel,
		debug:        opts.Debug,
		trash:        opts.Trash,
		onControl:    opts.OnControl,
		onFrame:      opts.OnFrame,
		onApplied:    opts.OnApplied,
		clock:        clock,
		logger:       logger,
		warnIndex:    make(map[string]int),
		shares:       make(map[string]*ShareConfig),
		snapshots:    make(map[string]protocol.IndexSnapshotBegin),
		sentIndex:    make(map[string]sentIndexState),
		pullSem:      make(chan struct{}, maxConcurrentPulls),
		pullTbl:      make(map[transferKey]*pullEntry),
		serveSem:     make(chan struct{}, maxConcurrentServes),
		indexSem:     make(chan struct{}, maxConcurrentIndexJobs),
		indexJobs:    make(chan string, maxPendingIndexKeys),
		indexPending: make(map[string]func()),
		indexRunning: make(map[string]bool),
		serveCancel:  make(map[string]context.CancelFunc),
		done:         make(chan struct{}),
	}
}

// startBounded starts fn only after reserving capacity. The single read
// loop calls it, so a failed non-blocking reservation is an explicit
// overload decision, not another hidden queue of goroutines.
func (s *Session) startBounded(sem chan struct{}, fn func()) bool {
	select {
	case sem <- struct{}{}:
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer func() { <-sem }()
			fn()
		}()
		return true
	default:
		return false
	}
}

// enqueueIndex schedules one coalescible index operation. At most one job
// for a key runs at once. While it runs, repeated notifications replace a
// single pending follow-up instead of creating waiters or disconnecting
// the peer.
func (s *Session) enqueueIndex(key string, fn func()) bool {
	s.indexMu.Lock()
	defer s.indexMu.Unlock()
	if _, exists := s.indexPending[key]; exists {
		s.indexPending[key] = fn
		s.stats.reconciliationsCoalesced.Add(1)
		return true
	}
	if len(s.indexPending) >= maxPendingIndexKeys {
		return false
	}
	s.indexPending[key] = fn
	if s.indexRunning[key] {
		return true
	}
	select {
	case s.indexJobs <- key:
		return true
	case <-s.ctx.Done():
		delete(s.indexPending, key)
		return false
	default:
		delete(s.indexPending, key)
		return false
	}
}

func (s *Session) runIndexWorker() {
	defer s.wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		case key := <-s.indexJobs:
			s.indexMu.Lock()
			fn, ok := s.indexPending[key]
			if ok {
				delete(s.indexPending, key)
				s.indexRunning[key] = true
			}
			s.indexMu.Unlock()
			if !ok {
				continue
			}

			select {
			case s.indexSem <- struct{}{}:
				fn()
				<-s.indexSem
			case <-s.ctx.Done():
				return
			}

			s.indexMu.Lock()
			delete(s.indexRunning, key)
			if _, pending := s.indexPending[key]; pending {
				s.indexJobs <- key // capacity is reserved by maxPendingIndexKeys
			}
			s.indexMu.Unlock()
		}
	}
}

// AddShare registers a share this Session keeps in sync with the peer.
// Safe to call before or after Start.
func (s *Session) AddShare(cfg ShareConfig) {
	c := cfg
	s.sharesMu.Lock()
	s.shares[cfg.ShareID] = &c
	s.sharesMu.Unlock()
}

// Writer returns this Session's protocol.StreamWriter, so internal/core's
// peer manager can send ShareList / SubscribeRequest / AccessUpdate / Ping
// frames on the same connection where Session writes
// IndexUpdate/FileRequest/FileChunk/Error. It is safe for concurrent use
// and never tears or interleaves frames. All of those are control frames,
// which it drains ahead of file bytes, so a caller never waits behind a
// transfer. That is what lets the read loop reply to a Ping inline (see
// SessionOptions.OnControl).
func (s *Session) Writer() *protocol.StreamWriter {
	return s.writer
}

// LocallyModifiedWarning records one receive-only "locally modified" event
// (SPEC.md §1, §5): a subscriber's local edit that diverged from the
// offerer under a receive-only subscription. Reverted reports whether the
// offerer already had content to revert to (a trash copy was taken and the
// offerer's version installed). When it is false, the divergence was only
// flagged because there was nothing to revert to, as with a local-only
// file the offerer does not have (see applyLocallyModified in apply.go).
// Session keeps a log of these (Session.LocallyModifiedWarnings) for the
// API and UI to show.
type LocallyModifiedWarning struct {
	ShareID  string
	RelPath  string
	At       time.Time
	Reverted bool
	Reason   string
}

// LocallyModifiedWarnings returns a snapshot of every LocallyModifiedWarning
// recorded so far, oldest first.
func (s *Session) LocallyModifiedWarnings() []LocallyModifiedWarning {
	s.warningsMu.Lock()
	defer s.warningsMu.Unlock()
	out := make([]LocallyModifiedWarning, len(s.warnings))
	copy(out, s.warnings)
	return out
}

// recordWarning records w, replacing any previous warning for the same
// share and relpath. It reports whether this is a *new* divergence, one
// with no warning already standing.
//
// The dedup does more than tidy. A locally-modified file is a standing
// condition, not an event. Every reconcile pass re-detects it for as long
// as it stays diverged, and a receive-only share with a handful of stray
// files reaches thousands of entries in an idle afternoon. Appending each
// one grew this slice without bound for the life of the connection and
// made the API repeat the same file once per pass in the UI's warning
// list. Keeping the newest warning per path gives the UI the set of
// currently-diverged files, which is what it was meant to show.
//
// apply.go uses the returned bool to log a divergence once, when it
// starts, instead of on every pass. See its call sites.
func (s *Session) recordWarning(w LocallyModifiedWarning) bool {
	key := w.ShareID + "\x00" + w.RelPath
	s.warningsMu.Lock()
	defer s.warningsMu.Unlock()
	if i, ok := s.warnIndex[key]; ok {
		s.warnings[i] = w
		return false
	}
	s.warnIndex[key] = len(s.warnings)
	s.warnings = append(s.warnings, w)
	return true
}

// retainWarnings drops every standing warning for shareID whose relpath is
// not in stillFlagged, and reports how many it dropped.
//
// A reconcile pass sees the whole share, so the paths it flags as locally
// modified are the complete set of currently-diverged files for that
// share. Anything holding a warning from an earlier pass but absent here
// has converged (the user reverted their edit, or the file was deleted),
// and its warning is stale. Without this the UI's warning list only grew:
// a divergence that resolved itself stayed on screen for the life of the
// connection.
//
// It runs once per pass instead of once per file, so the whole
// reconciliation takes one lock acquisition, not one per path in the
// share.
func (s *Session) retainWarnings(shareID string, stillFlagged map[string]bool) int {
	s.warningsMu.Lock()
	defer s.warningsMu.Unlock()

	kept := s.warnings[:0]
	dropped := 0
	for _, w := range s.warnings {
		if w.ShareID == shareID && !stillFlagged[w.RelPath] {
			dropped++
			continue
		}
		kept = append(kept, w)
	}
	if dropped == 0 {
		return 0
	}
	s.warnings = kept
	// Positions shifted; rebuild the index rather than patching it.
	s.warnIndex = make(map[string]int, len(s.warnings))
	for i, w := range s.warnings {
		s.warnIndex[w.ShareID+"\x00"+w.RelPath] = i
	}
	return dropped
}

func (s *Session) getShare(shareID string) (ShareConfig, bool) {
	s.sharesMu.RLock()
	defer s.sharesMu.RUnlock()
	cfg, ok := s.shares[shareID]
	if !ok {
		return ShareConfig{}, false
	}
	return *cfg, true
}

// acceptsInboundIndex is the admission gate for peer-supplied index state.
// Session.shares is the authorization source. Database rows and a wire
// share ID never confer access by themselves.
func (s *Session) acceptsInboundIndex(shareID string) bool {
	cfg, ok := s.getShare(shareID)
	return ok && !cfg.Direction.InboundBlocked
}

// servesOutboundShare is the admission gate for requests for our index or
// file bytes. Receive-only shares exist locally but must never disclose
// their local state to the peer.
func (s *Session) servesOutboundShare(shareID string) bool {
	cfg, ok := s.getShare(shareID)
	return ok && !cfg.Direction.OutboundBlocked
}

func (s *Session) noteSnapshotWritten(shareID, epoch, snapshotID string, high uint64) {
	s.sentMu.Lock()
	defer s.sentMu.Unlock()
	cur := s.sentIndex[shareID]
	if cur.epoch != epoch || high >= cur.high {
		s.sentIndex[shareID] = sentIndexState{epoch: epoch, high: high, snapshotID: snapshotID}
	}
}

func (s *Session) noteDeltaWritten(shareID, epoch string, high uint64) {
	s.sentMu.Lock()
	defer s.sentMu.Unlock()
	cur := s.sentIndex[shareID]
	if cur.epoch != epoch || high >= cur.high {
		s.sentIndex[shareID] = sentIndexState{epoch: epoch, high: high}
	}
}

// acceptIndexAck advances the persistent cursor only to the newest index
// endpoint this live session wrote. Requiring the exact newest endpoint
// keeps the admission state constant-sized. An older legitimate ack may be
// ignored, because the peer will also acknowledge the newer frame it has
// already received.
func (s *Session) acceptIndexAck(m protocol.IndexAck) bool {
	s.sentMu.Lock()
	sent, ok := s.sentIndex[m.ShareID]
	s.sentMu.Unlock()
	if !ok || m.Epoch != sent.epoch || m.AppliedSeq != sent.high || m.SnapshotID != sent.snapshotID {
		return false
	}
	cur, err := s.store.Cursor(s.ctx, s.peerID, m.ShareID, "outgoing")
	if err != nil || (cur.Epoch == m.Epoch && cur.AppliedSeq >= m.AppliedSeq) {
		return false
	}
	return s.store.SetCursor(s.ctx, s.peerID, m.ShareID, "outgoing", index.Cursor{
		Epoch: m.Epoch, AppliedSeq: m.AppliedSeq,
	}) == nil
}

// Start runs the session's read loop and its writer in background
// goroutines and returns immediately. ctx bounds the session's lifetime in
// addition to Close: canceling ctx (or calling Close) stops all background
// work.
//
// Writes before Start fail with protocol.ErrWriterNotStarted. Start
// launches the writer, not NewSession, so a Session built for something
// other than driving a connection never starts a goroutine. internal/sync's
// own tests do this, building one over a nil conn to reach its trash
// helpers.
func (s *Session) Start(ctx context.Context) error {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	switch s.lifecycle {
	case sessionRunning:
		return ErrSessionAlreadyStarted
	case sessionClosed:
		return ErrSessionClosed
	}
	s.ctx, s.cancel = context.WithCancel(ctx)
	if err := s.writer.Start(); err != nil {
		s.cancel()
		return err
	}
	s.lifecycle = sessionRunning
	s.wg.Add(maxConcurrentIndexJobs)
	for range maxConcurrentIndexJobs {
		go s.runIndexWorker()
	}
	s.wg.Add(1)
	go s.readLoop()
	return nil
}

// Done returns a channel closed when the session's read loop has exited:
// the peer closed the connection, the socket broke, or Close was called.
// It is the session's own "this connection is finished" signal. The owner
// should watch it, because nothing else in the session reacts to a
// disconnect. A caller that waits for protocol.Keepalive's dead timer
// instead pays SPEC.md §4's full 90s before it reconnects, even when the
// peer said goodbye cleanly and immediately.
//
// The channel is never closed for a Session that was never started.
func (s *Session) Done() <-chan struct{} {
	return s.done
}

// Close stops the session. It cancels the internal context, closes the
// underlying connection (unblocking any pending Read in the read loop),
// stops the writer, and waits for every goroutine Session started to
// exit. Safe to call more than once.
func (s *Session) Close() error {
	s.lifecycleMu.Lock()
	if s.lifecycle == sessionClosed {
		s.lifecycleMu.Unlock()
		s.wg.Wait()
		return nil
	}
	s.lifecycle = sessionClosed
	if s.cancel != nil {
		s.cancel()
	}
	// Close the connection before the writer. Closing it unblocks a write
	// already in progress, and StreamWriter.Close waits for that goroutine to
	// return.
	if s.conn != nil {
		_ = s.conn.Close()
	}
	_ = s.writer.Close()
	s.lifecycleMu.Unlock()
	s.wg.Wait()
	return nil
}

func (s *Session) logf(format string, args ...any) {
	s.logger.Printf("sync: peer %s: "+format, append([]any{s.peerLabel}, args...)...)
}

// debugf logs routine sync activity, suppressed unless SessionOptions.Debug was set.
func (s *Session) debugf(format string, args ...any) {
	if !s.debug {
		return
	}
	s.logf(format, args...)
}

// SyncShare sends a full snapshot IndexUpdate (Full: true) for shareID to
// the peer, unless the share's Direction blocks outbound updates
// (receive-only subscription). A caller uses it to start the initial sync
// after connecting, and internal/core's share watcher calls it (via
// propagateShare) to notify the peer of a local change.
func (s *Session) SyncShare(ctx context.Context, shareID string) error {
	cfg, ok := s.getShare(shareID)
	if !ok {
		return fmt.Errorf("sync: session: unknown share %s", shareID)
	}
	if !cfg.Direction.OutboundBlocked {
		out, err := s.store.Cursor(ctx, s.peerID, shareID, "outgoing")
		if err != nil {
			return err
		}
		return s.answerSyncRequest(ctx, protocol.IndexSyncRequest{ShareID: shareID, Epoch: out.Epoch, AppliedSeq: out.AppliedSeq})
	}
	c, err := s.store.Cursor(ctx, s.peerID, shareID, "incoming")
	if err != nil {
		return err
	}
	if err := s.writer.WriteLatestMessageContext(ctx, "index-sync\x00"+shareID, protocol.MsgIndexSyncRequest, protocol.IndexSyncRequest{ShareID: shareID, Epoch: c.Epoch, AppliedSeq: c.AppliedSeq, SnapshotID: c.SnapshotID, SnapshotBatch: c.SnapshotBatch}); err != nil {
		return err
	}
	return nil
}

func newSnapshotID() string { var b [16]byte; _, _ = rand.Read(b[:]); return hex.EncodeToString(b[:]) }

func (s *Session) answerSyncRequest(ctx context.Context, req protocol.IndexSyncRequest) error {
	// Re-check here as well as in readLoop. This runs asynchronously, and a
	// revocation can neuter the share after dispatch but before this goroutine
	// reaches the store.
	if !s.servesOutboundShare(req.ShareID) {
		return fmt.Errorf("sync: index request for inactive or outbound-blocked share %s", req.ShareID)
	}
	epoch, high, err := s.store.ShareState(ctx, req.ShareID)
	if err != nil {
		return err
	}
	oldest, err := s.store.OldestJournalSeq(ctx, req.ShareID, epoch)
	if err != nil {
		return err
	}
	cfg, _ := s.getShare(req.ShareID) // admission above guarantees it exists

	if canAnswerWithDeltas(req, epoch, high, oldest) {
		done, err := s.sendJournalDeltas(ctx, cfg, epoch, req.AppliedSeq)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		s.debugf("share %s: journal range holds a now-ignored path; answering with a full snapshot", req.ShareID)
	}
	rows, err := s.store.ListShare(ctx, req.ShareID, true)
	if err != nil {
		return err
	}
	// Ignored rows are parked, and ListShare already excludes those, so this
	// is normally a no-op. A rule added since the last rescan has not reached
	// the index yet, though, and a snapshot must never advertise a path this
	// node will refuse to serve.
	rows = filterIgnoredRows(cfg, rows)
	entryCount := len(rows)
	id := newSnapshotID()
	if err = s.writer.WriteMessage(protocol.MsgIndexSnapshotBegin, protocol.IndexSnapshotBegin{ShareID: req.ShareID, SnapshotID: id, Epoch: epoch, HighSeq: high}); err != nil {
		return err
	}
	var batch uint64
	for len(rows) > 0 {
		n := snapshotBatchLen(req.ShareID, id, batch, rows)
		if n == 0 {
			return fmt.Errorf("sync: index entry %s cannot fit in a frame", rows[0].RelPath)
		}
		files := make([]protocol.FileInfo, n)
		for i := range files {
			files[i] = rows[i].Info()
		}
		if err = s.writer.WriteMessage(protocol.MsgIndexSnapshotBatch, protocol.IndexSnapshotBatch{ShareID: req.ShareID, SnapshotID: id, Batch: batch, Files: files}); err != nil {
			return err
		}
		batch++
		rows = rows[n:]
	}
	return s.writer.WriteMessageOnWritten(protocol.MsgIndexSnapshotEnd, protocol.IndexSnapshotEnd{ShareID: req.ShareID, SnapshotID: id, BatchCount: batch}, func() {
		s.noteSnapshotWritten(req.ShareID, epoch, id, high)
		s.stats.snapshotsSent.Add(1)
		s.stats.snapshotEntriesSent.Add(uint64(entryCount))
	})
}

// journalPageSize bounds how many journal rows one read holds in memory at
// a time.
//
// This is a memory bound, not a protocol one. The peer acknowledges each
// batch as it lands, so where one read ends and the next begins is
// invisible on the wire. The page is sized to fill a delta batch
// (protocol.TargetIndexBatchSize) for typical entries, so paging costs no
// extra frames in practice. A share with a long journal no longer
// materialises tens of thousands of rows, version-vector maps included,
// in one slice for each peer that asks to resync.
const journalPageSize = 20000

// sendJournalDeltas walks the journal from applied to the end, sending each
// page as one or more IndexDeltaBatch frames. done is false when the range
// holds a path this share now ignores, which means the caller must answer
// with a full snapshot instead.
//
// A delta batch cannot omit an entry. The receiver requires the entries to
// be contiguous from FromSeq (see readLoop), and Store.ApplyPeerDelta also
// requires FromSeq == cursor+1 and len(rows) == ToSeq-FromSeq+1. If one is
// dropped, the peer rejects the batch and resyncs, forever. So an ignored
// path anywhere in the range hands the whole answer to a snapshot, which
// has no per-entry sequence contract and re-anchors the peer's cursor at
// HighSeq. This happens only for a path that was journalled while still
// syncing and has since become ignored, with a peer whose cursor predates
// that entry. The snapshot moves that peer past it for good. Pages sent
// before the ignored entry was reached are not a problem, because the
// snapshot supersedes them.
func (s *Session) sendJournalDeltas(ctx context.Context, cfg ShareConfig, epoch string, applied uint64) (done bool, err error) {
	shareID := cfg.ShareID
	pageSize := journalPageSize
	if s.testJournalPageSize > 0 {
		pageSize = s.testJournalPageSize
	}
	for {
		rows, err := s.store.JournalSince(ctx, shareID, epoch, applied, pageSize)
		if err != nil {
			return false, err
		}
		if len(rows) == 0 {
			return true, nil
		}
		if journalHasIgnored(cfg, rows) {
			return false, nil
		}
		page := rows
		for len(page) > 0 {
			n := deltaBatchLen(shareID, epoch, page)
			if n == 0 {
				return false, fmt.Errorf("sync: journal entry %s cannot fit in a frame", page[0].Row.RelPath)
			}
			entries := make([]protocol.IndexDeltaEntry, n)
			for i := range entries {
				entries[i] = protocol.IndexDeltaEntry{Seq: page[i].Seq, File: page[i].Row.Info()}
			}
			m := protocol.IndexDeltaBatch{ShareID: shareID, Epoch: epoch, FromSeq: entries[0].Seq, ToSeq: entries[n-1].Seq, Entries: entries}
			if err := s.writer.WriteMessageOnWritten(protocol.MsgIndexDeltaBatch, m, func() {
				s.noteDeltaWritten(shareID, epoch, m.ToSeq)
				s.stats.deltaBatchesSent.Add(1)
				s.stats.deltaEntriesSent.Add(uint64(len(m.Entries)))
			}); err != nil {
				return false, err
			}
			page = page[n:]
		}
		if len(rows) < pageSize {
			return true, nil
		}
		applied = rows[len(rows)-1].Seq
	}
}

// canAnswerWithDeltas reports whether the journal can carry this peer from
// its cursor to ours, given the share's current epoch, its highest
// journalled sequence, and the oldest sequence still on hand (0 when the
// journal holds nothing for this epoch).
//
// Every clause is a way of *not* being able to. Each one falls back to a
// full snapshot, which re-anchors the peer's cursor unconditionally:
//
//   - A different epoch means the sequence numbers are not comparable.
//   - A peer claiming a sequence past our own high-water mark is not a
//     peer we can compute a delta for. This happens after restoring this
//     node's database from a backup, when every peer is ahead of it.
//   - An entry the peer needs may have aged out of the journal, which is
//     pruned on a retention window (see core's journal sweep). Requiring
//     oldest <= AppliedSeq+1 is what makes pruning safe. An oldest of 0
//     means an empty journal, which is "nothing to send" only when the
//     peer is already at high. Otherwise the entries it needs are gone.
func canAnswerWithDeltas(req protocol.IndexSyncRequest, epoch string, high, oldest uint64) bool {
	if req.Epoch != epoch || req.AppliedSeq > high {
		return false
	}
	if req.AppliedSeq == high {
		// Already current. The delta path sends nothing, which is correct and
		// cheaper than a snapshot the peer would discard.
		return true
	}
	return oldest > 0 && oldest <= req.AppliedSeq+1
}

// journalHasIgnored reports whether any journal entry in rows names a path
// this share now ignores. Such entries are historical. They were written
// while the path was still syncing and survive in change_journal after the
// file row was dropped, because dropping an ignored row is deliberately
// not journalled (see index.Store.ApplyScanResult).
func journalHasIgnored(cfg ShareConfig, rows []index.JournalRow) bool {
	if cfg.Ignore == nil {
		return false
	}
	for _, r := range rows {
		if cfg.ignored(r.Row.RelPath, r.Row.Type == protocol.FileTypeDir) {
			return true
		}
	}
	return false
}

// filterIgnoredRows drops rows for paths this share ignores. It returns
// rows unchanged when the share has no ignore rules, the common case,
// which avoids an allocation.
func filterIgnoredRows(cfg ShareConfig, rows []index.FileRow) []index.FileRow {
	if cfg.Ignore == nil {
		return rows
	}
	out := make([]index.FileRow, 0, len(rows))
	for _, r := range rows {
		if cfg.ignored(r.RelPath, r.Type == protocol.FileTypeDir) {
			continue
		}
		out = append(out, r)
	}
	return out
}

// batchArrayHeaderSlack covers the one part of a batch envelope that grows
// with the number of entries in it. CBOR writes an array header of 1 byte
// for up to 23 elements, 2 up to 255, 3 up to 65535 and 5 beyond, while
// the envelope below is measured with an empty array (1 byte). Four bytes
// is therefore the exact worst case. Eight leaves room and costs at most
// one entry at a frame boundary.
const batchArrayHeaderSlack = 8

// snapshotBatchLen reports how many of rows fit in one IndexSnapshotBatch
// frame, and 0 if even the first one does not.
//
// It measures the fixed envelope once and then adds each row's own encoded
// length. CBOR array elements are self-delimiting values laid down back to
// back, so an entry contributes exactly its own encoding wherever it sits
// in the array. The obvious alternative is to re-encode the whole growing
// message after each candidate row and look at the total. That is
// quadratic, and not mildly so: planning one snapshot of a 20k-file share
// that way allocated 28 GB and took 77 seconds, while the same plan here
// is one encode per row. Any whole-message encode inside this loop brings
// that cost back.
func snapshotBatchLen(share, id string, b uint64, rows []index.FileRow) int {
	envelope, err := protocol.EncodedMessageSize(protocol.MsgIndexSnapshotBatch,
		protocol.IndexSnapshotBatch{ShareID: share, SnapshotID: id, Batch: b, Files: []protocol.FileInfo{}})
	if err != nil {
		return 0
	}
	total := envelope + batchArrayHeaderSlack
	for i, r := range rows {
		n, e := protocol.EncodedEntrySize(r.Info())
		if e != nil || total+n > protocol.TargetIndexBatchSize {
			return i
		}
		total += n
	}
	return len(rows)
}

// deltaBatchLen is snapshotBatchLen for an IndexDeltaBatch. Its envelope is
// measured with the *last* row's sequence number as ToSeq. Sequence
// numbers ascend and CBOR's integer width grows with the value, so
// whatever ToSeq the chosen batch ends on cannot encode wider than that.
func deltaBatchLen(share, epoch string, rows []index.JournalRow) int {
	if len(rows) == 0 {
		return 0
	}
	envelope, err := protocol.EncodedMessageSize(protocol.MsgIndexDeltaBatch,
		protocol.IndexDeltaBatch{
			ShareID: share, Epoch: epoch,
			FromSeq: rows[0].Seq, ToSeq: rows[len(rows)-1].Seq,
			Entries: []protocol.IndexDeltaEntry{},
		})
	if err != nil {
		return 0
	}
	total := envelope + batchArrayHeaderSlack
	for i, r := range rows {
		n, e := protocol.EncodedEntrySize(protocol.IndexDeltaEntry{Seq: r.Seq, File: r.Row.Info()})
		if e != nil || total+n > protocol.TargetIndexBatchSize {
			return i
		}
		total += n
	}
	return len(rows)
}

func (s *Session) sendIndexUpdate(shareID string, rows []index.FileRow, full bool) error {
	// This is the single funnel for MsgIndexUpdate, so the "never advertise
	// an ignored path" guarantee belongs here even though upstream code
	// already filters the rows that reach it.
	cfg, _ := s.getShare(shareID)
	rows = filterIgnoredRows(cfg, rows)

	files := make([]protocol.FileInfo, len(rows))
	for i, r := range rows {
		files[i] = r.Info()
	}
	if err := s.writer.WriteMessage(protocol.MsgIndexUpdate, protocol.IndexUpdate{
		ShareID: shareID, Files: files, Full: full,
	}); err != nil {
		return fmt.Errorf("sync: session: send index update for %s: %w", shareID, err)
	}
	return nil
}

// readLoop is the single reader of the connection (protocol.Reader is not
// safe for concurrent use). It observes each frame and routes it through
// the data-plane handler table or the core control callback. Individual
// handlers hand network-blocking or receive-dependent work to bounded
// workers, so no background operation waits on this loop. That would
// deadlock a pull against the loop that delivers its chunks.
//
// The control handler may enqueue a Pong inline. That is safe because
// urgent liveness writes cannot queue behind FileChunks, and no inline
// handler writes the socket directly.
//
// On exit, for any reason including a clean io.EOF, it closes s.done,
// which is how the owner learns the connection is over ([Session.Done]).
func (s *Session) readLoop() {
	defer s.wg.Done()
	defer close(s.done)
	for {
		typ, payload, err := s.reader.ReadFrame()
		if err != nil {
			return
		}

		if s.onFrame != nil {
			s.onFrame(typ)
		}
		if handler, ok := sessionFrameHandlers[typ]; ok {
			if !handler(s, payload) {
				return
			}
			continue
		}

		// Handshake/share-list/access/ping-pong messages are owned by the
		// core control plane. Session remains the connection's sole reader.
		if s.onControl != nil {
			s.onControl(typ, payload)
		}
	}
}

func (s *Session) reconcilePeerShare(ctx context.Context, shareID string) {
	if err := s.ReconcileShare(ctx, shareID); err != nil {
		s.logf("reconcile peer share %s: %v", shareID, err)
	}
}

// handleIndexUpdate is the core reconcile-and-apply pass, run under the
// read loop's bounded index-worker admission (see readLoop). It folds the
// peer's reported files into our mirror of their view (peer_files),
// reconciles that against our own current view ([Reconcile]), and executes
// every resulting Action against the filesystem and index. Unless this
// share's direction blocks outbound updates, it then reports back what
// changed locally so the peer's own reconcile pass converges too.
func (s *Session) handleIndexUpdate(ctx context.Context, msg protocol.IndexUpdate) {
	cfg, ok := s.getShare(msg.ShareID)
	if !ok {
		s.logf("index update for unknown share %s", msg.ShareID)
		return
	}
	if cfg.Direction.InboundBlocked {
		// Offerer of a read-only share: incoming changes are ignored outright
		// (SPEC.md §5), and the peer's reported state is not recorded at all.
		return
	}

	now := time.Now()
	peerRows := make([]index.FileRow, len(msg.Files))
	for i, f := range msg.Files {
		peerRows[i] = index.FileRowFromInfo(msg.ShareID, f, now)
	}

	s.reconcileMu.Lock()
	defer s.reconcileMu.Unlock()

	if err := s.store.UpsertPeerFiles(ctx, s.peerID, peerRows); err != nil {
		s.logf("upsert peer files for %s: %v", msg.ShareID, err)
		return
	}

	s.reconcileLocked(ctx, cfg, fmt.Sprintf("%d file(s) in peer update", len(msg.Files)))
}

// ReconcileShare runs a reconcile pass for shareID against what this session
// already knows about the peer, without waiting for the peer to send
// anything.
//
// handleIndexUpdate is otherwise the only thing that reconciles, so "the
// peer sent us rows" is the only trigger for noticing that a file is
// missing. That is not the only way to end up owing work. A pull can fail
// because the connection drops mid-transfer, the transfer stalls, or the
// write errors. That leaves the peer's rows already recorded in peer_files
// and the file still absent locally, and nothing revisits it. The
// offerer's index sync is incremental, so on reconnect its cursor says
// this peer already has those rows and it sends nothing. The reconciler
// never runs, and the file stays missing indefinitely.
//
// Measured on a 192 MiB transfer interrupted by killing the receiver: the
// file did not arrive on its own after two minutes on a healthy
// reconnected connection. It arrived within five seconds of an *unrelated*
// file being touched in the same share, because that finally produced an
// index update. Calling this when a share becomes active on a session
// closes that gap. Reconnecting is itself a reason to re-examine.
func (s *Session) ReconcileShare(ctx context.Context, shareID string) error {
	cfg, ok := s.getShare(shareID)
	if !ok {
		return fmt.Errorf("sync: reconcile share: %s is not active on this session", shareID)
	}
	if cfg.Direction.InboundBlocked {
		// Offerer of a read-only share: it never applies anything inbound, so a
		// reconcile has nothing to do (SPEC.md §5).
		return nil
	}
	s.reconcileMu.Lock()
	defer s.reconcileMu.Unlock()
	s.reconcileLocked(ctx, cfg, "reconnect")
	return nil
}

// reconcileLocked is the reconcile-and-apply pass shared by handleIndexUpdate
// and ReconcileShare. Callers must hold reconcileMu and must have already
// recorded any newly received peer rows. why names what prompted the pass,
// for the debug line.
func (s *Session) reconcileLocked(ctx context.Context, cfg ShareConfig, why string) {
	s.stats.reconciliations.Add(1)
	shareID := cfg.ShareID
	localRows, err := s.store.ListShare(ctx, shareID, true)
	if err != nil {
		s.logf("list share %s: %v", shareID, err)
		return
	}
	remoteRows, err := s.store.ListPeerFiles(ctx, s.peerID, shareID)
	if err != nil {
		s.logf("list peer files %s: %v", shareID, err)
		return
	}

	actions := Reconcile(rowsToInfos(localRows), rowsToInfos(remoteRows), s.nodeID, cfg.Direction, s.clock, cfg.Ignore)

	var mu sync.Mutex
	var changed []index.FileRow
	counts := map[ActionKind]int{}
	flagged := make(map[string]bool)
	for _, a := range actions {
		if a.Kind == ActionLocallyModified {
			flagged[a.RelPath] = true
		}
		if a.Kind != ActionNone {
			counts[a.Kind]++
		}
	}
	s.runActionWorkers(ctx, cfg, actions, func(_ Action, rows []index.FileRow) {
		if len(rows) == 0 {
			return
		}
		mu.Lock()
		changed = append(changed, rows...)
		mu.Unlock()
	})

	// Every path in the share was just reconciled, so flagged is the complete
	// current set of locally-modified files for it. Anything still holding a
	// warning from an earlier pass has converged.
	s.retainWarnings(shareID, flagged)

	if !cfg.Direction.OutboundBlocked && len(changed) > 0 {
		if err := s.sendIndexUpdate(shareID, changed, false); err != nil {
			s.logf("send delta for %s: %v", shareID, err)
		}
	}

	// The delta above only reaches the peer this update came from. Every
	// *other* peer of this share learns about it here. See
	// SessionOptions.OnApplied for why nothing else would tell them.
	if len(changed) > 0 && s.onApplied != nil {
		s.onApplied(shareID)
	}

	// A pass over an already-converged share produces nothing but ActionNone,
	// one per file, on every update from every peer. Log only when the pass
	// did something.
	if len(counts) > 0 {
		s.debugf("reconciled %s (%s): %s", shareID, why, formatActionCounts(counts))
	}
}

// runActionWorkers applies an arbitrarily large reconciliation plan with a
// fixed number of goroutines. The caller waits for completion, and the
// goroutine count does not depend on the number of peer-controlled file
// entries.
func (s *Session) runActionWorkers(ctx context.Context, cfg ShareConfig, actions []Action, consume func(Action, []index.FileRow)) {
	workers := maxConcurrentPulls
	if len(actions) < workers {
		workers = len(actions)
	}
	jobs := make(chan Action)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for a := range jobs {
				consume(a, s.applyAndPersist(ctx, cfg, a))
			}
		}()
	}
	for _, a := range actions {
		select {
		case jobs <- a:
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return
		}
	}
	close(jobs)
	wg.Wait()
}

// formatActionCounts renders a reconcile pass's non-ActionNone tally in a
// stable order, e.g. "Pull=3 Delete=1" (the names come from
// ActionKind.String). It uses a fixed order instead of ranging the map, so
// repeated passes produce comparable, diffable lines.
func formatActionCounts(counts map[ActionKind]int) string {
	order := []ActionKind{ActionPull, ActionDelete, ActionResurrect, ActionConflictCopy, ActionLocallyModified}
	parts := make([]string, 0, len(order))
	for _, k := range order {
		if n := counts[k]; n > 0 {
			parts = append(parts, fmt.Sprintf("%s=%d", k, n))
		}
	}
	return strings.Join(parts, " ")
}

// applyAndPersist executes one Action (applyAction) and writes every row it
// produced to the index. It returns the rows that were stored, which
// handleIndexUpdate reports back to the peer. It logs failures instead of
// returning them: one failing action must not stop the rest of the pass,
// and the caller can do nothing about it beyond the log.
//
// applyAction may return a non-nil error alongside non-empty rows. A
// conflict copy's two halves (a local rename and a network pull) can
// partially succeed (see applyConflictCopy's doc comment). Persist the rows
// that did land regardless of err, so a transient failure in one half
// never orphans a file that is already correctly on disk.
func (s *Session) applyAndPersist(ctx context.Context, cfg ShareConfig, a Action) []index.FileRow {
	rows, err := s.applyAction(ctx, cfg, a)
	if err != nil {
		s.logf("apply %s %s: %v", a.Kind, a.RelPath, err)
	}
	stored := make([]index.FileRow, 0, len(rows))
	for _, r := range rows {
		if err := s.store.PutFile(ctx, r); err != nil {
			s.logf("put file %s/%s: %v", r.ShareID, r.RelPath, err)
			continue
		}
		stored = append(stored, r)
	}
	return stored
}

func rowsToInfos(rows []index.FileRow) []protocol.FileInfo {
	out := make([]protocol.FileInfo, len(rows))
	for i, r := range rows {
		out[i] = r.Info()
	}
	return out
}

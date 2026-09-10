package sync

import (
	"context"
	"crypto/rand"
	"encoding/hex"
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
// peer"). Additional pulls queue on pullSem until a slot frees; chunks for
// every in-flight pull are interleaved on the one underlying stream
// (protocol.Writer is safe for concurrent writers — see message.go), which
// is what lets several transfers make progress at once without their own
// separate connections.
const maxConcurrentPulls = 4

// maxConcurrentServes bounds how many FileRequests from the peer we answer
// at once. SPEC.md §4 only specifies a limit on the puller side, but
// capping our own serving concurrency too keeps resource usage (open file
// descriptors, goroutines) bounded symmetrically instead of growing
// unboundedly with however many requests the peer happens to send at once.
const maxConcurrentServes = 4

// maxConcurrentIndexJobs bounds independently requested index work. Index
// updates themselves are serialized by reconcileMu, so a larger number only
// creates waiters and memory pressure without increasing useful throughput.
const maxConcurrentIndexJobs = 4

// --- the session: one authenticated peer connection --------------------

// ShareConfig is one share this Session keeps in sync with the peer at the
// other end of its connection: the local directory it maps to, and the
// Direction (SPEC.md §5) constraining which way changes may flow. Build
// Direction with DirectionFor rather than by hand.
type ShareConfig struct {
	ShareID   string
	Root      string // absolute local directory this share's files live under
	Direction Direction

	// Ignore reports whether a share-relative path is excluded from
	// syncing by this share's ignore rules — the built-ins, the global
	// ignore list, and the share's own .syncatignore (SPEC.md §5). nil
	// means nothing is ignored, which keeps the zero value usable.
	//
	// It is a closure rather than an *index.Matcher (which would compile
	// fine — internal/sync already imports internal/index) because
	// .syncatignore is re-read on every rescan: a snapshotted matcher
	// would go stale the moment the user edited the file, and nothing
	// re-adds a share when that happens. internal/core resolves this
	// against the share's current matcher on every call.
	Ignore func(relpath string, isDir bool) bool
}

// ignored is Ignore with the nil check folded in, since almost every call
// site is on a path where no ignore rules are configured at all.
func (c ShareConfig) ignored(relpath string, isDir bool) bool {
	return c.Ignore != nil && c.Ignore(relpath, isDir)
}

// Session drives sync for one already-authenticated peer connection:
// exchanging IndexUpdate for each configured share, pulling and serving
// file content, and applying the resulting actions ([Reconcile]'s output)
// to the local filesystem and index. One Session corresponds to
// one net.Conn (SPEC.md §4: a single stream carries every share synced
// with that peer) and can hold any number of shares.
//
// Session assumes internal/protocol's handshake and the share/access
// negotiation already happened, and handles only IndexUpdate, FileRequest,
// FileChunk, and file-transfer-scoped Error from that point on. Other
// message types arriving on the stream are handed to the control handler
// internal/core registers (see SetControlHandler), because driving those
// exchanges is the peer manager's job. This keeps Session fully driveable
// from a test with a bare net.Conn (e.g. transport.PipeTransport) and no
// real peer manager.
//
// Every background goroutine Session starts is tied to the context passed
// to Start and unwound by Close, which cancels that context, closes the
// connection (unblocking the read loop's pending Read), and waits for
// every goroutine to exit — so a Session never leaks goroutines across its
// own lifetime.
type Session struct {
	conn   net.Conn
	reader *protocol.Reader
	writer *protocol.StreamWriter

	store  *index.Store
	nodeID string // this node's ShortID
	peerID string // the peer's ShortID
	clock  Clock

	// trash is this Session's trash can (SPEC.md §7). Set via SetTrash;
	// nil until then, in which case trashHook is a no-op (see apply.go) —
	// so a Session is still fully usable in tests/contexts that don't
	// care about trash.
	trash *Trash

	// warningsMu/warnings record every LocallyModifiedWarning this Session
	// has raised (SPEC.md §1/§5's receive-only "flagged as locally
	// modified" UI warning), for a caller (the API layer) to read back.
	// warnIndex keys warnings by share+relpath so a file that stays
	// diverged across many reconcile passes occupies one entry rather
	// than one per pass — see recordWarning.
	warningsMu sync.Mutex
	warnings   []LocallyModifiedWarning
	warnIndex  map[string]int

	logger *log.Logger

	// peerLabel is what logf puts in front of every line for this session.
	// It defaults to peerID (an 8-byte hex ShortID) and is replaced by the
	// peer's configured name via SetPeerLabel, so sync's log lines can be
	// matched up with core's — which have always used the name — without
	// cross-referencing `syncat peer ls`.
	peerLabel string

	// debug enables debugf, the routine-activity log (per-pass reconcile
	// summaries). Off by default: these fire on every index update from
	// every peer, which is the right level of detail when diagnosing why a
	// share will not converge and far too much for a daemon at rest.
	debug bool

	// ctrlMu guards controlHandler/frameObserver, internal/core's hooks for
	// driving the parts of the connection lifecycle Session itself doesn't
	// own: ShareList/SubscribeRequest/AccessUpdate/Ping/Pong (see readLoop's
	// default case) and keepalive activity tracking (frameObserver fires
	// for every frame this Session reads, of any type, matching
	// protocol.Keepalive's "any frame counts as received traffic"
	// contract). Both are nil-safe: set them before calling Start to avoid
	// racing the read loop's first frame.
	ctrlMu         sync.Mutex
	controlHandler func(typ protocol.MsgType, payload []byte)
	frameObserver  func(typ protocol.MsgType)
	appliedHandler func(shareID string)

	sharesMu   sync.RWMutex
	shares     map[string]*ShareConfig
	snapshotMu sync.Mutex
	snapshots  map[string]protocol.IndexSnapshotBegin

	// reconcileMu serializes Reconcile+apply+index-update passes across
	// the whole session (all shares). IndexUpdates are handled
	// concurrently with everything else (see handleIndexUpdate's
	// goroutine in the read loop, needed to avoid a pull deadlocking
	// against the very read loop that would deliver its chunks), but two
	// reconcile passes racing each other — e.g. our own reply to peer A
	// triggering a fresh IndexUpdate back from peer A while a second one
	// is still mid-flight — could otherwise double-apply or interleave
	// badly. Locking the whole pass is the simplest correct answer; it
	// doesn't block transfers themselves, which run under this lock only
	// long enough to hand off to pullFile (the actual byte transfer waits
	// on the read loop independently).
	reconcileMu sync.Mutex

	pullMu      sync.Mutex
	pullSem     chan struct{}
	pullTbl     map[transferKey]*pullEntry
	serveSem    chan struct{}
	indexSem    chan struct{}
	serveMu     sync.Mutex
	serveCancel map[string]context.CancelFunc

	// pullActive/pullPeak instrument concurrent-pull behavior for tests
	// (see integration_test.go's interleaving test); always zero-cost in
	// production beyond a couple of atomic ops.
	pullActive int32
	pullPeak   int32

	// testServeDelay, if non-zero, is slept before serving each
	// FileRequest. It exists solely so a test can make transfers slow
	// enough to observe real concurrency (see integration_test.go); it is
	// unexported and never set outside this package's own tests.
	testServeDelay time.Duration

	// testPullStallTimeout, if non-zero, replaces pullStallTimeout. Same
	// deal as testServeDelay: a test can't wait a real minute to watch a
	// silent peer time out.
	testPullStallTimeout time.Duration

	// testJournalPageSize, if non-zero, replaces journalPageSize. Same deal
	// again: journalling 20,000 changes to watch a second page happen is
	// not a test anyone will keep running.
	testJournalPageSize int

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// done is closed when the read loop exits, i.e. the moment this session
	// stops being usable — a peer that closed the connection, a broken
	// socket, a frame we couldn't read. See [Session.Done]: the owner
	// (internal/core's peer manager) watches it so a disconnect is acted on
	// when it happens rather than when the keepalive's dead timer next
	// notices.
	done      chan struct{}
	closeOnce sync.Once
}

// NewSession constructs a Session over conn. nodeID is this node's own
// ShortID (used to bump version vectors on conflict resolution);
// peerID is the ShortID of the node at the other end of conn, used to key
// its mirrored index rows (index.Store.UpsertPeerFiles/ListPeerFiles). If
// clock is nil, time.Now is used (see reconcile.go's Clock). If logger is
// nil, log.Default() is used.
func NewSession(conn net.Conn, store *index.Store, nodeID, peerID string, clock Clock, logger *log.Logger) *Session {
	if clock == nil {
		clock = time.Now
	}
	if logger == nil {
		logger = log.Default()
	}
	return &Session{
		conn:        conn,
		reader:      protocol.NewReader(conn),
		writer:      protocol.NewStreamWriter(conn, 0),
		store:       store,
		nodeID:      nodeID,
		peerID:      peerID,
		peerLabel:   peerID,
		clock:       clock,
		logger:      logger,
		warnIndex:   make(map[string]int),
		shares:      make(map[string]*ShareConfig),
		snapshots:   make(map[string]protocol.IndexSnapshotBegin),
		pullSem:     make(chan struct{}, maxConcurrentPulls),
		pullTbl:     make(map[transferKey]*pullEntry),
		serveSem:    make(chan struct{}, maxConcurrentServes),
		indexSem:    make(chan struct{}, maxConcurrentIndexJobs),
		serveCancel: make(map[string]context.CancelFunc),
		done:        make(chan struct{}),
	}
}

// startBounded starts fn only after reserving capacity. It is called by the
// single read loop, so a failed non-blocking reservation is an explicit
// overload decision rather than another hidden queue of goroutines.
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

// AddShare registers a share this Session keeps in sync with the peer.
// Safe to call before or after Start.
func (s *Session) AddShare(cfg ShareConfig) {
	c := cfg
	s.sharesMu.Lock()
	s.shares[cfg.ShareID] = &c
	s.sharesMu.Unlock()
}

// SetTrash configures the Trash this Session's trashHook uses (SPEC.md
// §7). Safe to call before or after Start; nil (the default) makes
// trashHook a no-op.
func (s *Session) SetTrash(tr *Trash) {
	s.trash = tr
}

// SetPeerLabel replaces the peer identifier logf prefixes this session's
// log lines with (default: the peer's ShortID). internal/core passes the
// peer's configured name. Call before Start, like the other setters.
func (s *Session) SetPeerLabel(label string) {
	if label != "" {
		s.peerLabel = protocol.SanitizeDiagnostic(label)
	}
}

// SetDebug enables this session's routine-activity logging (see the debug
// field). Call before Start, like the other setters.
func (s *Session) SetDebug(debug bool) {
	s.debug = debug
}

// SetControlHandler registers fn to be called, synchronously from the read
// loop, for every frame type this Session doesn't itself handle: Hello,
// Auth, ShareList, SubscribeRequest, AccessUpdate, Ping, and Pong (see
// readLoop's default case). This is internal/core's hook for driving the
// peering flow (SPEC.md §2, §4, §6) on the same connection Session reads
// exclusively — Session remains "the single reader of the connection"
// (readLoop's own invariant); fn just gets a look at what readLoop would
// otherwise silently drop. fn should not block for long (it runs on the
// read loop goroutine, same as every other dispatch here); spin off a
// goroutine internally for anything that does I/O. Call before Start to
// avoid racing the very first frame the peer sends.
func (s *Session) SetControlHandler(fn func(typ protocol.MsgType, payload []byte)) {
	s.ctrlMu.Lock()
	s.controlHandler = fn
	s.ctrlMu.Unlock()
}

// SetFrameObserver registers fn to be called once per frame read, of any
// type, before dispatch — internal/core's hook for driving
// protocol.Keepalive.RecordReceived, since SPEC.md §4's "90s without
// traffic" dead-connection rule counts every message, not just Ping/Pong,
// and only Session's read loop ever sees the IndexUpdate/FileRequest/
// FileChunk/Error traffic that SetControlHandler's default case never
// reaches. Call before Start, for the same reason as SetControlHandler.
func (s *Session) SetFrameObserver(fn func(typ protocol.MsgType)) {
	s.ctrlMu.Lock()
	s.frameObserver = fn
	s.ctrlMu.Unlock()
}

// SetAppliedHandler registers fn to be called after a reconcile pass that
// actually changed something on disk, naming the share it changed —
// internal/core's hook for fanning that change out to this share's *other*
// peers.
//
// Session cannot do that itself: it owns one connection and knows nothing
// about the others. It already tells the peer it just heard from, by
// sending a delta back at the end of handleIndexUpdate, and for a two-node
// pairing that is the whole story. It is not the whole story for a share
// offered to more than one peer. SPEC.md §5's fan-out rule routes those
// through the offerer, so when bob's change reaches alice it is alice who
// has to pass it to carol — and nothing else will. The rescan that
// internal/core triggers from fsnotify compares the working tree against
// the index, and applying a pulled change updates both, so that scan finds
// no difference and stops before it would propagate. The periodic rescan
// finds the same nothing, so it never self-heals either.
//
// fn is called from the goroutine handling the peer's update, not the read
// loop, but it should still hand off anything slow: it runs while
// reconcileMu is held for that share.
func (s *Session) SetAppliedHandler(fn func(shareID string)) {
	s.ctrlMu.Lock()
	s.appliedHandler = fn
	s.ctrlMu.Unlock()
}

// Writer returns this Session's protocol.StreamWriter, so internal/core's
// peer manager can send ShareList / SubscribeRequest / AccessUpdate / Ping
// frames on the same connection Session writes
// IndexUpdate/FileRequest/FileChunk/Error to. It is safe for concurrent
// use, never tears or interleaves frames, and — because all of those are
// control frames, which it drains ahead of file bytes — never makes its
// caller wait behind a transfer. That last property is what lets the read
// loop reply to a Ping inline (see SetControlHandler).
func (s *Session) Writer() *protocol.StreamWriter {
	return s.writer
}

// LocallyModifiedWarning records one receive-only "locally modified" event
// (SPEC.md §1, §5): a subscriber's local edit that diverged from the
// offerer under a receive-only subscription. Reverted reports whether the
// offerer already had content to revert to (a trash copy was taken and the
// offerer's version installed) or the divergence was only flagged because
// there was nothing yet to revert to (a local-only file the offerer
// doesn't have — see applyLocallyModified in apply.go). Session keeps a
// log of these (Session.LocallyModifiedWarnings) for the API and UI to
// surface.
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
// share and relpath, and reports whether this is a *new* divergence — one
// for which no warning was already standing.
//
// The dedup is not just tidiness. A locally-modified file is a standing
// condition, not an event: it is re-detected by every reconcile pass for as
// long as it stays diverged, and a receive-only share with a handful of
// stray files reaches thousands of entries in an idle afternoon. Appending
// each one grew this slice without bound for the life of the connection and
// made the API repeat the same file once per pass in the UI's warning list.
// Keeping the newest warning per path gives the UI exactly the set of
// currently-diverged files, which is what it was always trying to show.
//
// The returned bool is what lets apply.go log a divergence once, when it
// starts, instead of on every pass — see its call sites.
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
// modified *are* the complete set of currently-diverged files for that
// share. Anything holding a warning from an earlier pass but absent here
// has converged — the user reverted their edit, or the file was deleted —
// and its warning is stale. Without this the UI's warning list only ever
// grew: a divergence that resolved itself stayed on screen for the life of
// the connection.
//
// Called once per pass rather than per file so the whole reconciliation
// costs one lock acquisition, not one per path in the share.
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
// Session.shares is the authorization source: database rows and a wire
// share ID never confer access by themselves.
func (s *Session) acceptsInboundIndex(shareID string) bool {
	cfg, ok := s.getShare(shareID)
	return ok && !cfg.Direction.InboundBlocked
}

// servesOutboundShare is the admission gate for requests for our index or
// file bytes. Receive-only shares are present locally but must never disclose
// their local state back to the peer.
func (s *Session) servesOutboundShare(shareID string) bool {
	cfg, ok := s.getShare(shareID)
	return ok && !cfg.Direction.OutboundBlocked
}

// Start begins the session's read loop and its writer in background
// goroutines and returns immediately. ctx bounds the session's lifetime in
// addition to Close: canceling ctx (or calling Close) stops all background
// work.
//
// Writes before Start fail with protocol.ErrWriterNotStarted — the writer
// is started here rather than in NewSession so that a Session constructed
// for something other than driving a connection (see internal/sync's own
// tests, which build one over a nil conn purely to reach its trash
// helpers) never starts a goroutine.
func (s *Session) Start(ctx context.Context) {
	s.ctx, s.cancel = context.WithCancel(ctx)
	s.writer.Start()
	s.wg.Add(1)
	go s.readLoop()
}

// Done returns a channel closed when the session's read loop has exited:
// the peer closed the connection, the socket broke, or Close was called.
// It is the session's own "this connection is finished" signal, and the
// owner is expected to watch it — nothing else in the session reacts to a
// disconnect, so a caller that instead waits for protocol.Keepalive's dead
// timer to notice pays SPEC.md §4's full 90s before it reconnects, even
// when the peer said goodbye cleanly and immediately.
//
// The channel is never closed for a Session that was never started.
func (s *Session) Done() <-chan struct{} {
	return s.done
}

// Close stops the session: it cancels the internal context, closes the
// underlying connection (unblocking any pending Read in the read loop),
// stops the writer, and waits for every goroutine Session started to
// exit. Safe to call more than once.
func (s *Session) Close() error {
	s.closeOnce.Do(func() {
		if s.cancel != nil {
			s.cancel()
		}
		// Before the writer: closing the connection is what unblocks a
		// write already in progress, and StreamWriter.Close waits for its
		// goroutine to return.
		_ = s.conn.Close()
		_ = s.writer.Close()
	})
	s.wg.Wait()
	return nil
}

func (s *Session) logf(format string, args ...any) {
	s.logger.Printf("sync: peer %s: "+format, append([]any{s.peerLabel}, args...)...)
}

// debugf logs routine sync activity, suppressed unless SetDebug was called.
func (s *Session) debugf(format string, args ...any) {
	if !s.debug {
		return
	}
	s.logf(format, args...)
}

// SyncShare sends a full snapshot IndexUpdate (Full: true) for shareID to
// the peer, unless the share's Direction blocks outbound updates
// (receive-only subscription). This is what a caller uses to kick off the
// initial sync after connecting, and what internal/core's share watcher
// calls (via propagateShare) to notify the peer of a local change.
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
	if err := s.writer.WriteMessage(protocol.MsgIndexSyncRequest, protocol.IndexSyncRequest{ShareID: shareID, Epoch: c.Epoch, AppliedSeq: c.AppliedSeq, SnapshotID: c.SnapshotID, SnapshotBatch: c.SnapshotBatch}); err != nil {
		return err
	}
	return nil
}

func newSnapshotID() string { var b [16]byte; _, _ = rand.Read(b[:]); return hex.EncodeToString(b[:]) }

func (s *Session) answerSyncRequest(ctx context.Context, req protocol.IndexSyncRequest) error {
	// Re-check here as well as in readLoop: this runs asynchronously, and a
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
	// Ignored rows are parked, and ListShare already excludes those, so
	// this is normally a no-op — but a rule added since the last rescan
	// has not been applied to the index yet, and a snapshot must never
	// advertise a path this node will refuse to serve.
	rows = filterIgnoredRows(cfg, rows)
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
	return s.writer.WriteMessage(protocol.MsgIndexSnapshotEnd, protocol.IndexSnapshotEnd{ShareID: req.ShareID, SnapshotID: id, BatchCount: batch})
}

// journalPageSize bounds how many journal rows one read holds in memory at
// a time.
//
// This is a memory bound, not a protocol one: the peer acknowledges each
// batch as it lands, so where one read ends and the next begins is
// invisible on the wire. The page is sized to still fill a delta batch
// (protocol.TargetIndexBatchSize) for typical entries, so paging costs no
// extra frames in practice, while a share with a long journal no longer
// materialises tens of thousands of rows -- version-vector map and all --
// in one slice per peer asking to resync.
const journalPageSize = 20000

// sendJournalDeltas walks the journal from applied to the end, sending each
// page as one or more IndexDeltaBatch frames. done is false when the range
// holds a path this share now ignores, which means the caller must answer
// with a full snapshot instead.
//
// A delta batch cannot simply omit an entry: the receiver requires the
// entries to be contiguous from FromSeq (see readLoop) and
// Store.ApplyPeerDelta additionally requires FromSeq == cursor+1 and
// len(rows) == ToSeq-FromSeq+1. Drop one and the peer rejects the batch and
// resyncs, forever. So an ignored path anywhere in the range hands the
// whole answer over to a snapshot, which carries no per-entry sequence
// contract and re-anchors the peer's cursor at HighSeq. This is reachable
// only for a path that was journalled while still syncing and has since
// become ignored, with a peer whose cursor predates that entry -- and the
// snapshot moves that peer past it for good. Pages already sent before the
// ignored entry was reached are not a problem: the snapshot supersedes
// them.
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
			if err := s.writer.WriteMessage(protocol.MsgIndexDeltaBatch, m); err != nil {
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
// Every clause is a way of *not* being able to, and each one falls back to
// a full snapshot, which re-anchors the peer's cursor unconditionally:
//
//   - A different epoch means the sequence numbers are not comparable.
//   - A peer claiming a sequence past our own high-water mark is not a
//     peer we can compute a delta for. (It happens: restore this node's
//     database from a backup and every peer is ahead of it.)
//   - An entry the peer needs may have aged out of the journal, which is
//     pruned on a retention window (see core's journal sweep). Requiring
//     oldest <= AppliedSeq+1 is what makes pruning safe; note that oldest
//     == 0 — an empty journal — is only "nothing to send" when the peer is
//     already at high, and otherwise means the entries it needs are gone.
func canAnswerWithDeltas(req protocol.IndexSyncRequest, epoch string, high, oldest uint64) bool {
	if req.Epoch != epoch || req.AppliedSeq > high {
		return false
	}
	if req.AppliedSeq == high {
		// Already current: the delta path sends nothing, which is both
		// correct and cheaper than a snapshot the peer would discard.
		return true
	}
	return oldest > 0 && oldest <= req.AppliedSeq+1
}

// journalHasIgnored reports whether any journal entry in rows names a path
// this share now ignores. Such entries are historical: they were written
// while the path was still syncing, and survive in change_journal after
// the file row itself was dropped, because dropping an ignored row is
// deliberately not journalled (see index.Store.ApplyScanResult).
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
// rows unchanged when the share has no ignore rules, which is the common
// case and the one worth not allocating for.
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
// with the number of entries in it: CBOR writes an array header of 1 byte
// for up to 23 elements, 2 up to 255, 3 up to 65535 and 5 beyond, while
// the envelope below is measured with an empty array (1 byte). Four bytes
// is therefore the exact worst case; eight leaves room and costs at most
// one entry at a frame boundary.
const batchArrayHeaderSlack = 8

// snapshotBatchLen reports how many of rows fit in one IndexSnapshotBatch
// frame, and 0 if even the first one does not.
//
// It measures the fixed envelope once and then adds each row's own encoded
// length, because CBOR array elements are self-delimiting values laid down
// back to back: an entry contributes exactly its own encoding, wherever it
// sits in the array. The obvious alternative — re-encode the whole growing
// message after each candidate row and look at the total — is quadratic,
// and not mildly so. Planning one snapshot of a 20k-file share that way
// allocated 28 GB and took 77 seconds; the same plan here is one encode
// per row. Anything that reintroduces a whole-message encode inside this
// loop reintroduces that.
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
// measured with the *last* row's sequence number as ToSeq: sequence numbers
// ascend, and CBOR's integer width grows with the value, so whatever ToSeq
// the chosen batch ends on cannot encode wider than that.
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
	// This is the single funnel for MsgIndexUpdate, so the "never
	// advertise an ignored path" guarantee belongs here even though the
	// rows reaching it are already filtered upstream.
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
// safe for concurrent use). It only decodes and dispatches; anything that
// might block on the network (serving a FileRequest) or that needs to
// itself receive FileChunks/Errors via this very loop (handling an
// IndexUpdate, which may pull files) is handed off to its own goroutine
// so the read loop is never the thing a background operation is waiting
// on — that would deadlock a pull against the loop that delivers its
// chunks.
//
// The one thing dispatched inline is the control handler, which writes
// (a Pong, in reply to a Ping). That is only safe because every frame it
// writes takes protocol.StreamWriter's priority lane, so it cannot queue
// behind a FileChunk: an inline write that could block on the network
// would stop this loop draining the socket, which is precisely what makes
// the peer's own writes stall, and two peers in that state deadlock.
//
// On exit — for any reason, including a clean io.EOF — it closes s.done,
// which is how the owner learns the connection is over ([Session.Done]).
func (s *Session) readLoop() {
	defer s.wg.Done()
	defer close(s.done)
	for {
		typ, payload, err := s.reader.ReadFrame()
		if err != nil {
			return
		}

		s.ctrlMu.Lock()
		observer, ctrlHandler := s.frameObserver, s.controlHandler
		s.ctrlMu.Unlock()
		if observer != nil {
			observer(typ)
		}

		switch typ {
		case protocol.MsgIndexSyncRequest:
			var m protocol.IndexSyncRequest
			if protocol.DecodeAndValidateMessage(payload, &m) != nil {
				continue
			}
			if !s.servesOutboundShare(m.ShareID) {
				s.logf("dropping index request for inactive or outbound-blocked share %s", m.ShareID)
				continue
			}
			if !s.startBounded(s.indexSem, func() {
				if err := s.answerSyncRequest(s.ctx, m); err != nil {
					s.logf("answer index sync: %v", err)
				}
			}) {
				s.logf("closing overloaded session: index workers are full")
				_ = s.conn.Close()
				return
			}
		case protocol.MsgIndexSnapshotBegin:
			var m protocol.IndexSnapshotBegin
			if protocol.DecodeAndValidateMessage(payload, &m) != nil {
				continue
			}
			if !s.acceptsInboundIndex(m.ShareID) || m.SnapshotID == "" {
				continue
			}
			s.snapshotMu.Lock()
			s.snapshots[m.ShareID] = m
			s.snapshotMu.Unlock()
			_ = s.store.SetCursor(s.ctx, s.peerID, m.ShareID, "incoming", index.Cursor{Epoch: m.Epoch, SnapshotID: m.SnapshotID})
		case protocol.MsgIndexSnapshotBatch:
			var m protocol.IndexSnapshotBatch
			if protocol.DecodeAndValidateMessage(payload, &m) != nil {
				continue
			}
			if !s.acceptsInboundIndex(m.ShareID) {
				continue
			}
			s.snapshotMu.Lock()
			b, ok := s.snapshots[m.ShareID]
			s.snapshotMu.Unlock()
			if !ok || b.SnapshotID != m.SnapshotID {
				continue
			}
			c, _ := s.store.Cursor(s.ctx, s.peerID, m.ShareID, "incoming")
			if m.Batch < c.SnapshotBatch {
				continue
			}
			if m.Batch != c.SnapshotBatch {
				_ = s.writer.WriteMessage(protocol.MsgIndexSyncRequest, protocol.IndexSyncRequest{ShareID: m.ShareID, Epoch: c.Epoch, AppliedSeq: c.AppliedSeq, SnapshotID: c.SnapshotID, SnapshotBatch: c.SnapshotBatch})
				continue
			}
			rr := make([]index.FileRow, len(m.Files))
			for i, f := range m.Files {
				rr[i] = index.FileRowFromInfo(m.ShareID, f, time.Now())
			}
			if s.store.StageSnapshotBatch(s.ctx, s.peerID, m.ShareID, m.SnapshotID, m.Batch, rr) == nil {
				c.SnapshotBatch++
				_ = s.store.SetCursor(s.ctx, s.peerID, m.ShareID, "incoming", c)
			}
		case protocol.MsgIndexSnapshotEnd:
			var m protocol.IndexSnapshotEnd
			if protocol.DecodeAndValidateMessage(payload, &m) != nil {
				continue
			}
			if !s.acceptsInboundIndex(m.ShareID) {
				continue
			}
			s.snapshotMu.Lock()
			b, ok := s.snapshots[m.ShareID]
			if ok && b.SnapshotID == m.SnapshotID {
				delete(s.snapshots, m.ShareID)
			}
			s.snapshotMu.Unlock()
			c, _ := s.store.Cursor(s.ctx, s.peerID, m.ShareID, "incoming")
			if ok && b.SnapshotID == m.SnapshotID && c.SnapshotBatch == m.BatchCount && s.store.CommitSnapshot(s.ctx, s.peerID, m.ShareID, m.SnapshotID, b.Epoch, b.HighSeq) == nil {
				_ = s.writer.WriteMessage(protocol.MsgIndexAck, protocol.IndexAck{ShareID: m.ShareID, Epoch: b.Epoch, AppliedSeq: b.HighSeq, SnapshotID: m.SnapshotID})
				if !s.startBounded(s.indexSem, func() {
					s.reconcilePeerShare(s.ctx, m.ShareID)
				}) {
					s.logf("closing overloaded session: index workers are full")
					_ = s.conn.Close()
					return
				}
			}
		case protocol.MsgIndexDeltaBatch:
			var m protocol.IndexDeltaBatch
			if protocol.DecodeAndValidateMessage(payload, &m) != nil {
				continue
			}
			if !s.acceptsInboundIndex(m.ShareID) {
				continue
			}
			rr := make([]index.FileRow, len(m.Entries))
			valid := true
			for i, e := range m.Entries {
				if e.Seq != m.FromSeq+uint64(i) {
					valid = false
					break
				}
				rr[i] = index.FileRowFromInfo(m.ShareID, e.File, time.Now())
			}
			if valid && s.store.ApplyPeerDelta(s.ctx, s.peerID, m.ShareID, m.Epoch, m.FromSeq, m.ToSeq, rr) == nil {
				_ = s.writer.WriteMessage(protocol.MsgIndexAck, protocol.IndexAck{ShareID: m.ShareID, Epoch: m.Epoch, AppliedSeq: m.ToSeq})
				if !s.startBounded(s.indexSem, func() {
					s.handleIndexUpdate(s.ctx, protocol.IndexUpdate{ShareID: m.ShareID, Files: rowsToInfos(rr)})
				}) {
					s.logf("closing overloaded session: index workers are full")
					_ = s.conn.Close()
					return
				}
			} else {
				c, _ := s.store.Cursor(s.ctx, s.peerID, m.ShareID, "incoming")
				_ = s.writer.WriteMessage(protocol.MsgIndexSyncRequest, protocol.IndexSyncRequest{ShareID: m.ShareID, Epoch: c.Epoch, AppliedSeq: c.AppliedSeq})
			}
		case protocol.MsgIndexAck:
			var m protocol.IndexAck
			if protocol.DecodeAndValidateMessage(payload, &m) == nil && s.servesOutboundShare(m.ShareID) {
				// Never let a forged acknowledgement skip data this node has not
				// produced. A later request can safely repeat already-sent rows;
				// accepting a cursor beyond our high-water mark loses them.
				epoch, high, err := s.store.ShareState(s.ctx, m.ShareID)
				if err == nil && m.Epoch == epoch && m.AppliedSeq <= high {
					_ = s.store.SetCursor(s.ctx, s.peerID, m.ShareID, "outgoing", index.Cursor{Epoch: m.Epoch, AppliedSeq: m.AppliedSeq})
				}
			}
		case protocol.MsgIndexUpdate:
			var msg protocol.IndexUpdate
			if err := protocol.DecodeAndValidateMessage(payload, &msg); err != nil {
				s.logf("decode IndexUpdate: %v", err)
				continue
			}
			if !s.acceptsInboundIndex(msg.ShareID) {
				continue
			}
			if !s.startBounded(s.indexSem, func() {
				s.handleIndexUpdate(s.ctx, msg)
			}) {
				s.logf("closing overloaded session: index workers are full")
				_ = s.conn.Close()
				return
			}

		case protocol.MsgFileRequest:
			var req protocol.FileRequest
			if err := protocol.DecodeAndValidateMessage(payload, &req); err != nil {
				s.logf("decode FileRequest: %v", err)
				continue
			}
			if !s.servesOutboundShare(req.ShareID) {
				s.sendFileError(req, protocol.ErrCodeFileNotFound, "unknown share")
				continue
			}
			if !s.startBounded(s.serveSem, func() {
				s.handleFileRequest(s.ctx, req)
			}) {
				s.logf("closing overloaded session: file workers are full")
				_ = s.conn.Close()
				return
			}

		case protocol.MsgFileChunk:
			hdr, data, err := protocol.DecodeFileChunk(payload)
			if err != nil {
				s.logf("decode FileChunk: %v", err)
				continue
			}
			s.routeChunk(hdr.TransferID, pullChunk{header: hdr, data: data, eof: hdr.EOF})

		case protocol.MsgError:
			var e protocol.Error
			if err := protocol.DecodeAndValidateMessage(payload, &e); err != nil {
				s.logf("decode Error: %v", err)
				continue
			}
			if e.TransferID != "" {
				s.routeChunk(e.TransferID, pullChunk{err: &protocol.RemoteError{Code: e.Code, Msg: e.Msg}})
			} else {
				s.logf("peer error: %s: %s", protocol.SanitizeDiagnostic(e.Code), protocol.SanitizeDiagnostic(e.Msg))
			}
		case protocol.MsgCancelTransfer:
			var m protocol.CancelTransfer
			if protocol.DecodeAndValidateMessage(payload, &m) == nil {
				s.cancelServe(m.TransferID)
			}

		default:
			// Handshake/share-list/access/ping-pong messages: not this
			// type's concern. Session assumes the handshake already
			// happened, and hands the rest to internal/core's peer
			// manager via SetControlHandler, if it registered one.
			if ctrlHandler != nil {
				ctrlHandler(typ, payload)
			}
		}
	}
}

func (s *Session) reconcilePeerShare(ctx context.Context, shareID string) {
	s.reconcileMu.Lock()
	defer s.reconcileMu.Unlock()
	rows, err := s.store.ListPeerFiles(ctx, s.peerID, shareID)
	if err != nil {
		return
	}
	// CommitSnapshot has already installed the mirror; feed reconciliation
	// without re-entering the peer-state persistence path.
	cfg, ok := s.getShare(shareID)
	if !ok || cfg.Direction.InboundBlocked {
		return
	}
	local, err := s.store.ListShare(ctx, shareID, true)
	if err != nil {
		return
	}
	actions := Reconcile(rowsToInfos(local), rowsToInfos(rows), s.nodeID, cfg.Direction, s.clock, cfg.Ignore)
	var changed atomic.Bool
	s.runActionWorkers(ctx, cfg, actions, func(_ Action, rows []index.FileRow) {
		if len(rows) > 0 {
			changed.Store(true)
		}
	})
	if changed.Load() && !cfg.Direction.OutboundBlocked {
		// Reconciliation results are journaled by applyAndPersist; publish
		// them so conflict copies converge on every peer.
		_ = s.answerSyncRequest(ctx, protocol.IndexSyncRequest{ShareID: shareID})
	}
}

// handleIndexUpdate is the core reconcile-and-apply pass, run under the
// read loop's bounded index-worker admission (see readLoop). It folds the peer's
// reported files into our mirror of their view (peer_files), reconciles
// that against our own current view ([Reconcile]), executes every
// resulting Action against the filesystem and index, and — unless this
// share's direction blocks outbound updates — reports back whatever
// actually changed locally so the peer's own reconcile pass converges too.
func (s *Session) handleIndexUpdate(ctx context.Context, msg protocol.IndexUpdate) {
	cfg, ok := s.getShare(msg.ShareID)
	if !ok {
		s.logf("index update for unknown share %s", msg.ShareID)
		return
	}
	if cfg.Direction.InboundBlocked {
		// Offerer of a read-only share: incoming changes are ignored
		// outright (SPEC.md §5), including not recording the peer's
		// reported state at all.
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
// handleIndexUpdate is otherwise the only thing that reconciles, which makes
// "the peer sent us rows" the only trigger for noticing that a file is
// missing. That is not the only way to end up owing work. A pull that fails
// — the connection drops mid-transfer, the transfer stalls, the write errors
// — leaves the peer's rows already recorded in peer_files and the file still
// absent locally, and nothing revisits it: the offerer's index sync is
// incremental, so on reconnect its cursor says this peer already has those
// rows and it sends nothing at all. The reconciler never runs, and the file
// stays missing indefinitely.
//
// Measured, on a 192 MiB transfer interrupted by killing the receiver: the
// file did not arrive on its own after two minutes on a healthy reconnected
// connection, and then arrived within five seconds of an *unrelated* file
// being touched in the same share — because that finally produced an index
// update. Calling this when a share becomes active on a session closes that
// gap: reconnecting is itself a reason to re-examine.
func (s *Session) ReconcileShare(ctx context.Context, shareID string) error {
	cfg, ok := s.getShare(shareID)
	if !ok {
		return fmt.Errorf("sync: reconcile share: %s is not active on this session", shareID)
	}
	if cfg.Direction.InboundBlocked {
		// Offerer of a read-only share: it never applies anything inbound,
		// so there is nothing for a reconcile to do (SPEC.md §5).
		return nil
	}
	s.reconcileMu.Lock()
	defer s.reconcileMu.Unlock()
	s.reconcileLocked(ctx, cfg, "reconnect")
	return nil
}

// reconcileLocked is the reconcile-and-apply pass shared by handleIndexUpdate
// and ReconcileShare. Callers must hold reconcileMu and must have already
// recorded any newly-received peer rows. why names what prompted the pass,
// for the debug line.
func (s *Session) reconcileLocked(ctx context.Context, cfg ShareConfig, why string) {
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

	// Every path in the share was just reconciled, so flagged is the
	// complete current set of locally-modified files for it; anything
	// still holding a warning from an earlier pass has converged.
	s.retainWarnings(shareID, flagged)

	if !cfg.Direction.OutboundBlocked && len(changed) > 0 {
		if err := s.sendIndexUpdate(shareID, changed, false); err != nil {
			s.logf("send delta for %s: %v", shareID, err)
		}
	}

	// The delta above only reaches the peer this update came from. Every
	// *other* peer of this share learns about it here — see
	// SetAppliedHandler for why nothing else would tell them.
	if len(changed) > 0 {
		s.ctrlMu.Lock()
		applied := s.appliedHandler
		s.ctrlMu.Unlock()
		if applied != nil {
			applied(shareID)
		}
	}

	// A pass over an already-converged share produces nothing but
	// ActionNone, and there are a great many of those — one per file, on
	// every update from every peer. Only say anything when the pass
	// actually did something.
	if len(counts) > 0 {
		s.debugf("reconciled %s (%s): %s", shareID, why, formatActionCounts(counts))
	}
}

// runActionWorkers applies an arbitrarily large reconciliation plan with a
// fixed number of goroutines. The caller waits for completion, but goroutine
// count is independent of the number of peer-controlled file entries.
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
// ActionKind.String). Fixed order rather than ranging the map so repeated
// passes produce comparable, diffable lines.
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
// produced to the index, returning the rows that were successfully stored
// — the ones handleIndexUpdate reports back to the peer. Failures are
// logged, not returned: one action failing must not stop the rest of the
// pass, and the caller has nothing to do about it beyond what's logged.
//
// applyAction may return a non-nil error alongside non-empty rows: a
// conflict copy's two halves (a local rename and a network pull) can
// partially succeed (see applyConflictCopy's doc comment). Persist
// whatever rows did land, regardless of err, so a transient failure in
// one half never orphans a file that's already correctly on disk.
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

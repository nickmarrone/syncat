package sync

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/nickmarrone/syncat/internal/index"
	"github.com/nickmarrone/syncat/internal/protocol"
)

// maxConcurrentPulls bounds the number of FileRequests this Session keeps
// outstanding to one peer at once (SPEC.md §4: "max 4 concurrent pulls per
// peer"). Additional pulls queue on pullSem until a slot frees; chunks for
// every in-flight pull are interleaved on the one underlying stream
// (protocol.Writer is safe for concurrent writers — see frame.go), which
// is what lets several transfers make progress at once without their own
// separate connections.
const maxConcurrentPulls = 4

// maxConcurrentServes bounds how many FileRequests from the peer we answer
// at once. SPEC.md §4 only specifies a limit on the puller side, but
// capping our own serving concurrency too keeps resource usage (open file
// descriptors, goroutines) bounded symmetrically instead of growing
// unboundedly with however many requests the peer happens to send at once.
const maxConcurrentServes = 4

// ShareConfig is one share this Session keeps in sync with the peer at the
// other end of its connection: the local directory it maps to, and the
// Direction (SPEC.md §5) constraining which way changes may flow. Build
// Direction with DirectionFor rather than by hand.
type ShareConfig struct {
	ShareID   string
	Root      string // absolute local directory this share's files live under
	Direction Direction
}

// transferKey identifies one in-flight pull by the (share, relpath) pair a
// FileChunk's header carries, so the read loop can demultiplex chunks for
// several concurrent transfers arriving interleaved on the same stream.
type transferKey struct {
	shareID string
	relpath string
}

// pullChunk is one unit handed from the read loop to a waiting pullFile
// call: either a chunk of data (possibly the final, EOF-marked one) or an
// error the peer reported for this specific transfer.
type pullChunk struct {
	data []byte
	eof  bool
	err  error
}

// pullEntry is the read loop's handle on one in-flight pull's consumer.
// The channel is buffered so a burst of chunks doesn't stall the read loop
// while the consumer is briefly busy writing the previous one to disk.
type pullEntry struct {
	ch chan pullChunk
}

// Session drives sync for one already-authenticated peer connection:
// exchanging IndexUpdate for each configured share, pulling and serving
// file content, and applying the resulting actions (5a's Reconcile
// output) to the local filesystem and index. One Session corresponds to
// one net.Conn (SPEC.md §4: a single stream carries every share synced
// with that peer) and can hold any number of shares.
//
// Session assumes the handshake (Phase 3) and share/access negotiation
// (Phase 6) already happened and handles only IndexUpdate, FileRequest,
// FileChunk, and file-transfer-scoped Error from that point on — other
// message types arriving on the stream are ignored, since driving those
// exchanges is Phase 7's job (wiring Session into the peer connection
// lifecycle). This keeps Session fully driveable from a test with a bare
// net.Conn (e.g. transport.PipeTransport) and no real peer manager.
//
// Every background goroutine Session starts is tied to the context passed
// to Start and unwound by Close, which cancels that context, closes the
// connection (unblocking the read loop's pending Read), and waits for
// every goroutine to exit — so a Session never leaks goroutines across its
// own lifetime.
type Session struct {
	conn   net.Conn
	reader *protocol.Reader
	writer *protocol.Writer

	store  *index.Store
	nodeID string // this node's ShortID
	peerID string // the peer's ShortID
	clock  Clock

	// trash is this Session's trash can (SPEC.md §7). Set via SetTrash;
	// nil until then, in which case trashHook is a no-op (see apply.go) —
	// so a Session is still fully usable in tests/contexts that don't
	// care about trash, exactly as before Phase 6.
	trash *Trash

	// warningsMu/warnings record every LocallyModifiedWarning this Session
	// has raised (SPEC.md §1/§5's receive-only "flagged as locally
	// modified" UI warning), for a caller (Phase 8's API) to read back.
	// onLocallyModified, if set via SetLocallyModifiedHandler, is called
	// with each warning as it's recorded, in addition to it being kept
	// here.
	warningsMu        sync.Mutex
	warnings          []LocallyModifiedWarning
	onLocallyModified func(LocallyModifiedWarning)

	logger *log.Logger

	sharesMu sync.RWMutex
	shares   map[string]*ShareConfig

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

	pullMu   sync.Mutex
	pullSem  chan struct{}
	pullTbl  map[transferKey]*pullEntry
	serveSem chan struct{}

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

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	closeOnce sync.Once
}

// NewSession constructs a Session over conn. nodeID is this node's own
// ShortID (used to bump version vectors on conflict resolution, per 5a);
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
		conn:     conn,
		reader:   protocol.NewReader(conn),
		writer:   protocol.NewWriter(conn),
		store:    store,
		nodeID:   nodeID,
		peerID:   peerID,
		clock:    clock,
		logger:   logger,
		shares:   make(map[string]*ShareConfig),
		pullSem:  make(chan struct{}, maxConcurrentPulls),
		pullTbl:  make(map[transferKey]*pullEntry),
		serveSem: make(chan struct{}, maxConcurrentServes),
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

// SetLocallyModifiedHandler registers fn to be called synchronously, from
// whichever goroutine is applying the action, every time this Session
// records a LocallyModifiedWarning (SPEC.md §1/§5). Phase 8's API layer
// uses this to surface the warning live rather than only polling
// LocallyModifiedWarnings. Safe to call before or after Start.
func (s *Session) SetLocallyModifiedHandler(fn func(LocallyModifiedWarning)) {
	s.warningsMu.Lock()
	s.onLocallyModified = fn
	s.warningsMu.Unlock()
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

// recordWarning appends w to the session's warning log and, if set, calls
// the registered handler.
func (s *Session) recordWarning(w LocallyModifiedWarning) {
	s.warningsMu.Lock()
	s.warnings = append(s.warnings, w)
	handler := s.onLocallyModified
	s.warningsMu.Unlock()
	if handler != nil {
		handler(w)
	}
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

// Start begins the session's read loop in a background goroutine and
// returns immediately. ctx bounds the session's lifetime in addition to
// Close: canceling ctx (or calling Close) stops all background work.
func (s *Session) Start(ctx context.Context) {
	s.ctx, s.cancel = context.WithCancel(ctx)
	s.wg.Add(1)
	go s.readLoop()
}

// Close stops the session: it cancels the internal context, closes the
// underlying connection (unblocking any pending Read in the read loop),
// and waits for every goroutine Session started to exit. Safe to call
// more than once.
func (s *Session) Close() error {
	s.closeOnce.Do(func() {
		if s.cancel != nil {
			s.cancel()
		}
		_ = s.conn.Close()
	})
	s.wg.Wait()
	return nil
}

func (s *Session) logf(format string, args ...any) {
	s.logger.Printf("sync: session %s: "+format, append([]any{s.peerID}, args...)...)
}

// SyncShare sends a full snapshot IndexUpdate (Full: true) for shareID to
// the peer, unless the share's Direction blocks outbound updates
// (receive-only subscription). This is what a caller uses to kick off the
// initial sync after connecting, and — since this phase has no live
// filesystem watcher wired in (that's Phase 7) — to notify the peer of a
// local change a test (or a future watcher) just made.
func (s *Session) SyncShare(ctx context.Context, shareID string) error {
	cfg, ok := s.getShare(shareID)
	if !ok {
		return fmt.Errorf("sync: session: unknown share %s", shareID)
	}
	if cfg.Direction.OutboundBlocked {
		return nil
	}
	rows, err := s.store.ListShare(ctx, shareID, true)
	if err != nil {
		return fmt.Errorf("sync: session: list share %s: %w", shareID, err)
	}
	return s.sendIndexUpdate(shareID, rows, true)
}

func (s *Session) sendIndexUpdate(shareID string, rows []index.FileRow, full bool) error {
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
func (s *Session) readLoop() {
	defer s.wg.Done()
	for {
		typ, payload, err := s.reader.ReadFrame()
		if err != nil {
			return
		}
		switch typ {
		case protocol.MsgIndexUpdate:
			var msg protocol.IndexUpdate
			if err := protocol.DecodeMessage(payload, &msg); err != nil {
				s.logf("decode IndexUpdate: %v", err)
				continue
			}
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				s.handleIndexUpdate(s.ctx, msg)
			}()

		case protocol.MsgFileRequest:
			var req protocol.FileRequest
			if err := protocol.DecodeMessage(payload, &req); err != nil {
				s.logf("decode FileRequest: %v", err)
				continue
			}
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				s.handleFileRequest(s.ctx, req)
			}()

		case protocol.MsgFileChunk:
			hdr, data, err := protocol.DecodeFileChunk(payload)
			if err != nil {
				s.logf("decode FileChunk: %v", err)
				continue
			}
			s.routeChunk(hdr.ShareID, hdr.RelPath, pullChunk{data: data, eof: hdr.EOF})

		case protocol.MsgError:
			var e protocol.Error
			if err := protocol.DecodeMessage(payload, &e); err != nil {
				s.logf("decode Error: %v", err)
				continue
			}
			if e.ShareID != "" || e.RelPath != "" {
				s.routeChunk(e.ShareID, e.RelPath, pullChunk{err: &protocol.RemoteError{Code: e.Code, Msg: e.Msg}})
			} else {
				s.logf("peer error: %s: %s", e.Code, e.Msg)
			}

		default:
			// Handshake/share-list/access/ping-pong messages: other
			// phases' concern (this Session assumes they already
			// happened, or are handled elsewhere on this connection).
		}
	}
}

// routeChunk hands one FileChunk/Error payload to the pull awaiting it, if
// any. A chunk for an unknown or already-finished/cancelled transfer
// (e.g. arriving after the puller gave up) is discarded here rather than
// corrupting or blocking on some other transfer's channel.
func (s *Session) routeChunk(shareID, relpath string, c pullChunk) {
	key := transferKey{shareID, relpath}
	s.pullMu.Lock()
	entry, ok := s.pullTbl[key]
	s.pullMu.Unlock()
	if !ok {
		return
	}
	select {
	case entry.ch <- c:
	case <-s.ctx.Done():
	}
}

// handleIndexUpdate is the core reconcile-and-apply pass, run in its own
// goroutine per incoming IndexUpdate (see readLoop). It folds the peer's
// reported files into our mirror of their view (peer_files), reconciles
// that against our own current view (5a's Reconcile), executes every
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

	localRows, err := s.store.ListShare(ctx, msg.ShareID, true)
	if err != nil {
		s.logf("list share %s: %v", msg.ShareID, err)
		return
	}
	remoteRows, err := s.store.ListPeerFiles(ctx, s.peerID, msg.ShareID)
	if err != nil {
		s.logf("list peer files %s: %v", msg.ShareID, err)
		return
	}

	actions := Reconcile(rowsToInfos(localRows), rowsToInfos(remoteRows), s.nodeID, cfg.Direction, s.clock)

	var mu sync.Mutex
	var changed []index.FileRow
	var wg sync.WaitGroup
	for _, a := range actions {
		a := a
		wg.Add(1)
		go func() {
			defer wg.Done()
			// applyAction may return a non-nil error alongside non-empty
			// rows: a conflict copy's two halves (a local rename and a
			// network pull) can partially succeed (see
			// applyConflictCopy's doc comment). Persist whatever rows did
			// land, regardless of err, so a transient failure in one half
			// never orphans a file that's already correctly on disk.
			rows, err := s.applyAction(ctx, cfg, a)
			if err != nil {
				s.logf("apply %s %s: %v", a.Kind, a.RelPath, err)
			}
			for _, r := range rows {
				if err := s.store.PutFile(ctx, r); err != nil {
					s.logf("put file %s/%s: %v", r.ShareID, r.RelPath, err)
					continue
				}
				mu.Lock()
				changed = append(changed, r)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if !cfg.Direction.OutboundBlocked && len(changed) > 0 {
		if err := s.sendIndexUpdate(msg.ShareID, changed, false); err != nil {
			s.logf("send delta for %s: %v", msg.ShareID, err)
		}
	}
}

func rowsToInfos(rows []index.FileRow) []protocol.FileInfo {
	out := make([]protocol.FileInfo, len(rows))
	for i, r := range rows {
		out[i] = r.Info()
	}
	return out
}

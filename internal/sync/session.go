package sync

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
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

// --- the session: one authenticated peer connection --------------------

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
	warningsMu sync.Mutex
	warnings   []LocallyModifiedWarning

	logger *log.Logger

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
		conn:     conn,
		reader:   protocol.NewReader(conn),
		writer:   protocol.NewStreamWriter(conn, 0),
		store:    store,
		nodeID:   nodeID,
		peerID:   peerID,
		clock:    clock,
		logger:   logger,
		shares:   make(map[string]*ShareConfig),
		pullSem:  make(chan struct{}, maxConcurrentPulls),
		pullTbl:  make(map[transferKey]*pullEntry),
		serveSem: make(chan struct{}, maxConcurrentServes),
		done:     make(chan struct{}),
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

// LocallyModifiedWarnings returns a snapshot of every LocallyModifiedWarning
// recorded so far, oldest first.
func (s *Session) LocallyModifiedWarnings() []LocallyModifiedWarning {
	s.warningsMu.Lock()
	defer s.warningsMu.Unlock()
	out := make([]LocallyModifiedWarning, len(s.warnings))
	copy(out, s.warnings)
	return out
}

// recordWarning appends w to the session's warning log.
func (s *Session) recordWarning(w LocallyModifiedWarning) {
	s.warningsMu.Lock()
	s.warnings = append(s.warnings, w)
	s.warningsMu.Unlock()
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
	s.logger.Printf("sync: session %s: "+format, append([]any{s.peerID}, args...)...)
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

		s.ctrlMu.Lock()
		observer, ctrlHandler := s.frameObserver, s.controlHandler
		s.ctrlMu.Unlock()
		if observer != nil {
			observer(typ)
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

// --- file transfer: pulling and serving bytes --------------------------

// pullFile fetches wireRelPath's content (as shareID's holder — the peer
// at the other end of this Session — currently has it under version) and
// streams it into dst as chunks arrive, never buffering more than one
// chunk (<=1 MiB, protocol.MaxFileChunkData) at a time. It returns the
// total number of bytes written.
//
// Concurrency: pullFile blocks until a slot is free in s.pullSem, which
// has capacity maxConcurrentPulls — this is the "max 4 concurrent pulls
// per peer" enforcement point (SPEC.md §4). Any number of goroutines may
// call pullFile at once; excess callers simply queue on the semaphore.
// While waiting for chunks, pullFile is fed by the Session's single read
// loop via routeChunk, which demultiplexes interleaved FileChunk frames
// for every concurrently in-flight pull by (shareID, relpath).
func (s *Session) pullFile(ctx context.Context, shareID, wireRelPath string, version protocol.VersionVector, dst io.Writer) (int64, error) {
	select {
	case s.pullSem <- struct{}{}:
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	defer func() { <-s.pullSem }()

	active := atomic.AddInt32(&s.pullActive, 1)
	defer atomic.AddInt32(&s.pullActive, -1)
	for {
		peak := atomic.LoadInt32(&s.pullPeak)
		if active <= peak || atomic.CompareAndSwapInt32(&s.pullPeak, peak, active) {
			break
		}
	}

	key := transferKey{shareID, wireRelPath}
	entry := &pullEntry{ch: make(chan pullChunk, 8)}
	s.pullMu.Lock()
	if _, exists := s.pullTbl[key]; exists {
		s.pullMu.Unlock()
		return 0, fmt.Errorf("sync: pull %s/%s: already in flight", shareID, wireRelPath)
	}
	s.pullTbl[key] = entry
	s.pullMu.Unlock()
	defer func() {
		s.pullMu.Lock()
		delete(s.pullTbl, key)
		s.pullMu.Unlock()
	}()

	if err := s.writer.WriteMessage(protocol.MsgFileRequest, protocol.FileRequest{
		ShareID: shareID,
		RelPath: wireRelPath,
		Version: version,
		// Offset is always 0: resume is deferred (SPEC.md §4 keeps the
		// field on the wire for a later phase to populate).
		Offset: 0,
	}); err != nil {
		return 0, fmt.Errorf("sync: pull %s/%s: send file request: %w", shareID, wireRelPath, err)
	}

	var total int64
	for {
		select {
		case c, ok := <-entry.ch:
			if !ok {
				return total, fmt.Errorf("sync: pull %s/%s: transfer channel closed", shareID, wireRelPath)
			}
			if c.err != nil {
				return total, fmt.Errorf("sync: pull %s/%s: %w", shareID, wireRelPath, c.err)
			}
			if len(c.data) > 0 {
				n, err := dst.Write(c.data)
				total += int64(n)
				if err != nil {
					return total, fmt.Errorf("sync: pull %s/%s: write: %w", shareID, wireRelPath, err)
				}
			}
			if c.eof {
				return total, nil
			}
		case <-ctx.Done():
			return total, ctx.Err()
		}
	}
}

// handleFileRequest serves one incoming FileRequest from our own share
// content, respecting maxConcurrentServes. A file we no longer have, or
// whose current version differs from what the requester asked for, gets
// an Error reply (SPEC.md §4/§5) rather than a hung or truncated stream.
func (s *Session) handleFileRequest(ctx context.Context, req protocol.FileRequest) {
	select {
	case s.serveSem <- struct{}{}:
	case <-ctx.Done():
		return
	}
	defer func() { <-s.serveSem }()

	cfg, ok := s.getShare(req.ShareID)
	if !ok {
		s.sendFileError(req, protocol.ErrCodeFileNotFound, "unknown share")
		return
	}

	row, err := s.store.GetFile(ctx, req.ShareID, req.RelPath)
	if err != nil || row.Deleted {
		s.sendFileError(req, protocol.ErrCodeFileNotFound, "file not found")
		return
	}
	if !Equal(row.Version, req.Version) {
		s.sendFileError(req, protocol.ErrCodeVersionChanged, "version has moved on")
		return
	}

	absPath, err := JoinSharePath(cfg.Root, req.RelPath)
	if err != nil {
		// Should be unreachable in practice: an invalid relpath can't have
		// made it into the index (ValidateRelPath gates every wire path
		// before it becomes an Action — see reconcile.go/path.go). Treat
		// it the same as "not found" rather than ever touching the
		// filesystem with it.
		s.sendFileError(req, protocol.ErrCodeFileNotFound, "invalid path")
		return
	}

	if s.testServeDelay > 0 {
		select {
		case <-time.After(s.testServeDelay):
		case <-ctx.Done():
			return
		}
	}

	if row.Type == protocol.FileTypeSymlink {
		s.serveSymlink(absPath, req, row.Version)
		return
	}

	f, err := os.Open(absPath)
	if err != nil {
		s.sendFileError(req, protocol.ErrCodeFileNotFound, "open: "+err.Error())
		return
	}
	defer f.Close()

	if err := s.streamFile(f, req, row.Version); err != nil {
		s.logf("serve %s/%s: %v", req.ShareID, req.RelPath, err)
	}
}

// streamFile sends f's contents as a sequence of FileChunk frames of at
// most protocol.MaxFileChunkData bytes each — f is never read into memory
// beyond one chunk at a time — followed by one final zero-length,
// EOF-marked chunk. Sending the EOF marker as its own frame (rather than
// trying to detect end-of-file on the same read that returned the last
// data) keeps the read side's contract simple: EOF is only ever true, and
// only needs handling, on a frame it already received.
func (s *Session) streamFile(f *os.File, req protocol.FileRequest, version protocol.VersionVector) error {
	buf := make([]byte, protocol.MaxFileChunkData)
	var offset int64
	for {
		n, readErr := f.Read(buf)
		if n > 0 {
			if err := s.writer.WriteFileChunk(protocol.FileChunkHeader{
				ShareID: req.ShareID, RelPath: req.RelPath, Version: version, Offset: offset, EOF: false,
			}, buf[:n]); err != nil {
				return fmt.Errorf("write chunk at offset %d: %w", offset, err)
			}
			offset += int64(n)
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return fmt.Errorf("read at offset %d: %w", offset, readErr)
		}
	}
	if err := s.writer.WriteFileChunk(protocol.FileChunkHeader{
		ShareID: req.ShareID, RelPath: req.RelPath, Version: version, Offset: offset, EOF: true,
	}, nil); err != nil {
		return fmt.Errorf("write eof chunk: %w", err)
	}
	return nil
}

// serveSymlink sends a symlink's target path as its "content" (SPEC.md
// §5: "the symlink entry itself (target string) syncs on Unix"), using
// the same chunk framing as a regular file.
func (s *Session) serveSymlink(absPath string, req protocol.FileRequest, version protocol.VersionVector) {
	target, err := os.Readlink(absPath)
	if err != nil {
		s.sendFileError(req, protocol.ErrCodeFileNotFound, "readlink: "+err.Error())
		return
	}
	data := []byte(target)
	if err := s.writer.WriteFileChunk(protocol.FileChunkHeader{
		ShareID: req.ShareID, RelPath: req.RelPath, Version: version, Offset: 0, EOF: false,
	}, data); err != nil {
		s.logf("serve symlink %s/%s: %v", req.ShareID, req.RelPath, err)
		return
	}
	if err := s.writer.WriteFileChunk(protocol.FileChunkHeader{
		ShareID: req.ShareID, RelPath: req.RelPath, Version: version, Offset: int64(len(data)), EOF: true,
	}, nil); err != nil {
		s.logf("serve symlink %s/%s: eof: %v", req.ShareID, req.RelPath, err)
	}
}

func (s *Session) sendFileError(req protocol.FileRequest, code, msg string) {
	_ = s.writer.WriteMessage(protocol.MsgError, protocol.Error{
		Code: code, Msg: msg, ShareID: req.ShareID, RelPath: req.RelPath,
	})
}

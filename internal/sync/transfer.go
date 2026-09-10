// transfer.go is the byte-moving half of Session: pulling a file's content
// from the peer (pullFile, fed by the read loop via routeChunk) and serving
// the peer's FileRequests from our own share content (handleFileRequest).
// session.go holds the Session type, its lifecycle, the read loop and the
// index exchange; apply.go holds the disk writes that consume pullFile.
package sync

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"

	"github.com/nickmarrone/syncat/internal/index"
	"github.com/nickmarrone/syncat/internal/protocol"
)

// --- file transfer: pulling and serving bytes --------------------------

// pullStallTimeout bounds how long one in-flight pull will wait for the
// next FileChunk before giving up.
//
// SPEC.md §4/§5 promise that a peer which no longer has a requested file,
// or whose version has moved on, answers with an Error rather than
// silence — but that is a promise about a *well-behaved* peer, and
// pullFile is what pays for it being broken. Without this, a FileRequest
// answered with neither a chunk nor an Error parks its caller until the
// whole session is torn down, holding one of the maxConcurrentPulls slots
// the entire time. Four such requests wedge every transfer with that peer
// while the connection still looks perfectly healthy: pings, index
// updates and share lists keep flowing, so nothing ever declares it dead.
//
// The timer is reset by every chunk received, so it bounds the gap
// between chunks rather than the transfer as a whole — a legitimately
// slow but progressing transfer of any size is unaffected.
const pullStallTimeout = 60 * time.Second

// transferKey identifies one in-flight pull by the (share, relpath) pair a
// FileChunk's header carries, so the read loop can demultiplex chunks for
// several concurrent transfers arriving interleaved on the same stream.
type transferKey struct {
	id string
}

// pullChunk is one unit handed from the read loop to a waiting pullFile
// call: either a chunk of data (possibly the final, EOF-marked one) or an
// error the peer reported for this specific transfer.
type pullChunk struct {
	header protocol.FileChunkHeader
	data   []byte
	eof    bool
	err    error
}

// pullQueueDepth is how many received chunks one in-flight pull will hold
// ahead of the goroutine writing them to disk.
//
// Every slot can hold a full protocol.MaxFileChunkData payload, and it
// holds the whole frame buffer that payload was sliced out of, so this
// number is a memory bound before it is anything else: depth x 1 MiB x
// maxConcurrentPulls per connected peer, resident for as long as transfers
// are running. At the depth of 8 this started at, three busy peers held
// close to 100 MiB in chunk buffers alone -- which the Go scavenger then
// returns to the OS only slowly, so the daemon's RSS climbed with transfer
// activity and stayed there.
//
// Two is enough to serve the purpose the buffer actually has: one chunk
// being written while the next arrives, so the read loop is not made to
// wait on a disk write. Anything deeper is buffering the transfer rather
// than smoothing it, and there is nothing to buffer against -- writing and
// hashing a 1 MiB chunk takes a millisecond or two, while delivering one
// over a relayed tunnel takes far longer. Blocking here when the queue is
// full is safe backpressure, unlike the control lane in
// protocol.StreamWriter: the read loop ends up waiting on a file write,
// which does not need the read loop to make progress.
const pullQueueDepth = 2

// pullEntry is the read loop's handle on one in-flight pull's consumer.
type pullEntry struct {
	ch chan pullChunk
}

// ErrTransferStalled is returned by a pull that went pullStallTimeout with
// no chunk and no Error from the peer — a peer that answered a
// FileRequest with silence. Wrapped, so callers can check it with
// errors.Is.
//
// The message names no duration on purpose: this is a package-level value,
// so it can't reflect testPullStallTimeout, and stating a timeout the pull
// didn't actually wait would be worse than stating none.
var ErrTransferStalled = errors.New("peer sent neither a chunk nor an error before the pull stall timeout")

const staleTransferLogInterval = time.Minute

// routeChunk hands one FileChunk/Error payload to the pull awaiting it, if
// any. A chunk for an unknown or already-finished/cancelled transfer
// (e.g. arriving after the puller gave up) is discarded here rather than
// corrupting or blocking on some other transfer's channel.
func (s *Session) routeChunk(transferID string, c pullChunk) {
	key := transferKey{id: transferID}
	s.pullMu.Lock()
	entry, ok := s.pullTbl[key]
	s.pullMu.Unlock()
	if !ok {
		s.noteStaleTransferFrame()
		return
	}
	select {
	case entry.ch <- c:
	case <-s.ctx.Done():
	}
}

func (s *Session) noteStaleTransferFrame() {
	s.stats.staleTransferFrames.Add(1)
	s.protocolViolation()
	now := s.clock()
	s.staleLogMu.Lock()
	if s.lastStaleLog.IsZero() || now.Sub(s.lastStaleLog) >= staleTransferLogInterval {
		count := s.staleSuppressed + 1
		s.staleSuppressed = 0
		s.lastStaleLog = now
		s.staleLogMu.Unlock()
		s.logf("dropped frame for an unknown or finished transfer (%d since last report)", count)
		return
	}
	s.staleSuppressed++
	s.staleLogMu.Unlock()
}

func newTransferID() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(id[:]), nil
}

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
func (s *Session) pullFile(ctx context.Context, shareID, wireRelPath string, version protocol.VersionVector, expectedSize int64, dst io.Writer) (int64, error) {
	select {
	case s.pullSem <- struct{}{}:
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	defer func() { <-s.pullSem }()

	done := s.notePullActive()
	defer done()
	s.stats.pullsStarted.Add(1)

	transferID, err := newTransferID()
	if err != nil {
		return 0, fmt.Errorf("sync: create transfer id: %w", err)
	}
	key := transferKey{id: transferID}
	entry := &pullEntry{ch: make(chan pullChunk, pullQueueDepth)}
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
		TransferID: transferID,
		ShareID:    shareID,
		RelPath:    wireRelPath,
		Version:    version,
		// Offset is always 0: resume is deferred (SPEC.md §4 keeps the
		// field on the wire for a later phase to populate).
		Offset: 0,
	}); err != nil {
		return 0, fmt.Errorf("sync: pull %s/%s: send file request: %w", shareID, wireRelPath, err)
	}

	// Real time, not s.clock: syncsvc.Clock is a bare func() time.Time with
	// no timer, and this bounds a network wait rather than anything a test
	// drives. Every test transfer answers immediately, so nothing waits on
	// it (see pullStallTimeout).
	stallAfter := pullStallTimeout
	if s.testPullStallTimeout > 0 {
		stallAfter = s.testPullStallTimeout
	}
	stall := time.NewTimer(stallAfter)
	defer stall.Stop()

	var total int64
	terminal := false
	defer func() {
		if !terminal {
			s.stats.transferCancellations.Add(1)
			_ = s.writer.WriteMessage(protocol.MsgCancelTransfer, protocol.CancelTransfer{TransferID: transferID})
		}
	}()
	for {
		select {
		case c, ok := <-entry.ch:
			if !ok {
				return total, fmt.Errorf("sync: pull %s/%s: transfer channel closed", shareID, wireRelPath)
			}
			if c.err != nil {
				terminal = true
				return total, fmt.Errorf("sync: pull %s/%s: %w", shareID, wireRelPath, c.err)
			}
			if c.header.TransferID != transferID || c.header.ShareID != shareID || c.header.RelPath != wireRelPath || !Equal(c.header.Version, version) || c.header.Offset != total {
				s.protocolViolation()
				return total, fmt.Errorf("sync: pull %s/%s: invalid chunk identity or offset", shareID, wireRelPath)
			}
			if len(c.data) > 0 {
				n, err := dst.Write(c.data)
				total += int64(n)
				s.stats.bytesReceived.Add(uint64(n))
				if err != nil {
					return total, fmt.Errorf("sync: pull %s/%s: write: %w", shareID, wireRelPath, err)
				}
			}
			if c.eof {
				if expectedSize >= 0 && total != expectedSize {
					return total, fmt.Errorf("sync: pull %s/%s: EOF at %d bytes, expected %d", shareID, wireRelPath, total, expectedSize)
				}
				terminal = true
				return total, nil
			}
			resetStallTimer(stall, stallAfter)
		case <-stall.C:
			s.stats.transferStalls.Add(1)
			return total, fmt.Errorf("sync: pull %s/%s: %w", shareID, wireRelPath, ErrTransferStalled)
		case <-ctx.Done():
			return total, ctx.Err()
		}
	}
}

// notePullActive counts one more in-flight pull in s.pullActive and raises
// s.pullPeak if this is the most that have ever been concurrent (the CAS
// loop is because several pulls starting at once race to publish their
// own count). It returns the matching decrement for the caller to defer
// when the pull finishes.
func (s *Session) notePullActive() (done func()) {
	active := atomic.AddInt32(&s.pullActive, 1)
	for {
		peak := atomic.LoadInt32(&s.pullPeak)
		if active <= peak || atomic.CompareAndSwapInt32(&s.pullPeak, peak, active) {
			break
		}
	}
	return func() { atomic.AddInt32(&s.pullActive, -1) }
}

// resetStallTimer re-arms t to fire d from now, whether or not it has
// already fired — a fired timer's expiry has to be drained from t.C first
// or the next select would consume it as a stall.
func resetStallTimer(t *time.Timer, d time.Duration) {
	if !t.Stop() {
		// Already fired and its value is sitting in the channel;
		// drain it so the reset below starts from empty.
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(d)
}

// handleFileRequest serves one incoming FileRequest from our own share
// content after readLoop reserves a maxConcurrentServes slot. A file we no longer have, or
// whose current version differs from what the requester asked for, gets
// an Error reply (SPEC.md §4/§5) rather than a hung or truncated stream.
func (s *Session) handleFileRequest(ctx context.Context, req protocol.FileRequest) {
	s.stats.servesStarted.Add(1)
	if req.Offset != 0 {
		s.sendFileError(req, protocol.ErrCodeUnsupportedOffset, "resume offsets are not supported")
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	if !s.registerServe(req.TransferID, cancel) {
		cancel()
		s.sendFileError(req, protocol.ErrCodeTransferFailed, "duplicate transfer id")
		return
	}
	defer func() { s.unregisterServe(req.TransferID); cancel() }()
	cfg, ok := s.getShare(req.ShareID)
	if !ok || cfg.Direction.OutboundBlocked {
		s.sendFileError(req, protocol.ErrCodeFileNotFound, "unknown share")
		return
	}

	if cfg.ignored(req.RelPath, false) {
		// Backstop. An ignored path has no index row, so the GetFile
		// below would fail anyway — but a peer can only have learned the
		// path from a journal entry we wrote before the rule existed, and
		// this keeps the answer authoritative rather than incidental. The
		// error is deliberately the same one a genuinely missing file
		// gets: what this node chooses not to share is its own business,
		// not a distinct condition to advertise.
		s.sendFileError(req, protocol.ErrCodeFileNotFound, "file not found")
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
		if err := s.streamSymlink(ctx, absPath, req, row); err != nil {
			code := protocol.ErrCodeTransferFailed
			if errors.Is(err, errFileChangedDuringTransfer) {
				code = protocol.ErrCodeVersionChanged
			}
			s.sendFileError(req, code, err.Error())
		}
		return
	}

	f, err := os.Open(absPath)
	if err != nil {
		s.sendFileError(req, protocol.ErrCodeFileNotFound, "open: "+err.Error())
		return
	}
	defer f.Close()

	if err := s.streamFile(ctx, f, req, row); err != nil {
		code := protocol.ErrCodeTransferFailed
		if errors.Is(err, errFileChangedDuringTransfer) {
			code = protocol.ErrCodeVersionChanged
		}
		s.sendFileError(req, code, err.Error())
	}
}

var errFileChangedDuringTransfer = errors.New("file changed during transfer")

// streamFile sends f's contents as a sequence of FileChunk frames of at
// most protocol.MaxFileChunkData bytes each — f is never read into memory
// beyond one chunk at a time — followed by one final zero-length,
// EOF-marked chunk. Sending the EOF marker as its own frame (rather than
// trying to detect end-of-file on the same read that returned the last
// data) keeps the read side's contract simple: EOF is only ever true, and
// only needs handling, on a frame it already received.
func (s *Session) streamFile(ctx context.Context, f *os.File, req protocol.FileRequest, row index.FileRow) error {
	before, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat before read: %w", err)
	}
	if before.Size() != row.Size || before.ModTime().UnixNano() != row.MTimeNS {
		return errFileChangedDuringTransfer
	}
	buf := make([]byte, protocol.MaxFileChunkData)
	hasher := sha256.New()
	var offset int64
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		n, readErr := f.Read(buf)
		if n > 0 {
			_, _ = hasher.Write(buf[:n])
			if err := s.writer.WriteFileChunkOnWritten(protocol.FileChunkHeader{
				TransferID: req.TransferID, ShareID: req.ShareID, RelPath: req.RelPath, Version: row.Version, Offset: offset, EOF: false,
			}, buf[:n], func() { s.stats.bytesSent.Add(uint64(n)) }); err != nil {
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
	after, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat after read: %w", err)
	}
	if after.Size() != before.Size() || after.ModTime() != before.ModTime() || offset != row.Size || !bytes.Equal(hasher.Sum(nil), row.SHA256) {
		return errFileChangedDuringTransfer
	}
	if err := s.writer.WriteFileChunk(protocol.FileChunkHeader{
		TransferID: req.TransferID, ShareID: req.ShareID, RelPath: req.RelPath, Version: row.Version, Offset: offset, EOF: true,
	}, nil); err != nil {
		return fmt.Errorf("write eof chunk: %w", err)
	}
	return nil
}

// streamSymlink sends a symlink's target path as its "content" (SPEC.md §5).
// Unlike a regular file, the target is one atomic, small read, so it can be
// fully verified against the indexed size and hash before any frame is queued.
// Lstat on both sides also detects replacement during the read without
// following the link outside the share.
func (s *Session) streamSymlink(ctx context.Context, absPath string, req protocol.FileRequest, row index.FileRow) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	before, err := os.Lstat(absPath)
	if err != nil {
		return fmt.Errorf("lstat before readlink: %w", err)
	}
	if before.Mode()&os.ModeSymlink == 0 {
		return errFileChangedDuringTransfer
	}
	target, err := os.Readlink(absPath)
	if err != nil {
		return fmt.Errorf("readlink: %w", err)
	}
	data := []byte(target)
	after, err := os.Lstat(absPath)
	if err != nil {
		return fmt.Errorf("lstat after readlink: %w", err)
	}
	sum := sha256.Sum256(data)
	if !os.SameFile(before, after) || before.Size() != after.Size() || before.ModTime() != after.ModTime() || int64(len(data)) != row.Size || !bytes.Equal(sum[:], row.SHA256) {
		return errFileChangedDuringTransfer
	}
	if err := s.writer.WriteFileChunkOnWritten(protocol.FileChunkHeader{
		TransferID: req.TransferID, ShareID: req.ShareID, RelPath: req.RelPath, Version: row.Version, Offset: 0, EOF: false,
	}, data, func() { s.stats.bytesSent.Add(uint64(len(data))) }); err != nil {
		return fmt.Errorf("write symlink target: %w", err)
	}
	if err := s.writer.WriteFileChunk(protocol.FileChunkHeader{
		TransferID: req.TransferID, ShareID: req.ShareID, RelPath: req.RelPath, Version: row.Version, Offset: int64(len(data)), EOF: true,
	}, nil); err != nil {
		return fmt.Errorf("write symlink eof: %w", err)
	}
	return nil
}

func (s *Session) sendFileError(req protocol.FileRequest, code, msg string) {
	_ = s.writer.WriteMessage(protocol.MsgError, protocol.Error{
		Code: code, Msg: msg, ShareID: req.ShareID, RelPath: req.RelPath,
		TransferID: req.TransferID,
	})
}

func (s *Session) registerServe(id string, cancel context.CancelFunc) bool {
	s.serveMu.Lock()
	defer s.serveMu.Unlock()
	if _, exists := s.serveCancel[id]; exists {
		return false
	}
	s.serveCancel[id] = cancel
	return true
}
func (s *Session) unregisterServe(id string) {
	s.serveMu.Lock()
	delete(s.serveCancel, id)
	s.serveMu.Unlock()
}
func (s *Session) cancelServe(id string) {
	s.serveMu.Lock()
	cancel := s.serveCancel[id]
	s.serveMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

package sync

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"

	"github.com/nickmarrone/syncat/internal/protocol"
)

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

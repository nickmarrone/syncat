package sync

import (
	"time"

	"github.com/nickmarrone/syncat/internal/index"
	"github.com/nickmarrone/syncat/internal/protocol"
)

// sessionFrameHandlers is the post-handshake dispatch table. Each handler
// owns decoding, semantic validation, live admission, and work scheduling for
// one wire type. false asks the framing loop to terminate the session.
var sessionFrameHandlers = map[protocol.MsgType]func(*Session, []byte) bool{
	protocol.MsgIndexSyncRequest:   handleIndexSyncRequestFrame,
	protocol.MsgIndexSnapshotBegin: handleIndexSnapshotBeginFrame,
	protocol.MsgIndexSnapshotBatch: handleIndexSnapshotBatchFrame,
	protocol.MsgIndexSnapshotEnd:   handleIndexSnapshotEndFrame,
	protocol.MsgIndexDeltaBatch:    handleIndexDeltaBatchFrame,
	protocol.MsgIndexAck:           handleIndexAckFrame,
	protocol.MsgIndexUpdate:        handleIndexUpdateFrame,
	protocol.MsgFileRequest:        handleFileRequestFrame,
	protocol.MsgFileChunk:          handleFileChunkFrame,
	protocol.MsgError:              handleErrorFrame,
	protocol.MsgCancelTransfer:     handleCancelTransferFrame,
}

func handleIndexSyncRequestFrame(s *Session, payload []byte) bool {
	var m protocol.IndexSyncRequest
	if !s.decodeWire(payload, &m) {
		return true
	}
	if !s.servesOutboundShare(m.ShareID) {
		s.rejectShareOperation()
		s.logf("dropping index request for inactive or outbound-blocked share %s", m.ShareID)
		return true
	}
	if !s.enqueueIndex("sync\x00"+m.ShareID, func() {
		if err := s.answerSyncRequest(s.ctx, m); err != nil {
			s.logf("answer index sync: %v", err)
		}
	}) {
		s.rejectWork()
		s.logf("closing overloaded session: index queue is full")
		_ = s.conn.Close()
		return false
	}
	return true
}

func handleIndexSnapshotBeginFrame(s *Session, payload []byte) bool {
	var m protocol.IndexSnapshotBegin
	if !s.decodeWire(payload, &m) {
		return true
	}
	if !s.acceptsInboundIndex(m.ShareID) || m.SnapshotID == "" {
		s.rejectShareOperation()
		return true
	}
	s.snapshotMu.Lock()
	s.snapshots[m.ShareID] = m
	s.snapshotMu.Unlock()
	_ = s.store.SetCursor(s.ctx, s.peerID, m.ShareID, "incoming", index.Cursor{Epoch: m.Epoch, SnapshotID: m.SnapshotID})
	return true
}

func handleIndexSnapshotBatchFrame(s *Session, payload []byte) bool {
	var m protocol.IndexSnapshotBatch
	if !s.decodeWire(payload, &m) {
		return true
	}
	if !s.acceptsInboundIndex(m.ShareID) {
		s.rejectShareOperation()
		return true
	}
	s.snapshotMu.Lock()
	b, ok := s.snapshots[m.ShareID]
	s.snapshotMu.Unlock()
	if !ok || b.SnapshotID != m.SnapshotID {
		s.protocolViolation()
		return true
	}
	c, _ := s.store.Cursor(s.ctx, s.peerID, m.ShareID, "incoming")
	if m.Batch < c.SnapshotBatch {
		return true
	}
	if m.Batch != c.SnapshotBatch {
		_ = s.writer.WriteLatestMessageContext(s.ctx, "index-sync\x00"+m.ShareID, protocol.MsgIndexSyncRequest, protocol.IndexSyncRequest{ShareID: m.ShareID, Epoch: c.Epoch, AppliedSeq: c.AppliedSeq, SnapshotID: c.SnapshotID, SnapshotBatch: c.SnapshotBatch})
		return true
	}
	rr := make([]index.FileRow, len(m.Files))
	for i, f := range m.Files {
		rr[i] = index.FileRowFromInfo(m.ShareID, f, time.Now())
	}
	if s.store.StageSnapshotBatch(s.ctx, s.peerID, m.ShareID, m.SnapshotID, m.Batch, rr) == nil {
		c.SnapshotBatch++
		_ = s.store.SetCursor(s.ctx, s.peerID, m.ShareID, "incoming", c)
	}
	return true
}

func handleIndexSnapshotEndFrame(s *Session, payload []byte) bool {
	var m protocol.IndexSnapshotEnd
	if !s.decodeWire(payload, &m) {
		return true
	}
	if !s.acceptsInboundIndex(m.ShareID) {
		s.rejectShareOperation()
		return true
	}
	s.snapshotMu.Lock()
	b, ok := s.snapshots[m.ShareID]
	if ok && b.SnapshotID == m.SnapshotID {
		delete(s.snapshots, m.ShareID)
	}
	s.snapshotMu.Unlock()
	c, _ := s.store.Cursor(s.ctx, s.peerID, m.ShareID, "incoming")
	if ok && b.SnapshotID == m.SnapshotID && c.SnapshotBatch == m.BatchCount && s.store.CommitSnapshot(s.ctx, s.peerID, m.ShareID, m.SnapshotID, b.Epoch, b.HighSeq) == nil {
		_ = s.writer.WriteLatestMessageContext(s.ctx, "index-ack\x00"+m.ShareID, protocol.MsgIndexAck, protocol.IndexAck{ShareID: m.ShareID, Epoch: b.Epoch, AppliedSeq: b.HighSeq, SnapshotID: m.SnapshotID})
		if !s.enqueueIndex("reconcile\x00"+m.ShareID, func() { s.reconcilePeerShare(s.ctx, m.ShareID) }) {
			s.rejectWork()
			s.logf("closing overloaded session: index queue is full")
			_ = s.conn.Close()
			return false
		}
	}
	return true
}

func handleIndexDeltaBatchFrame(s *Session, payload []byte) bool {
	var m protocol.IndexDeltaBatch
	if !s.decodeWire(payload, &m) {
		return true
	}
	if !s.acceptsInboundIndex(m.ShareID) {
		s.rejectShareOperation()
		return true
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
		_ = s.writer.WriteLatestMessageContext(s.ctx, "index-ack\x00"+m.ShareID, protocol.MsgIndexAck, protocol.IndexAck{ShareID: m.ShareID, Epoch: m.Epoch, AppliedSeq: m.ToSeq})
		if !s.enqueueIndex("reconcile\x00"+m.ShareID, func() { s.reconcilePeerShare(s.ctx, m.ShareID) }) {
			s.rejectWork()
			s.logf("closing overloaded session: index queue is full")
			_ = s.conn.Close()
			return false
		}
	} else {
		c, _ := s.store.Cursor(s.ctx, s.peerID, m.ShareID, "incoming")
		_ = s.writer.WriteLatestMessageContext(s.ctx, "index-sync\x00"+m.ShareID, protocol.MsgIndexSyncRequest, protocol.IndexSyncRequest{ShareID: m.ShareID, Epoch: c.Epoch, AppliedSeq: c.AppliedSeq})
	}
	return true
}

func handleIndexAckFrame(s *Session, payload []byte) bool {
	var m protocol.IndexAck
	if s.decodeWire(payload, &m) && s.servesOutboundShare(m.ShareID) {
		s.acceptIndexAck(m)
	} else if m.ShareID != "" {
		s.rejectShareOperation()
	}
	return true
}

func handleIndexUpdateFrame(s *Session, payload []byte) bool {
	var m protocol.IndexUpdate
	if !s.decodeWire(payload, &m) {
		return true
	}
	if !s.acceptsInboundIndex(m.ShareID) {
		s.rejectShareOperation()
		return true
	}
	if !s.startBounded(s.indexSem, func() { s.handleIndexUpdate(s.ctx, m) }) {
		s.rejectWork()
		s.logf("closing overloaded session: index workers are full")
		_ = s.conn.Close()
		return false
	}
	return true
}

func handleFileRequestFrame(s *Session, payload []byte) bool {
	var req protocol.FileRequest
	if !s.decodeWire(payload, &req) {
		return true
	}
	if !s.servesOutboundShare(req.ShareID) {
		s.rejectShareOperation()
		newTransferResponse(s, s.ctx, req).fail(protocol.ErrCodeFileNotFound, "unknown share")
		return true
	}
	if !s.startBounded(s.serveSem, func() { s.handleFileRequest(s.ctx, req) }) {
		s.rejectWork()
		s.logf("closing overloaded session: file workers are full")
		_ = s.conn.Close()
		return false
	}
	return true
}

func handleFileChunkFrame(s *Session, payload []byte) bool {
	hdr, data, err := protocol.DecodeFileChunk(payload)
	if err != nil {
		s.protocolViolation()
		s.logf("decode FileChunk: %v", err)
		return true
	}
	s.routeChunk(hdr.TransferID, pullChunk{header: hdr, data: data, eof: hdr.EOF})
	return true
}

func handleErrorFrame(s *Session, payload []byte) bool {
	var e protocol.Error
	if !s.decodeWire(payload, &e) {
		return true
	}
	if e.TransferID != "" {
		s.routeChunk(e.TransferID, pullChunk{err: &protocol.RemoteError{Code: e.Code, Msg: e.Msg}})
	} else {
		s.logf("peer error: %s: %s", protocol.SanitizeDiagnostic(e.Code), protocol.SanitizeDiagnostic(e.Msg))
	}
	return true
}

func handleCancelTransferFrame(s *Session, payload []byte) bool {
	var m protocol.CancelTransfer
	if s.decodeWire(payload, &m) {
		s.stats.transferCancellations.Add(1)
		s.cancelServe(m.TransferID)
	}
	return true
}

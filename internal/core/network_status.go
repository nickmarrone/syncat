package core

import (
	"time"

	"github.com/nickmarrone/syncat/internal/protocol"
	syncsvc "github.com/nickmarrone/syncat/internal/sync"
)

// WriterNetworkStatus exposes bounded queue gauges and cumulative write
// outcomes without identifying individual messages or payloads.
type WriterNetworkStatus struct {
	UrgentQueued, ControlQueued, BulkQueued       int
	UrgentCapacity, ControlCapacity, BulkCapacity int
	FramesWritten, UrgentFramesWritten            uint64
	ControlFramesWritten, BulkFramesWritten       uint64
	BytesWritten                                  uint64
	WriteFailures, WriteTimeouts                  uint64
	LastWriteDuration, MaxWriteDuration           time.Duration
}

// SessionNetworkStatus is the cumulative and live work performed by one
// configured peer across its connections during this node process.
type SessionNetworkStatus struct {
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
	Writer                                                  WriterNetworkStatus
}

// PeerNetworkStatus contains connection lifecycle and current/cumulative
// session telemetry for one configured peer.
type PeerNetworkStatus struct {
	DialAttempts, DialFailures                uint64
	TransportFailures, HandshakeFailures      uint64
	Backoffs                                  uint64
	TotalBackoff, LastBackoff                 time.Duration
	Connections, Reconnects, DedupLosses      uint64
	PingsSent, PongsReceived, DeadConnections uint64
	LastPingRTT                               time.Duration
	Session                                   SessionNetworkStatus
}

// NetworkStatus is the low-cardinality aggregate returned in Node.Status.
type NetworkStatus struct {
	InboundHandshakesActive int
	RejectedHandshakes      uint64
	RejectedOverload        uint64
	Totals                  PeerNetworkStatus
}

func addWriterStats(a, b protocol.StreamWriterStats, live bool) protocol.StreamWriterStats {
	a.FramesWritten += b.FramesWritten
	a.UrgentFramesWritten += b.UrgentFramesWritten
	a.ControlFramesWritten += b.ControlFramesWritten
	a.BulkFramesWritten += b.BulkFramesWritten
	a.BytesWritten += b.BytesWritten
	a.WriteFailures += b.WriteFailures
	a.WriteTimeouts += b.WriteTimeouts
	if b.LastWriteDuration != 0 {
		a.LastWriteDuration = b.LastWriteDuration
	}
	if b.MaxWriteDuration > a.MaxWriteDuration {
		a.MaxWriteDuration = b.MaxWriteDuration
	}
	if live {
		a.UrgentQueued, a.ControlQueued, a.BulkQueued = b.UrgentQueued, b.ControlQueued, b.BulkQueued
		a.UrgentCapacity, a.ControlCapacity, a.BulkCapacity = b.UrgentCapacity, b.ControlCapacity, b.BulkCapacity
	}
	return a
}

func addSessionStats(a, b syncsvc.SessionStats, live bool) syncsvc.SessionStats {
	if live {
		a.IndexWorkersActive, a.IndexQueueDepth, a.IndexQueueCapacity = b.IndexWorkersActive, b.IndexQueueDepth, b.IndexQueueCapacity
		a.ServeWorkersActive, a.PullsActive = b.ServeWorkersActive, b.PullsActive
	}
	a.ProtocolViolations += b.ProtocolViolations
	a.RejectedShareOperations += b.RejectedShareOperations
	a.RejectedWork += b.RejectedWork
	a.StaleTransferFrames += b.StaleTransferFrames
	a.PullsStarted += b.PullsStarted
	a.ServesStarted += b.ServesStarted
	a.BytesReceived += b.BytesReceived
	a.BytesSent += b.BytesSent
	a.TransferStalls += b.TransferStalls
	a.TransferCancellations += b.TransferCancellations
	a.HashFailures += b.HashFailures
	a.SnapshotsSent += b.SnapshotsSent
	a.SnapshotEntriesSent += b.SnapshotEntriesSent
	a.DeltaBatchesSent += b.DeltaBatchesSent
	a.DeltaEntriesSent += b.DeltaEntriesSent
	a.Reconciliations += b.Reconciliations
	a.ReconciliationsCoalesced += b.ReconciliationsCoalesced
	a.Writer = addWriterStats(a.Writer, b.Writer, live)
	return a
}

func toSessionNetworkStatus(s syncsvc.SessionStats) SessionNetworkStatus {
	w := s.Writer
	return SessionNetworkStatus{
		IndexWorkersActive: s.IndexWorkersActive, IndexQueueDepth: s.IndexQueueDepth, IndexQueueCapacity: s.IndexQueueCapacity,
		ServeWorkersActive: s.ServeWorkersActive, PullsActive: s.PullsActive,
		ProtocolViolations: s.ProtocolViolations, RejectedShareOperations: s.RejectedShareOperations,
		RejectedWork: s.RejectedWork, StaleTransferFrames: s.StaleTransferFrames,
		PullsStarted: s.PullsStarted, ServesStarted: s.ServesStarted,
		BytesReceived: s.BytesReceived, BytesSent: s.BytesSent,
		TransferStalls: s.TransferStalls, TransferCancellations: s.TransferCancellations, HashFailures: s.HashFailures,
		SnapshotsSent: s.SnapshotsSent, SnapshotEntriesSent: s.SnapshotEntriesSent,
		DeltaBatchesSent: s.DeltaBatchesSent, DeltaEntriesSent: s.DeltaEntriesSent, Reconciliations: s.Reconciliations,
		ReconciliationsCoalesced: s.ReconciliationsCoalesced,
		Writer: WriterNetworkStatus{
			UrgentQueued: w.UrgentQueued, ControlQueued: w.ControlQueued, BulkQueued: w.BulkQueued,
			UrgentCapacity: w.UrgentCapacity, ControlCapacity: w.ControlCapacity, BulkCapacity: w.BulkCapacity,
			FramesWritten: w.FramesWritten, UrgentFramesWritten: w.UrgentFramesWritten,
			ControlFramesWritten: w.ControlFramesWritten, BulkFramesWritten: w.BulkFramesWritten,
			BytesWritten: w.BytesWritten, WriteFailures: w.WriteFailures, WriteTimeouts: w.WriteTimeouts,
			LastWriteDuration: w.LastWriteDuration, MaxWriteDuration: w.MaxWriteDuration,
		},
	}
}

func addPeerNetworkStatus(a, b PeerNetworkStatus) PeerNetworkStatus {
	a.DialAttempts += b.DialAttempts
	a.DialFailures += b.DialFailures
	a.TransportFailures += b.TransportFailures
	a.HandshakeFailures += b.HandshakeFailures
	a.Backoffs += b.Backoffs
	a.TotalBackoff += b.TotalBackoff
	if b.LastBackoff > a.LastBackoff {
		a.LastBackoff = b.LastBackoff
	}
	a.Connections += b.Connections
	a.Reconnects += b.Reconnects
	a.DedupLosses += b.DedupLosses
	a.PingsSent += b.PingsSent
	a.PongsReceived += b.PongsReceived
	a.DeadConnections += b.DeadConnections
	if b.LastPingRTT > a.LastPingRTT {
		a.LastPingRTT = b.LastPingRTT
	}
	a.Session = addSessionNetworkStatus(a.Session, b.Session)
	return a
}

func addSessionNetworkStatus(a, b SessionNetworkStatus) SessionNetworkStatus {
	a.IndexWorkersActive += b.IndexWorkersActive
	a.IndexQueueDepth += b.IndexQueueDepth
	a.IndexQueueCapacity += b.IndexQueueCapacity
	a.ServeWorkersActive += b.ServeWorkersActive
	a.PullsActive += b.PullsActive
	a.ProtocolViolations += b.ProtocolViolations
	a.RejectedShareOperations += b.RejectedShareOperations
	a.RejectedWork += b.RejectedWork
	a.StaleTransferFrames += b.StaleTransferFrames
	a.PullsStarted += b.PullsStarted
	a.ServesStarted += b.ServesStarted
	a.BytesReceived += b.BytesReceived
	a.BytesSent += b.BytesSent
	a.TransferStalls += b.TransferStalls
	a.TransferCancellations += b.TransferCancellations
	a.HashFailures += b.HashFailures
	a.SnapshotsSent += b.SnapshotsSent
	a.SnapshotEntriesSent += b.SnapshotEntriesSent
	a.DeltaBatchesSent += b.DeltaBatchesSent
	a.DeltaEntriesSent += b.DeltaEntriesSent
	a.Reconciliations += b.Reconciliations
	a.ReconciliationsCoalesced += b.ReconciliationsCoalesced
	a.Writer.UrgentQueued += b.Writer.UrgentQueued
	a.Writer.ControlQueued += b.Writer.ControlQueued
	a.Writer.BulkQueued += b.Writer.BulkQueued
	a.Writer.UrgentCapacity += b.Writer.UrgentCapacity
	a.Writer.ControlCapacity += b.Writer.ControlCapacity
	a.Writer.BulkCapacity += b.Writer.BulkCapacity
	a.Writer.FramesWritten += b.Writer.FramesWritten
	a.Writer.UrgentFramesWritten += b.Writer.UrgentFramesWritten
	a.Writer.ControlFramesWritten += b.Writer.ControlFramesWritten
	a.Writer.BulkFramesWritten += b.Writer.BulkFramesWritten
	a.Writer.BytesWritten += b.Writer.BytesWritten
	a.Writer.WriteFailures += b.Writer.WriteFailures
	a.Writer.WriteTimeouts += b.Writer.WriteTimeouts
	if b.Writer.LastWriteDuration != 0 {
		a.Writer.LastWriteDuration = b.Writer.LastWriteDuration
	}
	if b.Writer.MaxWriteDuration > a.Writer.MaxWriteDuration {
		a.Writer.MaxWriteDuration = b.Writer.MaxWriteDuration
	}
	return a
}

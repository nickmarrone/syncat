// dto.go holds the converters that build the REST API's JSON shapes
// (package wire) from internal/core's plain status structs. core
// deliberately imports no encoding/json (see internal/core/status.go and
// SPEC.md §12), so this file is where that translation lives. Handlers
// never marshal core types directly: every response is a wire type.
package api

import (
	"strings"

	"github.com/nickmarrone/syncat/internal/api/wire"
	"github.com/nickmarrone/syncat/internal/config"
	"github.com/nickmarrone/syncat/internal/core"
	"github.com/nickmarrone/syncat/internal/version"
)

// Composite ids: config.Share/config.Peer are keyed by a single string
// (share id / peer key hex) that maps directly to a REST {id}. A
// subscription has no such field (SPEC.md §3 keys it by the pair
// peer+share_id), subscriptionID/parseSubscriptionID below encode that
// pair as "<peer-key-hex>:<share-id>" for the {id} path segment. Both
// halves are always plain hex, so splitting on the first ':' is
// unambiguous.

func toApprovalDTO(a core.Approval) wire.Approval {
	return wire.Approval{ID: a.ID, Kind: a.Kind, PeerKey: a.PeerKey, PeerName: a.PeerName, ShareID: a.ShareID, ShareName: a.ShareName, CreatedAt: a.CreatedAt}
}

func toSafetyDTO(s config.Safety) wire.Safety { return wire.Safety(s) }

func toHoldDTO(h core.HoldStatus) wire.Hold {
	return wire.Hold{
		ShareID: h.ShareID, ShareName: h.ShareName, PeerKey: h.PeerKey, PeerName: h.PeerName,
		Reason: h.Reason, Problem: h.Problem(), Remedy: h.Remedy(),
		Deletes: h.Deletes, Live: h.Live, Since: h.Since,
		Confirmed: h.Confirmed, Confirmable: h.Confirmable(),
	}
}

func toPeerDTO(p core.PeerStatus) wire.Peer {
	return wire.Peer{
		ID: p.PeerKey, PeerKey: p.PeerKey, ShortID: p.ShortID, Name: p.Name, RemoteName: p.RemoteName,
		Enabled: p.Enabled, State: string(p.State), LastError: p.LastError,
		LastConnectedAt: p.LastConnectedAt, ConnectedSince: p.ConnectedSince,
		Network: toPeerNetworkDTO(p.Network),
	}
}

func toWriterNetworkDTO(w core.WriterNetworkStatus) map[string]any {
	return map[string]any{
		"urgent_queued": w.UrgentQueued, "control_queued": w.ControlQueued, "latest_queued": w.LatestQueued, "bulk_queued": w.BulkQueued,
		"urgent_capacity": w.UrgentCapacity, "control_capacity": w.ControlCapacity, "latest_capacity": w.LatestCapacity, "bulk_capacity": w.BulkCapacity,
		"latest_replaced": w.LatestReplaced,
		"frames_written":  w.FramesWritten, "urgent_frames_written": w.UrgentFramesWritten,
		"control_frames_written": w.ControlFramesWritten, "bulk_frames_written": w.BulkFramesWritten,
		"bytes_written": w.BytesWritten, "write_failures": w.WriteFailures, "write_timeouts": w.WriteTimeouts,
		"last_write_seconds": w.LastWriteDuration.Seconds(), "max_write_seconds": w.MaxWriteDuration.Seconds(),
	}
}

func toSessionNetworkDTO(s core.SessionNetworkStatus) map[string]any {
	return map[string]any{
		"index_workers_active": s.IndexWorkersActive, "index_queue_depth": s.IndexQueueDepth, "index_queue_capacity": s.IndexQueueCapacity,
		"serve_workers_active": s.ServeWorkersActive, "pulls_active": s.PullsActive,
		"protocol_violations": s.ProtocolViolations, "rejected_share_operations": s.RejectedShareOperations,
		"rejected_work": s.RejectedWork, "stale_transfer_frames": s.StaleTransferFrames,
		"pulls_started": s.PullsStarted, "serves_started": s.ServesStarted,
		"bytes_received": s.BytesReceived, "bytes_sent": s.BytesSent,
		"transfer_stalls": s.TransferStalls, "transfer_cancellations": s.TransferCancellations, "hash_failures": s.HashFailures,
		"snapshots_sent": s.SnapshotsSent, "snapshot_entries_sent": s.SnapshotEntriesSent,
		"delta_batches_sent": s.DeltaBatchesSent, "delta_entries_sent": s.DeltaEntriesSent, "reconciliations": s.Reconciliations,
		"reconciliations_coalesced": s.ReconciliationsCoalesced,
		"writer":                    toWriterNetworkDTO(s.Writer),
	}
}

func toPeerNetworkDTO(p core.PeerNetworkStatus) map[string]any {
	return map[string]any{
		"dial_attempts": p.DialAttempts, "dial_failures": p.DialFailures,
		"transport_failures": p.TransportFailures, "handshake_failures": p.HandshakeFailures,
		"backoffs": p.Backoffs, "total_backoff_seconds": p.TotalBackoff.Seconds(), "last_backoff_seconds": p.LastBackoff.Seconds(),
		"connections": p.Connections, "reconnects": p.Reconnects, "dedup_losses": p.DedupLosses,
		"pings_sent": p.PingsSent, "pongs_received": p.PongsReceived, "dead_connections": p.DeadConnections,
		"last_ping_rtt_seconds": p.LastPingRTT.Seconds(), "session": toSessionNetworkDTO(p.Session),
	}
}

func toNetworkDTO(n core.NetworkStatus) map[string]any {
	return map[string]any{
		"inbound_handshakes_active": n.InboundHandshakesActive,
		"rejected_handshakes":       n.RejectedHandshakes,
		"rejected_overload":         n.RejectedOverload,
		"totals":                    toPeerNetworkDTO(n.Totals),
	}
}

func toShareDTO(s core.ShareStatus) wire.Share {
	access := make([]wire.ShareAccess, 0, len(s.Access))
	for _, a := range s.Access {
		access = append(access, wire.ShareAccess{PeerKey: a.PeerKey, PeerName: a.PeerName, Access: a.Access})
	}
	return wire.Share{
		ID: s.ShareID, Name: s.Name, Path: s.Path, Permission: s.Permission,
		ApprovalRequired: s.ApprovalRequired, Access: access, Safety: toSafetyDTO(s.Safety),
	}
}

func toRemoteShareDTO(r core.RemoteShareStatus) wire.RemoteShare {
	return wire.RemoteShare{
		PeerKey: r.PeerKey, PeerName: r.PeerName, ShareID: r.ShareID, Name: r.Name,
		Permission: r.Permission, ApprovalRequired: r.ApprovalRequired, Access: r.Access,
	}
}

func toWarningDTO(w core.WarningStatus) wire.Warning {
	return wire.Warning{ShareID: w.ShareID, RelPath: w.RelPath, At: w.At, Reverted: w.Reverted, Reason: w.Reason}
}

func toSubscriptionDTO(s core.SubscriptionStatus) wire.Subscription {
	warnings := make([]wire.Warning, 0, len(s.Warnings))
	for _, w := range s.Warnings {
		warnings = append(warnings, toWarningDTO(w))
	}
	return wire.Subscription{
		ID: subscriptionID(s.PeerKey, s.ShareID), PeerKey: s.PeerKey, PeerName: s.PeerName,
		ShareID: s.ShareID, ShareName: s.ShareName, LocalPath: s.LocalPath, Mode: s.Mode, Paused: s.Paused,
		Access: s.Access, Connected: s.Connected, Warnings: warnings, Safety: toSafetyDTO(s.Safety),
	}
}

func toRejectedConnDTO(r core.RejectedConnection) wire.RejectedConnection {
	return wire.RejectedConnection{PeerKey: r.PeerKey, PeerName: r.PeerName, At: r.At, Reason: r.Reason}
}

func toTrashDTO(st core.TrashStatus) wire.Trash {
	out := wire.Trash{ShareID: st.ShareID, Size: st.Size, Limit: st.Limit, RetentionDays: st.RetentionDays, Events: make([]wire.TrashEvent, 0, len(st.Events))}
	for _, ev := range st.Events {
		items := make([]wire.TrashItem, 0, len(ev.Items))
		for _, it := range ev.Items {
			items = append(items, wire.TrashItem{RelPath: it.RelPath, Kind: it.Kind, Size: it.Size})
		}
		out.Events = append(out.Events, wire.TrashEvent{ID: ev.ID, At: ev.At, Label: ev.Label, Source: ev.Source, Size: ev.Size, Items: items})
	}
	return out
}

func toStatusResponse(st core.Status) wire.Status {
	peers := make([]wire.Peer, 0, len(st.Peers))
	for _, p := range st.Peers {
		peers = append(peers, toPeerDTO(p))
	}
	shares := make([]wire.Share, 0, len(st.Shares))
	for _, s := range st.Shares {
		shares = append(shares, toShareDTO(s))
	}
	remoteShares := make([]wire.RemoteShare, 0, len(st.RemoteShares))
	for _, r := range st.RemoteShares {
		remoteShares = append(remoteShares, toRemoteShareDTO(r))
	}
	subs := make([]wire.Subscription, 0, len(st.Subscriptions))
	for _, s := range st.Subscriptions {
		subs = append(subs, toSubscriptionDTO(s))
	}
	holds := make([]wire.Hold, 0, len(st.Holds))
	for _, h := range st.Holds {
		holds = append(holds, toHoldDTO(h))
	}
	rejected := make([]wire.RejectedConnection, 0, len(st.RejectedConnections))
	for _, r := range st.RejectedConnections {
		rejected = append(rejected, toRejectedConnDTO(r))
	}
	return wire.Status{
		Version:  version.String(),
		NodeName: st.NodeName, NodeToken: st.NodeToken, ShortID: st.ShortID, PeerKey: st.PeerKey,
		StartedAt: st.StartedAt, UptimeSeconds: st.UptimeSeconds,
		Peers: peers, Shares: shares, RemoteShares: remoteShares, Subscriptions: subs,
		Holds: holds, Rejected: rejected, Network: toNetworkDTO(st.Network),
	}
}

// findPeerDTO returns the peer identified by id from st, or nil.
func findPeerDTO(st core.Status, id string) *wire.Peer {
	for _, p := range st.Peers {
		if p.PeerKey == id {
			dto := toPeerDTO(p)
			return &dto
		}
	}
	return nil
}

// findShareDTO returns the share identified by id from st, or nil.
func findShareDTO(st core.Status, id string) *wire.Share {
	for _, s := range st.Shares {
		if s.ShareID == id {
			dto := toShareDTO(s)
			return &dto
		}
	}
	return nil
}

// findSubscriptionDTO returns the subscription identified by (peerKey,
// shareID) from st, or nil.
func findSubscriptionDTO(st core.Status, peerKey, shareID string) *wire.Subscription {
	for _, s := range st.Subscriptions {
		if s.PeerKey == peerKey && s.ShareID == shareID {
			dto := toSubscriptionDTO(s)
			return &dto
		}
	}
	return nil
}

// subscriptionID encodes a subscription's composite (peer, share) key as
// a single REST {id} path segment.
func subscriptionID(peerKey, shareID string) string {
	return peerKey + ":" + shareID
}

// parseSubscriptionID reverses subscriptionID, splitting on the first ':'
// (both halves are plain hex, so this is unambiguous). ok is false if id
// isn't in that shape.
func parseSubscriptionID(id string) (peerKey, shareID string, ok bool) {
	peerKey, shareID, found := strings.Cut(id, ":")
	if !found || peerKey == "" || shareID == "" {
		return "", "", false
	}
	return peerKey, shareID, true
}

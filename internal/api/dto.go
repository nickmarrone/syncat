// dto.go holds the JSON shapes of the REST API and the converters that
// build them from internal/core's plain status structs. core deliberately
// imports no encoding/json (see internal/core/status.go and SPEC.md §12),
// so this file is where that marshaling lives.
package api

import (
	"strings"
	"time"

	"github.com/nickmarrone/syncat/internal/core"
	syncsvc "github.com/nickmarrone/syncat/internal/sync"
	"github.com/nickmarrone/syncat/internal/version"
)

// Below is the JSON marshaling internal/core deliberately doesn't do
// itself (SPEC.md §12): one DTO type per core.Status subtype, snake_case
// tags matching the wire-protocol field naming SPEC.md §4 already uses
// (share_id, approval_required, ...), plus the conversion functions that
// build them from a core.Status. Handlers never marshal core types
// directly — every response goes through one of these.
//
// Composite ids: config.Share/config.Peer are keyed by a single string
// (share id / peer key hex) that maps directly to a REST {id}. A
// subscription has no such field (SPEC.md §3 keys it by the pair
// peer+share_id) — subscriptionID/parseSubscriptionID below encode that
// pair as "<peer-key-hex>:<share-id>" for the {id} path segment. Both
// halves are always plain hex, so splitting on the first ':' is
// unambiguous.

// --- DTOs: the JSON shapes every response goes through -----------------

type statusResponse struct {
	// Version is this daemon's build, as version.String() renders it. It
	// is read straight from the binary rather than from core.Status
	// because it describes the build, not the node's state — nothing in
	// core has, or should have, an opinion about it.
	Version string `json:"version"`

	NodeName  string `json:"node_name"`
	NodeToken string `json:"node_token"`
	ShortID   string `json:"short_id"`
	PeerKey   string `json:"peer_key"`

	StartedAt     time.Time `json:"started_at"`
	UptimeSeconds float64   `json:"uptime_seconds"`

	Peers         []peerDTO         `json:"peers"`
	Shares        []shareDTO        `json:"shares"`
	RemoteShares  []remoteShareDTO  `json:"remote_shares"`
	Subscriptions []subscriptionDTO `json:"subscriptions"`
	Rejected      []rejectedConnDTO `json:"rejected_connections"`
	Network       map[string]any    `json:"network"`
}

type peerDTO struct {
	ID              string         `json:"id"` // full Ed25519 public key, hex — same as PeerKey
	PeerKey         string         `json:"peer_key"`
	ShortID         string         `json:"short_id"`
	Name            string         `json:"name"`
	RemoteName      string         `json:"remote_name"`
	Enabled         bool           `json:"enabled"`
	State           string         `json:"state"`
	LastError       string         `json:"last_error,omitempty"`
	LastConnectedAt time.Time      `json:"last_connected_at,omitempty"`
	ConnectedSince  time.Time      `json:"connected_since,omitempty"`
	Network         map[string]any `json:"network"`
}

type shareAccessDTO struct {
	PeerKey  string `json:"peer_key"`
	PeerName string `json:"peer_name,omitempty"`
	Access   string `json:"access"`
}

type shareDTO struct {
	ID               string           `json:"id"`
	Name             string           `json:"name"`
	Path             string           `json:"path"`
	Permission       string           `json:"permission"`
	ApprovalRequired bool             `json:"approval_required"`
	Access           []shareAccessDTO `json:"access"`
}

type remoteShareDTO struct {
	PeerKey          string `json:"peer_key"`
	PeerName         string `json:"peer_name,omitempty"`
	ShareID          string `json:"share_id"`
	Name             string `json:"name"`
	Permission       string `json:"permission"`
	ApprovalRequired bool   `json:"approval_required"`
	Access           string `json:"access"`
}

type warningDTO struct {
	ShareID  string    `json:"share_id"`
	RelPath  string    `json:"rel_path"`
	At       time.Time `json:"at"`
	Reverted bool      `json:"reverted"`
	Reason   string    `json:"reason"`
}

type subscriptionDTO struct {
	ID        string       `json:"id"`
	PeerKey   string       `json:"peer_key"`
	PeerName  string       `json:"peer_name,omitempty"`
	ShareID   string       `json:"share_id"`
	ShareName string       `json:"share_name,omitempty"`
	LocalPath string       `json:"local_path"`
	Mode      string       `json:"mode"`
	Paused    bool         `json:"paused"`
	Access    string       `json:"access"`
	Connected bool         `json:"connected"`
	Warnings  []warningDTO `json:"warnings,omitempty"`
}

type rejectedConnDTO struct {
	PeerKey  string    `json:"peer_key,omitempty"`
	PeerName string    `json:"peer_name,omitempty"`
	At       time.Time `json:"at"`
	Reason   string    `json:"reason"`
}

type trashEntryDTO struct {
	ShareID   string    `json:"share_id"`
	RelPath   string    `json:"rel_path"`
	TrashedAt time.Time `json:"trashed_at"`
	Size      int64     `json:"size"`
}

func toPeerDTO(p core.PeerStatus) peerDTO {
	return peerDTO{
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

func toShareDTO(s core.ShareStatus) shareDTO {
	access := make([]shareAccessDTO, 0, len(s.Access))
	for _, a := range s.Access {
		access = append(access, shareAccessDTO{PeerKey: a.PeerKey, PeerName: a.PeerName, Access: a.Access})
	}
	return shareDTO{
		ID: s.ShareID, Name: s.Name, Path: s.Path, Permission: s.Permission,
		ApprovalRequired: s.ApprovalRequired, Access: access,
	}
}

func toRemoteShareDTO(r core.RemoteShareStatus) remoteShareDTO {
	return remoteShareDTO{
		PeerKey: r.PeerKey, PeerName: r.PeerName, ShareID: r.ShareID, Name: r.Name,
		Permission: r.Permission, ApprovalRequired: r.ApprovalRequired, Access: r.Access,
	}
}

func toWarningDTO(w core.WarningStatus) warningDTO {
	return warningDTO{ShareID: w.ShareID, RelPath: w.RelPath, At: w.At, Reverted: w.Reverted, Reason: w.Reason}
}

func toSubscriptionDTO(s core.SubscriptionStatus) subscriptionDTO {
	warnings := make([]warningDTO, 0, len(s.Warnings))
	for _, w := range s.Warnings {
		warnings = append(warnings, toWarningDTO(w))
	}
	return subscriptionDTO{
		ID: subscriptionID(s.PeerKey, s.ShareID), PeerKey: s.PeerKey, PeerName: s.PeerName,
		ShareID: s.ShareID, ShareName: s.ShareName, LocalPath: s.LocalPath, Mode: s.Mode, Paused: s.Paused,
		Access: s.Access, Connected: s.Connected, Warnings: warnings,
	}
}

func toRejectedConnDTO(r core.RejectedConnection) rejectedConnDTO {
	return rejectedConnDTO{PeerKey: r.PeerKey, PeerName: r.PeerName, At: r.At, Reason: r.Reason}
}

func toTrashEntryDTO(e syncsvc.TrashEntry) trashEntryDTO {
	return trashEntryDTO{ShareID: e.ShareID, RelPath: e.RelPath, TrashedAt: e.TrashedAt, Size: e.Size}
}

func toStatusResponse(st core.Status) statusResponse {
	peers := make([]peerDTO, 0, len(st.Peers))
	for _, p := range st.Peers {
		peers = append(peers, toPeerDTO(p))
	}
	shares := make([]shareDTO, 0, len(st.Shares))
	for _, s := range st.Shares {
		shares = append(shares, toShareDTO(s))
	}
	remoteShares := make([]remoteShareDTO, 0, len(st.RemoteShares))
	for _, r := range st.RemoteShares {
		remoteShares = append(remoteShares, toRemoteShareDTO(r))
	}
	subs := make([]subscriptionDTO, 0, len(st.Subscriptions))
	for _, s := range st.Subscriptions {
		subs = append(subs, toSubscriptionDTO(s))
	}
	rejected := make([]rejectedConnDTO, 0, len(st.RejectedConnections))
	for _, r := range st.RejectedConnections {
		rejected = append(rejected, toRejectedConnDTO(r))
	}
	return statusResponse{
		Version:  version.String(),
		NodeName: st.NodeName, NodeToken: st.NodeToken, ShortID: st.ShortID, PeerKey: st.PeerKey,
		StartedAt: st.StartedAt, UptimeSeconds: st.UptimeSeconds,
		Peers: peers, Shares: shares, RemoteShares: remoteShares, Subscriptions: subs,
		Rejected: rejected, Network: toNetworkDTO(st.Network),
	}
}

// findPeerDTO returns the peer identified by id from st, or nil.
func findPeerDTO(st core.Status, id string) *peerDTO {
	for _, p := range st.Peers {
		if p.PeerKey == id {
			dto := toPeerDTO(p)
			return &dto
		}
	}
	return nil
}

// findShareDTO returns the share identified by id from st, or nil.
func findShareDTO(st core.Status, id string) *shareDTO {
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
func findSubscriptionDTO(st core.Status, peerKey, shareID string) *subscriptionDTO {
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

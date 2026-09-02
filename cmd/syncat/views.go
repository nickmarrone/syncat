package main

import "time"

// These mirror internal/api's JSON response shapes (dto.go) just closely
// enough for the CLI to render them — a separate, intentionally decoupled
// copy, since the CLI is a thin HTTP client (SPEC.md §1) and must not
// import internal/api or internal/core directly.

type peerView struct {
	ID              string    `json:"id"`
	ShortID         string    `json:"short_id"`
	Name            string    `json:"name"`
	RemoteName      string    `json:"remote_name"`
	Enabled         bool      `json:"enabled"`
	State           string    `json:"state"`
	LastError       string    `json:"last_error"`
	LastConnectedAt time.Time `json:"last_connected_at"`
	ConnectedSince  time.Time `json:"connected_since"`
}

type shareAccessView struct {
	PeerKey  string `json:"peer_key"`
	PeerName string `json:"peer_name"`
	Access   string `json:"access"`
}

type shareView struct {
	ID               string            `json:"id"`
	Name             string            `json:"name"`
	Path             string            `json:"path"`
	Permission       string            `json:"permission"`
	ApprovalRequired bool              `json:"approval_required"`
	Access           []shareAccessView `json:"access"`
}

type remoteShareView struct {
	PeerKey          string `json:"peer_key"`
	PeerName         string `json:"peer_name"`
	ShareID          string `json:"share_id"`
	Name             string `json:"name"`
	Permission       string `json:"permission"`
	ApprovalRequired bool   `json:"approval_required"`
	Access           string `json:"access"`
}

type warningView struct {
	ShareID  string    `json:"share_id"`
	RelPath  string    `json:"rel_path"`
	At       time.Time `json:"at"`
	Reverted bool      `json:"reverted"`
	Reason   string    `json:"reason"`
}

type subscriptionView struct {
	ID        string        `json:"id"`
	PeerKey   string        `json:"peer_key"`
	PeerName  string        `json:"peer_name"`
	ShareID   string        `json:"share_id"`
	ShareName string        `json:"share_name"`
	LocalPath string        `json:"local_path"`
	Mode      string        `json:"mode"`
	Paused    bool          `json:"paused"`
	Access    string        `json:"access"`
	Connected bool          `json:"connected"`
	Warnings  []warningView `json:"warnings"`
}

type rejectedView struct {
	PeerKey  string    `json:"peer_key"`
	PeerName string    `json:"peer_name"`
	At       time.Time `json:"at"`
	Reason   string    `json:"reason"`
}

type trashEntryView struct {
	ShareID   string    `json:"share_id"`
	RelPath   string    `json:"rel_path"`
	TrashedAt time.Time `json:"trashed_at"`
	Size      int64     `json:"size"`
}

type statusView struct {
	NodeName  string `json:"node_name"`
	NodeToken string `json:"node_token"`
	ShortID   string `json:"short_id"`
	PeerKey   string `json:"peer_key"`

	StartedAt     time.Time `json:"started_at"`
	UptimeSeconds float64   `json:"uptime_seconds"`

	Peers         []peerView         `json:"peers"`
	Shares        []shareView        `json:"shares"`
	RemoteShares  []remoteShareView  `json:"remote_shares"`
	Subscriptions []subscriptionView `json:"subscriptions"`
	Rejected      []rejectedView     `json:"rejected_connections"`
}

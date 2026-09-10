package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/nickmarrone/syncat/internal/api"
	"github.com/nickmarrone/syncat/internal/config"
)

// apiClient is the thin HTTP client every syncat subcommand (other than
// init/token, which work directly on disk before a daemon can exist)
// drives against the REST API (SPEC.md §1: "All other syncat subcommands
// are thin clients of that REST API").
type apiClient struct {
	baseURL string
	token   string
	http    *http.Client
}

// newAPIClient loads config.json (for the API address) and api.token (for
// auth) from paths and builds a client. Both are written by `syncat
// init`, so a missing one gets turned into a hint to run it, matching
// cmdToken's existing style.
func newAPIClient(paths *config.Paths) (*apiClient, error) {
	cfg, err := config.Load(paths.ConfigFile())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("no config found at %s (run `syncat init` first): %w", paths.ConfigFile(), err)
		}
		return nil, err
	}
	tokData, err := os.ReadFile(paths.APITokenFile())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("no api token found at %s (run `syncat init` first): %w", paths.APITokenFile(), err)
		}
		return nil, err
	}

	addr := cfg.APIAddr
	if addr == "" {
		addr = config.DefaultAPIAddr
	}
	normalized, err := api.NormalizeLoopbackAddress(addr)
	if err != nil {
		return nil, err
	}
	return &apiClient{
		baseURL: "http://" + normalized.ClientAuthority,
		token:   strings.TrimSpace(string(tokData)),
		http:    &http.Client{Timeout: 15 * time.Second},
	}, nil
}

// apiErrorBody mirrors internal/api's stable error response shape.
type apiErrorBody struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// do issues one API request. body, if non-nil, is JSON-encoded as the
// request body; out, if non-nil, receives the JSON-decoded response body
// on success. A transport-level failure (most commonly: nothing is
// listening on baseURL) is turned into a hint that the daemon may not be
// running, rather than a raw "connection refused".
func (c *apiClient) do(ctx context.Context, method, path string, body, out any) error {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode request: %w", err)
		}
		reader = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("X-Syncat-Token", c.token)

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("cannot reach syncat daemon at %s (is it running? try `syncat daemon`): %w", c.baseURL, err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response from %s: %w", c.baseURL, err)
	}

	if resp.StatusCode >= 400 {
		var eb apiErrorBody
		if json.Unmarshal(data, &eb) == nil && eb.Error.Message != "" {
			return errors.New(eb.Error.Message)
		}
		return fmt.Errorf("request failed with status %d", resp.StatusCode)
	}
	if out != nil && len(data) > 0 {
		if err := json.Unmarshal(data, out); err != nil {
			return fmt.Errorf("decode response from %s: %w", c.baseURL, err)
		}
	}
	return nil
}

// printJSON writes v to stdout as indented JSON, for every subcommand's
// --json flag.
func printJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// These mirror internal/api's JSON response shapes just closely
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
	Network       map[string]any     `json:"network"`
}

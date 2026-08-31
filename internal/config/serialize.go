package config

import (
	"encoding/json"
	"fmt"
)

// This file is the only place that knows the on-disk encoding of Config.
// Swapping the wire format later (see the deviation note in schema.go)
// should only require changes here: Marshal/Unmarshal's signatures, and the
// rest of the package's use of them, stay the same.

// configDoc mirrors Config for JSON encoding, using SPEC.md §3's
// snake_case field names.
type configDoc struct {
	NodeName              string         `json:"node_name"`
	APIAddr               string         `json:"api_addr"`
	TrashRetentionDays    int            `json:"trash_retention_days"`
	RescanIntervalSeconds int            `json:"rescan_interval_seconds"`
	Debug                 bool           `json:"debug"`
	GlobalIgnores         []string       `json:"global_ignores,omitempty"`
	Peers                 []peerDoc      `json:"peers,omitempty"`
	Shares                []shareDoc     `json:"shares,omitempty"`
	Subscriptions         []subscription `json:"subscriptions,omitempty"`
}

// peerDoc mirrors Peer, except Enabled is a pointer so Unmarshal can tell
// "absent from the file" (defaults to true) apart from an explicit false.
type peerDoc struct {
	Name    string `json:"name"`
	Token   string `json:"token"`
	Enabled *bool  `json:"enabled,omitempty"`
}

type shareDoc struct {
	ID               string            `json:"id"`
	Name             string            `json:"name"`
	Path             string            `json:"path"`
	Permission       string            `json:"permission"`
	ApprovalRequired bool              `json:"approval_required"`
	Access           map[string]string `json:"access,omitempty"`
}

// subscription mirrors Subscription 1:1 (no fields need pointer-defaulting)
// so it doubles as both the doc and public type's field layout.
type subscription struct {
	Peer      string `json:"peer"`
	ShareID   string `json:"share_id"`
	LocalPath string `json:"local_path"`
	Mode      string `json:"mode"`
	Paused    bool   `json:"paused"`
}

// Marshal encodes cfg as indented JSON (config.json stays hand-editable).
func Marshal(cfg *Config) ([]byte, error) {
	doc := configToDoc(cfg)
	data, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("config: marshal: %w", err)
	}
	return data, nil
}

// Unmarshal decodes JSON bytes into a Config, applying defaults (including
// Peer.Enabled defaulting to true when absent).
func Unmarshal(data []byte) (*Config, error) {
	var doc configDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("config: unmarshal: %w", err)
	}
	cfg := docToConfig(&doc)
	cfg.ApplyDefaults()
	return cfg, nil
}

func configToDoc(cfg *Config) *configDoc {
	doc := &configDoc{
		NodeName:              cfg.NodeName,
		APIAddr:               cfg.APIAddr,
		TrashRetentionDays:    cfg.TrashRetentionDays,
		RescanIntervalSeconds: cfg.RescanIntervalSeconds,
		Debug:                 cfg.Debug,
		GlobalIgnores:         cfg.GlobalIgnores,
	}
	for _, p := range cfg.Peers {
		enabled := p.Enabled
		doc.Peers = append(doc.Peers, peerDoc{Name: p.Name, Token: p.Token, Enabled: &enabled})
	}
	for _, s := range cfg.Shares {
		doc.Shares = append(doc.Shares, shareDoc{
			ID:               s.ID,
			Name:             s.Name,
			Path:             s.Path,
			Permission:       s.Permission,
			ApprovalRequired: s.ApprovalRequired,
			Access:           s.Access,
		})
	}
	for _, sub := range cfg.Subscriptions {
		doc.Subscriptions = append(doc.Subscriptions, subscription(sub))
	}
	return doc
}

func docToConfig(doc *configDoc) *Config {
	cfg := &Config{
		NodeName:              doc.NodeName,
		APIAddr:               doc.APIAddr,
		TrashRetentionDays:    doc.TrashRetentionDays,
		RescanIntervalSeconds: doc.RescanIntervalSeconds,
		Debug:                 doc.Debug,
		GlobalIgnores:         doc.GlobalIgnores,
	}
	for _, p := range doc.Peers {
		enabled := true
		if p.Enabled != nil {
			enabled = *p.Enabled
		}
		cfg.Peers = append(cfg.Peers, Peer{Name: p.Name, Token: p.Token, Enabled: enabled})
	}
	for _, s := range doc.Shares {
		cfg.Shares = append(cfg.Shares, Share{
			ID:               s.ID,
			Name:             s.Name,
			Path:             s.Path,
			Permission:       s.Permission,
			ApprovalRequired: s.ApprovalRequired,
			Access:           s.Access,
		})
	}
	for _, sub := range doc.Subscriptions {
		cfg.Subscriptions = append(cfg.Subscriptions, Subscription(sub))
	}
	return cfg
}

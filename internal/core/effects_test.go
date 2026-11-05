package core

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"reflect"
	"testing"

	"github.com/nickmarrone/syncat/internal/config"
	"github.com/nickmarrone/syncat/internal/protocol"
)

// testPeer returns a configured peer with a fresh key, and that key.
func testPeer(t *testing.T, name string) (config.Peer, string) {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tok, err := config.EncodeToken("pipe-unreachable", pub, name)
	if err != nil {
		t.Fatal(err)
	}
	return config.Peer{Name: name, Token: tok, Enabled: true}, hex.EncodeToString(pub)
}

// summary is the part of a configChange each test case checks, in a form
// that compares and prints well.
type summary struct {
	PeersAdded, PeersRemoved                     []string
	SharesAdded, SharesRemoved, SharesRedirected []string
	Access                                       []string // "share peer access"
	SubsStarted, SubsStopped                     []string // "peer/share"
	Announce, NodeRenamed                        bool
}

func summarize(c configChange) summary {
	var s summary
	for _, p := range c.peersAdded {
		s.PeersAdded = append(s.PeersAdded, p.Name)
	}
	s.PeersRemoved = c.peersRemoved
	for _, sh := range c.sharesAdded {
		s.SharesAdded = append(s.SharesAdded, sh.ID)
	}
	s.SharesRemoved = c.sharesRemoved
	for _, sh := range c.sharesRedirected {
		s.SharesRedirected = append(s.SharesRedirected, sh.ID)
	}
	for _, a := range c.access {
		s.Access = append(s.Access, a.share.ID+" "+a.peerKey+" "+a.access)
	}
	for _, sub := range c.subsStarted {
		s.SubsStarted = append(s.SubsStarted, sub.Peer+"/"+sub.ShareID)
	}
	for _, sub := range c.subsStopped {
		s.SubsStopped = append(s.SubsStopped, sub.Peer+"/"+sub.ShareID)
	}
	s.Announce, s.NodeRenamed = c.announce, c.nodeRenamed
	return s
}

// Each case edits a base config the way one mutation would and checks
// the effects diffConfig derives from that alone.
func TestDiffConfig(t *testing.T) {
	laptop, laptopKey := testPeer(t, "laptop")
	desk, deskKey := testPeer(t, "desk")
	base := func() *config.Config {
		cfg := config.Default()
		cfg.NodeName = "node"
		cfg.Peers = []config.Peer{laptop}
		cfg.Shares = []config.Share{{
			ID: "docs", Name: "docs", Path: "/docs", Permission: config.PermissionReadWrite,
			Access: map[string]string{laptopKey: protocol.AccessGranted},
		}}
		cfg.Subscriptions = []config.Subscription{{Peer: laptopKey, ShareID: "photos", LocalPath: "/photos"}}
		return cfg
	}

	for _, tc := range []struct {
		name string
		edit func(*config.Config)
		want summary
	}{
		{"nothing", func(*config.Config) {}, summary{}},
		{"safety only", func(c *config.Config) { c.Shares[0].Safety.HoldDeleteMin = 3 }, summary{}},
		{"add a share", func(c *config.Config) {
			c.Shares = append(c.Shares, config.Share{ID: "music", Path: "/music", Access: map[string]string{}})
		}, summary{SharesAdded: []string{"music"}, Announce: true}},
		{"remove a share and its access", func(c *config.Config) { c.Shares = nil },
			summary{SharesRemoved: []string{"docs"}, Announce: true}},
		{"rename a share", func(c *config.Config) { c.Shares[0].Name = "documents" }, summary{Announce: true}},
		{"approval required", func(c *config.Config) { c.Shares[0].ApprovalRequired = true }, summary{Announce: true}},
		{"permission", func(c *config.Config) { c.Shares[0].Permission = config.PermissionReadOnly },
			summary{SharesRedirected: []string{"docs"}, Announce: true}},
		{"revoke", func(c *config.Config) { c.Shares[0].Access[laptopKey] = protocol.AccessRevoked },
			summary{Access: []string{"docs " + laptopKey + " revoked"}}},
		{"grant a second peer", func(c *config.Config) { c.Shares[0].Access[deskKey] = protocol.AccessGranted },
			summary{Access: []string{"docs " + deskKey + " granted"}}},
		{"rename and revoke in one change", func(c *config.Config) {
			c.Shares[0].Name = "documents"
			c.Shares[0].Access[laptopKey] = protocol.AccessRevoked
		}, summary{Access: []string{"docs " + laptopKey + " revoked"}, Announce: true}},
		{"add a peer", func(c *config.Config) { c.Peers = append(c.Peers, desk) },
			summary{PeersAdded: []string{"desk"}}},
		{"remove a peer with everything of it", func(c *config.Config) {
			c.Peers = nil
			c.Subscriptions = nil
			delete(c.Shares[0].Access, laptopKey)
		}, summary{PeersRemoved: []string{laptopKey}, SubsStopped: []string{laptopKey + "/photos"}}},
		{"add a subscription", func(c *config.Config) {
			c.Subscriptions = append(c.Subscriptions, config.Subscription{Peer: laptopKey, ShareID: "music"})
		}, summary{SubsStarted: []string{laptopKey + "/music"}}},
		{"add a paused subscription", func(c *config.Config) {
			c.Subscriptions = append(c.Subscriptions, config.Subscription{Peer: laptopKey, ShareID: "music", Paused: true})
		}, summary{}},
		{"pause", func(c *config.Config) { c.Subscriptions[0].Paused = true },
			summary{SubsStopped: []string{laptopKey + "/photos"}}},
		{"remove a subscription", func(c *config.Config) { c.Subscriptions = nil },
			summary{SubsStopped: []string{laptopKey + "/photos"}}},
		{"rename the node", func(c *config.Config) { c.NodeName = "other" }, summary{NodeRenamed: true}},
		{"queue a pending peer", func(c *config.Config) {
			c.PendingPeers = append(c.PendingPeers, config.PendingPeer{Name: "stranger"})
		}, summary{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			old := base()
			new := cloneConfig(old)
			tc.edit(new)
			if got := summarize(diffConfig(old, new)); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("diffConfig:\n got %+v\nwant %+v", got, tc.want)
			}
		})
	}

	t.Run("resume", func(t *testing.T) {
		old := base()
		old.Subscriptions[0].Paused = true
		new := cloneConfig(old)
		new.Subscriptions[0].Paused = false
		want := summary{SubsStarted: []string{laptopKey + "/photos"}}
		if got := summarize(diffConfig(old, new)); !reflect.DeepEqual(got, want) {
			t.Errorf("diffConfig:\n got %+v\nwant %+v", got, want)
		}
	})
}

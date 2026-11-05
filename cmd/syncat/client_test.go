package main

import (
	"context"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nickmarrone/syncat/internal/api"
	"github.com/nickmarrone/syncat/internal/api/wire"
	"github.com/nickmarrone/syncat/internal/config"
	"github.com/nickmarrone/syncat/internal/core"
	"github.com/nickmarrone/syncat/internal/transport"
	"github.com/nickmarrone/syncat/internal/version"
)

// The CLI decodes what a real daemon sends into the same wire types the
// server encodes. Its own copies of the shapes used to drop whatever they
// did not list, such as each share's safety settings, from --json output.
func TestClientDecodesTheServersResponses(t *testing.T) {
	dir := t.TempDir()
	paths, err := config.ResolvePaths(filepath.Join(dir, "config"), filepath.Join(dir, "data"))
	if err != nil {
		t.Fatal(err)
	}
	if err := paths.EnsureDirs(); err != nil {
		t.Fatal(err)
	}
	identity, _, err := config.LoadOrCreateIdentityKey(paths.IdentityKeyFile())
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Default()
	cfg.NodeName = "test-node"
	node, err := core.Open(context.Background(), core.Options{
		Paths: paths, Config: cfg, Identity: identity, Transport: transport.NewPipeTransport("pipe-" + t.Name()),
		Logger: log.New(io.Discard, "", 0), DisableJitter: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { node.Close() })
	shareID, err := node.AddShare(t.TempDir(), "docs", config.PermissionReadWrite, false)
	if err != nil {
		t.Fatal(err)
	}

	const token = "test-token"
	hs := httptest.NewUnstartedServer(nil)
	hs.Config.Handler = api.NewServerWithAuthorities(node, token, log.New(io.Discard, "", 0), hs.Listener.Addr().String()).Handler()
	hs.Start()
	t.Cleanup(hs.Close)
	c := &apiClient{baseURL: hs.URL, token: token, http: &http.Client{Timeout: 5 * time.Second}}
	ctx := context.Background()

	var st wire.Status
	if err := c.do(ctx, http.MethodGet, "/api/status", nil, &st); err != nil {
		t.Fatalf("GET /api/status: %v", err)
	}
	if st.Version != version.String() || st.NodeName != "test-node" {
		t.Errorf("status = version %q, node %q; want %q, test-node", st.Version, st.NodeName, version.String())
	}
	if len(st.Shares) != 1 || st.Shares[0].ID != shareID || st.Shares[0].Safety.HoldDeleteMin != config.DefaultHoldDeleteMin {
		t.Errorf("status shares = %+v, want docs with its safety settings filled in", st.Shares)
	}

	var shares wire.ShareList
	if err := c.do(ctx, http.MethodGet, "/api/shares", nil, &shares); err != nil {
		t.Fatalf("GET /api/shares: %v", err)
	}
	if len(shares.Shares) != 1 || shares.Shares[0].Name != "docs" {
		t.Errorf("GET /api/shares = %+v, want docs", shares)
	}

	// An error response decodes as wire.Error and comes back as its message.
	err = c.do(ctx, http.MethodDelete, "/api/shares/nonesuch", nil, nil)
	if err == nil || !strings.Contains(err.Error(), "nonesuch") {
		t.Errorf("DELETE of an unknown share: err = %v, want the server's message naming it", err)
	}
}

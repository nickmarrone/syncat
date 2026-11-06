package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"io"
	"log"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/nickmarrone/syncat/internal/api"
	"github.com/nickmarrone/syncat/internal/api/wire"
	"github.com/nickmarrone/syncat/internal/config"
	"github.com/nickmarrone/syncat/internal/core"
	"github.com/nickmarrone/syncat/internal/transport"
	"github.com/nickmarrone/syncat/internal/version"
	"github.com/tailscale/tailcat"
)

// cliNode is a real core.Node behind a real API server, with config.json
// and api.token written where `syncat --config DIR --data DIR` looks, so
// the CLI's own run() can be driven end to end in-process: argument
// parsing, the HTTP client, the handler, and core.
type cliNode struct {
	t     *testing.T
	paths *config.Paths
	node  *core.Node
}

var stdoutMu sync.Mutex // run() prints to os.Stdout; tests swap it, one at a time

func newCLINode(t *testing.T) *cliNode {
	t.Helper()
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
	// LoadOrCreateTailcatKey would pick a DERP region over the network;
	// a key with a fixed region is all `syncat token` needs.
	tk := tailcat.NewPrivateKey()
	tk.Public.RegionID = 1
	tkData, err := json.Marshal(tk)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(paths.TailcatKeyFile(), tkData, 0o600); err != nil {
		t.Fatal(err)
	}

	hs := httptest.NewUnstartedServer(nil)
	cfg := config.Default()
	cfg.NodeName = "cli-node"
	cfg.APIAddr = hs.Listener.Addr().String()
	if err := config.Save(paths.ConfigFile(), cfg); err != nil {
		t.Fatal(err)
	}
	const token = "cli-test-token"
	if err := os.WriteFile(paths.APITokenFile(), []byte(token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	node, err := core.Open(context.Background(), core.Options{
		Paths: paths, Config: cfg, Identity: identity, Transport: transport.NewPipeTransport("pipe-cli-" + t.Name()),
		Logger: log.New(io.Discard, "", 0), DisableJitter: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { node.Close() })
	hs.Config.Handler = api.NewServerWithAuthorities(node, token, log.New(io.Discard, "", 0), cfg.APIAddr).Handler()
	hs.Start()
	t.Cleanup(hs.Close)
	return &cliNode{t: t, paths: paths, node: node}
}

// run runs `syncat --config ... --data ... ARGS...` and returns what it
// printed to stdout and the error run() returned.
func (c *cliNode) run(args ...string) (string, error) {
	c.t.Helper()
	return runCLI(c.t, append([]string{"--config", c.paths.ConfigDir, "--data", c.paths.DataDir}, args...)...)
}

// ok is run, failing the test on an error.
func (c *cliNode) ok(args ...string) string {
	c.t.Helper()
	out, err := c.run(args...)
	if err != nil {
		c.t.Fatalf("syncat %s: %v\noutput: %s", strings.Join(args, " "), err, out)
	}
	return out
}

// fails is run, failing the test unless run() errors with a message
// containing want.
func (c *cliNode) fails(want string, args ...string) {
	c.t.Helper()
	out, err := c.run(args...)
	if err == nil {
		c.t.Fatalf("syncat %s succeeded, want an error containing %q\noutput: %s", strings.Join(args, " "), want, out)
	}
	if !strings.Contains(err.Error(), want) {
		c.t.Fatalf("syncat %s: err = %q, want it to contain %q", strings.Join(args, " "), err, want)
	}
}

func runCLI(t *testing.T, args ...string) (string, error) {
	t.Helper()
	stdoutMu.Lock()
	defer stdoutMu.Unlock()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	orig := os.Stdout
	os.Stdout = w
	done := make(chan []byte)
	go func() { b, _ := io.ReadAll(r); done <- b }()
	runErr := run(args)
	os.Stdout = orig
	w.Close()
	return string(<-done), runErr
}

func decodeJSON[T any](t *testing.T, out string) T {
	t.Helper()
	var v T
	if err := json.Unmarshal([]byte(out), &v); err != nil {
		t.Fatalf("decode --json output: %v\n%s", err, out)
	}
	return v
}

func fakeToken(t *testing.T, name string) string {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tok, err := config.EncodeToken("pipe-unreachable-"+name, pub, name)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

// --- the top level ----------------------------------------------------------

func TestRunTopLevel(t *testing.T) {
	out, err := runCLI(t)
	if err != nil || !strings.Contains(out, "Usage:") {
		t.Errorf("no args: err=%v, want usage on stdout, got %q", err, out)
	}
	out, err = runCLI(t, "--version")
	if err != nil || strings.TrimSpace(out) != version.String() {
		t.Errorf("--version: %q, %v; want %q", out, err, version.String())
	}
	out, err = runCLI(t, "version")
	if err != nil || !strings.Contains(out, version.String()) {
		t.Errorf("version: %q, %v; want it to print %q", out, err, version.String())
	}
	if _, err := runCLI(t, "frobnicate"); err == nil || !strings.Contains(err.Error(), `unknown command "frobnicate"`) {
		t.Errorf("unknown command: err = %v", err)
	}
	if _, err := runCLI(t, "--no-such-flag"); err == nil {
		t.Error("an unknown global flag was accepted")
	}
}

func TestSplitLeadingPositional(t *testing.T) {
	for _, tc := range []struct {
		args      []string
		n         int
		pos, rest []string
	}{
		{nil, 1, nil, nil},
		{[]string{"a"}, 1, []string{"a"}, nil},
		{[]string{"a", "b", "--x"}, 1, []string{"a"}, []string{"b", "--x"}},
		{[]string{"a", "b", "c", "--mode", "m"}, 3, []string{"a", "b", "c"}, []string{"--mode", "m"}},
		{[]string{"a", "--mode", "m", "b"}, 3, []string{"a"}, []string{"--mode", "m", "b"}},
		{[]string{"--name", "n", "a"}, 1, nil, []string{"--name", "n", "a"}},
	} {
		pos, rest := splitLeadingPositional(tc.args, tc.n)
		if strings.Join(pos, "|") != strings.Join(tc.pos, "|") || strings.Join(rest, "|") != strings.Join(tc.rest, "|") {
			t.Errorf("splitLeadingPositional(%q, %d) = %q, %q; want %q, %q", tc.args, tc.n, pos, rest, tc.pos, tc.rest)
		}
	}
}

func TestFormatSize(t *testing.T) {
	for n, want := range map[int64]string{0: "0 B", 1023: "1023 B"} {
		if got := formatSize(n); got != want {
			t.Errorf("formatSize(%d) = %q, want %q", n, got, want)
		}
	}
	for _, n := range []int64{1 << 10, 5 << 20, 3 << 30} {
		if got := formatSize(n); strings.HasSuffix(got, " B") || got == "" {
			t.Errorf("formatSize(%d) = %q, want a scaled unit", n, got)
		}
	}
}

// --- commands that need no daemon -------------------------------------------

func TestTokenAndConfigSet(t *testing.T) {
	c := newCLINode(t)
	tok := strings.TrimSpace(c.ok("token"))
	if _, err := config.ParseToken(tok); err != nil {
		t.Errorf("token printed %q, which does not parse: %v", tok, err)
	}

	for _, tc := range []struct{ field, value, want string }{
		{"node_name", "renamed", "node_name = renamed"},
		{"debug", "true", "debug = true"},
		{"trash_retention_days", "7", "trash_retention_days = 7"},
		{"rescan_interval_seconds", "120", "rescan_interval_seconds = 120"},
	} {
		if out := c.ok("config", "set", tc.field, tc.value); !strings.Contains(out, tc.want) {
			t.Errorf("config set %s %s printed %q, want %q", tc.field, tc.value, out, tc.want)
		}
	}
	cfg, err := config.Load(c.paths.ConfigFile())
	if err != nil {
		t.Fatal(err)
	}
	if cfg.NodeName != "renamed" || !cfg.Debug || cfg.TrashRetentionDays != 7 || cfg.RescanIntervalSeconds != 120 {
		t.Errorf("config after set = %+v", cfg)
	}

	c.fails("unknown config field", "config", "set", "nope", "1")
	c.fails("must be an integer", "config", "set", "trash_retention_days", "seven")
	c.fails("must be a boolean", "config", "set", "debug", "maybe")
	c.fails("usage: syncat config set", "config", "set", "debug")
	c.fails("unknown config subcommand", "config", "get", "debug")
}

// --- shares -----------------------------------------------------------------

func TestShareCommands(t *testing.T) {
	c := newCLINode(t)
	if out := c.ok("share", "ls"); !strings.Contains(out, "no shares configured") {
		t.Errorf("share ls on an empty node printed %q", out)
	}
	dir := t.TempDir()
	out := c.ok("share", "add", dir, "--name", "docs", "--perm", "rw")
	if !strings.Contains(out, `added share "docs"`) || !strings.Contains(out, "read-write") {
		t.Errorf("share add printed %q", out)
	}
	shares := decodeJSON[[]wire.Share](t, c.ok("share", "ls", "--json"))
	if len(shares) != 1 || shares[0].Name != "docs" || shares[0].Path != dir || shares[0].Permission != config.PermissionReadWrite {
		t.Fatalf("share ls --json = %+v", shares)
	}
	id := shares[0].ID
	if out := c.ok("share", "ls"); !strings.Contains(out, id) || !strings.Contains(out, "docs") {
		t.Errorf("share ls printed %q, want the share's id and name", out)
	}

	out = c.ok("share", "set", "docs", "--name", "papers", "--perm", "ro", "--approval")
	if !strings.Contains(out, `"papers"`) || !strings.Contains(out, "approval=true") {
		t.Errorf("share set printed %q", out)
	}
	shares = decodeJSON[[]wire.Share](t, c.ok("share", "ls", "--json"))
	if shares[0].Name != "papers" || shares[0].Permission != config.PermissionReadOnly || !shares[0].ApprovalRequired {
		t.Errorf("after share set: %+v", shares[0])
	}
	c.ok("share", "set", id[:6], "--no-approval")

	c.fails("--name is required", "share", "add", dir)
	c.fails("usage: syncat share add", "share", "add", "--name", "x")
	c.fails("nothing to change", "share", "set", "papers")
	c.fails("mutually exclusive", "share", "set", "papers", "--approval", "--no-approval")
	c.fails("unknown share subcommand", "share", "frob")
	c.fails("usage: syncat share", "share")
	c.fails("nonesuch", "share", "rm", "nonesuch")

	if out := c.ok("share", "rm", "papers"); !strings.Contains(out, "removed share papers") {
		t.Errorf("share rm printed %q", out)
	}
	if out := c.ok("share", "ls"); !strings.Contains(out, "no shares configured") {
		t.Errorf("share ls after rm printed %q", out)
	}
}

func TestShareSafetyAndHolds(t *testing.T) {
	c := newCLINode(t)
	c.ok("share", "add", t.TempDir(), "--name", "docs", "--perm", "rw")

	out := c.ok("share", "safety", "docs")
	if !strings.Contains(out, ">= 50%") || !strings.Contains(out, ">= 20 of the files") {
		t.Errorf("share safety (defaults) printed %q", out)
	}
	c.ok("share", "safety", "docs", "--hold-percent", "30", "--hold-min", "5", "--trash-days", "3", "--trash-max", "1GiB")
	s := decodeJSON[wire.Safety](t, c.ok("share", "safety", "docs", "--json"))
	if s.HoldDeletePercent != 30 || s.HoldDeleteMin != 5 || s.TrashRetentionDays != 3 || s.TrashMaxSize != "1GiB" {
		t.Errorf("safety after set = %+v", s)
	}
	c.fails("usage: syncat share safety", "share", "safety")

	// Nothing is held, so there is nothing to confirm.
	if _, err := c.run("share", "confirm-deletes", "docs"); err == nil {
		t.Error("confirm-deletes on a share with no hold succeeded")
	}
	c.fails("usage: syncat share confirm-deletes", "share", "confirm-deletes")
}

// --- peers, approvals, remote shares ------------------------------------------

func TestPeerCommands(t *testing.T) {
	c := newCLINode(t)
	out := c.ok("peer", "add", fakeToken(t, "laptop"), "--name", "laptop")
	if !strings.Contains(out, "laptop") {
		t.Errorf("peer add printed %q", out)
	}
	peers := decodeJSON[wire.PeerList](t, c.ok("peer", "ls", "--json"))
	if len(peers.Peers) != 1 || peers.Peers[0].Name != "laptop" {
		t.Fatalf("peer ls --json = %+v", peers)
	}
	if out := c.ok("peer", "ls"); !strings.Contains(out, "laptop") || !strings.Contains(out, peers.Peers[0].ID) {
		t.Errorf("peer ls printed %q, want name and key", out)
	}

	c.fails("usage: syncat peer add", "peer", "add")
	if _, err := c.run("peer", "add", "garbage-token"); err == nil {
		t.Error("peer add with a garbage token succeeded")
	}
	own := strings.TrimSpace(c.ok("token"))
	if _, err := c.run("peer", "add", own); err == nil {
		t.Error("peer add with this node's own token succeeded")
	}
	c.fails("unknown peer subcommand", "peer", "frob")
	if _, err := c.run("peer", "approve", "peer-0123456789abcdef"); err == nil {
		t.Error("peer approve of a peer that is not pending succeeded")
	}

	if out := c.ok("approvals"); strings.TrimSpace(out) == "" {
		t.Error("approvals printed nothing on an empty queue, want a 'none pending' line")
	}
	c.fails("usage: syncat approvals", "approvals", "maybe", "x")
	if _, err := c.run("approvals", "grant", "share-nope"); err == nil {
		t.Error("approvals grant of an unknown id succeeded")
	}

	remotes := decodeJSON[[]wire.RemoteShare](t, c.ok("remote", "ls", "--json"))
	if len(remotes) != 0 {
		t.Errorf("remote ls --json with no connected peer = %+v", remotes)
	}
	c.ok("remote", "ls")

	if out := c.ok("peer", "rm", "laptop"); !strings.Contains(out, "laptop") {
		t.Errorf("peer rm printed %q", out)
	}
	if peers := decodeJSON[wire.PeerList](t, c.ok("peer", "ls", "--json")); len(peers.Peers) != 0 {
		t.Errorf("peer ls after rm = %+v", peers)
	}
}

// --- subscriptions ------------------------------------------------------------

func TestSubscriptionCommands(t *testing.T) {
	c := newCLINode(t)
	c.ok("peer", "add", fakeToken(t, "laptop"), "--name", "laptop")
	const shareID = "aa11bb22cc33dd44"
	local := filepath.Join(t.TempDir(), "sub")

	if out := c.ok("subscription", "ls"); !strings.Contains(out, "no subscriptions configured") {
		t.Errorf("subscription ls on an empty node printed %q", out)
	}
	out := c.ok("subscription", "add", "laptop", shareID, local, "--mode", "receive")
	if !strings.Contains(out, "subscribed to share "+shareID) || !strings.Contains(out, "receive") {
		t.Errorf("subscription add printed %q", out)
	}
	subs := decodeJSON[[]wire.Subscription](t, c.ok("subscription", "ls", "--json"))
	if len(subs) != 1 || subs[0].ShareID != shareID || subs[0].LocalPath != local || subs[0].Mode != config.ModeReceiveOnly {
		t.Fatalf("subscription ls --json = %+v", subs)
	}

	c.ok("subscription", "pause", "laptop", shareID)
	if subs := decodeJSON[[]wire.Subscription](t, c.ok("subscription", "ls", "--json")); !subs[0].Paused {
		t.Error("subscription pause did not pause it")
	}
	c.ok("subscription", "resume", "laptop", shareID[:6])
	if subs := decodeJSON[[]wire.Subscription](t, c.ok("subscription", "ls", "--json")); subs[0].Paused {
		t.Error("subscription resume did not resume it")
	}

	// MANUAL-TESTS §11: the local path given first, as if it were `share add`.
	c.fails("usage: syncat subscription add", "subscription", "add", local, "--mode", "mirror")
	if _, err := c.run("subscription", "add", "laptop", shareID, local, "--mode", "sideways"); err == nil {
		t.Error("subscription add with an invalid mode succeeded")
	}
	c.fails("usage: syncat subscription pause", "subscription", "pause", "laptop")
	c.fails("usage: syncat subscription rm", "subscription", "rm")
	c.fails("unknown subscription subcommand", "subscription", "frob")

	if out := c.ok("subscription", "rm", "laptop", shareID); !strings.Contains(out, "local files left in place") {
		t.Errorf("subscription rm printed %q", out)
	}
	if _, err := os.Stat(local); err != nil {
		t.Errorf("subscription rm removed the local folder: %v", err)
	}
	if subs := decodeJSON[[]wire.Subscription](t, c.ok("subscription", "ls", "--json")); len(subs) != 0 {
		t.Errorf("subscription ls after rm = %+v", subs)
	}
}

// --- trash and status ---------------------------------------------------------

func TestTrashAndStatusCommands(t *testing.T) {
	c := newCLINode(t)
	c.ok("share", "add", t.TempDir(), "--name", "docs", "--perm", "rw")

	if out := c.ok("trash", "ls", "docs"); !strings.Contains(out, "is empty") {
		t.Errorf("trash ls on an empty trash printed %q", out)
	}
	tr := decodeJSON[wire.Trash](t, c.ok("trash", "ls", "docs", "--json"))
	if len(tr.Events) != 0 || tr.RetentionDays == 0 {
		t.Errorf("trash ls --json = %+v", tr)
	}
	if _, err := c.run("trash", "undo", "docs", "1"); err == nil {
		t.Error("trash undo of an event that does not exist succeeded")
	}
	c.fails("usage: syncat trash ls", "trash", "ls")
	c.fails("usage: syncat trash undo", "trash", "undo", "docs")
	c.fails("unknown trash subcommand", "trash", "empty", "docs")

	st := decodeJSON[wire.Status](t, c.ok("status", "--json"))
	if st.NodeName != "cli-node" || len(st.Shares) != 1 {
		t.Errorf("status --json = node %q, %d shares", st.NodeName, len(st.Shares))
	}
	out := c.ok("status")
	for _, want := range []string{"cli-node", "docs"} {
		if !strings.Contains(out, want) {
			t.Errorf("status printed %q, want it to mention %q", out, want)
		}
	}
}

// With no daemon listening, every API command says so plainly.
func TestCommandsReportAnUnreachableDaemon(t *testing.T) {
	dir := t.TempDir()
	paths, err := config.ResolvePaths(filepath.Join(dir, "config"), filepath.Join(dir, "data"))
	if err != nil {
		t.Fatal(err)
	}
	if err := paths.EnsureDirs(); err != nil {
		t.Fatal(err)
	}
	cfg := config.Default()
	cfg.APIAddr = "127.0.0.1:1"
	if err := config.Save(paths.ConfigFile(), cfg); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(paths.APITokenFile(), []byte("t"), 0o600); err != nil {
		t.Fatal(err)
	}
	base := []string{"--config", paths.ConfigDir, "--data", paths.DataDir}
	for _, args := range [][]string{{"status"}, {"share", "ls"}, {"peer", "ls"}, {"subscription", "ls"}} {
		_, err := runCLI(t, append(base, args...)...)
		if err == nil || !strings.Contains(err.Error(), "is it running") {
			t.Errorf("syncat %s with no daemon: err = %v, want a hint to start it", strings.Join(args, " "), err)
		}
	}

	// And with no config at all, a hint to run init.
	empty := t.TempDir()
	_, err = runCLI(t, "--config", filepath.Join(empty, "c"), "--data", filepath.Join(empty, "d"), "status")
	if err == nil || !strings.Contains(err.Error(), "syncat init") {
		t.Errorf("status with no config: err = %v, want a hint to run init", err)
	}
}

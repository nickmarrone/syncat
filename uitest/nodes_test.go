package uitest

// The daemon side of the UI tests. scripts/run-ui-tests.sh uses
// scripts/lib/harness.sh to build the binary, create three nodes, start
// alice and bob, and pair them over real tailcat, then hands this package
// the harness's work directory. Everything here works on that layout
// ($WORK/<node>/{config,data,daemon.log}, $WORK/<node>/dir-<label>) so that
// the harness's own failure dump (daemon log tails, status, and every dir-*
// directory) covers what these tests did too.
//
// alice and bob belong to the harness, which started them and will stop
// them. carol is created by the harness but never started by it: the tests
// that need a daemon to stop and restart underneath the UI start carol
// themselves, so stopping it cannot break the pair the other tests share.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Timeouts for conditions on the daemon side. They mirror the harness's
// constants of the same purpose (scripts/lib/harness.sh), for the same
// reasons: a file crossing a real relay can take tens of seconds.
const (
	apiTimeout       = 30 * time.Second
	syncTimeout      = 90 * time.Second
	discoveryTimeout = 60 * time.Second
)

type env struct {
	work      string // the harness's work dir for this run
	bin       string // the syncat binary the harness built
	artifacts string // where screenshots and browser logs go
	chrome    string // the Chrome or Chromium binary, "" if none was found
}

var testEnv *env

// skipReason is why every test skips, or "" when they can run.
var skipReason string

func loadEnv() {
	work := os.Getenv("UITEST_WORK")
	bin := os.Getenv("UITEST_BIN")
	if work == "" || bin == "" {
		skipReason = "UITEST_WORK and UITEST_BIN are not set; run the UI tests with scripts/run-ui-tests.sh, which starts the daemons they drive"
		return
	}
	chrome := findChrome()
	if chrome == "" {
		skipReason = "no Chrome or Chromium found (looked at $UITEST_CHROME, google-chrome, google-chrome-stable, chromium, chromium-browser, /snap/bin/chromium); install one or set UITEST_CHROME"
		return
	}
	artifacts := os.Getenv("UITEST_ARTIFACTS")
	if artifacts == "" {
		artifacts = filepath.Join(work, "artifacts")
	}
	if err := os.MkdirAll(artifacts, 0o755); err != nil {
		skipReason = "cannot create the artifacts dir: " + err.Error()
		return
	}
	testEnv = &env{work: work, bin: bin, artifacts: artifacts, chrome: chrome}
}

// findChrome prefers an explicit UITEST_CHROME, then a Chrome on PATH, then
// Chromium. google-chrome comes first because the snap Chromium cannot read
// a user-data dir under /tmp, which is where chromedp puts one.
func findChrome() string {
	if p := os.Getenv("UITEST_CHROME"); p != "" {
		if _, err := os.Stat(p); err == nil {
			return p
		}
		return ""
	}
	for _, name := range []string{"google-chrome", "google-chrome-stable", "chromium", "chromium-browser"} {
		if p, err := exec.LookPath(name); err == nil {
			return p
		}
	}
	if _, err := os.Stat("/snap/bin/chromium"); err == nil {
		return "/snap/bin/chromium"
	}
	return ""
}

func TestMain(m *testing.M) {
	loadEnv()
	if skipReason != "" {
		fmt.Fprintln(os.Stderr, "uitest: SKIP:", skipReason)
	}
	os.Exit(m.Run())
}

// requireEnv skips the test when the tier cannot run here.
func requireEnv(t *testing.T) *env {
	t.Helper()
	if skipReason != "" {
		t.Skip(skipReason)
	}
	return testEnv
}

// --- nodes -------------------------------------------------------------------

type node struct {
	name string
	e    *env
	proc *exec.Cmd // set only for a daemon this package started (carol)
	done chan struct{}
}

func (e *env) node(name string) *node { return &node{name: name, e: e} }

func (n *node) root() string      { return filepath.Join(n.e.work, n.name) }
func (n *node) configDir() string { return filepath.Join(n.root(), "config") }
func (n *node) dataDir() string   { return filepath.Join(n.root(), "data") }
func (n *node) logPath() string   { return filepath.Join(n.root(), "daemon.log") }

// config reads the node's config.json as generic JSON, which is what the
// "same result as the CLI" tests compare.
func (n *node) config(t *testing.T) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(n.configDir(), "config.json"))
	if err != nil {
		t.Fatalf("read %s's config.json: %v", n.name, err)
	}
	var cfg map[string]any
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatalf("parse %s's config.json: %v", n.name, err)
	}
	return cfg
}

func (n *node) apiAddr(t *testing.T) string {
	t.Helper()
	addr, _ := n.config(t)["api_addr"].(string)
	if addr == "" {
		t.Fatalf("%s's config.json has no api_addr", n.name)
	}
	return addr
}

// url is the UI's address. It is exactly the configured api_addr, which is
// what /ui-token's Host check compares against, so the UI gets its token
// the way it does for a real user.
func (n *node) url(t *testing.T) string { return "http://" + n.apiAddr(t) + "/" }

// dir creates and returns $WORK/<node>/dir-<label>, the naming the harness's
// failure dump looks for.
func (n *node) dir(t *testing.T, label string) string {
	t.Helper()
	d := filepath.Join(n.root(), "dir-"+label)
	if err := os.MkdirAll(d, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", d, err)
	}
	return d
}

// run runs the CLI against this node and returns its stdout. It is the Go
// counterpart of the harness's nx.
func (n *node) run(args ...string) ([]byte, error) {
	full := append([]string{"--config", n.configDir(), "--data", n.dataDir()}, args...)
	cmd := exec.Command(n.e.bin, full...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return out, fmt.Errorf("syncat %s: %v: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return out, nil
}

func (n *node) cli(t *testing.T, args ...string) string {
	t.Helper()
	out, err := n.run(args...)
	if err != nil {
		t.Fatalf("%s: %v", n.name, err)
	}
	return strings.TrimSpace(string(out))
}

func (n *node) cliJSON(t *testing.T, v any, args ...string) {
	t.Helper()
	out := n.cli(t, append(args, "--json")...)
	if err := json.Unmarshal([]byte(out), v); err != nil {
		t.Fatalf("%s: parse `%s --json`: %v\n%s", n.name, strings.Join(args, " "), err, out)
	}
}

// api calls the node's REST API with the token from api.token, for setup
// and cleanup that has no CLI command (and to restore state a test changed).
func (n *node) api(t *testing.T, method, path string, body any) {
	t.Helper()
	tok, err := os.ReadFile(filepath.Join(n.configDir(), "api.token"))
	if err != nil {
		t.Fatalf("read %s's api.token: %v", n.name, err)
	}
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, "http://"+n.apiAddr(t)+path, rd)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Syncat-Token", strings.TrimSpace(string(tok)))
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s on %s: %v", method, path, n.name, err)
	}
	defer res.Body.Close()
	if res.StatusCode >= 300 {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("%s %s on %s: %s: %s", method, path, n.name, res.Status, b)
	}
}

// status is `syncat status --json`, decoded loosely.
func (n *node) status(t *testing.T) map[string]any {
	t.Helper()
	var st map[string]any
	n.cliJSON(t, &st, "status")
	return st
}

// --- the daemons this package owns ------------------------------------------

// start launches the daemon the way the harness's node_start does: the same
// arguments, its output appended to the same daemon.log with the same
// restart marker, and the work dir as its working directory.
func (n *node) start(t *testing.T) {
	t.Helper()
	if n.proc != nil {
		t.Fatalf("%s is already running", n.name)
	}
	logf, err := os.OpenFile(n.logPath(), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("open %s's log: %v", n.name, err)
	}
	fmt.Fprintf(logf, "\n===== %s: daemon starting (%s, from %s) =====\n", n.name, time.Now().UTC().Format("15:04:05"), t.Name())
	cmd := exec.Command(n.e.bin, "--config", n.configDir(), "--data", n.dataDir(), "daemon")
	cmd.Stdout, cmd.Stderr, cmd.Dir = logf, logf, n.e.work
	if err := cmd.Start(); err != nil {
		logf.Close()
		t.Fatalf("start %s: %v", n.name, err)
	}
	n.proc, n.done = cmd, make(chan struct{})
	go func(done chan struct{}) {
		cmd.Wait()
		logf.Close()
		close(done)
	}(n.done)
	t.Cleanup(func() { n.stop(t) })
	eventually(t, apiTimeout, n.name+"'s API to accept requests", func() bool {
		_, err := n.run("status")
		return err == nil
	})
}

// stop is SIGTERM and a wait, like node_stop. It is a no-op for a node this
// package did not start, and safe to call twice.
func (n *node) stop(t *testing.T) {
	t.Helper()
	if n.proc == nil {
		return
	}
	n.proc.Process.Signal(syscall.SIGTERM)
	select {
	case <-n.done:
	case <-time.After(30 * time.Second):
		n.proc.Process.Kill()
		<-n.done
		t.Errorf("%s ignored SIGTERM for 30s and was killed", n.name)
	}
	n.proc, n.done = nil, nil
}

// --- waiting -------------------------------------------------------------------

// eventually polls cond until it holds or the timeout passes, the Go
// counterpart of the harness's wait_for. It logs how long the wait took, so
// a passing run still shows where its time went.
func eventually(t *testing.T, timeout time.Duration, desc string, cond func() bool) {
	t.Helper()
	start := time.Now()
	for !cond() {
		if time.Since(start) > timeout {
			t.Fatalf("timed out after %s waiting for: %s", timeout, desc)
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Logf("ok: %s (%.1fs)", desc, time.Since(start).Seconds())
}

func fileContentIs(path, want string) func() bool {
	return func() bool {
		b, err := os.ReadFile(path)
		return err == nil && string(b) == want
	}
}

func pathAbsent(path string) func() bool {
	return func() bool {
		_, err := os.Stat(path)
		return os.IsNotExist(err)
	}
}

// --- lookups over the CLI's JSON ------------------------------------------------

func (n *node) peerKey(t *testing.T, peer string) string {
	t.Helper()
	var out struct {
		Peers []struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		} `json:"peers"`
	}
	n.cliJSON(t, &out, "peer", "ls")
	for _, p := range out.Peers {
		if p.Name == peer {
			return p.ID
		}
	}
	t.Fatalf("%s has no peer named %q", n.name, peer)
	return ""
}

type shareJSON struct {
	ID               string `json:"id"`
	Name             string `json:"name"`
	Path             string `json:"path"`
	Permission       string `json:"permission"`
	ApprovalRequired bool   `json:"approval_required"`
}

func (n *node) shares(t *testing.T) []shareJSON {
	t.Helper()
	var out []shareJSON
	n.cliJSON(t, &out, "share", "ls")
	return out
}

func (n *node) shareID(t *testing.T, name string) string {
	t.Helper()
	for _, s := range n.shares(t) {
		if s.Name == name {
			return s.ID
		}
	}
	t.Fatalf("%s has no share named %q", n.name, name)
	return ""
}

func (n *node) seesRemoteShare(shareID string) func() bool {
	return func() bool {
		out, err := n.run("remote", "ls", "--json")
		if err != nil {
			return false
		}
		var rs []struct {
			ShareID string `json:"share_id"`
		}
		if json.Unmarshal(out, &rs) != nil {
			return false
		}
		for _, r := range rs {
			if r.ShareID == shareID {
				return true
			}
		}
		return false
	}
}

func (n *node) subscriptionCount(t *testing.T) int {
	t.Helper()
	var subs []any
	n.cliJSON(t, &subs, "subscription", "ls")
	return len(subs)
}

// addShare offers dir as a share named name and returns its id, removing it
// again when the test ends so each test leaves the pair as it found it.
func (n *node) addShare(t *testing.T, dir, name string, extra ...string) string {
	t.Helper()
	n.cli(t, append([]string{"share", "add", dir, "--name", name}, extra...)...)
	id := n.shareID(t, name)
	t.Cleanup(func() { n.run("share", "rm", id) })
	return id
}

// subscribe subscribes n to from's share and removes the subscription at the
// end of the test.
func (n *node) subscribe(t *testing.T, from *node, shareID, local string, mode string) {
	t.Helper()
	eventually(t, discoveryTimeout, n.name+" to see "+from.name+"'s share "+shareID, n.seesRemoteShare(shareID))
	n.cli(t, "subscription", "add", from.name, shareID, local, "--mode", mode)
	t.Cleanup(func() { n.run("subscription", "rm", from.name, shareID) })
}

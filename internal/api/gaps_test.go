package api

import (
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nickmarrone/syncat/internal/api/wire"
	"github.com/nickmarrone/syncat/internal/config"
	"github.com/nickmarrone/syncat/internal/core"
)

// --- PATCH /api/node ----------------------------------------------------------

func TestNodePatchRenamesTheNode(t *testing.T) {
	srv, n, paths := newTestServer(t)
	oldToken := n.Status().NodeToken

	w := doRequest(t, srv, http.MethodPatch, "/api/node", testToken, map[string]string{"name": "renamed"})
	if w.Code != http.StatusOK {
		t.Fatalf("PATCH /api/node: %d %s", w.Code, w.Body.String())
	}
	var got map[string]string
	decodeJSONBody(t, w, &got)
	if got["node_name"] != "renamed" || got["node_token"] == "" || got["node_token"] == oldToken {
		t.Errorf("PATCH /api/node = %v; want the new name and a token carrying it", got)
	}
	if n.Status().NodeName != "renamed" {
		t.Errorf("node name after PATCH = %q", n.Status().NodeName)
	}
	cfg, err := config.Load(paths.ConfigFile())
	if err != nil {
		t.Fatal(err)
	}
	if cfg.NodeName != "renamed" {
		t.Errorf("config.json node_name after PATCH = %q, want it persisted", cfg.NodeName)
	}
}

func TestNodePatchRejectsBadInput(t *testing.T) {
	srv, n, _ := newTestServer(t)
	for _, tc := range []struct {
		name string
		body any
	}{
		{"empty name", map[string]string{"name": ""}},
		{"wrong type", map[string]int{"name": 5}},
	} {
		if w := doRequest(t, srv, http.MethodPatch, "/api/node", testToken, tc.body); w.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400; body %s", tc.name, w.Code, w.Body.String())
		}
	}
	if n.Status().NodeName != "test-node" {
		t.Errorf("a rejected PATCH renamed the node to %q", n.Status().NodeName)
	}
	// No fields at all is a no-op that still reports the node.
	w := doRequest(t, srv, http.MethodPatch, "/api/node", testToken, map[string]string{})
	if w.Code != http.StatusOK {
		t.Errorf("empty PATCH: status = %d, want 200", w.Code)
	}
	if w := doRequest(t, srv, http.MethodPatch, "/api/node", "", map[string]string{"name": "x"}); w.Code != http.StatusUnauthorized {
		t.Errorf("PATCH without a token: status = %d, want 401", w.Code)
	}
}

// --- GET /api/holds ------------------------------------------------------------

func TestHoldsListEmptyAndHeld(t *testing.T) {
	srv, n, _ := newTestServer(t)
	w := doRequest(t, srv, http.MethodGet, "/api/holds", testToken, nil)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"holds":[]`) {
		t.Fatalf("GET /api/holds on a node with no holds = %d %s; want 200 and an empty array, not null", w.Code, w.Body.String())
	}

	// A share whose folder vanishes while its files are indexed is held
	// (SPEC.md §7a, missing root).
	root := filepath.Join(t.TempDir(), "docs")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"a.txt", "b.txt"} {
		if err := os.WriteFile(filepath.Join(root, f), []byte(f), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	shareID, err := n.AddShare(root, "docs", config.PermissionReadWrite, false)
	if err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 5*time.Second, func() bool {
		st := n.Status()
		return len(st.Shares) == 1
	})
	time.Sleep(200 * time.Millisecond) // let the initial scan index both files
	if err := os.RemoveAll(root); err != nil {
		t.Fatal(err)
	}

	var holds wire.HoldList
	waitUntil(t, 10*time.Second, func() bool {
		w := doRequest(t, srv, http.MethodGet, "/api/holds", testToken, nil)
		if w.Code != http.StatusOK {
			return false
		}
		holds = wire.HoldList{}
		decodeJSONBody(t, w, &holds)
		return len(holds.Holds) > 0
	})
	h := holds.Holds[0]
	if h.ShareID != shareID || h.ShareName != "docs" || h.Problem == "" || h.Remedy == "" || h.Confirmable {
		t.Errorf("hold = %+v; want the docs share, with problem and remedy text, not confirmable (missing root)", h)
	}
	if w := doRequest(t, srv, http.MethodGet, "/api/holds", "", nil); w.Code != http.StatusUnauthorized {
		t.Errorf("GET /api/holds without a token: status = %d, want 401", w.Code)
	}
}

func waitUntil(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("condition not met within %s", timeout)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// --- DTO conversions -------------------------------------------------------------

// Every field of the core status types must reach the wire type. A dropped
// field is silent: the JSON just lacks a key the UI or CLI expected.
func TestStatusDTOsCarryEveryField(t *testing.T) {
	at := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)

	rs := toRemoteShareDTO(core.RemoteShareStatus{
		PeerKey: "pk", PeerName: "laptop", ShareID: "sid", Name: "docs",
		Permission: config.PermissionReadOnly, ApprovalRequired: true, Access: "granted",
	})
	if want := (wire.RemoteShare{PeerKey: "pk", PeerName: "laptop", ShareID: "sid", Name: "docs",
		Permission: config.PermissionReadOnly, ApprovalRequired: true, Access: "granted"}); rs != want {
		t.Errorf("toRemoteShareDTO = %+v, want %+v", rs, want)
	}

	wd := toWarningDTO(core.WarningStatus{ShareID: "sid", RelPath: "a.txt", At: at, Reverted: true, Reason: "locally modified"})
	if want := (wire.Warning{ShareID: "sid", RelPath: "a.txt", At: at, Reverted: true, Reason: "locally modified"}); wd != want {
		t.Errorf("toWarningDTO = %+v, want %+v", wd, want)
	}

	rc := toRejectedConnDTO(core.RejectedConnection{PeerKey: "pk", PeerName: "stranger", At: at, Reason: "unknown peer"})
	if want := (wire.RejectedConnection{PeerKey: "pk", PeerName: "stranger", At: at, Reason: "unknown peer"}); rc != want {
		t.Errorf("toRejectedConnDTO = %+v, want %+v", rc, want)
	}

	// The struct-literal comparisons above only prove the fields named
	// there. Guard against a field added to the core type later and never
	// mapped: both sides must have the same number of fields.
	for _, pair := range [][2]any{
		{core.RemoteShareStatus{}, wire.RemoteShare{}},
		{core.WarningStatus{}, wire.Warning{}},
		{core.RejectedConnection{}, wire.RejectedConnection{}},
	} {
		if a, b := reflect.TypeOf(pair[0]).NumField(), reflect.TypeOf(pair[1]).NumField(); a != b {
			t.Errorf("%T has %d fields but %T has %d: map the new one", pair[0], a, pair[1], b)
		}
	}
}

// --- the listener and middleware ---------------------------------------------------

func TestListenLoopback(t *testing.T) {
	for _, addr := range []string{"127.0.0.1:0", "localhost:0", ":0"} {
		ln, err := ListenLoopback(addr)
		if err != nil {
			t.Errorf("ListenLoopback(%q): %v", addr, err)
			continue
		}
		host, _, _ := net.SplitHostPort(ln.Addr().String())
		if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
			t.Errorf("ListenLoopback(%q) bound %s, not loopback", addr, ln.Addr())
		}
		ln.Close()
	}
	for _, addr := range []string{"0.0.0.0:0", "[::]:0", "192.0.2.1:0", "example.com:0", "no-port"} {
		if ln, err := ListenLoopback(addr); err == nil {
			ln.Close()
			t.Errorf("ListenLoopback(%q) bound %s, want a refusal", addr, ln.Addr())
		}
	}
}

// The logging middleware wraps the ResponseWriter; a handler behind it
// must still be able to flush (the web UI relies on prompt responses).
func TestMiddlewareKeepsFlushReachable(t *testing.T) {
	srv, _, _ := newTestServer(t)
	h := srv.logMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("x"))
		if err := http.NewResponseController(w).Flush(); err != nil {
			t.Errorf("Flush through the log middleware: %v", err)
		}
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/status", nil))
	if !rec.Flushed {
		t.Error("the underlying ResponseWriter was never flushed")
	}
}

package api

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nickmarrone/syncat/internal/config"
	"github.com/nickmarrone/syncat/internal/core"
	syncsvc "github.com/nickmarrone/syncat/internal/sync"
	"github.com/nickmarrone/syncat/internal/transport"
	"github.com/nickmarrone/syncat/internal/version"
)

const testToken = "test-token-0123456789abcdef"
const testSelfAddr = "127.0.0.1:8347"

var testCounter int

// newTestServer builds a Server over a freshly opened core.Node (PipeTransport,
// temp config/data dirs — SPEC.md §10's loopback transport stand-in),
// closed automatically via t.Cleanup.
func newTestServer(t *testing.T) (*Server, *core.Node, *config.Paths) {
	t.Helper()
	testCounter++
	dir := t.TempDir()
	paths, err := config.ResolvePaths(filepath.Join(dir, "config"), filepath.Join(dir, "data"))
	if err != nil {
		t.Fatalf("resolve paths: %v", err)
	}
	if err := paths.EnsureDirs(); err != nil {
		t.Fatalf("ensure dirs: %v", err)
	}
	identity, _, err := config.LoadOrCreateIdentityKey(paths.IdentityKeyFile())
	if err != nil {
		t.Fatalf("load identity: %v", err)
	}
	cfg := config.Default()
	cfg.NodeName = "test-node"

	tr := transport.NewPipeTransport(fmt.Sprintf("pipe-%s-%d", t.Name(), testCounter))
	n, err := core.Open(context.Background(), core.Options{
		Paths: paths, Config: cfg, Identity: identity, Transport: tr,
		Logger: log.New(io.Discard, "", 0), DisableJitter: true,
	})
	if err != nil {
		t.Fatalf("open node: %v", err)
	}
	t.Cleanup(func() { n.Close() })

	srv := NewServer(n, testToken, testSelfAddr, log.New(io.Discard, "", 0))
	return srv, n, paths
}

// doRequest issues a request straight into srv.Handler() (no real
// listener needed for anything except the loopback/Host/Origin checks,
// which set RemoteAddr/Host/Origin explicitly instead).
func doRequest(t *testing.T, srv *Server, method, path, token string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		reader = bytes.NewReader(data)
	}
	req := httptest.NewRequest(method, path, reader)
	req.RemoteAddr = "127.0.0.1:54321"
	req.Host = testSelfAddr
	if token != "" {
		req.Header.Set("X-Syncat-Token", token)
	}
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	return w
}

// fakePeerToken builds a syntactically valid sc1 token (SPEC.md §2) for a
// peer key that isn't backed by any running node — it's not reachable at
// its "tc" tailcat address. That's deliberate: tests exercising peer CRUD
// via the API only care that AddPeer accepts/parses a token and persists a
// peer, not that a connection can actually be established, so this avoids
// spinning up a second live node (and the dial/handshake/backoff
// goroutines that would come with it — unrelated background activity a
// peer-CRUD test has no reason to trigger).
func fakePeerToken(t *testing.T, name string) string {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate peer key: %v", err)
	}
	tok, err := config.EncodeToken("pipe-unreachable-"+name, pub, name)
	if err != nil {
		t.Fatalf("encode peer token: %v", err)
	}
	return tok
}

func decodeJSONBody(t *testing.T, w *httptest.ResponseRecorder, v any) {
	t.Helper()
	if err := json.Unmarshal(w.Body.Bytes(), v); err != nil {
		t.Fatalf("decode body %q: %v", w.Body.String(), err)
	}
}

// --- auth --------------------------------------------------------------

func TestAuthNoToken(t *testing.T) {
	srv, _, _ := newTestServer(t)
	w := doRequest(t, srv, http.MethodGet, "/api/status", "", nil)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("no token: status = %d, want 401; body: %s", w.Code, w.Body.String())
	}
	var eb errorBody
	decodeJSONBody(t, w, &eb)
	if eb.Error.Code != "unauthorized" {
		t.Errorf("error code = %q, want unauthorized", eb.Error.Code)
	}
}

func TestAuthWrongToken(t *testing.T) {
	srv, _, _ := newTestServer(t)
	w := doRequest(t, srv, http.MethodGet, "/api/status", "not-the-token", nil)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("wrong token: status = %d, want 401; body: %s", w.Code, w.Body.String())
	}
}

func TestAuthRightToken(t *testing.T) {
	srv, _, _ := newTestServer(t)
	w := doRequest(t, srv, http.MethodGet, "/api/status", testToken, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("right token: status = %d, want 200; body: %s", w.Code, w.Body.String())
	}
}

// --- /ui-token -----------------------------------------------------------

func TestUITokenRejectsNonLoopbackRemoteAddr(t *testing.T) {
	srv, _, _ := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/ui-token", nil)
	req.RemoteAddr = "203.0.113.42:12345" // a public, non-loopback address (TEST-NET-3, RFC 5737)
	req.Host = testSelfAddr
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusForbidden {
		t.Fatalf("non-loopback remote addr: status = %d, want 403; body: %s", w.Code, w.Body.String())
	}
}

func TestUITokenRejectsSpoofedForwardedFor(t *testing.T) {
	// X-Forwarded-For must never be trusted: RemoteAddr (from the real
	// TCP connection) is what's checked, no matter what a client claims.
	srv, _, _ := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/ui-token", nil)
	req.RemoteAddr = "203.0.113.42:12345"
	req.Host = testSelfAddr
	req.Header.Set("X-Forwarded-For", "127.0.0.1")
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusForbidden {
		t.Fatalf("spoofed X-Forwarded-For: status = %d, want 403; body: %s", w.Code, w.Body.String())
	}
}

func TestUITokenRejectsWrongHost(t *testing.T) {
	// Simulates DNS rebinding: RemoteAddr genuinely is loopback (the
	// rebound connection really does land on 127.0.0.1), but the browser
	// still sends the original Host from the URL bar.
	srv, _, _ := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/ui-token", nil)
	req.RemoteAddr = "127.0.0.1:54321"
	req.Host = "evil.example.com"
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusForbidden {
		t.Fatalf("wrong Host header: status = %d, want 403; body: %s", w.Code, w.Body.String())
	}
}

func TestUITokenRejectsCrossOrigin(t *testing.T) {
	srv, _, _ := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/ui-token", nil)
	req.RemoteAddr = "127.0.0.1:54321"
	req.Host = testSelfAddr
	req.Header.Set("Origin", "http://evil.example.com")
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusForbidden {
		t.Fatalf("cross-origin request: status = %d, want 403; body: %s", w.Code, w.Body.String())
	}
}

func TestUITokenAcceptsLoopbackSameOrigin(t *testing.T) {
	srv, _, _ := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/ui-token", nil)
	req.RemoteAddr = "127.0.0.1:54321"
	req.Host = testSelfAddr
	req.Header.Set("Origin", "http://"+testSelfAddr)
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("legit same-origin request: status = %d, want 200; body: %s", w.Code, w.Body.String())
	}
	var body map[string]string
	decodeJSONBody(t, w, &body)
	if body["token"] != testToken {
		t.Errorf("token = %q, want %q", body["token"], testToken)
	}
}

func TestUITokenAcceptsAbsentOrigin(t *testing.T) {
	// A same-origin request that omits Origin (common for plain GETs)
	// must still succeed — Host is the load-bearing check.
	srv, _, _ := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/ui-token", nil)
	req.RemoteAddr = "[::1]:54321"
	req.Host = testSelfAddr
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("absent Origin (IPv6 loopback): status = %d, want 200; body: %s", w.Code, w.Body.String())
	}
}

func TestUITokenRequiresNoAuthToken(t *testing.T) {
	// /ui-token is deliberately login-less: no X-Syncat-Token needed.
	srv, _, _ := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/ui-token", nil)
	req.RemoteAddr = "127.0.0.1:54321"
	req.Host = testSelfAddr
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", w.Code, w.Body.String())
	}
}

// --- status --------------------------------------------------------------

func TestStatusMarshals(t *testing.T) {
	srv, _, _ := newTestServer(t)
	w := doRequest(t, srv, http.MethodGet, "/api/status", testToken, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", w.Code, w.Body.String())
	}
	var body statusResponse
	decodeJSONBody(t, w, &body)
	if body.NodeName != "test-node" {
		t.Errorf("node_name = %q, want test-node", body.NodeName)
	}
	if body.NodeToken == "" {
		t.Error("node_token is empty")
	}
	if body.ShortID == "" {
		t.Error("short_id is empty")
	}
	// The UI reads this to label the header, and a bug report reads it to
	// say which build produced the behaviour, so it must never be blank.
	if body.Version != version.String() {
		t.Errorf("version = %q, want %q", body.Version, version.String())
	}
	if body.Peers == nil || body.Shares == nil || body.Subscriptions == nil {
		t.Error("Peers/Shares/Subscriptions should be present (possibly empty) arrays, not omitted")
	}
}

// --- peers -----------------------------------------------------------------

func TestPeersAddListDelete(t *testing.T) {
	srv, _, _ := newTestServer(t)
	otherToken := fakePeerToken(t, "bob")

	w := doRequest(t, srv, http.MethodPost, "/api/peers", testToken, map[string]string{"token": otherToken, "name": "bob"})
	if w.Code != http.StatusCreated {
		t.Fatalf("add peer: status = %d, want 201; body: %s", w.Code, w.Body.String())
	}
	var peer peerDTO
	decodeJSONBody(t, w, &peer)
	if peer.Name != "bob" || peer.ID == "" {
		t.Fatalf("unexpected peer body: %+v", peer)
	}

	w = doRequest(t, srv, http.MethodGet, "/api/peers", testToken, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("list peers: status = %d, want 200", w.Code)
	}
	var listResp struct {
		Peers []peerDTO `json:"peers"`
	}
	decodeJSONBody(t, w, &listResp)
	if len(listResp.Peers) != 1 || listResp.Peers[0].ID != peer.ID {
		t.Fatalf("list peers = %+v, want one entry matching %q", listResp.Peers, peer.ID)
	}

	w = doRequest(t, srv, http.MethodDelete, "/api/peers/"+peer.ID, testToken, nil)
	if w.Code != http.StatusNoContent {
		t.Fatalf("delete peer: status = %d, want 204; body: %s", w.Code, w.Body.String())
	}

	w = doRequest(t, srv, http.MethodDelete, "/api/peers/"+peer.ID, testToken, nil)
	if w.Code != http.StatusNotFound {
		t.Fatalf("delete unknown peer: status = %d, want 404; body: %s", w.Code, w.Body.String())
	}
}

func TestPeersAddBadToken(t *testing.T) {
	srv, _, _ := newTestServer(t)
	w := doRequest(t, srv, http.MethodPost, "/api/peers", testToken, map[string]string{"token": "not-a-valid-token"})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body: %s", w.Code, w.Body.String())
	}
}

func TestPeersAddMissingToken(t *testing.T) {
	srv, _, _ := newTestServer(t)
	w := doRequest(t, srv, http.MethodPost, "/api/peers", testToken, map[string]string{"name": "bob"})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body: %s", w.Code, w.Body.String())
	}
}

func TestPeersApproveNotImplemented(t *testing.T) {
	srv, _, _ := newTestServer(t)
	w := doRequest(t, srv, http.MethodPost, "/api/peers/deadbeef/approve", testToken, nil)
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501; body: %s", w.Code, w.Body.String())
	}
}

func TestApprovalsNotImplemented(t *testing.T) {
	srv, _, _ := newTestServer(t)
	w := doRequest(t, srv, http.MethodGet, "/api/approvals", testToken, nil)
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501; body: %s", w.Code, w.Body.String())
	}
}

// --- shares: full round trip (add, list, patch, delete) --------------------

func TestSharesRoundTrip(t *testing.T) {
	srv, _, _ := newTestServer(t)
	shareDir := t.TempDir()

	// add
	w := doRequest(t, srv, http.MethodPost, "/api/shares", testToken, map[string]any{
		"path": shareDir, "name": "docs", "permission": "ro", "approval_required": false,
	})
	if w.Code != http.StatusCreated {
		t.Fatalf("add share: status = %d, want 201; body: %s", w.Code, w.Body.String())
	}
	var share shareDTO
	decodeJSONBody(t, w, &share)
	if share.Name != "docs" || share.Path != shareDir || share.Permission != "read-only" {
		t.Fatalf("unexpected share body: %+v", share)
	}

	// list
	w = doRequest(t, srv, http.MethodGet, "/api/shares", testToken, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("list shares: status = %d, want 200", w.Code)
	}
	var listResp struct {
		Shares []shareDTO `json:"shares"`
	}
	decodeJSONBody(t, w, &listResp)
	if len(listResp.Shares) != 1 || listResp.Shares[0].ID != share.ID {
		t.Fatalf("list shares = %+v, want one entry matching %q", listResp.Shares, share.ID)
	}

	// patch
	w = doRequest(t, srv, http.MethodPatch, "/api/shares/"+share.ID, testToken, map[string]any{
		"name": "documents", "permission": "rw", "approval_required": true,
	})
	if w.Code != http.StatusOK {
		t.Fatalf("patch share: status = %d, want 200; body: %s", w.Code, w.Body.String())
	}
	var patched shareDTO
	decodeJSONBody(t, w, &patched)
	if patched.Name != "documents" || patched.Permission != "read-write" || !patched.ApprovalRequired {
		t.Fatalf("unexpected patched share: %+v", patched)
	}

	// delete
	w = doRequest(t, srv, http.MethodDelete, "/api/shares/"+share.ID, testToken, nil)
	if w.Code != http.StatusNoContent {
		t.Fatalf("delete share: status = %d, want 204; body: %s", w.Code, w.Body.String())
	}
	w = doRequest(t, srv, http.MethodGet, "/api/shares", testToken, nil)
	decodeJSONBody(t, w, &listResp)
	if len(listResp.Shares) != 0 {
		t.Fatalf("shares after delete = %+v, want none", listResp.Shares)
	}
}

func TestSharesAddMissingPath(t *testing.T) {
	srv, _, _ := newTestServer(t)
	w := doRequest(t, srv, http.MethodPost, "/api/shares", testToken, map[string]any{"name": "x"})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body: %s", w.Code, w.Body.String())
	}
}

func TestSharesAddMalformedJSON(t *testing.T) {
	srv, _, _ := newTestServer(t)
	req := httptest.NewRequest(http.MethodPost, "/api/shares", bytes.NewReader([]byte("{not json")))
	req.Header.Set("X-Syncat-Token", testToken)
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body: %s", w.Code, w.Body.String())
	}
}

func TestSharesPatchUnknownID(t *testing.T) {
	srv, _, _ := newTestServer(t)
	w := doRequest(t, srv, http.MethodPatch, "/api/shares/doesnotexist", testToken, map[string]any{"name": "x"})
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body: %s", w.Code, w.Body.String())
	}
}

func TestSharesDeleteUnknownID(t *testing.T) {
	srv, _, _ := newTestServer(t)
	w := doRequest(t, srv, http.MethodDelete, "/api/shares/doesnotexist", testToken, nil)
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body: %s", w.Code, w.Body.String())
	}
}

// --- remote-shares -----------------------------------------------------------

func TestRemoteSharesEmpty(t *testing.T) {
	srv, _, _ := newTestServer(t)
	w := doRequest(t, srv, http.MethodGet, "/api/remote-shares", testToken, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", w.Code, w.Body.String())
	}
	var resp struct {
		RemoteShares []remoteShareDTO `json:"remote_shares"`
	}
	decodeJSONBody(t, w, &resp)
	if len(resp.RemoteShares) != 0 {
		t.Fatalf("remote_shares = %+v, want none", resp.RemoteShares)
	}
}

// --- subscriptions -----------------------------------------------------------

func TestSubscriptionsAddPatchDelete(t *testing.T) {
	srv, _, _ := newTestServer(t)
	localDir := filepath.Join(t.TempDir(), "sub")

	// A real configured peer: core.AddSubscription rejects a peer it
	// doesn't have (see TestSubscriptionsAddRejectsUnknownPeer). It never
	// connects over the pipe transport, so it never sends a ShareList and
	// the share id below isn't checked against anything.
	w := doRequest(t, srv, http.MethodPost, "/api/peers", testToken, map[string]string{"token": fakePeerToken(t, "bob"), "name": "bob"})
	if w.Code != http.StatusCreated {
		t.Fatalf("add peer: status = %d, want 201; body: %s", w.Code, w.Body.String())
	}
	var peer peerDTO
	decodeJSONBody(t, w, &peer)

	w = doRequest(t, srv, http.MethodPost, "/api/subscriptions", testToken, map[string]any{
		"peer": peer.ID, "share_id": "bb22", "local_path": localDir, "mode": "mirror",
	})
	if w.Code != http.StatusCreated {
		t.Fatalf("add subscription: status = %d, want 201; body: %s", w.Code, w.Body.String())
	}
	var sub subscriptionDTO
	decodeJSONBody(t, w, &sub)
	if sub.ID != peer.ID+":bb22" || sub.Mode != "mirror" {
		t.Fatalf("unexpected subscription body: %+v", sub)
	}

	// patch: pause
	w = doRequest(t, srv, http.MethodPatch, "/api/subscriptions/"+sub.ID, testToken, map[string]any{"paused": true})
	if w.Code != http.StatusOK {
		t.Fatalf("patch subscription: status = %d, want 200; body: %s", w.Code, w.Body.String())
	}
	var patched subscriptionDTO
	decodeJSONBody(t, w, &patched)
	if !patched.Paused {
		t.Fatalf("subscription not paused: %+v", patched)
	}

	// patch: mode change unsupported
	w = doRequest(t, srv, http.MethodPatch, "/api/subscriptions/"+sub.ID, testToken, map[string]any{"mode": "receive"})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("patch mode: status = %d, want 400; body: %s", w.Code, w.Body.String())
	}

	// delete
	w = doRequest(t, srv, http.MethodDelete, "/api/subscriptions/"+sub.ID, testToken, nil)
	if w.Code != http.StatusNoContent {
		t.Fatalf("delete subscription: status = %d, want 204; body: %s", w.Code, w.Body.String())
	}
	w = doRequest(t, srv, http.MethodDelete, "/api/subscriptions/"+sub.ID, testToken, nil)
	if w.Code != http.StatusNotFound {
		t.Fatalf("delete unknown subscription: status = %d, want 404; body: %s", w.Code, w.Body.String())
	}
}

func TestSubscriptionsAddBadMode(t *testing.T) {
	srv, _, _ := newTestServer(t)
	w := doRequest(t, srv, http.MethodPost, "/api/subscriptions", testToken, map[string]any{
		"peer": "aa11", "share_id": "bb22", "local_path": t.TempDir(), "mode": "bogus",
	})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body: %s", w.Code, w.Body.String())
	}
}

func TestSubscriptionsPatchMalformedID(t *testing.T) {
	srv, _, _ := newTestServer(t)
	w := doRequest(t, srv, http.MethodPatch, "/api/subscriptions/no-colon-here", testToken, map[string]any{"paused": true})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body: %s", w.Code, w.Body.String())
	}
}

// --- trash -----------------------------------------------------------------

func TestTrashListUnknownShare(t *testing.T) {
	srv, _, _ := newTestServer(t)
	w := doRequest(t, srv, http.MethodGet, "/api/shares/doesnotexist/trash", testToken, nil)
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body: %s", w.Code, w.Body.String())
	}
}

func TestTrashListEmpty(t *testing.T) {
	srv, n, _ := newTestServer(t)
	id, err := n.AddShare(t.TempDir(), "share", "read-write", false)
	if err != nil {
		t.Fatalf("add share: %v", err)
	}
	w := doRequest(t, srv, http.MethodGet, "/api/shares/"+id+"/trash", testToken, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", w.Code, w.Body.String())
	}
	var resp struct {
		Entries []trashEntryDTO `json:"entries"`
	}
	decodeJSONBody(t, w, &resp)
	if len(resp.Entries) != 0 {
		t.Fatalf("entries = %+v, want none", resp.Entries)
	}
}

func TestTrashRestoreMissingRelPath(t *testing.T) {
	srv, n, _ := newTestServer(t)
	id, err := n.AddShare(t.TempDir(), "share", "read-write", false)
	if err != nil {
		t.Fatalf("add share: %v", err)
	}
	w := doRequest(t, srv, http.MethodPost, "/api/shares/"+id+"/trash/restore", testToken, map[string]any{})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body: %s", w.Code, w.Body.String())
	}
}

func TestTrashRestoreUnknownEntry(t *testing.T) {
	srv, n, _ := newTestServer(t)
	id, err := n.AddShare(t.TempDir(), "share", "read-write", false)
	if err != nil {
		t.Fatalf("add share: %v", err)
	}
	w := doRequest(t, srv, http.MethodPost, "/api/shares/"+id+"/trash/restore", testToken, map[string]any{"rel_path": "nope.txt"})
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body: %s", w.Code, w.Body.String())
	}
}

func TestTrashListAndRestoreRoundTrip(t *testing.T) {
	srv, n, paths := newTestServer(t)
	shareDir := t.TempDir()
	id, err := n.AddShare(shareDir, "share", "read-write", false)
	if err != nil {
		t.Fatalf("add share: %v", err)
	}

	// Put something in the trash the way applyDelete/pullAndInstall would
	// (see internal/sync/trash.go's doc comment) — a second Trash instance
	// over the same on-disk root as the node's own.
	tr := syncsvc.NewTrash(paths.TrashDir(), nil)
	src := filepath.Join(t.TempDir(), "foo.txt")
	if err := os.WriteFile(src, []byte("hello"), 0o644); err != nil {
		t.Fatalf("write src: %v", err)
	}
	if err := tr.Put(context.Background(), id, "foo.txt", src); err != nil {
		t.Fatalf("trash put: %v", err)
	}

	w := doRequest(t, srv, http.MethodGet, "/api/shares/"+id+"/trash", testToken, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("list: status = %d, want 200; body: %s", w.Code, w.Body.String())
	}
	var resp struct {
		Entries []trashEntryDTO `json:"entries"`
	}
	decodeJSONBody(t, w, &resp)
	if len(resp.Entries) != 1 || resp.Entries[0].RelPath != "foo.txt" {
		t.Fatalf("entries = %+v, want one foo.txt entry", resp.Entries)
	}

	w = doRequest(t, srv, http.MethodPost, "/api/shares/"+id+"/trash/restore", testToken, map[string]any{"rel_path": "foo.txt"})
	if w.Code != http.StatusOK {
		t.Fatalf("restore: status = %d, want 200; body: %s", w.Code, w.Body.String())
	}

	data, err := os.ReadFile(filepath.Join(shareDir, "foo.txt"))
	if err != nil {
		t.Fatalf("read restored file: %v", err)
	}
	if string(data) != "hello" {
		t.Fatalf("restored content = %q, want %q", data, "hello")
	}
}

// --- root / catch-all --------------------------------------------------------

func TestRootPlaceholder(t *testing.T) {
	srv, _, _ := newTestServer(t)
	// "/" and unauthenticated /api-shaped-but-unregistered paths should not
	// require auth (the future web UI must load before it has a token).
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", w.Code, w.Body.String())
	}
}

// TestSubscriptionsAddRejectsUnknownPeer pins that core's peer validation
// surfaces as a 400 rather than a 201 with an inert subscription behind it.
func TestSubscriptionsAddRejectsUnknownPeer(t *testing.T) {
	srv, node, _ := newTestServer(t)

	w := doRequest(t, srv, http.MethodPost, "/api/subscriptions", testToken, map[string]any{
		"peer": "nishinomiya", "share_id": "test", "local_path": filepath.Join(t.TempDir(), "sub"), "mode": "mirror",
	})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body: %s", w.Code, w.Body.String())
	}
	if subs := node.Status().Subscriptions; len(subs) != 0 {
		t.Fatalf("subscriptions = %+v, want the rejected one not to be persisted", subs)
	}
}

// TestSubscriptionsList covers the GET that completes the collection:
// /api/peers and /api/shares have always had one, and subscriptions
// were the one collection you could create, modify and delete but never
// enumerate — so `syncat subscription ls` had nothing to call.
func TestSubscriptionsList(t *testing.T) {
	srv, _, _ := newTestServer(t)

	w := doRequest(t, srv, http.MethodGet, "/api/subscriptions", testToken, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", w.Code, w.Body.String())
	}
	var empty struct {
		Subscriptions []subscriptionDTO `json:"subscriptions"`
	}
	decodeJSONBody(t, w, &empty)
	if len(empty.Subscriptions) != 0 {
		t.Fatalf("subscriptions = %+v, want none", empty.Subscriptions)
	}

	w = doRequest(t, srv, http.MethodPost, "/api/peers", testToken, map[string]string{"token": fakePeerToken(t, "bob"), "name": "bob"})
	if w.Code != http.StatusCreated {
		t.Fatalf("add peer: status = %d, want 201; body: %s", w.Code, w.Body.String())
	}
	var peer peerDTO
	decodeJSONBody(t, w, &peer)

	localDir := filepath.Join(t.TempDir(), "sub")
	w = doRequest(t, srv, http.MethodPost, "/api/subscriptions", testToken, map[string]any{
		"peer": peer.ID, "share_id": "bb22", "local_path": localDir, "mode": "mirror",
	})
	if w.Code != http.StatusCreated {
		t.Fatalf("add subscription: status = %d, want 201; body: %s", w.Code, w.Body.String())
	}

	w = doRequest(t, srv, http.MethodGet, "/api/subscriptions", testToken, nil)
	var resp struct {
		Subscriptions []subscriptionDTO `json:"subscriptions"`
	}
	decodeJSONBody(t, w, &resp)
	if len(resp.Subscriptions) != 1 {
		t.Fatalf("subscriptions = %+v, want exactly one", resp.Subscriptions)
	}
	got := resp.Subscriptions[0]
	if got.ID != peer.ID+":bb22" || got.ShareID != "bb22" || got.LocalPath != localDir || got.Mode != "mirror" {
		t.Fatalf("unexpected subscription: %+v", got)
	}

	// A pause has to be visible here, or `subscription ls` cannot show it.
	w = doRequest(t, srv, http.MethodPatch, "/api/subscriptions/"+got.ID, testToken, map[string]any{"paused": true})
	if w.Code != http.StatusOK {
		t.Fatalf("patch: status = %d, want 200; body: %s", w.Code, w.Body.String())
	}
	w = doRequest(t, srv, http.MethodGet, "/api/subscriptions", testToken, nil)
	decodeJSONBody(t, w, &resp)
	if len(resp.Subscriptions) != 1 || !resp.Subscriptions[0].Paused {
		t.Fatalf("subscriptions after pause = %+v, want one, paused", resp.Subscriptions)
	}
}

// --- access logging --------------------------------------------------------

// captureLogs points srv's logger at a buffer and returns a func that reads
// back everything logged so far.
func captureLogs(srv *Server) func() string {
	var buf bytes.Buffer
	srv.logger = log.New(&buf, "", 0)
	return buf.String
}

// TestLogMiddlewareSkipsSuccessfulStatusPolls covers the noise rule: the web
// UI polls GET /api/status every 2s for as long as a tab is open, so logging
// each one buries every other line in the daemon's log. A *failing* poll is
// still logged — a status request answering 401 is exactly the kind of thing
// the log exists for.
func TestLogMiddlewareSkipsSuccessfulStatusPolls(t *testing.T) {
	srv, _, _ := newTestServer(t)
	logged := captureLogs(srv)

	for i := 0; i < 5; i++ {
		if rec := doRequest(t, srv, "GET", "/api/status", testToken, nil); rec.Code != http.StatusOK {
			t.Fatalf("status poll %d: code = %d, want 200", i, rec.Code)
		}
	}
	if out := logged(); out != "" {
		t.Errorf("successful status polls logged %q, want nothing", out)
	}

	// A poll that fails auth must still be logged, with its status.
	if rec := doRequest(t, srv, "GET", "/api/status", "wrong-token", nil); rec.Code != http.StatusUnauthorized {
		t.Fatalf("bad-token poll: code = %d, want 401", rec.Code)
	}
	out := logged()
	if !strings.Contains(out, "/api/status") || !strings.Contains(out, "401") {
		t.Errorf("failed status poll logged %q, want a line naming /api/status and 401", out)
	}
}

// TestLogMiddlewareLogsOutcome covers the other half of the change: the
// access log records what happened, not merely that a request arrived. The
// old before-the-handler line made a 404 indistinguishable from a 200.
func TestLogMiddlewareLogsOutcome(t *testing.T) {
	srv, _, _ := newTestServer(t)
	logged := captureLogs(srv)

	if rec := doRequest(t, srv, "GET", "/api/peers", testToken, nil); rec.Code != http.StatusOK {
		t.Fatalf("peers list: code = %d, want 200", rec.Code)
	}
	if out := logged(); !strings.Contains(out, "GET /api/peers -> 200") {
		t.Errorf("logged %q, want a line containing \"GET /api/peers -> 200\"", out)
	}

	if rec := doRequest(t, srv, "DELETE", "/api/peers/nosuchpeer", testToken, nil); rec.Code == http.StatusOK {
		t.Fatalf("delete of a missing peer returned 200, want an error status")
	}
	if out := logged(); !strings.Contains(out, "DELETE /api/peers/nosuchpeer") {
		t.Errorf("logged %q, want the failing DELETE to appear", out)
	}
}

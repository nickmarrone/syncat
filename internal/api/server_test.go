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
	"testing"

	"github.com/nickmarrone/syncat/internal/config"
	"github.com/nickmarrone/syncat/internal/core"
	syncsvc "github.com/nickmarrone/syncat/internal/sync"
	"github.com/nickmarrone/syncat/internal/transport"
)

const testToken = "test-token-0123456789abcdef"
const testSelfAddr = "127.0.0.1:8347"

var testCounter int

// newTestServer builds a Server over a freshly opened core.Node (PipeTransport,
// temp config/data dirs — SPEC.md §10's in-memory transport stand-in),
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
// its "tc" connBlob. That's deliberate: tests exercising peer CRUD via the
// API only care that AddPeer accepts/parses a token and persists a peer,
// not that a connection can actually be established, so this avoids
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

	w := doRequest(t, srv, http.MethodPost, "/api/subscriptions", testToken, map[string]any{
		"peer": "aa11", "share_id": "bb22", "local_path": localDir, "mode": "mirror",
	})
	if w.Code != http.StatusCreated {
		t.Fatalf("add subscription: status = %d, want 201; body: %s", w.Code, w.Body.String())
	}
	var sub subscriptionDTO
	decodeJSONBody(t, w, &sub)
	if sub.ID != "aa11:bb22" || sub.Mode != "mirror" {
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

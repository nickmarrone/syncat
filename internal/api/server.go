// Package api serves the localhost REST API that both the web UI and the CLI subcommands drive (SPEC.md §8).
//
// internal/core deliberately imports neither encoding/json nor any HTTP
// package (SPEC.md §12), so this package owns all JSON marshaling (see
// dto.go) and all HTTP concerns: routing, auth, request size limits, and
// error shaping.
//
// Auth: every request under /api/ must carry header
// "X-Syncat-Token: <api.token contents>", compared with
// crypto/subtle.ConstantTimeCompare so a wrong guess can't be timed. The
// one exception is GET /ui-token, a login-less same-origin bootstrap the
// (future, Phase 9) web UI uses to fetch the token before it can set the
// header itself — see handleUIToken's doc comment for its defenses.
//
// The listener itself is loopback-only (ListenLoopback refuses to bind
// anything else) so the API is never reachable from the LAN even if the
// token leaks or a caller mis-set --api.
package api

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"

	syncsvc "github.com/nickmarrone/syncat/internal/sync"

	"github.com/nickmarrone/syncat/internal/core"
	"github.com/nickmarrone/syncat/internal/webui"
)

// maxRequestBody bounds the size of any request body this server reads
// (share paths, tokens, and JSON overhead are all tiny; 1 MiB is
// generous headroom while still refusing a hostile/broken client that
// tries to stream gigabytes at us).
const maxRequestBody = 1 << 20 // 1 MiB

// Server is the REST API described in SPEC.md §8: a thin HTTP layer over
// one *core.Node. Construct with NewServer, get an http.Handler via
// Handler, and serve it on a listener from ListenLoopback.
type Server struct {
	node   *core.Node
	token  string
	logger *log.Logger
	// selfAddr is the host:port this server is bound to (the actual
	// listener address, not necessarily the configured one — e.g. if the
	// configured port was 0). handleUIToken compares it against the
	// request's Host header as a DNS-rebinding defense; see that
	// handler's doc comment.
	selfAddr string

	mux *http.ServeMux
}

// NewServer builds a Server for node, authenticating requests against
// token (the contents of api.token) and using selfAddr (typically
// listener.Addr().String()) for /ui-token's Host-header check. logger, if
// nil, defaults to log.Default().
func NewServer(node *core.Node, token, selfAddr string, logger *log.Logger) *Server {
	if logger == nil {
		logger = log.Default()
	}
	s := &Server{node: node, token: token, selfAddr: selfAddr, logger: logger}
	s.mux = http.NewServeMux()
	s.routes()
	return s
}

// Handler returns the http.Handler to serve — wraps the routed mux with
// panic recovery (a handler bug must never crash the daemon) and basic
// access logging.
func (s *Server) Handler() http.Handler {
	return s.recoverMiddleware(s.logMiddleware(s.mux))
}

// ListenLoopback binds a TCP listener on addr, refusing to bind any host
// that isn't loopback (SPEC.md §8: "the API isn't exposed on the LAN even
// if the token leaks" — see the package doc comment). An empty host in
// addr (e.g. ":8347") is treated as 127.0.0.1, matching config.Default's
// APIAddr shape ("127.0.0.1:8347") rather than defaulting to "all
// interfaces" the way net.Listen normally would.
func ListenLoopback(addr string) (net.Listener, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("api: invalid listen address %q: %w", addr, err)
	}
	if host == "" {
		host = "127.0.0.1"
	}
	if !isLoopbackHost(host) {
		return nil, fmt.Errorf("api: refusing to bind non-loopback address %q: syncat's API must stay on localhost", addr)
	}
	ln, err := net.Listen("tcp", net.JoinHostPort(host, port))
	if err != nil {
		return nil, fmt.Errorf("api: listen on %q: %w", addr, err)
	}
	return ln, nil
}

func isLoopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// routes registers every SPEC.md §8 endpoint. Go 1.22+ ServeMux patterns
// (METHOD /path/{wildcard}) need no router library, matching the
// dependency budget in SPEC.md §10/CLAUDE.md.
func (s *Server) routes() {
	// Login-less bootstrap: no auth middleware, defended entirely by
	// handleUIToken's own loopback/Host/Origin checks.
	s.mux.HandleFunc("GET /ui-token", s.handleUIToken)

	s.mux.HandleFunc("GET /api/status", s.auth(s.handleStatus))
	// core.Node.RenameNode already existed for Phase 9's editable
	// dashboard node-name field (SPEC.md §9) but Phase 8 never wired a
	// route to it — added here since it's the smallest reasonable place
	// for it (peer with GET /api/status, which is where node_name is
	// read from).
	s.mux.HandleFunc("PATCH /api/node", s.auth(s.handleNodePatch))

	s.mux.HandleFunc("GET /api/peers", s.auth(s.handlePeersList))
	s.mux.HandleFunc("POST /api/peers", s.auth(s.handlePeersAdd))
	s.mux.HandleFunc("DELETE /api/peers/{id}", s.auth(s.handlePeersDelete))
	// Deferred: SPEC.md §2.3's pending-peer approval queue isn't
	// implemented (see internal/core's package doc comment) — an unknown
	// inbound key is rejected outright rather than queued. These two
	// endpoints exist in the URL space so a client gets a clear,
	// documented "not implemented" rather than a 404 that looks like a
	// typo.
	s.mux.HandleFunc("POST /api/peers/{id}/approve", s.auth(s.handleNotImplemented("peer approval queue (SPEC.md §2.3) is not implemented in this build")))
	s.mux.HandleFunc("GET /api/approvals", s.auth(s.handleNotImplemented("approval queue (SPEC.md §2.3/§6) is not implemented in this build")))
	s.mux.HandleFunc("POST /api/approvals/{id}", s.auth(s.handleNotImplemented("approval queue (SPEC.md §2.3/§6) is not implemented in this build")))

	s.mux.HandleFunc("GET /api/shares", s.auth(s.handleSharesList))
	s.mux.HandleFunc("POST /api/shares", s.auth(s.handleSharesAdd))
	s.mux.HandleFunc("PATCH /api/shares/{id}", s.auth(s.handleSharesPatch))
	s.mux.HandleFunc("DELETE /api/shares/{id}", s.auth(s.handleSharesDelete))

	s.mux.HandleFunc("GET /api/remote-shares", s.auth(s.handleRemoteShares))

	s.mux.HandleFunc("GET /api/subscriptions", s.auth(s.handleSubscriptionsList))
	s.mux.HandleFunc("POST /api/subscriptions", s.auth(s.handleSubscriptionsAdd))
	s.mux.HandleFunc("PATCH /api/subscriptions/{id}", s.auth(s.handleSubscriptionsPatch))
	s.mux.HandleFunc("DELETE /api/subscriptions/{id}", s.auth(s.handleSubscriptionsDelete))

	s.mux.HandleFunc("GET /api/shares/{id}/trash", s.auth(s.handleTrashList))
	s.mux.HandleFunc("POST /api/shares/{id}/trash/restore", s.auth(s.handleTrashRestore))

	// GET /api/events (SSE) is explicitly deferred by SPEC.md's Phase 8
	// scope for this build; the UI polls /api/status instead. Not
	// registered at all, so it 404s like any other unknown route rather
	// than pretending to stream.

	// Embedded web UI (internal/webui, go:embed) — see webui.Handler's
	// doc comment for its own path-traversal and auth reasoning.
	// Unauthenticated by design: a browser must be able to load the page
	// before it has a token (GET /ui-token is how it gets one).
	s.mux.Handle("/", webui.Handler())
}

// auth wraps h to require a valid X-Syncat-Token header, per SPEC.md §8.
func (s *Server) auth(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		got := r.Header.Get("X-Syncat-Token")
		if got == "" || subtle.ConstantTimeCompare([]byte(got), []byte(s.token)) != 1 {
			writeError(w, http.StatusUnauthorized, "unauthorized", "missing or invalid X-Syncat-Token header")
			return
		}
		h(w, r)
	}
}

// handleNotImplemented returns a handler that always reports 501 with a
// stable error shape and an explanatory message, for endpoints SPEC.md
// §8 lists but this build deliberately defers (see routes' comments).
func (s *Server) handleNotImplemented(message string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusNotImplemented, "not_implemented", message)
	}
}

func (s *Server) recoverMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				s.logger.Printf("api: panic handling %s %s: %v", r.Method, r.URL.Path, rec)
				writeError(w, http.StatusInternalServerError, "internal", "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func (s *Server) logMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.logger.Printf("api: %s %s", r.Method, r.URL.Path)
		next.ServeHTTP(w, r)
	})
}

// --- request/response helpers ---------------------------------------------

// errorBody is the stable JSON shape for every error response.
type errorBody struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	var body errorBody
	body.Error.Code = code
	body.Error.Message = message
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// decodeJSON reads and decodes r's body into v, enforcing maxRequestBody
// and writing a 400 error itself on any failure (empty body included,
// since every caller of decodeJSON needs a body). Returns false if it
// wrote an error response, in which case the handler must return
// immediately.
func decodeJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBody)
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(v); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "malformed JSON request body: "+err.Error())
		return false
	}
	return true
}

// requireField writes a 400 error and returns false if value is empty
// after trimming whitespace.
func requireField(w http.ResponseWriter, name, value string) bool {
	if strings.TrimSpace(value) == "" {
		writeError(w, http.StatusBadRequest, "bad_request", name+" is required")
		return false
	}
	return true
}

// mutationError classifies an error returned by one of core.Node's
// mutation methods into an HTTP status code and stable error code.
//
// internal/core's mutation API returns plain wrapped errors (fmt.Errorf),
// not typed/sentinel errors distinguishing "bad input" from "unknown id"
// from "conflicts with existing state" — see the deviation note in the
// package doc comment. Every message it produces is deterministic,
// synchronous validation output (never a leaked stack trace or raw
// syscall error), so it's safe to return verbatim to an already
// token-authenticated caller; classification here is purely a
// pattern-match on the wording those methods are documented to use, kept
// in one place so a future core change to typed errors only touches this
// function.
func mutationError(err error) (status int, code, message string) {
	if err == nil {
		return http.StatusOK, "", ""
	}
	if errors.Is(err, syncsvc.ErrRestoreDestExists) {
		return http.StatusConflict, "conflict", err.Error()
	}

	msg := err.Error()
	switch {
	// "no share matches" is internal/core/resolve.go's phrasing for a share
	// ref that names nothing. Every path that produces it is addressing a
	// share by URL — /api/shares/{id} and /api/shares/{id}/trash — so it is
	// the addressed resource being absent, i.e. a 404, exactly like the
	// "is not configured" lookups it now runs ahead of.
	//
	// Its peer counterpart ("no peer matches") is deliberately absent here.
	// A peer ref only ever reaches core from a request *body*
	// (POST /api/subscriptions), which is caller-fixable input and stays a
	// 400. DELETE /api/peers/{id} never produces it: RemovePeer resolves
	// best-effort and falls through to findPeerIndex's own "is not
	// configured", which this same case already maps to 404.
	case containsAny(msg, "is not configured", "is not a local share", "no trashed entry", "has no active watch", "no share matches"):
		return http.StatusNotFound, "not_found", msg
	case containsAny(msg, "already configured", "already exists", "overlaps"):
		return http.StatusConflict, "conflict", msg
	default:
		// Every other error core's mutation methods produce is input
		// validation (invalid permission/mode, non-absolute path, empty
		// required field, malformed token, ...) — 400 is the honest
		// default rather than 500, since these are all caller-fixable.
		return http.StatusBadRequest, "bad_request", msg
	}
}

func containsAny(s string, substrs ...string) bool {
	for _, sub := range substrs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

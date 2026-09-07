// Package api serves the localhost REST API that both the web UI and the CLI subcommands drive (SPEC.md §8).
//
// internal/core deliberately imports neither encoding/json nor any HTTP
// package (SPEC.md §12), so this package owns all JSON marshaling (the DTO
// types and converters in dto.go) and all HTTP concerns: routing, auth,
// request size limits, and error shaping (server.go); the per-endpoint
// handlers live in handlers.go.
//
// Auth: every request under /api/ must carry header
// "X-Syncat-Token: <api.token contents>", compared with
// crypto/subtle.ConstantTimeCompare so a wrong guess can't be timed. The
// one exception is GET /ui-token, a login-less same-origin bootstrap the
// embedded web UI uses to fetch the token before it can set the header
// itself — see handleUIToken's doc comment for its defenses.
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
	"time"

	"github.com/nickmarrone/syncat/internal/config"
	"github.com/nickmarrone/syncat/internal/core"
	syncsvc "github.com/nickmarrone/syncat/internal/sync"
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

// routes registers every SPEC.md §8 endpoint. Go 1.22+ ServeMux patterns
// (METHOD /path/{wildcard}) need no router library, matching the
// dependency budget in SPEC.md §10/CLAUDE.md.
func (s *Server) routes() {
	// Login-less bootstrap: no auth middleware, defended entirely by
	// handleUIToken's own loopback/Host/Origin checks.
	s.mux.HandleFunc("GET /ui-token", s.handleUIToken)

	s.mux.HandleFunc("GET /api/status", s.auth(s.handleStatus))
	// Backs the dashboard's editable node-name field (SPEC.md §9). It
	// sits beside GET /api/status, which is where node_name is read from.
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

	// GET /api/events (SSE) is deliberately not implemented; the UI polls
	// /api/status instead. Not registered at all, so it 404s like any
	// other unknown route rather than pretending to stream.

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

// --- /ui-token ---------------------------------------------------------------

// handleUIToken implements SPEC.md §8's "login-less same-origin bootstrap
// endpoint ... only served to localhost": it hands back the API token
// with no X-Syncat-Token required, so a freshly loaded web UI (which has
// no token yet) can fetch one. Because that makes it the one endpoint a
// malicious *local* web page could try to abuse, it applies three
// independent checks before answering:
//
//  1. RemoteAddr must be loopback. Read from the actual TCP connection
//     (http.Request.RemoteAddr), never a client-supplied header like
//     X-Forwarded-For — this server has no reverse proxy in front of it
//     (it binds loopback-only, see ListenLoopback), so RemoteAddr is
//     trustworthy and a spoofable header would only weaken the check.
//
//  2. Host header must equal this server's own bind address. This is the
//     specific defense against DNS rebinding: a page served from
//     attacker.com can have its DNS re-pointed at 127.0.0.1 mid-session,
//     after which a same-origin fetch("/ui-token") from that page really
//     does connect to this loopback server (RemoteAddr genuinely is
//     loopback) — but the browser still sets the Host header from the
//     URL's authority, "attacker.com", which it never lets page JS
//     override. Comparing Host against our real listener address catches
//     exactly that case, which the RemoteAddr check alone cannot.
//
//  3. Origin header, if present, must equal this server's own origin.
//     A cross-origin fetch/XHR from any other page (no rebinding
//     involved, just attacker.com directly requesting
//     http://127.0.0.1:<port>/ui-token) carries an Origin header the
//     browser also won't let page JS override; requiring it to match
//     rejects that request outright rather than relying solely on the
//     browser's same-origin policy to stop the attacker page from
//     *reading* the response (this server also sends no
//     Access-Control-Allow-Origin header, so that read would already be
//     blocked — this check adds defense in depth and a clean 403 instead
//     of quietly processing a request that came from nowhere legitimate).
//     Origin is intentionally *not* required to be present: some
//     legitimate same-origin requests (plain top-level navigation, and
//     historically some same-origin fetches) omit it, and Host has
//     already done the load-bearing check.
func (s *Server) handleUIToken(w http.ResponseWriter, r *http.Request) {
	if !isLoopbackAddr(r.RemoteAddr) {
		writeError(w, http.StatusForbidden, "forbidden", "this endpoint is only served to localhost")
		return
	}
	if r.Host != s.selfAddr {
		writeError(w, http.StatusForbidden, "forbidden", "request Host does not match this server")
		return
	}
	if origin := r.Header.Get("Origin"); origin != "" && origin != "http://"+s.selfAddr {
		writeError(w, http.StatusForbidden, "forbidden", "request Origin does not match this server")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"token": s.token})
}

// isLoopbackHost and isLoopbackAddr are deliberately two functions with
// slightly different semantics, matched to their two call sites:
//
//   - isLoopbackHost takes a bare host (already split from its port by
//     ListenLoopback) and accepts the literal "localhost" as well as any
//     loopback IP, because a user configuring api.addr = "localhost:8347"
//     means loopback and should be allowed to bind.
//
//   - isLoopbackAddr takes a host:port as found in http.Request.RemoteAddr
//     and accepts *only* loopback IP literals. RemoteAddr comes from the
//     TCP connection, which is always an IP, so a name like "localhost"
//     can never legitimately appear there; refusing it keeps the
//     /ui-token check (a security boundary) as narrow as possible.
func isLoopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func isLoopbackAddr(remoteAddr string) bool {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		host = remoteAddr
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
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

// statusRecorder captures the response status so logMiddleware can decide
// after the fact whether a request was worth a log line. WriteHeader is not
// guaranteed to be called — a handler that only writes a body implies 200 —
// so status is seeded with http.StatusOK rather than zero.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (rec *statusRecorder) WriteHeader(status int) {
	rec.status = status
	rec.ResponseWriter.WriteHeader(status)
}

// Unwrap lets http.ResponseController reach the underlying ResponseWriter,
// so wrapping here doesn't cost a handler its flush/hijack capabilities.
func (rec *statusRecorder) Unwrap() http.ResponseWriter { return rec.ResponseWriter }

// isPollEndpoint reports whether path is one the web UI polls on a timer
// rather than one a user or the CLI drives. The dashboard refreshes
// GET /api/status every 2s for as long as a browser tab is open, so logging
// every one of those buries every other line in the daemon's log — 30
// lines a minute that say only "a tab is still open". A *failing* poll is
// a different matter (see logMiddleware): a status request that starts
// answering 401 or 500 is exactly the kind of thing the log should carry.
func isPollEndpoint(path string) bool {
	return path == "/api/status"
}

func (s *Server) logMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		next.ServeHTTP(rec, r)

		// Log after the handler, not before, so the line carries the
		// outcome. The old before-the-call line reported only that a
		// request had arrived, which made a 401 from a stale token or a
		// 500 from a broken handler indistinguishable from a success.
		if rec.status < 400 && isPollEndpoint(r.URL.Path) {
			return
		}
		s.logger.Printf("api: %s %s -> %d (%s)", r.Method, r.URL.Path, rec.status, time.Since(start).Round(time.Millisecond))
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

// normalizePermission accepts both the shorthand the CLI's --perm flag
// uses (ro/rw) and the full config.Permission* values, so the API and CLI
// agree on what's valid without the CLI needing to translate first.
func normalizePermission(v string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "ro", "read-only", "readonly":
		return config.PermissionReadOnly, nil
	case "rw", "read-write", "readwrite":
		return config.PermissionReadWrite, nil
	default:
		return "", fmt.Errorf("invalid permission %q (want \"ro\"/\"read-only\" or \"rw\"/\"read-write\")", v)
	}
}

// normalizeMode accepts both the shorthand the CLI's --mode flag uses
// (mirror/receive) and the full config.Mode* values.
func normalizeMode(v string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "mirror":
		return config.ModeMirror, nil
	case "receive", "receive-only", "receiveonly":
		return config.ModeReceiveOnly, nil
	default:
		return "", fmt.Errorf("invalid mode %q (want \"mirror\" or \"receive\"/\"receive-only\")", v)
	}
}

// writeMutationError classifies err with mutationError and writes the
// resulting error response. It is the one way handlers report a failed
// core.Node mutation.
func writeMutationError(w http.ResponseWriter, err error) {
	status, code, msg := mutationError(err)
	writeError(w, status, code, msg)
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
	// "no share matches" is internal/core's matchRef phrasing for a share
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

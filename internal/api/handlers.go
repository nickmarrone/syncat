package api

import (
	"fmt"
	"net"
	"net/http"
	"strings"

	"github.com/nickmarrone/syncat/internal/config"
)

// --- status ------------------------------------------------------------

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, toStatusResponse(s.node.Status()))
}

// --- peers ---------------------------------------------------------------

func (s *Server) handlePeersList(w http.ResponseWriter, r *http.Request) {
	st := s.node.Status()
	peers := make([]peerDTO, 0, len(st.Peers))
	for _, p := range st.Peers {
		peers = append(peers, toPeerDTO(p))
	}
	writeJSON(w, http.StatusOK, map[string]any{"peers": peers})
}

type addPeerRequest struct {
	Token string `json:"token"`
	Name  string `json:"name"`
}

func (s *Server) handlePeersAdd(w http.ResponseWriter, r *http.Request) {
	var req addPeerRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if !requireField(w, "token", req.Token) {
		return
	}
	id, err := s.node.AddPeer(req.Name, req.Token)
	if err != nil {
		status, code, msg := mutationError(err)
		writeError(w, status, code, msg)
		return
	}
	peer := findPeerDTO(s.node.Status(), id)
	if peer == nil {
		writeError(w, http.StatusInternalServerError, "internal", "peer added but not found afterward")
		return
	}
	writeJSON(w, http.StatusCreated, peer)
}

func (s *Server) handlePeersDelete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.node.RemovePeer(id); err != nil {
		status, code, msg := mutationError(err)
		writeError(w, status, code, msg)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// --- shares ----------------------------------------------------------------

func (s *Server) handleSharesList(w http.ResponseWriter, r *http.Request) {
	st := s.node.Status()
	shares := make([]shareDTO, 0, len(st.Shares))
	for _, sh := range st.Shares {
		shares = append(shares, toShareDTO(sh))
	}
	writeJSON(w, http.StatusOK, map[string]any{"shares": shares})
}

type addShareRequest struct {
	Path             string `json:"path"`
	Name             string `json:"name"`
	Permission       string `json:"permission"`
	ApprovalRequired bool   `json:"approval_required"`
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
		return "", errInvalidf("invalid permission %q (want \"ro\"/\"read-only\" or \"rw\"/\"read-write\")", v)
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
		return "", errInvalidf("invalid mode %q (want \"mirror\" or \"receive\"/\"receive-only\")", v)
	}
}

func (s *Server) handleSharesAdd(w http.ResponseWriter, r *http.Request) {
	var req addShareRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if !requireField(w, "path", req.Path) || !requireField(w, "name", req.Name) {
		return
	}
	perm, err := normalizePermission(req.Permission)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	id, err := s.node.AddShare(req.Path, req.Name, perm, req.ApprovalRequired)
	if err != nil {
		status, code, msg := mutationError(err)
		writeError(w, status, code, msg)
		return
	}
	share := findShareDTO(s.node.Status(), id)
	if share == nil {
		writeError(w, http.StatusInternalServerError, "internal", "share added but not found afterward")
		return
	}
	writeJSON(w, http.StatusCreated, share)
}

type patchShareRequest struct {
	Name             *string           `json:"name"`
	Permission       *string           `json:"permission"`
	ApprovalRequired *bool             `json:"approval_required"`
	Access           map[string]string `json:"access"` // peer key hex -> granted|denied|revoked
}

func (s *Server) handleSharesPatch(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req patchShareRequest
	if !decodeJSON(w, r, &req) {
		return
	}

	if findShareDTO(s.node.Status(), id) == nil {
		writeError(w, http.StatusNotFound, "not_found", "share "+id+" is not configured")
		return
	}

	if req.Name != nil {
		if err := s.node.RenameShare(id, *req.Name); err != nil {
			status, code, msg := mutationError(err)
			writeError(w, status, code, msg)
			return
		}
	}
	if req.Permission != nil {
		perm, err := normalizePermission(*req.Permission)
		if err != nil {
			writeError(w, http.StatusBadRequest, "bad_request", err.Error())
			return
		}
		if err := s.node.SetSharePermission(id, perm); err != nil {
			status, code, msg := mutationError(err)
			writeError(w, status, code, msg)
			return
		}
	}
	if req.ApprovalRequired != nil {
		if err := s.node.SetShareApprovalRequired(id, *req.ApprovalRequired); err != nil {
			status, code, msg := mutationError(err)
			writeError(w, status, code, msg)
			return
		}
	}
	for peerKey, access := range req.Access {
		if err := s.node.SetShareAccess(id, peerKey, access); err != nil {
			status, code, msg := mutationError(err)
			writeError(w, status, code, msg)
			return
		}
	}

	share := findShareDTO(s.node.Status(), id)
	writeJSON(w, http.StatusOK, share)
}

func (s *Server) handleSharesDelete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.node.RemoveShare(id); err != nil {
		status, code, msg := mutationError(err)
		writeError(w, status, code, msg)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// --- remote shares -----------------------------------------------------------

func (s *Server) handleRemoteShares(w http.ResponseWriter, r *http.Request) {
	st := s.node.Status()
	shares := make([]remoteShareDTO, 0, len(st.RemoteShares))
	for _, rs := range st.RemoteShares {
		shares = append(shares, toRemoteShareDTO(rs))
	}
	writeJSON(w, http.StatusOK, map[string]any{"remote_shares": shares})
}

// --- subscriptions -----------------------------------------------------------

type addSubscriptionRequest struct {
	Peer      string `json:"peer"`
	ShareID   string `json:"share_id"`
	LocalPath string `json:"local_path"`
	Mode      string `json:"mode"`
}

func (s *Server) handleSubscriptionsAdd(w http.ResponseWriter, r *http.Request) {
	var req addSubscriptionRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if !requireField(w, "peer", req.Peer) || !requireField(w, "share_id", req.ShareID) || !requireField(w, "local_path", req.LocalPath) {
		return
	}
	mode, err := normalizeMode(req.Mode)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if err := s.node.AddSubscription(req.Peer, req.ShareID, req.LocalPath, mode); err != nil {
		status, code, msg := mutationError(err)
		writeError(w, status, code, msg)
		return
	}
	sub := findSubscriptionDTO(s.node.Status(), req.Peer, req.ShareID)
	if sub == nil {
		writeError(w, http.StatusInternalServerError, "internal", "subscription added but not found afterward")
		return
	}
	writeJSON(w, http.StatusCreated, sub)
}

type patchSubscriptionRequest struct {
	Paused *bool   `json:"paused"`
	Mode   *string `json:"mode"`
}

func (s *Server) handleSubscriptionsPatch(w http.ResponseWriter, r *http.Request) {
	peerKey, shareID, ok := parseSubscriptionID(r.PathValue("id"))
	if !ok {
		writeError(w, http.StatusBadRequest, "bad_request", "malformed subscription id (want \"<peer-key>:<share-id>\")")
		return
	}
	var req patchSubscriptionRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if findSubscriptionDTO(s.node.Status(), peerKey, shareID) == nil {
		writeError(w, http.StatusNotFound, "not_found", "subscription "+r.PathValue("id")+" is not configured")
		return
	}
	if req.Mode != nil {
		// core.Node exposes no subscription-mode-change method (only
		// pause/resume and remove — see internal/core/node.go); changing
		// mode means remove-then-re-add. Documented rather than silently
		// ignored.
		writeError(w, http.StatusBadRequest, "bad_request", "changing a subscription's mode is not supported by this build; remove and re-add the subscription with the new mode")
		return
	}
	if req.Paused != nil {
		if err := s.node.PauseSubscription(peerKey, shareID, *req.Paused); err != nil {
			status, code, msg := mutationError(err)
			writeError(w, status, code, msg)
			return
		}
	}
	sub := findSubscriptionDTO(s.node.Status(), peerKey, shareID)
	writeJSON(w, http.StatusOK, sub)
}

func (s *Server) handleSubscriptionsDelete(w http.ResponseWriter, r *http.Request) {
	peerKey, shareID, ok := parseSubscriptionID(r.PathValue("id"))
	if !ok {
		writeError(w, http.StatusBadRequest, "bad_request", "malformed subscription id (want \"<peer-key>:<share-id>\")")
		return
	}
	if err := s.node.RemoveSubscription(peerKey, shareID); err != nil {
		status, code, msg := mutationError(err)
		writeError(w, status, code, msg)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// --- trash (SPEC.md §7) -----------------------------------------------------

func (s *Server) handleTrashList(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	entries, err := s.node.ListTrash(id)
	if err != nil {
		status, code, msg := mutationError(err)
		writeError(w, status, code, msg)
		return
	}
	out := make([]trashEntryDTO, 0, len(entries))
	for _, e := range entries {
		out = append(out, toTrashEntryDTO(e))
	}
	writeJSON(w, http.StatusOK, map[string]any{"entries": out})
}

type restoreTrashRequest struct {
	RelPath string `json:"rel_path"`
}

func (s *Server) handleTrashRestore(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req restoreTrashRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if !requireField(w, "rel_path", req.RelPath) {
		return
	}
	row, err := s.node.RestoreTrash(r.Context(), id, req.RelPath)
	if err != nil {
		status, code, msg := mutationError(err)
		writeError(w, status, code, msg)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"share_id": id,
		"rel_path": req.RelPath,
		"version":  row.Version,
	})
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

func isLoopbackAddr(remoteAddr string) bool {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		host = remoteAddr
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// errInvalidf is a fmt.Errorf shorthand for normalizePermission/normalizeMode's
// validation errors.
func errInvalidf(format string, args ...any) error {
	return fmt.Errorf(format, args...)
}

package api

import (
	"net/http"

	"github.com/nickmarrone/syncat/internal/core"
)

// --- status ------------------------------------------------------------

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, toStatusResponse(s.node.Status()))
}

// --- node (PATCH /api/node) -----------------------------------------------

type patchNodeRequest struct {
	Name *string `json:"name"`
}

func (s *Server) handleNodePatch(w http.ResponseWriter, r *http.Request) {
	var req patchNodeRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.Name != nil {
		if !requireField(w, "name", *req.Name) {
			return
		}
		if err := s.node.RenameNode(*req.Name); err != nil {
			writeMutationError(w, err)
			return
		}
	}
	st := s.node.Status()
	writeJSON(w, http.StatusOK, map[string]string{"node_name": st.NodeName, "node_token": st.NodeToken})
}

// --- peers ---------------------------------------------------------------

func (s *Server) handlePeersList(w http.ResponseWriter, r *http.Request) {
	st := s.node.Status()
	peers := make([]peerDTO, 0, len(st.Peers))
	for _, p := range st.Peers {
		peers = append(peers, toPeerDTO(p))
	}
	approvals := s.node.Approvals()
	pending := make([]pendingPeerDTO, 0)
	for _, approval := range approvals {
		if approval.Kind != core.ApprovalPeer {
			continue
		}
		shortID := approval.PeerKey
		if len(shortID) > 16 {
			shortID = shortID[:16]
		}
		pending = append(pending, pendingPeerDTO{ID: approval.ID, PeerKey: approval.PeerKey, ShortID: shortID, Name: approval.PeerName, FirstSeen: approval.CreatedAt})
	}
	writeJSON(w, http.StatusOK, map[string]any{"peers": peers, "pending_peers": pending})
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
		writeMutationError(w, err)
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
		writeMutationError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handlePeerApprove(w http.ResponseWriter, r *http.Request) {
	if err := s.node.ApprovePendingPeer(r.PathValue("id")); err != nil {
		writeMutationError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// --- approvals ------------------------------------------------------------

func (s *Server) handleApprovalsList(w http.ResponseWriter, r *http.Request) {
	approvals := s.node.Approvals()
	out := make([]approvalDTO, 0, len(approvals))
	for _, approval := range approvals {
		out = append(out, toApprovalDTO(approval))
	}
	writeJSON(w, http.StatusOK, map[string]any{"approvals": out})
}

type approvalDecisionRequest struct {
	Decision string `json:"decision"`
}

func (s *Server) handleApprovalDecision(w http.ResponseWriter, r *http.Request) {
	var req approvalDecisionRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if err := s.node.DecideApproval(r.PathValue("id"), req.Decision); err != nil {
		writeMutationError(w, err)
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
		writeMutationError(w, err)
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
			writeMutationError(w, err)
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
			writeMutationError(w, err)
			return
		}
	}
	if req.ApprovalRequired != nil {
		if err := s.node.SetShareApprovalRequired(id, *req.ApprovalRequired); err != nil {
			writeMutationError(w, err)
			return
		}
	}
	for peerKey, access := range req.Access {
		if err := s.node.SetShareAccess(id, peerKey, access); err != nil {
			writeMutationError(w, err)
			return
		}
	}

	share := findShareDTO(s.node.Status(), id)
	writeJSON(w, http.StatusOK, share)
}

func (s *Server) handleSharesDelete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.node.RemoveShare(id); err != nil {
		writeMutationError(w, err)
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

// handleSubscriptionsList enumerates every configured subscription.
func (s *Server) handleSubscriptionsList(w http.ResponseWriter, r *http.Request) {
	st := s.node.Status()
	subs := make([]subscriptionDTO, 0, len(st.Subscriptions))
	for _, sub := range st.Subscriptions {
		subs = append(subs, toSubscriptionDTO(sub))
	}
	writeJSON(w, http.StatusOK, map[string]any{"subscriptions": subs})
}

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
		writeMutationError(w, err)
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
			writeMutationError(w, err)
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
		writeMutationError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// --- trash (SPEC.md §7) -----------------------------------------------------

func (s *Server) handleTrashList(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	entries, err := s.node.ListTrash(id)
	if err != nil {
		writeMutationError(w, err)
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
		writeMutationError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"share_id": id,
		"rel_path": req.RelPath,
		"version":  row.Version,
	})
}

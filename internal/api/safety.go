package api

import "net/http"

// --- holds and safety settings (see GLOSSARY.md) ------------------------
//
// The {id} in these routes is a share reference in the sense of
// internal/core/resolve.go, for a share this node offers or subscribes
// to, the same as the trash routes.

func (s *Server) handleHoldsList(w http.ResponseWriter, r *http.Request) {
	holds := s.node.Holds()
	out := make([]holdDTO, 0, len(holds))
	for _, h := range holds {
		out = append(out, toHoldDTO(h))
	}
	writeJSON(w, http.StatusOK, map[string]any{"holds": out})
}

type confirmHoldRequest struct {
	// Peer names the peer whose held change to let through. It may be
	// empty when the share has only one hold.
	Peer string `json:"peer"`
}

func (s *Server) handleHoldConfirm(w http.ResponseWriter, r *http.Request) {
	var req confirmHoldRequest
	if r.ContentLength != 0 && !decodeJSON(w, r, &req) {
		return
	}
	h, err := s.node.ConfirmHold(r.Context(), r.PathValue("id"), req.Peer)
	if err != nil {
		writeMutationError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toHoldDTO(h))
}

// patchSafetyRequest changes some of a share's safety settings and keeps
// the rest. A field set to 0 (or "" for trash_max_size) goes back to its
// default.
type patchSafetyRequest struct {
	HoldDeletePercent  *int    `json:"hold_delete_percent"`
	HoldDeleteMin      *int    `json:"hold_delete_min"`
	TrashRetentionDays *int    `json:"trash_retention_days"`
	TrashMaxSize       *string `json:"trash_max_size"`
}

func (s *Server) handleSafetyPatch(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req patchSafetyRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	cur, err := s.node.Safety(id)
	if err != nil {
		writeMutationError(w, err)
		return
	}
	if req.HoldDeletePercent != nil {
		cur.HoldDeletePercent = *req.HoldDeletePercent
	}
	if req.HoldDeleteMin != nil {
		cur.HoldDeleteMin = *req.HoldDeleteMin
	}
	if req.TrashRetentionDays != nil {
		cur.TrashRetentionDays = *req.TrashRetentionDays
	}
	if req.TrashMaxSize != nil {
		cur.TrashMaxSize = *req.TrashMaxSize
	}
	eff, err := s.node.SetSafety(id, cur)
	if err != nil {
		writeMutationError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toSafetyDTO(eff))
}

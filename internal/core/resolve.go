package core

import (
	"fmt"
	"sort"
	"strings"
)

// --- reference resolution ---------------------------------------------------

// Reference resolution for the CLI and REST surface.
//
// Every peer and share has an unreadable canonical id — a 64-hex Ed25519
// public key, a 16-hex share id — and a readable display name shown right
// beside it by `syncat peer ls` and `syncat remote ls`. Taking only the id
// is a usability trap: the name is what a user reads, remembers, and types,
// so `syncat subscription add nishinomiya test ./test/` is the natural
// command to reach for, and until these resolvers existed it was accepted
// verbatim and then failed silently forever (see AddSubscription's doc
// comment).
//
// So a "ref" here is any of:
//
//   - the exact canonical id;
//   - an exact display name, if it is unambiguous;
//   - a unique case-insensitive prefix of the canonical id (git-style).
//
// Checked strictly in that order, so an id always wins over a name that
// happens to look like one, and an exact match always wins over a prefix.
// Ambiguity is an error naming every candidate rather than a silent pick —
// resolving "test" to whichever share sorted first is exactly the class of
// bug this file exists to remove.

// matchRef applies the resolution order above to a set of candidates,
// returning the matched canonical id. kind names the thing being resolved
// ("peer", "share") for error messages; each candidate is an (id, name)
// pair, and name may be empty.
func matchRef(kind, ref string, candidates [][2]string) (string, error) {
	if ref == "" {
		return "", mutationErrorf(MutationInvalid, "empty %s reference", kind)
	}
	if len(candidates) == 0 {
		// An empty collection is the same typed miss as a non-empty set with
		// no matching candidate; wording remains solely diagnostic.
		return "", mutationErrorf(MutationNotFound, "no %s matches %q — no %ss are configured", kind, ref, kind)
	}

	var byName, byPrefix []string
	lower := strings.ToLower(ref)
	for _, c := range candidates {
		id, name := c[0], c[1]
		if id == ref {
			return id, nil
		}
		if name != "" && name == ref {
			byName = append(byName, id)
		}
		if strings.HasPrefix(strings.ToLower(id), lower) {
			byPrefix = append(byPrefix, id)
		}
	}

	for _, matches := range [][]string{byName, byPrefix} {
		switch len(matches) {
		case 0:
			continue
		case 1:
			return matches[0], nil
		default:
			return "", mutationErrorf(MutationInvalid, "%s %q is ambiguous — it matches %s; use the full id", kind, ref, joinIDs(matches))
		}
	}
	return "", mutationErrorf(MutationNotFound, "no %s matches %q — known %ss are %s", kind, ref, kind, describeCandidates(candidates))
}

func joinIDs(ids []string) string {
	sorted := append([]string(nil), ids...)
	sort.Strings(sorted)
	return strings.Join(sorted, ", ")
}

func describeCandidates(candidates [][2]string) string {
	out := make([]string, 0, len(candidates))
	for _, c := range candidates {
		if c[1] == "" {
			out = append(out, c[0])
			continue
		}
		out = append(out, fmt.Sprintf("%s (%s)", c[1], c[0]))
	}
	sort.Strings(out)
	return strings.Join(out, ", ")
}

// resolvePeerRef resolves a peer ref — hex key, display name, or unique key
// prefix — to the configured peer it names.
func (n *Node) resolvePeerRef(ref string) (*peerConn, error) {
	peers := n.snapshotPeers()
	candidates := make([][2]string, 0, len(peers))
	byID := make(map[string]*peerConn, len(peers))
	for _, pc := range peers {
		pc.mu.Lock()
		name := pc.name
		pc.mu.Unlock()
		candidates = append(candidates, [2]string{pc.peerKeyHex, name})
		byID[pc.peerKeyHex] = pc
	}
	id, err := matchRef("peer", ref, candidates)
	if err != nil {
		return nil, err
	}
	return byID[id], nil
}

// resolveOfferedShareRef resolves a share ref against what pc most recently
// offered us.
//
// A peer that has never sent a ShareList (not connected since this process
// started — remoteShares is in-memory only) leaves us nothing to resolve
// against, so the ref is passed through as a literal share id. Subscribing
// to a peer before ever connecting to it is legitimate, and refusing it
// outright would be worse than taking the id on faith.
func (n *Node) resolveOfferedShareRef(pc *peerConn, ref string) (string, error) {
	offered := pc.offeredShares()
	if len(offered) == 0 {
		return ref, nil
	}
	candidates := make([][2]string, 0, len(offered))
	for _, e := range offered {
		candidates = append(candidates, [2]string{e.ShareID, e.Name})
	}
	id, err := matchRef("share", ref, candidates)
	if err != nil {
		pc.mu.Lock()
		name := pc.name
		pc.mu.Unlock()
		return "", fmt.Errorf("peer %s: %w", name, err)
	}
	return id, nil
}

// resolveSubscriptionRef resolves the (peer, share) pair naming an existing
// subscription, for RemoveSubscription and PauseSubscription.
//
// Deliberately best-effort: whatever it cannot resolve it passes through
// unchanged, so the caller's own "is not configured" error is what the user
// sees. That matters most for the case resolution would otherwise make
// unfixable — a subscription whose peer is no longer configured, which is
// precisely the state a bad `subscribe` used to leave behind. Those have to
// stay removable by their literal stored values.
func (n *Node) resolveSubscriptionRef(peerRef, shareRef string) (peerKeyHex, shareID string) {
	peerKeyHex, shareID = peerRef, shareRef

	pc, err := n.resolvePeerRef(peerRef)
	if err != nil {
		return peerKeyHex, shareID
	}
	peerKeyHex = pc.peerKeyHex

	// Prefer the peer's offered names when we have them, but fall back to
	// the share ids actually recorded in config so an offline peer's
	// subscriptions can still be addressed by id or id prefix.
	names := map[string]string{}
	for _, e := range pc.offeredShares() {
		names[e.ShareID] = e.Name
	}
	n.cfgMu.RLock()
	var candidates [][2]string
	for _, s := range n.cfg.Subscriptions {
		if s.Peer == peerKeyHex {
			candidates = append(candidates, [2]string{s.ShareID, names[s.ShareID]})
		}
	}
	n.cfgMu.RUnlock()

	if id, err := matchRef("share", shareRef, candidates); err == nil {
		shareID = id
	}
	return peerKeyHex, shareID
}

// resolveLocalShareRef resolves a share ref against the shares this node
// offers.
func (n *Node) resolveLocalShareRef(ref string) (string, error) {
	n.cfgMu.RLock()
	candidates := make([][2]string, 0, len(n.cfg.Shares))
	for _, s := range n.cfg.Shares {
		candidates = append(candidates, [2]string{s.ID, s.Name})
	}
	n.cfgMu.RUnlock()
	return matchRef("share", ref, candidates)
}

// resolveShareOrSubscriptionRef resolves a share ref against everything this
// node holds a local copy of: the shares it offers plus the shares it
// subscribes to from peers. That union is exactly what the trash is keyed
// by (see shareOrSubscriptionRoot), since files are trashed on whichever
// side deleted them.
//
// A subscription's display name comes from the offering peer's last
// ShareList, so it is only known while that peer has been connected during
// this run. A subscription with no name yet is still matchable by id or id
// prefix — it simply contributes no name to match against.
func (n *Node) resolveShareOrSubscriptionRef(ref string) (string, error) {
	type subRef struct{ peerKeyHex, shareID string }

	n.cfgMu.RLock()
	candidates := make([][2]string, 0, len(n.cfg.Shares)+len(n.cfg.Subscriptions))
	for _, s := range n.cfg.Shares {
		candidates = append(candidates, [2]string{s.ID, s.Name})
	}
	subs := make([]subRef, 0, len(n.cfg.Subscriptions))
	for _, s := range n.cfg.Subscriptions {
		subs = append(subs, subRef{s.Peer, s.ShareID})
	}
	n.cfgMu.RUnlock()

	// Names are gathered after releasing cfgMu: offeredShares takes the
	// peerConn lock, and nesting the two would invert the order every
	// other path here uses.
	for _, s := range subs {
		var name string
		if pc := n.lookupPeer(s.peerKeyHex); pc != nil {
			for _, e := range pc.offeredShares() {
				if e.ShareID == s.shareID {
					name = e.Name
					break
				}
			}
		}
		candidates = append(candidates, [2]string{s.shareID, name})
	}
	return matchRef("share", ref, candidates)
}

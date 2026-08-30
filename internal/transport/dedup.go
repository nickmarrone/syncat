package transport

import (
	"bytes"
	"crypto/ed25519"
)

// KeepConnection reports, per SPEC.md §2.4, whether a connection should
// survive when a duplicate exists: if both sides of a peering dial each
// other successfully, both keep the connection dialed by the node with the
// lexicographically higher Ed25519 public key and close the other.
//
// local and peer are the two endpoints' application identity keys (see
// internal/config.IdentityKey), and dialed reports whether the connection
// in question is the one the local node dialed (outbound) as opposed to
// the one it accepted (inbound). Applying the rule needs the peer identity
// that the protocol handshake establishes (Phase 3), so this function only
// exposes the decision itself; Phase 7's peer manager calls it once both
// connections to a peer are authenticated.
//
// Both ends of a duplicate pair call this with dialed flipped — one side's
// outbound connection is the other side's inbound connection — and always
// agree on which single connection survives: for connection C dialed by
// node D and accepted by node O, D calls KeepConnection(D.key, O.key,
// true) and O calls KeepConnection(O.key, D.key, false); both evaluate to
// bytes.Compare(D.key, O.key) > 0, so they reach the same answer about C
// without coordinating. (Equal keys can't arise between distinct peers in
// practice; KeepConnection still returns a well-defined, self-consistent
// answer for them — the "peer" with the equal key is treated as not
// higher — it just isn't a meaningful dedup decision.)
func KeepConnection(local, peer ed25519.PublicKey, dialed bool) bool {
	localIsHigher := bytes.Compare(local, peer) > 0
	return dialed == localIsHigher
}

package protocol

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/nickmarrone/syncat/internal/config"
)

// CurrentProtoVersion is the syncat wire protocol version this build
// speaks (SPEC.md §4).
const CurrentProtoVersion = 1

// DefaultHandshakeTimeout bounds the whole handshake (SPEC.md §4): a peer
// that opens a connection and says nothing must not pin a goroutine
// forever.
const DefaultHandshakeTimeout = 30 * time.Second

// nonceSize is the required length of Hello.Nonce (SPEC.md §4).
const nonceSize = 32

// authContext domain-separates the handshake signature from any other use
// of the node's Ed25519 identity key, and from any other version of this
// protocol that might reuse the same key.
const authContext = "syncat-auth-v1"

// HandshakeConfig parameterizes one [Handshake] run.
type HandshakeConfig struct {
	// IdentityKey is this node's Ed25519 application identity keypair
	// (config.IdentityKey.Private).
	IdentityKey ed25519.PrivateKey

	// NodeName is this node's display name, sent in Hello.
	NodeName string

	// Token is this node's own sc1 node token (SPEC.md §2.3). It is sent
	// in Hello so that, once the pending-peer approval queue exists,
	// approving a pending peer alone suffices to complete peering. Its
	// embedded id must equal IdentityKey's public key, or the peer will
	// reject us (and we reject ourselves before ever writing it — see
	// [Handshake]).
	Token string

	// OurVersion is the proto_version this side offers in its Hello.
	// MinVersion is the lowest negotiated version this side is still
	// willing to accept. The negotiated version is min(OurVersion,
	// peer's proto_version); if that is below MinVersion, the handshake
	// is rejected with ErrCodeUnsupportedVersion (SPEC.md §4: "use
	// min(theirs, mine) if supported, else close with Error"). Both
	// default to CurrentProtoVersion, which is this phase's only
	// supported version.
	OurVersion int
	MinVersion int

	// IsKnownPeer reports whether pub is an authorized peer for this
	// connection. When dialing a specific configured peer, this is
	// typically "pub equals the key I dialed"; when accepting an inbound
	// connection, it's typically "pub is in my configured-peers set". An
	// unknown key is a hard rejection — SPEC.md §2.3's pending-peer
	// approval queue (recording an unknown inbound key for later
	// approval, rather than closing outright) is deferred past this
	// phase; both sides must already have pasted tokens.
	IsKnownPeer func(pub ed25519.PublicKey) bool

	// Timeout bounds the whole handshake; defaults to
	// DefaultHandshakeTimeout if <= 0.
	Timeout time.Duration
}

// HandshakeResult describes the peer authenticated by a successful
// [Handshake], for Phase 7's peer manager to use with
// transport.KeepConnection and the rest of the peering flow.
type HandshakeResult struct {
	PeerPub      ed25519.PublicKey
	PeerName     string
	PeerToken    string
	ProtoVersion int
}

// Handshake runs the mutually-authenticated syncat handshake (SPEC.md §4)
// over conn:
//
//  1. Both sides immediately send Hello (this call sends ours in a
//     background goroutine while concurrently reading the peer's, so
//     neither side blocks waiting for the other to go first).
//  2. Each side validates the peer's Hello (proto_version negotiation,
//     well-formed ed25519_pub, token consistency, known-peer check) and,
//     if that passes, sends Auth: a signature over a nonce transcript
//     that binds both sides' nonces in a fixed, direction-dependent order
//     (see the comment on the signing step below — this is what makes a
//     reflected/replayed Auth fail verification).
//  3. Each side verifies the peer's Auth signature against the peer's
//     claimed public key.
//
// On any validation failure this side sends an Error frame (best-effort;
// its own failure is ignored) and returns a non-nil error; the caller
// owns conn and must close it. On success, both sides have authenticated
// each other and HandshakeResult describes the peer.
//
// The whole call is bounded by cfg.Timeout (default
// DefaultHandshakeTimeout) and by ctx: conn's deadline is set for the
// duration of the call, and a background goroutine forces it to expire
// immediately if ctx is canceled first. The deadline is left expired (not
// cleared) on return; callers that keep conn afterward should call
// conn.SetDeadline(time.Time{}) themselves.
func Handshake(ctx context.Context, conn net.Conn, cfg HandshakeConfig) (*HandshakeResult, error) {
	if len(cfg.IdentityKey) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("protocol: handshake: identity key must be %d bytes, got %d", ed25519.PrivateKeySize, len(cfg.IdentityKey))
	}
	if cfg.IsKnownPeer == nil {
		return nil, errors.New("protocol: handshake: IsKnownPeer must not be nil")
	}
	ourVersion := cfg.OurVersion
	if ourVersion <= 0 {
		ourVersion = CurrentProtoVersion
	}
	minVersion := cfg.MinVersion
	if minVersion <= 0 {
		minVersion = CurrentProtoVersion
	}

	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = DefaultHandshakeTimeout
	}
	deadline := time.Now().Add(timeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return nil, fmt.Errorf("protocol: handshake: set deadline: %w", err)
	}

	// If ctx is canceled before the deadline above would fire on its own,
	// force any Read/Write already blocked (or about to block) to return
	// promptly by pulling the deadline in to "now". The watcher goroutine
	// always exits when Handshake returns, via stop.
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.SetDeadline(time.Now())
		case <-stop:
		}
	}()

	ourPub := cfg.IdentityKey.Public().(ed25519.PublicKey)
	ourNonce := make([]byte, nonceSize)
	if _, err := rand.Read(ourNonce); err != nil {
		return nil, fmt.Errorf("protocol: handshake: generate nonce: %w", err)
	}

	// A Token whose embedded id doesn't match our own key is a local
	// misconfiguration, not something the peer needs to tell us about;
	// catch it before ever writing a Hello we know is inconsistent.
	if ourTok, err := config.ParseToken(cfg.Token); err != nil {
		return nil, fmt.Errorf("protocol: handshake: our own token is invalid: %w", err)
	} else if !bytes.Equal(ourTok.ID, ourPub) {
		return nil, errors.New("protocol: handshake: our own token id does not match our identity key")
	}

	fw := NewWriter(conn)
	fr := NewReader(conn)

	ourHello := Hello{
		ProtoVersion: ourVersion,
		NodeName:     cfg.NodeName,
		Ed25519Pub:   []byte(ourPub),
		Token:        cfg.Token,
		Nonce:        ourNonce,
	}

	peerHello, err := exchangeHello(fw, fr, ourHello)
	if err != nil {
		return nil, err
	}

	if err := validateHelloShape(peerHello); err != nil {
		sendError(fw, ErrCodeBadHello, err.Error())
		return nil, fmt.Errorf("protocol: handshake: peer hello: %w", err)
	}

	negotiated, err := negotiateVersion(ourVersion, minVersion, peerHello.ProtoVersion)
	if err != nil {
		sendError(fw, ErrCodeUnsupportedVersion, err.Error())
		return nil, fmt.Errorf("protocol: handshake: %w", err)
	}

	peerPub := ed25519.PublicKey(peerHello.Ed25519Pub)
	if !cfg.IsKnownPeer(peerPub) {
		sendError(fw, ErrCodeUnauthorized, "peer key is not a configured peer")
		return nil, fmt.Errorf("protocol: handshake: unknown peer key %x", peerPub)
	}

	// Sign authContext || their_nonce || our_nonce ("their" and "our" from
	// this side's point of view). The peer verifies this same byte string
	// by recomputing it with its own labels swapped (its "our_nonce" is
	// the nonce it generated, which is the same value we call
	// peerHello.Nonce below when verifying its Auth) — see the comment
	// just before the Verify call for why this fixed, swapped ordering is
	// what defeats a reflected/replayed signature.
	ourSig := ed25519.Sign(cfg.IdentityKey, authTranscript(peerHello.Nonce, ourNonce))

	peerAuth, err := exchangeAuth(fw, fr, Auth{Sig: ourSig})
	if err != nil {
		return nil, err
	}

	// Verify against authContext || our_nonce || their_nonce: the mirror
	// image of what we signed above. This asymmetry (each side puts the
	// *other* side's nonce first) is what makes reflection fail: if a
	// peer merely echoes back a signature it observed from us on this
	// same connection — whether that's genuinely our Auth, or an
	// adversary who has no private key of their own and is hoping we'll
	// accept our own words as theirs — the transcript it was actually
	// produced over (their_nonce||our_nonce, from the signer's
	// perspective) does not match the transcript we require here
	// (our_nonce||their_nonce), because the two nonces are independent
	// 32-byte crypto/rand values and so essentially never equal. The
	// signature only verifies if it was produced, over exactly this
	// transcript, by the private key matching peerPub.
	if !ed25519.Verify(peerPub, authTranscript(ourNonce, peerHello.Nonce), peerAuth.Sig) {
		sendError(fw, ErrCodeBadAuth, "signature verification failed")
		return nil, errors.New("protocol: handshake: peer signature verification failed")
	}

	return &HandshakeResult{
		PeerPub:      peerPub,
		PeerName:     peerHello.NodeName,
		PeerToken:    peerHello.Token,
		ProtoVersion: negotiated,
	}, nil
}

// authTranscript builds the fixed-order byte string that gets signed and
// verified: the domain-separation context, then theirNonce, then
// ourNonce, from the caller's point of view (see the two call sites in
// Handshake for exactly which nonce is which on each side).
func authTranscript(theirNonce, ourNonce []byte) []byte {
	buf := make([]byte, 0, len(authContext)+len(theirNonce)+len(ourNonce))
	buf = append(buf, authContext...)
	buf = append(buf, theirNonce...)
	buf = append(buf, ourNonce...)
	return buf
}

// negotiateVersion implements SPEC.md §4's "use min(theirs, mine) if
// supported, else close with Error".
func negotiateVersion(our, minVer, their int) (int, error) {
	if their < 1 {
		return 0, fmt.Errorf("peer proto_version %d is invalid", their)
	}
	negotiated := their
	if our < negotiated {
		negotiated = our
	}
	if negotiated < minVer {
		return 0, fmt.Errorf("no common proto_version (ours %d, min %d, theirs %d)", our, minVer, their)
	}
	return negotiated, nil
}

// validateHelloShape checks the structural requirements SPEC.md §4 and
// this phase's hardening obligations place on a received Hello,
// independent of whether the sender turns out to be a known peer:
//   - ed25519_pub is exactly 32 bytes and not all-zero.
//   - nonce is exactly 32 bytes.
//   - token parses as a valid sc1 token whose embedded id matches
//     ed25519_pub.
func validateHelloShape(h Hello) error {
	if len(h.Ed25519Pub) != ed25519.PublicKeySize {
		return fmt.Errorf("ed25519_pub is %d bytes, want %d", len(h.Ed25519Pub), ed25519.PublicKeySize)
	}
	if isAllZero(h.Ed25519Pub) {
		return errors.New("ed25519_pub is all-zero")
	}
	if len(h.Nonce) != nonceSize {
		return fmt.Errorf("nonce is %d bytes, want %d", len(h.Nonce), nonceSize)
	}
	tok, err := config.ParseToken(h.Token)
	if err != nil {
		return fmt.Errorf("invalid token: %w", err)
	}
	if !bytes.Equal(tok.ID, h.Ed25519Pub) {
		return errors.New("token id does not match ed25519_pub")
	}
	return nil
}

func isAllZero(b []byte) bool {
	for _, x := range b {
		if x != 0 {
			return false
		}
	}
	return true
}

// exchangeHello writes ours in a goroutine (so we never block waiting for
// the peer to send first, matching SPEC.md §4's "both sides immediately
// send Hello") while reading the peer's Hello on the calling goroutine,
// then joins the write.
func exchangeHello(fw *Writer, fr *Reader, ours Hello) (Hello, error) {
	writeErrCh := make(chan error, 1)
	go func() { writeErrCh <- fw.WriteMessage(MsgHello, ours) }()

	payload, readErr := readExpected(fr, MsgHello)

	if writeErr := <-writeErrCh; writeErr != nil {
		if readErr == nil {
			readErr = fmt.Errorf("protocol: handshake: send hello: %w", writeErr)
		}
	}
	if readErr != nil {
		return Hello{}, readErr
	}

	var peer Hello
	if err := DecodeMessage(payload, &peer); err != nil {
		return Hello{}, fmt.Errorf("protocol: handshake: decode peer hello: %w", err)
	}
	return peer, nil
}

// exchangeAuth is exchangeHello's counterpart for the Auth message.
func exchangeAuth(fw *Writer, fr *Reader, ours Auth) (Auth, error) {
	writeErrCh := make(chan error, 1)
	go func() { writeErrCh <- fw.WriteMessage(MsgAuth, ours) }()

	payload, readErr := readExpected(fr, MsgAuth)

	if writeErr := <-writeErrCh; writeErr != nil {
		if readErr == nil {
			readErr = fmt.Errorf("protocol: handshake: send auth: %w", writeErr)
		}
	}
	if readErr != nil {
		return Auth{}, readErr
	}

	var peer Auth
	if err := DecodeMessage(payload, &peer); err != nil {
		return Auth{}, fmt.Errorf("protocol: handshake: decode peer auth: %w", err)
	}
	return peer, nil
}

// readExpected reads one frame and returns its payload if it has type
// want. A frame of type MsgError is decoded and returned as a *RemoteError
// instead of a generic "wrong type" error, since that's a more useful
// signal to the caller (the peer rejected us and told us why).
func readExpected(fr *Reader, want MsgType) ([]byte, error) {
	typ, payload, err := fr.ReadFrame()
	if err != nil {
		return nil, fmt.Errorf("protocol: handshake: read %s: %w", want, err)
	}
	if typ == MsgError {
		var e Error
		if decErr := DecodeMessage(payload, &e); decErr == nil {
			return nil, &RemoteError{Code: e.Code, Msg: e.Msg}
		}
		return nil, fmt.Errorf("protocol: handshake: peer sent an undecodable Error frame")
	}
	if typ != want {
		return nil, fmt.Errorf("protocol: handshake: expected %s, got %s", want, typ)
	}
	return payload, nil
}

// sendError best-effort notifies the peer why we're about to close the
// connection. The caller is closing (or the caller's caller is) either
// way, so a failure here is not itself a reportable error — the
// connection may already be broken, which is exactly the sort of
// situation that leads to wanting to send an Error in the first place.
func sendError(fw *Writer, code, msg string) {
	_ = fw.WriteMessage(MsgError, Error{Code: code, Msg: msg})
}

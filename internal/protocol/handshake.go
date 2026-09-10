// handshake.go: the mutually-authenticated Ed25519 handshake (SPEC.md §4)
// that every connection runs before a session starts. The keepalive timer
// that follows lives in keepalive.go.

package protocol

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/nickmarrone/syncat/internal/config"
)

// CurrentProtoVersion is the syncat wire protocol version this build
// speaks (SPEC.md §4).
const CurrentProtoVersion = 2

// DefaultHandshakeTimeout bounds the whole handshake (SPEC.md §4): a peer
// that opens a connection and says nothing must not pin a goroutine
// forever.
const DefaultHandshakeTimeout = 30 * time.Second

// nonceSize is the required length of Hello.Nonce (SPEC.md §4).
const nonceSize = 32

// authContext domain-separates the handshake signature from any other use
// of the node's Ed25519 identity key, and from any other version of this
// protocol that might reuse the same key.
const authContext = "syncat-handshake-v2"

// --- the mutually-authenticated handshake (SPEC.md §4) -----------------

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
	// default to CurrentProtoVersion, which is this build's only
	// supported version.
	OurVersion int
	MinVersion int

	// IsKnownPeer reports whether pub is an authorized peer for this
	// connection. When dialing a specific configured peer, this is
	// typically "pub equals the key I dialed"; when accepting an inbound
	// connection, it's typically "pub is in my configured-peers set". An
	// unknown key is a hard rejection — SPEC.md §2.3's pending-peer
	// approval queue (recording an unknown inbound key for later
	// approval, rather than closing outright) is not implemented; both
	// sides must already have pasted tokens.
	IsKnownPeer func(pub ed25519.PublicKey) bool

	// Timeout bounds the whole handshake; defaults to
	// DefaultHandshakeTimeout if <= 0.
	Timeout time.Duration
}

// HandshakeResult describes the peer authenticated by a successful
// [Handshake], for internal/core's peer manager to use with
// transport.KeepConnection and the rest of the peering flow.
type HandshakeResult struct {
	PeerPub      ed25519.PublicKey
	PeerName     string
	PeerToken    string
	ProtoVersion int
}

// The v2 handshake is role ordered. The initiator sends Hello; the
// responder returns one atomic HelloAuth; the initiator returns Auth; and
// the responder confirms it with Finished. Every write therefore has a
// reader already waiting, including over an unbuffered net.Pipe.
//
// On any validation failure this side sends an Error frame (best-effort;
// its own failure is ignored) and returns a non-nil error; the caller
// owns conn and must close it. On success, both sides have authenticated
// each other and HandshakeResult describes the peer.
//
// The whole call is bounded by cfg.Timeout (default
// DefaultHandshakeTimeout) and by ctx: conn's deadline is set for the
// duration of the call, and a background goroutine forces it to expire
// immediately if ctx is canceled first.
//
// On success the deadline is cleared before returning, so the caller gets
// conn back exactly as it handed it over. This is not merely tidy: the
// caller keeps conn for the whole life of the session that follows, and
// the deadline set here is an *absolute* time. Leaving it armed silently
// poisons the connection at handshakeStart+Timeout — every later Read and
// Write fails with os.ErrDeadlineExceeded even though the connection is
// perfectly healthy, which reads as a peer that connects, goes quiet, and
// gets torn down by SPEC.md §4's 90s dead rule, over and over.
//
// On failure the deadline is left as-is; the caller owns conn and must
// close it.
func InitiateHandshake(ctx context.Context, conn net.Conn, cfg HandshakeConfig) (*HandshakeResult, error) {
	return handshake(ctx, conn, cfg, true)
}

func AcceptHandshake(ctx context.Context, conn net.Conn, cfg HandshakeConfig) (*HandshakeResult, error) {
	return handshake(ctx, conn, cfg, false)
}

// Handshake is retained as the initiator entry point for source compatibility.
// New code should use InitiateHandshake or AcceptHandshake explicitly.
func Handshake(ctx context.Context, conn net.Conn, cfg HandshakeConfig) (*HandshakeResult, error) {
	return InitiateHandshake(ctx, conn, cfg)
}

func handshake(ctx context.Context, conn net.Conn, cfg HandshakeConfig, initiator bool) (*HandshakeResult, error) {
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

	stopWatcher, err := armHandshakeDeadline(ctx, conn, cfg.Timeout)
	if err != nil {
		return nil, err
	}
	defer stopWatcher()

	ourPub := cfg.IdentityKey.Public().(ed25519.PublicKey)
	ourNonce := make([]byte, nonceSize)
	if _, err := rand.Read(ourNonce); err != nil {
		return nil, fmt.Errorf("protocol: handshake: generate nonce: %w", err)
	}
	if err := checkOwnToken(cfg.Token, ourPub); err != nil {
		return nil, err
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

	var peerHello Hello
	var deferredServerSig []byte
	if initiator {
		if err := fw.WriteMessage(MsgHello, ourHello); err != nil {
			return nil, fmt.Errorf("protocol: handshake: send hello: %w", err)
		}
		var response HelloAuth
		if err := readMessage(fr, MsgAuth, &response); err != nil {
			return nil, err
		}
		peerHello = response.Hello
		// Retain the proof until validation establishes the claimed key.
		deferredServerSig = response.Sig
	} else {
		if err := readMessage(fr, MsgHello, &peerHello); err != nil {
			return nil, err
		}
	}

	// Validate before sending our identity. On rejection the initiator is
	// waiting for a response, so this Error write always has a reader.
	if err := validatePeerHello(cfg, ourVersion, minVersion, peerHello); err != nil {
		sendError(conn, fw, errorCode(err), err.Error())
		return nil, fmt.Errorf("protocol: handshake: peer hello: %w", err)
	}
	negotiated, _ := negotiateVersion(ourVersion, minVersion, peerHello.ProtoVersion)
	peerPub := ed25519.PublicKey(peerHello.Ed25519Pub)

	clientHello, serverHello := ourHello, peerHello
	if !initiator {
		clientHello, serverHello = peerHello, ourHello
	}
	proof := handshakeTranscript(clientHello, serverHello, negotiated)

	if initiator {
		if !ed25519.Verify(peerPub, appendRole(proof, "server"), deferredServerSig) {
			sendError(conn, fw, ErrCodeBadAuth, "server signature verification failed")
			return nil, errors.New("protocol: handshake: peer signature verification failed")
		}
		ourSig := ed25519.Sign(cfg.IdentityKey, appendRole(proof, "client"))
		if err := fw.WriteMessage(MsgAuth, Auth{Sig: ourSig}); err != nil {
			return nil, fmt.Errorf("protocol: handshake: send auth: %w", err)
		}
		var finished Finished
		if err := readMessage(fr, MsgFinished, &finished); err != nil {
			return nil, err
		}
	} else {
		ourSig := ed25519.Sign(cfg.IdentityKey, appendRole(proof, "server"))
		if err := fw.WriteMessage(MsgAuth, HelloAuth{Hello: ourHello, Sig: ourSig}); err != nil {
			return nil, fmt.Errorf("protocol: handshake: send hello auth: %w", err)
		}
		var clientAuth Auth
		if err := readMessage(fr, MsgAuth, &clientAuth); err != nil {
			return nil, err
		}
		if !ed25519.Verify(peerPub, appendRole(proof, "client"), clientAuth.Sig) {
			sendError(conn, fw, ErrCodeBadAuth, "client signature verification failed")
			return nil, errors.New("protocol: handshake: peer signature verification failed")
		}
		if err := fw.WriteMessage(MsgFinished, Finished{}); err != nil {
			return nil, fmt.Errorf("protocol: handshake: send finished: %w", err)
		}
	}

	// Authenticated. Retire the ctx watcher and disarm the deadline before
	// handing conn back — see this func's doc comment. Ordering matters:
	// stopping the watcher first keeps it from re-arming the deadline
	// behind us. (A cancel that has already been observed can still land
	// after this, but that only happens when the node is shutting down and
	// the session is being torn down anyway.)
	stopWatcher()
	if err := conn.SetDeadline(time.Time{}); err != nil {
		return nil, fmt.Errorf("protocol: handshake: clear deadline: %w", err)
	}

	return &HandshakeResult{
		PeerPub:      peerPub,
		PeerName:     peerHello.NodeName,
		PeerToken:    peerHello.Token,
		ProtoVersion: negotiated,
	}, nil
}

// armHandshakeDeadline bounds the handshake on conn by timeout
// (DefaultHandshakeTimeout if <= 0) or ctx's own deadline, whichever comes
// first, and starts a goroutine that pulls the deadline in to "now" if ctx
// is canceled before then. It returns stopWatcher, which retires that
// goroutine; it is idempotent and must only be called from the goroutine
// running [Handshake]. The deadline itself is left armed — clearing it (or
// not, on failure) is the caller's decision.
func armHandshakeDeadline(ctx context.Context, conn net.Conn, timeout time.Duration) (stopWatcher func(), err error) {
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
	stopped := false
	// Only ever called on Handshake's own goroutine, so the bool needs no
	// synchronisation.
	stopWatcher = func() {
		if !stopped {
			stopped = true
			close(stop)
		}
	}
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.SetDeadline(time.Now())
		case <-stop:
		}
	}()
	return stopWatcher, nil
}

// checkOwnToken confirms that token (this node's own sc1 token) parses
// and embeds ourPub. A Token whose embedded id doesn't match our own key
// is a local misconfiguration, not something the peer needs to tell us
// about; catch it before ever writing a Hello we know is inconsistent.
func checkOwnToken(token string, ourPub ed25519.PublicKey) error {
	ourTok, err := config.ParseToken(token)
	if err != nil {
		return fmt.Errorf("protocol: handshake: our own token is invalid: %w", err)
	}
	if !bytes.Equal(ourTok.ID, ourPub) {
		return errors.New("protocol: handshake: our own token id does not match our identity key")
	}
	return nil
}

func validatePeerHello(cfg HandshakeConfig, ourVersion, minVersion int, h Hello) error {
	if err := validateHelloShape(h); err != nil {
		return err
	}
	if _, err := negotiateVersion(ourVersion, minVersion, h.ProtoVersion); err != nil {
		return err
	}
	if !cfg.IsKnownPeer(ed25519.PublicKey(h.Ed25519Pub)) {
		return fmt.Errorf("unknown peer key %x", h.Ed25519Pub)
	}
	return nil
}

func errorCode(err error) string {
	s := err.Error()
	switch {
	case strings.Contains(s, "proto_version"):
		return ErrCodeUnsupportedVersion
	case strings.Contains(s, "unknown peer key"):
		return ErrCodeUnauthorized
	default:
		return ErrCodeBadHello
	}
}

func handshakeTranscript(client, server Hello, negotiated int) []byte {
	c, _ := encodeMessage(MsgHello, client)
	s, _ := encodeMessage(MsgHello, server)
	h := sha256.New()
	h.Write([]byte(authContext))
	for _, part := range [][]byte{c, s} {
		var n [4]byte
		binary.BigEndian.PutUint32(n[:], uint32(len(part)))
		h.Write(n[:])
		h.Write(part)
	}
	var v [4]byte
	binary.BigEndian.PutUint32(v[:], uint32(negotiated))
	h.Write(v[:])
	return h.Sum(nil)
}

func appendRole(transcript []byte, role string) []byte {
	b := make([]byte, 0, len(authContext)+len(role)+len(transcript)+2)
	b = append(b, authContext...)
	b = append(b, 0, byte(len(role)))
	b = append(b, role...)
	b = append(b, transcript...)
	return b
}

func readMessage(fr *Reader, want MsgType, dst any) error {
	payload, err := readExpected(fr, want)
	if err != nil {
		return err
	}
	if err := DecodeMessage(payload, dst); err != nil {
		return fmt.Errorf("protocol: handshake: decode %s: %w", want, err)
	}
	return nil
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
// SPEC.md §13.9's hardening obligations place on a received Hello,
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

// RemoteError wraps an Error message received from the peer, so callers
// can distinguish "the peer told us why it's closing" from a local
// decode/timeout/IO failure.
type RemoteError struct {
	Code string
	Msg  string
}

func (e *RemoteError) Error() string {
	return fmt.Sprintf("protocol: peer sent error %q: %s", SanitizeDiagnostic(e.Code), SanitizeDiagnostic(e.Msg))
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
func sendError(conn net.Conn, fw *Writer, code, msg string) {
	// Error delivery is diagnostic and must not delay rejection when the
	// peer violates the state machine and is not reading its response.
	_ = conn.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
	_ = fw.WriteMessage(MsgError, Error{Code: code, Msg: msg})
}

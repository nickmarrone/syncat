package protocol

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nickmarrone/syncat/internal/config"
	"github.com/nickmarrone/syncat/internal/transport"
)

// --- test helpers -----------------------------------------------------

var pipeAddrCounter int64

// newPipeConnPair returns two connected net.Conns, as if a dialed b, over
// internal/transport's net.Pipe-backed PipeTransport.
func newPipeConnPair(t *testing.T) (a, b net.Conn) {
	t.Helper()
	ctx := context.Background()
	n := atomic.AddInt64(&pipeAddrCounter, 1)
	addrA := fmt.Sprintf("handshake-test-a-%d", n)
	addrB := fmt.Sprintf("handshake-test-b-%d", n)

	trA := transport.NewPipeTransport(addrA)
	trB := transport.NewPipeTransport(addrB)

	acceptedB := make(chan net.Conn, 1)
	if err := trB.Start(ctx, func(c net.Conn) { acceptedB <- c }); err != nil {
		t.Fatalf("trB.Start: %v", err)
	}
	t.Cleanup(func() { trB.Close() })
	if err := trA.Start(ctx, func(net.Conn) {}); err != nil {
		t.Fatalf("trA.Start: %v", err)
	}
	t.Cleanup(func() { trA.Close() })

	connA, err := trA.Dial(ctx, addrB)
	if err != nil {
		t.Fatalf("trA.Dial: %v", err)
	}
	var connB net.Conn
	select {
	case connB = <-acceptedB:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the pipe accept")
	}
	return connA, connB
}

func newTestIdentity(t *testing.T) (ed25519.PrivateKey, ed25519.PublicKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate identity key: %v", err)
	}
	return priv, pub
}

func mustToken(t *testing.T, pub ed25519.PublicKey, name string) string {
	t.Helper()
	tok, err := config.EncodeToken("tc-fake-conn-blob", pub, name)
	if err != nil {
		t.Fatalf("EncodeToken: %v", err)
	}
	return tok
}

func mustNonce(t *testing.T) []byte {
	t.Helper()
	n := make([]byte, nonceSize)
	if _, err := rand.Read(n); err != nil {
		t.Fatalf("generate nonce: %v", err)
	}
	return n
}

func acceptAll(ed25519.PublicKey) bool { return true }

// runBothHandshakes runs Handshake concurrently on both ends of a
// connected pair — concurrently matters here, not just for speed: each
// side's Handshake blocks reading the other's Hello, so running them
// sequentially would have the first call time out waiting for a peer
// that hasn't sent anything yet, masking whatever behavior the test
// actually wants to observe.
func runBothHandshakes(t *testing.T, connA, connB net.Conn, cfgA, cfgB HandshakeConfig) (errA, errB error) {
	t.Helper()
	chA := make(chan error, 1)
	chB := make(chan error, 1)
	go func() {
		_, err := InitiateHandshake(context.Background(), connA, cfgA)
		chA <- err
	}()
	go func() {
		_, err := AcceptHandshake(context.Background(), connB, cfgB)
		chB <- err
	}()
	select {
	case errA = <-chA:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for A's handshake")
	}
	select {
	case errB = <-chB:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for B's handshake")
	}
	return errA, errB
}

// readFrame reads one frame or fails the test, with a bounded wait so a
// hung handshake under test doesn't hang the test suite too.
func readFrame(t *testing.T, fr *Reader) (MsgType, []byte) {
	t.Helper()
	type result struct {
		typ     MsgType
		payload []byte
		err     error
	}
	ch := make(chan result, 1)
	go func() {
		typ, payload, err := fr.ReadFrame()
		ch <- result{typ, payload, err}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("ReadFrame: %v", r.err)
		}
		return r.typ, r.payload
	case <-time.After(5 * time.Second):
		t.Fatal("timed out reading a frame")
		return 0, nil
	}
}

// --- happy path ---------------------------------------------------------

func TestHandshakeHappyPath(t *testing.T) {
	connA, connB := newPipeConnPair(t)
	defer connA.Close()
	defer connB.Close()

	privA, pubA := newTestIdentity(t)
	privB, pubB := newTestIdentity(t)
	tokA := mustToken(t, pubA, "alice")
	tokB := mustToken(t, pubB, "bob")

	cfgA := HandshakeConfig{
		IdentityKey: privA,
		NodeName:    "alice",
		Token:       tokA,
		IsKnownPeer: func(p ed25519.PublicKey) bool { return bytes.Equal(p, pubB) },
	}
	cfgB := HandshakeConfig{
		IdentityKey: privB,
		NodeName:    "bob",
		Token:       tokB,
		IsKnownPeer: func(p ed25519.PublicKey) bool { return bytes.Equal(p, pubA) },
	}

	type outcome struct {
		res *HandshakeResult
		err error
	}
	chA := make(chan outcome, 1)
	chB := make(chan outcome, 1)
	go func() {
		res, err := InitiateHandshake(context.Background(), connA, cfgA)
		chA <- outcome{res, err}
	}()
	go func() {
		res, err := AcceptHandshake(context.Background(), connB, cfgB)
		chB <- outcome{res, err}
	}()

	var outA, outB outcome
	select {
	case outA = <-chA:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for A's handshake")
	}
	select {
	case outB = <-chB:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for B's handshake")
	}

	if outA.err != nil {
		t.Fatalf("A's handshake failed: %v", outA.err)
	}
	if outB.err != nil {
		t.Fatalf("B's handshake failed: %v", outB.err)
	}

	if !bytes.Equal(outA.res.PeerPub, pubB) {
		t.Errorf("A sees peer pub %x, want %x", outA.res.PeerPub, pubB)
	}
	if outA.res.PeerName != "bob" {
		t.Errorf("A sees peer name %q, want %q", outA.res.PeerName, "bob")
	}
	if outA.res.PeerToken != tokB {
		t.Errorf("A sees peer token %q, want %q", outA.res.PeerToken, tokB)
	}
	if outA.res.ProtoVersion != CurrentProtoVersion {
		t.Errorf("A negotiated version %d, want %d", outA.res.ProtoVersion, CurrentProtoVersion)
	}

	if !bytes.Equal(outB.res.PeerPub, pubA) {
		t.Errorf("B sees peer pub %x, want %x", outB.res.PeerPub, pubA)
	}
	if outB.res.PeerName != "alice" {
		t.Errorf("B sees peer name %q, want %q", outB.res.PeerName, "alice")
	}
	if outB.res.ProtoVersion != CurrentProtoVersion {
		t.Errorf("B negotiated version %d, want %d", outB.res.ProtoVersion, CurrentProtoVersion)
	}
}

// --- rejections -----------------------------------------------------------

// TestHandshakeRejectsForgedSignature: a peer with a real, distinct
// identity sends a well-formed Hello but a random (not actually produced
// by its private key) Auth signature.
func TestHandshakeRejectsForgedSignature(t *testing.T) {
	connA, connB := newPipeConnPair(t)
	defer connA.Close()
	defer connB.Close()

	privA, pubA := newTestIdentity(t)
	tokA := mustToken(t, pubA, "alice")
	_, pubM := newTestIdentity(t) // Mallory's real pubkey; we deliberately never use a real signature from her.
	tokM := mustToken(t, pubM, "mallory")

	cfgA := HandshakeConfig{
		IdentityKey: privA,
		NodeName:    "alice",
		Token:       tokA,
		IsKnownPeer: func(p ed25519.PublicKey) bool { return bytes.Equal(p, pubM) },
		Timeout:     5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		_, err := AcceptHandshake(context.Background(), connA, cfgA)
		errCh <- err
	}()

	fw := NewWriter(connB)
	fr := NewReader(connB)

	nonceM := mustNonce(t)
	if err := fw.WriteMessage(MsgHello, Hello{ProtoVersion: CurrentProtoVersion, NodeName: "mallory", Ed25519Pub: pubM, Token: tokM, Nonce: nonceM}); err != nil {
		t.Fatalf("write hello: %v", err)
	}

	readFrame(t, fr) // drain A's HelloAuth

	garbage := make([]byte, ed25519.SignatureSize)
	if _, err := rand.Read(garbage); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	if err := fw.WriteMessage(MsgAuth, Auth{Sig: garbage}); err != nil {
		t.Fatalf("write auth: %v", err)
	}

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("Handshake accepted a forged signature")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Handshake to reject")
	}
}

// TestHandshakeRejectsReflectedSignature simulates a reflection attack:
// an attacker with no private key of her own connects to Alice, claims
// (in her Hello) to *be* Alice — pubkey and token are not secret, only
// gating connection attempts (SPEC.md §2) — and then, instead of
// producing a signature (which she cannot, lacking Alice's key), simply
// echoes Alice's own just-received Auth signature straight back as if it
// were her own.
//
// This specifically tests the asymmetric (their_nonce||our_nonce vs.
// our_nonce||their_nonce) transcript ordering documented in [Handshake]:
// with a naive symmetric transcript, this exact reflection would verify
// successfully (same key on both sides of ed25519.Verify, and the
// attacker never needed to know it), which is why the ordering has to be
// direction-dependent rather than merely "different peers have different
// keys" (already covered by TestHandshakeRejectsForgedSignature).
func TestHandshakeRejectsReflectedSignature(t *testing.T) {
	connA, connB := newPipeConnPair(t)
	defer connA.Close()
	defer connB.Close()

	privA, pubA := newTestIdentity(t)
	tokA := mustToken(t, pubA, "alice")

	cfgA := HandshakeConfig{
		IdentityKey: privA,
		NodeName:    "alice",
		Token:       tokA,
		// Deliberately treats "a peer asserting my own key" as known, to
		// isolate the crypto property under test from the (separate,
		// sensible) business-logic question of whether that assertion
		// should ever be trusted. The defense against reflection has to
		// hold even here, or it doesn't really hold.
		IsKnownPeer: func(p ed25519.PublicKey) bool { return bytes.Equal(p, pubA) },
		Timeout:     5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		_, err := AcceptHandshake(context.Background(), connA, cfgA)
		errCh <- err
	}()

	fw := NewWriter(connB)
	fr := NewReader(connB)

	claimNonce := mustNonce(t)
	if err := fw.WriteMessage(MsgHello, Hello{ProtoVersion: CurrentProtoVersion, NodeName: "mallory-as-alice", Ed25519Pub: pubA, Token: tokA, Nonce: claimNonce}); err != nil {
		t.Fatalf("write hello: %v", err)
	}

	_, authPayload := readFrame(t, fr) // A's HelloAuth
	var authA HelloAuth
	if err := DecodeMessage(authPayload, &authA); err != nil {
		t.Fatalf("decode A's auth: %v", err)
	}

	// Reflect A's own signature back, unmodified, claiming it's ours.
	if err := fw.WriteMessage(MsgAuth, Auth{Sig: authA.Sig}); err != nil {
		t.Fatalf("write reflected auth: %v", err)
	}

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("Handshake accepted a reflected signature")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Handshake to reject")
	}
}

// TestHandshakeRejectsUnknownPeer: B behaves as a perfectly normal peer,
// but A's IsKnownPeer never recognizes anyone.
func TestHandshakeRejectsUnknownPeer(t *testing.T) {
	connA, connB := newPipeConnPair(t)
	defer connA.Close()
	defer connB.Close()

	privA, pubA := newTestIdentity(t)
	privB, pubB := newTestIdentity(t)
	tokA := mustToken(t, pubA, "alice")
	tokB := mustToken(t, pubB, "bob")

	cfgA := HandshakeConfig{
		IdentityKey: privA, NodeName: "alice", Token: tokA,
		IsKnownPeer: func(ed25519.PublicKey) bool { return false },
		Timeout:     5 * time.Second,
	}
	cfgB := HandshakeConfig{
		IdentityKey: privB, NodeName: "bob", Token: tokB,
		IsKnownPeer: func(p ed25519.PublicKey) bool { return bytes.Equal(p, pubA) },
		Timeout:     5 * time.Second,
	}

	errA, errB := runBothHandshakes(t, connA, connB, cfgA, cfgB)

	if errA == nil {
		t.Fatal("A accepted an unknown peer")
	}
	if errB == nil {
		t.Fatal("B's handshake succeeded despite A rejecting it")
	}
	var remoteErr *RemoteError
	if errors.As(errB, &remoteErr) {
		if remoteErr.Code != ErrCodeUnauthorized {
			t.Errorf("B's remote error code = %q, want %q", remoteErr.Code, ErrCodeUnauthorized)
		}
	}
}

// TestHandshakeRejectsWrongLengthPubkey sends a Hello with a truncated
// ed25519_pub.
func TestHandshakeRejectsWrongLengthPubkey(t *testing.T) {
	connA, connB := newPipeConnPair(t)
	defer connA.Close()
	defer connB.Close()

	privA, pubA := newTestIdentity(t)
	tokA := mustToken(t, pubA, "alice")
	cfgA := HandshakeConfig{IdentityKey: privA, NodeName: "alice", Token: tokA, IsKnownPeer: acceptAll, Timeout: 5 * time.Second}

	errCh := make(chan error, 1)
	go func() {
		_, err := AcceptHandshake(context.Background(), connA, cfgA)
		errCh <- err
	}()

	fw := NewWriter(connB)
	_, somePub := newTestIdentity(t)
	badTok := mustToken(t, somePub, "bad") // a validly-shaped token, unrelated to the truncated key below
	badPub := somePub[:16]                 // wrong length: 16, not 32
	if err := fw.WriteMessage(MsgHello, Hello{ProtoVersion: CurrentProtoVersion, NodeName: "x", Ed25519Pub: badPub, Token: badTok, Nonce: mustNonce(t)}); err != nil {
		t.Fatalf("write hello: %v", err)
	}

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("Handshake accepted a wrong-length ed25519_pub")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Handshake to reject")
	}
}

// TestHandshakeRejectsAllZeroPubkey sends a Hello whose ed25519_pub is
// exactly 32 bytes but all zero. The accompanying token's id is also
// all-zero (and thus matches), isolating the all-zero check from the
// separate token/pubkey-mismatch check.
func TestHandshakeRejectsAllZeroPubkey(t *testing.T) {
	connA, connB := newPipeConnPair(t)
	defer connA.Close()
	defer connB.Close()

	privA, pubA := newTestIdentity(t)
	tokA := mustToken(t, pubA, "alice")
	cfgA := HandshakeConfig{IdentityKey: privA, NodeName: "alice", Token: tokA, IsKnownPeer: acceptAll, Timeout: 5 * time.Second}

	errCh := make(chan error, 1)
	go func() {
		_, err := AcceptHandshake(context.Background(), connA, cfgA)
		errCh <- err
	}()

	fw := NewWriter(connB)
	zeroPub := make(ed25519.PublicKey, ed25519.PublicKeySize)
	zeroTok := mustToken(t, zeroPub, "zero")
	if err := fw.WriteMessage(MsgHello, Hello{ProtoVersion: CurrentProtoVersion, NodeName: "zero", Ed25519Pub: zeroPub, Token: zeroTok, Nonce: mustNonce(t)}); err != nil {
		t.Fatalf("write hello: %v", err)
	}

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("Handshake accepted an all-zero ed25519_pub")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Handshake to reject")
	}
}

// TestHandshakeRejectsTokenPubkeyMismatch sends a Hello whose token
// embeds a different id than the Hello's own ed25519_pub.
func TestHandshakeRejectsTokenPubkeyMismatch(t *testing.T) {
	connA, connB := newPipeConnPair(t)
	defer connA.Close()
	defer connB.Close()

	privA, pubA := newTestIdentity(t)
	tokA := mustToken(t, pubA, "alice")
	cfgA := HandshakeConfig{IdentityKey: privA, NodeName: "alice", Token: tokA, IsKnownPeer: acceptAll, Timeout: 5 * time.Second}

	errCh := make(chan error, 1)
	go func() {
		_, err := AcceptHandshake(context.Background(), connA, cfgA)
		errCh <- err
	}()

	fw := NewWriter(connB)
	_, pub1 := newTestIdentity(t)
	_, pub2 := newTestIdentity(t)
	tokForPub2 := mustToken(t, pub2, "mismatch")
	if err := fw.WriteMessage(MsgHello, Hello{ProtoVersion: CurrentProtoVersion, NodeName: "x", Ed25519Pub: pub1, Token: tokForPub2, Nonce: mustNonce(t)}); err != nil {
		t.Fatalf("write hello: %v", err)
	}

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("Handshake accepted a token whose id disagrees with ed25519_pub")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Handshake to reject")
	}
}

// TestHandshakeRejectsUnsupportedVersion configures A to only negotiate
// down to proto_version 2 while B only ever speaks version 1, so A must
// reject with ErrCodeUnsupportedVersion (and B, reading A's resulting
// Error frame instead of an Auth, must surface it as a *RemoteError with
// that code).
func TestHandshakeRejectsUnsupportedVersion(t *testing.T) {
	connA, connB := newPipeConnPair(t)
	defer connA.Close()
	defer connB.Close()

	privA, pubA := newTestIdentity(t)
	privB, pubB := newTestIdentity(t)
	tokA := mustToken(t, pubA, "alice")
	tokB := mustToken(t, pubB, "bob")

	cfgA := HandshakeConfig{
		IdentityKey: privA, NodeName: "alice", Token: tokA,
		OurVersion: 2, MinVersion: 2, // will not accept anything below 2
		IsKnownPeer: func(p ed25519.PublicKey) bool { return bytes.Equal(p, pubB) },
		Timeout:     5 * time.Second,
	}
	cfgB := HandshakeConfig{
		IdentityKey: privB, NodeName: "bob", Token: tokB,
		OurVersion: 1, MinVersion: 1, // only ever speaks v1
		IsKnownPeer: func(p ed25519.PublicKey) bool { return bytes.Equal(p, pubA) },
		Timeout:     5 * time.Second,
	}

	errA, errB := runBothHandshakes(t, connA, connB, cfgA, cfgB)

	if errA == nil {
		t.Fatal("A accepted an unsupported negotiated proto_version")
	}
	if errB == nil {
		t.Fatal("B's handshake succeeded despite A rejecting the version")
	}
	var remoteErr *RemoteError
	if !errors.As(errB, &remoteErr) {
		t.Fatalf("B's error is not a *RemoteError (A should have sent an Error frame): %v", errB)
	}
	if remoteErr.Code != ErrCodeUnsupportedVersion {
		t.Errorf("B's remote error code = %q, want %q", remoteErr.Code, ErrCodeUnsupportedVersion)
	}
}

func TestHandshakeErrorCodeUsesTypedRejection(t *testing.T) {
	err := &HandshakeRejectionError{Code: ErrCodeUnauthorized, Err: errors.New("wording without classification hints")}
	if got := errorCode(fmt.Errorf("accept peer: %w", err)); got != ErrCodeUnauthorized {
		t.Fatalf("errorCode = %q, want %q", got, ErrCodeUnauthorized)
	}
	if got := errorCode(errors.New("unknown peer key text alone")); got != ErrCodeBadHello {
		t.Fatalf("untyped errorCode = %q, want %q", got, ErrCodeBadHello)
	}
}

// TestHandshakeTimesOutOnSilence: the peer "connects" (the pipe is
// already up) and then never says a word.
func TestHandshakeTimesOutOnSilence(t *testing.T) {
	connA, connB := newPipeConnPair(t)
	defer connA.Close()
	defer connB.Close()

	privA, pubA := newTestIdentity(t)
	tokA := mustToken(t, pubA, "alice")
	cfgA := HandshakeConfig{
		IdentityKey: privA, NodeName: "alice", Token: tokA,
		IsKnownPeer: acceptAll,
		Timeout:     100 * time.Millisecond,
	}

	start := time.Now()
	_, err := Handshake(context.Background(), connA, cfgA)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("Handshake succeeded against a silent peer")
	}
	if elapsed > 5*time.Second {
		t.Fatalf("Handshake took %v to time out (Timeout was 100ms) — looks like it isn't bounding the read", elapsed)
	}
}

// TestHandshakeAbortsOnContextCancel: cancelling the context aborts a
// handshake that is already blocked reading, even though cfg.Timeout is
// nowhere near expiring. Handshake implements this by pulling conn's
// deadline in to "now" from a watcher goroutine, which only works if the
// connection honours a deadline armed against a Read that has already
// started.
//
// It could not be written before the pipe transport was backed by a real
// socket: the buffered in-memory conn it used to use evaluated its read
// deadline once, at the top of Read, so this cancellation never did
// anything and the test would simply hang.
func TestHandshakeAbortsOnContextCancel(t *testing.T) {
	connA, connB := newPipeConnPair(t)
	defer connA.Close()
	defer connB.Close()

	privA, pubA := newTestIdentity(t)
	tokA := mustToken(t, pubA, "alice")
	cfgA := HandshakeConfig{
		IdentityKey: privA, NodeName: "alice", Token: tokA,
		IsKnownPeer: acceptAll,
		// Far enough out that only the context can end this handshake.
		Timeout: time.Hour,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := Handshake(ctx, connA, cfgA)
		done <- err
	}()

	// Drain A's Hello but never reply, so A is definitely parked in its
	// read of ours before the cancel lands.
	if _, _, err := NewReader(connB).ReadFrame(); err != nil {
		t.Fatalf("read A's hello: %v", err)
	}

	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Handshake succeeded against a peer that never replied")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Handshake did not return after its context was canceled; the ctx watcher's SetDeadline is not interrupting the blocked read")
	}
}

// TestHandshakeRejectsOwnMismatchedToken checks the purely local
// validation: a HandshakeConfig whose Token doesn't match its own
// IdentityKey is rejected before anything is even written to the
// connection.
func TestHandshakeRejectsOwnMismatchedToken(t *testing.T) {
	connA, connB := newPipeConnPair(t)
	defer connA.Close()
	defer connB.Close()

	privA, _ := newTestIdentity(t)
	_, otherPub := newTestIdentity(t)
	badTok := mustToken(t, otherPub, "not-me")

	cfgA := HandshakeConfig{IdentityKey: privA, NodeName: "alice", Token: badTok, IsKnownPeer: acceptAll, Timeout: 5 * time.Second}
	if _, err := Handshake(context.Background(), connA, cfgA); err == nil {
		t.Fatal("Handshake accepted a Token whose id doesn't match our own IdentityKey")
	}
}

// --- deadline hygiene ---------------------------------------------------

// deadlineRecorder wraps a net.Conn and records every SetDeadline call, so
// a test can assert what state the connection was left in.
type deadlineRecorder struct {
	net.Conn
	mu       sync.Mutex
	current  time.Time
	sawArmed bool // a non-zero deadline was set at some point
}

func (c *deadlineRecorder) SetDeadline(t time.Time) error {
	c.mu.Lock()
	c.current = t
	if !t.IsZero() {
		c.sawArmed = true
	}
	c.mu.Unlock()
	return c.Conn.SetDeadline(t)
}

func (c *deadlineRecorder) state() (current time.Time, sawArmed bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.current, c.sawArmed
}

// TestHandshakeClearsDeadlineOnSuccess pins the contract that a successful
// Handshake hands conn back with no deadline armed.
//
// This is a regression test for a bug that no other test could see. The
// deadline Handshake sets is an *absolute* time (handshakeStart+Timeout),
// and callers keep conn for the entire life of the session that follows.
// Left armed, it silently poisoned the connection 30s in: every Read and
// Write started failing with os.ErrDeadlineExceeded on a perfectly healthy
// link, the session's read loop exited without logging anything, and the
// peer was torn down by SPEC.md §4's 90s dead rule and redialed — forever,
// at a steady ~90s period.
//
// Nothing caught it because the deadline is 30s out by default and no test
// runs long enough to reach it. Hence asserting on the deadline directly
// rather than on downstream I/O.
func TestHandshakeClearsDeadlineOnSuccess(t *testing.T) {
	rawA, rawB := newPipeConnPair(t)
	defer rawA.Close()
	defer rawB.Close()
	connA := &deadlineRecorder{Conn: rawA}
	connB := &deadlineRecorder{Conn: rawB}

	privA, pubA := newTestIdentity(t)
	privB, pubB := newTestIdentity(t)

	errA, errB := runBothHandshakes(t, connA, connB,
		HandshakeConfig{
			IdentityKey: privA,
			NodeName:    "alice",
			Token:       mustToken(t, pubA, "alice"),
			IsKnownPeer: func(p ed25519.PublicKey) bool { return bytes.Equal(p, pubB) },
		},
		HandshakeConfig{
			IdentityKey: privB,
			NodeName:    "bob",
			Token:       mustToken(t, pubB, "bob"),
			IsKnownPeer: func(p ed25519.PublicKey) bool { return bytes.Equal(p, pubA) },
		})
	if errA != nil {
		t.Fatalf("A's handshake: %v", errA)
	}
	if errB != nil {
		t.Fatalf("B's handshake: %v", errB)
	}

	for _, tc := range []struct {
		name string
		conn *deadlineRecorder
	}{{"dialer", connA}, {"accepter", connB}} {
		current, sawArmed := tc.conn.state()
		// Guard against this test passing vacuously if the bounding
		// deadline is ever dropped from Handshake altogether.
		if !sawArmed {
			t.Errorf("%s: Handshake never armed a deadline; it is supposed to bound itself by cfg.Timeout", tc.name)
		}
		if !current.IsZero() {
			t.Errorf("%s: Handshake left a deadline armed at %v; it must be cleared on success or it will poison every later Read/Write on this conn", tc.name, current)
		}
	}
}

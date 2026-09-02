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
// internal/transport's in-memory PipeTransport (backed by pipe.go's
// buffered pipe — not a bare net.Pipe, which would deadlock on the
// handshake's "both sides send Hello immediately" requirement, and which
// doesn't support deadlines the way [TestHandshakeTimesOutOnSilence]
// needs).
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
		_, err := Handshake(context.Background(), connA, cfgA)
		chA <- err
	}()
	go func() {
		_, err := Handshake(context.Background(), connB, cfgB)
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
		res, err := Handshake(context.Background(), connA, cfgA)
		chA <- outcome{res, err}
	}()
	go func() {
		res, err := Handshake(context.Background(), connB, cfgB)
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
		_, err := Handshake(context.Background(), connA, cfgA)
		errCh <- err
	}()

	fw := NewWriter(connB)
	fr := NewReader(connB)

	readFrame(t, fr) // drain A's real Hello

	nonceM := mustNonce(t)
	if err := fw.WriteMessage(MsgHello, Hello{ProtoVersion: 1, NodeName: "mallory", Ed25519Pub: pubM, Token: tokM, Nonce: nonceM}); err != nil {
		t.Fatalf("write hello: %v", err)
	}

	readFrame(t, fr) // drain A's real Auth

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
		_, err := Handshake(context.Background(), connA, cfgA)
		errCh <- err
	}()

	fw := NewWriter(connB)
	fr := NewReader(connB)

	readFrame(t, fr) // drain A's real Hello

	claimNonce := mustNonce(t)
	if err := fw.WriteMessage(MsgHello, Hello{ProtoVersion: 1, NodeName: "mallory-as-alice", Ed25519Pub: pubA, Token: tokA, Nonce: claimNonce}); err != nil {
		t.Fatalf("write hello: %v", err)
	}

	_, authPayload := readFrame(t, fr) // A's real Auth
	var authA Auth
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
		_, err := Handshake(context.Background(), connA, cfgA)
		errCh <- err
	}()

	fw := NewWriter(connB)
	fr := NewReader(connB)
	readFrame(t, fr) // drain A's Hello

	_, somePub := newTestIdentity(t)
	badTok := mustToken(t, somePub, "bad") // a validly-shaped token, unrelated to the truncated key below
	badPub := somePub[:16]                 // wrong length: 16, not 32
	if err := fw.WriteMessage(MsgHello, Hello{ProtoVersion: 1, NodeName: "x", Ed25519Pub: badPub, Token: badTok, Nonce: mustNonce(t)}); err != nil {
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
		_, err := Handshake(context.Background(), connA, cfgA)
		errCh <- err
	}()

	fw := NewWriter(connB)
	fr := NewReader(connB)
	readFrame(t, fr) // drain A's Hello

	zeroPub := make(ed25519.PublicKey, ed25519.PublicKeySize)
	zeroTok := mustToken(t, zeroPub, "zero")
	if err := fw.WriteMessage(MsgHello, Hello{ProtoVersion: 1, NodeName: "zero", Ed25519Pub: zeroPub, Token: zeroTok, Nonce: mustNonce(t)}); err != nil {
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
		_, err := Handshake(context.Background(), connA, cfgA)
		errCh <- err
	}()

	fw := NewWriter(connB)
	fr := NewReader(connB)
	readFrame(t, fr) // drain A's Hello

	_, pub1 := newTestIdentity(t)
	_, pub2 := newTestIdentity(t)
	tokForPub2 := mustToken(t, pub2, "mismatch")
	if err := fw.WriteMessage(MsgHello, Hello{ProtoVersion: 1, NodeName: "x", Ed25519Pub: pub1, Token: tokForPub2, Nonce: mustNonce(t)}); err != nil {
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
// Nothing caught it because bufConn's Write deadline is a no-op and its
// read deadline is only consulted against real wall-clock time, which no
// in-memory test runs long enough to reach. Hence asserting on the
// deadline directly rather than on downstream I/O.
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

// fakeClock is a manually-driven [Clock] for deterministic keepalive
// tests: nothing here ever sleeps on a wall clock. Advance fires any
// pending After channels whose deadline it reaches, synchronously with
// respect to the caller (though the goroutines woken by those channels
// still run concurrently, same as with a real timer) — tests synchronize
// with those goroutines via their own channels, never by sleeping.
type fakeClock struct {
	mu      sync.Mutex
	now     time.Time
	waiters []fakeWaiter
	// notify is closed and replaced (the same channel-swap idiom used by
	// transport's bufConn) every time a new waiter registers, so
	// waitForWaiters can block on it instead of polling/sleeping.
	notify chan struct{}
}

type fakeWaiter struct {
	deadline time.Time
	ch       chan time.Time
}

func newFakeClock(start time.Time) *fakeClock {
	return &fakeClock{now: start, notify: make(chan struct{})}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) After(d time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	c.mu.Lock()
	deadline := c.now.Add(d)
	if d <= 0 {
		c.mu.Unlock()
		ch <- deadline
		return ch
	}
	c.waiters = append(c.waiters, fakeWaiter{deadline: deadline, ch: ch})
	old := c.notify
	c.notify = make(chan struct{})
	c.mu.Unlock()
	close(old)
	return ch
}

// Advance moves the fake clock forward by d, firing (synchronously, in
// deadline order) every waiter whose deadline has now been reached.
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	now := c.now
	var fired []fakeWaiter
	remaining := c.waiters[:0]
	for _, w := range c.waiters {
		if !w.deadline.After(now) {
			fired = append(fired, w)
		} else {
			remaining = append(remaining, w)
		}
	}
	c.waiters = remaining
	c.mu.Unlock()

	for _, w := range fired {
		w.ch <- now
	}
}

// waitForWaiters blocks until at least min goroutines are parked on
// clock.After (i.e. Keepalive.Run has reached its next select and is
// ready to observe an Advance), or timeout elapses. This is the
// synchronization primitive that keeps the Run-driving tests below
// deterministic: without it, a test's Advance could race Run's goroutine
// not yet having re-registered its next timer, silently attaching that
// registration to a clock value that already moved past when the test
// expected it to fire. The bounded time.After here is only a test
// safety-net deadline, not a polling interval — waking happens
// event-driven via c.notify.
func (c *fakeClock) waitForWaiters(min int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		c.mu.Lock()
		n := len(c.waiters)
		ch := c.notify
		c.mu.Unlock()
		if n >= min {
			return true
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return false
		}
		select {
		case <-ch:
		case <-time.After(remaining):
			return false
		}
	}
}

func TestKeepaliveNeedsPingAndDead(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := newFakeClock(start)
	k := NewKeepalive(clock, 30*time.Second, 90*time.Second)

	if k.NeedsPing() {
		t.Error("NeedsPing() immediately after construction: want false")
	}
	if k.Dead() {
		t.Error("Dead() immediately after construction: want false")
	}

	clock.Advance(29 * time.Second)
	if k.NeedsPing() {
		t.Error("NeedsPing() at 29s: want false")
	}

	clock.Advance(1 * time.Second) // now at 30s
	if !k.NeedsPing() {
		t.Error("NeedsPing() at 30s: want true")
	}
	if k.Dead() {
		t.Error("Dead() at 30s: want false")
	}

	k.RecordSent()
	if k.NeedsPing() {
		t.Error("NeedsPing() right after RecordSent: want false")
	}

	clock.Advance(60 * time.Second) // total 90s since start, but lastRecv never updated
	if !k.Dead() {
		t.Error("Dead() at 90s with nothing ever received since construction: want true")
	}

	k.RecordReceived()
	if k.Dead() {
		t.Error("Dead() right after RecordReceived: want false")
	}
}

func TestKeepaliveRunPingsAtIntervalAndPongResets(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := newFakeClock(start)
	k := NewKeepalive(clock, 30*time.Second, 90*time.Second)

	pings := make(chan struct{}, 10)
	deadCh := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go k.Run(ctx, 30*time.Second, func() {
		k.RecordSent()
		pings <- struct{}{}
	}, func() {
		deadCh <- struct{}{}
	})

	// First tick at +30s: idle since construction, so a ping is due. Wait
	// for Run to actually be parked on its timer before advancing past
	// it — see waitForWaiters' doc comment for why this matters.
	if !clock.waitForWaiters(1, 5*time.Second) {
		t.Fatal("timed out waiting for Run to register its first timer")
	}
	clock.Advance(30 * time.Second)
	select {
	case <-pings:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for first ping at 30s")
	}

	// Simulate the peer's Pong arriving, keeping the connection alive.
	k.RecordReceived()

	// Second tick at +60s: lastSent was reset by the ping callback at
	// +30s, so another ping is due.
	if !clock.waitForWaiters(1, 5*time.Second) {
		t.Fatal("timed out waiting for Run to register its second timer")
	}
	clock.Advance(30 * time.Second)
	select {
	case <-pings:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for second ping at 60s")
	}

	select {
	case <-deadCh:
		t.Fatal("Run reported dead while Pongs were arriving on schedule")
	default:
	}
}

func TestKeepaliveRunReportsDeadAfterSilence(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := newFakeClock(start)
	k := NewKeepalive(clock, 30*time.Second, 90*time.Second)

	pings := make(chan struct{}, 10)
	deadCh := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Note: onPing here does NOT call RecordReceived (no Pong ever
	// arrives), so lastRecv stays pinned at construction time and the
	// connection should be declared dead once 90s have elapsed.
	go k.Run(ctx, 30*time.Second, func() {
		k.RecordSent()
		pings <- struct{}{}
	}, func() {
		deadCh <- struct{}{}
	})

	for i := 0; i < 2; i++ {
		if !clock.waitForWaiters(1, 5*time.Second) {
			t.Fatalf("timed out waiting for Run to register timer #%d", i+1)
		}
		clock.Advance(30 * time.Second)
		select {
		case <-pings:
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for ping #%d", i+1)
		}
	}

	// Now at +60s. One more 30s tick reaches +90s of total silence on the
	// receive side.
	if !clock.waitForWaiters(1, 5*time.Second) {
		t.Fatal("timed out waiting for Run to register its third timer")
	}
	clock.Advance(30 * time.Second)
	select {
	case <-deadCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Run to report the connection dead at 90s")
	}

	// Run must have returned (not kept polling) once it declared the
	// connection dead.
	select {
	case <-pings:
		t.Error("Run kept sending pings after declaring the connection dead")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestKeepaliveRunStopsOnContextCancel(t *testing.T) {
	clock := newFakeClock(time.Now())
	k := NewKeepalive(clock, 30*time.Second, 90*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		k.Run(ctx, 30*time.Second, nil, nil)
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return promptly after ctx was canceled")
	}
}

func TestNewKeepaliveDefaults(t *testing.T) {
	k := NewKeepalive(nil, 0, 0)
	if k.clock == nil {
		t.Error("clock defaulted to nil, want RealClock")
	}
	if k.pingInterval != DefaultPingInterval {
		t.Errorf("pingInterval = %v, want %v", k.pingInterval, DefaultPingInterval)
	}
	if k.deadAfter != DefaultDeadAfter {
		t.Errorf("deadAfter = %v, want %v", k.deadAfter, DefaultDeadAfter)
	}
}

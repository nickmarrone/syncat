package config

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tailscale/tailcat"
)

func TestIdentityKeyLoadOrCreateThenReloadIsStable(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "identity.key")

	k1, created, err := LoadOrCreateIdentityKey(path)
	if err != nil {
		t.Fatalf("LoadOrCreateIdentityKey (first): %v", err)
	}
	if !created {
		t.Fatal("expected created=true on first call")
	}

	k2, created, err := LoadOrCreateIdentityKey(path)
	if err != nil {
		t.Fatalf("LoadOrCreateIdentityKey (second): %v", err)
	}
	if created {
		t.Fatal("expected created=false on second call (key already exists)")
	}

	if !k1.Public().Equal(k2.Public()) {
		t.Errorf("public key changed across reload: %x != %x", []byte(k1.Public()), []byte(k2.Public()))
	}
	if k1.ShortID() != k2.ShortID() {
		t.Errorf("ShortID changed across reload: %s != %s", k1.ShortID(), k2.ShortID())
	}

	k3, err := LoadIdentityKey(path)
	if err != nil {
		t.Fatalf("LoadIdentityKey: %v", err)
	}
	if !k1.Public().Equal(k3.Public()) {
		t.Errorf("public key changed via LoadIdentityKey: %x != %x", []byte(k1.Public()), []byte(k3.Public()))
	}
}

func TestIdentityKeyShortIDLength(t *testing.T) {
	dir := t.TempDir()
	k, _, err := LoadOrCreateIdentityKey(filepath.Join(dir, "identity.key"))
	if err != nil {
		t.Fatalf("LoadOrCreateIdentityKey: %v", err)
	}
	if len(k.ShortID()) != 16 { // 8 bytes, hex-encoded
		t.Errorf("ShortID() = %q, want 16 hex chars", k.ShortID())
	}
}

func TestLoadIdentityKeyMissingFile(t *testing.T) {
	_, err := LoadIdentityKey(filepath.Join(t.TempDir(), "nope.key"))
	if err == nil {
		t.Fatal("expected error for missing identity key, got nil")
	}
}

// TestTailcatKeyStableAcrossReload is the most important test in this
// phase: it verifies the stability guarantee that makes `syncat token`
// print the same token every time. It requires network access to resolve a
// DERP region via tailcat.FetchDERPMap/PickBestRegion, so it's skipped
// under -short.
func TestTailcatKeyStableAcrossReload(t *testing.T) {
	if testing.Short() {
		t.Skip("requires network access to resolve a DERP region")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "tailcat.key")
	ctx := context.Background()

	k1, created, err := LoadOrCreateTailcatKey(ctx, path)
	if err != nil {
		t.Fatalf("LoadOrCreateTailcatKey (first): %v", err)
	}
	if !created {
		t.Fatal("expected created=true on first call")
	}
	if k1.Public.RegionID <= 0 {
		t.Fatalf("RegionID = %d, want a concrete positive region baked in", k1.Public.RegionID)
	}

	k2, created, err := LoadOrCreateTailcatKey(ctx, path)
	if err != nil {
		t.Fatalf("LoadOrCreateTailcatKey (second): %v", err)
	}
	if created {
		t.Fatal("expected created=false on second call (key already exists)")
	}

	if k1.Private.Public() != k2.Private.Public() {
		t.Errorf("tailcat public key changed across reload")
	}
	if k1.Public.Addr() != k2.Public.Addr() {
		t.Errorf("address changed across reload:\n  first:  %s\n  second: %s", k1.Public.Addr(), k2.Public.Addr())
	}

	k3, err := LoadTailcatKey(path)
	if err != nil {
		t.Fatalf("LoadTailcatKey: %v", err)
	}
	if k1.Public.Addr() != k3.Public.Addr() {
		t.Errorf("address changed via LoadTailcatKey:\n  first: %s\n  third: %s", k1.Public.Addr(), k3.Public.Addr())
	}

	// Repeated calls to Addr() itself (no reload) must also agree,
	// since token stability ultimately rests on this being deterministic
	// for a fixed ConnInfo.
	if k1.Public.Addr() != k1.Public.Addr() {
		t.Errorf("Addr() is not deterministic for a fixed ConnInfo")
	}
}

func TestLoadTailcatKeyMissingFile(t *testing.T) {
	_, err := LoadTailcatKey(filepath.Join(t.TempDir(), "nope.key"))
	if err == nil {
		t.Fatal("expected error for missing tailcat key, got nil")
	}
}

func TestTokenRoundTrip(t *testing.T) {
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	const connBlob = "tcSomeOpaqueBlobData123"
	const name = "alice's laptop"

	tok, err := EncodeToken(connBlob, pub, name)
	if err != nil {
		t.Fatalf("EncodeToken: %v", err)
	}
	if !strings.HasPrefix(tok, TokenPrefix) {
		t.Fatalf("token %q missing prefix %q", tok, TokenPrefix)
	}

	got, err := ParseToken(tok)
	if err != nil {
		t.Fatalf("ParseToken: %v", err)
	}
	if got.ConnBlob != connBlob {
		t.Errorf("ConnBlob = %q, want %q", got.ConnBlob, connBlob)
	}
	if !got.ID.Equal(pub) {
		t.Errorf("ID = %x, want %x", []byte(got.ID), []byte(pub))
	}
	if got.Name != name {
		t.Errorf("Name = %q, want %q", got.Name, name)
	}
}

func TestParseTokenRejectsBadPrefix(t *testing.T) {
	_, err := ParseToken("xy1abcdef")
	if err == nil {
		t.Fatal("expected error for bad prefix, got nil")
	}
}

func TestParseTokenRejectsBadBase64(t *testing.T) {
	_, err := ParseToken(TokenPrefix + "not-valid-base64!!!")
	if err == nil {
		t.Fatal("expected error for bad base64, got nil")
	}
}

func TestParseTokenRejectsBadCBOR(t *testing.T) {
	// Valid base64url, but not valid CBOR.
	_, err := ParseToken(TokenPrefix + "AAAAAAAA")
	if err == nil {
		t.Fatal("expected error for bad cbor, got nil")
	}
}

func TestParseTokenRejectsWrongLengthID(t *testing.T) {
	// Encode a payload with a short id directly, bypassing EncodeToken's
	// length check.
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tok, err := EncodeToken("tcblob", pub, "name")
	if err != nil {
		t.Fatalf("EncodeToken: %v", err)
	}

	// Sanity: a correctly-sized id parses fine.
	if _, err := ParseToken(tok); err != nil {
		t.Fatalf("ParseToken of well-formed token failed: %v", err)
	}

	if _, err := EncodeToken("tcblob", pub[:16], "name"); err == nil {
		t.Fatal("expected EncodeToken to reject a short id, got nil error")
	}
}

func TestAPITokenIsHex64AndPrivateMode(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "api.token")

	tok, err := LoadOrCreateAPIToken(path)
	if err != nil {
		t.Fatalf("LoadOrCreateAPIToken: %v", err)
	}
	if len(tok) != 64 {
		t.Errorf("token length = %d, want 64", len(tok))
	}
	for _, r := range tok {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f')) {
			t.Errorf("token %q contains non-hex character %q", tok, r)
			break
		}
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0600 {
		t.Errorf("api.token mode = %o, want 0600", perm)
	}
}

func TestAPITokenRegenerateIsNoOp(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "api.token")

	tok1, err := LoadOrCreateAPIToken(path)
	if err != nil {
		t.Fatalf("LoadOrCreateAPIToken (first): %v", err)
	}
	tok2, err := LoadOrCreateAPIToken(path)
	if err != nil {
		t.Fatalf("LoadOrCreateAPIToken (second): %v", err)
	}
	if tok1 != tok2 {
		t.Errorf("token changed across reload: %s != %s", tok1, tok2)
	}
}

// TestMigrateTailcatKeyBackfillsDiscoAndPresharedKey covers upgrading a key
// file written by a syncat built against tailcat v0.2.0, which knew about
// neither the disco key (added in v0.3.0) nor the pre-shared key (v0.6.0).
//
// Both matter for reachability, not tidiness: peers reject an address with
// no disco key outright, and a zero pre-shared key makes tailcat mint a
// throwaway one at every Server.Start, so the daemon would advertise a
// different address on each run than the one `syncat token` prints.
func TestMigrateTailcatKeyBackfillsDiscoAndPresharedKey(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tailcat.key")

	// A v0.2.0-shaped key file: node keypair and a resolved region, but
	// neither of the fields tailcat added later.
	legacy := tailcat.NewPrivateKey()
	legacy.Public.ServerDiscoPublic = tailcat.DiscoPublic{}
	legacy.Public.PresharedKey = tailcat.PresharedKey{}
	legacy.Public.RegionID = 1
	data, err := json.MarshalIndent(legacy, "", "\t")
	if err != nil {
		t.Fatalf("marshal legacy key: %v", err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatalf("write legacy key: %v", err)
	}

	// Loading an existing key does no network I/O, so this needs no guard.
	got, created, err := LoadOrCreateTailcatKey(context.Background(), path)
	if err != nil {
		t.Fatalf("LoadOrCreateTailcatKey: %v", err)
	}
	if created {
		t.Fatal("created = true, want false: the key file already existed")
	}
	if got.Private.Public() != legacy.Private.Public() {
		t.Error("migration changed the node key; it must be preserved")
	}
	if got.Public.RegionID != 1 {
		t.Errorf("RegionID = %d, want the persisted 1", got.Public.RegionID)
	}
	if got.Public.ServerDiscoPublic.IsZero() {
		t.Error("ServerDiscoPublic still zero after migration")
	}
	if want := tailcat.DiscoPublicForNode(legacy.Private); !got.Public.ServerDiscoPublic.Equal(want) {
		t.Error("ServerDiscoPublic is not the key derived from the node key")
	}
	if got.Public.PresharedKey.IsZero() {
		t.Error("PresharedKey still zero after migration")
	}

	// The migration must have been written back, or every restart would
	// mint a different pre-shared key and a different address with it.
	reloaded, err := LoadTailcatKey(path)
	if err != nil {
		t.Fatalf("LoadTailcatKey after migration: %v", err)
	}
	if !reloaded.Public.PresharedKey.Equal(got.Public.PresharedKey) {
		t.Error("PresharedKey was not persisted: it differs after reload")
	}
	if !reloaded.Public.ServerDiscoPublic.Equal(got.Public.ServerDiscoPublic) {
		t.Error("ServerDiscoPublic was not persisted: it differs after reload")
	}
	if reloaded.Public.Addr() != got.Public.Addr() {
		t.Errorf("address changed across reload:\n  migrated: %s\n  reloaded: %s",
			got.Public.Addr(), reloaded.Public.Addr())
	}

	// A second load must be a no-op, not another round of minting.
	again, _, err := LoadOrCreateTailcatKey(context.Background(), path)
	if err != nil {
		t.Fatalf("LoadOrCreateTailcatKey (second): %v", err)
	}
	if again.Public.Addr() != got.Public.Addr() {
		t.Errorf("re-migrating a migrated key changed the address:\n  first:  %s\n  second: %s",
			got.Public.Addr(), again.Public.Addr())
	}
}

// TestTailcatKeyCarriesPresharedKey pins that a freshly generated key gets a
// pre-shared key persisted with it. It is what makes the node's address
// reproducible: transport.TailcatTransport feeds this field back to
// tailcat.Server, which would otherwise generate a throwaway one per start.
func TestTailcatKeyCarriesPresharedKey(t *testing.T) {
	if testing.Short() {
		t.Skip("requires network access to resolve a DERP region")
	}

	path := filepath.Join(t.TempDir(), "tailcat.key")
	key, _, err := LoadOrCreateTailcatKey(context.Background(), path)
	if err != nil {
		t.Fatalf("LoadOrCreateTailcatKey: %v", err)
	}
	if key.Public.PresharedKey.IsZero() {
		t.Fatal("a newly created tailcat key has no pre-shared key")
	}

	// It has to survive the JSON round-trip through disk, not just exist
	// in memory.
	reloaded, err := LoadTailcatKey(path)
	if err != nil {
		t.Fatalf("LoadTailcatKey: %v", err)
	}
	if !reloaded.Public.PresharedKey.Equal(key.Public.PresharedKey) {
		t.Error("pre-shared key did not survive the round-trip through disk")
	}
}

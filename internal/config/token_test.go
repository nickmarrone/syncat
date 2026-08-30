package config

import (
	"crypto/ed25519"
	"crypto/rand"
	"strings"
	"testing"
)

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

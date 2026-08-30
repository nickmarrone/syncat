package config

import (
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/fxamacker/cbor/v2"
)

// TokenPrefix versions the syncat node token format (SPEC.md §2).
const TokenPrefix = "sc1"

// tokenPayload is the CBOR body of a node token. It's encoded as a CBOR map
// (not an array) keyed by these short field names so that decoding ignores
// unknown fields going forward (SPEC.md §11 forward-compat obligation).
type tokenPayload struct {
	TC   string `cbor:"tc"`
	ID   []byte `cbor:"id"`
	Name string `cbor:"name"`
}

// NodeToken is the parsed form of a syncat node token: the tailcat
// connection blob, the node's Ed25519 public key, and its suggested display
// name.
type NodeToken struct {
	ConnBlob string
	ID       ed25519.PublicKey
	Name     string
}

// EncodeToken builds an `sc1...` token wrapping connBlob (a tailcat
// ConnBlob string), the node's Ed25519 public key, and its display name.
func EncodeToken(connBlob string, id ed25519.PublicKey, name string) (string, error) {
	if len(id) != ed25519.PublicKeySize {
		return "", fmt.Errorf("config: token: identity key must be %d bytes, got %d", ed25519.PublicKeySize, len(id))
	}

	payload := tokenPayload{TC: connBlob, ID: []byte(id), Name: name}
	data, err := cbor.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("config: token: cbor encode: %w", err)
	}
	return TokenPrefix + base64.RawURLEncoding.EncodeToString(data), nil
}

// ParseToken decodes an `sc1...` token, rejecting a missing/wrong prefix,
// invalid base64, invalid CBOR, or an id that isn't exactly 32 bytes.
func ParseToken(token string) (*NodeToken, error) {
	rest, ok := strings.CutPrefix(token, TokenPrefix)
	if !ok {
		return nil, fmt.Errorf("config: token: missing %q prefix", TokenPrefix)
	}

	data, err := base64.RawURLEncoding.DecodeString(rest)
	if err != nil {
		return nil, fmt.Errorf("config: token: base64 decode: %w", err)
	}

	var payload tokenPayload
	if err := cbor.Unmarshal(data, &payload); err != nil {
		return nil, fmt.Errorf("config: token: cbor decode: %w", err)
	}

	if len(payload.ID) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("config: token: id has %d bytes, want %d", len(payload.ID), ed25519.PublicKeySize)
	}

	return &NodeToken{
		ConnBlob: payload.TC,
		ID:       ed25519.PublicKey(payload.ID),
		Name:     payload.Name,
	}, nil
}

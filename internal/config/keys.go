package config

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/tailscale/tailcat"
)

// --- the Ed25519 application identity key (SPEC.md §2) -----------------

// IdentityKey is the node's Ed25519 application identity keypair (SPEC.md
// §2), used to sign the syncat protocol handshake independent of whatever
// the tailcat transport itself authenticates.
//
// On-disk format (identity.key, mode 0600): the 64-byte Ed25519 private key
// (seed || public key, as returned by crypto/ed25519), hex-encoded, with a
// trailing newline. This is deliberately the simplest possible encoding;
// the public key is derived from it rather than stored separately.
type IdentityKey struct {
	Private ed25519.PrivateKey
}

// Public returns the Ed25519 public key.
func (k *IdentityKey) Public() ed25519.PublicKey {
	return k.Private.Public().(ed25519.PublicKey)
}

// ShortID returns the first 8 bytes of the Ed25519 public key, hex-encoded.
// This is the node id used as a version-vector key (SPEC.md §5).
func (k *IdentityKey) ShortID() string {
	pub := k.Public()
	return hex.EncodeToString(pub[:8])
}

// LoadIdentityKey reads an existing identity key from path. It returns an
// error wrapping os.ErrNotExist if the file doesn't exist.
func LoadIdentityKey(path string) (*IdentityKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read identity key %s: %w", path, err)
	}
	return parseIdentityKey(data)
}

// LoadOrCreateIdentityKey loads the identity key at path, generating and
// persisting a new one if none exists. created reports whether a new key
// was generated.
func LoadOrCreateIdentityKey(path string) (key *IdentityKey, created bool, err error) {
	if data, readErr := os.ReadFile(path); readErr == nil {
		key, err = parseIdentityKey(data)
		return key, false, err
	} else if !errors.Is(readErr, os.ErrNotExist) {
		return nil, false, fmt.Errorf("config: read identity key %s: %w", path, readErr)
	}

	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, false, fmt.Errorf("config: generate identity key: %w", err)
	}
	key = &IdentityKey{Private: priv}
	if err := saveIdentityKey(path, key); err != nil {
		return nil, false, err
	}
	return key, true, nil
}

func parseIdentityKey(data []byte) (*IdentityKey, error) {
	raw, err := hex.DecodeString(strings.TrimSpace(string(data)))
	if err != nil {
		return nil, fmt.Errorf("config: decode identity key: %w", err)
	}
	if len(raw) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("config: identity key has %d bytes, want %d", len(raw), ed25519.PrivateKeySize)
	}
	return &IdentityKey{Private: ed25519.PrivateKey(raw)}, nil
}

func saveIdentityKey(path string, key *IdentityKey) error {
	encoded := hex.EncodeToString(key.Private) + "\n"
	if err := writeFileAtomic(path, []byte(encoded), 0600); err != nil {
		return fmt.Errorf("config: save identity key %s: %w", path, err)
	}
	return nil
}

// --- the tailcat transport key -----------------------------------------

// LoadTailcatKey reads an existing tailcat saved key from path. It returns
// an error wrapping os.ErrNotExist if the file doesn't exist.
func LoadTailcatKey(path string) (*tailcat.PrivateKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read tailcat key %s: %w", path, err)
	}
	var priv tailcat.PrivateKey
	if err := json.Unmarshal(data, &priv); err != nil {
		return nil, fmt.Errorf("config: parse tailcat key %s: %w", path, err)
	}
	return &priv, nil
}

// LoadOrCreateTailcatKey loads the tailcat saved key at path, generating and
// persisting a new one if none exists. created reports whether a new key
// was generated.
//
// Generating a key requires resolving a concrete DERP relay region once
// (network access) and baking it into PrivateKey.Public.RegionID. This
// deviates from leaving RegionID at tailcat's "auto" sentinel (-1): tailcat
// re-runs a latency netcheck on every server start when RegionID is -1,
// which can select a different region each run and would make `syncat
// token` print a different token on every invocation. Resolving once at
// init time and persisting the concrete RegionID keeps the token stable for
// the life of the key, matching cmd/tailcat's `genkey --fixed-region`.
func LoadOrCreateTailcatKey(ctx context.Context, path string) (key *tailcat.PrivateKey, created bool, err error) {
	if data, readErr := os.ReadFile(path); readErr == nil {
		var priv tailcat.PrivateKey
		if err := json.Unmarshal(data, &priv); err != nil {
			return nil, false, fmt.Errorf("config: parse tailcat key %s: %w", path, err)
		}
		return &priv, false, nil
	} else if !errors.Is(readErr, os.ErrNotExist) {
		return nil, false, fmt.Errorf("config: read tailcat key %s: %w", path, readErr)
	}

	priv := tailcat.NewPrivateKey()

	fetchCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	dm, err := tailcat.FetchDERPMap(fetchCtx, tailcat.ExpandForServer)
	if err != nil {
		return nil, false, fmt.Errorf("config: fetch DERP map: %w", err)
	}
	regionID, err := tailcat.PickBestRegion(fetchCtx, dm)
	if err != nil {
		return nil, false, fmt.Errorf("config: pick nearest DERP region: %w", err)
	}
	if regionID == 0 {
		return nil, false, errors.New("config: could not determine nearest DERP region (no usable latency measurements)")
	}
	priv.Public.RegionID = regionID

	data, err := json.MarshalIndent(priv, "", "\t")
	if err != nil {
		return nil, false, fmt.Errorf("config: encode tailcat key: %w", err)
	}
	if err := writeFileAtomic(path, data, 0600); err != nil {
		return nil, false, fmt.Errorf("config: save tailcat key %s: %w", path, err)
	}
	return priv, true, nil
}

// TokenPrefix versions the syncat node token format (SPEC.md §2).
const TokenPrefix = "sc1"

// --- sc1 node tokens (SPEC.md §2) --------------------------------------

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

// apiTokenBytes is the number of random bytes in api.token, hex-encoded to
// 64 characters (SPEC.md §3).
const apiTokenBytes = 32

// --- the REST API token (SPEC.md §3) -----------------------------------

// LoadOrCreateAPIToken loads the REST API auth token from path, generating
// and persisting a new random 64-hex-character token (mode 0600) if none
// exists. Regenerating is a no-op if a valid token is already present.
func LoadOrCreateAPIToken(path string) (string, error) {
	if data, err := os.ReadFile(path); err == nil {
		tok := strings.TrimSpace(string(data))
		if _, err := hex.DecodeString(tok); err != nil || len(tok) != apiTokenBytes*2 {
			return "", fmt.Errorf("config: api token %s is malformed (want %d hex chars)", path, apiTokenBytes*2)
		}
		return tok, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("config: read api token %s: %w", path, err)
	}

	raw := make([]byte, apiTokenBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("config: generate api token: %w", err)
	}
	tok := hex.EncodeToString(raw)
	if err := writeFileAtomic(path, []byte(tok), 0600); err != nil {
		return "", fmt.Errorf("config: save api token %s: %w", path, err)
	}
	return tok, nil
}

// --- share ids ---------------------------------------------------------

// NewShareID returns a random 8-byte hex-encoded (16 character) share id,
// generated at share creation and stable for the share's life (SPEC.md §3).
func NewShareID() (string, error) {
	raw := make([]byte, 8)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("config: generate share id: %w", err)
	}
	return hex.EncodeToString(raw), nil
}
